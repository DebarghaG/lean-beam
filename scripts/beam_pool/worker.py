"""One execution context per worker; Beam owns all proof-state semantics.

Workers run against immutable project/runtime artifacts. Snapshots are trusted native
code-bearing artifacts, admitted only after exact runtime and dependency hash checks.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

from .lsp import Lsp, RpcError
from .wire import MAX_BODY, read_frame, write_frame


def digest_file(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def runtime_digest(project, plugin):
    """Linux runtime identity, independent of installation paths.

    Include every SDK/project shared library as well as the executable's native
    dependency closure. Imported object files are checked separately per document.
    """
    prefix = Path(subprocess.check_output(["lean", "--print-prefix"], text=True).strip())
    files = {prefix / "bin/lean", plugin.resolve()}
    for root in (prefix / "lib", project, plugin.parent):
        files.update(path.resolve() for path in root.rglob("*.so") if path.is_file())
    for root in os.environ.get("LEAN_PATH", "").split(os.pathsep):
        if root:
            files.update(path.resolve() for path in Path(root).rglob("*.so") if path.is_file())
    for executable in (prefix / "bin/lean", plugin):
        linked = subprocess.check_output(["ldd", str(executable)], text=True)
        for line in linked.splitlines():
            if "not found" in line:
                raise RuntimeError(f"unresolved native library: {line.strip()}")
            words = line.replace("=>", " ").split()
            files.update(Path(word).resolve() for word in words if word.startswith("/"))
    # Names are used by Lean's closure relocation; bytes must match too.
    entries = sorted(set((path.name, digest_file(path)) for path in files))
    return hashlib.sha256(repr(entries).encode()).hexdigest()


class Worker:
    def __init__(self, project, plugin, token, *, memory_limit=256 * 1024 * 1024,
                 handle_limit=4096, document_limit=16, request_timeout=120):
        self.project, self.plugin = Path(project).resolve(), Path(plugin).resolve()
        if len(token) < 32:
            raise ValueError("BEAM_POOL_TOKEN must contain at least 32 characters")
        self.token = token
        self.memory_limit, self.handle_limit = memory_limit, handle_limit
        self.document_limit = document_limit
        self.request_timeout = request_timeout
        self.generation = uuid.uuid4().hex
        self.documents, self.handles, self.hashes = {}, {}, {}
        self.lock = asyncio.Lock()
        self.active = set()
        self.temp = tempfile.TemporaryDirectory(prefix="beam-worker-")
        self.lsp = None

    async def start(self):
        self.runtime = await asyncio.to_thread(runtime_digest, self.project, self.plugin)
        # Lean LSP derives module identities from LEAN_SRC_PATH, not initialize.rootUri.
        # Preserve Lake's source-root order, with a project-relative fallback for
        # standalone proof files instead of embedding an absolute file URI in names.
        source_roots = [root for root in os.environ.get("LEAN_SRC_PATH", "").split(os.pathsep) if root]
        if str(self.project) not in source_roots:
            source_roots.append(str(self.project))
        self.lsp = await Lsp.start(self.project, self.plugin, env={
            **os.environ, "BEAM_SNAPSHOT_RUNTIME": self.runtime,
            "LEAN_SRC_PATH": os.pathsep.join(source_roots),
        })

    def source(self, relative):
        path = (self.project / relative).resolve()
        if not path.is_relative_to(self.project) or path.suffix != ".lean":
            raise ValueError("expected a Lean source inside the worker project")
        return path

    async def document(self, relative):
        path = self.source(relative)
        source_hash = digest_file(path)
        if relative in self.documents:
            doc, original = self.documents.pop(relative)
            self.documents[relative] = doc, original
            if original != source_hash:
                raise ValueError("worker project changed; start a new worker generation")
            return doc
        if len(self.documents) >= self.document_limit:
            victim = next((name for name in self.documents
                           if not any(value[1] == name for value in self.handles.values())), None)
            if victim is None:
                raise ValueError("worker document budget exhausted by retained handles")
            old_doc, _ = self.documents.pop(victim)
            await self.lsp.notify("textDocument/didClose", {"textDocument": old_doc})
        doc = await self.lsp.open(path)
        self.documents[relative] = doc, source_hash
        return doc

    async def call_lsp(self, method, params):
        return await self.lsp.call(method, params, timeout=self.request_timeout)

    def dependency_hashes(self, paths):
        result = []
        for name in paths:
            path = Path(name)
            stat = path.stat()
            stamp = stat.st_ino, stat.st_size, stat.st_mtime_ns
            cached = self.hashes.get(name)
            if cached is None or cached[0] != stamp:
                cached = self.hashes[name] = stamp, digest_file(path)
            result.append(cached[1])
        return result

    def loaded_libraries(self, context):
        paths = set()
        for line in Path(f"/proc/{context['processId']}/maps").read_text().splitlines():
            parts = line.split(maxsplit=5)
            if "x" not in parts[1] or len(parts) != 6 or parts[5].startswith("["):
                continue
            path = Path(parts[5])
            if not path.is_file():
                raise ValueError("snapshot uses an anonymous or deleted executable mapping")
            paths.add(path)
        names = sorted(paths)
        # Lean's closure relocation keys include the loaded library's absolute path.
        # Exact bytes alone are insufficient; reject a layout mismatch before reading.
        return sorted([str(path), digest] for path, digest in
                      zip(names, self.dependency_hashes([str(path) for path in names])))

    def retain(self, handle, relative):
        key = uuid.uuid4().hex
        self.handles[key] = handle, relative
        return key

    def resolve(self, key, relative):
        value = self.handles.get(key)
        if value is None or value[1] != relative:
            raise ValueError("unknown handle or wrong document")
        return value[0]

    async def execute(self, op, p, body):
        if op == "status":
            return {"runtime": self.runtime, "handles": len(self.handles), "slots": 1}, b""
        relative = p["path"]
        doc = await self.document(relative)
        if op == "open":
            context = await self.call_lsp("$/lean/beam/snapshotContext", {"textDocument": doc})
            return {"identity": context["identity"],
                    "dependencies": self.dependency_hashes(context["regions"]),
                    "residentBytes": context["residentBytes"]}, b""
        retaining = op == "import" or (op in ("runAt", "runWith") and p.get("store", False))
        if retaining and len(self.handles) >= self.handle_limit:
            raise ValueError("worker handle budget exhausted")
        if op == "runAt" or op == "runWith":
            params = {"textDocument": doc, "text": p["text"], "storeHandle": p.get("store", False)}
            if op == "runAt":
                params["position"] = p["position"]
            else:
                params["handle"] = self.resolve(p["handle"], relative)
                # Distributed snapshots are reusable inputs. Linear consumption belongs
                # to the coordinator and is deliberately not offered by this prototype.
                params["linear"] = False
            start = time.monotonic()
            result = await self.call_lsp(f"$/lean/{op}", params)
            result["serviceSeconds"] = time.monotonic() - start
            if result.get("handle") is not None:
                result["handle"] = self.retain(result["handle"], relative)
            return result, b""
        if op == "release":
            handle = self.resolve(p["handle"], relative)
            await self.call_lsp("$/lean/releaseHandle", {"textDocument": doc, "handle": handle})
            del self.handles[p["handle"]]
            return {}, b""
        if op == "export":
            started = time.monotonic()
            handle = self.resolve(p["handle"], relative)
            path = Path(self.temp.name) / (uuid.uuid4().hex + ".snapshot")
            try:
                context = await self.call_lsp("$/lean/beam/exportSnapshot", {
                    "textDocument": doc, "handle": handle, "path": str(path),
                })
                if path.stat().st_size > MAX_BODY:
                    raise ValueError("snapshot exceeds transfer limit")
                data = path.read_bytes()
                manifest = {"identity": context["identity"],
                            "dependencies": self.dependency_hashes(context["regions"]),
                            "libraries": self.loaded_libraries(context),
                            "sha256": hashlib.sha256(data).hexdigest()}
                manifest["exportSeconds"] = time.monotonic() - started
                return manifest, data
            finally:
                path.unlink(missing_ok=True)
        if op == "import":
            manifest = p["manifest"]
            if hashlib.sha256(body).hexdigest() != manifest["sha256"]:
                raise ValueError("snapshot checksum mismatch")
            context = await self.call_lsp("$/lean/beam/snapshotContext", {"textDocument": doc})
            differences = []
            for key in set(manifest["identity"]) | set(context["identity"]):
                if manifest["identity"].get(key) != context["identity"].get(key):
                    differences.append(f"identity.{key}")
            if manifest["dependencies"] != self.dependency_hashes(context["regions"]):
                differences.append("dependencies")
            if manifest["libraries"] != self.loaded_libraries(context):
                differences.append("loadedLibraries")
            if differences:
                raise RpcError({"code": "incompatibleSnapshot", "message":
                                "snapshot runtime, document, or dependencies differ: " + ", ".join(sorted(differences))})
            # Lean's LSP starts a file process for each document. Account for all of
            # them, not just the process receiving this import.
            resident = {context["processId"]: context["residentBytes"]}
            for other, _ in self.documents.values():
                if other != doc:
                    usage = await self.call_lsp("$/lean/beam/snapshotContext", {"textDocument": other})
                    resident[usage["processId"]] = usage["residentBytes"]
            if sum(resident.values()) + len(body) > self.memory_limit:
                raise ValueError("worker snapshot memory budget exhausted")
            path = Path(self.temp.name) / (uuid.uuid4().hex + ".snapshot")
            try:
                path.write_bytes(body)
                handle = await self.call_lsp("$/lean/beam/importSnapshot", {
                    "textDocument": doc, "path": str(path), "identity": manifest["identity"],
                })
                return {"handle": self.retain(handle, relative)}, b""
            finally:
                path.unlink(missing_ok=True)
        raise ValueError(f"unknown worker operation: {op}")

    async def connected(self, reader, writer):
        task = asyncio.current_task()
        self.active.add(task)
        execution = disconnected = None
        try:
            async with asyncio.timeout(self.request_timeout + 30):
                header, body = await read_frame(reader)
                if not hmac.compare_digest(str(header.get("token", "")), self.token):
                    raise ValueError("invalid worker capability")
                if header.get("generation") not in (None, self.generation):
                    raise RpcError({"code": "generationChanged", "message": "worker generation changed"})

                async def run():
                    # Admission is bounded by the coordinator; this worker exposes one
                    # actual execution slot, not several aliases for one locked process.
                    async with self.lock:
                        return await self.execute(header["op"], header["payload"], body)

                execution = asyncio.create_task(run())
                disconnected = asyncio.create_task(reader.read(1))
                done, _ = await asyncio.wait((execution, disconnected), return_when=asyncio.FIRST_COMPLETED)
                if disconnected in done:
                    execution.cancel()
                    await asyncio.gather(execution, return_exceptions=True)
                    return
                result, data = await execution
                await write_frame(writer, {"generation": self.generation, "result": result}, data)
        except (Exception, asyncio.CancelledError) as error:
            if execution is not None and not execution.done():
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
            if not isinstance(error, asyncio.CancelledError):
                try:
                    detail = error.error if isinstance(error, RpcError) else {
                        "code": "workerRejected", "message": str(error) or type(error).__name__,
                    }
                    await write_frame(writer, {"generation": self.generation, "error": detail})
                except (ConnectionError, OSError):
                    pass
        finally:
            if disconnected is not None:
                disconnected.cancel()
                await asyncio.gather(disconnected, return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            self.active.discard(task)

    async def close(self):
        tasks = list(self.active)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.lsp is not None:
            await self.lsp.close()
        self.temp.cleanup()

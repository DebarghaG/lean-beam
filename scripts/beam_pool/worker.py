"""A replaceable warm worker for one immutable project and one exact Beam runtime."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import time
import uuid
from dataclasses import replace
from pathlib import Path

from .mcp import Mcp
from .protocol import (Failure, Snapshot, boolean, canonical, digest_file, fields, natural,
                       string)
from .transport import Emit


class Worker:
    def __init__(self, root: Path, mcp: Path, lean: Path, plugin: Path, *, slots: int = 1,
                 timeout: float = 60, max_handles: int = 4096, idle_ttl: float = 1800):
        self.snapshot = Snapshot.create(root)
        # Resolve elan through its supported Lean query, preserving the executable's argv[0].
        prefix = subprocess.check_output([str(lean), "--print-prefix"], cwd=root, text=True).strip()
        lean = Path(prefix) / "bin" / "lean"
        runtime_files = [mcp, mcp.parent / "beam-daemon", lean, plugin,
                         *sorted((lean.parent.parent / "lib/lean").glob("*.so"))]
        self.runtime = hashlib.sha256(canonical([digest_file(p) for p in runtime_files])).hexdigest()
        self.snapshot = replace(self.snapshot, identity=hashlib.sha256(
            canonical([self.snapshot.identity, self.runtime])).hexdigest())
        self.command = [str(mcp.resolve()), "--lean-cmd", str(lean.resolve()),
                        "--lean-plugin", str(plugin.resolve())]
        self.slots, self.timeout, self.max_handles = slots, timeout, max_handles
        self.generation = uuid.uuid4().hex
        self.mcp: Mcp | None = None
        self.versions: dict[str, int] = {}
        self.file_locks: dict[str, asyncio.Lock] = {}
        self.handles: dict[bytes, float] = {}
        self.busy = self.reserved_handles = 0
        self.completed = self.failures = self.restarts = 0
        self.lifecycle = asyncio.Lock()
        self.reset_task: asyncio.Task | None = None
        self.closing = False
        self.idle_ttl, self.last_activity = idle_ttl, time.monotonic()
        self.maintenance = None

    async def start(self) -> None:
        self.mcp = Mcp(self.command, str(self.snapshot.root), self.slots)
        await self.mcp.start()
        if self.maintenance is None:
            self.maintenance = asyncio.create_task(self.expire_idle())

    async def expire_idle(self) -> None:
        # Recover raw handles orphaned by a gateway crash. Public leases are shorter than this.
        while True:
            await asyncio.sleep(min(30, self.idle_ttl / 2))
            if self.handles and not self.busy and time.monotonic() - self.last_activity > self.idle_ttl:
                self.begin_recycle(self.mcp)

    async def close(self) -> None:
        self.closing = True
        if self.maintenance:
            self.maintenance.cancel()
            await asyncio.gather(self.maintenance, return_exceptions=True)
        if self.reset_task:
            await asyncio.shield(self.reset_task)
        if self.mcp:
            await self.mcp.close()

    async def recycle(self, old: Mcp) -> None:
        async with self.lifecycle:
            if self.mcp is not old:
                return
            self.generation = uuid.uuid4().hex
            await old.close()
            self.versions.clear()
            self.handles.clear()
            self.restarts += 1
            if not self.closing:
                await self.start()

    def begin_recycle(self, old: Mcp) -> None:
        if self.reset_task is None or self.reset_task.done():
            self.reset_task = asyncio.create_task(self.recycle(old))

    async def sync(self, path: str, emit: Emit) -> int:
        self.snapshot.check_file(path)
        async with self.file_locks.setdefault(path, asyncio.Lock()):
            if path not in self.versions:
                result = await self.mcp.call("lean_sync", {
                    "workspace": {"root": str(self.snapshot.root)}, "path": path}, emit)
                self.versions[path] = natural(result, "version")
            return self.versions[path]

    async def dispatch(self, op: str, args: dict, emit: Emit) -> dict:
        if op == "describe":
            fields(args, {"snapshot", "offset"})
            if string(args, "snapshot") != self.snapshot.identity:
                raise Failure("contentModified", "worker snapshot changed")
            offset = natural(args, "offset")
            items = sorted(self.snapshot.files.items())
            end = min(offset + 512, len(items))
            return {"snapshot": self.snapshot.identity, "runtime": self.runtime,
                    "files": dict(items[offset:end]), "next": end if end < len(items) else None}
        if op == "info":
            fields(args, set())
            return {"snapshot": self.snapshot.identity, "runtime": self.runtime,
                    "generation": self.generation, "slots": self.slots, "busy": self.busy,
                    "handles": len(self.handles), "ready": not self.closing and
                    (self.reset_task is None or self.reset_task.done()) and
                    self.mcp is not None and self.mcp.proc.returncode is None,
                    "completed": self.completed, "failures": self.failures, "restarts": self.restarts}
        common = {"snapshot", "generation", "path"}
        if op == "runAt":
            fields(args, common | {"line", "character", "text", "store"}, {"source"})
            natural(args, "line"), natural(args, "character")
            string(args, "text")
            storing = boolean(args, "store")
        elif op == "runWith":
            fields(args, common | {"handle", "text", "linear"}, {"source"})
            string(args, "text")
            boolean(args, "linear")
            storing = True
        elif op == "release":
            fields(args, common | {"handle"})
            storing = False
        elif op == "warm":
            fields(args, common)
            storing = False
        else:
            raise Failure("invalidParams", "unknown worker operation")
        if string(args, "snapshot") != self.snapshot.identity:
            raise Failure("contentModified", "worker has a different project snapshot")
        if string(args, "generation") != self.generation:
            raise Failure("contentModified", "worker generation changed")
        if self.closing or (self.reset_task and not self.reset_task.done()):
            raise Failure("workerLost", "worker is draining")
        if self.busy >= self.slots:
            raise Failure("overloaded", "worker has no execution slot")
        if storing and len(self.handles) + self.reserved_handles - int(
                op == "runWith" and args["linear"]) >= self.max_handles:
            raise Failure("resourceExhausted", "worker handle budget reached; release branches")
        path = string(args, "path", 4096)
        if "source" in args:
            source = string(args, "source", 3 * 1024 * 1024)
            selected = self.snapshot.check_file(path)
            if selected.read_bytes().decode("utf-8") != source:
                raise Failure("contentModified", "client source differs from the prepared pool snapshot")
        key = canonical(args["handle"]) if "handle" in args else None
        if key is not None and key not in self.handles:
            raise Failure("contentModified", "unknown or released worker handle")
        self.busy += 1
        self.reserved_handles += int(storing)
        old = self.mcp
        generation = self.generation
        try:
            async with asyncio.timeout(self.timeout):
                version = await self.sync(path, emit)
                params = {"workspace": {"root": str(self.snapshot.root)}, "path": path}
                if op == "warm":
                    result = {"version": version}
                else:
                    if op == "runAt":
                        name = "lean_run_at_handle" if storing else "lean_run_at"
                        params.update(version=version, line=args["line"], character=args["character"],
                                      text=args["text"])
                    elif op == "runWith":
                        name = "lean_run_with_linear" if args["linear"] else "lean_run_with"
                        params.update(handle=args["handle"], text=args["text"])
                    else:
                        name = "lean_release"
                        params.update(handle=args["handle"])
                    result = await old.call(name, params, emit)
                self.snapshot.check_file(path)
                if generation != self.generation:
                    raise Failure("contentModified", "worker was restarted during execution")
                if op == "release" or (op == "runWith" and args["linear"]):
                    self.handles.pop(key, None)
                if result.get("next_handle") is not None:
                    self.handles[canonical(result["next_handle"])] = time.monotonic()
                self.completed += 1
                return {"generation": generation, "result": result}
        except (TimeoutError, asyncio.CancelledError) as error:
            self.begin_recycle(old)
            self.failures += 1
            if isinstance(error, TimeoutError):
                raise Failure("deadlineExceeded", "worker execution deadline exceeded") from error
            raise
        except Failure as error:
            self.failures += 1
            if error.code in {"workerLost", "protocolError"}:
                self.begin_recycle(old)
            raise
        finally:
            self.last_activity = time.monotonic()
            self.busy -= 1
            self.reserved_handles -= int(storing)

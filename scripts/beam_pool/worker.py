"""A bounded set of private Beam workspaces over one prepared project/runtime."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import tempfile
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path

from .mcp import Mcp
from .protocol import (Failure, Snapshot, boolean, canonical, digest_file, fields, natural,
                       string)
from .transport import Emit
from .revisions import Revision, Revisions
from .workspace import Workspace


@dataclass(eq=False)
class Context:
    view: Workspace
    group: str
    environment: str
    revision: str
    sequence: int = -1
    versions: dict[str, tuple[str, int]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0
    touched: float = field(default_factory=time.monotonic)
    retired: bool = False


@dataclass
class Retained:
    context: Context
    path: str
    source: str


async def file_io(function, *args):
    # Cancellation cannot detach a filesystem writer from its owning context.
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.shield(task)
        raise


class Worker:
    def __init__(self, root: Path, mcp: Path, lean: Path, plugin: Path, *, slots: int = 1,
                 timeout: float = 60, max_handles: int = 4096, idle_ttl: float = 1800,
                 cancel_grace: float = 3, max_contexts: int = 4,
                 max_project_bytes: int = 1024 * 1024 * 1024):
        self.paths = root, mcp, lean, plugin
        self.prepared = False
        self.cancel_grace = cancel_grace
        self.slots, self.timeout, self.max_handles = slots, timeout, max_handles
        self.generation = uuid.uuid4().hex
        self.mcp: Mcp | None = None
        self.max_contexts, self.max_project_bytes = max_contexts, max_project_bytes
        self.contexts: dict[tuple[str, str], Context] = {}
        self.context_lock = asyncio.Lock()
        self.active_revisions: Counter[str] = Counter()
        self.handles: dict[bytes, Retained] = {}
        self.projects: Revisions | None = None
        self.workspaces = None
        self.busy = self.reserved_handles = 0
        self.completed = self.failures = self.restarts = 0
        self.lifecycle = asyncio.Lock()
        self.reset_task: asyncio.Task | None = None
        self.closing = False
        self.idle_ttl = idle_ttl
        self.maintenance = None

    def prepare(self) -> None:
        root, mcp, lean, plugin = self.paths
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
                        "--lean-plugin", str(plugin.resolve()), "--respond-to-cancellation"]
        self.prepared = True

    async def start(self) -> None:
        if not self.prepared:
            await asyncio.to_thread(self.prepare)
        if self.projects is None:
            self.projects = Revisions(max_bytes=self.max_project_bytes, ttl=self.idle_ttl)
            self.workspaces = tempfile.TemporaryDirectory(prefix="beam-pool-workspaces-")
        self.mcp = Mcp(self.command, str(self.snapshot.root), self.slots, drain_on_cancel=True)
        await self.mcp.start()
        if self.maintenance is None:
            self.maintenance = asyncio.create_task(self.expire_idle())

    async def expire_idle(self) -> None:
        # Recover raw handles orphaned by a gateway crash. Public leases are shorter than this.
        while True:
            await asyncio.sleep(min(30, self.idle_ttl / 2))
            async with self.context_lock:
                for context in list(self.contexts.values()):
                    if not context.users and time.monotonic() - context.touched > self.idle_ttl:
                        await self.drop_context(context)
                self.projects.collect(self.pinned_revisions())

    def pinned_revisions(self) -> set[str]:
        return set(self.active_revisions) | {c.revision for c in self.contexts.values()}

    async def close(self) -> None:
        self.closing = True
        if self.maintenance:
            self.maintenance.cancel()
            await asyncio.gather(self.maintenance, return_exceptions=True)
        if self.reset_task:
            await asyncio.shield(self.reset_task)
        if self.mcp:
            await self.mcp.close()
        if self.workspaces:
            await file_io(self.workspaces.cleanup)
        if self.projects:
            self.projects.close()

    async def recycle(self, old: Mcp) -> None:
        async with self.lifecycle:
            if self.mcp is not old:
                return
            self.generation = uuid.uuid4().hex
            await old.close()
            for context in self.contexts.values():
                context.retired = True
                if not context.users and context.view.root.exists():
                    await file_io(context.view.close)
            self.contexts.clear()
            self.handles.clear()
            self.restarts += 1
            if not self.closing:
                await self.start()

    def begin_recycle(self, old: Mcp) -> None:
        if self.reset_task is None or self.reset_task.done():
            self.reset_task = asyncio.create_task(self.recycle(old))

    async def drop_context(self, context: Context) -> None:
        context.retired = True
        self.contexts.pop((context.group, context.environment), None)
        self.handles = {key: h for key, h in self.handles.items() if h.context is not context}
        try:
            async with asyncio.timeout(10):
                await self.mcp.call("lean_drop_workspace", {"workspace": {"root": str(context.view.root)}})
        except (TimeoutError, Failure):
            self.begin_recycle(self.mcp)
        finally:
            if not context.users and context.view.root.exists():
                await file_io(context.view.close)

    async def context(self, revision: Revision) -> Context:
        async with self.context_lock:
            key = revision.group, revision.environment
            context = self.contexts.get(key)
            if context is None:
                # An operator-warmed, unowned workspace can be adopted by the first client.
                context = self.contexts.pop(("warm", revision.environment), None)
                if context is not None:
                    context.group = revision.group
                    self.contexts[key] = context
            if context is None:
                if len(self.contexts) >= self.max_contexts:
                    pinned = {id(h.context) for h in self.handles.values()}
                    idle = [c for c in self.contexts.values() if not c.users and id(c) not in pinned]
                    if not idle:
                        raise Failure("resourceExhausted", "worker workspace budget reached; release branches")
                    await self.drop_context(min(idle, key=lambda c: c.touched))
                root = Path(self.workspaces.name) / uuid.uuid4().hex
                view = await file_io(Workspace, root, self.snapshot.root, self.projects)
                context = Context(view, revision.group, revision.environment, revision.identity)
                self.contexts[key] = context
            context.users += 1
            context.touched = time.monotonic()
            return context

    async def drain(self, task: asyncio.Task, old: Mcp, selected: list[Context], path: str) -> None:
        task.cancel()
        try:
            async with asyncio.timeout(self.cancel_grace):
                try:
                    reply = await asyncio.shield(task)
                except asyncio.CancelledError:
                    if not task.cancelled():
                        raise
                    return  # No MCP call remains outstanding.
                except Failure as error:
                    if error.code in {"workerLost", "protocolError"}:
                        raise
                    return  # A terminal request error also proves completion.
                raw = reply["result"].get("next_handle")
                if raw is not None:
                    release = asyncio.create_task(old.call("lean_release", {
                        "workspace": {"root": str(selected[0].view.root)}, "path": path, "handle": raw}))
                    release.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                    await asyncio.shield(release)
                    self.handles.pop(canonical(raw), None)
        except (asyncio.CancelledError, Exception):
            # Any failed drain leaves execution or handle ownership uncertain.
            if selected and self.mcp is old:
                await self.drop_context(selected[0])
            else:
                self.begin_recycle(old)

    async def sync(self, context: Context, path: str, source: str, emit: Emit) -> int:
        previous = context.versions.get(path)
        if previous is None or previous[0] != source:
            result = await self.mcp.call("lean_sync", {
                "workspace": {"root": str(context.view.root)}, "path": path}, emit)
            context.versions[path] = source, natural(result, "version")
            if previous is not None:
                self.handles = {k: h for k, h in self.handles.items()
                                if h.context is not context or h.path != path}
        return context.versions[path][1]

    async def dispatch(self, op: str, args: dict, emit: Emit) -> dict:
        if op in {"put", "publish"}:
            args = dict(args)
            generation = args.pop("generation", None)
            if generation != self.generation:
                raise Failure("contentModified", "worker generation changed")
            if op == "publish" and args.get("snapshot") != self.snapshot.identity:
                raise Failure("contentModified", "worker has a different prepared project")
            pinned = self.pinned_revisions()
            self.projects.collect(pinned)
            return self.projects.put(args, pinned) if op == "put" else self.projects.publish(args, pinned)
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
            fields(args, common | {"line", "character", "text", "store", "source", "revision", "group"})
            natural(args, "line"), natural(args, "character")
            string(args, "text")
            storing = boolean(args, "store")
        elif op == "runWith":
            fields(args, common | {"handle", "text", "linear", "source", "group"})
            string(args, "text")
            boolean(args, "linear")
            storing = True
        elif op == "release":
            fields(args, common | {"handle", "group"})
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
        source = string(args, "source", 3 * 1024 * 1024) if "source" in args else None
        revision = None
        if op == "warm":
            source = self.snapshot.check_file(path).read_bytes().decode("utf-8")
            published = self.projects.publish({"snapshot": self.snapshot.identity, "group": "warm",
                                               "sequence": 0, "files": {}})
            revision = self.projects.get(published["revision"], "warm")
        else:
            group = string(args, "group", 128)
            if op == "runAt":
                revision = self.projects.get(string(args, "revision", 128), group)
                if revision.snapshot != self.snapshot.identity:
                    raise Failure("contentModified", "revision belongs to another prepared project")
        key = canonical(args["handle"]) if "handle" in args else None
        if key is not None and key not in self.handles:
            raise Failure("contentModified", "unknown or released worker handle")
        retained = self.handles.get(key)
        if retained and (retained.context.group != group or retained.path != path or
                         (source is not None and retained.source != source)):
            raise Failure("contentModified", "handle belongs to a different workspace or source revision")
        self.busy += 1
        self.reserved_handles += int(storing)
        old = self.mcp
        generation = self.generation
        selected: list[Context] = []
        if revision:
            self.active_revisions[revision.identity] += 1

        async def execute() -> dict:
            if retained:
                context = retained.context
                context.users += 1
            else:
                context = await self.context(revision)
            selected.append(context)
            try:
                async with context.lock:
                    return await execute_in_context(context)
            finally:
                context.users -= 1
                context.touched = time.monotonic()
                if context.retired and not context.users and context.view.root.exists():
                    await file_io(context.view.close)

        async def execute_in_context(context: Context) -> dict:
            if revision and revision.sequence > context.sequence:
                await file_io(context.view.apply, revision.files)
                context.sequence, context.revision = revision.sequence, revision.identity
            if context.retired or generation != self.generation:
                raise Failure("contentModified", "workspace was retired")
            if source is not None:
                await file_io(context.view.source, path, source)
            version = await self.sync(context, path, source, emit) if not retained else None
            if asyncio.current_task().cancelling():
                raise asyncio.CancelledError  # A cancelled sync must not start a tactic afterward.
            params = {"workspace": {"root": str(context.view.root)}, "path": path}
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
            if source is not None:
                await file_io(context.view.source, path, source)
            if generation != self.generation:
                raise Failure("contentModified", "worker was restarted during execution")
            if op == "release" or (op == "runWith" and args["linear"]):
                self.handles.pop(key, None)
            if result.get("next_handle") is not None:
                self.handles[canonical(result["next_handle"])] = Retained(context, path, source)
            self.completed += 1
            return {"generation": generation, "result": result}

        def finished(task: asyncio.Task) -> None:
            if revision:
                self.active_revisions[revision.identity] -= 1
                if not self.active_revisions[revision.identity]:
                    del self.active_revisions[revision.identity]
            if not task.cancelled():
                task.exception()

        operation = asyncio.create_task(execute())
        operation.add_done_callback(finished)
        try:
            async with asyncio.timeout(self.timeout):
                return await asyncio.shield(operation)
        except (TimeoutError, asyncio.CancelledError) as error:
            await self.drain(operation, old, selected, path)
            self.failures += 1
            if isinstance(error, TimeoutError):
                raise Failure("deadlineExceeded", "worker execution deadline exceeded") from error
            raise
        except Failure as error:
            self.failures += 1
            if error.code in {"workerLost", "protocolError"}:
                self.begin_recycle(old)
            raise
        except Exception as error:
            if selected:
                await self.drop_context(selected[0])
            raise Failure("internalError", "worker workspace failed") from error
        finally:
            self.busy -= 1
            self.reserved_handles -= int(storing)

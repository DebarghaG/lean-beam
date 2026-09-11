"""Bounded fair scheduling and worker ownership for Beam continuation handles."""

from __future__ import annotations

import asyncio
import socket
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field

from .protocol import (Failure, boolean, fields, natural, relative_path, string)
from .transport import Emit, rpc
from .revisions import CHUNK_SIZE, Revisions


@dataclass
class Peer:
    endpoint: tuple[str, int]
    info: dict = field(default_factory=dict)
    busy: int = 0


@dataclass
class Handle:
    peer: Peer
    generation: str
    raw: dict
    snapshot: str
    path: str
    group: str
    revision: str
    touched: float = field(default_factory=time.monotonic)
    users: int = 0
    consuming: bool = False


@dataclass(eq=False)
class Job:
    op: str
    args: dict
    group: str
    result: asyncio.Future
    emit: Emit
    handle: Handle | None = None
    consumes: bool = False
    reservation: int = 0
    created: float = field(default_factory=time.monotonic)
    task: asyncio.Task | None = None


class Pool:
    def __init__(self, endpoints: list[tuple[str, int]], token: str, *, dns: tuple[str, int] | None = None,
                 max_queue: int = 256, per_group: int = 64, max_handles: int = 4096,
                 timeout: float = 120, handle_ttl: float = 900,
                 max_project_bytes: int = 1024 * 1024 * 1024):
        self.endpoints, self.token, self.dns = endpoints, token, dns
        self.max_queue, self.per_group, self.max_handles = max_queue, per_group, max_handles
        self.timeout, self.handle_ttl = timeout, handle_ttl
        self.peers: dict[tuple[str, int], Peer] = {}
        self.handles: dict[str, Handle] = {}
        self.jobs: set[Job] = set()
        self.queues: OrderedDict[str, deque[Job]] = OrderedDict()
        self.groups: dict[str, int] = {}
        self.queued = self.running = self.reserved_handles = 0
        self.completed = self.failed = 0
        self.wakeup = asyncio.Event()
        self.background: list[asyncio.Task] = []
        self.closing = False
        self.cursor = 0
        self.expiry_lock = asyncio.Lock()
        self.projects = Revisions(max_bytes=max_project_bytes)

    def pinned_revisions(self) -> set[str]:
        return ({h.revision for h in self.handles.values()} |
                {j.args["revision"] for j in self.jobs if "revision" in j.args})

    async def start(self) -> None:
        await self.discover()
        self.background = [asyncio.create_task(self.discovery_loop()), asyncio.create_task(self.schedule()),
                           asyncio.create_task(self.expiry_loop())]

    async def close(self) -> None:
        self.closing = True
        for task in self.background:
            task.cancel()
        jobs = list(self.jobs)
        for job in jobs:
            if job.task:
                job.task.cancel()
            if not job.result.done():
                job.result.set_exception(Failure("workerLost", "pool is shutting down"))
        await asyncio.gather(*self.background, *(j.task for j in jobs if j.task),
                             return_exceptions=True)
        for job in list(self.jobs):
            self.retire(job)
        self.queues.clear()
        self.queued = 0
        self.projects.close()

    async def discover(self) -> None:
        endpoints = set(self.endpoints)
        if self.dns:
            try:
                records = await asyncio.get_running_loop().getaddrinfo(
                    *self.dns, family=socket.AF_INET, type=socket.SOCK_STREAM)
                endpoints.update((r[4][0], self.dns[1]) for r in records)
            except OSError:
                pass
        owners = {id(handle.peer) for handle in self.handles.values()}
        for endpoint, peer in list(self.peers.items()):
            if endpoint not in endpoints:
                peer.info["ready"] = False
                if not peer.busy and id(peer) not in owners:
                    del self.peers[endpoint]

        async def update(endpoint: tuple[str, int]) -> None:
            peer = self.peers.setdefault(endpoint, Peer(endpoint))
            try:
                info = await rpc(endpoint, self.token, "info", {}, timeout=3)
                fields(info, {"snapshot", "runtime", "generation", "slots", "busy", "handles",
                              "ready", "completed", "failures", "restarts"})
                if natural(info, "slots") == 0:
                    raise Failure("protocolError", "worker has no slots")
                for key in ("snapshot", "runtime", "generation"):
                    string(info, key, 128)
                boolean(info, "ready")
                peer.info = info
            except Failure:
                peer.info["ready"] = False
        await asyncio.gather(*(update(e) for e in endpoints))
        self.wakeup.set()

    async def discovery_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            await self.discover()

    async def expiry_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            await self.expire_handles()
            self.projects.collect(self.pinned_revisions())

    async def expire_handles(self) -> None:
        async def expire_on_peer(entries: list[tuple[str, Handle]]) -> None:
            for identity, handle in entries:
                if self.handles.get(identity) is not handle or handle.users:
                    continue
                peer = handle.peer
                if peer.info.get("generation") != handle.generation:
                    self.handles.pop(identity, None)
                    continue
                if time.monotonic() - handle.touched <= self.handle_ttl:
                    continue
                if not peer.info.get("ready"):
                    self.handles.pop(identity, None)
                    continue
                if peer.busy >= peer.info.get("slots", 1):
                    break
                peer.busy += 1
                handle.users += 1
                try:
                    await rpc(peer.endpoint, self.token, "release", {
                        "snapshot": handle.snapshot, "generation": handle.generation,
                        "path": handle.path, "handle": handle.raw, "group": handle.group}, timeout=5)
                except Failure as error:
                    if error.code == "overloaded":
                        break
                    if error.code in {"workerLost", "deadlineExceeded"}:
                        peer.info["ready"] = False
                finally:
                    handle.users -= 1
                    peer.busy -= 1
                    self.wakeup.set()
                self.handles.pop(identity, None)

        async with self.expiry_lock:
            by_peer: dict[int, list[tuple[str, Handle]]] = {}
            for identity, handle in self.handles.items():
                by_peer.setdefault(id(handle.peer), []).append((identity, handle))
            await asyncio.gather(*(expire_on_peer(entries) for entries in by_peer.values()))

    def get_handle(self, value: str) -> Handle:
        handle = self.handles.get(value)
        if handle is None or handle.peer.info.get("generation") != handle.generation:
            raise Failure("contentModified", "handle is unknown, released, expired, or belongs to a lost worker")
        if time.monotonic() - handle.touched > self.handle_ttl:
            raise Failure("contentModified", "handle lease expired")
        handle.touched = time.monotonic()
        return handle

    async def dispatch(self, op: str, args: dict, emit: Emit) -> dict:
        if op in {"put", "publish"}:
            self.projects.collect(self.pinned_revisions())
            return (self.projects.put(args, self.pinned_revisions()) if op == "put" else
                    self.projects.publish(args, self.pinned_revisions()))
        if op == "info":
            fields(args, set())
            return {"workers": [{"endpoint": list(p.endpoint), **p.info, "assigned": p.busy}
                                for p in self.peers.values()], "queued": self.queued,
                    "running": self.running, "handles": len(self.handles),
                    "completed": self.completed, "failed": self.failed}
        if op == "describe":
            fields(args, {"snapshot", "offset"})
            snapshot = string(args, "snapshot")
            natural(args, "offset")
            peer = next((p for p in self.peers.values() if p.info.get("ready") and
                         p.info.get("snapshot") == snapshot), None)
            if peer is None:
                raise Failure("contentModified", "no worker serves this snapshot")
            return await rpc(peer.endpoint, self.token, "describe", args)
        handle = None
        if op == "runAt":
            fields(args, {"snapshot", "path", "line", "character", "text", "store", "group", "source", "revision"})
            string(args, "snapshot", 128)
            relative_path(string(args, "path", 4096))
            natural(args, "line")
            natural(args, "character")
            string(args, "text")
            boolean(args, "store")
        elif op in {"runWith", "release"}:
            required = {"handle", "text", "linear", "source", "group"} if op == "runWith" else {"handle", "group"}
            fields(args, required)
            handle = self.get_handle(string(args, "handle", 128))
            if op == "runWith":
                string(args, "text")
                boolean(args, "linear")
            if handle.consuming or ((op == "release" or args.get("linear", False)) and handle.users):
                raise Failure("handleBusy", "consuming or releasing a handle requires its readers to finish")
        else:
            raise Failure("invalidParams", "unknown pool operation")
        group = string(args, "group", 128)
        if handle and handle.group != group:
            raise Failure("contentModified", "handle belongs to another workspace")
        if op == "runAt":
            revision = self.projects.get(string(args, "revision", 128), group)
            if revision.snapshot != args["snapshot"]:
                raise Failure("contentModified", "revision belongs to another prepared project")
        if "source" in args:
            string(args, "source", 3 * 1024 * 1024)
        if self.closing:
            raise Failure("workerLost", "pool is draining")
        if self.queued >= self.max_queue or self.groups.get(group, 0) >= self.per_group:
            raise Failure("overloaded", "pool queue or agent-group budget reached")
        storing = op == "runWith" or args.get("store", False)
        consumes = op == "release" or (op == "runWith" and args["linear"])
        reservation = int(storing and not consumes)
        if storing and len(self.handles) + self.reserved_handles + reservation > self.max_handles:
            raise Failure("resourceExhausted", "pool handle budget reached; release branches")
        if handle:
            handle.users += 1
            handle.consuming = consumes
        job = Job(op, args, group, asyncio.get_running_loop().create_future(), emit,
                  handle, consumes, reservation)
        # Observe failures after the requesting connection has gone away.
        job.result.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        self.jobs.add(job)
        self.queues.setdefault(group, deque()).append(job)
        self.groups[group] = self.groups.get(group, 0) + 1
        self.queued += 1
        self.reserved_handles += reservation
        self.wakeup.set()
        return await self.wait_job(job)

    async def wait_job(self, job: Job) -> dict:
        try:
            remaining = self.timeout - (time.monotonic() - job.created)
            if job.result.done():
                return job.result.result()
            async with asyncio.timeout(max(0, remaining)):
                return await asyncio.shield(job.result)
        except TimeoutError as error:
            raise Failure("deadlineExceeded", "pool deadline includes time in queue") from error
        finally:
            if not job.result.done():
                if job.task:
                    job.task.cancel()
                else:
                    job.result.set_exception(Failure("requestCancelled", "request ended in queue"))
                self.wakeup.set()

    def select_peer(self, job: Job) -> Peer | None:
        if job.op in {"runWith", "release"}:
            peer = job.handle.peer
            if peer.info.get("generation") != job.handle.generation or not peer.info.get("ready"):
                raise Failure("contentModified", "owning worker is unavailable")
            return peer if peer.busy < peer.info["slots"] else None
        snapshot = job.handle.snapshot if job.handle else job.args["snapshot"]
        matching = [p for p in self.peers.values() if p.info.get("snapshot") == snapshot]
        if not matching and any(p.info.get("ready") for p in self.peers.values()):
            raise Failure("contentModified", "no worker serves this project/runtime snapshot")
        peers = [p for p in matching if p.info.get("ready")]
        runtimes = {p.info["runtime"] for p in peers}
        if len(runtimes) > 1:
            raise Failure("contentModified", "snapshot has incompatible worker runtimes")
        peers = [p for p in peers if p.busy < p.info["slots"]]
        if not peers:
            return None
        least = min(p.busy / p.info["slots"] for p in peers)
        candidates = [p for p in peers if p.busy / p.info["slots"] == least]
        self.cursor += 1
        return candidates[self.cursor % len(candidates)]

    async def schedule(self) -> None:
        while True:
            await self.wakeup.wait()
            self.wakeup.clear()
            made_progress = True
            while made_progress:
                made_progress = False
                for group in list(self.queues):
                    queue = self.queues[group]
                    peer = None
                    # A pinned branch must not block unrelated work in the same caller group.
                    for job in queue:
                        try:
                            if not job.result.done():
                                peer = self.select_peer(job)
                        except Failure as error:
                            job.result.set_exception(error)
                        if job.result.done() or peer:
                            break
                    if job.result.done() or peer:
                        queue.remove(job)
                        if not queue:
                            del self.queues[group]
                        else:
                            self.queues.move_to_end(group)
                        self.queued -= 1
                        made_progress = True
                        if peer:
                            peer.busy += 1
                            self.running += 1
                            job.task = asyncio.create_task(self.execute(job, peer))
                            job.task.add_done_callback(
                                lambda task, job=job, peer=peer: self.finish_execution(job, peer, task))
                        else:
                            self.retire(job)

    def finish_execution(self, job: Job, peer: Peer, task: asyncio.Task) -> None:
        # A task cancelled before its first step still owns an admission slot.
        if task.cancelled() and not job.result.done():
            job.result.set_exception(Failure("requestCancelled", "request cancelled"))
        peer.busy -= 1
        self.running -= 1
        self.retire(job)
        self.wakeup.set()

    def retire(self, job: Job) -> None:
        self.jobs.discard(job)
        self.groups[job.group] -= 1
        if not self.groups[job.group]:
            del self.groups[job.group]
        self.reserved_handles -= job.reservation
        if job.handle:
            job.handle.users -= 1
            if job.consumes:
                job.handle.consuming = False

    async def execute(self, job: Job, peer: Peer) -> None:
        generation = peer.info["generation"]
        snapshot = job.handle.snapshot if job.handle else job.args["snapshot"]
        path = job.handle.path if job.handle else job.args["path"]
        common = {"snapshot": snapshot, "generation": generation, "path": path, "group": job.group}

        async def call(op: str, args: dict) -> dict:
            if op in {"runAt", "runWith"} and "source" in job.args:
                args = args | {"source": job.args["source"]}
            reply = await rpc(peer.endpoint, self.token, op, common | args, job.emit,
                              timeout=max(0.001, self.timeout - (time.monotonic() - job.created)))
            fields(reply, {"generation", "result"})
            if reply["generation"] != generation or peer.info.get("generation") != generation:
                raise Failure("contentModified", "worker changed during request")
            return reply["result"]

        try:
            if job.op == "runAt":
                revision = self.projects.get(job.args["revision"], job.group)
                wire = revision.wire() | {"generation": generation}
                prepared = await rpc(peer.endpoint, self.token, "publish", wire, timeout=self.timeout)
                fields(prepared, {"revision", "missing"})
                for digest in prepared["missing"]:
                    # Every worker asks for only the content absent from its own cache.
                    if digest not in revision.files.values():
                        raise Failure("protocolError", "worker requested an unrelated project file")
                    upload = uuid.uuid4().hex
                    with self.projects.blob(digest).open("rb") as stream:
                        offset = 0
                        size = self.projects.blobs[digest][0]
                        while True:
                            chunk = stream.read(CHUNK_SIZE)
                            last = offset + len(chunk) == size
                            reply = await rpc(peer.endpoint, self.token, "put", {
                                "generation": generation, "group": job.group, "upload": upload,
                                "offset": offset, "data": chunk.hex(), "last": last}, timeout=self.timeout)
                            offset += len(chunk)
                            if last:
                                if reply.get("digest") != digest:
                                    raise Failure("protocolError", "worker received different project bytes")
                                break
                if prepared["missing"]:
                    prepared = await rpc(peer.endpoint, self.token, "publish", wire, timeout=self.timeout)
                if prepared.get("revision") != revision.identity:
                    raise Failure("protocolError", "worker did not accept the project revision")
                result = await call("runAt", {k: job.args[k] for k in ("line", "character", "text")} |
                                    {"store": job.args.get("store", False), "revision": revision.identity})
            elif job.op == "runWith":
                result = await call("runWith", {"handle": job.handle.raw, "text": job.args["text"],
                                                "linear": job.args.get("linear", False)})
            elif job.op == "release":
                result = await call("release", {"handle": job.handle.raw})
            result = dict(result)
            if job.consumes:
                self.handles.pop(job.args["handle"], None)
            result.pop("workspace", None)
            raw = result.get("next_handle")
            if raw is not None:
                identity = uuid.uuid4().hex
                revision_id = job.handle.revision if job.handle else job.args["revision"]
                self.handles[identity] = Handle(peer, generation, raw, snapshot, path, job.group, revision_id)
                result["next_handle"] = identity
            if not job.result.done():
                job.result.set_result({"result": result})
            self.completed += 1
        except asyncio.CancelledError:
            if not job.result.done():
                job.result.set_exception(Failure("requestCancelled", "request cancelled"))
            self.failed += 1
            raise
        except Failure as error:
            # RPC deadlines also wait for drain; only an uncertain transport retires the peer.
            if error.code == "workerLost":
                peer.info["ready"] = False
            if not job.result.done():
                job.result.set_exception(error)
            self.failed += 1
        except Exception as error:
            if not job.result.done():
                job.result.set_exception(Failure("internalError", str(error)))
            self.failed += 1

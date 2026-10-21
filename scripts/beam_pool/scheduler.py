"""Locality-first randomized batch stealing over Beam proof snapshots.

For donor work W, transfer delay C, and source serialization stall S, moving x
units predicts max(W + S - x, C + x). Choose a whole-job split minimizing that
quantity, and require a margin over leaving the queue alone. These are estimates,
not a general speedup guarantee for arbitrary Lean tactics.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from dataclasses import dataclass, field
import math
import random
import time
import uuid


def steal_count(costs, donor_work, transfer, source_stall=0.0, margin=0.0):
    """Number of jobs to take from the tail, or zero if moving is not worthwhile."""
    values = [donor_work, transfer, source_stall, margin, *costs]
    if any(not math.isfinite(x) or x < 0 for x in values):
        raise ValueError("work and transfer estimates must be finite and nonnegative")
    best, count, moved = donor_work - margin, 0, 0.0
    for index, cost in enumerate(reversed(costs), 1):
        moved += cost
        finish = max(donor_work + source_stall - moved, transfer + moved)
        if finish < best:
            best, count = finish, index
    return count


@dataclass(eq=False)
class Snapshot:
    path: str
    replicas: dict
    key: str = field(default_factory=lambda: uuid.uuid4().hex)
    users: int = 0
    closing: bool = False
    idle: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self):
        self.idle.set()

    def acquire(self):
        if self.closing:
            raise ValueError("snapshot is closing")
        self.users += 1
        self.idle.clear()

    def drop(self):
        self.users -= 1
        if not self.users:
            self.idle.set()


@dataclass(eq=False)
class Job:
    path: str
    text: str
    store: bool
    cost: float
    future: asyncio.Future
    snapshot: Snapshot | None = None
    position: dict | None = None
    started: float = 0.0
    queued_at: int | None = None
    queued_weight: float = 0.0


class Pool:
    """A single coordinator for reusable proof handles on immutable project workers.

    Worker restarts are fenced. Executing requests are never retried automatically;
    a failed transfer may safely return its still-unexecuted batch to the donor.
    """

    def __init__(self, peers, *, stealing=True, seed=0, queue_limit=4096,
                 cache_bytes=256 * 1024 * 1024, cold_transfer_seconds=1.0,
                 source_stall_seconds=0.25, margin_seconds=0.025):
        if not peers:
            raise ValueError("at least one worker is required")
        self.peers = peers
        self.stealing, self.random = stealing, random.Random(seed)
        self.queues = [deque() for _ in peers]
        self.queued_work = [0.0 for _ in peers]
        self.active = [None for _ in peers]
        self.healthy = [True for _ in peers]
        self.wakes = [asyncio.Event() for _ in peers]
        self.tasks = []
        self.snapshots = set()
        self.cache = OrderedDict()
        self.cache_size, self.cache_limit = 0, cache_bytes
        self.export_lock = asyncio.Lock()
        self.queue_limit, self.outstanding = queue_limit, 0
        self.copy_seconds = cold_transfer_seconds
        self.source_stall = source_stall_seconds
        self.margin = margin_seconds
        self.service_seconds = 0.05
        self.closed = False
        self.failed_copies = set()
        self.releases = set()
        self.metrics = {"exports": 0, "imports": 0, "steals": 0, "stolenJobs": 0,
                        "transferBytes": 0, "transferSeconds": 0.0, "copyFailures": 0,
                        "completedByWorker": [0 for _ in peers]}

    async def start(self):
        statuses = await asyncio.gather(*(peer.call("status") for peer in self.peers))
        if len({status[0]["runtime"] for status in statuses}) != 1:
            raise ValueError("workers have different Lean/Beam runtimes")
        self.tasks = [asyncio.create_task(self._worker(index)) for index in range(len(self.peers))]
        return self

    def _wake(self):
        for wake in self.wakes:
            wake.set()

    def _load(self, index):
        job = self.active[index]
        remaining = max(0.0, job.cost - (time.monotonic() - job.started)) if job else 0.0
        return remaining + self.queued_work[index]

    def _put(self, index, jobs):
        for job in jobs:
            self.queues[index].append(job)
            job.queued_at = index
            job.queued_weight = 0.0 if job.future.cancelled() else job.cost
            self.queued_work[index] += job.queued_weight

    def _unqueue(self, job):
        if job.queued_at is not None:
            index = job.queued_at
            self.queued_work[index] = max(0.0, self.queued_work[index] - job.queued_weight)
            job.queued_at, job.queued_weight = None, 0.0

    def _enqueue(self, path, text, store, cost, snapshot=None, position=None):
        if self.closed or not self.tasks:
            raise RuntimeError("pool is not running")
        if self.outstanding >= self.queue_limit:
            raise ValueError("pool queue budget exhausted")
        cost = self.service_seconds if cost is None else cost
        if not math.isfinite(cost) or cost <= 0:
            raise ValueError("expected a positive finite service time")
        candidates = [i for i in range(len(self.peers)) if self.healthy[i] and
                      (snapshot is None or i in snapshot.replicas)]
        if not candidates:
            raise RuntimeError("no live replica of the requested snapshot")
        owner = min(candidates, key=self._load)
        if snapshot is not None:
            if snapshot not in self.snapshots:
                raise ValueError("snapshot belongs to another pool")
            snapshot.acquire()
        future = asyncio.get_running_loop().create_future()
        job = Job(path, text, store, cost, future, snapshot, position)
        self._put(owner, [job])
        def cancelled(future):
            if future.cancelled() and job.queued_at is not None:
                index = job.queued_at
                self.queued_work[index] = max(0.0, self.queued_work[index] - job.queued_weight)
                job.queued_weight = 0.0
                self._wake()
        future.add_done_callback(cancelled)
        self.outstanding += 1
        self._wake()
        return future

    async def run_at(self, path, line, character, text, *, store=True, estimate=None):
        return await self._enqueue(path, text, store, estimate,
                                   position={"line": line, "character": character})

    def submit(self, snapshot, text, *, store=False, estimate=None):
        """Submit one non-linear continuation; cancellation of its future cancels the job."""
        return self._enqueue(snapshot.path, text, store, estimate, snapshot)

    async def run_with(self, snapshot, text, *, store=False, estimate=None):
        return await self.submit(snapshot, text, store=store, estimate=estimate)

    def _finished(self, job):
        if job.snapshot is not None:
            job.snapshot.drop()
        self.outstanding -= 1
        self._wake()

    def _fail_unstarted(self, jobs, error):
        while jobs:
            job = jobs.popleft() if isinstance(jobs, deque) else jobs.pop()
            self._unqueue(job)
            if not job.future.done():
                job.future.set_exception(error)
            self._finished(job)

    def _fence_failed_peers(self):
        for index, peer in enumerate(self.peers):
            if getattr(peer, "failed", False):
                self.healthy[index] = False
                self._fail_unstarted(self.queues[index], ConnectionError("worker lost; queued job was not executed"))

    async def _artifact(self, snapshot, source):
        # Serialize at most one uncached export at a time, bounding transient blob
        # memory and coalescing concurrent requests for the same parent.
        async with self.export_lock:
            if snapshot.key in self.cache:
                self.cache.move_to_end(snapshot.key)
                return self.cache[snapshot.key]
            manifest, data = await self.peers[source].call("export", {
                "path": snapshot.path, "handle": snapshot.replicas[source],
            })
            self.metrics["exports"] += 1
            self.source_stall = 0.75 * self.source_stall + 0.25 * manifest.get("exportSeconds", self.source_stall)
            if len(data) > self.cache_limit:
                raise ValueError("snapshot exceeds coordinator cache budget")
            while self.cache_size + len(data) > self.cache_limit:
                _, (_, old_data) = self.cache.popitem(last=False)
                self.cache_size -= len(old_data)
            self.cache[snapshot.key] = manifest, data
            self.cache_size += len(data)
            return manifest, data

    async def _copy(self, snapshot, source, destination):
        if destination in snapshot.replicas:
            return
        started = time.monotonic()
        manifest, data = await self._artifact(snapshot, source)
        result, _ = await self.peers[destination].call("import", {
            "path": snapshot.path, "manifest": manifest,
        }, data)
        snapshot.replicas[destination] = result["handle"]
        elapsed = time.monotonic() - started
        self.copy_seconds = 0.75 * self.copy_seconds + 0.25 * elapsed
        self.metrics["imports"] += 1
        self.metrics["transferBytes"] += len(data)
        self.metrics["transferSeconds"] += elapsed

    async def _steal(self, destination):
        victims = [i for i, queue in enumerate(self.queues)
                   if i != destination and self.healthy[i] and queue]
        # The standard randomized two-choice rule avoids a global donor sort.
        choices = self.random.sample(victims, min(2, len(victims)))
        choices.sort(key=self._load, reverse=True)
        for source in choices:
            queue = self.queues[source]
            snapshot = queue[-1].snapshot
            if snapshot is None or (snapshot.key, destination) in self.failed_copies:
                continue
            batch = []
            for job in reversed(queue):
                if job.snapshot is not snapshot or job.future.cancelled():
                    break
                batch.append(job)
            batch.reverse()
            cached = destination in snapshot.replicas
            exported = snapshot.key in self.cache
            active = self.active[source]
            remaining = max(0.0, active.cost - (time.monotonic() - active.started)) if active else 0.0
            transfer = 0.0 if cached else self.copy_seconds + (0.0 if exported else remaining)
            stall = 0.0 if cached or exported else self.source_stall
            count = steal_count([job.cost for job in batch], self._load(source), transfer,
                                stall, self.margin)
            if not count:
                continue
            # Reserve jobs before awaiting any I/O: the donor can never execute this batch.
            reserved = [queue.pop() for _ in range(count)]
            for job in reserved:
                self._unqueue(job)
            try:
                await self._copy(snapshot, source, destination)
            except asyncio.CancelledError:
                self._put(source, reversed(reserved))
                self._wake()
                raise
            except Exception:
                # No tactic ran on the destination. A rejected/incomplete copy is safe
                # to abandon; the reserved jobs still hold the original snapshot alive.
                self._fence_failed_peers()
                if self.healthy[source]:
                    self._put(source, reversed(reserved))
                else:
                    self._fail_unstarted(reserved, ConnectionError("snapshot source lost before transfer"))
                self.failed_copies.add((snapshot.key, destination))
                self.metrics["copyFailures"] += 1
                self._wake()
                continue
            self._put(destination, reversed(reserved))
            self.metrics["steals"] += 1
            self.metrics["stolenJobs"] += count
            return True
        return False

    async def _execute(self, index, job):
        params = {"path": job.path, "text": job.text, "store": job.store}
        if job.snapshot is None:
            op, params["position"] = "runAt", job.position
        else:
            op, params["handle"] = "runWith", job.snapshot.replicas[index]
        operation = asyncio.create_task(self.peers[index].call(op, params))

        def cancel_operation(future):
            if future.cancelled():
                operation.cancel()

        job.future.add_done_callback(cancel_operation)
        try:
            result, _ = await operation
            duration = result.pop("serviceSeconds")
            self.service_seconds = 0.8 * self.service_seconds + 0.2 * duration
            if result.get("handle") is not None:
                snapshot = Snapshot(job.path, {index: result["handle"]})
                self.snapshots.add(snapshot)
                result["handle"] = snapshot
            if not job.future.done():
                job.future.set_result(result)
            elif result.get("handle") is not None:
                await self.release(result["handle"])
            self.metrics["completedByWorker"][index] += 1
        except asyncio.CancelledError:
            if self.closed:
                raise
            if not job.future.done():
                job.future.cancel()
        except Exception as error:
            # Never replay a request after an ambiguous transport failure.
            if not job.future.done():
                job.future.set_exception(error)
            self._fence_failed_peers()
        finally:
            job.future.remove_done_callback(cancel_operation)

    async def _worker(self, index):
        while not self.closed:
            self.wakes[index].clear()
            if not self.healthy[index]:
                await self.wakes[index].wait()
                continue
            if not self.queues[index]:
                if not self.stealing or not await self._steal(index):
                    await self.wakes[index].wait()
                    continue
            job = self.queues[index].popleft()
            self._unqueue(job)
            try:
                if not job.future.cancelled():
                    job.started = time.monotonic()
                    self.active[index] = job
                    await self._execute(index, job)
            finally:
                if self.closed and not job.future.done():
                    job.future.cancel()
                self.active[index] = None
                self._finished(job)

    async def release(self, snapshot):
        if snapshot not in self.snapshots:
            raise ValueError("unknown snapshot")
        if snapshot.closing:
            raise ValueError("snapshot is already closing")
        snapshot.closing = True
        task = asyncio.create_task(self._release(snapshot))
        self.releases.add(task)
        def completed(task):
            self.releases.discard(task)
            if not task.cancelled():
                task.exception()  # Observe cleanup failures even if the caller cancelled.
        task.add_done_callback(completed)
        # Cleanup outlives a cancelled caller; close also waits for pending cleanup.
        await asyncio.shield(task)

    async def _release(self, snapshot):
        await snapshot.idle.wait()
        outcomes = await asyncio.gather(*(self.peers[index].call("release", {
            "path": snapshot.path, "handle": raw,
        }) for index, raw in snapshot.replicas.items()), return_exceptions=True)
        self.snapshots.remove(snapshot)
        old = self.cache.pop(snapshot.key, None)
        if old is not None:
            self.cache_size -= len(old[1])
        self.failed_copies = {entry for entry in self.failed_copies if entry[0] != snapshot.key}
        errors = [result for result in outcomes if isinstance(result, BaseException)]
        if errors:
            raise RuntimeError(f"snapshot release failed on {len(errors)} worker(s)") from errors[0]

    async def close(self):
        self.closed = True
        self._wake()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for queue in self.queues:
            while queue:
                job = queue.popleft()
                self._unqueue(job)
                job.future.cancel()
                self._finished(job)
        for snapshot in list(self.snapshots):
            if not snapshot.closing:
                try:
                    await self.release(snapshot)
                except Exception:
                    pass  # A lost worker's process owns its remaining mappings.
        self.cache.clear()
        self.cache_size = 0
        if self.releases:
            await asyncio.gather(*self.releases, return_exceptions=True)
            self.releases.clear()

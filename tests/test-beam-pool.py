#!/usr/bin/env python3
"""Scheduler invariants, including failures before execution and cancellation."""
import asyncio
import itertools
import json
import hashlib
from pathlib import Path
import random
import sys
import time
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.beam_pool.scheduler import Pool, steal_count
from scripts.beam_pool.lsp import Lsp
from scripts.beam_pool.worker import Worker, digest_file


class SplitTest(unittest.TestCase):
    def test_equal_jobs_reduce_to_latency_adjusted_half(self):
        self.assertEqual(steal_count([1] * 10, 10, 2), 4)
        self.assertEqual(steal_count([1] * 10, 10, 0), 5)
        self.assertEqual(steal_count([0.001] * 10, 0.01, 1), 0)

    def test_selected_split_improves_predicted_finish(self):
        rng = random.Random(7)
        for _ in range(1000):
            costs = [rng.uniform(0.001, 3) for _ in range(rng.randint(1, 30))]
            work = sum(costs) + rng.random()
            transfer, stall, margin = rng.random() * 10, rng.random(), 0.05
            count = steal_count(costs, work, transfer, stall, margin)
            if count:
                moved = sum(costs[-count:])
                self.assertLess(max(work + stall - moved, transfer + moved), work - margin)

    def test_bad_estimates_rejected(self):
        for bad in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                steal_count([1], 1, bad)


class FakePeer:
    counter = itertools.count()

    def __init__(self, *, reject_import=False):
        self.handles = set()
        self.reject_import = reject_import
        self.exports = self.imports = self.runs = 0
        self.executing = 0

    def mint(self):
        handle = str(next(self.counter))
        self.handles.add(handle)
        return handle

    async def call(self, op, p=None, body=b""):
        p = p or {}
        if op == "status":
            return {"runtime": "same", "slots": 1}, b""
        if op == "runAt":
            return {"success": True, "handle": self.mint(), "serviceSeconds": 0.001}, b""
        if op == "import":
            self.imports += 1
            await asyncio.sleep(0.003)
            if self.reject_import:
                raise ValueError("incompatible artifact")
            return {"handle": self.mint()}, b""
        assert p["handle"] in self.handles, (op, p)
        if op == "export":
            self.exports += 1
            await asyncio.sleep(0.003)
            return {}, b"snapshot"
        if op == "release":
            self.handles.remove(p["handle"])
            return {}, b""
        assert op == "runWith"
        self.runs += 1
        self.executing += 1
        start = time.monotonic()
        try:
            await asyncio.sleep(float(p["text"]))
            result = {"success": True, "serviceSeconds": time.monotonic() - start}
            if p["store"]:
                result["handle"] = self.mint()
            return result, b""
        finally:
            self.executing -= 1


class PoolTest(unittest.IsolatedAsyncioTestCase):
    async def make_pool(self, peers, **kwargs):
        pool = await Pool(peers, cold_transfer_seconds=0.006, source_stall_seconds=0.003,
                          margin_seconds=0.001, **kwargs).start()
        self.addAsyncCleanup(pool.close)
        handle = (await pool.run_at("Proof.lean", 0, 0, "skip"))["handle"]
        return pool, handle

    async def test_batch_steals_share_one_export_and_release_all_replicas(self):
        peers = [FakePeer() for _ in range(3)]
        pool, handle = await self.make_pool(peers)
        results = await asyncio.gather(*(pool.submit(handle, "0.03", estimate=0.03) for _ in range(30)))
        self.assertTrue(all(result["success"] for result in results))
        self.assertEqual(sum(peer.exports for peer in peers), 1)
        self.assertEqual(sum(peer.imports for peer in peers), 2)
        self.assertTrue(all(peer.runs > 0 for peer in peers))
        await pool.release(handle)
        self.assertEqual(pool.cache_size, 0)
        self.assertTrue(all(not peer.handles for peer in peers))

    async def test_copy_failure_returns_unexecuted_jobs(self):
        peers = [FakePeer(), FakePeer(reject_import=True)]
        pool, handle = await self.make_pool(peers)
        await asyncio.gather(*(pool.submit(handle, "0.01", estimate=0.03) for _ in range(12)))
        self.assertEqual(peers[0].runs, 12)
        self.assertEqual(peers[1].runs, 0)
        self.assertEqual(pool.metrics["copyFailures"], 1)
        self.assertEqual(pool.outstanding, 0)

    async def test_release_waits_for_queued_and_running_readers(self):
        peers = [FakePeer()]
        pool, handle = await self.make_pool(peers)
        running = pool.submit(handle, "0.03")
        queued = pool.submit(handle, "0.03")
        queued.cancel()
        release = asyncio.create_task(pool.release(handle))
        await asyncio.sleep(0.005)
        self.assertFalse(release.done())
        with self.assertRaises(ValueError):
            pool.submit(handle, "0.01")
        await running
        await release
        self.assertEqual(pool.outstanding, 0)
        self.assertFalse(peers[0].handles)

    async def test_cancellation_does_not_retry_execution(self):
        peers = [FakePeer()]
        pool, handle = await self.make_pool(peers)
        job = pool.submit(handle, "1")
        await asyncio.sleep(0.005)
        job.cancel()
        await pool.release(handle)
        self.assertEqual(peers[0].runs, 1)
        self.assertEqual(peers[0].executing, 0)

    async def test_close_finishes_pending_futures(self):
        pool, handle = await self.make_pool([FakePeer()])
        jobs = [pool.submit(handle, "1") for _ in range(3)]
        await asyncio.sleep(0.005)
        await pool.close()
        self.assertTrue(all(job.done() for job in jobs))
        self.assertEqual(pool.outstanding, 0)

    async def test_small_jobs_stay_local(self):
        peers = [FakePeer(), FakePeer()]
        pool, handle = await self.make_pool(peers)
        pool.copy_seconds = 1.0
        await asyncio.gather(*(pool.submit(handle, "0.001", estimate=0.001) for _ in range(5)))
        self.assertEqual(pool.metrics["exports"], 0)

    async def test_cancelled_release_still_releases_replicas(self):
        peers = [FakePeer()]
        pool, handle = await self.make_pool(peers)
        work = pool.submit(handle, "0.02")
        release = asyncio.create_task(pool.release(handle))
        await asyncio.sleep(0.002)
        release.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await release
        await work
        await pool.close()
        self.assertFalse(peers[0].handles)

    async def test_lost_worker_fails_queue_without_replaying(self):
        class LostPeer(FakePeer):
            failed = False

            async def call(self, op, p=None, body=b""):
                if op == "runWith":
                    self.runs += 1
                    self.failed = True
                    raise ConnectionError("lost during execution")
                return await super().call(op, p, body)

        peer = LostPeer()
        pool, handle = await self.make_pool([peer], stealing=False)
        jobs = [pool.submit(handle, "1") for _ in range(5)]
        results = await asyncio.gather(*jobs, return_exceptions=True)
        self.assertTrue(all(isinstance(result, ConnectionError) for result in results))
        self.assertEqual(peer.runs, 1)
        self.assertEqual(pool.outstanding, 0)
        with self.assertRaises(RuntimeError):
            pool.submit(handle, "0.01")


class LspTest(unittest.IsolatedAsyncioTestCase):
    async def test_open_reports_fatal_setup_errors_without_rebuilding(self):
        client = object.__new__(Lsp)
        client.notify = AsyncMock()
        client.call = AsyncMock(return_value={"hasFatalSnapshot": True, "saveReadiness": {
            "currentDiagnostics": [{"message": "Failed to load the Lake workspace"}],
        }})
        path = Path(__file__).resolve().parent / "scenario/docs/SimpleProof.lean"
        with self.assertRaisesRegex(RuntimeError, "Failed to load the Lake workspace"):
            await client.open(path)
        self.assertEqual(client.notify.call_args_list[0].args[1]["dependencyBuildMode"], "never")
        self.assertEqual(client.notify.call_args_list[-1].args[0], "textDocument/didClose")
        # Ordinary proof errors must not prevent checking candidates in the document.
        client.call.return_value["hasFatalSnapshot"] = False
        self.assertEqual((await client.open(path))["uri"], path.as_uri())

    async def test_cancelled_open_closes_unadmitted_document(self):
        client = object.__new__(Lsp)
        client.notify = AsyncMock()
        client.call = AsyncMock(side_effect=asyncio.CancelledError())
        path = Path(__file__).resolve().parent / "scenario/docs/SimpleProof.lean"
        with self.assertRaises(asyncio.CancelledError):
            await client.open(path)
        self.assertEqual(client.notify.call_args_list[-1].args[0], "textDocument/didClose")

    async def test_server_and_client_request_ids_are_independent(self):
        class Writer:
            def write(self, data):
                pass

            async def drain(self):
                pass

        class Process:
            stdout = asyncio.StreamReader()
            stdin = Writer()

        process = Process()
        client = Lsp(process)
        request = asyncio.create_task(client.call("example", {}, timeout=1))
        await asyncio.sleep(0)
        for message in ({"id": 1, "method": "workspace/semanticTokens/refresh", "params": {}},
                        {"id": 1, "result": {"ok": True}}):
            payload = json.dumps(message).encode()
            process.stdout.feed_data(f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
        self.assertEqual(await request, {"ok": True})
        process.stdout.feed_eof()
        await client.reader


class WorkerBudgetTest(unittest.IsolatedAsyncioTestCase):
    async def test_mapping_budget_counts_other_file_processes(self):
        root = Path(__file__).resolve().parents[1]
        paths = ["tests/scenario/docs/SimpleProof.lean", "tests/scenario/docs/SimpleProofB.lean"]
        docs = [{"uri": (root / path).as_uri(), "version": 1} for path in paths]

        class FakeLsp:
            async def call(self, method, params, timeout):
                assert method == "$/lean/beam/snapshotContext", "over-budget import reached the unsafe reader"
                return {"identity": {}, "regions": [], "residentBytes": 60,
                        "processId": docs.index(params["textDocument"])}

            async def close(self):
                pass

        worker = Worker(root, root / "plugin.so", "test" * 8, memory_limit=100)
        self.addAsyncCleanup(worker.close)
        worker.lsp = FakeLsp()
        worker.loaded_libraries = lambda context: []
        worker.documents = {path: (doc, digest_file(root / path)) for path, doc in zip(paths, docs)}
        body = b"0123456789"
        with self.assertRaisesRegex(ValueError, "memory budget"):
            await worker.execute("import", {
                "path": paths[0], "manifest": {"identity": {}, "dependencies": [], "libraries": [],
                                                "sha256": hashlib.sha256(body).hexdigest()},
            }, body)

    async def test_full_handle_budget_still_allows_readonly_continuation(self):
        root = Path(__file__).resolve().parents[1]
        path = "tests/scenario/docs/SimpleProof.lean"
        doc = {"uri": (root / path).as_uri(), "version": 1}

        class FakeLsp:
            async def call(self, method, params, timeout):
                assert method == "$/lean/runWith"
                return {"success": True}

            async def close(self):
                pass

        worker = Worker(root, root / "plugin.so", "test" * 8, handle_limit=1)
        self.addAsyncCleanup(worker.close)
        worker.lsp = FakeLsp()
        worker.documents[path] = doc, digest_file(root / path)
        worker.handles["parent"] = {"value": "native"}, path
        result, _ = await worker.execute("runWith", {"path": path, "handle": "parent", "text": "trivial"}, b"")
        self.assertTrue(result["success"])


if __name__ == "__main__":
    unittest.main()

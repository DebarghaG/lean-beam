#!/usr/bin/env python3
"""Pool admission, cancellation, and private protocol tests."""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.beam_pool.pool import Pool
from scripts.beam_pool.protocol import Failure, fields
from scripts.beam_pool.transport import RpcServer, rpc, read_frame

TOKEN = "pool-test-capability-0001"


class ProtocolTest(unittest.TestCase):
    def test_closed_records(self):
        for value in [None, [], {"a": 1, "extra": 2}]:
            with self.assertRaises(Failure):
                fields(value, {"a"})


class SchedulingTest(unittest.IsolatedAsyncioTestCase):
    """Deterministically exercise admission and disconnects without tactic timing races."""
    async def asyncSetUp(self):
        self.gate = asyncio.Event()
        self.started = []
        self.cancelled = asyncio.Event()
        self.servers = []
        endpoints = []
        for index in range(2):
            async def dispatch(op, args, emit, index=index):
                if op == "info":
                    return dict(snapshot="s", runtime="r", generation=str(index), slots=1, busy=0,
                                handles=0, ready=True, completed=0, failures=0, restarts=0)
                text = args.get("text", op)
                self.started.append(text)
                if text == "block":
                    try:
                        await self.gate.wait()
                    except asyncio.CancelledError:
                        self.cancelled.set()
                        raise
                result = {"success": True, "next_handle": None}
                if args.get("store") or op == "runWith":
                    result["next_handle"] = {"id": len(self.started)}
                return {"generation": str(index), "result": result}
            server = RpcServer(dispatch, TOKEN)
            endpoints.append(await server.start("127.0.0.1", 0))
            self.servers.append(server)
        self.pool = Pool(endpoints, TOKEN, per_group=3, timeout=10)
        await self.pool.start()
        self.gateway = RpcServer(self.pool.dispatch, TOKEN)
        self.address = await self.gateway.start("127.0.0.1", 0)

    async def asyncTearDown(self):
        await self.gateway.close()
        await self.pool.close()
        for server in self.servers:
            await server.close()

    async def run_at(self, text, *, group="a", snapshot="s", store=False, **changes):
        return await rpc(self.address, TOKEN, "runAt", {
            "snapshot": snapshot, "path": "Proof.lean", "line": 0, "character": 0,
            "text": text, "source": "fixture", "store": store, "group": group, **changes})

    async def run_with(self, handle, text, *, linear=False):
        return await rpc(self.address, TOKEN, "runWith", {
            "handle": handle, "text": text, "linear": linear, "source": "fixture", "group": "a"})

    async def release(self, handle):
        return await rpc(self.address, TOKEN, "release", {"handle": handle, "group": "a"})

    async def test_rejects_invalid_execution_parameters(self):
        for changes in [{"path": "../secret.lean"}, {"line": True}, {"store": 1}, {"extra": True}]:
            with self.subTest(changes=changes), self.assertRaises(Failure) as error:
                await self.run_at("skip", **changes)
            self.assertEqual(error.exception.code, "invalidParams")
        self.assertFalse(self.started)

    async def test_capability(self):
        with self.assertRaises(Failure) as error:
            await rpc(self.address, "wrong", "info", {})
        self.assertEqual(error.exception.code, "unauthorized")

    async def until(self, condition):
        async with asyncio.timeout(3):
            while not condition():
                await asyncio.sleep(.005)

    async def test_fair_admission_and_bounded_groups(self):
        # Reserve the second peer so only one slot can run.
        list(self.pool.peers.values())[1].busy = 1
        first = asyncio.create_task(self.run_at("block"))
        await self.until(lambda: "block" in self.started)
        a1 = asyncio.create_task(self.run_at("a1"))
        a2 = asyncio.create_task(self.run_at("a2"))
        await self.until(lambda: self.pool.queued == 2)
        with self.assertRaises(Failure) as failure:
            await self.run_at("excess")
        self.assertEqual(failure.exception.code, "overloaded")
        b1 = asyncio.create_task(self.run_at("b1", group="b"))
        await self.until(lambda: self.pool.queued == 3)
        self.gate.set()
        await asyncio.gather(first, a1, a2, b1)
        self.assertEqual(self.started, ["block", "a1", "b1", "a2"])

    async def test_pinned_queue_bypass_and_reader_exclusion(self):
        root = (await self.run_at("root", store=True))["result"]["next_handle"]
        first = asyncio.create_task(self.run_with(root, "block"))
        await self.until(lambda: "block" in self.started)
        pinned = asyncio.create_task(self.run_with(root, "pinned"))
        await self.until(lambda: self.pool.queued == 1)
        with self.assertRaises(Failure) as failure:
            await self.release(root)
        self.assertEqual(failure.exception.code, "handleBusy")
        # Same group, later arrival: it can use the otherwise idle worker.
        await self.run_at("independent")
        self.assertNotIn("pinned", self.started)
        self.gate.set()
        await asyncio.gather(first, pinned)

    async def test_active_disconnect_cancels_only_its_request(self):
        first = asyncio.create_task(self.run_at("block"))
        second = asyncio.create_task(self.run_at("block"))
        await self.until(lambda: self.started.count("block") == 2)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await self.until(self.cancelled.is_set)
        self.gate.set()
        self.assertTrue((await second)["result"]["success"])
        await self.until(lambda: not self.pool.jobs)
        self.assertEqual(self.pool.running, 0)
        self.assertEqual(self.pool.groups, {})

    async def test_queued_disconnect_releases_admission(self):
        for peer in self.pool.peers.values():
            peer.busy = 1
        task = asyncio.create_task(self.run_at("queued", store=True))
        await self.until(lambda: self.pool.queued == 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await self.until(lambda: not self.pool.jobs)
        self.assertEqual(self.started, [])
        self.assertEqual(self.pool.queued, 0)
        self.assertEqual(self.pool.reserved_handles, 0)
        self.assertEqual(self.pool.groups, {})

    async def test_completed_requests_are_not_retained(self):
        root = (await self.run_at("root", store=True))["result"]["next_handle"]
        for _ in range(300):
            root = (await self.run_with(root, "step", linear=True))["result"]["next_handle"]
        await self.until(lambda: not self.pool.jobs)
        self.assertEqual(len(self.pool.handles), 1)
        self.assertEqual(self.pool.reserved_handles, 0)
        await self.release(root)
        self.assertFalse(self.pool.handles)

    async def test_wrong_snapshot_fails_without_waiting_for_deadline(self):
        with self.assertRaises(Failure) as failure:
            await self.run_at("skip", snapshot="different")
        self.assertEqual(failure.exception.code, "contentModified")

    async def test_mixed_runtime_rejected_even_when_peer_busy(self):
        peer = list(self.pool.peers.values())[1]
        peer.info["runtime"], peer.busy = "different", 1
        with self.assertRaises(Failure) as failure:
            await self.run_at("skip")
        self.assertEqual(failure.exception.code, "contentModified")

    async def test_malformed_frames_and_typed_response(self):
        for payload in [b'{"id":1,"id":2}\n', b'{"n":NaN}\n', b'[]\n']:
            reader = asyncio.StreamReader()
            reader.feed_data(payload)
            reader.feed_eof()
            with self.assertRaises(Failure):
                await read_frame(reader)
        async def invalid(op, args, emit):
            return []
        server = RpcServer(invalid, TOKEN)
        address = await server.start("127.0.0.1", 0)
        try:
            with self.assertRaises(Failure) as failure:
                await rpc(address, TOKEN, "info", {})
            self.assertEqual(failure.exception.code, "protocolError")
        finally:
            await server.close()


if __name__ == "__main__":
    unittest.main()

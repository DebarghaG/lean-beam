#!/usr/bin/env python3
"""Pool admission, cancellation, and private protocol tests."""

import asyncio
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.beam_pool.pool import Pool
from scripts.beam_pool.mcp import Mcp
from scripts.beam_pool.protocol import Failure, canonical, fields
from scripts.beam_pool.transport import RpcServer, rpc, read_frame
from scripts.beam_pool.worker import Worker

TOKEN = "pool-test-capability-0001"


class ProtocolTest(unittest.TestCase):
    def test_closed_records(self):
        for value in [None, [], {"a": 1, "extra": 2}]:
            with self.assertRaises(Failure):
                fields(value, {"a"})


class McpLifetimeTest(unittest.IsolatedAsyncioTestCase):
    async def test_failed_drain_retires_worker(self):
        worker = Worker(Path.cwd(), Path("unused"), Path("unused"), Path("unused"))
        worker.begin_recycle = Mock()
        old = object()
        async def invalid_reply():
            raise ValueError("invalid terminal result")
        task = asyncio.create_task(invalid_reply())
        await asyncio.gather(task, return_exceptions=True)
        await worker.drain(task, old, "Proof.lean")
        worker.begin_recycle.assert_called_once_with(old)

    async def test_cancelled_success_releases_discarded_handle(self):
        # Reply successfully after cancellation wins, as an uninterruptible tactic may do.
        code = '''import json, os, sys
pending = None
handles = 0
def reply(identity, result):
    print(json.dumps({"jsonrpc": "2.0", "id": identity,
        "result": {"isError": False, "structuredContent": result}}), flush=True)
for line in sys.stdin:
    message = json.loads(line)
    if message["method"] == "notifications/cancelled":
        handles += 1
        reply(pending, {"next_handle": {"id": 1}})
    elif message["params"]["name"] == "wait":
        pending = message["id"]
        print(json.dumps({"jsonrpc": "2.0", "method": "notifications/progress",
            "params": {"progressToken": pending}}), flush=True)
    elif message["params"]["name"] == "lean_release":
        handles -= 1
        reply(message["id"], {})
    else:
        reply(message["id"], {"handles": handles,
            "credentials": any(k in os.environ for k in ("BEAM_POOL_TOKEN", "BEAM_LEAN_POOL_CONFIG"))})
'''
        mcp = Mcp([sys.executable, "-c", code], str(Path.cwd()), 1, drain_on_cancel=True)
        self.addAsyncCleanup(mcp.close)
        with patch.dict(os.environ, BEAM_POOL_TOKEN=TOKEN, BEAM_LEAN_POOL_CONFIG="unused"):
            await mcp.start()
        worker = Worker(Path.cwd(), Path("unused"), Path("unused"), Path("unused"))
        worker.snapshot = SimpleNamespace(root=Path.cwd())
        started = asyncio.Event()
        async def emit(event):
            started.set()
        async def execute():
            result = await mcp.call("wait", {}, emit)
            worker.handles[canonical(result["next_handle"])] = 0
            return {"result": result}
        task = asyncio.create_task(execute())
        await asyncio.wait_for(started.wait(), 2)
        await worker.drain(task, mcp, "Proof.lean")
        self.assertEqual(await mcp.call("inspect", {}), {"handles": 0, "credentials": False})
        self.assertFalse(worker.handles)
        self.assertIsNone(worker.reset_task)


class SchedulingTest(unittest.IsolatedAsyncioTestCase):
    """Deterministically exercise admission and disconnects without tactic timing races."""
    async def asyncSetUp(self):
        self.gate = asyncio.Event()
        self.started = []
        self.cancelled = asyncio.Event()
        self.cancel_gate = asyncio.Event()
        self.cancel_gate.set()
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
                        await self.cancel_gate.wait()
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

    async def test_cancellation_holds_worker_slot_until_drain(self):
        list(self.pool.peers.values())[1].busy = 1
        self.cancel_gate.clear()
        first = asyncio.create_task(self.run_at("block"))
        await self.until(lambda: "block" in self.started)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await self.until(self.cancelled.is_set)
        following = asyncio.create_task(self.run_at("following"))
        try:
            await self.until(lambda: self.pool.queued == 1 or "following" in self.started)
            self.assertNotIn("following", self.started)
            self.assertEqual(self.pool.running, 1)
        finally:
            self.cancel_gate.set()
            await following

    async def test_unacknowledged_cancellation_retires_peer(self):
        owner, other = self.pool.peers.values()
        other.busy = 1
        self.pool.background[0].cancel()
        await asyncio.gather(self.pool.background[0], return_exceptions=True)
        self.cancel_gate.clear()
        first = asyncio.create_task(self.run_at("block"))
        await self.until(lambda: "block" in self.started)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        try:
            async with asyncio.timeout(7):
                while self.pool.jobs:
                    await asyncio.sleep(.01)
            self.assertFalse(owner.info["ready"])
            self.assertEqual(owner.busy, 0)
        finally:
            self.cancel_gate.set()

    async def test_completed_requests_are_not_retained(self):
        root = (await self.run_at("root", store=True))["result"]["next_handle"]
        for _ in range(300):
            root = (await self.run_with(root, "step", linear=True))["result"]["next_handle"]
        await self.until(lambda: not self.pool.jobs)
        self.assertEqual(len(self.pool.handles), 1)
        self.assertEqual(self.pool.reserved_handles, 0)
        await self.release(root)
        self.assertFalse(self.pool.handles)

    async def test_queued_consumers_preserve_handle_on_disconnect_or_timeout(self):
        for op in ("linear", "release"):
            for ending in ("disconnect", "timeout"):
                with self.subTest(op=op, ending=ending):
                    root = (await self.run_at("root", store=True))["result"]["next_handle"]
                    before = list(self.started)
                    for peer in self.pool.peers.values():
                        peer.busy = 1
                    self.pool.timeout = .1 if ending == "timeout" else 10
                    call = self.run_with(root, "step", linear=True) if op == "linear" else self.release(root)
                    task = asyncio.create_task(call)
                    await self.until(lambda: self.pool.queued == 1)
                    with self.assertRaises(Failure) as busy:
                        await self.run_with(root, "reader")
                    self.assertEqual(busy.exception.code, "handleBusy")
                    if ending == "disconnect":
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    else:
                        with self.assertRaises(Failure) as failure:
                            await task
                        self.assertEqual(failure.exception.code, "deadlineExceeded")
                    await self.until(lambda: not self.pool.jobs)
                    self.assertEqual(self.started, before)
                    self.assertIn(root, self.pool.handles)
                    self.assertEqual(self.pool.reserved_handles, 0)
                    self.pool.timeout = 10
                    for peer in self.pool.peers.values():
                        peer.busy = 0
                    self.pool.wakeup.set()
                    await self.release(root)

    async def test_worker_rejection_preserves_handle_and_readiness(self):
        from scripts.beam_pool import pool as module
        root = (await self.run_at("root", store=True))["result"]["next_handle"]
        owner = self.pool.handles[root].peer
        real_rpc = module.rpc
        for code in ("overloaded", "contentModified"):
            async def reject(endpoint, token, op, args, *rest, **kwargs):
                if args.get("text") == "reject":
                    raise Failure(code, "request rejected before execution")
                return await real_rpc(endpoint, token, op, args, *rest, **kwargs)
            with patch.object(module, "rpc", side_effect=reject):
                with self.assertRaises(Failure) as failure:
                    await self.run_with(root, "reject", linear=True)
                self.assertEqual(failure.exception.code, code)
            self.assertIn(root, self.pool.handles)
            self.assertTrue(owner.info["ready"])
            child = (await self.run_with(root, "step"))["result"]["next_handle"]
            await self.release(child)

    async def test_expiry_skips_changed_generation(self):
        root = (await self.run_at("root", store=True))["result"]["next_handle"]
        handle = self.pool.handles[root]
        handle.peer.info["generation"] = "replacement"
        with patch("scripts.beam_pool.pool.rpc") as release:
            await self.pool.expire_handles()
        release.assert_not_awaited()
        self.assertNotIn(root, self.pool.handles)

    async def test_expiry_does_not_block_discovery_or_other_workers(self):
        from scripts.beam_pool import pool as module
        await self.run_at("first", store=True)
        await self.run_at("second", store=True)
        for handle in self.pool.handles.values():
            handle.touched -= self.pool.handle_ttl + 1
        entered, unblock = set(), asyncio.Event()
        real_rpc = module.rpc
        async def release(endpoint, token, op, args, *rest, **kwargs):
            if op == "release":
                entered.add(endpoint)
                await unblock.wait()
                return {}
            return await real_rpc(endpoint, token, op, args, *rest, **kwargs)
        with patch.object(module, "rpc", side_effect=release), \
             patch.object(self.pool, "discover", wraps=self.pool.discover) as discover:
            try:
                await self.until(lambda: len(entered) == 2)
                count = discover.await_count
                await self.until(lambda: discover.await_count > count)
            finally:
                unblock.set()
                await self.until(lambda: not self.pool.handles)

    async def test_discovery_gap_preserves_peer_identity(self):
        root = (await self.run_at("root", store=True))["result"]["next_handle"]
        owner = self.pool.handles[root].peer
        self.pool.endpoints.remove(owner.endpoint)
        await self.pool.discover()
        with self.assertRaises(Failure) as failure:
            await self.run_with(root, "step")
        self.assertEqual(failure.exception.code, "contentModified")
        self.pool.endpoints.append(owner.endpoint)
        await self.pool.discover()
        self.assertIs(self.pool.peers[owner.endpoint], owner)
        self.assertTrue((await self.run_with(root, "step"))["result"]["success"])

    async def test_connection_cap_applies_before_first_frame(self):
        server = RpcServer(self.pool.dispatch, TOKEN, max_connections=1)
        address = await server.start("127.0.0.1", 0)
        writers = []
        try:
            _, first = await asyncio.open_connection(*address)
            writers.append(first)
            await self.until(lambda: len(server.tasks) == 1)
            reader, second = await asyncio.open_connection(*address)
            writers.append(second)
            self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
            self.assertEqual(len(server.tasks), 1)
        finally:
            for writer in writers:
                writer.close()
            await asyncio.gather(*(writer.wait_closed() for writer in writers))
            await server.close()

    async def test_health_deadline_does_not_wait_for_execution_cleanup(self):
        unblock = asyncio.Event()
        async def slow_info(op, args, emit):
            try:
                await unblock.wait()
            except asyncio.CancelledError:
                await unblock.wait()
                raise
            return {}
        server = RpcServer(slow_info, TOKEN)
        address = await server.start("127.0.0.1", 0)
        try:
            with self.assertRaises(Failure) as failure:
                await asyncio.wait_for(rpc(address, TOKEN, "info", {}, timeout=.05), .5)
            self.assertEqual(failure.exception.code, "deadlineExceeded")
        finally:
            unblock.set()
            await server.close()

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

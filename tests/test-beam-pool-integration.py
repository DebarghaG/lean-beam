#!/usr/bin/env python3
"""Real CLI/MCP regressions for internal pool routing; no additional Python dependencies."""

import asyncio
import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.beam_pool.attach import attach
from scripts.beam_pool.mcp import Mcp
from scripts.beam_pool.pool import Pool
from scripts.beam_pool.protocol import Failure, Snapshot
from scripts.beam_pool.transport import RpcServer
from scripts.beam_pool.worker import Worker

TOKEN = "integration-test-pool-capability"


class AttachmentTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="beam-pool-attach-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "lean-toolchain").write_text("leanprover/lean4:v4.33.0\n")
        (self.root / "Main.lean").write_text("example : True := by trivial\n")
        self.files = Snapshot.create(self.root).files
        self.snapshots = ["prepared"]
        self.config = self.root / ".beam/client.json"

    async def rpc(self, endpoint, token, op, args):
        if op == "info":
            return {"workers": [{"snapshot": s, "ready": True} for s in self.snapshots]}
        items = sorted(self.files.items())
        offset = args["offset"]
        return {"snapshot": args["snapshot"], "runtime": "exact-runtime",
                "files": dict(items[offset:offset+1]),
                "next": offset+1 if offset+1 < len(items) else None}

    async def test_paginated_attachment_keeps_credentials_out_of_config(self):
        with patch("scripts.beam_pool.attach.rpc", side_effect=self.rpc):
            await attach(self.root, ("localhost", 9000), TOKEN, self.config)
        self.assertNotIn(TOKEN, self.config.read_text())
        self.assertEqual(self.config.stat().st_mode & 0o777, 0o600)
        binding = json.loads(self.config.read_text())["bindings"][0]
        self.assertEqual({i["path"] for i in binding["inputs"]}, self.files.keys())

    async def test_source_mismatch_preserves_existing_configuration(self):
        with patch("scripts.beam_pool.attach.rpc", side_effect=self.rpc):
            await attach(self.root, ("localhost", 9000), TOKEN, self.config)
            original = self.config.read_bytes()
            (self.root / "Main.lean").write_text("example : True := True.intro\n")
            with self.assertRaises(Failure) as error:
                await attach(self.root, ("localhost", 9000), TOKEN, self.config)
        self.assertEqual(error.exception.code, "contentModified")
        self.assertEqual(self.config.read_bytes(), original)

    async def test_multiple_runtimes_require_explicit_operator_selection(self):
        self.snapshots.append("another-runtime")
        with patch("scripts.beam_pool.attach.rpc", side_effect=self.rpc):
            with self.assertRaises(Failure):
                await attach(self.root, ("localhost", 9000), TOKEN, self.config)
            reply = await attach(self.root, ("localhost", 9000), TOKEN, self.config, "prepared")
        self.assertEqual(reply["snapshot"], "prepared")


class IntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="beam-pool-integration-")
        self.addCleanup(self.temp.cleanup)
        self.addAsyncCleanup(self.close_resources)
        self.cli_owner = self.native = self.gateway = self.pool = None
        self.workers, self.servers = [], []
        self.state = Path(self.temp.name)
        self.root = self.state / "project"
        self.root.mkdir()
        (self.root / "lean-toolchain").write_text((REPO / "lean-toolchain").read_text())
        (self.root / "lakefile.toml").write_text('name = "pool_integration"\n')
        # Prepare Lake metadata before hashing an immutable snapshot.
        process = await asyncio.create_subprocess_exec("lake", "update", cwd=self.root,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        self.assertEqual(await process.wait(), 0)
        self.code = ('import Lean\nelab "pool_sleep" : tactic => do let _ ← IO.sleep 10000; pure ()\n'
                     'example : True ∧ True := by\n  constructor <;> trivial\n')
        (self.root / "Proof.lean").write_text(self.code)
        (self.root / "Other.lean").write_text(self.code)
        worker_root = self.state / "prepared-worker-project"
        shutil.copytree(self.root, worker_root)
        endpoints = []
        for _ in range(2):
            worker = Worker(worker_root, REPO / ".lake/build/bin/lean-beam-mcp",
                            Path(shutil.which("lean")), REPO / ".lake/build/lib/libbeam_Beam_LSP.so")
            self.workers.append(worker)
            await worker.start()
            server = RpcServer(worker.dispatch, TOKEN)
            self.servers.append(server)
            endpoints.append(await server.start("127.0.0.1", 0))
        self.pool = Pool(endpoints, TOKEN, timeout=30)
        await self.pool.start()
        self.gateway = RpcServer(self.pool.dispatch, TOKEN)
        self.endpoint = await self.gateway.start("127.0.0.1", 0)
        self.config = self.state / "pool.json"
        await attach(self.root, self.endpoint, TOKEN, self.config)
        self.native = Mcp(["env", f"BEAM_LEAN_POOL_CONFIG={self.config}", f"BEAM_POOL_TOKEN={TOKEN}",
            str(REPO / ".lake/build/bin/lean-beam-mcp"), "--lean-cmd", shutil.which("lean"),
            "--lean-plugin", str(REPO / ".lake/build/lib/libbeam_Beam_LSP.so")], str(self.root), 2)
        await self.native.start()
        self.version = (await self.call("lean_sync"))["version"]

    async def close_resources(self):
        if self.cli_owner:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.cli("stop"), 10)
            try:
                await asyncio.wait_for(self.cli_owner.wait(), 10)
            except TimeoutError:
                self.cli_owner.terminate()
                await self.cli_owner.wait()
        if self.native:
            await self.native.close()
        if self.gateway:
            await self.gateway.close()
        if self.pool:
            await self.pool.close()
        for server in self.servers:
            await server.close()
        for worker in self.workers:
            await worker.close()

    async def call(self, name, **args):
        return await self.native.call(name, {"workspace": {"root": str(self.root)},
                                            "path": "Proof.lean", **args})

    async def root_handle(self):
        return (await self.call("lean_run_at_handle", version=self.version, line=3, character=2,
                                text="constructor"))["next_handle"]

    async def test_mcp_routes_independent_calls_and_preserves_results(self):
        replies = await asyncio.gather(*(self.call("lean_run_at", version=self.version, line=3,
            character=2, text="((run_tac do let _ ← IO.sleep 50; pure ()); constructor <;> trivial)")
            for _ in range(8)))
        self.assertTrue(all(r["success"] and not r["proof_state"]["goals"] for r in replies))
        self.assertTrue(all(w.completed > 1 for w in self.workers))
        self.assertEqual(set(replies[0]), {"success", "messages", "traces", "proof_state", "next_handle", "workspace"})
        self.assertFalse(any(w.handles for w in self.workers))
        failed = await self.call("lean_run_at", version=self.version, line=3, character=2,
                                 text="exact pool_missing_witness")
        self.assertFalse(failed["success"])

    async def test_command_continuations_preserve_isolation(self):
        reply = await self.call("lean_run_at_handle", version=self.version, line=2, character=0,
                                text="def poolOnly : Nat := 7")
        self.assertTrue(reply["success"])
        root = reply["next_handle"]
        child = await self.call("lean_run_with", handle=root, text="#check poolOnly")
        self.assertTrue(child["success"])
        fresh = await self.call("lean_run_at", version=self.version, line=2, character=0,
                                text="#check poolOnly")
        self.assertFalse(fresh["success"])
        await self.call("lean_release", handle=root)
        await self.call("lean_release", handle=child["next_handle"])

    async def test_mcp_handles_linear_release_and_wrong_file(self):
        root = await self.root_handle()
        child = (await self.call("lean_run_with", handle=root, text="trivial"))["next_handle"]
        self.assertEqual(len(self.pool.handles), 2)
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", path="Other.lean", handle=root, text="trivial")
        self.assertEqual(error.exception.code, "invalidParams")
        solved = await self.call("lean_run_with_linear", handle=child, text="all_goals trivial")
        self.assertTrue(solved["success"])
        with self.assertRaises(Failure):
            await self.call("lean_run_with", handle=child, text="trivial")
        await self.call("lean_release", handle=root)
        if solved["next_handle"]:
            await self.call("lean_release", handle=solved["next_handle"])
        self.assertEqual(len(self.pool.handles), 0)
        self.assertFalse(any(w.handles for w in self.workers))

    async def test_close_reopen_invalidates_handle_even_at_same_version(self):
        handle = await self.root_handle()
        await self.call("lean_close")
        self.assertEqual((await self.call("lean_sync"))["version"], self.version)
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", handle=handle, text="trivial")
        self.assertEqual(error.exception.code, "contentModified")

    async def test_linear_replacement_at_handle_budget(self):
        self.pool.max_handles = 1
        for worker in self.workers:
            worker.max_handles = 1
        root = await self.root_handle()
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", handle=root, text="skip")
        self.assertEqual(error.exception.code, "resourceExhausted")
        child = await self.call("lean_run_with_linear", handle=root, text="skip")
        self.assertEqual(len(self.pool.handles), 1)
        self.assertEqual(sum(len(w.handles) for w in self.workers), 1)
        await self.call("lean_release", handle=child["next_handle"])

    async def test_expired_lease_releases_remote_state(self):
        handle = await self.root_handle()
        for retained in self.pool.handles.values():
            retained.touched = time.monotonic() - self.pool.handle_ttl - 1
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", handle=handle, text="trivial")
        self.assertEqual(error.exception.code, "contentModified")
        await self.pool.expire_handles()
        self.assertFalse(self.pool.handles)
        self.assertFalse(any(w.handles for w in self.workers))

    async def test_source_and_import_changes_fail_before_remote_execution(self):
        count = self.pool.completed
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_at", version=self.version+1, line=3, character=2, text="skip")
        self.assertEqual(error.exception.code, "contentModified")
        self.assertEqual(self.pool.completed, count)
        # Deliberately violate the prepared-input contract in this negative regression.
        (self.root / "Other.lean").write_text(self.code + "\n-- changed input\n")
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_at", version=self.version, line=3, character=2, text="skip")
        self.assertEqual(error.exception.code, "contentModified")
        self.assertEqual(self.pool.completed, count)

    async def test_source_must_match_the_attached_worker(self):
        await self.call("lean_close")
        (self.root / "Proof.lean").write_text(self.code + "\n-- changed source\n")
        version = (await self.call("lean_sync"))["version"]
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_at", version=version, line=3, character=2, text="skip")
        self.assertEqual(error.exception.code, "contentModified")
        self.assertFalse(any(w.handles for w in self.workers))

    async def test_close_during_execution_rejects_and_releases_the_result(self):
        task = asyncio.create_task(self.call("lean_run_at_handle", version=self.version,
            line=3, character=2, text="run_tac do let _ ← IO.sleep 1000; pure ()"))
        async with asyncio.timeout(5):
            while not any(w.busy for w in self.workers):
                if task.done():
                    await task
                await asyncio.sleep(.01)
        await self.call("lean_close")
        with self.assertRaises(Failure) as error:
            await task
        self.assertEqual(error.exception.code, "contentModified")
        self.assertFalse(self.pool.handles)
        self.assertFalse(any(w.handles for w in self.workers))

    async def test_workspace_eviction_invalidates_continuation(self):
        handle = await self.root_handle()
        await self.native.call("lean_drop_workspace", {"workspace": {"root": str(self.root)}})
        await self.call("lean_sync")
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", handle=handle, text="trivial")
        self.assertEqual(error.exception.code, "contentModified")

    async def test_deleted_client_source_releases_the_remote_result(self):
        task = asyncio.create_task(self.call("lean_run_at_handle", version=self.version,
            line=3, character=2, text="run_tac do let _ ← IO.sleep 1000; pure ()"))
        async with asyncio.timeout(5):
            while not any(w.busy for w in self.workers):
                if task.done():
                    await task
                await asyncio.sleep(.01)
        await self.call("lean_close")
        (self.root / "Proof.lean").unlink()
        with self.assertRaises(Failure) as error:
            await task
        self.assertEqual(error.exception.code, "contentModified")
        self.assertFalse(self.pool.handles)
        self.assertFalse(any(w.handles for w in self.workers))

    async def test_mcp_cancellation_reaches_the_owning_worker(self):
        handle = await self.root_handle()
        generations = [w.generation for w in self.workers]
        task = asyncio.create_task(self.call("lean_run_with", handle=handle, text="pool_sleep"))
        async with asyncio.timeout(5):
            while not any(w.busy for w in self.workers):
                if task.done():
                    await task
                await asyncio.sleep(.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        async with asyncio.timeout(15):
            while all(w.generation == old for w,old in zip(self.workers,generations)):
                await asyncio.sleep(.01)

    async def test_worker_loss_keeps_typed_failure_without_local_reexecution(self):
        root = await self.root_handle()
        for worker in self.workers:
            await worker.recycle(worker.mcp)
        await self.pool.discover()
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", handle=root, text="all_goals trivial")
        self.assertEqual(error.exception.code, "contentModified")

    def cli_env(self):
        return dict(os.environ, BEAM_HOME=str(REPO), BEAM_LEAN_POOL_CONFIG=str(self.config),
                    BEAM_POOL_TOKEN=TOKEN, BEAM_SESSION_ROOT=str(self.state / "sessions"),
                    BEAM_BUNDLE_DIR=str(REPO / ".beam/pool-integration-bundles"))

    async def cli(self, *args):
        proc = await asyncio.create_subprocess_exec(str(REPO / "scripts/lean-beam"),
            "--root", str(self.root), *args, env=self.cli_env(),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await proc.communicate()
        self.assertEqual(proc.returncode, 0, err.decode())
        return json.loads(out)

    async def test_existing_cli_uses_the_same_internal_route(self):
        self.cli_owner = await asyncio.create_subprocess_exec(str(REPO / "scripts/lean-beam"),
            "--root", str(self.root), "serve", env=self.cli_env(),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        async with asyncio.timeout(180):
            line = await self.cli_owner.stdout.readline()
        self.assertTrue(line, "CLI session failed to start")
        synced = await self.cli("sync", "Proof.lean")
        version = str(synced["result"]["version"])
        before = self.pool.completed
        reply = await self.cli("run-at-handle", "Proof.lean", version, "3", "2", "constructor")
        self.assertTrue(reply["result"]["success"])
        self.assertGreater(self.pool.completed, before)
        handle = json.dumps(reply["result"]["handle"])
        child = await self.cli("run-with-linear", "Proof.lean", handle, "all_goals trivial")
        self.assertTrue(child["result"]["success"])
        if "handle" in child["result"]:
            await self.cli("release", "Proof.lean", json.dumps(child["result"]["handle"]))


if __name__ == "__main__":
    unittest.main()

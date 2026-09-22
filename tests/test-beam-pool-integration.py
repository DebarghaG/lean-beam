#!/usr/bin/env python3
"""Real CLI/MCP regressions for internal pool routing; no additional Python dependencies."""

import asyncio
import contextlib
import json
import os
import shutil
import sys
import tempfile
import threading
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
from scripts.beam_pool.worker import Worker, file_io

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
        manifest = self.config.parent / binding["inputs"]
        self.assertEqual({i["path"] for i in json.loads(manifest.read_text())}, self.files.keys())
        self.assertEqual(manifest.stat().st_mode & 0o777, 0o600)

    async def test_multiple_workspace_bindings_share_their_input_manifest(self):
        with tempfile.TemporaryDirectory(prefix="beam-pool-attach-peer-") as other:
            shutil.copytree(self.root, other, dirs_exist_ok=True)
            with patch("scripts.beam_pool.attach.rpc", side_effect=self.rpc):
                await attach(self.root, ("localhost", 9000), TOKEN, self.config)
                await attach(Path(other), ("localhost", 9000), TOKEN, self.config)
        bindings = json.loads(self.config.read_text())["bindings"]
        self.assertEqual(len(bindings), 2)
        self.assertEqual(bindings[0]["inputs"], bindings[1]["inputs"])
        self.assertLess(self.config.stat().st_size, 2048)

    async def test_concurrent_attachments_preserve_both_bindings(self):
        self.config.parent.mkdir()
        self.config.write_text('{"schema": 2, "bindings": []}\n')
        readers = threading.Barrier(2)
        read_text = Path.read_text

        def read_together(path, *args, **kwargs):
            text = read_text(path, *args, **kwargs)
            if path == self.config:
                # Force overlapping reads if writers are unlocked. A serialized writer
                # proceeds when the barrier expires, then lets the other read its update.
                try:
                    readers.wait(timeout=1)
                except threading.BrokenBarrierError:
                    pass
            return text

        with tempfile.TemporaryDirectory(prefix="beam-pool-attach-peer-") as other:
            shutil.copytree(self.root, other, dirs_exist_ok=True)
            with patch("scripts.beam_pool.attach.rpc", side_effect=self.rpc), \
                    patch.object(Path, "read_text", read_together):
                async with asyncio.timeout(10):
                    await asyncio.gather(*(asyncio.to_thread(asyncio.run,
                        attach(root, ("localhost", 9000), TOKEN, self.config))
                        for root in (self.root, Path(other))))
            bindings = json.loads(self.config.read_text())["bindings"]
            self.assertEqual({b["root"] for b in bindings}, {str(self.root), other})
            for binding in bindings:
                self.assertTrue((self.config.parent / binding["inputs"]).is_file())

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
        (self.root / "lakefile.toml").write_text(
            'name = "pool_integration"\n[[lean_lib]]\nname = "Proof"\n'
            '[[lean_lib]]\nname = "Helper"\n')
        (self.root / "Helper.lean").write_text('import Lean\ndef helper : Nat := 1\n')
        # Prepare Lake metadata before hashing an immutable snapshot.
        process = await asyncio.create_subprocess_exec("lake", "update", cwd=self.root,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        self.assertEqual(await process.wait(), 0)
        process = await asyncio.create_subprocess_exec("lake", "build", "Helper", cwd=self.root,
            stdout=asyncio.subprocess.DEVNULL)
        self.assertEqual(await process.wait(), 0)
        self.code = ('import Helper\nelab "pool_sleep" : tactic => do let _ ← IO.sleep 10000; pure ()\n'
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
        synced = await self.call("lean_sync", diagnostics_in_result=True)
        self.assertEqual(synced["readiness"]["blocking_error_count"], 0, synced)
        self.version = synced["version"]

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

    async def test_unused_workspace_manifests_are_not_loaded(self):
        config = json.loads(self.config.read_text())
        config["bindings"].append(config["bindings"][0] | {
            "root": str(self.state / "unused-workspace"), "binding": "unused",
            "inputs": "missing.inputs.json"})
        self.config.write_text(json.dumps(config))
        result = await self.call("lean_run_at", version=self.version, line=3, character=2,
                                 text="constructor <;> trivial")
        self.assertTrue(result["success"])
        self.assertFalse(result["proof_state"]["goals"])
        self.assertEqual(self.pool.completed, 1)

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

    async def test_stale_version_fails_before_remote_execution(self):
        count = self.pool.completed
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_at", version=self.version+1, line=3, character=2, text="skip")
        self.assertEqual(error.exception.code, "contentModified")
        self.assertEqual(self.pool.completed, count)

    async def second_client(self):
        root = self.state / "second-client"
        shutil.copytree(self.state / "prepared-worker-project", root)
        await attach(root, self.endpoint, TOKEN, self.config)
        native = Mcp(list(self.native.command), str(root), 2)
        self.addAsyncCleanup(native.close)
        await native.start()
        async def call(name, **args):
            return await native.call(name, {"workspace": {"root": str(root)}, "path": "Proof.lean", **args})
        version = (await call("lean_sync"))["version"]
        return call, version

    async def test_edit_sync_probe_preserves_other_clients_on_same_worker(self):
        unused = list(self.pool.peers.values())[1]
        unused.busy = 1
        old = await self.root_handle()
        other, other_version = await self.second_client()
        retained = (await other("lean_run_at_handle", version=other_version, line=3,
                                character=2, text="constructor"))["next_handle"]
        owners = {id(h.peer) for h in self.pool.handles.values()}
        self.assertEqual(len(owners), 1)
        (self.root / "Proof.lean").write_text(self.code.replace("True ∧ True", "True ∧ (2 = 2)"))
        version = (await self.call("lean_sync"))["version"]
        self.assertGreater(version, self.version)
        edited = await self.call("lean_run_at", version=version, line=3, character=2, text="constructor")
        self.assertTrue(edited["success"])
        self.assertEqual(edited["proof_state"]["goals"][1]["target"], "2 = 2")
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_with", handle=old, text="all_goals trivial")
        self.assertEqual(error.exception.code, "contentModified")
        solved = await other("lean_run_with", handle=retained, text="all_goals trivial")
        self.assertTrue(solved["success"])
        self.assertFalse(solved["proof_state"]["goals"])
        await other("lean_release", handle=retained)
        await other("lean_release", handle=solved["next_handle"])
        self.assertTrue(all(w.restarts == 0 for w in self.workers))
        unused.busy = 0

    async def test_cache_pressure_cannot_evict_a_revision_being_applied(self):
        unused = list(self.pool.peers.values())[1]
        unused.busy = 1
        await self.call("lean_run_at", version=self.version, line=3, character=2, text="skip")
        worker = next(w for w in self.workers if w.contexts)
        (self.root / "Proof.lean").write_text(self.code.replace("True ∧ True", "True ∧ (2 = 2)"))
        version = (await self.call("lean_sync"))["version"]
        applying, proceed = asyncio.Event(), asyncio.Event()

        async def pause_apply(function, *args):
            if function.__name__ == "apply":
                applying.set()
                await proceed.wait()
            return await file_io(function, *args)

        with patch("scripts.beam_pool.worker.file_io", side_effect=pause_apply):
            task = asyncio.create_task(self.call("lean_run_at", version=version, line=3,
                                                character=2, text="constructor"))
            try:
                await asyncio.wait_for(applying.wait(), 10)
                with self.assertRaises(Failure) as error:
                    worker.projects.make_room(worker.projects.max_bytes, worker.pinned_revisions())
                self.assertEqual(error.exception.code, "resourceExhausted")
            finally:
                proceed.set()
                result = await task
        self.assertTrue(result["success"])
        self.assertEqual(result["proof_state"]["goals"][1]["target"], "2 = 2")
        self.assertFalse(worker.active_revisions)
        unused.busy = 0

    async def test_workspace_budget_preserves_handles_and_evicts_released_state(self):
        unused = list(self.pool.peers.values())[1]
        unused.busy = 1
        for worker in self.workers:
            worker.max_contexts = 1
        retained = await self.root_handle()
        other, version = await self.second_client()
        with self.assertRaises(Failure) as error:
            await other("lean_run_at", version=version, line=3, character=2, text="skip")
        self.assertEqual(error.exception.code, "resourceExhausted")
        resumed = await self.call("lean_run_with", handle=retained, text="all_goals trivial")
        self.assertTrue(resumed["success"])
        await self.call("lean_release", handle=retained)
        await self.call("lean_release", handle=resumed["next_handle"])
        solved = await other("lean_run_at", version=version, line=3, character=2,
                             text="constructor <;> trivial")
        self.assertTrue(solved["success"])
        self.assertFalse(solved["proof_state"]["goals"])
        self.assertTrue(all(len(w.contexts) <= 1 and w.restarts == 0 for w in self.workers))
        unused.busy = 0

    async def test_new_file_and_unchanged_metadata_edits_are_transferred(self):
        await self.call("lean_run_at", version=self.version, line=3, character=2, text="skip")
        new = self.root / "Nested/New.lean"
        new.parent.mkdir()
        new.write_text("example : 3 = 3 := by\n  sorry\n")
        version = (await self.call("lean_sync", path="Nested/New.lean"))["version"]
        result = await self.call("lean_run_at", path="Nested/New.lean", version=version,
                                 line=1, character=2, text="rfl")
        self.assertTrue(result["success"])
        stamp = new.stat()
        new.write_text("example : 4 = 4 := by\n  sorry\n")
        os.utime(new, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        version = (await self.call("lean_sync", path="Nested/New.lean"))["version"]
        result = await self.call("lean_run_at", path="Nested/New.lean", version=version,
                                 line=1, character=2, text="skip")
        self.assertEqual(result["proof_state"]["goals"][0]["target"], "4 = 4")

    async def test_saved_imports_create_private_dependency_revisions(self):
        first = await self.call("lean_run_at_handle", version=self.version, line=2, character=0,
                                text="def savedHelper : Nat := helper")
        self.assertTrue(first["success"])
        (self.root / "Helper.lean").write_text("import Lean\ndef helper : Nat := 2\n")
        await self.call("lean_sync", path="Helper.lean")
        await self.call("lean_save", path="Helper.lean")
        # An existing continuation retains its imported environment until its own file is refreshed.
        continued = await self.call("lean_run_with", handle=first["next_handle"], text="#eval helper")
        self.assertEqual(continued["messages"][0]["text"], "1")
        await self.call("lean_release", handle=continued["next_handle"])
        await self.call("lean_release", handle=first["next_handle"])
        version = (await self.call("lean_refresh"))["version"]
        fresh = await self.call("lean_run_at", version=version, line=2, character=0, text="#eval helper")
        self.assertTrue(fresh["success"])
        self.assertEqual(fresh["messages"][0]["text"], "2")
        self.assertEqual((self.state / "prepared-worker-project/Helper.lean").read_text(),
                         "import Lean\ndef helper : Nat := 1\n")

    async def test_configuration_changes_still_require_a_new_environment(self):
        await self.root_handle()
        (self.root / "lakefile.toml").write_text('name = "different"\n')
        with self.assertRaises(Failure) as error:
            await self.call("lean_run_at", version=self.version, line=3, character=2, text="skip")
        self.assertEqual(error.exception.code, "contentModified")

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

    async def test_uncooperative_cancellation_retires_only_its_workspace(self):
        handle = await self.root_handle()
        owner = next(iter(self.pool.handles.values())).peer
        other_peer = next(p for p in self.pool.peers.values() if p is not owner)
        other_peer.busy = 1
        other, version = await self.second_client()
        retained = (await other("lean_run_at_handle", version=version, line=3, character=2,
                                text="constructor"))["next_handle"]
        other_peer.busy = 0
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
            while any(w.busy for w in self.workers):
                await asyncio.sleep(.01)
        self.assertEqual([w.generation for w in self.workers], generations)
        solved = await other("lean_run_with", handle=retained, text="all_goals trivial")
        self.assertTrue(solved["success"])
        await other("lean_release", handle=solved["next_handle"])
        await other("lean_release", handle=retained)

    async def test_cooperative_cancellation_and_timeout_preserve_other_clients_handles(self):
        handle = await self.root_handle()
        owner = next(h.peer for h in self.pool.handles.values())
        other = next(p for p in self.pool.peers.values() if p is not owner)
        worker = self.workers[self.pool.endpoints.index(owner.endpoint)]
        generation = worker.generation
        second = Mcp(list(self.native.command), str(self.root), 2)
        self.addAsyncCleanup(second.close)
        await second.start()

        async def call(name, **args):
            return await second.call(name, {
                "workspace": {"root": str(self.root)}, "path": "Proof.lean", **args})

        version = (await call("lean_sync"))["version"]
        other.busy = 1
        retained = (await call("lean_run_at_handle", version=version, line=3, character=2,
                               text="constructor"))["next_handle"]
        self.assertTrue(all(h.peer is owner for h in self.pool.handles.values()))
        other.busy = 0
        text = ("run_tac do\n  for _ in [0:1000] do\n"
                "    Lean.Core.checkInterrupted\n    let _ ← IO.sleep 10\n    pure ()")
        for ending in ("cancel", "timeout"):
            with self.subTest(ending=ending):
                worker.timeout = .3 if ending == "timeout" else 60
                task = asyncio.create_task(self.call("lean_run_with", handle=handle, text=text))
                async with asyncio.timeout(5):
                    while not worker.mcp.pending:
                        if task.done():
                            await task
                            self.fail("cooperative tactic finished before cancellation")
                        await asyncio.sleep(.01)
                if ending == "cancel":
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                else:
                    with self.assertRaises(Failure) as error:
                        await task
                    self.assertEqual(error.exception.code, "deadlineExceeded")
                async with asyncio.timeout(10):
                    while worker.busy or self.pool.jobs:
                        await asyncio.sleep(.01)
                self.assertEqual(worker.generation, generation)
                self.assertIsNone(worker.reset_task)
                self.assertEqual(len(worker.handles), 2)
                self.assertTrue(owner.info["ready"])
                worker.timeout = 60
                child = await call("lean_run_with", handle=retained, text="all_goals trivial")
                self.assertTrue(child["success"])
                await call("lean_release", handle=child["next_handle"])

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

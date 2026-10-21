#!/usr/bin/env python3
"""Real Beam proof snapshot transfer between independent Lean processes."""
import asyncio
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.beam_pool.lsp import Lsp, RpcError


class TransferTest(unittest.IsolatedAsyncioTestCase):
    async def pair(self, source_name="SimpleProof.lean", text="have transferredFact : True := trivial"):
        temporary = tempfile.TemporaryDirectory(prefix="beam-transfer-test-")
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "proof.snapshot"
        plugin = ROOT / ".lake/build/lib/libbeam_Beam_LSP.so"
        source = ROOT / "tests/scenario/docs" / source_name
        env = {**os.environ, "BEAM_SNAPSHOT_RUNTIME": "0" * 64}
        sender = await Lsp.start(ROOT, plugin, env=env)
        self.addAsyncCleanup(sender.close)
        doc = await sender.open(source)
        line = next(index for index, value in enumerate(source.read_text().splitlines()) if "sorry" in value or value == "  trivial")
        result = await sender.call("$/lean/runAt", {
            "textDocument": doc, "position": {"line": line, "character": 2},
            "text": text, "storeHandle": True,
        })
        self.assertTrue(result["success"], result)
        context = await sender.call("$/lean/beam/exportSnapshot", {
            "textDocument": doc, "handle": result["handle"], "path": str(path),
        }, timeout=30)
        await sender.close()
        receiver = await Lsp.start(ROOT, plugin, env=env)
        self.addAsyncCleanup(receiver.close)
        doc = await receiver.open(source)
        return receiver, doc, path, context["identity"]

    async def import_snapshot(self, receiver, doc, path, identity):
        return await receiver.call("$/lean/beam/importSnapshot", {
            "textDocument": doc, "path": str(path), "identity": identity,
        })

    async def release(self, receiver, doc, handle):
        await receiver.call("$/lean/releaseHandle", {"textDocument": doc, "handle": handle})

    async def resident(self, receiver, doc):
        context = await receiver.call("$/lean/beam/snapshotContext", {"textDocument": doc})
        return context["residentBytes"]

    async def test_parent_release_branches_and_reexport(self):
        receiver, doc, path, identity = await self.pair()
        parent = await self.import_snapshot(receiver, doc, path, identity)
        self.assertEqual(await self.resident(receiver, doc), path.stat().st_size)
        children = []
        for name in ("leftFact", "rightFact"):
            child = await receiver.call("$/lean/runWith", {
                "textDocument": doc, "handle": parent,
                "text": f"have {name} : True := transferredFact", "storeHandle": True,
            })
            self.assertTrue(child["success"], child)
            children.append(child["handle"])
        await self.release(receiver, doc, parent)
        self.assertGreater(await self.resident(receiver, doc), 0)
        isolated = await receiver.call("$/lean/runWith", {
            "textDocument": doc, "handle": children[1], "text": "exact leftFact",
        })
        self.assertFalse(isolated["success"])
        copied = path.with_name("descendant.snapshot")
        context = await receiver.call("$/lean/beam/exportSnapshot", {
            "textDocument": doc, "handle": children[0], "path": str(copied),
        })
        for child in children:
            await self.release(receiver, doc, child)
        self.assertEqual(await self.resident(receiver, doc), 0)
        descendant = await self.import_snapshot(receiver, doc, copied, context["identity"])
        solved = await receiver.call("$/lean/runWith", {
            "textDocument": doc, "handle": descendant, "text": "exact leftFact",
        })
        self.assertTrue(solved["success"], solved)
        self.assertEqual(solved["proofState"]["goals"], [])
        await self.release(receiver, doc, descendant)
        self.assertEqual(await self.resident(receiver, doc), 0)

    async def test_universe_notation_and_local_instance_survive(self):
        receiver, doc, path, identity = await self.pair("SnapshotTransfer.lean", "all_goals\n  " + "\n  ".join([
            "have saved : x = x := rfl", "have notationFact : answer = 7 := rfl",
            "letI : Inhabited α := ⟨Witness.value⟩",
        ]))
        handle = await self.import_snapshot(receiver, doc, path, identity)
        result = await receiver.call("$/lean/runWith", {
            "textDocument": doc, "handle": handle,
            "text": "all_goals\n  have inhabitant : α := default\n  have h : answer = 7 := notationFact\n  exact saved",
        })
        self.assertTrue(result["success"], result)
        self.assertEqual(result["proofState"]["goals"], [])
        await self.release(receiver, doc, handle)
        self.assertEqual(await self.resident(receiver, doc), 0)

    async def test_identity_and_version_rejected_before_read(self):
        receiver, doc, path, identity = await self.pair()
        for field, value in (("format", 99), ("compiler", "wrong"),
                             ("runtime", "1" * 64), ("source", "different source")):
            with self.subTest(field=field), self.assertRaisesRegex(RpcError, "incompatible"):
                await self.import_snapshot(receiver, doc, path.with_name("does-not-exist"),
                                           {**identity, field: value})
        with self.assertRaisesRegex(RpcError, "version mismatch"):
            await self.import_snapshot(receiver, {**doc, "version": 2}, path, identity)
        self.assertEqual(await self.resident(receiver, doc), 0)

    async def test_edit_invalidates_imported_handle_and_frees_mapping(self):
        receiver, doc, path, identity = await self.pair()
        handle = await self.import_snapshot(receiver, doc, path, identity)
        await receiver.notify("textDocument/didChange", {
            "textDocument": {**doc, "version": 2}, "contentChanges": [{"text": "example : True := by\n  trivial\n"}],
        })
        await receiver.call("$/beam/waitForDiagnostics", {"uri": doc["uri"], "version": 2})
        with self.assertRaises(RpcError):
            await receiver.call("$/lean/runWith", {
                "textDocument": {**doc, "version": 2}, "handle": handle, "text": "exact transferredFact",
            })
        self.assertEqual(await self.resident(receiver, {**doc, "version": 2}), 0)

    async def test_inflight_request_pins_released_parent(self):
        receiver, doc, path, identity = await self.pair("RunWithMixedConcurrencyProof.lean")
        handle = await self.import_snapshot(receiver, doc, path, identity)
        running = asyncio.create_task(receiver.call("$/lean/runWith", {
            "textDocument": doc, "handle": handle, "text": "mixed_sleep_exact",
        }))
        await asyncio.sleep(0.05)
        await self.release(receiver, doc, handle)
        self.assertGreater(await self.resident(receiver, doc), 0)
        result = await running
        self.assertTrue(result["success"], result)
        self.assertEqual(await self.resident(receiver, doc), 0)

    async def test_cancelled_request_drops_its_region_pin(self):
        receiver, doc, path, identity = await self.pair("RunWithMixedConcurrencyProof.lean")
        handle = await self.import_snapshot(receiver, doc, path, identity)
        running = asyncio.create_task(receiver.call("$/lean/runWith", {
            "textDocument": doc, "handle": handle, "text": "mixed_sleep_exact",
        }))
        await asyncio.sleep(0.05)
        running.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await running
        await self.release(receiver, doc, handle)
        self.assertEqual(await self.resident(receiver, doc), 0)

    async def test_sender_can_exit(self):
        plugin = ROOT / ".lake/build/lib/libbeam_Beam_LSP.so"
        source = ROOT / "tests/scenario/docs/SimpleProof.lean"
        with tempfile.TemporaryDirectory(prefix="beam-transfer-") as directory:
            path = Path(directory) / "source.snapshot"
            copied = Path(directory) / "copied.snapshot"
            env = {**os.environ, "BEAM_SNAPSHOT_RUNTIME": "0" * 64}
            sender = await Lsp.start(ROOT, plugin, env=env)
            try:
                doc = await sender.open(source)
                result = await sender.call("$/lean/runAt", {
                    "textDocument": doc, "position": {"line": 1, "character": 2},
                    "text": "have transferredFact : True := trivial", "storeHandle": True,
                })
                self.assertTrue(result["success"], result)
                identity = await sender.call("$/lean/beam/exportSnapshot", {
                    "textDocument": doc, "handle": result["handle"], "path": str(path),
                }, timeout=30)
                shutil.copyfile(path, copied)
                path.unlink()
                print(f"snapshot bytes: {copied.stat().st_size}", flush=True)
            finally:
                await sender.close()
            receiver = await Lsp.start(ROOT, plugin, env=env)
            try:
                doc = await receiver.open(source)
                handle = await receiver.call("$/lean/beam/importSnapshot", {
                    "textDocument": doc, "path": str(copied), "identity": identity["identity"],
                })
                result = await receiver.call("$/lean/runWith", {
                    "textDocument": doc, "handle": handle, "text": "exact transferredFact",
                })
                self.assertTrue(result["success"], result)
                self.assertEqual(result["proofState"]["goals"], [])
                await receiver.call("$/lean/releaseHandle", {"textDocument": doc, "handle": handle})
            finally:
                await receiver.close()


if __name__ == "__main__":
    unittest.main()

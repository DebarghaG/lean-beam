#!/usr/bin/env python3
"""Docker integration: needs beam-pool:local and free localhost port 9000."""

import asyncio
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.beam_pool.mcp import Mcp
from scripts.beam_pool.protocol import Failure
from scripts.beam_pool.transport import rpc


async def main():
    token = uuid.uuid4().hex
    env = dict(os.environ, BEAM_POOL_TOKEN=token)
    project = "beam-pool-smoke-" + uuid.uuid4().hex[:8]
    command = ["docker", "compose", "-p", project, "-f", "deploy/beam-pool/compose.yaml"]
    temporary = tempfile.TemporaryDirectory(prefix="beam-pool-compose-client-")
    state = Path(temporary.name)
    native = second = None

    def compose(*args):
        subprocess.run([*command, *args], env=env, check=True, stdout=subprocess.DEVNULL)

    async def operator(*args):
        proc = await asyncio.create_subprocess_exec(
            str(REPO / "scripts/beam-pool"), *args, env=env, stdout=asyncio.subprocess.PIPE)
        out, _ = await proc.communicate()
        if proc.returncode:
            raise RuntimeError(f"beam-pool exited with {proc.returncode}")
        return json.loads(out)

    async def ready(count):
        async with asyncio.timeout(120):
            while True:
                try:
                    info = await rpc(("127.0.0.1", 9000), token, "info", {}, timeout=2)
                    if sum(w.get("ready", False) for w in info["workers"]) == count:
                        return
                except Failure:
                    pass
                await asyncio.sleep(.2)

    try:
        await asyncio.to_thread(compose, "up", "-d", "--scale", "worker=2")
        await ready(2)
        status = await operator("status")
        assert sum(w.get("ready", False) for w in status["workers"]) == 2
        local = state / "project"
        shutil.copytree(REPO / "tests/pool_project", local,
                        ignore=shutil.ignore_patterns(".lake", ".beam"))
        config = state / "pool.json"
        await operator("attach", "--root", str(local), "--config", str(config))
        native = Mcp([
            "env", f"BEAM_LEAN_POOL_CONFIG={config}", f"BEAM_POOL_TOKEN={token}",
            str(REPO / ".lake/build/bin/lean-beam-mcp"), "--lean-cmd", shutil.which("lean"),
            "--lean-plugin", str(REPO / ".lake/build/lib/libbeam_Beam_LSP.so"),
        ], str(local), 2)
        await native.start()

        async def call(name, **args):
            return await native.call(name, {
                "workspace": {"root": str(local)}, "path": "Workload.lean", **args})

        version = (await call("lean_sync"))["version"]
        source = (local / "Workload.lean").read_text().splitlines()
        proof_line = source.index("  constructor <;> trivial")
        arithmetic_line = source.index("  omega")
        root = (await call("lean_run_at_handle", version=version, line=proof_line,
                           character=2, text="constructor"))["next_handle"]
        await asyncio.to_thread(compose, "up", "-d", "--scale", "worker=3")
        await ready(3)
        solved = await call("lean_run_with_linear", handle=root, text="all_goals trivial")
        assert solved["success"] and not solved["proof_state"]["goals"]
        if solved["next_handle"]:
            await call("lean_release", handle=solved["next_handle"])
        await asyncio.to_thread(compose, "up", "-d", "--scale", "worker=1")
        await ready(1)
        solved = await call("lean_run_at", version=version, line=arithmetic_line,
                            character=2, text="omega")
        assert solved["success"] and not solved["proof_state"]["goals"]
        second_root = state / "second-project"
        shutil.copytree(local, second_root, ignore=shutil.ignore_patterns(".lake", ".beam"))
        await operator("attach", "--root", str(second_root), "--config", str(config))
        second = Mcp(list(native.command), str(second_root), 2)
        await second.start()
        async def other(name, **args):
            return await second.call(name, {"workspace": {"root": str(second_root)},
                                             "path": "Workload.lean", **args})
        other_version = (await other("lean_sync"))["version"]
        retained = (await other("lean_run_at_handle", version=other_version, line=proof_line,
                                 character=2, text="constructor"))["next_handle"]
        edited = source[:2] + ["def poolEditedMarker : Nat := 42", ""] + source[2:]
        started = time.perf_counter()
        (local / "Workload.lean").write_text("\n".join(edited) + "\n")
        version = (await call("lean_sync"))["version"]
        proof_line += 2
        result = await call("lean_run_at", version=version, line=proof_line, character=2,
                            text="have h : poolEditedMarker = 42 := rfl")
        assert result["success"], result
        edit_ms = (time.perf_counter() - started) * 1000
        resumed = await other("lean_run_with", handle=retained, text="all_goals trivial")
        assert resumed["success"] and not resumed["proof_state"]["goals"], resumed
        await other("lean_release", handle=retained)
        await other("lean_release", handle=resumed["next_handle"])
        warm_ms = []
        for _ in range(12):
            started = time.perf_counter()
            result = await call("lean_run_at", version=version, line=proof_line, character=2,
                                text="constructor <;> trivial")
            assert result["success"] and not result["proof_state"]["goals"], result
            warm_ms.append((time.perf_counter() - started) * 1000)
        status = await operator("status")
        assert status["handles"] == 0 and status["queued"] == 0 and status["running"] == 0, status
        print(json.dumps({"test": "pool-compose", "workers": [2, 3, 1], "cpus_per_worker": 1,
            "edit_sync_probe_ms": round(edit_ms, 2), "warm_probe_samples": len(warm_ms),
            "warm_probe_p50_ms": round(statistics.median(warm_ms), 2),
            "warm_probe_p95_ms": round(sorted(warm_ms)[int(len(warm_ms) * .95)], 2),
            "other_client_continuation": "passed", "completed": status["completed"]}))
    finally:
        try:
            if native:
                await native.close()
            if second:
                await second.close()
        finally:
            await asyncio.to_thread(compose, "down", "--timeout", "15")
            temporary.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

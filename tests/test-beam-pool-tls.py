#!/usr/bin/env python3
"""Two real workers behind Traefik TLS/SNI and stunnel. Requires Docker and openssl.

Use --client against a dedicated, otherwise idle two-worker pool through a TLS adapter.
BEAM_POOL_TEST_ENDPOINT defaults to 127.0.0.1:9000; BEAM_POOL_TEST_PROJECT to /project.
"""

import asyncio
import contextlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.beam_pool.attach import attach
from scripts.beam_pool.mcp import Mcp
from scripts.beam_pool.protocol import Failure
from scripts.beam_pool.transport import rpc


async def client():
    host, port = os.environ.get("BEAM_POOL_TEST_ENDPOINT", "127.0.0.1:9000").rsplit(":", 1)
    endpoint = host, int(port)
    token = os.environ["BEAM_POOL_TOKEN"]
    prepared = Path(os.environ.get("BEAM_POOL_TEST_PROJECT", "/project"))
    processes, handles, tasks = [], [], []

    async def status():
        return await rpc(endpoint, token, "info", {}, timeout=5)

    async def until(predicate, seconds=10):
        async with asyncio.timeout(seconds):
            while True:
                info = await status()
                if predicate(info):
                    return info
                for task in tasks:
                    if task.done() and not task.cancelled():
                        task.result()
                        raise AssertionError("long-running request finished prematurely")
                await asyncio.sleep(.05)

    def solved(result):
        assert result["success"] and not result["proof_state"]["goals"], result

    with tempfile.TemporaryDirectory(prefix="beam-tls-client-") as directory:
        state = Path(directory)
        config = state / "pool.json"
        try:
            async with asyncio.timeout(120):
                while True:
                    try:
                        info = await status()
                        if sum(w.get("ready", False) for w in info["workers"]) == 2:
                            break
                    except (Failure, OSError):
                        pass
                    await asyncio.sleep(.2)
            assert info["running"] == info["queued"] == info["handles"] == 0, info
            try:
                await rpc(endpoint, "incorrect-pool-capability", "info", {})
                raise AssertionError("wrong capability accepted")
            except Failure as error:
                assert error.code == "unauthorized", error
            calls, versions, roots = [], [], []
            source = (prepared / "Workload.lean").read_text()
            line = source.splitlines().index("  constructor <;> trivial")
            for number in range(2):
                root = state / f"agent{number}"
                roots.append(root)
                shutil.copytree(prepared, root, ignore=shutil.ignore_patterns(".beam"))
                await attach(root, endpoint, token, config)
                mcp = Mcp(["env", f"BEAM_LEAN_POOL_CONFIG={config}", f"BEAM_POOL_TOKEN={token}",
                    str(REPO / ".lake/build/bin/lean-beam-mcp"),
                    "--lean-cmd", shutil.which("lean"), "--lean-plugin",
                    str(REPO / ".lake/build/lib/libbeam_Beam_LSP.so")], str(root), 2)
                processes.append(mcp)
                await mcp.start()

                async def call(name, _mcp=mcp, _root=root, **args):
                    return await _mcp.call(name, {"workspace": {"root": str(_root)},
                                                 "path": "Workload.lean", **args})

                calls.append(call)
                versions.append((await call("lean_sync"))["version"])
                handle = (await call("lean_run_at_handle", version=versions[-1],
                                    line=line, character=2, text="constructor"))["next_handle"]
                handles.append((call, handle))
            failed = await calls[0]("lean_run_at", version=versions[0], line=line,
                                    character=2, text="exact False.elim (by assumption)")
            assert not failed["success"], failed
            for result in await asyncio.gather(*(calls[i % 2]("lean_run_at", version=versions[i % 2],
                    line=line, character=2, text="constructor <;> trivial") for i in range(8))):
                solved(result)

            # Occupy both workers, then cancel a queued linear continuation.
            tactic = ("run_tac do\n  for _ in [0:6000] do\n"
                      "    Lean.Core.checkInterrupted\n    let _ ← IO.sleep 10\n    pure ()")
            for i in range(2):
                tasks.append(asyncio.create_task(calls[i]("lean_run_at", version=versions[i],
                    line=line, character=2, text=tactic)))
            await until(lambda s: s["running"] == 2)
            queued = asyncio.create_task(calls[0]("lean_run_with_linear", handle=handles[0][1], text="skip"))
            tasks.append(queued)
            await until(lambda s: s["queued"] == 1)
            queued.cancel()
            await asyncio.gather(queued, return_exceptions=True)
            await until(lambda s: s["queued"] == 0)
            started = time.monotonic()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await until(lambda s: s["running"] == 0, seconds=8)
            cancel_seconds = time.monotonic() - started
            tasks.clear()
            for call, handle in handles:
                result = await call("lean_run_with", handle=handle, text="all_goals trivial")
                solved(result)
                await call("lean_release", handle=result["next_handle"])

            # Close before editing, then verify the other agent's retained branch still works.
            await calls[0]("lean_release", handle=handles[0][1])
            handles.pop(0)
            await calls[0]("lean_close")
            (roots[0] / "Workload.lean").write_text(source.replace("True ∧ True", "True ∧ (2 = 2)"))
            version = (await calls[0]("lean_sync"))["version"]
            solved(await calls[0]("lean_run_at", version=version, line=line, character=2,
                                  text="constructor <;> trivial"))
            call, handle = handles[0]
            result = await call("lean_run_with", handle=handle, text="all_goals trivial")
            solved(result)
            await call("lean_release", handle=result["next_handle"])
            await call("lean_release", handle=handle)
            handles.clear()
            final = await until(lambda s: s["running"] == s["queued"] == s["handles"] == 0)
            print(json.dumps({"test": "pool-tls", "workers": 2, "agents": 2,
                "parallel_proofs": 8, "queued_and_running_cancellation": "passed",
                "cancel_drain_seconds": round(cancel_seconds, 3),
                "edit_isolation": "passed", "remaining_handles": final["handles"]}), flush=True)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for call, handle in handles:
                with contextlib.suppress(Failure):
                    await call("lean_release", handle=handle)
            for process in processes:
                await process.close()


def local():
    image = os.environ.get("BEAM_POOL_IMAGE", "beam-pool:local")
    project = "beam-tls-" + secrets.token_hex(4)
    agent_image = project + "-agent"
    with tempfile.TemporaryDirectory(prefix="beam-tls-") as directory:
        state = Path(directory)
        state.chmod(0o755)  # Non-root containers need to traverse the fixture directory.
        token = secrets.token_hex(32)
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(state / "server.key"), "-out", str(state / "server.crt"),
            "-subj", "/CN=gateway.test", "-addext", "subjectAltName=DNS:gateway.test"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        routes = {"tls": {"certificates": [{"certFile": "/config/server.crt", "keyFile": "/config/server.key"}]},
            "tcp": {"routers": {"beam": {"rule": "HostSNI(`gateway.test`)",
                "entryPoints": ["web"], "service": "beam", "tls": {}}},
                "services": {"beam": {"loadBalancer": {"servers": [{"address": "gateway:9000"}]}}}}}
        (state / "routes.json").write_text(json.dumps(routes))
        config = (REPO / "deploy/beam-pool/stunnel.conf").read_text().replace("GATEWAY_HOST", "gateway.test")
        config = config.replace("connect = gateway.test:443", "connect = ingress:443")
        config = config.replace("/etc/ssl/certs/ca-certificates.crt", "/config/server.crt")
        (state / "stunnel.conf").write_text(config)
        dockerfile = f"FROM {image}\nUSER root\nRUN apt-get update && apt-get install -y --no-install-recommends stunnel4 && rm -rf /var/lib/apt/lists/*\nUSER 1000:1000\n"
        subprocess.run(["docker", "build", "-t", agent_image, "-"], input=dockerfile.encode(), check=True)
        worker = {"image": image, "cpus": 1, "mem_limit": "2g", "init": True,
                  "environment": {"BEAM_POOL_TOKEN": token}, "networks": ["backend"]}
        services = {"worker1": worker, "worker2": worker,
            "gateway": {**worker, "mem_limit": "1g", "command": ["serve", "--listen", "0.0.0.0:9000",
                "--worker", "worker1:9001", "--worker", "worker2:9001"]},
            "ingress": {"image": "traefik:v3.6.2", "networks": ["backend", "frontend"],
                "volumes": [f"{state}:/config:ro"], "command": ["--entrypoints.web.address=:443",
                    "--providers.file.filename=/config/routes.json", "--log.level=ERROR"]},
            "agent": {"image": agent_image, "networks": ["frontend"],
                "environment": {"BEAM_POOL_TOKEN": token}, "volumes": [f"{state}:/config:ro",
                    f"{Path(__file__).resolve()}:/opt/beam/tests/test-beam-pool-tls.py:ro"],
                "entrypoint": ["sh", "-c"], "command": ["stunnel /config/stunnel.conf & "
                    "exec python3 /opt/beam/tests/test-beam-pool-tls.py --client"]}}
        compose_file = state / "compose.json"
        compose_file.write_text(json.dumps({"services": services,
            "networks": {"backend": {"internal": True}, "frontend": {}}}))
        compose_file.chmod(0o600)
        compose = ["docker", "compose", "-p", project, "-f", str(compose_file)]
        try:
            subprocess.run([*compose, "up", "-d", "worker1", "worker2", "gateway", "ingress"], check=True)
            subprocess.run([*compose, "run", "--rm", "agent"], check=True, timeout=300)
        finally:
            subprocess.run([*compose, "down", "--timeout", "15"], check=True)
            subprocess.run(["docker", "image", "rm", agent_image], check=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["--client"]:
        asyncio.run(client())
    else:
        local()

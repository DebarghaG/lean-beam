#!/usr/bin/env python3
"""Real Beam workers over TCP, optionally in two independently built Docker images.

Run under `lake env`. Timings are a synthetic scheduling experiment, not a prover
benchmark. Both modes execute the same tactics and require solved proof states.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
from pathlib import Path
import secrets
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.beam_pool.scheduler import Pool
from scripts.beam_pool.wire import Peer, RemoteError
from scripts.beam_pool.worker import Worker

SOURCE = "tests/scenario/docs/SimpleProof.lean"


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


class Cluster:
    def __init__(self, docker, count):
        self.docker, self.count = docker, count
        self.token = secrets.token_hex(32)
        self.containers, self.workers, self.servers, self.peers = [], [], [], []

    async def start(self):
        if self.docker:
            prefix = command("lean", "--print-prefix")
            for index in range(self.count):
                name = "beam-snapshot-test-" + uuid.uuid4().hex[:12]
                image = f"beam-snapshot-worker:{'a' if index % 2 == 0 else 'b'}"
                project = "/project" if index % 2 == 0 else "/workspace/project"
                sdk = "/opt/lean"
                subprocess.run([
                    "docker", "run", "--rm", "-d", "--name", name,
                    "--user", f"{os.getuid()}:{os.getgid()}",
                    "--cpus", "2", "--memory", "3g", "--pids-limit", "256",
                    "-p", "127.0.0.1::8765", "-e", "BEAM_POOL_TOKEN",
                    "-e", f"PATH={sdk}/bin:/usr/local/bin:/usr/bin:/bin",
                    "-e", f"LEAN_PATH={project}/.lake/build/lib/lean",
                    "-e", f"LD_LIBRARY_PATH=/opt/beam/lib:{sdk}/lib/lean:{sdk}/lib",
                    "--workdir", project,
                    "--mount", f"type=bind,src={ROOT},dst={project},readonly",
                    "--mount", f"type=bind,src={prefix},dst={sdk},readonly",
                    "--mount", f"type=bind,src={ROOT / '.lake/build/lib'},dst=/opt/beam/lib,readonly",
                    image, "--project", project, "--plugin", "/opt/beam/lib/libbeam_Beam_LSP.so",
                    "--host", "0.0.0.0", "--port", "8765",
                ], env={**os.environ, "BEAM_POOL_TOKEN": self.token},
                    check=True, stdout=subprocess.DEVNULL)
                self.containers.append(name)
                ports = json.loads(command("docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", name))
                port = int(ports["8765/tcp"][0]["HostPort"])
                self.peers.append(Peer("127.0.0.1", port, self.token))
            for peer in self.peers:
                deadline = time.monotonic() + 90
                while True:
                    try:
                        await peer.call("status")
                        break
                    except (ConnectionError, OSError, asyncio.IncompleteReadError):
                        peer.failed = False  # Startup probing precedes pool admission.
                        if time.monotonic() >= deadline:
                            for name in self.containers:
                                print(command("docker", "logs", name), file=sys.stderr)
                            raise
                        await asyncio.sleep(0.2)
        else:
            for _ in range(self.count):
                worker = Worker(ROOT, ROOT / ".lake/build/lib/libbeam_Beam_LSP.so", self.token)
                self.workers.append(worker)
                await worker.start()
                server = await asyncio.start_server(worker.connected, "127.0.0.1", 0)
                self.servers.append(server)
                self.peers.append(Peer("127.0.0.1", server.sockets[0].getsockname()[1], self.token))
        return self

    async def close(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        await asyncio.gather(*(worker.close() for worker in self.workers))
        for name in self.containers:
            subprocess.run(["docker", "stop", "--time", "10", name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


async def transfer_checks(peers):
    sender, receiver = peers[:2]
    result, _ = await sender.call("runAt", {
        "path": SOURCE, "position": {"line": 1, "character": 2},
        "text": "have transferredFact : True := trivial", "store": True,
    })
    assert result["success"], result
    parent = result["handle"]
    manifest, data = await sender.call("export", {"path": SOURCE, "handle": parent})
    # Neither a mislabeled native artifact nor corrupted bytes may reach Lean's unsafe reader.
    for bad_manifest, bad_data in (({**manifest, "sha256": "0" * 64}, data),
                                   (manifest, data[:-1] + bytes([data[-1] ^ 1]))):
        try:
            await receiver.call("import", {"path": SOURCE, "manifest": bad_manifest}, bad_data)
            raise AssertionError("corrupt snapshot accepted")
        except RemoteError as error:
            assert "checksum" in str(error), error
    for field in ("dependencies", "libraries", "runtime"):
        bad = copy.deepcopy(manifest)
        if field == "runtime":
            bad["identity"]["runtime"] = "0" * 64
        elif field == "libraries":
            bad["libraries"][0][1] = "0" * 64
        else:
            bad["dependencies"][0] = "0" * 64
        try:
            await receiver.call("import", {"path": SOURCE, "manifest": bad}, data)
            raise AssertionError(f"mismatched {field} accepted")
        except RemoteError as error:
            assert "differ" in str(error), error
    imported, _ = await receiver.call("import", {"path": SOURCE, "manifest": manifest}, data)
    await sender.call("release", {"path": SOURCE, "handle": parent})
    result, _ = await receiver.call("runWith", {
        "path": SOURCE, "handle": imported["handle"], "text": "have childFact : True := transferredFact", "store": True,
    })
    assert result["success"], result
    child = result["handle"]
    await receiver.call("release", {"path": SOURCE, "handle": imported["handle"]})
    # Re-export an imported descendant from a different project installation path.
    returned_manifest, returned_data = await receiver.call("export", {"path": SOURCE, "handle": child})
    returned, _ = await sender.call("import", {"path": SOURCE, "manifest": returned_manifest}, returned_data)
    await receiver.call("release", {"path": SOURCE, "handle": child})
    result, _ = await sender.call("runWith", {
        "path": SOURCE, "handle": returned["handle"], "text": "exact childFact",
    })
    assert result["success"] and result["proofState"]["goals"] == [], result
    await sender.call("release", {"path": SOURCE, "handle": returned["handle"]})
    context, _ = await receiver.call("open", {"path": SOURCE})
    assert context["residentBytes"] == 0, context["residentBytes"]
    return len(data)


async def workload(peers, *, stealing, jobs, steps):
    pool = await Pool(peers, stealing=stealing, seed=17).start()
    try:
        result = await pool.run_at(SOURCE, 1, 2, "have transferredFact : True := trivial")
        assert result["success"], result
        parent = result["handle"]
        # Real tactic elaboration and kernel checking, with no artificial sleep.
        tactic = "all_goals\n" + "  have h : (1 : Nat) = 1 := rfl\n" * steps + "  exact transferredFact"
        started = time.monotonic()
        calibration = await pool.run_with(parent, tactic)
        estimate = time.monotonic() - started
        assert calibration["success"] and calibration["proofState"]["goals"] == [], calibration
        started = time.monotonic()
        results = await asyncio.gather(*(pool.submit(parent, tactic, estimate=estimate) for _ in range(jobs)))
        elapsed = time.monotonic() - started
        assert all(result["success"] and result["proofState"]["goals"] == [] for result in results), results
        await pool.release(parent)
        return {"stealing": stealing, "jobs": jobs, "steps": steps,
                "seconds": elapsed, "calibratedServiceSeconds": estimate, **pool.metrics}
    finally:
        await pool.close()


async def chain_checks(peers):
    pool = await Pool(peers).start()
    try:
        parent = (await pool.run_at(SOURCE, 1, 2, "have transferredFact : True := trivial"))["handle"]
        for _ in range(12):
            result = await pool.run_with(parent, "skip", store=True)
            assert result["success"], result
            await pool.release(parent)
            parent = result["handle"]
        solved = await pool.run_with(parent, "exact transferredFact")
        assert solved["success"] and solved["proofState"]["goals"] == [], solved
        await pool.release(parent)
        assert pool.metrics["exports"] == 0, pool.metrics
    finally:
        await pool.close()


async def main(args):
    cluster = Cluster(args.docker, args.workers)
    try:
        await cluster.start()
        stale = Peer(cluster.peers[0].host, cluster.peers[0].port, cluster.token)
        stale.generation = "previous-worker-generation"
        try:
            await stale.call("status")
            raise AssertionError("stale generation accepted")
        except ConnectionError:
            assert stale.failed
        size = await transfer_checks(cluster.peers)
        await chain_checks(cluster.peers)
        results = []
        for trial in range(args.repeat):
            for stealing in ((False, True) if trial % 2 == 0 else (True, False)):
                result = await workload(cluster.peers, stealing=stealing, jobs=args.jobs, steps=args.steps)
                result["trial"] = trial
                results.append(result)
                print(json.dumps(result), flush=True)
        if args.require_steal:
            assert all(result["imports"] > 0 for result in results if result["stealing"]), "workload did not amortize a transfer"
        for peer in cluster.peers:
            status, _ = await peer.call("status")
            assert status["handles"] == 0, status
        medians = {enabled: statistics.median(result["seconds"] for result in results if result["stealing"] == enabled)
                   for enabled in (False, True)}
        status, _ = await cluster.peers[0].call("status")
        report = {"docker": args.docker, "workers": args.workers, "snapshotBytes": size,
                  "leanVersion": command("lean", "--version"), "runtime": status["runtime"],
                  "runs": results, "speedup": medians[False] / medians[True],
                  "pinnedMedianSeconds": medians[False], "stealingMedianSeconds": medians[True]}
        if args.report:
            Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report), flush=True)
    finally:
        await cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", action="store_true")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--jobs", type=int, default=24)
    parser.add_argument("--steps", type=int, default=500)
    parser.add_argument("--require-steal", action="store_true")
    parser.add_argument("--report")
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    if args.workers < 2:
        parser.error("integration checks require at least two workers")
    asyncio.run(main(args))

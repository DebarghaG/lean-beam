#!/usr/bin/env python3
"""Bounded replay of beam-bench tasks through two Docker snapshot workers.

Read-only project mounts preserve the corpus. This checks real continuation
semantics and scheduling decisions; it is not a statistically powered benchmark.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.beam_pool.scheduler import Pool
from scripts.beam_pool.wire import Peer


def command(*args, cwd=None, **kwargs):
    return subprocess.check_output(args, cwd=cwd, text=True, **kwargs).strip()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def outcome(result):
    return {"success": result["success"], "goals": result.get("proofState", {}).get("goals")}


class Cluster:
    def __init__(self, project, runtime):
        self.project, self.runtime = project.resolve(), runtime.resolve()
        self.names, self.peers = [], []
        self.token = secrets.token_hex(32)

    async def start(self):
        sdk = command("lean", "--print-prefix", cwd=self.project)
        environment = json.loads(command("lake", "env", "python3", "-c",
                                        "import os,json; print(json.dumps(dict(os.environ)))",
                                        cwd=self.project))
        def mapped(value):
            return value.replace(str(self.project), "/project").replace(sdk, "/opt/lean")
        for variant in ("a", "b"):
            name = "beam-corpus-" + uuid.uuid4().hex[:12]
            args = ["docker", "run", "--rm", "-d", "--name", name,
                    "--user", f"{os.getuid()}:{os.getgid()}",
                    "--cpus", "2", "--memory", "8g", "--pids-limit", "256",
                    "-p", "127.0.0.1::8765", "-e", "BEAM_POOL_TOKEN",
                    "-e", "LEAN_NUM_THREADS=4",
                    "-e", "PATH=/opt/lean/bin:/usr/local/bin:/usr/bin:/bin"]
            for key in ("LEAN_PATH", "LEAN_SRC_PATH", "LD_LIBRARY_PATH"):
                value = mapped(environment.get(key, ""))
                if key == "LD_LIBRARY_PATH":
                    value = "/opt/beam/lib:" + value
                args += ["-e", f"{key}={value}"]
            for source, target in ((self.project, "/project"), (sdk, "/opt/lean"),
                                   (self.runtime / ".lake/build/lib", "/opt/beam/lib")):
                args += ["--mount", f"type=bind,src={source},dst={target},readonly"]
            args += [f"beam-snapshot-worker:{variant}", "--project", "/project",
                     "--plugin", "/opt/beam/lib/libbeam_Beam_LSP.so",
                     "--host", "0.0.0.0", "--port", "8765", "--timeout", "120"]
            command(*args, env={**os.environ, "BEAM_POOL_TOKEN": self.token})
            self.names.append(name)
            ports = json.loads(command("docker", "inspect", "--format",
                                       "{{json .NetworkSettings.Ports}}", name))
            self.peers.append(Peer("127.0.0.1", int(ports["8765/tcp"][0]["HostPort"]), self.token))
        for peer in self.peers:
            deadline = time.monotonic() + 120
            while True:
                try:
                    await peer.call("status")
                    break
                except (ConnectionError, OSError, asyncio.IncompleteReadError):
                    peer.failed = False  # Startup probing, before pool admission.
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.2)

    def close(self, output):
        for index, name in enumerate(self.names):
            logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
            (output / f"worker-{index}.log").write_text(logs.stdout + logs.stderr)
            subprocess.run(["docker", "stop", "--time", "5", name],
                           stdout=subprocess.DEVNULL, check=False)


async def call(peer, operation, task, **params):
    result, _ = await peer.call(operation, {"path": task["file"], **params})
    return result


async def migration_check(peers, task):
    print(f"migration: {task['theorem']}", flush=True)
    owner = 0
    initial = await call(peers[owner], "runAt", task, text="skip", store=True,
                         position={"line": task["proof_start"], "character": task["indent"]})
    assert initial["success"] and initial.get("handle"), initial
    parent = initial["handle"]
    print("  initial proof handle ready", flush=True)
    records = []
    for index, step in enumerate(task["steps"]):
        destination = 1 - owner
        started = time.monotonic()
        print(f"  step {index + 1}: exporting", flush=True)
        async with asyncio.timeout(30):
            manifest, data = await peers[owner].call("export", {"path": task["file"], "handle": parent})
        print(f"  step {index + 1}: importing {len(data)} bytes", flush=True)
        imported, _ = await peers[destination].call("import", {
            "path": task["file"], "manifest": manifest,
        }, data)
        copy_seconds = time.monotonic() - started
        comparisons = []
        for label, text in (("original", step["text"]), ("simp", "simp"), ("linarith", "linarith")):
            local, remote = await asyncio.gather(
                call(peers[owner], "runWith", task, handle=parent, text=text),
                call(peers[destination], "runWith", task, handle=imported["handle"], text=text))
            assert outcome(local) == outcome(remote), (task["theorem"], index, label, local, remote)
            comparisons.append({"candidate": label, "outcome": outcome(local),
                                "localSeconds": local["serviceSeconds"],
                                "importedSeconds": remote["serviceSeconds"]})
        advanced = await call(peers[destination], "runWith", task, handle=imported["handle"],
                              text=step["text"], store=True)
        assert advanced["success"], advanced
        await call(peers[owner], "release", task, handle=parent)
        await call(peers[destination], "release", task, handle=imported["handle"])
        parent, owner = advanced["handle"], destination
        records.append({"step": index, "snapshotBytes": len(data), "copySeconds": copy_seconds,
                        "comparisons": comparisons})
        print(f"  step {index + 1}: candidate agreement, {len(data)} bytes transferred", flush=True)
    assert advanced["proofState"]["goals"] == [], advanced
    await call(peers[owner], "release", task, handle=parent)
    for peer in peers:
        context = await call(peer, "open", task)
        assert context["residentBytes"] == 0, context["residentBytes"]
    return {"theorem": task["theorem"], "solved": True, "steps": records}


async def scheduled_replay(peers, tasks, stealing):
    pool = await Pool(peers, stealing=stealing, seed=17).start()
    records = []
    started = time.monotonic()
    try:
        for task in tasks:
            print(f"scheduler stealing={stealing}: {task['theorem']}", flush=True)
            result = await pool.run_at(task["file"], task["proof_start"], task["indent"], "skip")
            assert result["success"], result
            parent = result["handle"]
            steps = []
            for step in task["steps"]:
                results = await asyncio.gather(
                    pool.submit(parent, step["text"], store=True),
                    pool.submit(parent, "simp"), pool.submit(parent, "linarith"))
                assert results[0]["success"], results[0]
                steps.append([outcome(result) for result in results])
                await pool.release(parent)
                parent = results[0]["handle"]
            assert results[0]["proofState"]["goals"] == [], results[0]
            await pool.release(parent)
            records.append({"theorem": task["theorem"], "solved": True, "steps": steps})
        return {"stealing": stealing, "seconds": time.monotonic() - started,
                "metrics": dict(pool.metrics), "tasks": records}
    finally:
        await pool.close()


async def main(args):
    corpus, project, runtime = args.corpus.resolve(), args.project.resolve(), args.runtime.resolve()
    tasks = [task for task in json.loads(corpus.read_text())
             if task["project"] == project.name and len(task["steps"]) <= args.max_steps][:args.limit]
    assert tasks, "No matching corpus tasks"
    args.output.mkdir(parents=True, exist_ok=False)
    sources = {task["file"]: digest(project / task["file"]) for task in tasks}
    cluster = Cluster(project, runtime)
    report = {"beamCommit": command("git", "rev-parse", "HEAD", cwd=ROOT),
              "runnerSha256": digest(__file__),
              "workerSourcesSha256": {str(path.relative_to(ROOT)): digest(path)
                                      for path in sorted((ROOT / "scripts/beam_pool").glob("*.py"))},
              "projectCommit": command("git", "rev-parse", "HEAD", cwd=project),
              "toolchain": (project / "lean-toolchain").read_text().strip(),
              "corpusSha256": digest(corpus), "sources": sources,
              "pluginSha256": digest(runtime / ".lake/build/lib/libbeam_Beam_LSP.so"),
              "workers": 2, "cpusPerWorker": 2, "memoryGiBPerWorker": 8,
              "images": [command("docker", "image", "inspect", "--format", "{{.Id}}",
                                 f"beam-snapshot-worker:{variant}") for variant in ("a", "b")],
              "selectedTasks": [task["theorem"] for task in tasks],
              "phases": args.phases,
              "migration": [], "scheduled": []}
    try:
        await cluster.start()
        report["runtime"] = (await cluster.peers[0].call("status"))[0]["runtime"]
        opened = time.monotonic()
        for name in sources:
            print(f"open: {name}", flush=True)
            await asyncio.gather(*(peer.call("open", {"path": name}) for peer in cluster.peers))
        report["openSeconds"] = time.monotonic() - opened
        if "pinned" in args.phases:
            report["scheduled"].append(await scheduled_replay(cluster.peers, tasks, False))
        if "migration" in args.phases:
            for task in tasks:
                report["migration"].append(await migration_check(cluster.peers, task))
        if "stealing" in args.phases:
            report["scheduled"].append(await scheduled_replay(cluster.peers, tasks, True))
        if len(report["scheduled"]) == 2:
            assert report["scheduled"][0]["tasks"] == report["scheduled"][1]["tasks"]
        if report["scheduled"]:
            for migration, scheduled in zip(report["migration"], report["scheduled"][0]["tasks"]):
                assert [[candidate["outcome"] for candidate in step["comparisons"]]
                        for step in migration["steps"]] == scheduled["steps"]
        for peer in cluster.peers:
            status, _ = await peer.call("status")
            assert status["handles"] == 0, status
            for name in sources:
                context, _ = await peer.call("open", {"path": name})
                assert context["residentBytes"] == 0, context["residentBytes"]
        assert all(digest(project / name) == checksum for name, checksum in sources.items())
        report["passed"] = True
    except BaseException as error:
        report["passed"] = False
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        cluster.close(args.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=4)
    parser.add_argument("--max-steps", type=int, default=3)
    parser.add_argument("--phases", nargs="+", choices=["pinned", "migration", "stealing"],
                        default=["pinned", "migration", "stealing"])
    asyncio.run(main(parser.parse_args()))

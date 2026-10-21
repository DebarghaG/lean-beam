#!/usr/bin/env python3
"""Bounded replay of beam-bench tasks through two Docker snapshot workers.

Read-only project mounts preserve the corpus. This checks real continuation
semantics and scheduling decisions; it is not a statistically powered benchmark.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import random
import secrets
import statistics
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
    def __init__(self, project, runtime, workers=2):
        self.project, self.runtime = project.resolve(), runtime.resolve()
        self.workers = workers
        self.names, self.peers = [], []
        self.token = secrets.token_hex(32)

    async def start(self):
        sdk = command("lean", "--print-prefix", cwd=self.project)
        environment = json.loads(command("lake", "env", "python3", "-c",
                                        "import os,json; print(json.dumps(dict(os.environ)))",
                                        cwd=self.project))
        def mapped(value):
            return value.replace(str(self.project), "/project").replace(sdk, "/opt/lean")
        for index in range(self.workers):
            variant = "a" if index % 2 == 0 else "b"
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
        records.append({"step": index, "snapshotBytes": manifest["uncompressedBytes"],
                        "wireBytes": len(data), "exportSeconds": manifest["exportSeconds"],
                        "copySeconds": copy_seconds,
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


async def branch_batch(peers, task, mode, jobs, seed, transfer_prior):
    """Repeat frozen, real candidates to isolate scheduling throughput on one proof state.

    This deliberate fan-out is a scaling microbenchmark, not a recorded search trace.
    Each trial starts with a new parent, coordinator, and empty export/replica cache.
    """
    pool = await Pool(peers[:1] if mode == "serial" else peers,
                      stealing=mode == "stealing", seed=seed,
                      cold_transfer_seconds=transfer_prior["copySeconds"],
                      source_stall_seconds=transfer_prior["exportSeconds"]).start()
    try:
        parent = (await pool.run_at(task["file"], task["proof_start"], task["indent"], "skip"))["handle"]
        candidates = [task["steps"][0]["text"], "simp", "linarith"]
        expected, estimates = [], []
        # Identical untimed calibration in all modes; never inflate estimates to force a steal.
        for text in candidates:
            samples = []
            outcomes = []
            for _ in range(3):
                started = time.monotonic()
                result = await pool.run_with(parent, text)
                samples.append(time.monotonic() - started)
                outcomes.append(outcome(result))
            assert all(value == outcomes[0] for value in outcomes)
            estimates.append(statistics.median(samples))
            expected.append(outcome(result))
        counts_before = list(pool.metrics["completedByWorker"])
        completed = []
        started = time.monotonic()
        futures = [pool.submit(parent, candidates[i % 3], estimate=estimates[i % 3])
                   for i in range(jobs)]
        for future in futures:
            future.add_done_callback(lambda _: completed.append(time.monotonic() - started))
        results = await asyncio.gather(*futures)
        elapsed = time.monotonic() - started
        assert all(outcome(result) == expected[i % 3] for i, result in enumerate(results))
        metrics = dict(pool.metrics)
        metrics["completedByWorker"] = [after - before for after, before in
                                         zip(metrics["completedByWorker"], counts_before)]
        await pool.release(parent)
        return {"theorem": task["theorem"], "mode": mode, "jobs": jobs, "seconds": elapsed,
                "calibratedSeconds": estimates, "candidateOutcomes": expected,
                "completionP50Seconds": statistics.median(completed),
                "completionP95Seconds": sorted(completed)[math.ceil(0.95 * jobs) - 1],
                "metrics": metrics}
    finally:
        await pool.close()


def scaling_summary(records, seed):
    """Paired ratios, with a reproducible percentile bootstrap over complete trial pairs."""
    summaries = []
    groups = sorted({(run["theorem"], run["jobs"]) for run in records})
    for theorem, jobs in groups:
        runs = [run for run in records if (run["theorem"], run["jobs"]) == (theorem, jobs)]
        paired = {}
        for run in runs:
            paired.setdefault(run["trial"], {})[run["mode"]] = run
        assert all(set(pair) == {"serial", "pinned", "stealing"} for pair in paired.values())
        assert all(run["candidateOutcomes"] == runs[0]["candidateOutcomes"] for run in runs)
        ratios = [pair["pinned"]["seconds"] / pair["stealing"]["seconds"]
                  for pair in paired.values()]
        rng = random.Random(seed)
        bootstrap = sorted(math.exp(statistics.mean(math.log(rng.choice(ratios))
                           for _ in ratios)) for _ in range(10000))
        summaries.append({"theorem": theorem, "jobs": jobs, "pairedSpeedups": ratios,
                          "geometricMeanSpeedup": statistics.geometric_mean(ratios),
                          "pairedBootstrap95Percent": ([bootstrap[250], bootstrap[9749]]
                                                       if len(ratios) > 1 else None),
                          "medianSeconds": {mode: statistics.median(run["seconds"] for run in runs
                                             if run["mode"] == mode)
                                            for mode in ("serial", "pinned", "stealing")},
                          "everyStealingTrialMigrated": all(pair["stealing"]["metrics"]["imports"] > 0
                                                            for pair in paired.values())})
    return summaries


async def main(args):
    corpus, project, runtime = args.corpus.resolve(), args.project.resolve(), args.runtime.resolve()
    tasks = [task for task in json.loads(corpus.read_text())
             if task["project"] == project.name and len(task["steps"]) <= args.max_steps][:args.limit]
    assert tasks, "No matching corpus tasks"
    args.output.mkdir(parents=True, exist_ok=False)
    sources = {task["file"]: digest(project / task["file"]) for task in tasks}
    cluster = Cluster(project, runtime, args.workers)
    report = {"beamCommit": command("git", "rev-parse", "HEAD", cwd=ROOT),
              "runnerSha256": digest(__file__),
              "workerSourcesSha256": {str(path.relative_to(ROOT)): digest(path)
                                      for path in sorted((ROOT / "scripts/beam_pool").glob("*.py"))},
              "projectCommit": command("git", "rev-parse", "HEAD", cwd=project),
              "toolchain": (project / "lean-toolchain").read_text().strip(),
              "corpusSha256": digest(corpus), "sources": sources,
              "pluginSha256": digest(runtime / ".lake/build/lib/libbeam_Beam_LSP.so"),
              "workers": args.workers, "cpusPerWorker": 2, "memoryGiBPerWorker": 8,
              "images": [command("docker", "image", "inspect", "--format", "{{.Id}}",
                                 f"beam-snapshot-worker:{variant}") for variant in ("a", "b")],
              "selectedTasks": [task["theorem"] for task in tasks],
              "phases": args.phases,
              "scalingWorkload": "Repeated original/simp/linarith candidates at each initial proof state; not a search trace",
              "seed": args.seed, "repeat": args.repeat, "batchJobs": args.batch_jobs,
              "transferPrior": {"copySeconds": 1.0, "exportSeconds": 0.25},
              "hostLoadAtStart": os.getloadavg(),
              "migration": [], "scheduled": [], "scaling": []}
    try:
        await cluster.start()
        report["runtime"] = (await cluster.peers[0].call("status"))[0]["runtime"]
        if args.transfer_profile:
            profile = json.loads(args.transfer_profile.read_text())
            if not profile.get("passed") or profile["runtime"] != report["runtime"] or \
                    profile["workerSourcesSha256"] != report["workerSourcesSha256"]:
                raise ValueError("transfer profile must be a passing run of this worker/runtime")
            samples = [step for task in profile["migration"] for step in task["steps"]]
            if not samples:
                raise ValueError("transfer profile contains no migration measurements")
            report["transferPrior"] = {key: statistics.median(step[key] for step in samples)
                                       for key in ("copySeconds", "exportSeconds")}
            report["transferProfileSha256"] = digest(args.transfer_profile)
            report["profileCopySeconds"] = sum(step["copySeconds"] for step in samples)
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
        if "scaling" in args.phases:
            rng = random.Random(args.seed)
            orders = list(itertools.permutations(("serial", "pinned", "stealing")))
            orders *= math.ceil(args.repeat / len(orders))
            rng.shuffle(orders)
            for task in tasks:
                for jobs in args.batch_jobs:
                    for trial, order in enumerate(orders[:args.repeat]):
                        for mode in order:
                            run = await branch_batch(cluster.peers, task, mode, jobs, args.seed + trial,
                                                     report["transferPrior"])
                            run["trial"] = trial
                            report["scaling"].append(run)
                            print(json.dumps(run), flush=True)
                            (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            report["scalingSummary"] = scaling_summary(report["scaling"], args.seed)
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
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--repeat", type=int, default=6)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--batch-jobs", type=int, nargs="+", default=[3, 192])
    parser.add_argument("--transfer-profile", type=Path,
                        help="Seed copy/export estimates from a separate passing migration report")
    parser.add_argument("--phases", nargs="+", choices=["pinned", "migration", "stealing", "scaling"],
                        default=["pinned", "migration", "stealing"])
    args = parser.parse_args()
    if args.workers < 2 or min(args.repeat, args.limit, args.max_steps, *args.batch_jobs) <= 0:
        parser.error("at least two workers and positive workload sizes are required")
    if max(args.batch_jobs) > 4096:
        parser.error("batch size exceeds the pool queue budget")
    asyncio.run(main(args))

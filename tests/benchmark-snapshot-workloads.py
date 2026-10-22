#!/usr/bin/env python3
"""Compare pinned and movable continuations across distinct qualified proof states.

Mixed trials interleave a frozen candidate stream across roots. Tree trials run
the saved proof chains concurrently, checking three siblings at every depth.
Root admission is either concurrent (distributed) or sequential (concentrated).
Neither placement uses private scheduler state or changes the scheduling policy.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import statistics
import time

spec = importlib.util.spec_from_file_location('qualification', Path(__file__).with_name('qualify-snapshot-corpus.py'))
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)
corpus = qualification.corpus


class Resources:
    """Read cgroups directly so Docker commands do not perturb timed requests."""

    def __init__(self, cluster):
        self.groups = []
        for name in cluster.names:
            pid = corpus.command('docker', 'inspect', '--format', '{{.State.Pid}}', name)
            group = next(line.split(':', 2)[2] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines()
                         if line.startswith('0::'))
            self.groups.append(Path('/sys/fs/cgroup') / group.lstrip('/'))

    def read(self):
        rows = []
        for group in self.groups:
            cpu = {key: int(value) for key, value in
                   (line.split() for line in (group / 'cpu.stat').read_text().splitlines())}
            rows.append({'cpuSeconds': cpu['usage_usec'] / 1e6,
                         'throttledSeconds': cpu['throttled_usec'] / 1e6,
                         'memoryBytes': int((group / 'memory.current').read_text())})
        return rows


async def trial(cluster, cases, configuration, prior, seed, repeats):
    pool = await corpus.Pool(cluster.peers, stealing=configuration['mode'] == 'stealing', seed=seed,
                             cold_transfer_seconds=prior['copySeconds'],
                             source_stall_seconds=prior['exportSeconds']).start()
    record = dict(configuration, status='running')
    observer = None
    try:
        started = time.monotonic()
        async def mint(case):
            task = case['task']
            result = await pool.run_at(task['file'], task['proof_start'], task['indent'], 'skip')
            if not result['success'] or result.get('handle') is None:
                raise RuntimeError('Could not retain qualified proof root')
            return result['handle']
        roots = (await asyncio.gather(*(mint(case) for case in cases))
                 if configuration['placement'] == 'distributed' else
                 [await mint(case) for case in cases])
        record['rootAdmissionSeconds'] = time.monotonic() - started
        record['rootsByWorker'] = [sum(index in root.replicas for root in roots)
                                  for index in range(len(cluster.peers))]
        metrics_before = copy.deepcopy(pool.metrics)
        resources = Resources(cluster)
        before = resources.read()
        peaks = [row['memoryBytes'] for row in before]
        latencies, completions, chains = [], [], []
        driver_cpu = time.process_time()
        started = time.monotonic()

        async def observe():
            while True:
                for index, row in enumerate(resources.read()):
                    peaks[index] = max(peaks[index], row['memoryBytes'])
                await asyncio.sleep(0.5)
        observer = asyncio.create_task(observe())

        async def collect(future, candidate, admitted):
            result = await future
            finished = time.monotonic()
            if corpus.outcome(result) != candidate['outcome']:
                raise RuntimeError('Candidate differs from qualified local reference')
            latencies.append(finished - admitted)
            completions.append(finished - started)
            return result

        if configuration['workload'] == 'mixed':
            rng = random.Random(seed)
            requests = []
            # One candidate per root in every shuffled round, independent of
            # worker count and mode. Adjacent same-parent jobs are not fabricated.
            for number in range(repeats):
                order = list(range(len(cases)))
                rng.shuffle(order)
                for index in order:
                    candidate = cases[index]['steps'][0]['candidates'][number % 3]
                    admitted = time.monotonic()
                    future = pool.submit(roots[index], candidate['text'], estimate=candidate['serviceSeconds'])
                    requests.append(asyncio.create_task(collect(future, candidate, admitted)))
            results = await asyncio.gather(*requests, return_exceptions=True)
            errors = [result for result in results if isinstance(result, BaseException)]
            if errors:
                raise errors[0]
        else:
            async def chain(index):
                parent = roots[index]
                for step in cases[index]['steps']:
                    requests = []
                    for number, candidate in enumerate(step['candidates']):
                        admitted = time.monotonic()
                        future = pool.submit(parent, candidate['text'], store=number == 0,
                                             estimate=candidate['serviceSeconds'])
                        requests.append(collect(future, candidate, admitted))
                    results = await asyncio.gather(*requests)
                    child = results[0].get('handle')
                    if child is None or not results[0]['success']:
                        raise RuntimeError('Qualified continuation did not return a child')
                    await pool.release(parent)
                    parent = child
                if results[0]['proofState']['goals']:
                    raise RuntimeError('Qualified proof chain did not close')
                await pool.release(parent)
                chains.append(time.monotonic() - started)
            results = await asyncio.gather(*(chain(index) for index in range(len(cases))), return_exceptions=True)
            errors = [result for result in results if isinstance(result, BaseException)]
            if errors:
                raise errors[0]
        elapsed = time.monotonic() - started
        after = resources.read()
        for index, row in enumerate(after):
            peaks[index] = max(peaks[index], row['memoryBytes'])
        record.update(status='passed', seconds=elapsed, attempts=len(completions),
                      attemptsPerSecond=len(completions) / elapsed,
                      latencyP50Seconds=statistics.median(latencies),
                      latencyP95Seconds=sorted(latencies)[math.ceil(.95 * len(latencies)) - 1],
                      completionP95Seconds=sorted(completions)[math.ceil(.95 * len(completions)) - 1],
                      closedChains=len(chains), chainCompletionSeconds=chains,
                      driverCpuSeconds=time.process_time() - driver_cpu,
                      workerCpuSeconds=[b['cpuSeconds'] - a['cpuSeconds'] for a, b in zip(before, after)],
                      workerThrottledSeconds=[b['throttledSeconds'] - a['throttledSeconds'] for a, b in zip(before, after)],
                      workerPeakObservedMemoryBytes=peaks,
                      metrics={key: ([b - a for a, b in zip(metrics_before[key], value)]
                                     if isinstance(value, list) else value - metrics_before[key])
                               for key, value in pool.metrics.items()})
    except Exception as error:
        record.update(status='error', error=qualification.failure(error, 'trial'))
    finally:
        if observer is not None:
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)
        started = time.monotonic()
        await pool.close()
        record['cleanupSeconds'] = time.monotonic() - started
        status = [(await peer.call('status'))[0] for peer in cluster.peers]
        record['released'] = all(row['handles'] == 0 for row in status)
        if not record['released']:
            record['status'] = 'leak'
    return record


async def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    qualified = json.loads(args.qualification.read_text())
    assert qualified['completed'], 'Qualification is not complete'
    assert qualified['corpusSha256'] == corpus.digest(args.corpus), 'Corpus changed after qualification'
    worker_sources = {p.name: corpus.digest(p) for p in sorted((corpus.ROOT / 'scripts/beam_pool').glob('*.py'))}
    assert qualified['workerSourcesSha256'] == worker_sources, 'Worker implementation changed after qualification'
    assert qualified['pluginSha256'] == corpus.digest(args.runtime / '.lake/build/lib/libbeam_Beam_LSP.so')
    tasks = json.loads(args.corpus.read_text())
    indexed = {(task['file'], task['theorem']): task for task in tasks if task['project'] == args.project.name}
    cases, excluded = [], []
    for row in qualified['tasks']:
        if row['baseline']['status'] == row['migration']['status'] == 'passed':
            cases.append({'task': indexed[(row['file'], row['theorem'])], 'steps': row['baseline']['steps'],
                          'transfers': row['migration']['transfers']})
        else:
            excluded.append({'theorem': row['theorem'], 'baseline': row['baseline']['status'],
                             'migration': row['migration']['status']})
    assert len(cases) >= 2, 'Distinct-state scaling requires at least two qualified proofs'
    assert len(cases) * args.repeats <= 4096, 'Mixed batch exceeds the default pool queue budget'
    assert all(len(step['candidates']) == 3 for case in cases for step in case['steps'])
    assert qualified['sources'] == {name: corpus.digest(args.project / name) for name in qualified['sources']}
    transfers = [transfer for case in cases for transfer in case['transfers']]
    prior = {'copySeconds': max(t['copySeconds'] for t in transfers),
             'exportSeconds': max(t['exportSeconds'] for t in transfers)}
    report = {'completed': False, 'project': args.project.name,
              'beamCommit': corpus.command('git', 'rev-parse', 'HEAD', cwd=corpus.ROOT),
              'runnerSha256': corpus.digest(__file__), 'qualificationSha256': corpus.digest(args.qualification),
              'helperSha256': {Path(module.__file__).name: corpus.digest(module.__file__)
                               for module in [qualification, corpus]},
              'workerSourcesSha256': worker_sources,
              'pluginSha256': corpus.digest(args.runtime / '.lake/build/lib/libbeam_Beam_LSP.so'),
              'images': [corpus.command('docker', 'image', 'inspect', '--format', '{{.Id}}',
                                       f'beam-snapshot-worker:{variant}') for variant in ['a', 'b']],
              'configuration': {'workers': args.workers, 'trials': args.trials, 'seed': args.seed,
                                'workloads': args.workloads,
                                'mixedAttemptsPerRoot': args.repeats, 'cpusPerWorker': args.cpus,
                                'workerCpuSets': args.cpu_sets, 'driverCpuSet': sorted(os.sched_getaffinity(0)),
                                'memoryGiBPerWorker': args.memory_gib, 'snapshotMiB': args.snapshot_mib,
                                'priors': prior, 'serviceEstimates': 'Untimed local qualification service times'},
              'cases': [{'theorem': c['task']['theorem'], 'file': c['task']['file'],
                         'steps': len(c['steps'])} for c in cases],
              'excluded': excluded, 'preparation': [], 'runs': [], 'hostLoadAtStart': os.getloadavg()}
    def checkpoint():
        qualification.save(args.output / 'report.json', report)
    checkpoint()
    # Fresh workers for each count; shared prepared documents across paired
    # trials. Roots, coordinator and snapshot export cache are fresh each trial.
    for workers in args.workers:
        cluster = corpus.Cluster(args.project, args.runtime, workers, cpus=args.cpus,
                                 memory_gib=args.memory_gib, snapshot_mib=args.snapshot_mib,
                                 timeout=args.timeout, threads=2,
                                 cpu_sets=args.cpu_sets[:workers] if args.cpu_sets else None)
        setup = {'workers': workers, 'status': 'running', 'opened': []}
        report['preparation'].append(setup)
        started = time.monotonic()
        try:
            await cluster.start()
            setup['startupSeconds'] = time.monotonic() - started
            meter = Resources(cluster)
            prepared_bytes = None
            for index, peer in enumerate(cluster.peers):
                if prepared_bytes is not None:
                    additional = max(0, prepared_bytes - meter.read()[index]['memoryBytes'])
                    if qualification.memory_available() < args.reserve_gib * 1024**3 + additional:
                        raise RuntimeError('Observed worker footprint would breach host memory reserve')
                for filename in sorted({c['task']['file'] for c in cases}):
                    if qualification.memory_available() < args.reserve_gib * 1024**3:
                        raise RuntimeError('Host memory reserve reached while preparing workers')
                    before = time.monotonic()
                    await peer.call('open', {'path': filename})
                    setup['opened'].append({'worker': index, 'file': filename,
                                            'seconds': time.monotonic() - before})
                    checkpoint()
                    if qualification.memory_available() < args.reserve_gib * 1024**3:
                        raise RuntimeError('Host memory reserve reached after document preparation')
                prepared_bytes = max(prepared_bytes or 0, meter.read()[index]['memoryBytes'])
            setup.update(status='passed', seconds=time.monotonic() - started,
                         resources=qualification.resources(cluster))
            checkpoint()
            placements = ['distributed'] if workers == 1 else ['distributed', 'concentrated']
            for repetition in range(args.trials):
                configurations = [{'workers': workers, 'trial': repetition, 'placement': placement,
                                   'workload': workload, 'mode': mode}
                                  for placement in placements for workload in args.workloads
                                  for mode in ['pinned', 'stealing']]
                random.Random(args.seed + repetition).shuffle(configurations)
                for configuration in configurations:
                    print('trial', configuration, flush=True)
                    run = await trial(cluster, cases, configuration, prior, args.seed + repetition, args.repeats)
                    report['runs'].append(run)
                    checkpoint()
                    print('result', run['status'], run.get('seconds'), run.get('metrics'), flush=True)
                    if run['status'] != 'passed':
                        raise RuntimeError('Trial failed; stop this worker count and preserve evidence')
            setup['finalResidentBytes'] = [(await peer.call('open', {'path': filename}))[0]['residentBytes']
                                           for peer in cluster.peers
                                           for filename in sorted({c['task']['file'] for c in cases})]
            setup['released'] = all(value == 0 for value in setup['finalResidentBytes'])
        except Exception as error:
            setup.update(status='error', error=qualification.failure(error, 'workerCount'))
        finally:
            output = args.output / f'workers-{workers}'
            output.mkdir()
            cluster.close(output)
            checkpoint()
    report['sourceUnchanged'] = all(corpus.digest(args.project / name) == value
                                    for name, value in qualified['sources'].items())
    report['completed'] = True
    checkpoint()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['corpus', 'qualification', 'project', 'runtime', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--workers', nargs='+', type=int, default=[1, 2, 4, 8])
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--workloads', nargs='+', choices=['mixed', 'tree'], default=['mixed', 'tree'])
    parser.add_argument('--repeats', type=int, default=72)
    parser.add_argument('--seed', type=int, default=41)
    parser.add_argument('--cpus', type=int, default=2)
    parser.add_argument('--cpu-sets', nargs='+', help='One Docker CPU set per worker at the largest worker count')
    parser.add_argument('--memory-gib', type=int, default=16)
    parser.add_argument('--snapshot-mib', type=int, default=1024)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--reserve-gib', type=int, default=16)
    args = parser.parse_args()
    if min(*args.workers, args.trials, args.repeats, args.cpus, args.memory_gib,
           args.snapshot_mib, args.timeout, args.reserve_gib) <= 0 or args.repeats % 3:
        parser.error('counts must be positive and repeats must be divisible by three')
    if args.cpu_sets is not None and len(args.cpu_sets) < max(args.workers):
        parser.error('provide a CPU set for every worker at the largest worker count')
    asyncio.run(main(args))

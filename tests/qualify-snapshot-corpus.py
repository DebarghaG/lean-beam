#!/usr/bin/env python3
"""Qualify every selected corpus proof before measuring distributed throughput.

Local replay is the reference. A separate replay migrates its continuing state
at the beginning, middle and last step, comparing every candidate with that
reference. Rejections, transport failures and unvisited suffixes remain explicit.
Projects are mounted read-only; each file gets a fresh bounded worker cluster.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time

spec = importlib.util.spec_from_file_location('snapshot_corpus', Path(__file__).with_name('benchmark-snapshot-corpus.py'))
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def failure(error, stage):
    record = {'stage': stage, 'type': type(error).__name__, 'message': str(error) or type(error).__name__}
    if hasattr(error, 'error'):
        record['detail'] = error.error
    return record


def resources(cluster):
    samples = []
    for name in cluster.names:
        state = json.loads(corpus.command('docker', 'inspect', '--format', '{{json .State}}', name))
        row = {'container': name, 'running': state['Running'], 'oomKilled': state['OOMKilled']}
        if state['Running']:
            try:
                group = next(line.split(':', 2)[2] for line in
                             Path(f'/proc/{state["Pid"]}/cgroup').read_text().splitlines()
                             if line.startswith('0::'))
                root = Path('/sys/fs/cgroup') / group.lstrip('/')
                for field in ['memory.current', 'memory.peak', 'cpu.stat', 'memory.events']:
                    row[field] = (root / field).read_text().strip()
            except OSError as error:
                row['observationError'] = str(error)
        samples.append(row)
    return samples


def memory_available():
    return next(int(line.split()[1]) * 1024 for line in Path('/proc/meminfo').read_text().splitlines()
                if line.startswith('MemAvailable:'))


async def replay(cluster, task, tactics, record, checkpoint, reference=None, transfer_timeout=120):
    handles = set()
    owner = 0
    stage = 'mint'
    record.update(status='running', steps=[], transfers=[], cleanupErrors=[])
    checkpoint()
    started = time.monotonic()

    async def call(index, operation, **params):
        result, _ = await cluster.peers[index].call(operation, {'path': task['file'], **params})
        if operation in {'runAt', 'runWith'} and result.get('handle') is not None:
            handles.add((index, result['handle']))
        return result

    async def release(index, handle):
        await call(index, 'release', handle=handle)
        handles.remove((index, handle))

    try:
        initial = await call(owner, 'runAt', position={'line': task['proof_start'], 'character': task['indent']},
                             text='skip', store=True)
        if not initial['success'] or initial.get('handle') is None:
            record.update(status='rejected', rejection=initial)
            return
        parent = initial['handle']
        boundaries = {0, len(task['steps']) // 2, len(task['steps']) - 1}
        for index, step in enumerate(task['steps']):
            if reference is not None and index in boundaries:
                destination = 1 - owner
                stage = f'export:{index}'
                copied = time.monotonic()
                record['activeStage'] = stage
                checkpoint()
                manifest, payload = await asyncio.wait_for(cluster.peers[owner].call('export', {
                    'path': task['file'], 'handle': parent}), transfer_timeout)
                transfer = {'step': index, 'from': owner, 'to': destination,
                            'exportRpcSeconds': time.monotonic() - copied,
                            **{key: manifest[key] for key in ['uncompressedBytes', 'exportSeconds',
                                'exportStagesSeconds', 'nativeExportStagesSeconds']},
                            'wireBytes': len(payload)}
                record['transfers'].append(transfer)
                checkpoint()
                stage = f'import:{index}'
                record['activeStage'] = stage
                checkpoint()
                imported, _ = await asyncio.wait_for(cluster.peers[destination].call('import', {
                    'path': task['file'], 'manifest': manifest}, payload), transfer_timeout)
                del payload
                handles.add((destination, imported['handle']))
                transfer.update(copySeconds=time.monotonic() - copied,
                                importSeconds=imported['importSeconds'],
                                importStagesSeconds=imported['importStagesSeconds'])
                await release(owner, parent)
                owner, parent = destination, imported['handle']
            stage = f'candidates:{index}'
            row = {'step': index, 'candidates': []}
            record['steps'].append(row)
            advanced = None
            for candidate, text in enumerate([step['text'], *tactics]):
                before = time.monotonic()
                result = await call(owner, 'runWith', handle=parent, text=text, store=candidate == 0)
                value = corpus.outcome(result)
                attempt = {'text': text, 'outcome': value, 'seconds': time.monotonic() - before,
                           'serviceSeconds': result['serviceSeconds']}
                if not result['success']:
                    attempt['messages'] = result.get('messages', [])
                row['candidates'].append(attempt)
                if reference is not None:
                    attempt['matchesReference'] = value == reference['steps'][index]['candidates'][candidate]['outcome']
                    if not attempt['matchesReference']:
                        record['status'] = 'mismatch'
                        return
                if candidate == 0:
                    advanced = result
            checkpoint()
            if not advanced['success'] or advanced.get('handle') is None:
                record.update(status='rejected', rejectedStep=index)
                return
            await release(owner, parent)
            parent = advanced['handle']
        record['status'] = 'passed' if advanced['proofState']['goals'] == [] else 'openGoals'
    except Exception as error:
        record.update(status='error', error=failure(error, stage))
    finally:
        record['seconds'] = time.monotonic() - started
        record.pop('activeStage', None)
        record['completedSteps'] = sum(len(s['candidates']) == len(tactics) + 1 for s in record['steps'])
        record['unvisitedSteps'] = len(task['steps']) - record['completedSteps']
        for index, handle in list(handles):
            try:
                await release(index, handle)
            except Exception as error:
                record['cleanupErrors'].append(failure(error, 'release'))
        record['resources'] = resources(cluster)
        checkpoint()


async def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    tasks = [t for t in json.loads(args.corpus.read_text()) if t['project'] == args.project.name]
    if args.theorems:
        tasks = [t for t in tasks if t['theorem'] in args.theorems]
    assert tasks, 'No selected proofs'
    report = {
        'completed': False, 'beamCommit': corpus.command('git', 'rev-parse', 'HEAD', cwd=corpus.ROOT),
        'runnerSha256': corpus.digest(__file__), 'clusterRunnerSha256': corpus.digest(corpus.__file__),
        'workerSourcesSha256': {p.name: corpus.digest(p) for p in sorted((corpus.ROOT / 'scripts/beam_pool').glob('*.py'))},
        'project': str(args.project), 'projectCommit': corpus.command('git', 'rev-parse', 'HEAD', cwd=args.project),
        'toolchain': (args.project / 'lean-toolchain').read_text().strip(),
        'corpusSha256': corpus.digest(args.corpus),
        'pluginSha256': corpus.digest(args.runtime / '.lake/build/lib/libbeam_Beam_LSP.so'),
        'sources': {t['file']: corpus.digest(args.project / t['file']) for t in tasks},
        'images': [corpus.command('docker', 'image', 'inspect', '--format', '{{.Id}}', f'beam-snapshot-worker:{v}')
                   for v in ['a', 'b']],
        'configuration': {'workers': 2, 'cpusPerWorker': args.cpus, 'memoryGiBPerWorker': args.memory_gib,
                          'snapshotMiB': args.snapshot_mib, 'timeoutSeconds': args.timeout,
                          'transferTimeoutSeconds': args.transfer_timeout,
                          'tactics': args.tactics, 'migrationSteps': 'first, middle and last'},
        'hostLoadAtStart': os.getloadavg(), 'files': [],
        'tasks': [{'theorem': t['theorem'], 'file': t['file'], 'steps': len(t['steps']),
                   'baseline': {'status': 'pending'}, 'migration': {'status': 'pending'}} for t in tasks]}
    def checkpoint():
        save(args.output / 'report.json', report)
    checkpoint()
    for number, filename in enumerate(report['sources']):
        selected = [(task, row) for task, row in zip(tasks, report['tasks']) if task['file'] == filename]
        file_record = {'file': filename, 'attempts': []}
        report['files'].append(file_record)
        cluster = None

        async def ensure_cluster():
            nonlocal cluster
            if cluster is not None:
                return
            if memory_available() < args.reserve_gib * 1024**3:
                raise RuntimeError('Available host memory is below the configured reserve')
            cluster = corpus.Cluster(args.project, args.runtime, cpus=args.cpus, memory_gib=args.memory_gib,
                                     timeout=args.timeout, snapshot_mib=args.snapshot_mib)
            await cluster.start()
            attempt = {'runtime': (await cluster.peers[0].call('status'))[0]['runtime'], 'opened': []}
            file_record['attempts'].append(attempt)
            # Baseline opens only one worker; receiver preparation is separate.
            before = time.monotonic()
            await cluster.peers[0].call('open', {'path': filename})
            attempt['opened'].append({'worker': 0, 'seconds': time.monotonic() - before,
                                      'resources': resources(cluster)})
            checkpoint()

        def close_cluster():
            nonlocal cluster
            if cluster is not None:
                logs = args.output / f'file-{number}-attempt-{len(file_record["attempts"])}'
                logs.mkdir(exist_ok=True)
                cluster.close(logs)
                cluster = None

        try:
            for task, row in selected:
                print('baseline', task['project'], task['theorem'], flush=True)
                try:
                    await ensure_cluster()
                    await replay(cluster, task, args.tactics, row['baseline'], checkpoint)
                except Exception as error:
                    row['baseline'].update(status='error', error=failure(error, 'open'))
                print('baseline result', task['theorem'], row['baseline']['status'], flush=True)
                checkpoint()
                if row['baseline']['status'] == 'error' or row['baseline'].get('cleanupErrors'):
                    close_cluster()
            for task, row in selected:
                if row['baseline']['status'] != 'passed':
                    row['migration'] = {'status': 'notRun', 'reason': 'Local proof chain did not pass'}
                    checkpoint()
                    continue
                print('migration', task['project'], task['theorem'], flush=True)
                try:
                    await ensure_cluster()
                    attempt = file_record['attempts'][-1]
                    if len(attempt['opened']) == 1:
                        if memory_available() < args.reserve_gib * 1024**3:
                            raise RuntimeError('Available host memory is below the configured reserve')
                        before = time.monotonic()
                        await cluster.peers[1].call('open', {'path': filename})
                        attempt['opened'].append({'worker': 1, 'seconds': time.monotonic() - before,
                                                  'resources': resources(cluster)})
                        checkpoint()
                    await replay(cluster, task, args.tactics, row['migration'], checkpoint, row['baseline'],
                                 transfer_timeout=args.transfer_timeout)
                except Exception as error:
                    row['migration'].update(status='error', error=failure(error, 'openReceiver'))
                print('migration result', task['theorem'], row['migration']['status'], flush=True)
                checkpoint()
                if row['migration']['status'] == 'error' or row['migration'].get('cleanupErrors'):
                    close_cluster()
            if cluster is not None:
                file_record['finalStatus'] = [(await p.call('status'))[0] for p in cluster.peers]
                opened = [o['worker'] for o in file_record['attempts'][-1]['opened']]
                file_record['residentBytes'] = [(await cluster.peers[i].call('open', {'path': filename}))[0]['residentBytes']
                                                for i in opened]
                file_record['released'] = all(s['handles'] == 0 for s in file_record['finalStatus']) and \
                    all(n == 0 for n in file_record['residentBytes'])
                checkpoint()
        finally:
            close_cluster()
    report['sourceUnchanged'] = all(corpus.digest(args.project / f) == h for f, h in report['sources'].items())
    report['completed'] = True
    report['passed'] = report['sourceUnchanged'] and all(
        row['baseline']['status'] == row['migration']['status'] == 'passed' and
        not row['baseline']['cleanupErrors'] and not row['migration']['cleanupErrors']
        for row in report['tasks']) and all(f.get('released', False) for f in report['files'])
    checkpoint()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['corpus', 'project', 'runtime', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--theorems', nargs='+')
    parser.add_argument('--cpus', type=int, default=4)
    parser.add_argument('--memory-gib', type=int, default=16)
    parser.add_argument('--snapshot-mib', type=int, default=256)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--transfer-timeout', type=int, default=120)
    parser.add_argument('--reserve-gib', type=int, default=16)
    parser.add_argument('--tactics', type=Path)
    args = parser.parse_args()
    args.tactics = json.loads(args.tactics.read_text()) if args.tactics else ['simp', 'linarith']
    if not isinstance(args.tactics, list) or any(not isinstance(t, str) or not t.strip() for t in args.tactics):
        parser.error('tactics must be an array of nonempty strings')
    if min(args.cpus, args.memory_gib, args.snapshot_mib, args.timeout, args.transfer_timeout,
           args.reserve_gib) <= 0:
        parser.error('resource limits must be positive')
    asyncio.run(main(args))

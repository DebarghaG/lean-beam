# Distributed proof snapshots

This experimental pool runs Beam proof continuations across Linux workers. It builds on the
upstream Lean Beam implementation, using the existing `runAt`, `runWith`, and handle-release
semantics. The coordinator is a small Python library; workers communicate over TCP and call Beam's
LSP plugin. The installed CLI and MCP interfaces continue to use the local broker.

The worker and coordinator here are a new experimental implementation on upstream Beam, rather
than an extension of the earlier broker-integrated pool. Independent `runAt` requests can already
be assigned to any prepared worker, with or without snapshot stealing. Migration addresses a
different constraint: `runWith` continuations depend on a retained state that initially exists on
one worker. Full integration with the earlier pool's agent-facing routing is not implemented.

A limited `beam-bench` replay now passes both locally and after snapshot migration. Real corpus
snapshots are much larger and slower to copy than the synthetic fixtures; see the
[corpus evaluation](#corpus-evaluation) below.

## State transfer

Beam retains the complete core, meta, term, and tactic state. Export uses Lean's
`CompactedRegion.save` with closure relocation, the same machinery used by Lean's incremental
snapshots. Imported module regions are dependencies of the snapshot, rather than serialized again.
Before export, Beam checks that document and proof reporting tasks have finished, then resolves
their cancellation tokens using the same preparation as Lean's incremental snapshot saver. This
also settles tokens retained inside cached realization results. Unfinished reporting rejects export
before any token is changed. Each resumed request receives fresh cancellation state.
Request cancellation and top-level editor reporting state are cleared. Proof environments, local
instances, universe parameters, name generators, and extension state remain in the object graph.
No tactic replay or process checkpointing is involved.

A worker verifies SHA-256 hashes of the snapshot, imported regions, runtime artifacts, and executable
libraries actually mapped into the Lean worker before calling the unsafe native reader. The plugin
also checks the format, Lean compiler identity, source text, module name, import order, and region
sizes. Workers must have byte-identical Lean/Beam binaries and compatible project artifacts on the
same architecture. Lean's closure relocator also requires native libraries at the same absolute load
paths; the Docker layout pins the SDK and plugin paths. Project source roots can differ when loaded
native library paths remain the same. Different Lean versions, native libraries, or library layouts
are rejected. Native snapshots are trusted code-bearing artifacts, not an interchange
format for untrusted uploads.

Import creates a new worker-local handle. An external owner destroys each derived snapshot's heap
objects before releasing the shared mapping. The mapping stays alive during execution and through
descendants, even after the original handle is released. Normal local snapshots retain their
existing representation and incur no region-wrapper allocation.

## Scheduling

Each worker exposes one actual execution slot. New jobs choose the least-loaded existing replica of
their parent. An idle worker samples up to two donors, tries the busier one first, and considers a
contiguous batch of queued siblings sharing a parent. It reserves that batch before any transfer.
The parent is exported once, cached at the coordinator, and imported once per destination. Further
steals to an existing replica require no snapshot transfer.

For estimated donor work `W`, destination transfer delay `C`, source serialization stall `S`, and
work moved `x`, the predicted completion time is:

```text
max(W + S - x, C + x)
```

The scheduler evaluates whole-job boundaries and chooses the best split only if it improves on `W`
by a configured margin. When `S = 0`, divisible work gives `x = max(0, (W - C) / 2)`: the familiar
steal-half rule adjusted for communication delay. Service-time estimates come from worker execution
timings or a caller's `estimate`; export and copy costs are updated from observations.

This combines established [locality-aware work stealing](https://distributed.dask.org/en/latest/work-stealing.html)
and randomized two-choice sampling with a latency-aware split. The split minimizes this two-worker
cost model; noisy estimates, heterogeneous hardware, arbitrary tactic costs, and network contention
mean it is not a universal speedup guarantee. The latency analysis in
[Gast et al.](https://arxiv.org/abs/1805.00857) uses stronger workload assumptions than this implementation.

Jobs are removed from the donor queue before transfer, so the donor cannot also execute them. A failed
copy returns still-unexecuted work to its source when that source remains available. An ambiguous
failure during execution is surfaced to the caller and is never automatically retried. A lost
connection fences that worker generation and fails its queued jobs. This coordinator does not offer
durable recovery, exactly-once execution, or consensus.

## Running workers

Build the plugin for the project's exact Lean toolchain. Run each worker in that project's `lake env`
with immutable source and build artifacts. The worker emits its chosen port and generation as JSON.
Document opening disables Lake dependency builds and reports fatal setup diagnostics. Prepare all
imports before starting workers. Docker workers need Git for Lake's dependency inspection; when
mounting an existing checkout, run with its owner's UID/GID to preserve Git ownership checks.

```bash
lake build Beam.LSP:shared
export BEAM_POOL_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
lake env python3 -m scripts.beam_pool worker \
  --project "$PWD" --plugin "$PWD/.lake/build/lib/libbeam_Beam_LSP.so" \
  --host 127.0.0.1 --port 8765
```

Run another worker with a different port or on another machine. Copy the same project/runtime
artifacts there; no shared snapshot directory is required. On separate hosts, use `--cert` and
`--key` with a trusted TLS certificate and pass a verifying `ssl.SSLContext` to `Peer`, or use an
authenticated private tunnel. The default listener is loopback. The bearer capability is supplied
through `BEAM_POOL_TOKEN`, not a command-line argument.

A LeanBeam client can keep one coordinator in its search process and replace pinned continuation calls with
this interface:

```python
import asyncio
import os
from scripts.beam_pool.scheduler import Pool
from scripts.beam_pool.wire import Peer

async def search():
    token = os.environ["BEAM_POOL_TOKEN"]
    pool = await Pool([
        Peer("127.0.0.1", 8765, token),
        Peer("127.0.0.1", 8766, token),
    ]).start()
    try:
        result = await pool.run_at("Proof.lean", 4, 2, "skip", store=True)
        assert result["success"], result["messages"]
        parent = result["handle"]
        results = await asyncio.gather(*[
            pool.submit(parent, tactic, estimate=0.2)
            for tactic in ["simp", "omega", "assumption"]
        ])
        await pool.release(parent)
        return results
    finally:
        await pool.close()
```

`Snapshot` objects are coordinator-owned handles, not JSON payloads. Use `store=True` on continuations
to obtain child handles; these survive release of their parent. Keep the pool alive while using
them. A source command handle stays pinned if export is rejected. Distributed linear
consumption is not exposed: this interface accepts reusable continuations only. Source changes
require a new worker generation; the normal local Beam edit/sync APIs remain available separately.

Snapshots use standard zlib compression at level 1 for transport. The checksum covers the encoded
bytes; import verifies it, admits the declared uncompressed size against the mapping budget, and
requires one complete stream of exactly that size. Decompression output is limited to the admitted
size plus one byte so oversized streams can be rejected. Compression changes neither Lean's native
format nor the proof state.

Defaults bound the queue to 4,096 jobs, the coordinator's compressed export cache to 256 MiB, each worker
to 4,096 handles, 16 open documents and 256 MiB of uncompressed imported mappings, and a single
compressed transfer to 128 MiB. Exports must also fit the worker's snapshot memory budget.
Documents without retained handles are evicted when needed. Mapping admission counts all file
processes belonging to the worker. These bounds do not
cap arbitrary tactic heap allocation; use container/process memory limits too. Cache eviction drops
serialized bytes, while explicit release drops all known replicas. A connection loss after a remote
handle has been created can leave an unacknowledged handle until that worker exits; its handle and
mapping budgets still apply. Restart the worker to reclaim such uncertain remote state.

## Reproducing the checks

```bash
python3 tests/test-beam-pool.py
lake env python3 tests/test-beam-snapshot-transfer.py
bash tests/test-lsp.sh

docker build -f deploy/snapshot-worker.Dockerfile --target worker-a -t beam-snapshot-worker:a .
docker build -f deploy/snapshot-worker.Dockerfile --target worker-b -t beam-snapshot-worker:b .
lake env python3 tests/test-beam-pool-integration.py \
  --docker --workers 4 --jobs 48 --steps 500 --repeat 3 --require-steal \
  --report /tmp/beam-pool-report.json
```

The Docker test uses two images with different configuration but the same native runtime, separate
processes and temporary directories, different project source paths, loopback TCP, and
read-only mounts for project/runtime inputs.
It checks corrupted and mismatched artifacts, continuation after source release, region reclamation,
a sequential chain that should stay local, and identical CPU-bound tactic batches with stealing off
and on. All candidates must solve their proof goals. For the cheap-work case, use `--steps 1` and
omit `--require-steal`; exporting should not amortize its cost.

The native tests additionally kill the sender before import, retain branches after parent release,
re-export descendants, preserve local notation/instances/universes, and exercise in-flight release,
cancellation, identity mismatch, and edit invalidation. Scheduler tests cover batch reservation,
coalesced export, failed-copy rollback, worker loss, release races, queue cancellation, and the split's
predicted improvement over 1,000 generated workloads.

The [recorded three-trial run](benchmarks/snapshot-pool-2026-10-21.json) on 2026-10-21 used four
containers and 48 branches with 500 small tactic elaborations per branch. Alternating pinned and
stealing order gave medians of 6.208 s and 2.122 s respectively (2.93×). Each stealing trial exported
one snapshot and imported it on three workers. With 24 one-step branches on two workers, medians
were 56.4 ms and 54.6 ms and the scheduler performed no transfers. These small timing differences
should be treated as noise; the relevant cheap-work result is avoiding the copy.

The JSON includes all trials, runtime identity, image IDs, resource limits, and an implementation
fingerprint. `completedByWorker` includes the seed and calibration requests; `transferSeconds` sums
concurrent copies, and `stolenJobs` counts moves, including jobs moved more than once. Calibration
uses an idle-worker round trip.

These are correctness checks and a synthetic scheduling experiment. They do not rerun the original
LeanBeam corpus, establish cross-machine WAN performance, or provide an MLSys evaluation. Real
search traces, larger imported environments, skewed task durations, and heterogeneous workers need
separate measurements before making those claims.

## Corpus evaluation

The [2026-10-21 corpus report](benchmarks/snapshot-corpus-sanity-2026-10-21.json) samples four
three-step proofs from two `PrimeNumberTheoremAnd` files in `beam-bench/work/tasks.json`, using
Lean 4.33.1 and two Docker workers. It replays the exact saved steps against the original source
positions, with read-only project mounts. Each state also checks `simp` and `linarith`.

Both pinned and stealing-enabled replay solved all four proofs and accepted all 12 original steps.
All 36 candidate outcomes agreed, including 11 rejected alternatives. The small sibling batches
stayed local, with zero exports or imports. Warm replay took 1.961 s and 1.688 s, respectively;
this single ordered run does not establish a speedup.

The initial migration failure was inside serialization, before network transfer. The elaboration
environment retained four unresolved cancellation promises through cached realization diagnostics.
Lean's compactor waits for task results, so merely removing the top-level cancellation token did
not suffice. A temporary graph diagnostic identified these promises; no graph walker or private
environment-field manipulation is part of the implementation. Completing reporting tokens as
described above removes the stall. A small regression fixture times out with the former exporter
and passes with the preparation step; a second test checks that unfinished work is rejected without
cancelling it.

The first resulting native snapshot was 220,474,028 bytes, exceeding the existing 128 MiB wire limit.
Compression reduces its transfer to about 46.6 MB. The
[migration report](benchmarks/snapshot-corpus-migration-2026-10-21.json) records all four proofs solved
after 12 transfers, with all 36 local/imported candidate outcomes agreeing and all imported mappings
released. Copies took 6.5–8.6 seconds on this machine. That cost matters: a handful of cheap branches
cannot amortize migration.

To reproduce with the prepared corpus and a Beam plugin built for its exact toolchain:

```bash
python3 tests/benchmark-snapshot-corpus.py \
  --corpus /path/to/beam-bench/work/tasks.json \
  --project /path/to/beam-bench/projects/PrimeNumberTheoremAnd \
  --runtime /path/to/lean-beam-built-for-4.33.1 \
  --output /tmp/beam-corpus-sanity
```

The default phases are pinned replay, explicit migration, and stealing-enabled replay. Use
`--phases migration --limit 1` for a bounded migration check. The harness writes a report and worker
logs even when a phase fails, and never overwrites an existing output directory.

For controlled throughput measurements, add `--phases migration scaling --limit 1 --repeat 6
--batch-jobs 3 384`. The scaling phase repeats the three frozen candidates at the same initial proof
state. This is an explicit fan-out microbenchmark using real tactics and environments, not a search
trace or an end-to-end theorem-proving result. It compares:

- one active worker;
- the same multi-worker allocation with affinity pinned to the parent;
- that allocation with work stealing enabled.

Every run creates a fresh parent and coordinator, with empty export and replica caches. The same
untimed calibration estimates candidate costs in every mode. Timed completion includes scheduling,
export, compression, network transfer, import, and all candidate responses. File opening is reported
separately; handle setup and calibration are excluded from the measured batch. Six trials balance
all mode-order permutations;
the random seed, source/runtime fingerprints, per-trial timings, candidate agreement, and transfer
counts are recorded. Reports include paired pinned/stealing ratios and a descriptive 95% bootstrap
interval over trial pairs. A speedup attributed to migration requires actual imports in every
stealing trial. Three cheap candidates provide the control where copies should be avoided.

The [six-trial scaling run](benchmarks/snapshot-corpus-scaling-2026-10-21.json) used two workers with
two CPUs each on the same host. For 384 evaluations, median batch times were 47.184 seconds with one
worker, 47.086 seconds with pinned affinity, and 30.049 seconds with stealing. The geometric mean of
paired pinned/stealing ratios was **1.554×**, with a descriptive paired-bootstrap interval of
**1.536–1.577×**. Every stealing trial exported and imported exactly once and split the completed
work approximately evenly. For three evaluations, pinned and stealing medians were 0.349 and
0.353 seconds, with zero transfers in all trials. That small difference does not establish a
performance benefit.

Initial transfer estimates also need evaluation. The default pool starts with a one-second copy
estimate and a 0.25-second source stall estimate, which are optimistic for this project. In the
divisible-work model, a beneficial split requires `W > C + S`; these corpus measurements put that
threshold near 15 seconds of queued work, before whole-job boundaries and the safety margin.
The existing `Pool` parameters `cold_transfer_seconds` and `source_stall_seconds` accept measured
priors. For a separate benchmark with a measured prior, pass
`--transfer-profile /path/to/passing-migration-report.json`. The harness checks the worker/runtime
identity and uses the median copy/export times from that report. It records the profile hash and
the time spent on those earlier copies. Profiling is outside the timed batch and must be charged
when reporting cold, end-to-end performance; it is not free startup work.

An intermediate queue makes that distinction concrete. With 24 evaluations and the
[default prior](benchmarks/snapshot-corpus-medium-default-2026-10-21.json), pinned batches took a
median 2.835 seconds, while stealing took 8.843 seconds. All six stealing trials copied a snapshot:
the optimistic estimate admitted a transfer whose actual cost exceeded the queued work. This is a
performance regression, even though every candidate result remained correct.

Using a [measured prior](benchmarks/snapshot-corpus-medium-profiled-2026-10-21.json) of 7.964 seconds
for copy and 7.008 seconds for source export, all six intermediate-queue trials avoided transfer.
Pinned and stealing medians were 3.014 and 2.890 seconds. The paired ratio interval includes 1, so
this establishes avoidance of the costly copy, not a speedup. The prior came from three earlier
copies totaling 24.558 seconds, excluding their document setup and verification work. Those costs
must be accounted for when deciding whether profiling is worthwhile for a short-lived deployment.
The pool already accepts these measured costs; the harness uses that existing interface without
changing the scheduling rule or inflating candidate estimates.

| Queued evaluations | Transfer prior | Pinned median | Stealing median | Copies per stealing trial |
| ---: | --- | ---: | ---: | ---: |
| 3 | Default | 0.349 s | 0.353 s | 0 |
| 24 | Default | 2.835 s | 8.843 s | 1 |
| 24 | Measured separately | 3.014 s | 2.890 s | 0 |
| 384 | Default | 47.086 s | 30.049 s | 1 |

Use the same `scaling` command with `--batch-jobs 24` to reproduce the intermediate queue.
For its measured-prior variant, add `--transfer-profile` pointing to the preceding scaling report
that includes the migration phase. Keep each output directory distinct.

For a broader evaluation, replay a fixed real search trace with unique candidates and dependencies,
then run live search at identical model and tactic budgets. Report time to proof and solved count as
well as throughput, tail latency, transfer bytes, and CPU-hours. Separate warm search time from cold
document/runtime setup. Sweep worker count and queue width, and include balanced queues, skewed queues,
and sequential chains. Same-machine containers establish process isolation and a local cost model;
cross-host runs must measure their actual bandwidth and latency before claiming distributed scaling.

### Transfer stages and configurable workloads

Worker export replies include `exportStagesSeconds` for `nativeExport`, `read`, `compress`,
`dependencies`, `libraries`, and `checksum`. Import replies include `importStagesSeconds` for
`checksum`, `context`, `dependencies`, `libraries`, `residentCheck`, `decompress`, `write`, and
`nativeImport`, along with `importSeconds`. These are non-overlapping wall-clock measurements.
The native stages include the LSP round trip and native request preparation. Within native export,
`nativeExportStagesSeconds` separates `preparation` (identity and completed-reporting checks) from
`save` (Lean's compacted-region save and compactor cleanup). These are wall times, not CPU times,
and are subdivisions of `nativeExport`, not additional costs. The corpus migration report records `exportRpcSeconds` and the
complete `copySeconds`. The remainder outside worker export/import includes transport, queueing,
framing, and cleanup; it is not a measurement of network time alone.

Use `--tactics FILE.json` with a JSON array of alternative tactic strings to vary the workload.
The original corpus tactic is always included, and source files remain read-only. The
[bounded automation set](benchmarks/snapshot-tactics-bounded.json) includes simplification,
arithmetic, and heartbeat-limited search. A rejected tactic is a measured evaluation, not a solved
proof. Migration compares local/imported outcomes; scaling compares every result with calibration.
Each scaling run records the exact candidates, calibration cost, and parent setup cost separately
from the timed batch. `documentOpenSeconds` records concurrent worker preparation per file.

`--theorems NAME ...` selects tasks before `--limit`; `--max-steps` controls the accepted proof
length. Use `--copy-seconds C --source-stall-seconds S` to exercise the existing scheduler parameters.
A supplied `--transfer-profile` overrides both values with its measured medians. These options
permit sensitivity tests across projects, tactic mixes, and queue sizes without changing the
scheduling algorithm or requiring a particular agent trace.

The [PFR report](benchmarks/snapshot-tactics-pfr-2026-10-21.json) and
[PNT report](benchmarks/snapshot-tactics-pnt-2026-10-21.json) use that bounded automation set plus
the original tactic: nine candidates at each state. Both projects use the same conservative initial
estimates, `C = 8 s` and `S = 7 s`, without changing the pool defaults or the scheduling algorithm.
Each project has six balanced trials at 9, 72, and 576 evaluations, comparing one worker, two
workers with pinned affinity, and two workers with stealing. Each worker has two CPUs and 8 GiB;
the workers run on the same host. The 72- and 576-job batches repeat the nine candidates to measure
throughput. They are not recorded agent workloads.

| Project | Evaluations | Pinned median | Stealing median | Paired geometric speedup | Trials with migration |
| --- | ---: | ---: | ---: | ---: | ---: |
| PFR | 9 | 0.306 s | 0.268 s | 1.108× | 0/6 |
| PFR | 72 | 2.202 s | 2.226 s | 1.001× | 0/6 |
| PFR | 576 | 18.073 s | 13.323 s | 1.245× | 4/6 |
| PNT | 9 | 0.677 s | 0.687 s | 0.998× | 0/6 |
| PNT | 72 | 5.348 s | 5.424 s | 0.994× | 0/6 |
| PNT | 576 | 43.651 s | 28.703 s | 1.506× | 6/6 |

All four small/intermediate cases have paired-bootstrap intervals containing 1; none establishes
a speedup. They do establish that these settings avoid the expensive copies seen with optimistic
priors. The wide PNT case improves in every trial, with a descriptive 95% paired-bootstrap interval
of 1.492–1.522×. The wide PFR policy comparison has an interval of 1.123–1.362×, but only four
trials migrate. Its conservative estimates sometimes leave a profitable queue local; the aggregate
policy gain must not be described as six successful migration speedups. The earlier requirement
for attributing a result to migration in every trial remains unmet for PFR.

The migration phase solves each selected three-step proof after three transfers. All 54
local/imported candidate comparisons across the two projects agree, and all imported mappings are
released. Stage medians over the three transfers in each project are:

| Measurement | PFR | PNT |
| --- | ---: | ---: |
| Uncompressed snapshot | 144.2 MB | 221.1 MB |
| Compressed snapshot | 29.7 MB | 46.8 MB |
| Completed-reporting preparation | 0.011 s | 0.031 s |
| Native save and compactor cleanup | 3.109 s | 5.814 s |
| Compression | 0.506 s | 0.791 s |
| Native import request | 0.059 s | 0.087 s |
| Complete copy | 4.498 s | 8.156 s |

Most copy time is in native save, while preparation is measured in milliseconds. This does not
justify weakening completed-reporting checks or replacing Lean's serializer. The measurements
identify where a future optimization would need to help; they do not demonstrate such an
optimization. Worker document opening took 7.909 s for PFR and 25.643 s for PNT, separately from
the warm batch timings. Large source-file elaboration costs alone do not imply expensive warm
tactic evaluations, because Beam already reuses the elaborated context.

To reproduce either project, select its prepared runtime and project directory:

```bash
python3 tests/benchmark-snapshot-corpus.py \
  --corpus /path/to/beam-bench/work/tasks.json \
  --project /path/to/prepared/project --runtime /path/to/matching/beam-runtime \
  --output /tmp/beam-tactic-sweep \
  --phases migration scaling --limit 1 --repeat 6 --batch-jobs 9 72 576 \
  --tactics docs/benchmarks/snapshot-tactics-bounded.json \
  --copy-seconds 8 --source-stall-seconds 7
```

PFR uses Lean 4.35.0-rc2; PNT uses Lean 4.33.1. These limited runs support retaining the migration
capability for sufficiently wide continuation queues. Agent evaluation should also measure the
simpler case of independent `runAt` command batches, whose parallel execution needs no snapshot
transfer, before attributing an agent-level improvement to migration.

### Full saved-corpus runners

`tests/qualify-snapshot-corpus.py` visits every saved proof for the selected project, without a
proof-length filter. It first replays each chain locally, checking the original step, `simp`, and
`linarith` from each parent. A separate replay of each passing chain moves the continuing state at
the first, middle, and last step, then compares every candidate with the local reference. The
report records rejected fragments, export/import errors, unvisited suffixes, resource observations,
source hashes, and cleanup outcomes. Each source file gets a fresh pair of workers. A file that
cannot be prepared is recorded once; dependent proofs remain explicitly unrun.

```bash
python3 tests/qualify-snapshot-corpus.py \
  --corpus /path/to/saved-tasks.json \
  --project /path/to/prepared/project --runtime /path/to/matching/beam-runtime \
  --output /tmp/beam-qualification

python3 tests/benchmark-snapshot-workloads.py \
  --corpus /path/to/saved-tasks.json --qualification /tmp/beam-qualification/report.json \
  --project /path/to/prepared/project --runtime /path/to/matching/beam-runtime \
  --output /tmp/beam-workloads --workers 1 2 4 8 --trials 3
```

The second runner includes every proof that passed both qualification phases, and lists the
excluded cases. It checks implementation and input hashes before using the reference outcomes.
The mixed workload interleaves 72 candidates per distinct root in seeded shuffled rounds, cycling
through the original first step, `simp`, and `linarith`. The tree workload advances all saved proof
chains concurrently, checking those three siblings at each depth. `--workloads mixed --repeats 384`
selects a wider interleaved workload; admission must fit the default 4,096-job queue budget.

Concurrent root admission uses the ordinary least-loaded routing policy. Sequential admission
concentrates roots on one worker, providing an explicit affinity-skew control. Actual root placement
is recorded. Both placements compare pinned and stealing modes in shuffled order. Every trial has
fresh handles, coordinator state, and an empty export cache. Warm document preparation is shared
between paired trials and reported separately. These are replay workloads, not LLM search traces.

Initial transfer estimates use the largest observed copy and export times in qualification;
candidate estimates use local qualification service times. They are measurements under the
qualification resource limits, not guarantees under the scaling allocation. The runner records
wall time, candidate and chain completion, latency, CPU use and throttling, sampled memory,
transfer counts and bytes, failures, and cleanup. Optional `--cpu-sets` fixes each worker's Docker
CPU affinity; the report also records the driver's affinity. Worker counts that cannot prepare
their documents within the host memory reserve are reported as preparation failures.

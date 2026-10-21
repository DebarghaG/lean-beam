# Distributed proof snapshots

This experimental pool runs Beam proof continuations across Linux workers. It builds on the
upstream Lean Beam implementation, using the existing `runAt`, `runWith`, and handle-release
semantics. The coordinator is a small Python library; workers communicate over TCP and call Beam's
LSP plugin. The installed CLI and MCP interfaces continue to use the local broker.

## State transfer

Beam retains the complete core, meta, term, and tactic state. Export uses Lean's
`CompactedRegion.save` with closure relocation, the same machinery used by Lean's incremental
snapshots. Imported module regions are dependencies of the snapshot, rather than serialized again.
Only request cancellation and editor reporting state are cleared. Proof environments, local
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

Greenbeam can keep one coordinator in its search process and replace pinned continuation calls with
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

Defaults bound the queue to 4,096 jobs, the coordinator's export cache to 256 MiB, each worker to
4,096 handles, 16 open documents and 256 MiB of imported mappings, and a single transfer to 128 MiB.
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
Greenbeam corpus, establish cross-machine WAN performance, or provide an MLSys evaluation. Real
search traces, larger imported environments, skewed task durations, and heterogeneous workers need
separate measurements before making those claims.

# Worker pool

Beam can send speculative Lean requests to a pool of workers. Agents keep using the same CLI/MCP
commands and continuation handles. Independent requests can use different workers; continuations
stay with the worker that created them. Sync, navigation, and save still run locally.

This is optional and experimental. Build your project and dependencies before copying them to
workers. Attachment needs matching sources and compiled imports, including `.olean` files. After
that, Beam sends source edits, new files, and changed build artifacts automatically. Each client
gets private worker workspaces over the prepared dependencies. Agents keep their usual edit,
sync, probe, and save workflow; edited imports still need the usual save and refresh steps.
Use separate local worktrees for agents that edit independently.
The example project imports only the Lean standard library, already included in the image.
Pool clients and workers require Linux; the pool scripts also require Python 3.11 or later.

## Run locally

Build Beam and choose a shared token:

```bash
lake build beam-cli beam-daemon lean-beam-mcp Beam.LSP:shared
export BEAM_POOL_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
```

Run these in separate terminals with the same token, replacing `/path/to/project` with a prepared
Lean project. Each worker admits one request at a time by default.

```bash
scripts/beam-pool worker --root /path/to/project --listen 127.0.0.1:9001
scripts/beam-pool worker --root /path/to/project --listen 127.0.0.1:9002
scripts/beam-pool serve --worker 127.0.0.1:9001 --worker 127.0.0.1:9002
```

Attach the matching local project and start the usual Beam session:

```bash
scripts/beam-pool attach --root /path/to/project --config "$PWD/.beam/pool.json"
export BEAM_LEAN_POOL_CONFIG="$PWD/.beam/pool.json"
scripts/lean-beam --root /path/to/project serve
```

For MCP, set `BEAM_LEAN_POOL_CONFIG` and `BEAM_POOL_TOKEN` in the MCP server's environment instead.
Restart an existing session after changing either setting. Attachment checks file hashes; it does
not copy or build the project. Unattached projects continue to run locally.
Keep the generated `.inputs.json` files beside the config; Beam loads each workspace's inputs on
first use, and matching workspaces share the same file.

`scripts/beam-pool status` shows available workers, queued requests, and retained handles.
Use `--help` on each command for limits and endpoint options.

## Containers

The example image includes the small project in `tests/pool_project`. Replace it with your prepared
project for real use.

```bash
docker build -f deploy/beam-pool/Dockerfile -t beam-pool:local .
docker compose -f deploy/beam-pool/compose.yaml up -d --scale worker=4
```

The Compose file gives each worker a one-CPU quota. This does not pin it to a particular core.
Attach the matching local project as above.

## Kubernetes

With [kind](https://kind.sigs.k8s.io/docs/user/quick-start/) and `kubectl` installed, use the image
built above and the same `BEAM_POOL_TOKEN`:

```bash
kind create cluster --name beam-pool
kind load docker-image beam-pool:local --name beam-pool
kubectl create namespace beam-pool
kubectl -n beam-pool create secret generic beam-pool-auth --from-literal=token="$BEAM_POOL_TOKEN"
kubectl -n beam-pool apply -f deploy/beam-pool/kubernetes.yaml
kubectl -n beam-pool rollout status deployment/beam-workers
kubectl -n beam-pool rollout status deployment/beam-pool
kubectl -n beam-pool port-forward service/beam-pool 9000:9000
```

Keep port-forward running and attach the matching local project as above. Scale workers with
`kubectl -n beam-pool scale deployment/beam-workers --replicas=6`; keep one gateway.
Remove the local cluster with `kind delete cluster --name beam-pool` when finished.
For a remote cluster, publish the image and use the same immutable digest for both deployments in
the [manifest](../deploy/beam-pool/kubernetes.yaml).

Tested with `tests/pool_project` on a three-node kind v0.33.0 cluster running Kubernetes v1.37.0:
CLI/MCP proofs, six simultaneous requests, scaling 4 → 6 → 2 workers, continuation handles across
scale-up, and worker pod replacement. Handles on a removed worker are invalidated.

## Limits

- Toolchain and Lake configuration changes require a new prepared environment and attachment.
  Ordinary source edits and module saves do not.
- Run one gateway. Restarting it loses handles. Removing a worker loses its handles too; there is
  no automatic migration or graceful scale-down controller.
- Cancellation and timeouts allow three seconds for execution to stop, then retire its private
  workspace if needed. An MCP process failure still loses every handle on that worker. Linear
  handles may be consumed once execution starts. Queued cancellations preserve handles.
- Idle handles expire; explicit release frees them sooner. Closing a local document invalidates
  its handles, but remote cleanup waits for expiry.
- Remote progress and diagnostic events are not streamed. Final results still include messages.
- Workers cache up to four private workspaces by default. Live handles keep their workspace in
  memory. Use `--max-contexts` and `--max-project-bytes` to set workspace and file-cache budgets;
  requests fail with `resourceExhausted` when live state fills them. Each revision allows 8,192
  changed files. Unused state expires or is evicted to make room.
- Attachment hashes the prepared inputs twice. File watching avoids rescanning unchanged projects;
  edits scan metadata and transfer changed content. The first probe on a worker still loads its
  Lean environment. Pending socket operations poll with delays that grow from 1 to 10 ms.
- Transport uses a shared token over plain TCP. Keep it on a trusted private network.

## Tests

```bash
python3 tests/test-beam-pool.py
python3 tests/test-beam-pool-integration.py
python3 tests/test-beam-pool-compose.py  # needs the Docker image and free port 9000
```

The CLI integration test caches runtime bundles in the ignored `.beam/pool-integration-bundles`.

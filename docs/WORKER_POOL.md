# Worker pool

Beam can send speculative Lean requests to a pool of workers. Agents keep using the same CLI/MCP
commands and continuation handles. Independent requests can use different workers; continuations
stay with the worker that created them. Sync, navigation, and save still run locally.

This is optional and experimental. Workers need identical, prepared project files and dependencies.
The pool scripts require Linux and Python 3.11 or later.

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
Attach the matching local project as above. A [Kubernetes example](../deploy/beam-pool/kubernetes.yaml)
is also provided; set the image digest and create its `beam-pool-auth` Secret before applying it.
The Kubernetes example has not been tested on a live cluster.

## Limits

- Project inputs must stay unchanged. After edits, prepare new workers, attach again, and restart
  Beam. Requests check document versions, source content, and other prepared files' metadata.
- Run one gateway. Restarting it loses handles. Removing a worker loses its handles too; there is
  no automatic migration or graceful scale-down controller.
- Cancellation retires the affected worker. Idle handles expire; explicit release frees them
  sooner. Closing a local document invalidates its handles, but remote cleanup waits for expiry.
- Transport uses a shared token over plain TCP. Keep it on a trusted private network.

## Tests

```bash
python3 tests/test-beam-pool.py
python3 tests/test-beam-pool-integration.py
python3 tests/test-beam-pool-compose.py  # needs the Docker image and free port 9000
```

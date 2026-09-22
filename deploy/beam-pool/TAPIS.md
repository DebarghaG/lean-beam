# Tapis

Agents run the usual Beam CLI/MCP on their own Linux machine. The default Tapis plan runs one
gateway and one-CPU workers. A local stunnel process connects Beam to Tapis's TLS/SNI ingress.

Build and publish the [pool image](Dockerfile) with a fixed release tag. Have a Tapis
administrator allowlist its repository. Workers for one project need the same architecture and
runtime. Use the same Lean version and compatible native artifacts on the agent machine.
The image includes Lean 4.33 and a prepared Mathlib environment at `/project`, without an
example proof. Use the same image and platform for workers and agent-side MCP. Attach a copy of
`/project` before adding private proof files; Beam sends source edits to the workers.
Put proofs in `Proofs.lean` or under `Proofs/`, the workspace's registered Lean library.
Private dependencies need an identical prepared snapshot on both sides.
Workloads, credentials, and benchmark results stay outside the image.

```bash
docker build -f deploy/beam-pool/Dockerfile -t YOUR_ACCOUNT/lean-beam:YOUR_RELEASE .
BEAM_POOL_IMAGE=YOUR_ACCOUNT/lean-beam:YOUR_RELEASE BEAM_POOL_TEST_MATHLIB=1 \
  BEAM_POOL_TEST_CLIENTS=4 BEAM_POOL_TEST_MEMORY=12g BEAM_POOL_TEST_TIMEOUT=1200 \
  python3 tests/test-beam-pool-tls.py
```

## Create the pool

Put `TAPIS_URL=https://tacc.tapis.io`, `TAPIS_ACCESS_TOKEN`, and a random `BEAM_POOL_TOKEN`
in a private environment file outside Git. Generate the pool token once and retain it:

```bash
mkdir -p .beam
(umask 077; set -C; python3 -c 'import secrets; print("BEAM_POOL_TOKEN=" + secrets.token_hex(32))' > .beam/pool.env)
python3 deploy/beam-pool/tapis.py plan \
  --image YOUR_ACCOUNT/lean-beam:YOUR_RELEASE --prefix beam \
  > .beam/tapis-plan.json
python3 deploy/beam-pool/tapis.py --env-file /path/to/tapis.env --env-file .beam/pool.env \
  up .beam/tapis-plan.json --workers 2
```

The helper checks image approval and existing pod definitions before creating resources.
It creates a shared Tapis secret, two workers, and the gateway. Rerunning with `--workers 4`
adds workers without restarting the gateway. The plan reserves addresses for up to 20 workers;
it only creates the requested number. It never stops or replaces existing pods.

Workers default to 12 GiB for Mathlib and up to four cached client workspaces. Use `--memory MIB`
to change this after measuring your workload. For other prepared dependencies, add
`--snapshot SNAPSHOT_ID`; the snapshot must contain the prepared project at its root.
The Tapis plan allows 105 seconds per worker request, including workspace initialization.
The client's 120-second deadline includes queueing, so bursts of new Mathlib workspaces can
still time out. Measure cold starts separately from warm proof requests.
Pods stop after 12 hours by default; change `time_to_stop_default` to `-1`
in the plan before creating a long-lived service. Gateway restarts lose all handles.

`--site` and `--tenant` default to `tacc`. Worker addresses use Tapis's current internal service
naming convention, `pods-SITE-TENANT-POD`. Confirm gateway-to-worker DNS and connectivity in
your tenant. Individual workers use `local_only` networking. Do not put one load-balanced
address in front of all workers: continuation handles belong to individual workers.

## Connect the agents

```bash
python3 deploy/beam-pool/tapis.py --env-file /path/to/tapis.env status .beam/tapis-plan.json
```

Copy [stunnel.conf](stunnel.conf) locally and replace every `GATEWAY_HOST` with the gateway's
returned hostname. Install stunnel and run `stunnel /path/to/stunnel.conf`. It verifies the
gateway certificate and listens only on loopback. The CA bundle path in the example is for
Debian/Ubuntu. Then load the same pool token and attach the project:

```bash
set -a
. .beam/pool.env
set +a
scripts/beam-pool attach --root /path/to/project --endpoint 127.0.0.1:9000 \
  --config "$PWD/.beam/pool.json"
export BEAM_LEAN_POOL_CONFIG="$PWD/.beam/pool.json"
scripts/beam-pool status --endpoint 127.0.0.1:9000
```

Start the usual Beam CLI/MCP with those environment variables. Agents need no Tapis token;
that token is only for deployment. Tapis's HTTP `tapis_auth` does not authenticate this TCP
endpoint. Beam authenticates requests with the shared pool token. Use separate local worktrees
for independently editing agents.

## Check it

Tapis `AVAILABLE` only means the container is running. Require two ready workers in Beam's
status output, then test proofs, cancellation, and continuations through the external endpoint.

```bash
python3 tests/test-beam-pool-tapis.py
BEAM_POOL_IMAGE=YOUR_ACCOUNT/lean-beam:YOUR_RELEASE python3 tests/test-beam-pool-tls.py
```

The Docker test uses two workers, Traefik v3.6.2, stunnel, and two MCP clients by default. It checks
concurrent proofs, rejected tactics, queued/running cancellation, retained handles, and source
isolation. It removes its containers afterward. It does not contact Tapis.

For a dedicated two-worker Tapis pilot, run the same test with `--client`, setting
`BEAM_POOL_TOKEN`, `BEAM_POOL_TEST_PROJECT` to the matching prepared environment, and optionally
`BEAM_POOL_TEST_ENDPOINT` to the local adapter address. Set `BEAM_POOL_TEST_MATHLIB=1` for Mathlib.
That mode also checks native-code tactics and importing a saved checkpoint on a worker.
This needs the test script, its fixture, and matching Beam binaries on the agent machine.

Tested on the live `tacc` tenant on 2026-09-21 with two one-CPU workers. The `tests/pool_project`
pilot covered eight concurrent proofs, rejected tactics, cancellation, handles, and isolated edits.
TLS verification, internal worker DNS, and repeatable `up` passed.

Platform reference: [Tapis Pods](https://tapis.readthedocs.io/en/latest/technical/pods.html).

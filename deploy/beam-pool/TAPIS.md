# Tapis

Agents run the usual Beam CLI/MCP on their own Linux machine. Tapis runs one gateway and
one-CPU workers. A local stunnel process connects Beam's TCP client to Tapis's TLS/SNI ingress.

Build and publish the [pool image](Dockerfile) with a fixed release tag. Have a Tapis
administrator allowlist its repository. Workers for one project need the same architecture and
runtime. Use the same Lean version and compatible native artifacts on the agent machine.
The bundled `/project` is a small public smoke-test project.
Keep private projects in a Tapis snapshot, not in a public image; copy the same prepared
sources and build artifacts to the agent machine before attaching.

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

For a real project, add `--snapshot SNAPSHOT_ID --memory MIB` when making the plan.
The snapshot must contain the prepared project at its root. The 2048 MiB default is for the
small test project. Pods stop after 12 hours by default; change `time_to_stop_default` to `-1`
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

The Docker test uses two workers, Traefik v3.6.2, stunnel, and two real MCP agents. It checks
concurrent proofs, rejected tactics, queued/running cancellation, retained handles, and source
isolation. It removes its containers afterward. It does not contact Tapis.

For a dedicated two-worker Tapis pilot, run the same test with `--client`, setting
`BEAM_POOL_TOKEN`, `BEAM_POOL_TEST_PROJECT` to a copy of `tests/pool_project`, and optionally
`BEAM_POOL_TEST_ENDPOINT` to the local adapter address. This needs a built Beam checkout.

Tested on the live `tacc` tenant on 2026-09-21 with two one-CPU workers and two external MCP
agents: eight concurrent proofs, rejected tactics, queued/running cancellation, retained handles,
and isolated source edits. TLS verification and internal worker DNS passed. Repeating `up` left
the running pods unchanged. This used only `tests/pool_project`; large projects and scale-up
remain untested on Tapis.

Platform reference: [Tapis Pods](https://tapis.readthedocs.io/en/latest/technical/pods.html).

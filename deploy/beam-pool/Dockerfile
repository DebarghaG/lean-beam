# syntax=docker/dockerfile:1
FROM ubuntu:24.04 AS toolchain
ARG TARGETARCH
ARG LEAN_VERSION=4.33.0
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl zstd build-essential git python3
RUN case "$TARGETARCH" in arm64) platform=linux_aarch64 ;; amd64) platform=linux ;; *) exit 1 ;; esac && \
    mkdir -p /opt/lean && \
    curl --fail --location --retry 3 \
      "https://github.com/leanprover/lean4/releases/download/v${LEAN_VERSION}/lean-${LEAN_VERSION}-${platform}.tar.zst" \
      -o /tmp/lean.tar.zst && \
    tar --zstd -xf /tmp/lean.tar.zst --strip-components=1 -C /opt/lean && \
    rm /tmp/lean.tar.zst
ENV PATH="/opt/lean/bin:${PATH}"

FROM toolchain AS build
WORKDIR /opt/beam
COPY Beam.lean lakefile.lean lake-manifest.json lean-toolchain LICENSE ./
COPY Beam ./Beam
COPY scripts/beam_pool ./scripts/beam_pool
RUN test "$(cat lean-toolchain)" = "leanprover/lean4:v${LEAN_VERSION}" && \
    lake build lean-beam-mcp beam-daemon Beam.LSP:shared

FROM toolchain AS mathlib-build
WORKDIR /project
COPY deploy/beam-pool/mathlib/lakefile.toml deploy/beam-pool/mathlib/lake-manifest.json deploy/beam-pool/mathlib/lean-toolchain ./
RUN python3 - <<'PY'
import json
import subprocess
from pathlib import Path

for package in json.loads(Path("lake-manifest.json").read_text())["packages"]:
    directory = Path(".lake/packages") / package["name"]
    subprocess.run(["git", "init", "-q", str(directory)], check=True)
    def git(*args):
        subprocess.run(["git", "-C", str(directory), *args], check=True)
    git("remote", "add", "origin", package["url"])
    git("fetch", "-q", "--depth=1", "origin", package["rev"])
    git("checkout", "-q", "--detach", "FETCH_HEAD")
PY
RUN test "$(cat lean-toolchain)" = "leanprover/lean4:v${LEAN_VERSION}" && \
    lake exe cache get && \
    test "$(cat .lake/packages/mathlib/lean-toolchain)" = "$(cat lean-toolchain)"
# Lake needs the generated build files when an editor opens a proof.
RUN rm -f /project/.lake/packages/mathlib/.lake/build/bin/cache*
RUN python3 - <<'PY'
import subprocess
from pathlib import Path

for package in Path(".lake/packages").iterdir():
    subprocess.run(["git", "-C", str(package), "sparse-checkout", "set", "--no-cone",
        "/*", "!/.github/", "!/doc/", "!/docs/", "!/scripts/", "!/test/", "!/tests/",
        "!/MathlibTest/"], check=True)
PY
COPY tests/pool_mathlib/Proofs.lean /tmp/Proofs.lean
RUN --network=none lake --no-build setup-file /tmp/Proofs.lean > /tmp/setup.json && \
    lake env lean /tmp/Proofs.lean

FROM ubuntu:24.04 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends python3 libgmp10 libuv1t64 \
    ca-certificates git stunnel4 && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/lean /opt/lean
COPY --from=build /opt/beam/.lake/build/bin/lean-beam-mcp /opt/beam/.lake/build/bin/beam-daemon /opt/beam/.lake/build/bin/
COPY --from=build /opt/beam/.lake/build/lib/libbeam_Beam_LSP.so /opt/beam/.lake/build/lib/
COPY --from=build /opt/beam/LICENSE /opt/beam/
COPY --from=build /opt/beam/scripts /opt/beam/scripts
COPY --from=mathlib-build --chown=1000:1000 /project /project
ENV PATH="/opt/lean/bin:${PATH}"
WORKDIR /opt/beam
USER 1000:1000
ENTRYPOINT ["python3", "-m", "scripts.beam_pool"]
CMD ["worker", "--root", "/project", "--listen", "0.0.0.0:9001"]

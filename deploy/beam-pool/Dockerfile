# syntax=docker/dockerfile:1
FROM ubuntu:24.04 AS build
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
WORKDIR /opt/beam
COPY . .
RUN test "$(cat lean-toolchain)" = "leanprover/lean4:v${LEAN_VERSION}" && \
    lake build lean-beam-mcp beam-daemon beam-cli Beam.LSP:shared

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends python3 libgmp10 libuv1t64 \
    ca-certificates git && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/lean /opt/lean
COPY --from=build /opt/beam /opt/beam
COPY --chown=1000:1000 tests/pool_project /project
ENV PATH="/opt/lean/bin:${PATH}"
WORKDIR /opt/beam
USER 1000:1000
ENTRYPOINT ["python3", "-m", "scripts.beam_pool"]
CMD ["worker", "--root", "/project", "--listen", "0.0.0.0:9001", "--warm-file", "Workload.lean"]

FROM python:3.12-slim-bookworm AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
COPY scripts/beam_pool /opt/worker/beam_pool
ENV PYTHONPATH=/opt/worker \
    PATH=/opt/lean/bin:$PATH \
    LEAN_PATH=/project/.lake/build/lib/lean \
    LD_LIBRARY_PATH=/opt/beam/lib:/opt/lean/lib/lean:/opt/lean/lib
WORKDIR /project
ENTRYPOINT ["python3", "-m", "beam_pool", "worker"]
CMD ["--project", "/project", "--plugin", "/opt/beam/lib/libbeam_Beam_LSP.so", "--host", "0.0.0.0", "--port", "8765"]

FROM runtime AS worker-a
LABEL beam.snapshot.test-variant="a"

FROM runtime AS worker-b
ENV MALLOC_ARENA_MAX=2
LABEL beam.snapshot.test-variant="b"

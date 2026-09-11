"""Start workers, run the pool, and attach Beam projects."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import sys
from pathlib import Path

from .pool import Pool
from .protocol import Failure
from .transport import RpcServer, rpc
from .worker import Worker


def endpoint(value: str) -> tuple[str, int]:
    try:
        host, port = value.rsplit(":", 1)
        port = int(port)
        if not host or not 0 <= port <= 65535:
            raise ValueError()
        return host, port
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected host:port") from error


def positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[2]
    p = argparse.ArgumentParser(prog="beam-pool", description="Beam worker pool for prepared Lean projects")
    sub = p.add_subparsers(dest="command", required=True)
    worker = sub.add_parser("worker")
    worker.add_argument("--root", type=Path, required=True)
    worker.add_argument("--mcp", type=Path, default=root / ".lake/build/bin/lean-beam-mcp")
    worker.add_argument("--plugin", type=Path, default=root / ".lake/build/lib/libbeam_Beam_LSP.so")
    worker.add_argument("--lean", type=Path, default=Path(shutil.which("lean") or "lean"))
    worker.add_argument("--listen", type=endpoint, default=("127.0.0.1", 9001))
    worker.add_argument("--slots", type=positive, default=1)
    worker.add_argument("--timeout", type=positive, default=60)
    worker.add_argument("--max-handles", type=positive, default=4096)
    worker.add_argument("--max-contexts", type=positive, default=4)
    worker.add_argument("--max-project-bytes", type=positive, default=1024 * 1024 * 1024)
    worker.add_argument("--idle-ttl", type=positive, default=1800)
    worker.add_argument("--warm-file", action="append", default=[])
    pool = sub.add_parser("serve")
    pool.add_argument("--worker", type=endpoint, action="append", default=[])
    pool.add_argument("--worker-dns", type=endpoint)
    pool.add_argument("--listen", type=endpoint, default=("127.0.0.1", 9000))
    pool.add_argument("--max-queue", type=positive, default=256)
    pool.add_argument("--per-group", type=positive, default=64)
    pool.add_argument("--max-handles", type=positive, default=4096)
    pool.add_argument("--max-project-bytes", type=positive, default=1024 * 1024 * 1024)
    pool.add_argument("--timeout", type=positive, default=120)
    pool.add_argument("--handle-ttl", type=positive, default=900)
    status = sub.add_parser("status", help="show worker availability and pool counters")
    status.add_argument("--endpoint", type=endpoint, default=("127.0.0.1", 9000))
    attachment = sub.add_parser("attach", help="bind ordinary Beam CLI/MCP to a verified pool snapshot")
    attachment.add_argument("--root", type=Path, required=True)
    attachment.add_argument("--endpoint", type=endpoint, default=("127.0.0.1", 9000))
    attachment.add_argument("--config", type=Path, required=True)
    attachment.add_argument("--snapshot")
    health = sub.add_parser("health", help="exit successfully only when the service is ready")
    health.add_argument("--endpoint", type=endpoint, default=("127.0.0.1", 9001))
    return p


async def main(args: argparse.Namespace) -> None:
    token = os.environ.get("BEAM_POOL_TOKEN", "")
    if len(token) < 16:
        raise Failure("invalidParams", "set BEAM_POOL_TOKEN to a capability of at least 16 characters")
    if args.command == "attach":
        from .attach import attach
        print(json.dumps(await attach(args.root, args.endpoint, token, args.config, args.snapshot)))
        return
    if args.command == "health":
        result = await rpc(args.endpoint, token, "info", {}, timeout=2)
        if not result.get("ready", any(w.get("ready") for w in result.get("workers", []))):
            raise Failure("workerLost", "service is not ready")
        return
    if args.command == "status":
        result = await rpc(args.endpoint, token, "info", {})
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "worker":
        service = Worker(args.root, args.mcp, args.lean, args.plugin, slots=args.slots,
                         timeout=args.timeout, max_handles=args.max_handles, idle_ttl=args.idle_ttl,
                         max_contexts=args.max_contexts, max_project_bytes=args.max_project_bytes)
    else:
        if not args.worker and not args.worker_dns:
            raise Failure("invalidParams", "supply at least one --worker or --worker-dns")
        service = Pool(args.worker, token, dns=args.worker_dns, max_queue=args.max_queue,
                       per_group=args.per_group, timeout=args.timeout, max_handles=args.max_handles,
                       handle_ttl=args.handle_ttl, max_project_bytes=args.max_project_bytes)
    server = RpcServer(service.dispatch, token)
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    try:
        await service.start()
        if args.command == "worker":
            async def quiet(_: dict) -> None:
                pass
            for path in args.warm_file:
                await service.dispatch("warm", {"snapshot": service.snapshot.identity,
                    "generation": service.generation, "path": path}, quiet)
        address = await server.start(*args.listen)
        info = {"ready": True, "endpoint": list(address), "role": args.command}
        if args.command == "worker":
            info["snapshot"] = service.snapshot.identity
        print(json.dumps(info), flush=True)
        await stop.wait()
    finally:
        await server.close()
        await service.close()


if __name__ == "__main__":
    try:
        asyncio.run(main(parser().parse_args()))
    except (Failure, ValueError, OSError) as error:
        failure = error if isinstance(error, Failure) else Failure("setupFailed", str(error))
        print(json.dumps({"ok": False, "error": failure.json()}), file=sys.stderr)
        sys.exit(1)

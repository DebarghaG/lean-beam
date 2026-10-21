"""Run a Beam snapshot worker. Start under `lake env` for the target project."""
import argparse
import asyncio
import json
import os
import signal
import ssl

from .worker import Worker


async def serve(args):
    worker = Worker(args.project, args.plugin, os.environ.get("BEAM_POOL_TOKEN", ""),
                    memory_limit=args.memory_mib * 1024 * 1024, handle_limit=args.handles,
                    document_limit=args.documents,
                    request_timeout=args.timeout)
    server = None
    try:
        await worker.start()
        context = None
        if args.cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(args.cert, args.key)
        server = await asyncio.start_server(worker.connected, args.host, args.port, ssl=context)
        print(json.dumps({"host": args.host, "port": server.sockets[0].getsockname()[1],
                          "generation": worker.generation, "runtime": worker.runtime}), flush=True)
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stopped.set)
        await stopped.wait()
    finally:
        if server is not None:
            server.close()
            await server.wait_closed()
        await worker.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["worker"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--handles", type=int, default=4096)
    parser.add_argument("--documents", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--cert")
    parser.add_argument("--key")
    args = parser.parse_args()
    if min(args.memory_mib, args.handles, args.documents, args.timeout) <= 0:
        parser.error("budgets and timeout must be positive")
    if bool(args.cert) != bool(args.key):
        parser.error("--cert and --key must be supplied together")
    asyncio.run(serve(args))


if __name__ == "__main__":
    main()

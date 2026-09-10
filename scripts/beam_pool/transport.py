"""Bounded newline JSON transport, with one request lifetime per TCP connection.

Used internally between the broker, pool, and workers.
Closing a client connection cancels its operation. Progress precedes one terminal reply.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import uuid
from collections.abc import Awaitable, Callable

from .protocol import Failure, MAX_FRAME, canonical, fields, string

Emit = Callable[[dict], Awaitable[None]]
Dispatch = Callable[[str, dict, Emit], Awaitable[dict]]


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


async def read_frame(reader: asyncio.StreamReader) -> dict:
    try:
        line = await reader.readline()
        if not line:
            raise Failure("workerLost", "peer closed the connection")
        if len(line) > MAX_FRAME or not line.endswith(b"\n"):
            raise Failure("invalidParams", "frame exceeds limit or is incomplete")
        value = json.loads(line, object_pairs_hook=unique_object, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not isinstance(value, dict):
            raise ValueError("expected object")
        return value
    except (ValueError, UnicodeError) as error:
        raise Failure("invalidParams", "malformed JSON frame") from error


async def write_frame(writer: asyncio.StreamWriter, value: dict) -> None:
    encoded = canonical(value)
    if len(encoded) + 1 > MAX_FRAME:
        raise Failure("resourceExhausted", "response exceeds frame limit")
    writer.write(encoded + b"\n")
    async with asyncio.timeout(10):
        await writer.drain()


async def rpc(endpoint: tuple[str, int], token: str, op: str, args: dict,
              emit: Emit | None = None, timeout: float = 120) -> dict:
    writer = None
    identity = uuid.uuid4().hex
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(*endpoint, limit=MAX_FRAME)
            await write_frame(writer, {"v": 1, "id": identity, "token": token, "op": op, "args": args})
            while True:
                frame = await read_frame(reader)
                if frame.get("id") != identity:
                    raise Failure("protocolError", "response request id mismatch")
                if "event" in frame:
                    fields(frame, {"id", "event"})
                    if not isinstance(frame["event"], dict):
                        raise Failure("protocolError", "progress must be an object")
                    if emit:
                        await emit(frame["event"])
                    continue
                fields(frame, {"id", "ok"}, {"result", "error"})
                if frame["ok"] is True and set(frame) == {"id", "ok", "result"}:
                    if not isinstance(frame["result"], dict):
                        raise Failure("protocolError", "result must be an object")
                    return frame["result"]
                if frame["ok"] is False and set(frame) == {"id", "ok", "error"}:
                    raise Failure.decode(frame["error"])
                raise Failure("protocolError", "invalid response envelope")
    except TimeoutError as error:
        raise Failure("deadlineExceeded", "request deadline exceeded") from error
    except (ConnectionError, OSError) as error:
        raise Failure("workerLost", "cannot reach worker") from error
    finally:
        if writer:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()


class RpcServer:
    def __init__(self, dispatch: Dispatch, token: str, *, max_connections: int = 512):
        self.dispatch, self.token = dispatch, token
        self.max_connections = max_connections
        self.tasks: set[asyncio.Task] = set()
        self.server: asyncio.Server | None = None

    async def start(self, host: str, port: int) -> tuple[str, int]:
        self.server = await asyncio.start_server(self.handle, host, port, limit=MAX_FRAME)
        return self.server.sockets[0].getsockname()[:2]

    async def close(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        owner = asyncio.current_task()
        self.tasks.add(owner)
        job = watch = None
        identity = ""
        write_lock = asyncio.Lock()

        async def send(value: dict) -> None:
            async with write_lock:
                await write_frame(writer, value)

        async def emit(value: dict) -> None:
            await send({"id": identity, "event": value})

        try:
            async with asyncio.timeout(10):
                frame = await read_frame(reader)
            fields(frame, {"v", "id", "token", "op", "args"})
            identity = string(frame, "id", 128)
            if type(frame["v"]) is not int or frame["v"] != 1 or not identity:
                raise Failure("invalidParams", "unsupported protocol version or empty request id")
            if not hmac.compare_digest(string(frame, "token", 1024).encode(), self.token.encode()):
                raise Failure("unauthorized", "invalid pool capability")
            if len(self.tasks) > self.max_connections:
                raise Failure("overloaded", "connection limit reached")
            if not isinstance(frame["args"], dict):
                raise Failure("invalidParams", "args must be an object")
            job = asyncio.create_task(self.dispatch(string(frame, "op", 32), frame["args"], emit))
            watch = asyncio.create_task(reader.read(1))
            done, _ = await asyncio.wait({job, watch}, return_when=asyncio.FIRST_COMPLETED)
            if job in done:
                result = job.result()
                await send({"id": identity, "ok": True, "result": result})
            else:
                job.cancel()
                await asyncio.gather(job, return_exceptions=True)
        except Failure as error:
            with contextlib.suppress(ConnectionError, OSError):
                await send({"id": identity, "ok": False, "error": error.json()})
        except (ConnectionError, OSError, TimeoutError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception as error:
            import sys
            print(f"pool internal failure: {type(error).__name__}: {error}", file=sys.stderr)
            with contextlib.suppress(ConnectionError, OSError):
                await send({"id": identity, "ok": False,
                            "error": Failure("internalError", "internal pool failure").json()})
        finally:
            for task in (job, watch):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(*(t for t in (job, watch) if t), return_exceptions=True)
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()
            self.tasks.discard(owner)

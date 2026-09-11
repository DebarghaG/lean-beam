"""One owned, multiplexed modern MCP process; never parse human-readable output."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import uuid
from collections import deque
from dataclasses import dataclass

from .protocol import Failure, MAX_FRAME, canonical
from .transport import Emit, read_frame


@dataclass
class Pending:
    result: asyncio.Future
    progress: asyncio.Queue


class Mcp:
    def __init__(self, command: list[str], root: str, threads: int, *, drain_on_cancel: bool = False):
        self.command, self.root, self.threads = command, root, threads
        self.drain_on_cancel = drain_on_cancel
        self.proc = None
        self.pending: dict[str, Pending] = {}
        self.write_lock = asyncio.Lock()
        self.readers: list[asyncio.Task] = []
        self.stderr = deque(maxlen=40)
        self.closed = False
        self.failure: Failure | None = None

    async def start(self) -> None:
        env = dict(os.environ, LEAN_NUM_THREADS=str(self.threads))
        # Workers execute locally; inheriting the client binding would recursively route back.
        env.pop("BEAM_LEAN_POOL_CONFIG", None)
        env.pop("BEAM_POOL_TOKEN", None)
        self.proc = await asyncio.create_subprocess_exec(
            *self.command, cwd=self.root, env=env, start_new_session=True,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, limit=MAX_FRAME)
        self.readers = [asyncio.create_task(self.read_stdout()), asyncio.create_task(self.read_stderr())]

    async def send(self, value: dict) -> None:
        async with self.write_lock:
            if self.failure:
                raise self.failure
            if self.closed or self.proc.returncode is not None:
                raise Failure("workerLost", "MCP process is unavailable")
            self.proc.stdin.write(canonical(value) + b"\n")
            await self.proc.stdin.drain()

    async def read_stderr(self) -> None:
        while line := await self.proc.stderr.readline():
            text = line.decode(errors="replace").rstrip()
            self.stderr.append(text)
            print(f"beam-worker: {text}", file=sys.stderr)

    async def read_stdout(self) -> None:
        failure = Failure("workerLost", "MCP stdout closed")
        try:
            while True:
                message = await read_frame(self.proc.stdout)
                if message.get("jsonrpc") != "2.0":
                    raise Failure("protocolError", "invalid MCP envelope")
                if "method" in message:
                    if "id" in message:
                        raise Failure("protocolError", "unexpected MCP server request")
                    if message["method"] == "notifications/progress":
                        pending = self.pending.get(message.get("params", {}).get("progressToken"))
                        if pending and not pending.progress.full():
                            pending.progress.put_nowait(message["params"])
                    continue
                pending = self.pending.get(message.get("id"))
                if pending is None:
                    raise Failure("protocolError", "unknown MCP response id")
                if not pending.result.done():
                    pending.result.set_result(message)
        except Failure as error:
            failure = error
        except (ConnectionError, OSError) as error:
            failure = Failure("workerLost", str(error))
        except Exception:
            failure = Failure("protocolError", "malformed MCP response")
        finally:
            self.failure = failure
            for pending in self.pending.values():
                if not pending.result.done():
                    pending.result.set_exception(failure)

    async def call(self, name: str, args: dict, emit: Emit | None = None) -> dict:
        identity = uuid.uuid4().hex
        pending = Pending(asyncio.get_running_loop().create_future(), asyncio.Queue(maxsize=8))
        pending.result.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        self.pending[identity] = pending

        async def forward() -> None:
            while True:
                event = await pending.progress.get()
                if emit:
                    await emit(event)

        pump = asyncio.create_task(forward())
        try:
            try:
                await self.send({"jsonrpc": "2.0", "id": identity, "method": "tools/call", "params": {
                    "name": name, "arguments": args, "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientCapabilities": {},
                        "io.modelcontextprotocol/clientInfo": {"name": "beam-pool", "version": "1"},
                        "progressToken": identity}}})
                message = await asyncio.shield(pending.result)
            except asyncio.CancelledError:
                with contextlib.suppress(ConnectionError, Failure):
                    await self.send({"jsonrpc": "2.0", "method": "notifications/cancelled",
                                     "params": {"requestId": identity, "reason": "pool request ended"}})
                if not self.drain_on_cancel:
                    raise
                # Only used with --respond-to-cancellation; Worker bounds the drain lifetime.
                message = await asyncio.shield(pending.result)
            if "error" in message:
                error = message["error"]
                raise Failure("mcpError", error["message"], error)
            result = message.get("result", {})
            structured = result.get("structuredContent")
            if not isinstance(structured, dict) or type(result.get("isError")) is not bool:
                raise Failure("protocolError", "MCP tool result lacks typed content")
            if result["isError"]:
                error = structured.get("error", structured)
                raise Failure(str(error.get("code", "mcpError")),
                              str(error.get("message", "MCP tool failed")), structured)
            return structured
        finally:
            self.pending.pop(identity, None)
            pump.cancel()
            # A cancellation racing a completed response must not lose its retained handle.
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(pump, return_exceptions=True)

    async def close(self, grace: float = 3) -> None:
        if self.closed:
            return
        self.closed = True
        if self.proc:
            # EOF exercises Beam's owned shutdown and exact admission cancellation path first.
            self.proc.stdin.close()
            try:
                await asyncio.wait_for(self.proc.wait(), grace)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.proc.pid, signal.SIGKILL)
                await self.proc.wait()
            for task in self.readers:
                task.cancel()
            await asyncio.gather(*self.readers, return_exceptions=True)

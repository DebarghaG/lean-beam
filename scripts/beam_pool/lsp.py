"""Small asynchronous client for the existing Beam LSP request family."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from pathlib import Path


class RpcError(RuntimeError):
    def __init__(self, error):
        self.error = error
        super().__init__(error.get("message", str(error)))


class Lsp:
    def __init__(self, process):
        self.process = process
        self.pending = {}
        self.next_id = 0
        self.write_lock = asyncio.Lock()
        self.reader = asyncio.create_task(self._read())
        self.closing = False

    @classmethod
    async def start(cls, project: Path, plugin: Path, *, env=None):
        process = await asyncio.create_subprocess_exec(
            "lean", "--server", f"--plugin={plugin}",
            "-DstderrAsMessages=false", "-Dexperimental.module=true",
            cwd=project, env=env,
            start_new_session=True,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        )
        client = cls(process)
        try:
            await client.call("initialize", {
                "processId": os.getpid(), "rootUri": project.as_uri(),
                "capabilities": {}, "initializationOptions": {"hasWidgets": False},
            })
            await client.notify("initialized", {})
        except BaseException:
            await client.close()
            raise
        return client

    async def _read(self):
        failure = RuntimeError("Lean worker exited")
        try:
            while True:
                header = await self.process.stdout.readuntil(b"\r\n\r\n")
                fields = dict(line.split(b":", 1) for line in header.strip().split(b"\r\n"))
                size = int(fields[b"Content-Length"])
                if size > 64 * 1024 * 1024:
                    raise RuntimeError("oversized Lean response")
                message = json.loads(await self.process.stdout.readexactly(size))
                if "method" in message:
                    if "id" in message:
                        # Server requests have an independent ID namespace. In particular,
                        # refresh requests may reuse the ID of an outstanding client call.
                        if message["method"] in ("workspace/semanticTokens/refresh",
                                                  "workspace/inlayHint/refresh",
                                                  "workspace/codeLens/refresh",
                                                  "window/workDoneProgress/create"):
                            await self._write({"id": message["id"], "result": None})
                        else:
                            await self._write({"id": message["id"], "error": {
                                "code": -32601, "message": "unsupported client request",
                            }})
                    continue
                if "id" not in message:
                    continue
                future = self.pending.get(message["id"])
                if future is not None and not future.done():
                    if "error" in message:
                        future.set_exception(RpcError(message["error"]))
                    else:
                        future.set_result(message["result"])
                    self.pending.pop(message["id"], None)
        except (Exception, asyncio.CancelledError) as error:
            failure = RuntimeError(f"Lean connection lost: {error}")
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(failure)
            self.pending.clear()

    async def _write(self, message):
        data = json.dumps({"jsonrpc": "2.0", **message}, separators=(",", ":")).encode()
        async with self.write_lock:
            self.process.stdin.write(f"Content-Length: {len(data)}\r\n\r\n".encode() + data)
            await self.process.stdin.drain()

    async def notify(self, method, params):
        await self._write({"method": method, "params": params})

    async def call(self, method, params, timeout=120):
        if self.reader.done():
            raise RuntimeError("Lean connection is closed")
        self.next_id += 1
        request_id = self.next_id
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            await self._write({"id": request_id, "method": method, "params": params})
            return await asyncio.wait_for(asyncio.shield(future), timeout)
        except (asyncio.CancelledError, TimeoutError):
            # Keep the execution slot until Lean acknowledges cancellation. If it cannot
            # drain, fence the process; an unacknowledged tactic must never be retried.
            await self.notify("$/cancelRequest", {"id": request_id})
            try:
                await asyncio.wait_for(asyncio.shield(future), 5)
            except RpcError:
                pass
            except TimeoutError:
                await self.close()
            raise
        finally:
            self.pending.pop(request_id, None)

    async def open(self, path: Path, *, timeout=120):
        document = {"uri": path.as_uri(), "version": 1}
        await self.notify("textDocument/didOpen", {"dependencyBuildMode": "never", "textDocument": {
            **document, "languageId": "lean4", "text": path.read_text(),
        }})
        try:
            ready = await self.call("$/beam/waitForDiagnostics", {"uri": document["uri"], "version": 1},
                                    timeout=timeout)
            if ready["hasFatalSnapshot"]:
                diagnostics = ready["saveReadiness"]["currentDiagnostics"]
                raise RuntimeError("Lean document setup failed: " +
                                   "\n".join(item["message"] for item in diagnostics))
        except BaseException:
            try:
                await self.notify("textDocument/didClose", {"textDocument": document})
            except (ConnectionError, OSError):
                pass  # Preserve the original failure when the transport has closed.
            raise
        return document

    async def close(self):
        if self.closing:
            return
        self.closing = True
        if self.process.returncode is None:
            if not self.pending and not self.reader.done():
                try:
                    await self.call("shutdown", None, timeout=3)
                    await self.notify("exit", None)
                except (Exception, asyncio.CancelledError):
                    pass
            try:
                await asyncio.wait_for(self.process.wait(), 3)
            except TimeoutError:
                os.killpg(self.process.pid, signal.SIGKILL)
                if self.reader.done():
                    # asyncio's subprocess wait also waits for pipe transports to close.
                    # Drain a failed reader's bounded residual output after killing the group.
                    await self.process.stdout.read()
                await self.process.wait()
        if self.reader is not asyncio.current_task():
            await self.reader

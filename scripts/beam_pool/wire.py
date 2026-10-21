"""Bounded JSON headers and binary payloads over authenticated TCP (optionally TLS)."""
from __future__ import annotations

import asyncio
import json
import struct

MAX_HEADER = 4 * 1024 * 1024
MAX_BODY = 128 * 1024 * 1024


class RemoteError(RuntimeError):
    def __init__(self, error):
        self.error = error
        super().__init__(error["message"])


async def read_frame(reader):
    header_size, body_size = struct.unpack("!IQ", await reader.readexactly(12))
    if header_size > MAX_HEADER or body_size > MAX_BODY:
        raise ValueError("frame exceeds configured bounds")
    header = json.loads(await reader.readexactly(header_size))
    if not isinstance(header, dict):
        raise ValueError("frame header must be an object")
    return header, await reader.readexactly(body_size)


async def write_frame(writer, header, body=b""):
    header = json.dumps(header, separators=(",", ":")).encode()
    if len(header) > MAX_HEADER or len(body) > MAX_BODY:
        raise ValueError("frame exceeds configured bounds")
    writer.write(struct.pack("!IQ", len(header), len(body)))
    writer.write(header)
    # Drain in chunks so a slow peer cannot accumulate arbitrary write buffers.
    for offset in range(0, len(body), 256 * 1024):
        writer.write(memoryview(body)[offset:offset + 256 * 1024])
        await writer.drain()
    await writer.drain()


class Peer:
    def __init__(self, host, port, token, *, ssl=None, timeout=180):
        self.host, self.port, self.token = host, port, token
        self.ssl, self.timeout = ssl, timeout
        self.generation = None
        self.failed = False

    async def call(self, op, payload=None, body=b""):
        if self.failed:
            raise ConnectionError("worker connection is fenced; create a new pool for a new generation")
        try:
            return await self._call(op, payload, body)
        except (ConnectionError, OSError, asyncio.IncompleteReadError, TimeoutError):
            self.failed = True
            raise

    async def _call(self, op, payload, body):
        async with asyncio.timeout(self.timeout):
            reader, writer = await asyncio.open_connection(self.host, self.port, ssl=self.ssl)
            try:
                await write_frame(writer, {
                    "token": self.token, "op": op, "payload": payload or {},
                    "generation": self.generation,
                }, body)
                response, data = await read_frame(reader)
                if "error" in response:
                    if response["error"].get("code") == "generationChanged":
                        raise ConnectionError("worker generation changed")
                    raise RemoteError(response["error"])
                if self.generation is not None and response["generation"] != self.generation:
                    raise ConnectionError("worker generation changed")
                self.generation = response["generation"]
                return response["result"], data
            finally:
                writer.close()
                await writer.wait_closed()

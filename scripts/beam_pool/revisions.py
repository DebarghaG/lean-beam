"""Bounded file uploads and immutable project revisions, shared by gateway and workers."""

from __future__ import annotations

import hashlib
import re
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from .protocol import Failure, boolean, canonical, fields, input_file, natural, relative_path, source_file, string

CHUNK_SIZE = 64 * 1024


@dataclass
class Revision:
    identity: str
    snapshot: str
    group: str
    sequence: int
    files: dict[str, str | None]
    environment: str
    pending: bool = False
    touched: float = field(default_factory=time.monotonic)

    def wire(self) -> dict:
        return {"snapshot": self.snapshot, "group": self.group,
                "sequence": self.sequence, "files": self.files}


@dataclass
class Upload:
    path: Path
    size: int = 0
    touched: float = field(default_factory=time.monotonic)
    digest: object = field(default_factory=hashlib.sha256)


class Revisions:
    def __init__(self, *, max_bytes: int = 1024 * 1024 * 1024, max_revisions: int = 256,
                 ttl: float = 1800):
        self.temp = tempfile.TemporaryDirectory(prefix="beam-pool-revisions-")
        self.root = Path(self.temp.name)
        self.max_bytes, self.max_revisions, self.ttl = max_bytes, max_revisions, ttl
        self.used = 0
        self.blobs: dict[str, tuple[int, float]] = {}
        self.uploads: dict[tuple[str, str], Upload] = {}
        self.revisions: dict[str, Revision] = {}

    def close(self) -> None:
        self.temp.cleanup()

    def collect(self, pinned: set[str] = frozenset()) -> None:
        cutoff = time.monotonic() - self.ttl
        for key, revision in list(self.revisions.items()):
            if key not in pinned and (revision.touched < cutoff or
                                      (revision.pending and revision.touched < time.monotonic() - 120)):
                del self.revisions[key]
        referenced = {digest for r in self.revisions.values() for digest in r.files.values() if digest}
        for key, (size, touched) in list(self.blobs.items()):
            if key not in referenced and touched < cutoff:
                self.blob(key).unlink(missing_ok=True)
                self.used -= size
                del self.blobs[key]
        for key, upload in list(self.uploads.items()):
            if upload.touched < time.monotonic() - 120:
                upload.path.unlink(missing_ok=True)
                self.used -= upload.size
                del self.uploads[key]

    def make_room(self, size: int, pinned: set[str]) -> None:
        pinned = pinned | {r.identity for r in self.revisions.values() if r.pending}
        if size > self.max_bytes:
            raise Failure("resourceExhausted", "file chunk exceeds the pool file cache")
        if self.used + size <= self.max_bytes and len(self.blobs) < 32768:
            return
        idle = sorted((r for r in self.revisions.values() if r.identity not in pinned),
                      key=lambda r: r.touched)
        for revision in [None, *idle]:
            if revision:
                self.revisions.pop(revision.identity, None)
            referenced = {d for r in self.revisions.values() for d in r.files.values() if d}
            for digest, (length, _) in sorted(self.blobs.items(), key=lambda item: item[1][1]):
                if digest not in referenced:
                    self.blob(digest).unlink()
                    self.used -= length
                    del self.blobs[digest]
                if self.used + size <= self.max_bytes and len(self.blobs) < 32768:
                    return
        raise Failure("resourceExhausted", "pool file cache is full; release branches or increase its budget")

    def blob(self, digest: str) -> Path:
        if not re.fullmatch("[0-9a-f]{64}", digest):
            raise Failure("invalidParams", "invalid file digest")
        return self.root / digest

    def put(self, args: dict, pinned: set[str] = frozenset()) -> dict:
        fields(args, {"group", "upload", "offset", "data", "last"})
        group, upload = string(args, "group", 128), string(args, "upload", 128)
        offset, last = natural(args, "offset"), boolean(args, "last")
        encoded = string(args, "data", CHUNK_SIZE * 2)
        if len(encoded) % 2 or not re.fullmatch("[0-9a-f]*", encoded):
            raise Failure("invalidParams", "invalid file chunk")
        data = bytes.fromhex(encoded)
        key = group, upload
        previous = self.uploads.get(key)
        if previous is None:
            if offset or len(self.uploads) >= 64:
                raise Failure("invalidParams" if offset else "resourceExhausted", "upload unavailable")
            path = self.root / ("upload-" + hashlib.sha256(canonical(key)).hexdigest())
            previous = Upload(path)
        if offset != previous.size:
            raise Failure("invalidParams", "file chunk is out of order")
        self.make_room(len(data), pinned)
        with previous.path.open("ab" if previous.size else "wb") as stream:
            stream.write(data)
        self.used += len(data)
        previous.size += len(data)
        previous.digest.update(data)
        previous.touched = time.monotonic()
        self.uploads[key] = previous
        if not last:
            return {"digest": None}
        digest = previous.digest.hexdigest()
        if digest in self.blobs:
            previous.path.unlink()
            self.used -= previous.size
        else:
            previous.path.replace(self.blob(digest))
        self.blobs[digest] = previous.size, time.monotonic()
        del self.uploads[key]
        return {"digest": digest}

    def publish(self, args: dict, pinned: set[str] = frozenset()) -> dict:
        fields(args, {"snapshot", "group", "sequence", "files"})
        snapshot, group = string(args, "snapshot", 128), string(args, "group", 128)
        sequence = natural(args, "sequence")
        files = args["files"]
        if not isinstance(files, dict) or len(files) > 8192:
            raise Failure("resourceExhausted", "project revision has too many changed files")
        for path, digest in files.items():
            relative_path(path)
            if not input_file(path):
                raise Failure("invalidParams", "file is not a Lean project input")
            if digest is not None:
                if not isinstance(digest, str):
                    raise Failure("invalidParams", "invalid file digest")
                self.blob(digest)
        missing = sorted({d for d in files.values() if d and d not in self.blobs})
        identity = hashlib.sha256(canonical(args)).hexdigest()
        if identity not in self.revisions and len(self.revisions) >= self.max_revisions:
            idle = [r for r in self.revisions.values() if r.identity not in pinned and not r.pending]
            if not idle:
                raise Failure("resourceExhausted", "pool revision cache is full")
            del self.revisions[min(idle, key=lambda r: r.touched).identity]
        environment = hashlib.sha256(canonical([snapshot, {
            p: d for p, d in files.items() if not source_file(p)}])).hexdigest()
        self.revisions[identity] = Revision(identity, snapshot, group, sequence, dict(files), environment,
                                            pending=bool(missing))
        return {"revision": None if missing else identity, "missing": missing}

    def get(self, identity: str, group: str) -> Revision:
        revision = self.revisions.get(identity)
        if revision is None or revision.group != group or revision.pending:
            raise Failure("contentModified", "project revision is unavailable",
                          {"reason": "revisionMissing"})
        revision.touched = time.monotonic()
        return revision

"""Closed wire records and content-addressed project snapshots (protocol 1)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MAX_FRAME = 4 * 1024 * 1024
MAX_TEXT = 128 * 1024


class Failure(Exception):
    def __init__(self, code: str, message: str, data: Any = None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data

    def json(self) -> dict:
        return {"code": self.code, "message": self.message, "data": self.data}

    @classmethod
    def decode(cls, value: Any) -> Failure:
        fields(value, {"code", "message", "data"})
        return cls(string(value, "code"), string(value, "message"), value["data"])


def fields(value: Any, required: set[str], optional: set[str] = frozenset()) -> dict:
    if not isinstance(value, dict) or not required <= value.keys() <= required | optional:
        raise Failure("invalidParams", f"expected fields {sorted(required)}, optional {sorted(optional)}")
    return value


def string(value: dict, key: str, limit: int = MAX_TEXT) -> str:
    item = value[key]
    if not isinstance(item, str) or len(item.encode()) > limit:
        raise Failure("invalidParams", f"invalid string: {key}")
    return item


def natural(value: dict, key: str) -> int:
    item = value[key]
    if type(item) is not int or item < 0:
        raise Failure("invalidParams", f"invalid natural number: {key}")
    return item


def boolean(value: dict, key: str) -> bool:
    if type(value[key]) is not bool:
        raise Failure("invalidParams", f"invalid boolean: {key}")
    return value[key]


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def relative_path(value: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise Failure("invalidParams", "path must be a normalized relative file path")
    return value


@dataclass(frozen=True)
class Snapshot:
    root: Path
    identity: str
    files: dict[str, str]

    @classmethod
    def create(cls, root: Path) -> Snapshot:
        """Hash source, configuration, and compiled imports, excluding mutable runtime state."""
        import os
        root = root.resolve(strict=True)
        if not (root / "lean-toolchain").is_file():
            raise Failure("invalidParams", "project must contain lean-toolchain")
        entries = {}
        suffixes = {".lean", ".olean", ".server", ".private", ".ir", ".sig", ".so", ".dylib", ".dll"}
        for directory, dirs, names in os.walk(root):
            dirs[:] = sorted(d for d in dirs if d not in {
                ".git", ".beam", ".codex-worktrees", "__pycache__", "node_modules", ".venv"})
            for name in sorted(names):
                path = Path(directory) / name
                if path.suffix in suffixes or name in {"lean-toolchain", "lakefile.toml", "lake-manifest.json"}:
                    if not path.resolve().is_relative_to(root):
                        raise Failure("invalidParams", f"snapshot contains an external symlink: {path}")
                    entries[path.relative_to(root).as_posix()] = digest_file(path)
        identity = hashlib.sha256(canonical(entries)).hexdigest()
        return cls(root, identity, entries)

    def check_file(self, name: str) -> Path:
        relative_path(name)
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root) or name not in self.files or path.suffix != ".lean":
            raise Failure("invalidParams", "file is outside the prepared snapshot")
        try:
            matches = digest_file(path) == self.files[name]
        except OSError:
            matches = False
        if not matches:
            raise Failure("contentModified", "snapshot source changed; prepare a new worker")
        return path

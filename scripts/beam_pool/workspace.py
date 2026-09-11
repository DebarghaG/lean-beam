"""Private source files over a prepared dependency tree; no mounts or project-wide copies."""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

from .protocol import EXCLUDED_DIRS, Failure, relative_path, source_file
from .revisions import Revisions


class Workspace:
    def __init__(self, root: Path, base: Path, store: Revisions):
        self.root, self.base, self.store = root, base, store
        self.files: dict[str, str | None] = {}
        root.mkdir()
        self.link_directory(root, base)
        # Lake may compile its configuration. Keep that cache and the runtime's files private.
        self.expand(root / ".lake")
        for name in ("lean-toolchain", "lakefile.lean", "lakefile.toml", "lake-manifest.json"):
            if (root / name).is_file():
                self.copy_source(name)
        for name in ("lakefile.olean", "lakefile.olean.trace", "lakefile.ilean"):
            if (root / ".lake" / name).is_file():
                self.copy_source(".lake/" + name)

    def link_directory(self, target: Path, original: Path) -> None:
        if not original.is_dir():
            return
        for entry in original.iterdir():
            if entry.name not in EXCLUDED_DIRS:
                (target / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())

    def expand(self, path: Path) -> None:
        if path == self.root:
            return
        self.expand(path.parent)
        if path.is_symlink():
            original = path.resolve()
            if not original.is_dir():
                raise Failure("invalidParams", "project input parent is not a directory")
            path.unlink()
            path.mkdir()
            self.link_directory(path, original)
        elif not path.exists():
            path.mkdir()
        elif not path.is_dir():
            raise Failure("invalidParams", "project input parent is not a directory")

    def copy_source(self, name: str, original: Path | None = None) -> Path:
        relative_path(name)
        target = self.root / name
        self.expand(target.parent)
        if original is None:
            if not target.is_symlink():
                return target
            original = target.resolve(strict=True)
        temporary = target.with_name(target.name + "." + uuid.uuid4().hex)
        try:
            shutil.copyfile(original, temporary)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return target

    def apply(self, files: dict[str, str | None]) -> None:
        for name in self.files.keys() | files.keys():
            if name in self.files and name in files and self.files[name] == files[name]:
                continue
            target = self.root / name
            self.expand(target.parent)
            if name not in files:
                target.unlink(missing_ok=True)
                original = self.base / name
                if original.exists():
                    target.symlink_to(original)
            elif files[name] is None:
                target.unlink(missing_ok=True)
            elif source_file(name):
                self.copy_source(name, self.store.blob(files[name]))
            else:
                temporary = target.with_name(target.name + "." + uuid.uuid4().hex)
                temporary.symlink_to(self.store.blob(files[name]))
                os.replace(temporary, target)
        self.files = dict(files)

    def source(self, name: str, text: str) -> Path:
        relative_path(name)
        if not source_file(name):
            raise Failure("invalidParams", "request needs a Lean source file")
        target = self.copy_source(name)
        try:
            matches = target.read_bytes().decode("utf-8") == text
        except (OSError, UnicodeError):
            matches = False
        if not matches:
            raise Failure("contentModified", "request source does not match its project revision")
        return target

    def close(self) -> None:
        shutil.rmtree(self.root)

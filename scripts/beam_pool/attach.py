"""Operator setup: verify a prepared project and bind ordinary Beam CLI/MCP to its pool."""

import fcntl
import hashlib
import json
import os
import uuid
from pathlib import Path

from .protocol import Failure, Snapshot, fields
from .transport import rpc


async def attach(root: Path, endpoint: tuple[str, int], token: str,
                 config: Path, snapshot: str | None = None) -> dict:
    root = root.resolve(strict=True)
    local = Snapshot.create(root)
    before = {path: (root / path).stat() for path in local.files}
    # Bracket the identity check with metadata observations, including the initial hash/stat gap.
    if Snapshot.create(root).files != local.files:
        raise Failure("contentModified", "project changed during attachment")
    info = await rpc(endpoint, token, "info", {})
    choices = {w["snapshot"] for w in info["workers"] if w.get("ready")}
    if snapshot:
        choices &= {snapshot}
    matched = []
    for candidate in sorted(choices):
        remote, offset = {}, 0
        while offset is not None:
            page = await rpc(endpoint, token, "describe", {"snapshot": candidate, "offset": offset})
            fields(page, {"snapshot", "runtime", "files", "next"})
            if page["snapshot"] != candidate:
                raise Failure("contentModified", "worker snapshot changed during attachment")
            if not isinstance(page["files"], dict) or remote.keys() & page["files"].keys():
                raise Failure("invalidParams", "invalid snapshot manifest page")
            remote.update(page["files"])
            if page["next"] is not None and (type(page["next"]) is not int or page["next"] <= offset):
                raise Failure("invalidParams", "snapshot pagination did not advance")
            offset = page["next"]
        if remote == local.files:
            matched.append(candidate)
    if len(matched) != 1:
        raise Failure("contentModified", "attachment requires exactly one matching prepared snapshot; "
                      "prepare identical project artifacts, or select --snapshot to disambiguate")
    inputs = []
    for path, old in sorted(before.items()):
        current = (root / path).stat()
        if (old.st_mtime_ns, old.st_size) != (current.st_mtime_ns, current.st_size):
            raise Failure("contentModified", "project changed during attachment")
        sec, nsec = divmod(current.st_mtime_ns, 1_000_000_000)
        inputs.append({"path": path, "size": current.st_size, "sec": sec, "nsec": nsec,
                       "digest": local.files[path]})
    config = config.absolute()
    manifest = json.dumps(inputs, separators=(",", ":")).encode()
    if len(manifest) > 32 * 1024 * 1024:
        raise Failure("resourceExhausted", "workspace input manifest exceeds 32 MiB")
    inputs_name = config.name + "." + hashlib.sha256(manifest).hexdigest() + ".inputs.json"
    config.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Atomic replacement protects readers; the lock preserves other concurrent attachments.
    with config.with_name(config.name + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        data = {"schema": 2, "bindings": []}
        if config.exists():
            data = json.loads(config.read_text())
            fields(data, {"schema", "bindings"})
            if data["schema"] != 2 or not isinstance(data["bindings"], list):
                raise Failure("invalidParams", "unsupported binding configuration; attach to a new config path")
        data["bindings"] = [b for b in data["bindings"] if b["root"] != str(root)]
        data["bindings"].append({"root": str(root), "host": endpoint[0], "port": endpoint[1],
            "snapshot": matched[0], "binding": uuid.uuid4().hex, "inputs": inputs_name})
        manifest_path = config.parent / inputs_name
        if not manifest_path.exists():
            temporary = manifest_path.with_name(manifest_path.name + "." + uuid.uuid4().hex + ".tmp")
            try:
                with temporary.open("xb") as stream:
                    os.chmod(temporary, 0o600)
                    stream.write(manifest)
                os.replace(temporary, manifest_path)
            finally:
                temporary.unlink(missing_ok=True)
        temporary = config.with_name(config.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("x") as stream:
                os.chmod(temporary, 0o600)
                json.dump(data, stream, indent=2)
                stream.write("\n")
            os.replace(temporary, config)
        finally:
            temporary.unlink(missing_ok=True)
    return {"config": str(config), "root": str(root), "snapshot": matched[0], "inputs": len(inputs),
            "restart_required": True, "environment": {"BEAM_LEAN_POOL_CONFIG": str(config)}}

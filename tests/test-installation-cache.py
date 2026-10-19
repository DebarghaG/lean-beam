#!/usr/bin/env python3
# Copyright (c) 2026 Lean FRO LLC. All rights reserved.
# Released under Apache 2.0 license as described in the file LICENSE.
"""Mutated qualified cache inputs must not change a published test installation."""

import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
generation = Path(sys.argv[1]).resolve()
install_root = generation.parent.parent
home = Path(os.environ["HOME"]).resolve()
validation = Path(os.environ.get("BEAM_VALIDATION_ROOT", str(home.parent))).resolve()
assert validation.is_relative_to(Path("/tmp").resolve()) and validation.name.startswith(("beam-validate-", "beam-daemon-env-"))
assert home.is_relative_to(validation) and install_root.is_relative_to(validation)
assert generation.parent == install_root / "versions"
current = install_root / "current"
assert current.resolve() == generation
manifest = generation / "manifest.json"
manifest_bytes = manifest.read_bytes()
record = json.loads(manifest_bytes)
options = [option for target in record["components"]["targets"] for option in ["--toolchain", target["toolchain"]]]
versions = sorted(path.name for path in generation.parent.iterdir())
cache_records = sorted((install_root / "state/install-bundles").glob("*/*/metadata.json"))
assert len(cache_records) == len(record["components"]["targets"]), cache_records
count = 0
for cache_record in cache_records:
    cache_bytes = cache_record.read_bytes()
    cached = json.loads(cache_bytes)
    assert cached["schemaVersion"] == 6
    workspace = cache_record.parent / "workspace"
    for artifact in cached["artifacts"]:
        assert artifact["source"] == artifact["destination"]
        output = workspace / artifact["source"]
        assert not output.is_symlink() and output.resolve().is_relative_to(workspace.resolve())
        original = output.read_bytes()
        assert hashlib.sha256(original).hexdigest() == artifact["hash"]
        mode = stat.S_IMODE(output.stat().st_mode)
        output.chmod(mode | stat.S_IWUSR)
        try:
            output.write_bytes(b"unqualified replacement bytes\n")
            result = subprocess.run(["bash", "scripts/install-beam.sh", "--dont-ask", *options],
                                    cwd=ROOT, env=dict(os.environ, BEAM_INSTALL_ROOT=str(install_root)),
                                    capture_output=True, text=True, encoding="utf-8", timeout=120)
            assert result.returncode != 0 and "construction cache failed verification" in result.stderr, result
            assert current.resolve() == generation
            assert manifest.read_bytes() == manifest_bytes
            assert cache_record.read_bytes() == cache_bytes
            assert sorted(path.name for path in generation.parent.iterdir()) == versions
            assert not list(install_root.glob(".staging-*"))
            count += 1
        finally:
            try:
                output.write_bytes(original)
            finally:
                output.chmod(mode)
print(f"all {count} independently mutated daemon/plugin/library cache inputs rejected; current unchanged", flush=True)

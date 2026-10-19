#!/usr/bin/env python3
# Copyright (c) 2026 Lean FRO LLC. All rights reserved.
# Released under Apache 2.0 license as described in the file LICENSE.
"""Both production validators consume the same writer-generated structural corpus."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="beam-manifest-contract-") as temporary:
    corpus = Path(temporary) / "records.json"
    subprocess.run([sys.executable, "-X", "utf8", ROOT / "tests/test-native-provenance.py",
                    "NativeBuildRecordTest.test_shared_manifest_contract"],
                   env=dict(os.environ, BEAM_MANIFEST_CORPUS_OUTPUT=str(corpus)), check=True, timeout=120)
    suffix = ".exe" if os.name == "nt" else ""
    subprocess.run([ROOT / ".lake/build/bin" / ("beam-installation-manifest-test" + suffix), corpus],
                   check=True, timeout=30)

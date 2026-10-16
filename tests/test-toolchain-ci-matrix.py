#!/usr/bin/env python3

# Copyright (c) 2026 Lean FRO LLC. All rights reserved.
# Released under Apache 2.0 license as described in the file LICENSE.
# Author: Emilio J. Gallego Arias

from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parent.parent


class ToolchainCiMatrixTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="beam-toolchain-matrix-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        shutil.copyfile(REPO / "scripts/ci-toolchains.py", self.root / "scripts/ci-toolchains.py")
        # Synthetic policy cases stay independent of the project's moving support window.
        self.pinned = "leanprover/lean4:v99.2.0-rc1"
        self.stable = "leanprover/lean4:v99.1.2"
        self.points = [self.pinned, self.stable]
        (self.root / "validated-lean-toolchains").write_text("\n".join(self.points) + "\n")
        (self.root / "compatible-lean-release-lines").write_text(
            "leanprover/lean4:v99.2\nleanprover/lean4:v99.1\n")
        (self.root / "lean-toolchain").write_text(self.pinned + "\n")

    def replace(self, relative, old, new):
        path = self.root / relative
        text = path.read_text()
        self.assertIn(old, text)
        path.write_text(text.replace(old, new))

    @contextmanager
    def registry_contents(self, relative, contents):
        path = self.root / relative
        original = path.read_text()
        path.write_text(contents)
        try:
            yield
        finally:
            path.write_text(original)

    def run_generator(self, expected_error=None, cwd=None, root=None):
        root = root or self.root
        result = subprocess.run(
            [sys.executable, str(root / "scripts/ci-toolchains.py")],
            cwd=cwd or self.root, capture_output=True, text=True, check=False,
        )
        if expected_error is None:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, "")
            self.assertEqual(len(result.stdout.splitlines()), 1)
            return json.loads(result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(expected_error, result.stderr)
            self.assertNotIn("Traceback", result.stderr)
            self.assertEqual(result.stdout, "", "invalid input must not publish a partial matrix")

    def test_fixture_matrix(self):
        self.assertEqual(self.run_generator(), self.points)

    def test_checked_in_registries(self):
        points = [line.strip() for line in (REPO / "validated-lean-toolchains").read_text().splitlines()
                  if line.strip() and not line.lstrip().startswith("#")]
        self.assertEqual(self.run_generator(root=REPO), points)

    def test_registry_update_changes_matrix_without_workflow_copy(self):
        point = self.stable
        line, patch = point.rsplit(".", 1)
        replacement = f"{line}.{int(patch) + 1}"
        self.replace("validated-lean-toolchains", point, replacement)
        self.assertEqual(self.run_generator(), [replacement if p == point else p for p in self.points])

    def test_comments_whitespace_and_registry_order(self):
        points = list(reversed(self.points))
        (self.root / "validated-lean-toolchains").write_text(
            " # supported points\n\n" + "\n".join(f"  {point}  " for point in points) + "\n")
        self.assertEqual(self.run_generator(), points)

    def test_paths_are_relative_to_script_not_caller(self):
        self.assertEqual(self.run_generator(cwd=self.root.parent), self.points)

    def test_unpinned_rc_is_rejected(self):
        point = self.stable
        self.replace("validated-lean-toolchains", point, point + "-rc1")
        self.run_generator("only the pinned default may be a release candidate")

    def test_pinned_rc_is_accepted(self):
        candidate = self.pinned.split("-rc", 1)[0] + "-rc7"
        self.replace("validated-lean-toolchains", self.pinned, candidate)
        (self.root / "lean-toolchain").write_text(candidate + "\n")
        self.assertIn(candidate, self.run_generator())

    def test_noncanonical_pinned_rc_is_rejected(self):
        candidate = self.pinned.split("-rc", 1)[0] + "-rc01"
        self.replace("validated-lean-toolchains", self.pinned, candidate)
        (self.root / "lean-toolchain").write_text(candidate + "\n")
        self.run_generator("expected a canonical Lean toolchain")

    def test_two_stable_points_in_one_line_are_rejected(self):
        point = self.stable
        line, patch = point.rsplit(".", 1)
        extra = f"{line}.{int(patch) + 1}"
        self.replace("validated-lean-toolchains", point, f"{point}\n{extra}")
        self.run_generator("expected exactly one validated point")

    def test_missing_release_line_coverage_is_rejected(self):
        point = self.stable
        self.replace("validated-lean-toolchains", point + "\n", "")
        self.run_generator("expected exactly one validated point")

    def test_stable_and_rc_in_one_line_are_rejected(self):
        point = self.pinned
        stable = point.split("-rc", 1)[0]
        extra = stable if point != stable else stable + "-rc8"
        self.replace("validated-lean-toolchains", point, f"{point}\n{extra}")
        self.run_generator("expected exactly one validated point")

    def test_toolchain_outside_supported_lines_is_rejected(self):
        self.replace("validated-lean-toolchains", self.stable, "leanprover/lean4:v999.0.0")
        self.run_generator("validated toolchain is outside compatible-lean-release-lines")

    def test_unvalidated_default_is_rejected(self):
        (self.root / "lean-toolchain").write_text("leanprover/lean4:v999.0.0\n")
        self.run_generator("pinned toolchain is not validated")

    def test_duplicate_registries_are_rejected(self):
        for relative, expected in [
            ("validated-lean-toolchains", "duplicate validated Lean toolchain"),
            ("compatible-lean-release-lines", "duplicate compatible Lean release line"),
        ]:
            with self.subTest(registry=relative):
                path = self.root / relative
                original = path.read_text()
                entry = next(line for line in original.splitlines() if line and not line.startswith("#"))
                with self.registry_contents(relative, original + entry + "\n"):
                    self.run_generator(expected)

    def test_noncanonical_toolchains_are_rejected(self):
        original = (self.root / "validated-lean-toolchains").read_text()
        point = self.stable
        for candidate in ["leanprover/lean4:v99.01.2", "leanprover/lean4:nightly-2026-10-13",
                          point + '"', point + " extra"]:
            with self.subTest(toolchain=candidate):
                with self.registry_contents("validated-lean-toolchains", original.replace(point, candidate)):
                    self.run_generator("expected a canonical Lean toolchain")

    def test_malformed_release_line_is_rejected(self):
        line = self.stable.rsplit(".", 1)[0]
        self.replace("compatible-lean-release-lines", line, line + ".1")
        self.run_generator("invalid compatible Lean release line")

    def test_empty_registries_are_rejected(self):
        for relative, expected in [
            ("validated-lean-toolchains", "no validated Lean toolchains listed"),
            ("compatible-lean-release-lines", "no compatible Lean release lines listed"),
        ]:
            with self.subTest(registry=relative):
                with self.registry_contents(relative, "# empty\n\n"):
                    self.run_generator(expected)

    def test_missing_registry_is_reported_without_matrix(self):
        (self.root / "validated-lean-toolchains").unlink()
        self.run_generator("validated-lean-toolchains")


if __name__ == "__main__":
    unittest.main()

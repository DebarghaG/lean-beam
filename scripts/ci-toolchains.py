#!/usr/bin/env python3

# Copyright (c) 2026 Lean FRO LLC. All rights reserved.
# Released under Apache 2.0 license as described in the file LICENSE.
# Author: Emilio J. Gallego Arias

"""Validate the supported Lean toolchains and emit their CI matrix values as JSON."""

from collections import Counter
import json
from pathlib import Path
import re
import sys

canonical_toolchain_re = re.compile(
    r"^leanprover/lean4:v"
    r"(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)\."
    r"(?P<patch>0|[1-9][0-9]*)"
    r"(?:-rc(?P<rc>[1-9][0-9]*))?$"
)
release_line_re = re.compile(
    r"^leanprover/lean4:v"
    r"(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)$"
)


def load_registry_entries(path: Path) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
    entries: list[tuple[int, str]] = []
    seen: set[str] = set()
    duplicates: list[tuple[int, str]] = []

    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        entry = raw.strip()
        if not entry or entry.startswith("#"):
            continue
        if entry in seen:
            duplicates.append((line_no, entry))
        seen.add(entry)
        entries.append((line_no, entry))

    return entries, duplicates


def validated_toolchains(root: Path) -> list[str]:
    validated_registry_path = root / "validated-lean-toolchains"
    compatible_registry_path = root / "compatible-lean-release-lines"
    pinned_toolchain_path = root / "lean-toolchain"
    validated_entries, validated_duplicates = load_registry_entries(validated_registry_path)
    compatible_entries, compatible_duplicates = load_registry_entries(compatible_registry_path)

    validated = [entry for _, entry in validated_entries]
    compatible = [entry for _, entry in compatible_entries]
    pinned_toolchain = pinned_toolchain_path.read_text(encoding="utf-8").strip()
    failures: list[str] = []

    if not validated:
        failures.append(f"{validated_registry_path}: no validated Lean toolchains listed")
    if not compatible:
        failures.append(f"{compatible_registry_path}: no compatible Lean release lines listed")

    points_per_line: Counter[str] = Counter()
    for line_no, toolchain in validated_entries:
        match = canonical_toolchain_re.fullmatch(toolchain)
        if match is None:
            failures.append(
                f"{validated_registry_path}:{line_no}: expected a canonical Lean toolchain: {toolchain}"
            )
            continue
        if match.group("rc") is not None and toolchain != pinned_toolchain:
            failures.append(
                f"{validated_registry_path}:{line_no}: only the pinned default may be a release "
                f"candidate: {toolchain}"
            )
        release_line = f"leanprover/lean4:v{match.group('major')}.{match.group('minor')}"
        points_per_line[release_line] += 1
        if release_line not in compatible:
            failures.append(
                f"{validated_registry_path}:{line_no}: validated toolchain is outside "
                f"compatible-lean-release-lines: {toolchain}"
            )
    for line_no, toolchain in validated_duplicates:
        failures.append(
            f"{validated_registry_path}:{line_no}: duplicate validated Lean toolchain: {toolchain}"
        )

    for line_no, release_line in compatible_entries:
        if release_line_re.fullmatch(release_line) is None:
            failures.append(
                f"{compatible_registry_path}:{line_no}: invalid compatible Lean release line: {release_line}"
            )
    for line_no, release_line in compatible_duplicates:
        failures.append(
            f"{compatible_registry_path}:{line_no}: duplicate compatible Lean release line: {release_line}"
        )

    for release_line in compatible:
        count = points_per_line[release_line]
        if count != 1:
            failures.append(
                f"{validated_registry_path}: expected exactly one validated point for {release_line}, "
                f"found {count}"
            )

    if pinned_toolchain not in validated:
        failures.append(f"{pinned_toolchain_path}: pinned toolchain is not validated: {pinned_toolchain}")

    if failures:
        raise ValueError("\n".join(failures))

    return validated


def main() -> int:
    try:
        toolchains = validated_toolchains(Path(__file__).resolve().parent.parent)
    except (OSError, ValueError) as error:
        print(f"toolchain CI matrix: {error}", file=sys.stderr)
        return 1
    print(json.dumps(toolchains, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

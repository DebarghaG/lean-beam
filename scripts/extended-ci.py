#!/usr/bin/env python3

# Copyright (c) 2026 Lean FRO LLC. All rights reserved.
# Released under Apache 2.0 license as described in the file LICENSE.

"""Request full macOS CI for the current committed revision on a verified fork."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = "leanprover/lean-beam"
DEFAULT_FORK = "ejgallego/lean-beam-ci-macos"


def run(*args, capture=True):
    result = subprocess.run(args, cwd=ROOT, check=True, text=True, capture_output=capture)
    return result.stdout.strip() if capture else ""


def api(path):
    return json.loads(run("gh", "api", path))


def verify_fork(name, metadata):
    # GitHub follows repository redirects. Never push through one, especially the
    # former ejgallego/lean-beam name, which now resolves to upstream.
    if metadata["full_name"].lower() != name.lower():
        raise ValueError(f"{name} redirects to {metadata['full_name']}; use the fork's exact name")
    if name.lower() == UPSTREAM or not metadata.get("fork"):
        raise ValueError("extended CI requires a fork, not the upstream repository")
    network = [metadata.get(key, {}).get("full_name", "").lower()
               for key in ("parent", "source")]
    if UPSTREAM not in network:
        raise ValueError(f"the fork must belong to the {UPSTREAM} network")


def find_run(repo, branch, sha):
    runs = json.loads(run("gh", "run", "list", "--repo", repo,
                          "--workflow", "ci.yml", "--event", "workflow_dispatch",
                          "--branch", branch, "--limit", "100", "--json",
                          "databaseId,headSha,url"))
    return next((item for item in runs if item["headSha"] == sha), None)


def submit(repo, wait=False):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("--repo must be OWNER/REPO")
    if run("git", "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("use a clean committed worktree; local edits are not included in extended CI")
    sha = run("git", "rev-parse", "HEAD")
    metadata = api(f"repos/{repo}")
    verify_fork(repo, metadata)
    variable = api(f"repos/{repo}/actions/variables/BEAM_FULL_MACOS_CI")
    if variable.get("value") != "true":
        raise ValueError(f"set BEAM_FULL_MACOS_CI=true in {repo} before requesting extended CI")
    workflow = api(f"repos/{repo}/actions/workflows/ci.yml")
    if workflow["state"] != "active":
        raise ValueError(f"enable ci.yml in {repo} before requesting extended CI")

    # Each revision has an immutable branch. A normal push rejects any conflicting
    # history, and neither origin nor the fork's default branch is changed.
    branch = f"extended-ci/{sha}"
    target = f"git@github.com:{repo}.git"
    remote_ref = run("git", "ls-remote", "--heads", target, f"refs/heads/{branch}")
    if remote_ref and remote_ref.split()[0] != sha:
        raise ValueError(f"{repo}:{branch} already points to another commit")
    run("git", "push", target, f"{sha}:refs/heads/{branch}")
    result = find_run(repo, branch, sha)
    if result is None:
        run("gh", "workflow", "run", "ci.yml", "--repo", repo, "--ref", branch)
        deadline = time.monotonic() + 60
        while result is None and time.monotonic() < deadline:
            time.sleep(2)
            result = find_run(repo, branch, sha)
        if result is None:
            raise ValueError(f"dispatch accepted, but no run found yet; inspect {repo}:{branch} before retrying")
    print(f"repository={repo}\nhead={sha}\nbranch={branch}\nrun={result['url']}", flush=True)
    if wait:
        run("gh", "run", "watch", str(result["databaseId"]), "--repo", repo,
            "--exit-status", capture=False)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_FORK, help="full-matrix fork (default: %(default)s)")
    parser.add_argument("--wait", action="store_true", help="wait and return a failure status if CI fails")
    args = parser.parse_args()
    try:
        submit(args.repo, args.wait)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        detail = str(error)
        if isinstance(error, subprocess.CalledProcessError):
            detail = ((error.stderr or "").strip() or (error.stdout or "").strip()
                      or f"{error.cmd[0]} exited with status {error.returncode}")
        print(f"extended-ci: {detail}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

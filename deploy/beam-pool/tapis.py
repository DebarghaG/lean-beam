#!/usr/bin/env python3
"""Render and start a Beam pool on Tapis. Does not restart or remove existing pods."""

import argparse
import base64
import copy
import hmac
import json
import os
from pathlib import Path
import re
import shlex
import sys
import urllib.error
import urllib.parse
import urllib.request


class ApiError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        super().__init__(f"Tapis HTTP {status}: {message}")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Tapis:
    def __init__(self, env):
        self.url = env.get("TAPIS_URL", "https://tacc.tapis.io").rstrip("/")
        parsed = urllib.parse.urlsplit(self.url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or
                parsed.password or parsed.query or parsed.fragment or parsed.path):
            raise ValueError("TAPIS_URL must be an HTTPS origin")
        self.token = env.get("TAPIS_ACCESS_TOKEN", "")
        if not self.token:
            raise ValueError("set TAPIS_ACCESS_TOKEN")
        # Only used to spell secret references. The API verifies the token on every call.
        try:
            claims = json.loads(base64.urlsafe_b64decode(self.token.split(".")[1] + "==="))
            self.username = claims["tapis/username"]
            if not re.fullmatch(r"[A-Za-z0-9_-]+", self.username):
                raise ValueError()
        except (IndexError, KeyError, ValueError, TypeError):
            raise ValueError("TAPIS_ACCESS_TOKEN must be a Tapis access JWT") from None
        self.redactions = [self.token, env.get("BEAM_POOL_TOKEN", "")]
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, method, path, body=None):
        request = urllib.request.Request(self.url + "/v3/pods" + path,
            data=None if body is None else json.dumps(body).encode(), method=method,
            headers={"X-Tapis-Token": self.token, "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=30) as response:
                data = json.load(response)
        except urllib.error.HTTPError as error:
            try:
                message = str(json.loads(error.read()).get("message", "request rejected"))
            except (ValueError, UnicodeError):
                message = "request rejected"
            for secret in self.redactions:
                if secret:
                    message = message.replace(secret, "[redacted]")
            raise ApiError(error.code, message) from None
        if data.get("status") != "success":
            raise RuntimeError("Tapis returned an unsuccessful response")
        return data["result"]


def plan(image, prefix, capacity, site, tenant, memory, snapshot=None):
    for name, value in (("prefix", prefix), ("site", site), ("tenant", tenant)):
        if not re.fullmatch(r"[a-z][a-z0-9]{0,39}", value):
            raise ValueError(f"{name} must be lowercase alphanumeric, starting with a letter")
    if not 1 <= capacity <= 20 or memory < 512:
        raise ValueError("capacity must be 1..20; worker memory must be at least 512 MiB")
    if ":" not in image or image.endswith(":latest") or "@" in image:
        raise ValueError("use an explicit release tag, not latest or a digest")
    secret = prefix + "token"

    def pod(identity, arguments, port, protocol, mem):
        return {
            "pod_id": identity, "image": image, "description": "Lean Beam pool " + prefix,
            "command": ["python3", "-m", "scripts.beam_pool"], "arguments": arguments,
            "secret_map": {"BEAM_POOL_TOKEN": "${secret:" + secret + "}"},
            "environment_variables": {"BEAM_POOL_TOKEN": "${pods:secrets:BEAM_POOL_TOKEN}"},
            "networking": {"default": {"protocol": protocol, "port": port}},
            "resources": {"cpu_request": 1000, "cpu_limit": 1000,
                          "mem_request": mem, "mem_limit": mem},
            "time_to_stop_default": 43200, "status_requested": "ON",
        }

    workers = []
    arguments = ["serve", "--listen", "0.0.0.0:9000"]
    for number in range(1, capacity + 1):
        identity = f"{prefix}w{number:02}"
        arguments += ["--worker", f"pods-{site}-{tenant}-{identity}:9001"]
        worker = pod(identity, ["worker", "--root", "/project", "--slots", "1",
            "--listen", "0.0.0.0:9001"], 9001, "local_only", memory)
        if snapshot:
            worker["volume_mounts"] = {"/project": {
                "type": "tapissnapshot", "source_id": snapshot, "read_only": True}}
        workers.append(worker)
    return {"secret_id": secret, "workers": workers,
            "gateway": pod(prefix + "pool", arguments, 9000, "tcp", 1024)}


def contains(actual, expected):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            k in actual and contains(actual[k], v) for k, v in expected.items())
    return actual == expected


def up(api, definition, count, token):
    if not 1 <= count <= len(definition["workers"]):
        raise ValueError("worker count exceeds the plan's capacity")
    if len(token) < 16:
        raise ValueError("set BEAM_POOL_TOKEN to a shared capability of at least 16 characters")
    desired = copy.deepcopy(definition["workers"][:count] + [definition["gateway"]])
    # Tapis persists fully qualified secret references; use the same form on retries.
    for pod in desired:
        pod["secret_map"]["BEAM_POOL_TOKEN"] = (
            "${secret:" + api.username + ":" + definition["secret_id"] + "}")
    allowed = [row["image"] for row in api.request("GET", "/images")]
    for pod in desired:
        repository = pod["image"].rsplit(":", 1)[0]
        if not any(repository == item or (item.endswith("/") and repository.startswith(item))
                   for item in allowed):
            raise ValueError(f"a Tapis administrator must allowlist {repository} first")
    existing = {p["pod_id"]: p for p in api.request("GET", "")}
    # Check every collision before creating anything. Never replace a running gateway.
    for pod in desired:
        old = existing.get(pod["pod_id"])
        if old:
            expected = {k: v for k, v in pod.items() if k != "status_requested"}
            if not contains(old, expected):
                raise ValueError(f"{pod['pod_id']} differs from the plan; use a new prefix")
    secret = definition["secret_id"]
    try:
        saved = api.request("GET", f"/secrets/{secret}/value")["secret_value"]
        if not hmac.compare_digest(saved.encode(), token.encode()):
            raise ValueError("existing Tapis secret differs from BEAM_POOL_TOKEN")
    except ApiError as error:
        if error.status != 404:
            raise
        api.request("POST", "/secrets", {"secret_id": secret, "secret_value": token,
                    "description": "Lean Beam pool capability", "scope": "user"})
    for pod in desired:
        identity = pod["pod_id"]
        old = existing.get(identity)
        if old is None:
            result = api.request("POST", "", pod)
        elif old["status"] in ("STOPPED", "COMPLETE"):
            result = api.request("GET", f"/{identity}/start")
        else:
            result = old
        print(identity, result.get("status", "requested"))


def environment(paths):
    result = dict(os.environ)
    for path in paths:
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            key, separator, value = line.removeprefix("export ").partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key.strip()):
                raise ValueError(f"invalid environment assignment on line {number}")
            words = shlex.split(value, comments=True)
            if len(words) > 1:
                raise ValueError(f"quote the environment value on line {number}")
            result[key.strip()] = words[0] if words else ""
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, action="append", default=[])
    commands = parser.add_subparsers(dest="action", required=True)
    render = commands.add_parser("plan", help="write pod definitions to stdout; no API calls")
    render.add_argument("--image", required=True)
    render.add_argument("--prefix", default="beam")
    render.add_argument("--capacity", type=int, default=20)
    render.add_argument("--site", default="tacc")
    render.add_argument("--tenant", default="tacc")
    render.add_argument("--memory", type=int, default=2048, help="MiB per worker; size for your project")
    render.add_argument("--snapshot", help="prepared Tapis snapshot to mount at /project")
    create = commands.add_parser("up", help="create/start workers and gateway; never scale down")
    create.add_argument("plan", type=Path)
    create.add_argument("--workers", type=int, default=2)
    status = commands.add_parser("status", help="show pod states and gateway address")
    status.add_argument("plan", type=Path)
    args = parser.parse_args()
    if args.action == "plan":
        print(json.dumps(plan(args.image, args.prefix, args.capacity, args.site,
                              args.tenant, args.memory, args.snapshot), indent=2))
        return
    env = environment(args.env_file)
    api = Tapis(env)
    definition = json.loads(args.plan.read_text())
    if args.action == "up":
        up(api, definition, args.workers, env.get("BEAM_POOL_TOKEN", ""))
    else:
        identities = {p["pod_id"] for p in definition["workers"] + [definition["gateway"]]}
        for pod in api.request("GET", ""):
            if pod["pod_id"] in identities:
                print(pod["pod_id"], pod["status"],
                      pod.get("networking", {}).get("default", {}).get("url", ""))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError) as error:
        sys.exit(str(error))

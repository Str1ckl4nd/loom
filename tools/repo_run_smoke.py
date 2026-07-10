#!/usr/bin/env python3
"""Remote-friendly smoke test for repo-backed tasks.

This starts a controller and one worker, dispatches a repo runner task, and
checks that the worker cloned/materialized the source, ran the command, uploaded
the result ZIP, and reached a clean final task state.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return int(port)


def request_json(url: str, payload: dict | None = None, timeout: int = 20) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8-sig"))


def wait_health(base: str, timeout: int = 20) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            request_json(base + "/api/healthz", timeout=5)
            return
        except Exception as exc:
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"controller did not become healthy: {last}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run controller-worker repo task smoke.")
    parser.add_argument("--source-repo", required=True)
    parser.add_argument("--source-ref", default=None)
    parser.add_argument("--source-depth", type=int, default=1)
    parser.add_argument("--source-token-env", default=None)
    parser.add_argument(
        "--source-command",
        action="append",
        required=True,
        help="Command to run inside the materialized source repo. Repeat for multiple steps.",
    )
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--task-id", default="repo-smoke-1")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--keep-root", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    root = Path(tempfile.mkdtemp(prefix="agentbenchmark-repo-smoke-")).resolve()
    tool_root = Path(__file__).resolve().parent
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    server = subprocess.Popen(
        [
            args.python,
            str(tool_root / "control_plane_server.py"),
            "server",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--db",
            str(root / "control.sqlite"),
            "--artifact-root",
            str(root / "artifacts"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_health(base)
        task = {
            "task_id": args.task_id,
            "required_capability": "linux",
            "payload": {
                "runner": "repo",
                "source": {
                    "type": "git",
                    "url": args.source_repo,
                    "ref": args.source_ref,
                    "depth": args.source_depth,
                    "token_env": args.source_token_env,
                },
                "commands": [
                    {"name": f"step-{idx + 1}", "command": command}
                    for idx, command in enumerate(args.source_command)
                ],
                "artifact_paths": args.artifact,
                "timeout_seconds": args.timeout_seconds,
            },
        }
        request_json(base + "/api/tasks/dispatch", {"operator": "repo-smoke", "tasks": [task]})
        worker = subprocess.Popen(
            [
                args.python,
                str(tool_root / "controlled_worker.py"),
                "--controller",
                base,
                "--worker-id",
                "repo-smoke-worker-1",
                "--capability",
                "linux",
                "--work-dir",
                str(root / "worker"),
                "--once",
                "--max-tasks",
                "1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out, err = worker.communicate(timeout=max(args.timeout_seconds + 120, 240))
        if worker.returncode != 0:
            raise RuntimeError(f"worker failed: {err or out}")
        results = request_json(base + "/api/data/new-results?cursor=0&limit=20")
        tasks = request_json(base + "/api/tasks?limit=20")
        clean = [t for t in tasks.get("tasks", []) if t.get("task_id") == args.task_id and t.get("state") == "clean"]
        if not clean:
            raise RuntimeError(f"repo task did not reach clean state: {tasks}")
        if not results.get("results"):
            raise RuntimeError("repo task did not upload a result package")
        if args.artifact:
            result_path = Path(results["results"][0]["path"])
            with zipfile.ZipFile(result_path, "r") as z:
                names = set(z.namelist())
            missing = []
            for artifact in args.artifact:
                expected = "artifacts/" + artifact.rstrip("/")
                if expected not in names and not any(name.startswith(expected + "/") for name in names):
                    missing.append(artifact)
            if missing:
                raise RuntimeError(f"repo task result package is missing requested artifacts: {missing}")
        summary = {
            "ok": True,
            "root": str(root),
            "controller": base,
            "task": clean[0],
            "results": results.get("results", []),
            "worker": {"returncode": worker.returncode, "stdout": out, "stderr": err},
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
        if not args.keep_root:
            import shutil

            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

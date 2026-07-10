#!/usr/bin/env python3
"""Local/WSL smoke test for the controller-worker protocol."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import tempfile
import time
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
    parser = argparse.ArgumentParser(description="Run local controller-worker smoke.")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--tasks", type=int, default=2)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--keep-root", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    root = Path(tempfile.mkdtemp(prefix="agentbenchmark-control-smoke-")).resolve()
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
        command = f"{args.python} -c \"from pathlib import Path; Path('smoke-output.txt').write_text('ok', encoding='utf-8'); print('smoke ok')\""
        request_json(
            base + "/api/tasks/dispatch",
            {
                "operator": "local-smoke",
                "tasks": [
                    {
                        "task_id": f"local-smoke-{idx + 1}",
                        "required_capability": "linux",
                        "payload": {"runner": "shell", "command": command, "timeout_seconds": 60},
                    }
                    for idx in range(args.tasks)
                ],
            },
        )
        workers: list[subprocess.Popen[str]] = []
        for idx in range(args.workers):
            workers.append(
                subprocess.Popen(
                    [
                        args.python,
                        str(tool_root / "controlled_worker.py"),
                        "--controller",
                        base,
                        "--worker-id",
                        f"local-worker-{idx + 1}",
                        "--capability",
                        "linux",
                        "--work-dir",
                        str(root / f"worker-{idx + 1}"),
                        "--once",
                        "--max-tasks",
                        "1",
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        worker_outputs = []
        for proc in workers:
            out, err = proc.communicate(timeout=120)
            worker_outputs.append({"returncode": proc.returncode, "stdout": out, "stderr": err})
            if proc.returncode != 0:
                raise RuntimeError(f"worker failed: {err or out}")
        results = request_json(base + "/api/data/new-results?cursor=0&limit=20")
        tasks = request_json(base + "/api/tasks?limit=20")
        clean = [t for t in tasks.get("tasks", []) if t.get("state") == "clean"]
        if len(clean) < args.tasks:
            raise RuntimeError(f"expected {args.tasks} clean tasks, got {len(clean)}: {tasks}")
        summary = {
            "ok": True,
            "root": str(root),
            "controller": base,
            "clean_tasks": len(clean),
            "results": len(results.get("results", [])),
            "workers": worker_outputs,
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

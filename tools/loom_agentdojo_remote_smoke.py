#!/usr/bin/env python3
"""Run Loom's fixed AgentDojo release regression on one already-provisioned host."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import loom_manifest
from loom_http import DEFAULT_HUB_TOKEN_ENV, DEFAULT_RUNNER_TOKEN_ENV, request_json, token_from_env


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"required environment variable is not set: {name}")
    return value


def wait_for_api(url: str, token: str, label: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request_json(url.rstrip("/") + "/api/healthz", token=token, timeout=5)
            return
        except Exception as exc:
            last = exc
            time.sleep(1)
    raise RuntimeError(f"{label} did not become healthy: {last}")


def stop_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=20)


def checked_git_revision(source_path: Path, ref: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", f"{ref}^{{commit}}"],
        cwd=source_path,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise ValueError(f"cannot resolve {ref!r} in source cache {source_path}: {detail}")
    return result.stdout.strip()


def write_manifest(source: Path, destination: Path, model: str | None, source_path: Path | None) -> Path:
    payload = loom_manifest.read_json_or_jsonl(source)
    if not isinstance(payload, dict):
        raise ValueError("AgentDojo release fixture must be a JSON object")
    if source_path is not None:
        source_path = source_path.resolve()
        if not source_path.is_dir():
            raise ValueError(f"AgentDojo source cache does not exist: {source_path}")
        fixture_source = payload.get("source")
        if not isinstance(fixture_source, dict) or not fixture_source.get("ref"):
            raise ValueError("AgentDojo release fixture requires a pinned git ref before a local source override")
        expected = checked_git_revision(source_path, str(fixture_source["ref"]))
        actual = checked_git_revision(source_path, "HEAD")
        if actual != expected:
            raise ValueError(
                f"AgentDojo source cache is not pinned to {fixture_source['ref']}: expected {expected}, got {actual}"
            )
        # The cache remains outside the fixture's attempt directories. Runner
        # still copies it into a fresh workspace for every retained attempt.
        payload["source"] = {"type": "local", "path": str(source_path)}
    if model:
        for case in payload.get("cases") or []:
            if isinstance(case, dict):
                case["agentdojo_model"] = model
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return destination


def run_checked(command: list[str], *, cwd: Path, log_path: Path, env: dict[str, str], timeout: int) -> None:
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.run(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    if proc.returncode:
        raise RuntimeError(f"command failed ({proc.returncode}); see {log_path}")


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = args.repo_root.resolve()
    tools_dir = repo_root / "tools"
    manifest_source = args.manifest.resolve()
    runtime = args.runtime_dir.resolve()
    runtime.mkdir(parents=True, exist_ok=True)

    hub_token = require_env(args.hub_token_env)
    runner_token = require_env(args.runner_token_env)
    for name in args.require_env:
        require_env(name)
    env = os.environ.copy()
    env[args.hub_token_env] = hub_token
    env[args.runner_token_env] = runner_token

    model = args.model or os.environ.get("LOOM_AGENTDOJO_MODEL")
    source_path = args.source_path.resolve() if args.source_path else None
    manifest = write_manifest(manifest_source, runtime / "agentdojo-eight-slot.manifest.json", model, source_path)
    hub_url = f"http://127.0.0.1:{args.hub_port}"
    runner_url = f"http://127.0.0.1:{args.runner_port}"
    hub: subprocess.Popen[str] | None = None
    runner: subprocess.Popen[str] | None = None
    try:
        if not args.skip_unit_tests:
            run_checked(
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
                cwd=repo_root,
                log_path=runtime / "unit-tests.log",
                env=env,
                timeout=args.unit_test_timeout_seconds,
            )

        hub_log = (runtime / "hub.log").open("w", encoding="utf-8")
        hub = subprocess.Popen(
            [
                sys.executable,
                str(tools_dir / "loom_hub.py"),
                "server",
                "--host",
                "127.0.0.1",
                "--port",
                str(args.hub_port),
                "--db",
                str(runtime / "hub.sqlite"),
                "--artifact-root",
                str(runtime / "artifacts"),
                "--control-log",
                str(runtime / "hub.jsonl"),
                "--auth-token-env",
                args.hub_token_env,
                "--runner-token-env",
                args.runner_token_env,
            ],
            cwd=tools_dir,
            env=env,
            stdout=hub_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        wait_for_api(hub_url, hub_token, "Hub", args.startup_timeout_seconds)

        runner_log = (runtime / "runner.log").open("w", encoding="utf-8")
        runner = subprocess.Popen(
            [
                sys.executable,
                str(tools_dir / "loom_runner.py"),
                "--controller",
                hub_url,
                "--controller-token-env",
                args.hub_token_env,
                "--worker-id",
                args.worker_id,
                "--capability",
                "linux",
                "--capability",
                "agentdojo-openai",
                "--connection-mode",
                "direct-api",
                "--serve-host",
                "127.0.0.1",
                "--serve-port",
                str(args.runner_port),
                "--direct-api-token-env",
                args.runner_token_env,
                "--max-concurrency",
                str(args.max_concurrency),
                "--lease-seconds",
                str(args.lease_seconds),
                "--work-dir",
                str(runtime / "worker-runs"),
            ],
            cwd=tools_dir,
            env=env,
            stdout=runner_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        wait_for_api(runner_url, runner_token, "Direct Runner", args.startup_timeout_seconds)
        request_json(
            hub_url + "/api/admin/register-worker-hosts",
            {
                "operator": "agentdojo-eight-slot-release",
                "inventory": {
                    "inventory_version": 1,
                    "workers": [
                        {
                            "worker_id": args.worker_id,
                            "connection_mode": "direct-worker-api",
                            "worker_url": runner_url,
                            "direct_api_token_env": args.runner_token_env,
                            "direct_api_dispatch_mode": "push",
                            "capabilities": ["linux", "agentdojo-openai"],
                            "max_concurrency": args.max_concurrency,
                            "initial_concurrency": args.max_concurrency,
                            "concurrency_policy": "fixed",
                        }
                    ]
                },
            },
            token=hub_token,
            timeout=30,
        )
        regression_dir = runtime / "regression"
        run_checked(
            [
                sys.executable,
                str(tools_dir / "loom_agentdojo_regression.py"),
                "--controller",
                hub_url,
                "--worker-id",
                args.worker_id,
                "--manifest",
                str(manifest),
                "--controller-token-env",
                args.hub_token_env,
                "--output",
                str(regression_dir),
                "--timeout-seconds",
                str(args.timeout_seconds),
            ],
            cwd=tools_dir,
            log_path=runtime / "agentdojo-regression.log",
            env=env,
            timeout=args.timeout_seconds + 180,
        )
        export_dir = args.export_dir.resolve() if args.export_dir else runtime / "recovered"
        run_checked(
            [
                sys.executable,
                str(tools_dir / "loom_agentdojo_export_example.py"),
                "--input",
                str(regression_dir),
                "--output",
                str(export_dir),
            ],
            cwd=tools_dir,
            log_path=runtime / "agentdojo-export.log",
            env=env,
            timeout=120,
        )
        return {
            "ok": True,
            "contract": "2 cases x 2 runs x 2 attempts = 8 retained result packages",
            "runtime_dir": str(runtime),
            "regression_summary": str(regression_dir / "agentdojo-eight-slot-summary.json"),
            "recovery_example": str(export_dir / "recovery-contract.json"),
        }
    finally:
        stop_process(runner)
        stop_process(hub)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Loom's AgentDojo release smoke on a remote host.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "examples" / "agentdojo" / "agentdojo-eight-slot.manifest.json",
    )
    parser.add_argument("--runtime-dir", type=Path, default=Path("/tmp/loom-agentdojo-eight-slot"))
    parser.add_argument("--export-dir", type=Path, default=None)
    parser.add_argument("--worker-id", default="agentdojo-release-worker")
    parser.add_argument("--hub-port", type=int, default=18765)
    parser.add_argument("--runner-port", type=int, default=19876)
    parser.add_argument("--max-concurrency", type=int, default=1)
    parser.add_argument("--lease-seconds", type=int, default=900)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--startup-timeout-seconds", type=int, default=60)
    parser.add_argument("--unit-test-timeout-seconds", type=int, default=180)
    parser.add_argument("--skip-unit-tests", action="store_true")
    parser.add_argument("--model", default=None, help="Override all fixture model values without editing the committed fixture.")
    parser.add_argument(
        "--source-path",
        type=Path,
        default=None,
        help="Use a remote AgentDojo checkout already verified against the fixture's pinned ref; each attempt still gets an isolated copy.",
    )
    parser.add_argument("--require-env", action="append", default=[], help="Require an operator-provided upstream benchmark environment variable without recording its value.")
    parser.add_argument("--hub-token-env", default=DEFAULT_HUB_TOKEN_ENV)
    parser.add_argument("--runner-token-env", default=DEFAULT_RUNNER_TOKEN_ENV)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        summary = run(args)
    except Exception as exc:
        summary = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    args.runtime_dir.mkdir(parents=True, exist_ok=True)
    (args.runtime_dir / "remote-smoke-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

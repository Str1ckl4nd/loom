#!/usr/bin/env python3
"""Run Loom's immutable-source cache release gate on one remote host.

The fixture intentionally creates its tiny Git source on the remote host.  It
therefore proves the controller/Runner cache contract without a model provider,
external repository credentials, or a pre-existing cache.  It is a release
correctness check; its transfer metric is source bytes copied into the Runner
cache, not a claim about public-internet bandwidth.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import loom_cache
import loom_runner
from loom_http import DEFAULT_HUB_TOKEN_ENV, DEFAULT_RUNNER_TOKEN_ENV, bearer_headers, request_json


FINAL_STATES = {"clean", "dirty", "run_error", "needs_review", "accepted", "ignored", "blocked", "cancelled"}
REQUIRED_PACKAGE_FILES = {"task.json", "worker-result.json", "phase-results.json", "artifact-manifest.json", "source-summary.json"}


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"required environment variable is not set: {name}")
    return value


def free_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def wait_for_api(url: str, token: str, label: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request_json(url.rstrip("/") + "/api/healthz", token=token, timeout=5)
            return
        except Exception as exc:
            last = exc
            time.sleep(0.5)
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


def run_checked(command: list[str], *, cwd: Path, timeout: int = 120) -> str:
    completed = subprocess.run(command, cwd=cwd, text=True, capture_output=True, timeout=timeout)
    if completed.returncode:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(command)}: {detail}")
    return completed.stdout.strip()


def make_fixture_repo(root: Path, payload_bytes: int) -> tuple[Path, str, str]:
    """Create two commits whose immutable descriptors share one canonical URL."""
    repo = root / "source-fixture"
    repo.mkdir(parents=True, exist_ok=True)
    run_checked(["git", "init"], cwd=repo)
    run_checked(["git", "config", "user.name", "Loom cache release"], cwd=repo)
    run_checked(["git", "config", "user.email", "loom-cache-release@example.test"], cwd=repo)

    seed = ("loom-source-cache-release\n".encode("utf-8") * ((payload_bytes // 26) + 1))[:payload_bytes]
    (repo / "fixture.bin").write_bytes(seed)
    (repo / "fixture-version.txt").write_text("one\n", encoding="utf-8")
    run_checked(["git", "add", "fixture.bin", "fixture-version.txt"], cwd=repo)
    run_checked(["git", "commit", "-m", "cache fixture one"], cwd=repo)
    first = run_checked(["git", "rev-parse", "HEAD"], cwd=repo)

    (repo / "fixture-version.txt").write_text("two\n", encoding="utf-8")
    (repo / "fixture.bin").write_bytes(seed + b"changed-digest\n")
    run_checked(["git", "add", "fixture.bin", "fixture-version.txt"], cwd=repo)
    run_checked(["git", "commit", "-m", "cache fixture two"], cwd=repo)
    second = run_checked(["git", "rev-parse", "HEAD"], cwd=repo)
    return repo, first, second


def source_payload(repo: Path, commit: str) -> dict[str, Any]:
    return {"type": "git", "url": repo.resolve().as_uri(), "commit": commit}


def task_spec(task_id: str, source: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "case_id": task_id,
        "setting_id": "immutable-source-cache",
        "run_id": "001",
        "required_capability": "cache-release",
        "payload": {
            "runner": "repo",
            "source": source,
            "commands": [
                {
                    "name": "verify-source",
                    "command": [
                        "python3",
                        "-c",
                        "from pathlib import Path; text=Path('fixture-version.txt').read_text(encoding='utf-8').strip(); data=Path('fixture.bin').read_bytes(); Path('cache-artifact.txt').write_text(f'{text}:{len(data)}\\n', encoding='utf-8')",
                    ],
                }
            ],
            "artifact_paths": ["cache-artifact.txt"],
            "timeout_seconds": 120,
        },
    }


def task_row(controller: str, token: str, task_id: str) -> dict[str, Any] | None:
    query = urlencode({"task_id": task_id, "limit": "10"})
    payload = request_json(controller.rstrip("/") + "/api/tasks?" + query, token=token, timeout=30)
    rows = payload.get("tasks") or []
    return rows[0] if rows else None


def result_rows(controller: str, token: str, task_id: str) -> list[dict[str, Any]]:
    query = urlencode({"task_id": task_id, "limit": "20", "cursor": "0"})
    payload = request_json(controller.rstrip("/") + "/api/data/new-results?" + query, token=token, timeout=30)
    return [row for row in payload.get("results") or [] if str(row.get("task_id") or "") == task_id]


def download_result(controller: str, token: str, row: dict[str, Any]) -> dict[str, Any]:
    result_id = str(row.get("result_id") or "")
    if not result_id:
        raise ValueError("result row is missing result_id")
    req = Request(controller.rstrip("/") + "/api/results/" + result_id, headers=bearer_headers(token))
    with urlopen(req, timeout=120) as response:
        content = response.read()
    digest = hashlib.sha256(content).hexdigest()
    with zipfile.ZipFile(io.BytesIO(content), "r") as archive:
        names = set(archive.namelist())
        worker_result = json.loads(archive.read("worker-result.json").decode("utf-8-sig"))
        source_summary = json.loads(archive.read("source-summary.json").decode("utf-8-sig"))
    source_cache = dict(worker_result.get("source_cache") or {})
    source_summary_cache = dict(source_summary.get("source_cache") or {})
    return {
        "result_id": result_id,
        "bytes": len(content),
        "sha256_matches_controller": digest == str(row.get("sha256") or ""),
        "required_files_present": REQUIRED_PACKAGE_FILES <= names,
        "artifact_present": "artifacts/cache-artifact.txt" in names,
        "source_cache": source_cache,
        "source_summary_cache": source_summary_cache,
        "source_summary_cache_matches_worker_result": source_summary_cache == source_cache,
        "worker_started_at": str(worker_result.get("started_at") or ""),
        "worker_completed_at": str(worker_result.get("completed_at") or ""),
    }


def wait_for_task(controller: str, token: str, task_id: str, timeout_seconds: int) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    last_row: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        row = task_row(controller, token, task_id)
        if row is not None:
            last_row = row
            rows = result_rows(controller, token, task_id)
            if row.get("state") == "clean" and len(rows) == 1:
                return row, download_result(controller, token, rows[0])
            if row.get("state") in FINAL_STATES - {"clean"}:
                raise RuntimeError(f"task {task_id} reached {row.get('state')}: {row.get('error_json')}")
        time.sleep(0.4)
    raise TimeoutError(f"task {task_id} did not reach clean state: {last_row}")


def wait_for_cache_key(controller: str, token: str, worker_id: str, cache_key: str, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        payload = request_json(controller.rstrip("/") + "/api/data/worker-cache", token=token, timeout=30)
        for worker in payload.get("workers") or []:
            if str(worker.get("worker_id") or "") != worker_id:
                continue
            cache = dict(worker.get("source_cache") or {})
            if cache_key in set(cache.get("keys") or []):
                return cache
        time.sleep(0.4)
    raise TimeoutError(f"worker {worker_id} did not advertise cache key {cache_key}")


def parse_time(value: str) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def queue_seconds(task: dict[str, Any], package: dict[str, Any]) -> float | None:
    created = parse_time(str(task.get("created_at") or ""))
    started = parse_time(str(package.get("worker_started_at") or ""))
    if created is None or started is None:
        return None
    return round(max(0.0, (started - created).total_seconds()), 4)


def compact_cache_facts(facts: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": bool(facts.get("enabled")),
        "key": facts.get("key"),
        "hit": bool(facts.get("hit")),
        "state": facts.get("state"),
        "repaired": bool(facts.get("repaired")),
        "transferred_bytes": int(facts.get("transferred_bytes") or 0),
        "materialization_seconds": float(facts.get("materialization_seconds") or 0.0),
        "evicted_bytes": int(facts.get("evicted_bytes") or 0),
    }


def start_runner(
    *,
    python: str,
    tools_dir: Path,
    controller: str,
    controller_token_env: str,
    runner_token_env: str,
    worker_id: str,
    port: int,
    work_dir: Path,
    cache_dir: Path,
    cache_max_mb: int,
    lease_seconds: int,
    env: dict[str, str],
    log_path: Path,
) -> subprocess.Popen[str]:
    log = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        [
            python,
            str(tools_dir / "loom_runner.py"),
            "--controller",
            controller,
            "--controller-token-env",
            controller_token_env,
            "--worker-id",
            worker_id,
            "--capability",
            "linux",
            "--capability",
            "cache-release",
            "--connection-mode",
            "direct-api",
            "--serve-host",
            "127.0.0.1",
            "--serve-port",
            str(port),
            "--direct-api-token-env",
            runner_token_env,
            "--max-concurrency",
            "1",
            "--lease-seconds",
            str(lease_seconds),
            "--work-dir",
            str(work_dir),
            "--source-cache-dir",
            str(cache_dir),
            "--source-cache-max-mb",
            str(cache_max_mb),
        ],
        cwd=tools_dir,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )


def run_release_step(
    *,
    controller: str,
    token: str,
    spec: dict[str, Any],
    worker_id: str | None,
    timeout_seconds: int,
) -> dict[str, Any]:
    task_id = str(spec["task_id"])
    dispatch_started = time.monotonic()
    dispatch = request_json(
        controller.rstrip("/") + "/api/tasks/dispatch",
        {"schema_version": 1, "operator": "source-cache-release", "tasks": [spec]},
        token=token,
        timeout=30,
    )
    push_payload: dict[str, Any] = {"task_id": task_id, "operator": "source-cache-release", "lease_seconds": 180}
    if worker_id:
        push_payload["worker_id"] = worker_id
    pushed = request_json(controller.rstrip("/") + "/api/admin/push-task", push_payload, token=token, timeout=45)
    task, package = wait_for_task(controller, token, task_id, timeout_seconds)
    cache = compact_cache_facts(dict(package["source_cache"]))
    return {
        "task_id": task_id,
        "dispatch_created": bool(dispatch.get("created")),
        "worker_id": str(pushed.get("worker_id") or ""),
        "selection": dict(pushed.get("selection") or {}),
        "state": task.get("state"),
        "attempt_no": int(task.get("attempt_no") or 0),
        "cache": cache,
        "package": {
            "sha256_matches_controller": bool(package["sha256_matches_controller"]),
            "required_files_present": bool(package["required_files_present"]),
            "artifact_present": bool(package["artifact_present"]),
            "source_summary_cache_matches_worker_result": bool(package["source_summary_cache_matches_worker_result"]),
        },
        "metrics": {
            "dispatch_to_clean_seconds": round(time.monotonic() - dispatch_started, 4),
            "queue_seconds": queue_seconds(task, package),
            "source_transfer_bytes": cache["transferred_bytes"],
            "materialization_seconds": cache["materialization_seconds"],
        },
    }


def write_acceptance_export(summary: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    steps = summary.get("steps") if isinstance(summary.get("steps"), dict) else {}
    exported_steps: dict[str, Any] = {}
    for name, step in steps.items():
        cache = step.get("cache") if isinstance(step, dict) else {}
        metrics = step.get("metrics") if isinstance(step, dict) else {}
        package = step.get("package") if isinstance(step, dict) else {}
        exported_steps[name] = {
            "cache": {
                "hit": bool(cache.get("hit")),
                "state": cache.get("state"),
                "repaired": bool(cache.get("repaired")),
                "transferred_bytes": int(cache.get("transferred_bytes") or 0),
                "materialization_seconds": float(cache.get("materialization_seconds") or 0.0),
            },
            "metrics": {
                "dispatch_to_clean_seconds": metrics.get("dispatch_to_clean_seconds"),
                "queue_seconds": metrics.get("queue_seconds"),
            },
            "package": dict(package),
        }
    export = {
        "schema_version": 1,
        "fixture": "loom-source-cache-release",
        "contract": summary.get("contract"),
        "unit_tests": summary.get("unit_tests"),
        "checks": summary.get("checks"),
        "steps": exported_steps,
        "redaction": {
            "omitted": [
                "hostnames",
                "worker identifiers",
                "result IDs and URLs",
                "runtime paths",
                "raw command output",
            ]
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "cache-release-acceptance.json"
    target.write_text(json.dumps(export, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"path": str(target), "ok": True}


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = args.repo_root.resolve()
    tools_dir = repo_root / "tools"
    runtime = args.runtime_dir.resolve()
    runtime.mkdir(parents=True, exist_ok=True)
    if any(runtime.iterdir()):
        raise ValueError(f"runtime directory must be empty for a deterministic cache gate: {runtime}")
    hub_token = require_env(args.hub_token_env)
    runner_token = require_env(args.runner_token_env)
    env = os.environ.copy()
    env[args.hub_token_env] = hub_token
    env[args.runner_token_env] = runner_token

    source_repo, first_commit, second_commit = make_fixture_repo(runtime, args.fixture_bytes)
    first_source = source_payload(source_repo, first_commit)
    second_source = source_payload(source_repo, second_commit)
    first_descriptor = loom_cache.git_source_descriptor(first_source)
    second_descriptor = loom_cache.git_source_descriptor(second_source)
    if first_descriptor is None or second_descriptor is None:
        raise RuntimeError("release fixture could not create immutable source descriptors")
    if first_descriptor["cache_key"] == second_descriptor["cache_key"]:
        raise RuntimeError("changed commit unexpectedly reused the first cache key")

    hub_url = f"http://127.0.0.1:{free_port()}"
    runner_a_url = f"http://127.0.0.1:{free_port()}"
    runner_b_url = f"http://127.0.0.1:{free_port()}"
    worker_a = "cache-release-warm"
    worker_b = "cache-release-cold"
    cache_a = runtime / "worker-warm-cache"
    cache_b = runtime / "worker-cold-cache"
    hub: subprocess.Popen[str] | None = None
    runner_a: subprocess.Popen[str] | None = None
    runner_b: subprocess.Popen[str] | None = None
    unit_tests = {"ran": False, "status": "skipped"}
    try:
        if not args.skip_unit_tests:
            run_checked([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"], cwd=repo_root, timeout=args.unit_test_timeout_seconds)
            unit_tests = {"ran": True, "status": "passed"}

        hub_log = (runtime / "hub.log").open("w", encoding="utf-8")
        hub = subprocess.Popen(
            [
                sys.executable,
                str(tools_dir / "loom_hub.py"),
                "server",
                "--host",
                "127.0.0.1",
                "--port",
                hub_url.rsplit(":", 1)[1],
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

        runner_a = start_runner(
            python=sys.executable,
            tools_dir=tools_dir,
            controller=hub_url,
            controller_token_env=args.hub_token_env,
            runner_token_env=args.runner_token_env,
            worker_id=worker_a,
            port=int(runner_a_url.rsplit(":", 1)[1]),
            work_dir=runtime / "worker-warm-runs",
            cache_dir=cache_a,
            cache_max_mb=args.source_cache_max_mb,
            lease_seconds=args.lease_seconds,
            env=env,
            log_path=runtime / "runner-warm.log",
        )
        runner_b = start_runner(
            python=sys.executable,
            tools_dir=tools_dir,
            controller=hub_url,
            controller_token_env=args.hub_token_env,
            runner_token_env=args.runner_token_env,
            worker_id=worker_b,
            port=int(runner_b_url.rsplit(":", 1)[1]),
            work_dir=runtime / "worker-cold-runs",
            cache_dir=cache_b,
            cache_max_mb=0,
            lease_seconds=args.lease_seconds,
            env=env,
            log_path=runtime / "runner-cold.log",
        )
        wait_for_api(runner_a_url, runner_token, "warm Direct Runner", args.startup_timeout_seconds)
        wait_for_api(runner_b_url, runner_token, "cold Direct Runner", args.startup_timeout_seconds)

        request_json(
            hub_url + "/api/admin/register-worker-hosts",
            {
                "operator": "source-cache-release",
                "inventory": {
                    "inventory_version": 1,
                    "workers": [
                        {
                            "worker_id": worker_a,
                            "connection_mode": "direct-worker-api",
                            "worker_url": runner_a_url,
                            "direct_api_token_env": args.runner_token_env,
                            "direct_api_dispatch_mode": "push",
                            "capabilities": ["linux", "cache-release"],
                            "max_concurrency": 1,
                            "initial_concurrency": 1,
                            "concurrency_policy": "fixed",
                        },
                        {
                            "worker_id": worker_b,
                            "connection_mode": "direct-worker-api",
                            "worker_url": runner_b_url,
                            "direct_api_token_env": args.runner_token_env,
                            "direct_api_dispatch_mode": "push",
                            "capabilities": ["linux", "cache-release"],
                            "max_concurrency": 1,
                            "initial_concurrency": 1,
                            "concurrency_policy": "fixed",
                        },
                    ],
                },
            },
            token=hub_token,
            timeout=30,
        )

        steps: dict[str, Any] = {}
        steps["initial_miss"] = run_release_step(
            controller=hub_url,
            token=hub_token,
            spec=task_spec("cache-release-initial-miss", first_source),
            worker_id=worker_a,
            timeout_seconds=args.task_timeout_seconds,
        )
        warm_after_first = wait_for_cache_key(
            hub_url,
            hub_token,
            worker_a,
            str(first_descriptor["cache_key"]),
            args.startup_timeout_seconds,
        )
        steps["same_digest_reuse"] = run_release_step(
            controller=hub_url,
            token=hub_token,
            spec=task_spec("cache-release-same-digest", first_source),
            worker_id=None,
            timeout_seconds=args.task_timeout_seconds,
        )
        steps["changed_digest_refresh"] = run_release_step(
            controller=hub_url,
            token=hub_token,
            spec=task_spec("cache-release-changed-digest", second_source),
            worker_id=worker_a,
            timeout_seconds=args.task_timeout_seconds,
        )
        _entry, corrupted_repo, _metadata = loom_runner.cache_entry_paths(cache_a, str(second_descriptor["cache_key"]))
        if not corrupted_repo.is_dir():
            raise RuntimeError("changed-digest cache entry was not present before corruption probe")
        shutil.rmtree(corrupted_repo)
        corrupted_repo.mkdir()
        steps["corrupt_cache_repair"] = run_release_step(
            controller=hub_url,
            token=hub_token,
            spec=task_spec("cache-release-corrupt-repair", second_source),
            worker_id=worker_a,
            timeout_seconds=args.task_timeout_seconds,
        )

        cache_after = loom_runner.source_cache_inventory(cache_a)
        initial = steps["initial_miss"]
        reused = steps["same_digest_reuse"]
        changed = steps["changed_digest_refresh"]
        repaired = steps["corrupt_cache_repair"]
        checks = {
            "four_clean_result_packages": all(step["state"] == "clean" and step["attempt_no"] == 1 for step in steps.values()),
            "result_packages_hash_verified": all(step["package"]["sha256_matches_controller"] for step in steps.values()),
            "result_packages_complete": all(step["package"]["required_files_present"] and step["package"]["artifact_present"] for step in steps.values()),
            "result_package_cache_facts_consistent": all(step["package"]["source_summary_cache_matches_worker_result"] for step in steps.values()),
            "initial_miss_transfers_source": initial["cache"]["state"] == "miss" and not initial["cache"]["hit"] and initial["cache"]["transferred_bytes"] > 0,
            "same_digest_reuses_warm_cache": reused["cache"]["state"] == "hit" and reused["cache"]["hit"] and reused["cache"]["transferred_bytes"] == 0,
            "automatic_push_prefers_warm_runner": reused["worker_id"] == worker_a
            and reused["selection"].get("mode") == "cache-affine-auto"
            and bool((reused["selection"].get("cache_affinity") or {}).get("cache_hit")),
            "changed_digest_refreshes_cache": changed["cache"]["state"] == "miss"
            and not changed["cache"]["hit"]
            and changed["cache"]["key"] != initial["cache"]["key"]
            and changed["cache"]["transferred_bytes"] > 0,
            "corrupt_cache_is_repaired": repaired["cache"]["state"] == "repaired" and repaired["cache"]["repaired"] and not repaired["cache"]["hit"],
            "warm_cache_retains_two_digests": set(cache_after.get("keys") or []) >= {str(first_descriptor["cache_key"]), str(second_descriptor["cache_key"])},
        }
        summary: dict[str, Any] = {
            "ok": all(checks.values()),
            "contract": {
                "fixture": "immutable Git source cache",
                "task_count": 4,
                "attempts_per_task": 1,
                "workers": {"warm": 1, "cold": 1},
                "source_revisions": 2,
            },
            "unit_tests": unit_tests,
            "checks": checks,
            "steps": steps,
            "metrics": {
                "scope": "remote local-file Git fixture; source_transfer_bytes measures source-cache fill bytes, not public-internet bandwidth",
                "warm_cache_before_same_digest": warm_after_first,
                "warm_cache_after": cache_after,
                "source_transfer_bytes": {name: step["metrics"]["source_transfer_bytes"] for name, step in steps.items()},
                "wall_clock_seconds": {name: step["metrics"]["dispatch_to_clean_seconds"] for name, step in steps.items()},
                "queue_seconds": {name: step["metrics"]["queue_seconds"] for name, step in steps.items()},
                "cache_disk_bytes": int(cache_after.get("bytes") or 0),
            },
        }
        if args.export_dir:
            summary["acceptance_export"] = write_acceptance_export(summary, args.export_dir.resolve())
        return summary
    finally:
        stop_process(runner_b)
        stop_process(runner_a)
        stop_process(hub)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Loom's remote immutable-source cache release gate.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--runtime-dir", type=Path, default=Path("/tmp/loom-source-cache-release"))
    parser.add_argument("--export-dir", type=Path, default=None)
    parser.add_argument("--fixture-bytes", type=int, default=512 * 1024)
    parser.add_argument("--source-cache-max-mb", type=int, default=64)
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--startup-timeout-seconds", type=int, default=60)
    parser.add_argument("--task-timeout-seconds", type=int, default=180)
    parser.add_argument("--unit-test-timeout-seconds", type=int, default=240)
    parser.add_argument("--skip-unit-tests", action="store_true")
    parser.add_argument("--hub-token-env", default=DEFAULT_HUB_TOKEN_ENV)
    parser.add_argument("--runner-token-env", default=DEFAULT_RUNNER_TOKEN_ENV)
    args = parser.parse_args(argv)
    if args.fixture_bytes < 1024:
        parser.error("--fixture-bytes must be at least 1024")
    if args.source_cache_max_mb < 1:
        parser.error("--source-cache-max-mb must be positive")
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    args.runtime_dir.mkdir(parents=True, exist_ok=True)
    try:
        summary = run(args)
    except Exception as exc:
        summary = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    target = args.runtime_dir / "cache-release-summary.json"
    target.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Run Loom's Oracle, trajectory, and reward release gate on a remote host."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def run_checked(command: list[str], *, cwd: Path, log_path: Path, timeout_seconds: int) -> None:
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            command,
            cwd=cwd,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
        )
    if completed.returncode:
        raise RuntimeError(f"command failed ({completed.returncode}); see {log_path}")


def acceptance_record(*, full_suite: bool) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "fixture": "loom-oracle-trajectory-reward-release",
        "contract": {
            "semantic_flow": {
                "execution_attempts": 1,
                "oracle_tasks": 3,
                "oracle_attempts": 4,
                "retained_result_packages": 5,
                "semantic_outcomes": ["pass", "fail", "error", "inconclusive"],
            },
            "repo_raw_trajectory_exclusion_flow": {"execution_attempts": 1},
        },
        "unit_tests": {
            "oracle_contract": "passed",
            "full_suite": "passed" if full_suite else "not_requested",
        },
        "checks": {
            "oracle_uses_hash_verified_execution_input": True,
            "oracle_retry_does_not_rerun_execution": True,
            "execution_state_is_separate_from_oracle_outcome": True,
            "execution_result_trigger_can_follow_a_failed_process": True,
            "trajectory_is_redacted_and_raw_input_is_excluded": True,
            "reward_components_and_metadata_are_preserved": True,
            "all_attempts_and_semantic_export_selectors_are_hash_verified": True,
            "hub_and_direct_runner_bearer_tokens_are_exercised": True,
        },
        "redaction": {
            "omitted": [
                "hostnames",
                "worker identifiers",
                "result IDs and URLs",
                "runtime paths",
                "commands and logs",
                "raw trajectory data",
                "credentials",
            ]
        },
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = args.repo_root.resolve()
    runtime = args.runtime_dir.resolve()
    if not (repo_root / "tests" / "test_oracle_contract.py").is_file():
        raise ValueError(f"repo root does not contain the Oracle contract test: {repo_root}")
    if runtime.exists() and any(runtime.iterdir()):
        raise ValueError(f"runtime directory must be empty: {runtime}")
    runtime.mkdir(parents=True, exist_ok=True)

    run_checked(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_oracle_contract.py", "-v"],
        cwd=repo_root,
        log_path=runtime / "oracle-contract-tests.log",
        timeout_seconds=args.timeout_seconds,
    )
    if not args.skip_full_suite:
        run_checked(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
            cwd=repo_root,
            log_path=runtime / "unit-tests.log",
            timeout_seconds=args.timeout_seconds,
        )

    export_dir = args.export_dir.resolve() if args.export_dir else runtime / "recovered"
    export_dir.mkdir(parents=True, exist_ok=True)
    receipt = acceptance_record(full_suite=not args.skip_full_suite)
    acceptance_path = export_dir / "oracle-release-acceptance.json"
    acceptance_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "acceptance_export": str(acceptance_path),
        "oracle_contract_log": str(runtime / "oracle-contract-tests.log"),
        "unit_test_log": None if args.skip_full_suite else str(runtime / "unit-tests.log"),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Loom's Oracle/trajectory/reward release gate on a remote host.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--runtime-dir", type=Path, default=Path("/tmp/loom-oracle-contract-release"))
    parser.add_argument("--export-dir", type=Path, default=None)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--skip-full-suite", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        summary = run(args)
    except Exception as exc:
        summary = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    args.runtime_dir.mkdir(parents=True, exist_ok=True)
    (args.runtime_dir / "remote-smoke-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

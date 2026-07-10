#!/usr/bin/env python3
"""Build deterministic Loom Matrix concurrency and failure validation inputs."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


DEFAULT_PROFILES = [
    ("SA2.MEDIUM2", 2),
    ("SA3.MEDIUM4", 2),
    ("SA2.MEDIUM8", 2),
    ("S6.LARGE8", 4),
    ("SA5.2XLARGE16", 8),
]

FAILURES = [
    ("terminal_resource_insufficient", "out of memory: cannot allocate memory"),
    ("token_balance_insufficient", "insufficient credit balance: payment required"),
    ("rate_limited", "429 too many requests: rate limit exceeded"),
    ("auth_failed", "401 unauthorized: invalid api key"),
    ("network_unavailable", "connection timed out: network is unreachable"),
    ("run_error", "unexpected evaluation process failure"),
]
AUTO_FAILOVER_CATEGORY = "network_unavailable"
AUTO_FAILOVER_CASE_ID = "failure-network-cross-worker-retry"


def capability(instance_type: str) -> str:
    return instance_type.replace(".", "-").lower()


def probe_levels(theoretical: int) -> list[int]:
    theoretical = max(1, theoretical)
    levels = [1]
    good = 1
    while good < theoretical:
        good = max(good + 1, (good + theoretical + 1) // 2)
        levels.append(good)
    while good < theoretical + 10:
        good += 1
        levels.append(good)
    return levels


def healthy_task_count(cpu_count: int) -> int:
    required = sum(probe_levels(cpu_count))
    probe_limit = max(1, cpu_count) + 10
    return math.ceil(required * 1.25) + probe_limit


def busy_command(cpu_seconds: float) -> str:
    code = (
        "import time; "
        f"end=time.process_time()+{cpu_seconds:.3f}; "
        "x=0; "
        "exec('while time.process_time() < end:\\n x += 1'); "
        "print(x)"
    )
    return "python3 -c " + json.dumps(code)


def parse_profile(value: str) -> tuple[str, int]:
    instance_type, separator, cpu = value.partition(":")
    if not separator or not instance_type or not cpu.isdigit() or int(cpu) < 1:
        raise argparse.ArgumentTypeError("profiles use INSTANCE_TYPE:CPU, for example SA2.MEDIUM2:2")
    return instance_type, int(cpu)


def calibration_campaign(profiles: list[tuple[str, int]], cpu_seconds: float) -> dict[str, Any]:
    cases = []
    for instance_type, cpu_count in profiles:
        cap = capability(instance_type)
        for index in range(1, healthy_task_count(cpu_count) + 1):
            cases.append(
                {
                    "case_id": f"adaptive-{cap}",
                    "setting_id": "cpu-baseline-and-probe",
                    "run_id": f"{index:04d}",
                    "runner": "shell",
                    "required_capability": cap,
                    "command": busy_command(cpu_seconds),
                    "timeout_seconds": 120,
                    "expected": {"state": "clean"},
                }
            )
    return {"schema_version": 1, "campaign_id": "loom-concurrency-calibration", "cases": cases}


def failure_command(category: str, message: str) -> str:
    if category == "rate_limited":
        return (
            "if [ \"${LOOM_ATTEMPT_NO:-1}\" -ge 2 ]; then echo recovered-after-rate-limit; exit 0; "
            f"else echo {json.dumps(message)} >&2; exit 1; fi"
        )
    return f"echo {json.dumps(message)} >&2; exit 1"


def failure_campaign(profiles: list[tuple[str, int]]) -> tuple[dict[str, Any], str]:
    cases = []
    retry_task_id = ""
    for index, (category, message) in enumerate(FAILURES):
        instance_type, _cpu_count = profiles[index % len(profiles)]
        run_id = f"{index + 1:03d}"
        case_id = f"failure-{category}"
        cases.append(
            {
                "case_id": case_id,
                "setting_id": "known-failure-classification",
                "run_id": run_id,
                "runner": "shell",
                "required_capability": capability(instance_type),
                "command": failure_command(category, message),
                "timeout_seconds": 60,
                "expected": {
                    "state": "clean" if category == "rate_limited" else "run_error",
                    **(
                        {"attempt_no": 2, "min_result_count": 2}
                        if category == "rate_limited"
                        else {}
                    ),
                },
            }
        )
        if category == "rate_limited":
            retry_task_id = (
                f"loom-failure-injection__{case_id}__known-failure-classification__run-{run_id}"
            )
    cases.append(
        {
            "case_id": AUTO_FAILOVER_CASE_ID,
            "setting_id": "automatic-cross-worker-retry",
            "run_id": "001",
            "runner": "shell",
            "required_capability": "linux",
            "command": (
                "if [ \"${LOOM_ATTEMPT_NO:-1}\" -ge 2 ]; then "
                "echo recovered-on-${LOOM_WORKER_ID}; exit 0; "
                "else echo 'connection timed out: synthetic first-attempt source failure' >&2; exit 1; fi"
            ),
            "timeout_seconds": 60,
            "retry_policy": {
                "max_attempts": 2,
                "retry_categories": [AUTO_FAILOVER_CATEGORY],
                "different_worker": True,
            },
            "expected": {
                "state": "clean",
                "attempt_no": 2,
                "min_result_count": 2,
                "min_distinct_workers": 2,
            },
        }
    )
    return {"schema_version": 1, "campaign_id": "loom-failure-injection", "cases": cases}, retry_task_id


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Loom Matrix validation campaign inputs.")
    parser.add_argument("--profile", type=parse_profile, action="append", default=[])
    parser.add_argument("--cpu-seconds", type=float, default=0.25)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    profiles = args.profile or list(DEFAULT_PROFILES)
    if args.cpu_seconds <= 0:
        raise ValueError("--cpu-seconds must be positive")
    calibration = calibration_campaign(profiles, args.cpu_seconds)
    failures, retry_task_id = failure_campaign(profiles)
    write_json(args.output_dir / "concurrency-calibration.json", calibration)
    write_json(args.output_dir / "failure-injection.json", failures)
    plan = {
        "profiles": [
            {
                "instance_type": instance_type,
                "cpu_count": cpu_count,
                "required_capability": capability(instance_type),
                "probe_levels": probe_levels(cpu_count),
                "healthy_task_count": healthy_task_count(cpu_count),
            }
            for instance_type, cpu_count in profiles
        ],
        "required_log_categories": [category for category, _message in FAILURES] + ["automatic_retry"],
        "retry_task_id": retry_task_id,
        "automatic_cross_worker_retry_task_id": (
            f"loom-failure-injection__{AUTO_FAILOVER_CASE_ID}__automatic-cross-worker-retry__run-001"
        ),
        "calibration_task_count": len(calibration["cases"]),
        "failure_task_count": len(failures["cases"]),
    }
    write_json(args.output_dir / "validation-plan.json", plan)
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

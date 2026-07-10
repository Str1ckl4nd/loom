#!/usr/bin/env python3
"""Unsupported reference wrapper for Tencent provision/run/cleanup validation.

Automatic cloud resource lifecycle is outside the supported project scope.
This file remains only as historical validation and community reference code.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


REFERENCE_NOTICE = (
    "cloud resource lifecycle is outside the supported project scope; "
    "this Tencent E2E wrapper is retained as an unsupported reference, and "
    "maintained support requires a contributor-owned PR"
)


def run_logged(cmd: list[str], log_path: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True)
    except KeyboardInterrupt:
        log_path.write_text(
            json.dumps({"cmd": cmd, "interrupted": True}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        raise
    log_path.write_text(
        json.dumps({"cmd": cmd, "returncode": proc.returncode}, ensure_ascii=False) + "\n"
        + "\n--- stdout ---\n"
        + (proc.stdout or "")
        + "\n--- stderr ---\n"
        + (proc.stderr or ""),
        encoding="utf-8",
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}; see {log_path}")
    return proc


def append_repeated(cmd: list[str], flag: str, values: list[Any]) -> None:
    for value in values:
        cmd.extend([flag, str(value)])


def provision_cmd(args: argparse.Namespace, provision_script: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(provision_script),
        "create",
        "--region",
        args.region,
        "--zone",
        args.zone,
        "--image-id",
        args.image_id,
        "--controller-type",
        args.controller_type,
        "--controller-mode",
        args.controller_mode,
        "--instance-charge-type",
        args.instance_charge_type,
        "--system-disk-type",
        args.system_disk_type,
        "--system-disk-size",
        str(args.system_disk_size),
        "--bandwidth-mbps",
        str(args.bandwidth_mbps),
        "--ssh-user",
        args.ssh_user,
        "--worker-command-port",
        str(args.worker_command_port),
        "--controller-port",
        str(args.port),
        "--ssh-control-persist",
        args.ssh_control_persist,
        "--output-dir",
        str(args.output_dir),
    ]
    append_repeated(cmd, "--worker-type", args.worker_type)
    append_repeated(cmd, "--worker-max-concurrency", args.worker_max_concurrency)
    append_repeated(cmd, "--worker-connection-mode", args.worker_connection_mode)
    append_repeated(cmd, "--worker-system-disk-type", args.worker_system_disk_type)
    for flag, value in (
        ("--vpc-id", args.vpc_id),
        ("--subnet-id", args.subnet_id),
        ("--vpc-cidr", args.vpc_cidr),
        ("--subnet-cidr", args.subnet_cidr),
        ("--ssh-cidr", args.ssh_cidr),
        ("--controller-cidr", args.controller_cidr),
        ("--worker-cidr", args.worker_cidr),
        ("--direct-worker-cidr", args.direct_worker_cidr),
        ("--controller-public-url", args.controller_public_url),
        ("--controller-worker-url", args.controller_worker_url),
        ("--local-controller-bind-host", args.local_controller_bind_host),
        ("--name-prefix", args.name_prefix),
    ):
        if value:
            cmd.extend([flag, value])
    if args.disable_ssh_bootstrap:
        cmd.append("--disable-ssh-bootstrap")
    return cmd


def matrix_cmd(args: argparse.Namespace, matrix_script: Path, inventory: Path, summary: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(matrix_script),
        "--inventory",
        str(inventory),
        "--remote-dir",
        args.remote_dir,
        "--port",
        str(args.port),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--operator",
        args.operator,
        "--output",
        str(summary),
    ]
    append_repeated(cmd, "--dispatch-spec", args.dispatch_spec)
    append_repeated(cmd, "--forward-env", args.forward_env)
    append_repeated(cmd, "--require-log-category", args.require_log_category)
    append_repeated(cmd, "--retry-task-id", args.retry_task_id)
    if args.expected_workers is not None:
        cmd.extend(["--expected-workers", str(args.expected_workers)])
    if args.require_concurrency_stable:
        cmd.append("--require-concurrency-stable")
    if args.skip_download_results:
        cmd.append("--skip-download-results")
    if args.bootstrap_check_only:
        cmd.append("--bootstrap-check-only")
    return cmd


def cleanup_cmd(args: argparse.Namespace, provision_script: Path, resources: Path) -> list[str]:
    return [sys.executable, str(provision_script), "cleanup", "--region", args.region, "--resources", str(resources)]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unsupported reference wrapper for Tencent provision/run/cleanup validation.",
        epilog=REFERENCE_NOTICE,
    )
    parser.add_argument("--region", default="ap-guangzhou")
    parser.add_argument("--zone", required=True)
    parser.add_argument("--vpc-id", default=None)
    parser.add_argument("--subnet-id", default=None)
    parser.add_argument("--vpc-cidr", default=None)
    parser.add_argument("--subnet-cidr", default=None)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--controller-type", default="SA2.MEDIUM2")
    parser.add_argument("--controller-mode", choices=["ssh-start", "prestarted", "local-process"], default="ssh-start")
    parser.add_argument("--controller-public-url", default=None)
    parser.add_argument("--controller-worker-url", default=None)
    parser.add_argument("--local-controller-bind-host", default="127.0.0.1")
    parser.add_argument("--instance-charge-type", choices=["SPOTPAID", "POSTPAID_BY_HOUR"], default="SPOTPAID")
    parser.add_argument("--worker-type", action="append", default=[])
    parser.add_argument("--worker-max-concurrency", type=int, action="append", default=[])
    parser.add_argument("--worker-connection-mode", choices=["ssh-start", "long-poll", "direct-worker-api"], action="append", default=[])
    parser.add_argument("--worker-command-port", type=int, default=9876)
    parser.add_argument("--ssh-control-persist", default="10m")
    parser.add_argument("--system-disk-type", default="CLOUD_PREMIUM")
    parser.add_argument("--worker-system-disk-type", action="append", default=[])
    parser.add_argument("--system-disk-size", type=int, default=20)
    parser.add_argument("--bandwidth-mbps", type=int, default=1)
    parser.add_argument("--ssh-user", default="ubuntu")
    parser.add_argument("--disable-ssh-bootstrap", action="store_true")
    parser.add_argument("--ssh-cidr", default=None)
    parser.add_argument("--controller-cidr", default=None)
    parser.add_argument("--worker-cidr", default=None)
    parser.add_argument("--direct-worker-cidr", default=None)
    parser.add_argument("--name-prefix", default=None)
    parser.add_argument("--dispatch-spec", type=Path, action="append", required=True)
    parser.add_argument("--remote-dir", default="/tmp/agentbenchmark-control-worker")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--forward-env", action="append", default=[])
    parser.add_argument("--operator", default="tencent-matrix")
    parser.add_argument("--expected-workers", type=int, default=None)
    parser.add_argument("--require-concurrency-stable", action="store_true")
    parser.add_argument("--require-log-category", action="append", default=[])
    parser.add_argument("--retry-task-id", action="append", default=[])
    parser.add_argument("--skip-download-results", action="store_true")
    parser.add_argument("--bootstrap-check-only", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    print(f"NOTICE: {REFERENCE_NOTICE}", file=sys.stderr)
    args = parse_args(argv)
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    tool_root = Path(__file__).resolve().parent
    provision_script = tool_root / "tencent_cloud_provision.py"
    matrix_script = tool_root / "tencent_cloud_matrix.py"
    resources = args.output_dir / "resources.json"
    inventory = args.output_dir / "inventory.json"
    summary = args.output_dir / "tencent-matrix-summary.json"
    cleanup_result: dict[str, Any] = {"attempted": False}
    status: dict[str, Any] = {"ok": False, "summary": str(summary), "resources": str(resources)}
    interrupted = False
    try:
        run_logged(provision_cmd(args, provision_script), args.output_dir / "provision.log")
        run_logged(matrix_cmd(args, matrix_script, inventory, summary), args.output_dir / "matrix.log")
        status = {"ok": True, "summary": str(summary), "resources": str(resources)}
    except KeyboardInterrupt:
        interrupted = True
        status["error"] = {"type": "KeyboardInterrupt", "detail": "operator interrupted run; cleanup continued"}
    except Exception as exc:
        status["error"] = {"type": type(exc).__name__, "detail": str(exc)}
    finally:
        if resources.exists():
            cleanup_result["attempted"] = True
            proc = run_logged(cleanup_cmd(args, provision_script, resources), args.output_dir / "cleanup.log", check=False)
            cleanup_result["returncode"] = proc.returncode
            cleanup_result["log"] = str(args.output_dir / "cleanup.log")
        else:
            cleanup_result["reason"] = "resources.json was not created"
    status["cleanup"] = cleanup_result
    (args.output_dir / "e2e-status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(status, ensure_ascii=False, indent=2))
    if interrupted:
        return 130
    if not status.get("ok") or cleanup_result.get("returncode", 0) != 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

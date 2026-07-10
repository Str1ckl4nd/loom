#!/usr/bin/env python3
"""Loom Runner.

The Runner registers with Loom Hub, heartbeats, claims tasks, runs task
packages, uploads a ZIP result package, and reports completion or failure. It
uses only the standard library for portability across supplied hosts.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import glob
import hashlib
import json
import os
import platform
import resource
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from loom_contract import CONCURRENCY_POLICIES, CORE_PREVIEW_VERSION, RUNNER_API_VERSION, metadata
from loom_http import DEFAULT_HUB_TOKEN_ENV, DEFAULT_RUNNER_TOKEN_ENV, bearer_headers, is_loopback_host, token_from_env, token_matches
from loom_resources import normalize_capacity, normalize_capacity_overrides


DEFAULT_LOCAL_COPY_IGNORES = {
    ".git",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "loom-runs",
}
GNU_TIME = Path("/usr/bin/time")


def resource_snapshot(path: Path | None = None) -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    mem_total_mb = 0.0
    mem_available_mb = 0.0
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        values: dict[str, float] = {}
        for line in meminfo.read_text(encoding="utf-8", errors="ignore").splitlines():
            if ":" not in line:
                continue
            key, rest = line.split(":", 1)
            parts = rest.strip().split()
            if parts and parts[0].isdigit():
                values[key] = float(parts[0]) / 1024.0
        mem_total_mb = values.get("MemTotal", 0.0)
        mem_available_mb = values.get("MemAvailable", 0.0)
    disk_total_mb = 0.0
    disk_available_mb = 0.0
    try:
        disk = shutil.disk_usage(path or Path.cwd())
        disk_total_mb = disk.total / (1024.0 * 1024.0)
        disk_available_mb = disk.free / (1024.0 * 1024.0)
    except OSError:
        pass
    return {
        "cpu_count": cpu_count,
        "mem_total_mb": round(mem_total_mb, 2),
        "mem_available_mb": round(mem_available_mb, 2),
        "disk_total_mb": round(disk_total_mb, 2),
        "disk_available_mb": round(disk_available_mb, 2),
        "loadavg": os.getloadavg() if hasattr(os, "getloadavg") else None,
    }


def configured_resource_capacity(args: argparse.Namespace, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = getattr(args, "resource_capacity", None)
    return normalize_capacity(raw, snapshot=snapshot or resource_snapshot(getattr(args, "work_dir", None)))


def child_usage() -> resource.struct_rusage:
    return resource.getrusage(resource.RUSAGE_CHILDREN)


def maxrss_to_mb(value: float) -> float:
    if sys.platform == "darwin":
        return value / (1024.0 * 1024.0)
    return value / 1024.0


def utc_now() -> str:
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def request_json(
    base: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
    token: str | None = None,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    headers.update(bearer_headers(token))
    req = Request(base.rstrip("/") + path, data=data, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8-sig"))


def upload_file(
    base: str,
    task_id: str,
    worker_id: str,
    path: Path,
    timeout: int = 120,
    token: str | None = None,
) -> dict[str, Any]:
    data = path.read_bytes()
    headers = {
        "Content-Type": "application/zip",
        "X-Task-Id": task_id,
        "X-Worker-Id": worker_id,
        "Content-Length": str(len(data)),
    }
    headers.update(bearer_headers(token))
    req = Request(
        base.rstrip("/") + "/api/results/upload",
        data=data,
        headers=headers,
    )
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8-sig"))


def shell_command(payload: dict[str, Any]) -> list[str] | str:
    command = payload.get("command")
    if command:
        return command
    return "python3 -c \"print('loom worker noop')\""


def command_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    commands = payload.get("phases")
    is_phase_list = commands is not None
    if commands is None:
        commands = payload.get("commands")
    if commands is None:
        commands = [shell_command(payload)]
    if isinstance(commands, str):
        commands = [{"command": commands}]
    elif isinstance(commands, list) and (not commands or isinstance(commands[0], str)):
        commands = [{"command": cmd} for cmd in commands]
    out: list[dict[str, Any]] = []
    for idx, raw in enumerate(commands, start=1):
        if isinstance(raw, dict):
            spec = dict(raw)
        else:
            spec = {"command": raw}
        spec.setdefault("name", f"command-{idx:02d}")
        if is_phase_list:
            spec.setdefault("phase", spec["name"])
            spec.setdefault("phase_index", idx)
        out.append(spec)
    return out


def build_env(payload: dict[str, Any], command_spec: dict[str, Any] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    for key, value in (payload.get("env") or {}).items():
        env[str(key)] = str(value)
    for key, value in ((command_spec or {}).get("env") or {}).items():
        env[str(key)] = str(value)
    # Scheduler-owned metadata is immutable from a campaign or phase process.
    for key, value in (payload.get("_loom_runtime_env") or {}).items():
        env[str(key)] = str(value)
    if command_spec:
        phase_name = command_spec.get("phase") or command_spec.get("name")
        if phase_name:
            env["LOOM_PHASE_NAME"] = str(phase_name)
        if command_spec.get("phase_index") is not None:
            env["LOOM_PHASE_INDEX"] = str(command_spec["phase_index"])
    return env


def command_with_args(spec: dict[str, Any]) -> list[str] | str:
    command = spec.get("command")
    args = spec.get("args") or []
    if not args:
        return command
    if not isinstance(args, list):
        raise ValueError("command args must be a list")
    rendered_args = [str(item) for item in args]
    if isinstance(command, str):
        return command + " " + " ".join(shlex.quote(item) for item in rendered_args)
    if isinstance(command, list):
        return [*map(str, command), *rendered_args]
    raise ValueError("command must be a string or list when args are present")


def run_process(
    command: list[str] | str,
    *,
    cwd: Path,
    timeout: int,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    before_usage = child_usage()
    metric_path: Path | None = None
    measured_command: list[str] | str = command
    shell = isinstance(command, str)
    if sys.platform.startswith("linux") and GNU_TIME.exists():
        fd, metric_text = tempfile.mkstemp(prefix="loom-time-", suffix=".txt")
        os.close(fd)
        metric_path = Path(metric_text)
        measured_command = [str(GNU_TIME), "-v", "-o", str(metric_path)]
        if isinstance(command, str):
            measured_command.extend(["sh", "-c", command])
        else:
            measured_command.extend(["--", *command])
        shell = False
    try:
        proc = subprocess.run(
            measured_command,
            cwd=str(cwd),
            shell=shell,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
        )
        proc.args = command
        duration = time.monotonic() - started
        if metric_path is not None:
            usage = parse_gnu_time(metric_path, duration)
        else:
            after_usage = child_usage()
            usage = {
                "duration_seconds": round(duration, 4),
                "user_cpu_seconds": round(max(0.0, after_usage.ru_utime - before_usage.ru_utime), 4),
                "system_cpu_seconds": round(max(0.0, after_usage.ru_stime - before_usage.ru_stime), 4),
                "max_rss_mb": round(maxrss_to_mb(after_usage.ru_maxrss), 2),
                "measurement_source": "process_cumulative_fallback",
            }
            usage["cpu_seconds"] = round(usage["user_cpu_seconds"] + usage["system_cpu_seconds"], 4)
        setattr(proc, "resource_usage", usage)
        return proc
    finally:
        if metric_path is not None:
            metric_path.unlink(missing_ok=True)


def parse_gnu_time(path: Path, duration_seconds: float) -> dict[str, Any]:
    values: dict[str, float] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        for label, key in (
            ("User time (seconds):", "user_cpu_seconds"),
            ("System time (seconds):", "system_cpu_seconds"),
            ("Maximum resident set size (kbytes):", "max_rss_kb"),
        ):
            if stripped.startswith(label):
                try:
                    values[key] = float(stripped[len(label) :].strip())
                except ValueError:
                    values[key] = 0.0
    user_cpu = max(0.0, values.get("user_cpu_seconds", 0.0))
    system_cpu = max(0.0, values.get("system_cpu_seconds", 0.0))
    return {
        "duration_seconds": round(duration_seconds, 4),
        "user_cpu_seconds": round(user_cpu, 4),
        "system_cpu_seconds": round(system_cpu, 4),
        "cpu_seconds": round(user_cpu + system_cpu, 4),
        "max_rss_mb": round(max(0.0, values.get("max_rss_kb", 0.0)) / 1024.0, 2),
        "measurement_source": "gnu_time_v",
    }


def redact_secretish_text(value: Any) -> Any:
    if isinstance(value, list):
        return [redact_secretish_text(item) for item in value]
    if isinstance(value, tuple):
        return [redact_secretish_text(item) for item in value]
    if not isinstance(value, str):
        return value
    return re.sub(r"(https?://)([^/\s:@]+):([^@\s/]+)@", r"\1***:***@", value)


def git_auth_environment(source: dict[str, Any]) -> tuple[dict[str, str] | None, Path | None]:
    token_env = str(source.get("token_env") or "").strip()
    if not token_env:
        return None, None
    token = os.environ.get(token_env)
    if not token:
        return None, None
    username = str(source.get("username") or "x-access-token")
    fd, path_text = tempfile.mkstemp(prefix="loom-git-askpass-")
    askpass_path = Path(path_text)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("#!/bin/sh\n")
        f.write("case \"$1\" in\n")
        f.write(f"  *Username*) printf '%s\\n' {shlex.quote(username)} ;;\n")
        f.write(f"  *) printf '%s\\n' {shlex.quote(token)} ;;\n")
        f.write("esac\n")
    askpass_path.chmod(0o700)
    env = os.environ.copy()
    env["GIT_ASKPASS"] = str(askpass_path)
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env, askpass_path


def write_command_logs(task_dir: Path, name: str, proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    safe_name = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in name)[:80] or "command"
    stdout_path = f"{safe_name}.stdout.txt"
    stderr_path = f"{safe_name}.stderr.txt"
    (task_dir / stdout_path).write_text(proc.stdout or "", encoding="utf-8")
    (task_dir / stderr_path).write_text(proc.stderr or "", encoding="utf-8")
    result = {
        "name": name,
        "command": redact_secretish_text(proc.args),
        "exit_code": int(proc.returncode),
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
    }
    usage = getattr(proc, "resource_usage", None)
    if isinstance(usage, dict):
        result["resource_usage"] = usage
    return result


def aggregate_resource_usage(records: list[dict[str, Any]], duration_seconds: float) -> dict[str, Any]:
    usages = [record.get("resource_usage") for record in records if isinstance(record.get("resource_usage"), dict)]
    if not usages:
        return {
            "duration_seconds": round(duration_seconds, 4),
            "user_cpu_seconds": 0.0,
            "system_cpu_seconds": 0.0,
            "cpu_seconds": 0.0,
            "max_rss_mb": 0.0,
            "measurement_source": "unavailable",
        }
    user_cpu = sum(float(item.get("user_cpu_seconds") or 0.0) for item in usages)
    system_cpu = sum(float(item.get("system_cpu_seconds") or 0.0) for item in usages)
    measured_duration = sum(float(item.get("duration_seconds") or 0.0) for item in usages)
    return {
        "duration_seconds": round(measured_duration or duration_seconds, 4),
        "task_duration_seconds": round(duration_seconds, 4),
        "user_cpu_seconds": round(user_cpu, 4),
        "system_cpu_seconds": round(system_cpu, 4),
        "cpu_seconds": round(user_cpu + system_cpu, 4),
        "max_rss_mb": round(max(float(item.get("max_rss_mb") or 0.0) for item in usages), 2),
        "measurement_source": "+".join(sorted({str(item.get("measurement_source") or "unknown") for item in usages})),
        "measured_processes": len(usages),
    }


def has_glob_magic(value: str) -> bool:
    return any(ch in value for ch in "*?[")


def ensure_relative_pattern(pattern: str) -> str:
    path = Path(pattern)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"artifact path must be relative and stay inside workspace: {pattern}")
    return pattern


def copy_file_preserving_relative(src: Path, base: Path, dest_root: Path) -> dict[str, Any]:
    rel = src.relative_to(base)
    dest = dest_root / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    digest = hashlib.sha256()
    with src.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": rel.as_posix(), "bytes": src.stat().st_size, "sha256": digest.hexdigest()}


def collect_artifacts(workspace_dir: Path, task_dir: Path, patterns: list[str]) -> list[dict[str, Any]]:
    artifact_root = task_dir / "artifacts"
    collected: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for raw_pattern in patterns:
        pattern = ensure_relative_pattern(str(raw_pattern))
        matches: list[Path] = []
        if has_glob_magic(pattern):
            matches = [Path(p) for p in glob.glob(str(workspace_dir / pattern), recursive=True)]
        else:
            candidate = workspace_dir / pattern
            if candidate.exists():
                matches = [candidate]
        for match in sorted(matches):
            if match.is_dir():
                files = [p for p in match.rglob("*") if p.is_file()]
            elif match.is_file():
                files = [match]
            else:
                files = []
            for file_path in files:
                resolved = file_path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                collected.append(copy_file_preserving_relative(file_path, workspace_dir, artifact_root))
    write_json(task_dir / "artifact-summary.json", {"artifacts": collected})
    write_json(task_dir / "artifact-manifest.json", {"schema_version": 1, "artifacts": collected})
    return collected


def transient_git_failure(proc: subprocess.CompletedProcess[str]) -> bool:
    text = f"{proc.stdout or ''}\n{proc.stderr or ''}".lower()
    return any(
        marker in text
        for marker in (
            "http2 framing",
            "connection reset",
            "connection timed out",
            "could not resolve host",
            "remote end hung up unexpectedly",
            "rpc failed",
            "early eof",
            "tls connection",
            "ssl connection",
            "network is unreachable",
        )
    )


def materialize_git_source(source: dict[str, Any], workspace_dir: Path, task_dir: Path, timeout: int) -> dict[str, Any]:
    url = str(source.get("url") or source.get("repo_url") or "")
    if not url:
        raise ValueError("repo runner source requires url")
    ref = source.get("ref")
    depth = source.get("depth")
    git_prefix = ["git", "-c", "http.version=HTTP/1.1"]
    clone_cmd = [*git_prefix, "clone"]
    if depth:
        clone_cmd.extend(["--depth", str(depth)])
    if ref and source.get("branch", True):
        clone_cmd.extend(["--branch", str(ref)])
    clone_cmd.extend([url, str(workspace_dir)])
    git_env, askpass_path = git_auth_environment(source)
    try:
        clone = run_process(clone_cmd, cwd=task_dir, timeout=timeout, env=git_env)
        steps = [write_command_logs(task_dir, "materialize-git-clone", clone)]
        attempt = 1
        while clone.returncode != 0 and transient_git_failure(clone) and attempt < 3:
            attempt += 1
            shutil.rmtree(workspace_dir, ignore_errors=True)
            time.sleep(attempt * 2)
            clone = run_process(clone_cmd, cwd=task_dir, timeout=timeout, env=git_env)
            steps.append(write_command_logs(task_dir, f"materialize-git-clone-retry-{attempt}", clone))
        if clone.returncode != 0 and transient_git_failure(clone):
            return {"ok": False, "steps": steps, "failure": "transient_git_network_error"}
        if clone.returncode != 0 and ref:
            shutil.rmtree(workspace_dir, ignore_errors=True)
            fallback_cmd = [*git_prefix, "clone", url, str(workspace_dir)]
            clone = run_process(fallback_cmd, cwd=task_dir, timeout=timeout, env=git_env)
            steps.append(write_command_logs(task_dir, "materialize-git-clone-fallback", clone))
        if clone.returncode != 0:
            return {"ok": False, "steps": steps}
        if ref:
            checkout = run_process(["git", "checkout", str(ref)], cwd=workspace_dir, timeout=timeout, env=git_env)
            steps.append(write_command_logs(task_dir, "materialize-git-checkout", checkout))
            if checkout.returncode != 0:
                return {"ok": False, "steps": steps}
        rev = run_process(["git", "rev-parse", "HEAD"], cwd=workspace_dir, timeout=timeout)
        steps.append(write_command_logs(task_dir, "materialize-git-rev-parse", rev))
    finally:
        if askpass_path is not None:
            askpass_path.unlink(missing_ok=True)
    return {
        "ok": rev.returncode == 0,
        "type": "git",
        "url": url,
        "ref": ref,
        "commit": (rev.stdout or "").strip() if rev.returncode == 0 else None,
        "token_env": str(source.get("token_env") or "") or None,
        "steps": steps,
    }


def materialize_local_source(source: dict[str, Any], workspace_dir: Path) -> dict[str, Any]:
    src = Path(str(source.get("path") or source.get("local_path") or "")).expanduser()
    if not src.exists():
        raise ValueError(f"local source does not exist: {src}")
    ignore_names = set(DEFAULT_LOCAL_COPY_IGNORES)
    ignore_names.update(str(item) for item in source.get("ignore") or [])

    def ignore(_dir: str, names: list[str]) -> set[str]:
        return {name for name in names if name in ignore_names}

    shutil.copytree(src, workspace_dir, ignore=ignore)
    commit = None
    if (workspace_dir / ".git").exists():
        rev = run_process(["git", "rev-parse", "HEAD"], cwd=workspace_dir, timeout=30)
        if rev.returncode == 0:
            commit = (rev.stdout or "").strip()
    return {"ok": True, "type": "local", "path": str(src), "commit": commit, "ignored": sorted(ignore_names)}


def materialize_workspace(payload: dict[str, Any], task_dir: Path) -> tuple[Path, dict[str, Any]]:
    source = dict(payload.get("source") or {})
    if not source and (payload.get("repo_url") or payload.get("ref")):
        source = {"type": "git", "url": payload.get("repo_url"), "ref": payload.get("ref")}
    source_type = str(source.get("type") or ("git" if source.get("url") or source.get("repo_url") else "local"))
    workspace_dir = task_dir / "workspace"
    if workspace_dir.exists():
        shutil.rmtree(workspace_dir)
    timeout = int(payload.get("materialize_timeout_seconds") or payload.get("timeout_seconds") or 600)
    if source_type in {"git", "repo"}:
        materialized = materialize_git_source(source, workspace_dir, task_dir, timeout)
    elif source_type == "local":
        materialized = materialize_local_source(source, workspace_dir)
    else:
        raise ValueError(f"unsupported source type: {source_type}")
    write_json(task_dir / "source-summary.json", materialized)
    return workspace_dir, materialized


def run_repo_task(payload: dict[str, Any], task_dir: Path) -> dict[str, Any]:
    workspace_dir, materialized = materialize_workspace(payload, task_dir)
    command_results: list[dict[str, Any]] = []
    exit_code = 0 if materialized.get("ok") else 2
    commands = command_list(payload)
    if exit_code == 0:
        for idx, spec in enumerate(commands, start=1):
            command = command_with_args(spec)
            if not command:
                continue
            timeout = int(spec.get("timeout_seconds") or payload.get("timeout_seconds") or 300)
            rel_cwd = str(spec.get("cwd") or ".")
            if Path(rel_cwd).is_absolute() or ".." in Path(rel_cwd).parts:
                raise ValueError(f"command cwd must be relative and stay inside workspace: {rel_cwd}")
            cwd = workspace_dir / rel_cwd
            proc = run_process(command, cwd=cwd, timeout=timeout, env=build_env(payload, spec))
            command_result = write_command_logs(task_dir, f"{idx:02d}-{spec.get('name')}", proc)
            command_result["cwd"] = rel_cwd
            command_result["phase"] = str(spec.get("phase") or spec.get("name") or f"command-{idx:02d}")
            command_result["phase_index"] = int(spec.get("phase_index") or idx)
            command_result["timeout_seconds"] = timeout
            command_results.append(command_result)
            if proc.returncode != 0:
                exit_code = int(proc.returncode)
                continue_on_error = (
                    bool(spec["continue_on_error"])
                    if "continue_on_error" in spec
                    else bool(payload.get("continue_on_error"))
                )
                if not continue_on_error:
                    break
    phase_report = {
        "schema_version": 1,
        "task_id": str((payload.get("_loom_runtime_env") or {}).get("LOOM_TASK_ID") or ""),
        "attempt_no": int((payload.get("_loom_runtime_env") or {}).get("LOOM_ATTEMPT_NO") or 1),
        "phases": command_results,
    }
    write_json(task_dir / "phase-results.json", phase_report)
    artifact_patterns = [str(p) for p in payload.get("artifact_paths") or payload.get("artifacts") or []]
    for spec in commands:
        artifact_patterns.extend(str(pattern) for pattern in spec.get("artifact_paths") or [])
    artifacts = collect_artifacts(workspace_dir, task_dir, artifact_patterns) if workspace_dir.exists() else []
    return {
        "materialized": materialized,
        "commands": command_results,
        "phases": command_results,
        "artifacts": artifacts,
        "exit_code": exit_code,
        "verdict": "clean" if exit_code == 0 else "run_error",
    }


def zip_task_dir(task_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(task_dir):
            root_path = Path(root)
            if root_path == task_dir:
                dirs[:] = [name for name in dirs if name != "workspace"]
            for name in files:
                item = root_path / name
                rel = item.relative_to(task_dir)
                z.write(item, rel.as_posix())


def run_task(task: dict[str, Any], work_root: Path) -> tuple[Path, dict[str, Any]]:
    task_id = task["task_id"]
    payload = dict(task.get("payload") or {})
    normalized = payload.get("normalized") if isinstance(payload.get("normalized"), dict) else {}
    payload["_loom_runtime_env"] = {
        "LOOM_TASK_ID": task_id,
        "LOOM_ATTEMPT_NO": str(max(1, int(task.get("attempt_no") or 1))),
        "LOOM_WORKER_ID": str(task.get("assigned_worker_id") or ""),
        "LOOM_CAMPAIGN_ID": str(normalized.get("campaign_id") or ""),
        "LOOM_CASE_ID": str(task.get("case_id") or normalized.get("case_id") or ""),
        "LOOM_RUN_ID": str(task.get("run_id") or normalized.get("run_id") or ""),
        "LOOM_SETTING_ID": str(task.get("setting_id") or normalized.get("setting_id") or ""),
    }
    attempt_no = max(1, int(task.get("attempt_no") or 1))
    task_root = work_root / task_id
    task_dir = task_root / f"attempt-{attempt_no:03d}"
    # A retry must never inherit prior logs, artifacts, or a workspace checkout.
    shutil.rmtree(task_dir, ignore_errors=True)
    task_dir.mkdir(parents=True, exist_ok=True)
    write_json(task_dir / "task.json", task)

    started = utc_now()
    started_monotonic = time.monotonic()
    runner = payload.get("runner", "noop")
    timeout = int(payload.get("timeout_seconds") or 300)
    stdout = ""
    stderr = ""
    exit_code = 0
    measurement_records: list[dict[str, Any]] = []
    try:
        if runner == "shell":
            command = shell_command(payload)
            proc = run_process(command, cwd=task_dir, timeout=timeout, env=build_env(payload))
            stdout = proc.stdout
            stderr = proc.stderr
            exit_code = int(proc.returncode)
            usage = getattr(proc, "resource_usage", None)
            if isinstance(usage, dict):
                measurement_records.append({"resource_usage": usage})
        elif runner == "repo":
            repo_result = run_repo_task(payload, task_dir)
            stdout = json.dumps(repo_result, ensure_ascii=False, indent=2) + "\n"
            stderr = ""
            exit_code = int(repo_result["exit_code"])
            measurement_records.extend(repo_result.get("commands") or [])
            measurement_records.extend((repo_result.get("materialized") or {}).get("steps") or [])
        elif runner == "noop":
            stdout = "noop ok\n"
        else:
            exit_code = 2
            stderr = f"unsupported runner: {runner}\n"
    except subprocess.TimeoutExpired as exc:
        exit_code = 124
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = (exc.stderr if isinstance(exc.stderr, str) else "") + f"\ntimeout after {exc.timeout} seconds\n"
    except Exception as exc:
        exit_code = 2
        stderr = f"{type(exc).__name__}: {exc}\n"
    duration_seconds = time.monotonic() - started_monotonic
    measured_usage = aggregate_resource_usage(measurement_records, duration_seconds)

    (task_dir / "stdout.txt").write_text(stdout, encoding="utf-8")
    (task_dir / "stderr.txt").write_text(stderr, encoding="utf-8")
    result = {
        "task_id": task_id,
        "worker_observed_at": utc_now(),
        "started_at": started,
        "completed_at": utc_now(),
        "runner": runner,
        "exit_code": exit_code,
        "verdict": "clean" if exit_code == 0 else "run_error",
        "stdout_path": "stdout.txt",
        "stderr_path": "stderr.txt",
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
        "controller_concurrency": task.get("controller_concurrency") or {},
        "resource_capacity": resource_snapshot(),
        "resource_usage": measured_usage,
        "resource_admission": task.get("resource_admission") or {},
    }
    if runner == "repo":
        result["repo_result_path"] = "stdout.txt"
        result["phase_result_path"] = "phase-results.json"
    if normalized:
        result["normalized"] = normalized
    write_json(task_dir / "worker-result.json", result)
    zip_path = task_root / f"attempt-{attempt_no:03d}.zip"
    zip_task_dir(task_dir, zip_path)
    return zip_path, result


def controller_token(args: argparse.Namespace) -> str | None:
    return getattr(args, "controller_token", None)


class LeaseRenewer:
    """Keep a controller-owned lease live while a task or result upload runs."""

    def __init__(self, args: argparse.Namespace, task_id: str):
        self.args = args
        self.task_id = task_id
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.renewal_count = 0
        self.last_error: str | None = None
        self.lock = threading.Lock()

    def _interval_seconds(self) -> int:
        lease_seconds = max(30, int(self.args.lease_seconds))
        return max(10, min(120, lease_seconds // 3))

    def _run(self) -> None:
        while not self.stop_event.wait(self._interval_seconds()):
            try:
                request_json(
                    self.args.controller,
                    "/api/tasks/renew",
                    {
                        "worker_id": self.args.worker_id,
                        "task_id": self.task_id,
                        "lease_seconds": self.args.lease_seconds,
                    },
                    timeout=max(30, min(int(self.args.lease_seconds), 120)),
                    token=controller_token(self.args),
                )
                with self.lock:
                    self.renewal_count += 1
                    self.last_error = None
            except Exception as exc:
                # The task still owns its current lease. Keep trying until the
                # worker can report a definitive completion or failure.
                with self.lock:
                    self.last_error = f"{type(exc).__name__}: {exc}"

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
        with self.lock:
            return {"renewal_count": self.renewal_count, "last_error": self.last_error}


def heartbeat(args: argparse.Namespace, current_runs: list[dict[str, Any]], phase: str) -> dict[str, Any]:
    return request_json(
        args.controller,
        "/api/workers/heartbeat",
        {
            "worker_id": args.worker_id,
            "current_runs": current_runs,
            "health": {
                "phase": phase,
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "resources": resource_snapshot(),
                "time": utc_now(),
            },
        },
        token=controller_token(args),
    )


def register(args: argparse.Namespace) -> dict[str, Any]:
    snapshot = resource_snapshot(getattr(args, "work_dir", None))
    return request_json(
        args.controller,
        "/api/workers/register",
        {
            "worker_id": args.worker_id,
            "capabilities": args.capability,
            "max_concurrency": args.max_concurrency,
            "initial_concurrency": args.initial_concurrency,
            "concurrency_policy": args.concurrency_policy,
            "resource_capacity": configured_resource_capacity(args, snapshot),
            "runner_api_version": RUNNER_API_VERSION,
            "health": {"hostname": socket.gethostname(), "platform": platform.platform(), "resources": snapshot},
        },
        token=controller_token(args),
    )


def handle_one(args: argparse.Namespace, work_root: Path) -> bool:
    heartbeat(args, [], "claiming")
    claim = request_json(
        args.controller,
        "/api/tasks/claim",
        {"worker_id": args.worker_id, "limit": 1, "lease_seconds": args.lease_seconds},
        token=controller_token(args),
    )
    tasks = claim.get("tasks") or []
    if not tasks:
        heartbeat(args, [], "idle")
        return False
    task = tasks[0]
    task_id = task["task_id"]
    current = [{"task_id": task_id, "package_id": task.get("package_id"), "phase": "starting"}]
    heartbeat(args, current, "starting")
    request_json(args.controller, "/api/tasks/start", {"worker_id": args.worker_id, "task_id": task_id}, token=controller_token(args))
    lease_renewer = LeaseRenewer(args, task_id)
    lease_renewer.start()
    try:
        current[0]["phase"] = "running"
        heartbeat(args, current, "running")
        zip_path, result = run_task(task, work_root)
        current[0]["phase"] = "uploading"
        heartbeat(args, current, "uploading")
        upload = upload_file(args.controller, task_id, args.worker_id, zip_path, token=controller_token(args))
        if not (upload.get("auto_retry") or {}).get("queued"):
            request_json(
                args.controller,
                "/api/tasks/complete",
                {"worker_id": args.worker_id, "task_id": task_id, "result": result, "upload": upload},
                token=controller_token(args),
            )
        heartbeat(args, [], "completed")
        return True
    except Exception as exc:
        request_json(
            args.controller,
            "/api/tasks/fail",
            {
                "worker_id": args.worker_id,
                "task_id": task_id,
                "controller_concurrency": task.get("controller_concurrency") or {},
                "error": {"type": type(exc).__name__, "detail": str(exc)},
            },
            token=controller_token(args),
        )
        heartbeat(args, [], "failed")
        if args.fail_fast:
            raise
        return True
    finally:
        lease_renewer.stop()


def claim_tasks(
    args: argparse.Namespace,
    active_count: int,
    desired_concurrency: int,
    max_to_claim: int | None = None,
) -> list[dict[str, Any]]:
    capacity = max(0, min(args.max_concurrency, desired_concurrency) - active_count)
    if max_to_claim is not None:
        capacity = min(capacity, max(0, max_to_claim))
    if capacity <= 0:
        return []
    payload: dict[str, Any] = {"worker_id": args.worker_id, "limit": capacity, "lease_seconds": args.lease_seconds}
    if args.claim_wait_seconds:
        payload["wait_seconds"] = args.claim_wait_seconds
        payload["wait_poll_seconds"] = min(max(args.poll_seconds, 1), 5)
    claim = request_json(
        args.controller,
        "/api/tasks/claim",
        payload,
        timeout=max(30, int(args.claim_wait_seconds) + 10),
        token=controller_token(args),
    )
    return claim.get("tasks") or []


def execute_claimed_task(args: argparse.Namespace, work_root: Path, task: dict[str, Any]) -> dict[str, Any]:
    task_id = task["task_id"]
    request_json(args.controller, "/api/tasks/start", {"worker_id": args.worker_id, "task_id": task_id}, token=controller_token(args))
    lease_renewer = LeaseRenewer(args, task_id)
    lease_renewer.start()
    try:
        zip_path, result = run_task(task, work_root)
        upload = upload_file(args.controller, task_id, args.worker_id, zip_path, token=controller_token(args))
        if not (upload.get("auto_retry") or {}).get("queued"):
            request_json(
                args.controller,
                "/api/tasks/complete",
                {"worker_id": args.worker_id, "task_id": task_id, "result": result, "upload": upload},
                token=controller_token(args),
            )
        return {"task_id": task_id, "ok": True, "result": result, "upload": upload}
    except Exception as exc:
        request_json(
            args.controller,
            "/api/tasks/fail",
            {
                "worker_id": args.worker_id,
                "task_id": task_id,
                "controller_concurrency": task.get("controller_concurrency") or {},
                "error": {"type": type(exc).__name__, "detail": str(exc)},
            },
            token=controller_token(args),
        )
        if args.fail_fast:
            raise
        return {"task_id": task_id, "ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    finally:
        lease_renewer.stop()


def run_concurrent_loop(args: argparse.Namespace, work_root: Path) -> int:
    completed = 0
    idle_rounds = 0
    desired_concurrency = max(1, args.initial_concurrency)
    futures: dict[Future[dict[str, Any]], dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=args.max_concurrency) as executor:
        while True:
            current_runs = [
                {
                    "task_id": meta["task"]["task_id"],
                    "package_id": meta["task"].get("package_id"),
                    "phase": "running",
                }
                for meta in futures.values()
            ]
            heartbeat_response = heartbeat(args, current_runs, "running" if futures else "idle")
            desired_concurrency = max(
                1,
                min(args.max_concurrency, int(heartbeat_response.get("desired_concurrency") or desired_concurrency)),
            )

            while futures:
                done = [future for future in futures if future.done()]
                if not done:
                    break
                for future in done:
                    meta = futures.pop(future)
                    try:
                        future.result()
                    except Exception as exc:
                        if args.fail_fast:
                            raise
                        sys.stderr.write(f"task {meta['task']['task_id']} failed in worker loop: {exc}\n")
                    completed += 1

            if args.max_tasks and completed >= args.max_tasks and not futures:
                break

            tasks = []
            if not futures and (not args.max_tasks or completed < args.max_tasks):
                remaining = None
                if args.max_tasks:
                    remaining = max(0, args.max_tasks - completed)
                tasks = claim_tasks(args, 0, desired_concurrency, max_to_claim=remaining)

            if tasks:
                idle_rounds = 0
                for task in tasks:
                    future = executor.submit(execute_claimed_task, args, work_root, task)
                    futures[future] = {"task": task, "started_at": utc_now()}
                continue

            if futures:
                done, _pending = wait(futures.keys(), timeout=args.poll_seconds, return_when=FIRST_COMPLETED)
                for future in done:
                    meta = futures.pop(future)
                    try:
                        future.result()
                    except Exception as exc:
                        if args.fail_fast:
                            raise
                        sys.stderr.write(f"task {meta['task']['task_id']} failed in worker loop: {exc}\n")
                    completed += 1
                continue

            idle_rounds += 1
            if args.once and idle_rounds >= 1:
                break
            time.sleep(args.poll_seconds)
    heartbeat(args, [], "completed")
    return completed


class DirectWorkerServer(ThreadingHTTPServer):
    worker_args: argparse.Namespace
    work_root: Path
    state_lock: threading.Lock
    run_thread: threading.Thread | None
    last_result: dict[str, Any]
    direct_api_token: str | None
    push_executor: ThreadPoolExecutor
    push_futures: dict[str, Future[dict[str, Any]]]


class DirectWorkerHandler(BaseHTTPRequestHandler):
    server: DirectWorkerServer

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8-sig"))

    def _require_auth(self) -> bool:
        if token_matches(self.headers, self.server.direct_api_token):
            return True
        self._json(401, {"error": "unauthorized"})
        return False

    def _active_pushes(self) -> list[str]:
        return [task_id for task_id, future in self.server.push_futures.items() if not future.done()]

    def do_GET(self) -> None:
        if not self._require_auth():
            return
        if self.path.rstrip("/") == "/api/healthz":
            thread = self.server.run_thread
            self._json(
                200,
                {
                    "ok": True,
                    "worker_id": self.server.worker_args.worker_id,
                    "mode": "direct-api",
                    "core_preview_version": CORE_PREVIEW_VERSION,
                    "runner_api_version": RUNNER_API_VERSION,
                    "capabilities": metadata("runner")["capabilities"],
                    "concurrency_policy": self.server.worker_args.concurrency_policy,
                    "initial_concurrency": self.server.worker_args.initial_concurrency,
                    "max_concurrency": self.server.worker_args.max_concurrency,
                    "running": bool(thread and thread.is_alive()),
                    "active_pushes": self._active_pushes(),
                    "last_result": self.server.last_result,
                    "resources": resource_snapshot(),
                    "resource_capacity": configured_resource_capacity(self.server.worker_args),
                },
            )
            return
        if self.path.rstrip("/") == "/api/meta":
            self._json(
                200,
                {
                    **metadata("runner"),
                    "worker_id": self.server.worker_args.worker_id,
                    "auth_required": bool(self.server.direct_api_token),
                    "connection_mode": "direct-api",
                    "concurrency_policy": self.server.worker_args.concurrency_policy,
                    "initial_concurrency": self.server.worker_args.initial_concurrency,
                    "max_concurrency": self.server.worker_args.max_concurrency,
                    "resource_capacity": configured_resource_capacity(self.server.worker_args),
                },
            )
            return
        self._json(404, {"error": "not_found", "path": self.path})

    def do_POST(self) -> None:
        if not self._require_auth():
            return
        path = self.path.rstrip("/") or "/"
        try:
            if path == "/api/register":
                result = register(self.server.worker_args)
                self._json(200, {"ok": True, "register": result})
                return
            if path == "/api/run-loop":
                self._start_run_loop(self._read_json())
                return
            if path == "/api/tasks/execute":
                self._execute_task(self._read_json())
                return
            self._json(404, {"error": "not_found", "path": self.path})
        except Exception as exc:
            self._json(500, {"error": type(exc).__name__, "detail": str(exc)})

    def _start_run_loop(self, payload: dict[str, Any]) -> None:
        with self.server.state_lock:
            if self._active_pushes():
                self._json(409, {"error": "push_tasks_active", "active_pushes": self._active_pushes()})
                return
            thread = self.server.run_thread
            if thread and thread.is_alive():
                self._json(202, {"ok": True, "state": "already_running", "worker_id": self.server.worker_args.worker_id})
                return
            run_args = argparse.Namespace(**vars(self.server.worker_args))
            if payload.get("max_tasks") is not None:
                run_args.max_tasks = max(0, int(payload.get("max_tasks") or 0))
            if payload.get("once") is not None:
                run_args.once = bool(payload.get("once"))

            def target() -> None:
                try:
                    completed = run_concurrent_loop(run_args, self.server.work_root)
                    result = {"ok": True, "completed": completed, "finished_at": utc_now()}
                except Exception as exc:
                    result = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}, "finished_at": utc_now()}
                with self.server.state_lock:
                    self.server.last_result = result

            thread = threading.Thread(target=target, daemon=True)
            self.server.run_thread = thread
            self.server.last_result = {"ok": True, "state": "starting", "started_at": utc_now()}
            thread.start()
        self._json(202, {"ok": True, "state": "started", "worker_id": self.server.worker_args.worker_id})

    def _execute_task(self, payload: dict[str, Any]) -> None:
        task = payload.get("task")
        if not isinstance(task, dict):
            self._json(400, {"error": "task_object_required"})
            return
        task_id = str(task.get("task_id") or "")
        worker_id = self.server.worker_args.worker_id
        if not task_id:
            self._json(400, {"error": "task_id_required"})
            return
        if str(task.get("assigned_worker_id") or "") != worker_id:
            self._json(409, {"error": "assigned_worker_mismatch", "worker_id": worker_id, "task_id": task_id})
            return
        with self.server.state_lock:
            loop = self.server.run_thread
            if loop and loop.is_alive():
                self._json(409, {"error": "pull_loop_active", "task_id": task_id})
                return
            existing = self.server.push_futures.get(task_id)
            if existing and not existing.done():
                self._json(202, {"ok": True, "state": "already_running", "task_id": task_id, "worker_id": worker_id})
                return
            active = self._active_pushes()
            if len(active) >= self.server.worker_args.max_concurrency:
                self._json(
                    429,
                    {
                        "error": "worker_capacity_reached",
                        "worker_id": worker_id,
                        "active_pushes": active,
                        "max_concurrency": self.server.worker_args.max_concurrency,
                    },
                )
                return
            future = self.server.push_executor.submit(execute_claimed_task, self.server.worker_args, self.server.work_root, task)
            self.server.push_futures[task_id] = future
            self.server.last_result = {"ok": True, "state": "accepted", "task_id": task_id, "accepted_at": utc_now()}

            def complete(done: Future[dict[str, Any]]) -> None:
                try:
                    result = done.result()
                except Exception as exc:
                    result = {"task_id": task_id, "ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
                with self.server.state_lock:
                    self.server.last_result = {**result, "finished_at": utc_now()}

            future.add_done_callback(complete)
        self._json(202, {"ok": True, "state": "accepted", "task_id": task_id, "worker_id": worker_id})

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))


def serve_direct_api(args: argparse.Namespace, work_root: Path) -> int:
    if not is_loopback_host(args.serve_host) and not args.direct_api_token:
        raise ValueError("direct Worker API requires --direct-api-token-env when --serve-host is not loopback")
    register(args)
    server = DirectWorkerServer((args.serve_host, args.serve_port), DirectWorkerHandler)
    server.worker_args = args
    server.work_root = work_root
    server.state_lock = threading.Lock()
    server.run_thread = None
    server.last_result = {}
    server.direct_api_token = args.direct_api_token
    server.push_executor = ThreadPoolExecutor(max_workers=args.max_concurrency)
    server.push_futures = {}
    print(
        json.dumps(
            {
                "event": "direct_worker_started",
                "worker_id": args.worker_id,
                "url": f"http://{args.serve_host}:{args.serve_port}",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if args.direct_api_run_on_start:
        with server.state_lock:
            run_args = argparse.Namespace(**vars(args))

            def target() -> None:
                try:
                    completed = run_concurrent_loop(run_args, work_root)
                    result = {"ok": True, "completed": completed, "finished_at": utc_now()}
                except Exception as exc:
                    result = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}, "finished_at": utc_now()}
                with server.state_lock:
                    server.last_result = result

            server.last_result = {"ok": True, "state": "starting", "started_at": utc_now()}
            server.run_thread = threading.Thread(target=target, daemon=True)
            server.run_thread.start()
    server.serve_forever()
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Loom Runner.")
    parser.add_argument("--version", action="version", version=f"Loom Runner v{CORE_PREVIEW_VERSION} Core Preview (runner API v{RUNNER_API_VERSION})")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--controller-token-env", default=DEFAULT_HUB_TOKEN_ENV, help="Environment variable containing the Hub bearer token. Leave unset only for a loopback-only Hub.")
    parser.add_argument("--worker-id", default=f"worker-{socket.gethostname()}-{uuid.uuid4().hex[:8]}")
    parser.add_argument("--capability", action="append", default=[])
    parser.add_argument("--work-dir", type=Path, default=Path("loom-runs/runner"))
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--lease-seconds", type=int, default=600)
    parser.add_argument("--max-concurrency", type=int, default=1, help="Hard worker-side concurrency cap. The controller chooses the active desired concurrency.")
    parser.add_argument("--initial-concurrency", type=int, default=1, help="Initial desired concurrency reported to the controller.")
    parser.add_argument("--concurrency-policy", choices=sorted(CONCURRENCY_POLICIES), default="fixed", help="fixed keeps the configured level except explicit resource/rate-limit backoff; adaptive probes upward and backoff levels.")
    parser.add_argument("--resource-capacity-json", default=None, help="Optional scheduler capacity JSON. Omitted fields use the Runner's observed host capacity.")
    parser.add_argument("--max-tasks", type=int, default=0, help="0 means unlimited until --once exits on idle.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--connection-mode", choices=["poll", "long-poll", "direct-api"], default="poll")
    parser.add_argument("--claim-wait-seconds", type=int, default=0, help="Hold empty claim requests open for this many seconds.")
    parser.add_argument("--serve-host", default="127.0.0.1")
    parser.add_argument("--serve-port", type=int, default=9876)
    parser.add_argument("--direct-api-token-env", default=DEFAULT_RUNNER_TOKEN_ENV, help="Environment variable containing the Direct Runner API bearer token.")
    parser.add_argument("--direct-api-run-on-start", action="store_true")
    args = parser.parse_args(argv)
    args.controller_token = token_from_env(args.controller_token_env)
    args.direct_api_token = token_from_env(args.direct_api_token_env)
    if not args.capability:
        args.capability = ["linux" if os.name != "nt" else "windows", "*"]
    args.max_concurrency = int(args.max_concurrency)
    args.initial_concurrency = int(args.initial_concurrency)
    if args.resource_capacity_json:
        try:
            args.resource_capacity = json.loads(args.resource_capacity_json)
        except json.JSONDecodeError as exc:
            raise ValueError("resource_capacity_json must be a JSON object") from exc
        if not isinstance(args.resource_capacity, dict):
            raise ValueError("resource_capacity_json must be a JSON object")
    else:
        args.resource_capacity = {}
    if args.max_concurrency < 1:
        raise ValueError("max_concurrency must be positive")
    if args.initial_concurrency < 1 or args.initial_concurrency > args.max_concurrency:
        raise ValueError("initial_concurrency must satisfy 1 <= initial_concurrency <= max_concurrency")
    args.resource_capacity = normalize_capacity_overrides(args.resource_capacity)
    normalize_capacity(args.resource_capacity, snapshot=resource_snapshot())
    if args.connection_mode == "long-poll" and args.claim_wait_seconds <= 0:
        args.claim_wait_seconds = 25
    if args.connection_mode == "poll":
        args.claim_wait_seconds = max(0, args.claim_wait_seconds)
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    args.work_dir = args.work_dir.expanduser().resolve()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    if args.connection_mode == "direct-api":
        return serve_direct_api(args, args.work_dir)
    register(args)
    completed = run_concurrent_loop(args, args.work_dir)
    print(json.dumps({"worker_id": args.worker_id, "completed": completed}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Loom Matrix inventory-driven remote Hub/Runner validation.

Provide one Loom Hub host and any number of Loom Runner hosts with independent
CPU/memory shapes. Loom Matrix deploys the standard-library Hub and Runner
scripts over SSH, dispatches a normalized task spec, starts Runners with hard
concurrency caps, and gathers Hub summary and control logs.

This file intentionally does not create or destroy CVMs. That keeps validation
repeatable across pre-provisioned Tencent Cloud hosts and avoids coupling the
Loom Matrix to one cloud-account provisioning policy.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.client import HTTPConnection, HTTPResponse, HTTPSConnection
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from loom_contract import CONCURRENCY_POLICIES, CORE_PREVIEW_VERSION, INVENTORY_SCHEMA_VERSION
from loom_http import DEFAULT_HUB_TOKEN_ENV, DEFAULT_RUNNER_TOKEN_ENV, bearer_headers, token_from_env
from loom_resources import normalize_capacity_overrides


FINAL_STATES = {"clean", "dirty", "run_error", "needs_review", "accepted", "ignored", "blocked", "cancelled"}
SSH_STARTED_MODES = {"ssh-start", "long-poll", "direct-worker-api"}
HUB_TOKEN: str | None = None
HUB_TOKEN_ENV = DEFAULT_HUB_TOKEN_ENV
DEFAULT_DIRECT_RUNNER_TOKEN_ENV = DEFAULT_RUNNER_TOKEN_ENV


def run(cmd: list[str], *, timeout: int = 120, check: bool = True, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, input=input_text, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return proc


def connection_dict(host: dict[str, Any]) -> dict[str, Any]:
    return host.get("connection") if isinstance(host.get("connection"), dict) else {}


def connection_mode(host: dict[str, Any]) -> str:
    return str(host.get("connection_mode") or host.get("transport_mode") or connection_dict(host).get("mode") or "ssh-start")


def controller_mode(controller: dict[str, Any]) -> str:
    return str(controller.get("connection_mode") or controller.get("mode") or "ssh-start")


def endpoint_dict(host: dict[str, Any]) -> dict[str, Any]:
    return host.get("endpoint") if isinstance(host.get("endpoint"), dict) else {}


def ssh_base(host: dict[str, Any]) -> list[str]:
    cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=15",
    ]
    control_persist = host.get("ssh_control_persist") or connection_dict(host).get("ssh_control_persist")
    if control_persist:
        cmd.extend(["-o", "ControlMaster=auto", "-o", "ControlPath=~/.ssh/loom-%C", "-o", f"ControlPersist={control_persist}"])
    if host.get("key_path"):
        cmd.extend(["-i", str(host["key_path"])])
    if host.get("port"):
        cmd.extend(["-p", str(host["port"])])
    return cmd


def host_target(host: dict[str, Any]) -> str:
    user = host.get("user") or "ubuntu"
    return f"{user}@{host['host']}"


def ssh(host: dict[str, Any], command: str, *, timeout: int = 180, check: bool = True) -> str:
    return run([*ssh_base(host), host_target(host), command], timeout=timeout, check=check).stdout


def wait_ssh(host: dict[str, Any], timeout: int = 600) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            ssh(host, "true", timeout=25)
            return
        except Exception as exc:
            last = exc
            time.sleep(5)
    raise RuntimeError(f"SSH did not become ready for {host_target(host)}: {last}")


def scp(host: dict[str, Any], local: Path, remote: str, *, timeout: int = 120) -> None:
    cmd = ["scp", "-o", "StrictHostKeyChecking=accept-new"]
    control_persist = host.get("ssh_control_persist") or connection_dict(host).get("ssh_control_persist")
    if control_persist:
        cmd.extend(["-o", "ControlMaster=auto", "-o", "ControlPath=~/.ssh/loom-%C", "-o", f"ControlPersist={control_persist}"])
    if host.get("key_path"):
        cmd.extend(["-i", str(host["key_path"])])
    if host.get("port"):
        cmd.extend(["-P", str(host["port"])])
    cmd.extend([str(local), f"{host_target(host)}:{remote}"])
    run(cmd, timeout=timeout)


def open_direct_response(
    url: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 20,
    token: str | None = None,
) -> tuple[HTTPConnection | HTTPSConnection, HTTPResponse]:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"controller URL must use http or https: {url}")
    connection_type = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
    connection = connection_type(parsed.hostname, parsed.port, timeout=timeout)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    headers.update(bearer_headers(HUB_TOKEN if token is None else token))
    try:
        connection.request("POST" if body is not None else "GET", path, body=body, headers=headers)
        response = connection.getresponse()
    except Exception:
        connection.close()
        raise
    if not 200 <= response.status < 300:
        detail = response.read(12000).decode("utf-8", errors="replace")
        connection.close()
        raise RuntimeError(f"controller HTTP {response.status} {response.reason}: {detail}")
    return connection, response


def request_json(url: str, payload: dict[str, Any] | None = None, timeout: int = 20, token: str | None = None) -> dict[str, Any]:
    connection, response = open_direct_response(url, payload, timeout=timeout, token=token)
    try:
        return json.loads(response.read().decode("utf-8-sig"))
    finally:
        connection.close()


def wait_http(url: str, timeout: int = 90) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            request_json(url + "/api/healthz", timeout=5)
            return
        except Exception as exc:
            last = exc
            time.sleep(3)
    raise RuntimeError(f"controller did not become healthy: {last}")


def wait_worker_api(url: str, timeout: int = 90, token: str | None = None) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            request_json(url.rstrip("/") + "/api/healthz", timeout=5, token=token)
            return
        except Exception as exc:
            last = exc
            time.sleep(3)
    raise RuntimeError(f"direct worker API did not become healthy: {url}: {last}")


def wait_workers_registered(public_url: str, worker_ids: list[str], timeout: int = 180) -> dict[str, Any]:
    expected = set(worker_ids)
    deadline = time.time() + timeout
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = request_json(public_url.rstrip("/") + "/api/data/active-workers?max_age_seconds=300", timeout=15)
        seen = {str(worker.get("worker_id")) for worker in last.get("workers") or []}
        if expected.issubset(seen):
            return last
        time.sleep(3)
    missing = sorted(expected - {str(worker.get("worker_id")) for worker in last.get("workers") or []})
    raise RuntimeError(f"workers did not register before dispatch: {missing}")


def apply_inventory_defaults(inventory: dict[str, Any]) -> dict[str, Any]:
    defaults = inventory.get("connection_defaults") or {}
    if not defaults:
        return inventory
    for section in ("controller",):
        if isinstance(inventory.get(section), dict):
            for key, value in defaults.items():
                inventory[section].setdefault(key, value)
    workers = inventory.get("workers")
    if isinstance(workers, list):
        for worker in workers:
            if isinstance(worker, dict):
                for key, value in defaults.items():
                    worker.setdefault(key, value)
    return inventory


def load_inventory(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("inventory must be a JSON object")
    if "inventory_version" not in data:
        raise ValueError("inventory requires explicit inventory_version")
    try:
        inventory_version = int(data["inventory_version"])
    except (TypeError, ValueError) as exc:
        raise ValueError("inventory_version must be an integer") from exc
    if inventory_version != INVENTORY_SCHEMA_VERSION:
        raise ValueError(f"unsupported inventory_version: {inventory_version}")
    data = apply_inventory_defaults(data)
    if not isinstance(data.get("controller"), dict):
        raise ValueError("inventory requires a controller object")
    workers = data.get("workers")
    if not isinstance(workers, list):
        raise ValueError("inventory workers must be an array")
    if not workers:
        raise ValueError("inventory requires at least one worker host")
    if any(not isinstance(worker, dict) for worker in workers):
        raise ValueError("inventory workers must be objects")
    worker_ids = [str(worker.get("worker_id") or worker.get("host") or "") for worker in workers]
    if any(not worker_id for worker_id in worker_ids) or len(set(worker_ids)) != len(worker_ids):
        raise ValueError("inventory worker IDs must be present and unique")
    for index, worker in enumerate(workers, start=1):
        if "max_concurrency" not in worker or "initial_concurrency" not in worker:
            raise ValueError(f"inventory worker {worker_ids[index - 1]} requires max_concurrency and initial_concurrency")
        try:
            max_concurrency = int(worker["max_concurrency"])
            initial_concurrency = int(worker["initial_concurrency"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"inventory worker {worker_ids[index - 1]} concurrency values must be integers") from exc
        if max_concurrency < 1 or initial_concurrency < 1 or initial_concurrency > max_concurrency:
            raise ValueError(f"inventory worker {worker_ids[index - 1]} requires 1 <= initial_concurrency <= max_concurrency")
        policy = str(worker.get("concurrency_policy") or "").strip().lower()
        if policy not in CONCURRENCY_POLICIES:
            raise ValueError(f"inventory worker {worker_ids[index - 1]} requires concurrency_policy fixed or adaptive")
        worker["max_concurrency"] = max_concurrency
        worker["initial_concurrency"] = initial_concurrency
        worker["concurrency_policy"] = policy
        if "resource_capacity" in worker:
            worker["resource_capacity"] = normalize_capacity_overrides(worker["resource_capacity"])
    return data


def remote_setup(host: dict[str, Any], remote_dir: str, tool_root: Path) -> None:
    ssh(host, f"mkdir -p {shlex.quote(remote_dir)}")
    for name in (
        "loom_contract.py",
        "loom_http.py",
        "loom_resources.py",
        "loom_hub.py",
        "loom_runner.py",
        "loom_manifest.py",
    ):
        scp(host, tool_root / name, f"{remote_dir}/{name}")


def source_capability(hostname: str) -> str:
    suffix = re.sub(r"[^a-z0-9]+", "-", hostname.lower()).strip("-")
    return f"source-{suffix}"


def source_requirements(dispatch_specs: list[Path]) -> dict[str, dict[str, Any]]:
    requirements: dict[str, dict[str, Any]] = {}
    for dispatch_spec in dispatch_specs:
        payload = json.loads(dispatch_spec.read_text(encoding="utf-8-sig"))
        for task in payload.get("tasks") or []:
            task_payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
            source = task_payload.get("source") if isinstance(task_payload.get("source"), dict) else {}
            url = str(source.get("url") or source.get("repo_url") or "")
            parsed = urlsplit(url)
            if not parsed.hostname:
                continue
            capability = source_capability(parsed.hostname)
            default_port = 443 if parsed.scheme == "https" else 80 if parsed.scheme == "http" else 22
            requirements[capability] = {
                "capability": capability,
                "hostname": parsed.hostname,
                "port": parsed.port or default_port,
                "source_url": url,
            }
    return requirements


def task_expectations(dispatch_specs: list[Path]) -> dict[str, dict[str, Any]]:
    expectations: dict[str, dict[str, Any]] = {}
    for dispatch_spec in dispatch_specs:
        payload = json.loads(dispatch_spec.read_text(encoding="utf-8-sig"))
        for task in payload.get("tasks") or []:
            task_id = str(task.get("task_id") or "")
            raw = task.get("expected")
            if not task_id or raw is None:
                continue
            if isinstance(raw, str):
                expected = {"state": raw}
            elif isinstance(raw, dict):
                expected = dict(raw)
            else:
                raise ValueError(f"task {task_id} expected must be a state string or object")
            state = expected.get("state")
            if state is not None and str(state) not in FINAL_STATES:
                raise ValueError(f"task {task_id} has unsupported expected state: {state}")
            for key in ("attempt_no", "min_result_count", "min_distinct_workers"):
                if key in expected:
                    expected[key] = int(expected[key])
                    if expected[key] < 1:
                        raise ValueError(f"task {task_id} expected.{key} must be positive")
            if task_id in expectations and expectations[task_id] != expected:
                raise ValueError(f"conflicting expectations for task {task_id}")
            expectations[task_id] = expected
    return expectations


def probe_worker_sources(worker: dict[str, Any], requirements: dict[str, dict[str, Any]]) -> dict[str, Any]:
    capabilities = [str(value) for value in worker.get("capabilities") or []]
    observed = set(capabilities)
    probes: list[dict[str, Any]] = []
    for capability, requirement in requirements.items():
        if capability in observed:
            probes.append({**requirement, "ok": True, "mode": "predeclared", "attempts": 0})
            continue
        if connection_mode(worker) not in SSH_STARTED_MODES or worker.get("predeployed"):
            probes.append({**requirement, "ok": False, "mode": "not_ssh_probeable", "attempts": 0})
            continue
        code = (
            "import socket; "
            f"connection=socket.create_connection(({requirement['hostname']!r},{int(requirement['port'])}),8); "
            "connection.close()"
        )
        last_error = ""
        ok = False
        attempts = 0
        for attempts in range(1, 3):
            proc = run(
                [*ssh_base(worker), host_target(worker), f"python3 -c {shlex.quote(code)}"],
                timeout=20,
                check=False,
            )
            if proc.returncode == 0:
                ok = True
                break
            last_error = (proc.stderr or proc.stdout or "source connectivity probe failed")[-1000:]
            if attempts < 2:
                time.sleep(2)
        if ok:
            capabilities.append(capability)
            observed.add(capability)
        probes.append(
            {
                **requirement,
                "ok": ok,
                "mode": "ssh_tcp_probe",
                "attempts": attempts,
                "error": None if ok else last_error,
            }
        )
    worker["capabilities"] = capabilities
    return {
        "worker_id": worker.get("worker_id"),
        "connection_mode": connection_mode(worker),
        "capabilities": capabilities,
        "probes": probes,
    }


def probe_source_capabilities(workers: list[dict[str, Any]], dispatch_specs: list[Path]) -> dict[str, Any]:
    requirements = source_requirements(dispatch_specs)
    if not requirements:
        return {"requirements": [], "workers": [], "ok": True}
    with ThreadPoolExecutor(max_workers=min(8, len(workers))) as executor:
        reports = list(executor.map(lambda worker: probe_worker_sources(worker, requirements), workers))
    missing = [
        capability
        for capability in requirements
        if not any(capability in set(str(value) for value in worker.get("capabilities") or []) for worker in workers)
    ]
    report = {"requirements": list(requirements.values()), "workers": reports, "missing": missing, "ok": not missing}
    if missing:
        raise RuntimeError(f"no registered worker can reach required source capabilities: {missing}")
    return report


def start_hub(controller: dict[str, Any], remote_dir: str, port: int, forwarded_env: dict[str, str]) -> None:
    if not HUB_TOKEN:
        raise ValueError(f"remote Hub startup requires {HUB_TOKEN_ENV} in the operator environment")
    env_file = write_remote_env(controller, remote_dir, forwarded_env)
    source_env = f". {shlex.quote(env_file)} || exit 1; " if env_file else ""
    cmd = (
        f"cd {shlex.quote(remote_dir)} || exit 1; "
        f"{source_env}nohup python3 loom_hub.py server --host 0.0.0.0 --port {port} "
        f"--db hub.sqlite --artifact-root artifacts --control-log hub.jsonl "
        f"--auth-token-env {shlex.quote(HUB_TOKEN_ENV)} "
        f"--runner-token-env {shlex.quote(DEFAULT_DIRECT_RUNNER_TOKEN_ENV)} "
        f"< /dev/null > hub.stdout.log 2> hub.stderr.log &"
    )
    ssh(controller, cmd)


def start_local_hub(
    controller: dict[str, Any],
    tool_root: Path,
    output: Path,
    port: int,
) -> dict[str, Any]:
    runtime_dir = output.parent / "local-hub"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = runtime_dir / "hub.stdout.log"
    stderr_path = runtime_dir / "hub.stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    cmd = [
        sys.executable,
        str(tool_root / "loom_hub.py"),
        "server",
        "--host",
        str(controller.get("bind_host") or "127.0.0.1"),
        "--port",
        str(port),
        "--db",
        str(runtime_dir / "hub.sqlite"),
        "--artifact-root",
        str(runtime_dir / "artifacts"),
        "--control-log",
        str(runtime_dir / "hub.jsonl"),
        "--auth-token-env",
        HUB_TOKEN_ENV,
        "--runner-token-env",
        DEFAULT_DIRECT_RUNNER_TOKEN_ENV,
    ]
    proc = subprocess.Popen(cmd, text=True, stdout=stdout_handle, stderr=stderr_handle)
    return {
        "process": proc,
        "stdout_handle": stdout_handle,
        "stderr_handle": stderr_handle,
        "control_log": runtime_dir / "hub.jsonl",
    }


def stop_local_hub(runtime: dict[str, Any] | None) -> None:
    if not runtime:
        return
    proc = runtime["process"]
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    runtime["stdout_handle"].close()
    runtime["stderr_handle"].close()


def dispatch(controller_url: str, dispatch_spec: Path) -> dict[str, Any]:
    payload = json.loads(dispatch_spec.read_text(encoding="utf-8-sig"))
    last: Exception | None = None
    for attempt in range(1, 4):
        try:
            result = request_json(controller_url.rstrip("/") + "/api/tasks/dispatch", payload, timeout=180)
            result["delivery_attempt"] = attempt
            return result
        except Exception as exc:
            last = exc
            if attempt < 3:
                time.sleep(3)
    raise RuntimeError(f"dispatch delivery failed after 3 idempotent attempts: {last}")


def retry_task(controller_url: str, task_id: str, operator: str) -> dict[str, Any]:
    return request_json(
        controller_url.rstrip("/") + "/api/admin/retry-task",
        {"task_id": task_id, "operator": operator},
        timeout=60,
    )


def register_worker_hosts(controller_url: str, inventory: dict[str, Any], operator: str) -> dict[str, Any]:
    return request_json(
        controller_url.rstrip("/") + "/api/admin/register-worker-hosts",
        {"operator": operator, "inventory": inventory},
        timeout=60,
    )


def write_remote_env(worker: dict[str, Any], remote_dir: str, forwarded_env: dict[str, str]) -> str | None:
    if not forwarded_env:
        return None
    worker_id = worker.get("worker_id") or worker["host"].replace(".", "-")
    env_text = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in forwarded_env.items()) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp:
        tmp.write(env_text)
        tmp_path = Path(tmp.name)
    try:
        remote_path = f"{remote_dir}/worker-{worker_id}.env"
        scp(worker, tmp_path, remote_path)
        ssh(worker, f"chmod 600 {shlex.quote(remote_path)}")
        return remote_path
    finally:
        tmp_path.unlink(missing_ok=True)


def direct_worker_url(worker: dict[str, Any]) -> str:
    endpoint = endpoint_dict(worker)
    if endpoint.get("worker_url"):
        return str(endpoint["worker_url"]).rstrip("/")
    port = int(worker.get("serve_port") or endpoint.get("command_port") or 9876)
    host = endpoint.get("private_host") or endpoint.get("public_host") or worker.get("host")
    return f"http://{host}:{port}"


def direct_api_dispatch_mode(worker: dict[str, Any]) -> str:
    mode = str(worker.get("direct_api_dispatch_mode") or "pull").strip().lower()
    if mode not in {"pull", "push"}:
        raise ValueError(f"worker {worker.get('worker_id') or worker.get('host')} has unsupported direct_api_dispatch_mode: {mode}")
    return mode


def start_worker(worker: dict[str, Any], controller_private_url: str, remote_dir: str, forwarded_env: dict[str, str]) -> dict[str, Any]:
    worker_id = worker.get("worker_id") or worker["host"].replace(".", "-")
    mode = connection_mode(worker)
    if mode == "prestarted":
        return {"worker_id": worker_id, "connection_mode": mode, "started": False}
    max_concurrency = int(worker.get("max_concurrency") or worker.get("cpu") or 1)
    initial_concurrency = int(worker.get("initial_concurrency") or 1)
    concurrency_policy = str(worker.get("concurrency_policy") or "fixed")
    resource_capacity = json.dumps(worker.get("resource_capacity") or {}, ensure_ascii=False, separators=(",", ":"))
    capabilities = worker.get("capabilities") or ["linux"]
    cap_args = " ".join("--capability " + shlex.quote(str(cap)) for cap in capabilities)
    env_file = write_remote_env(worker, remote_dir, forwarded_env)
    source_env = f". {shlex.quote(env_file)} || exit 1; " if env_file else ""
    env_prefix = ""
    for key, value in (worker.get("env") or {}).items():
        env_prefix += f"{shlex.quote(str(key))}={shlex.quote(str(value))} "
    worker_runtime_mode = "long-poll" if mode == "long-poll" else "poll"
    direct_dispatch_mode = "pull"
    if mode == "direct-worker-api":
        worker_runtime_mode = "direct-api"
        direct_dispatch_mode = direct_api_dispatch_mode(worker)
    claim_wait = int(worker.get("claim_wait_seconds") or (25 if worker_runtime_mode == "long-poll" else 0))
    serve_port = int(worker.get("serve_port") or endpoint_dict(worker).get("command_port") or 9876)
    serve_host = str(worker.get("serve_host") or ("0.0.0.0" if worker_runtime_mode == "direct-api" else "127.0.0.1"))
    direct_token_env = str(worker.get("direct_api_token_env") or DEFAULT_DIRECT_RUNNER_TOKEN_ENV)
    run_on_start = bool(worker.get("direct_api_run_on_start", True))
    if mode == "direct-worker-api" and direct_dispatch_mode == "push":
        run_on_start = False
    direct_start_arg = " --direct-api-run-on-start" if worker_runtime_mode == "direct-api" and run_on_start else ""
    cmd = (
        f"cd {shlex.quote(remote_dir)} || exit 1; "
        f"{source_env}{env_prefix}nohup python3 loom_runner.py "
        f"--controller {shlex.quote(controller_private_url)} "
        f"--worker-id {shlex.quote(worker_id)} "
        f"{cap_args} "
        f"--work-dir worker-runs/{shlex.quote(worker_id)} "
        f"--max-concurrency {max_concurrency} "
        f"--initial-concurrency {initial_concurrency} "
        f"--concurrency-policy {shlex.quote(concurrency_policy)} "
        f"--resource-capacity-json {shlex.quote(resource_capacity)} "
        f"--poll-seconds {int(worker.get('poll_seconds') or 5)} "
        f"--connection-mode {shlex.quote(worker_runtime_mode)} "
        f"--claim-wait-seconds {claim_wait} "
        f"--controller-token-env {shlex.quote(HUB_TOKEN_ENV)} "
        f"--serve-host {shlex.quote(serve_host)} "
        f"--serve-port {serve_port} "
        f"--direct-api-token-env {shlex.quote(direct_token_env)} "
        f"{direct_start_arg} "
        f"< /dev/null > worker-{shlex.quote(worker_id)}.stdout.log 2> worker-{shlex.quote(worker_id)}.stderr.log &"
    )
    ssh(worker, cmd)
    info = {
        "worker_id": worker_id,
        "connection_mode": mode,
        "started": True,
        "initial_concurrency": initial_concurrency,
        "max_concurrency": max_concurrency,
        "concurrency_policy": concurrency_policy,
        "resource_capacity": worker.get("resource_capacity") or {},
    }
    if mode == "direct-worker-api":
        url = direct_worker_url(worker)
        direct_token = token_from_env(direct_token_env)
        if not direct_token:
            raise ValueError(f"direct worker {worker_id} requires token environment variable: {direct_token_env}")
        wait_worker_api(url, token=direct_token)
        info["worker_url"] = url
        info["direct_api_dispatch_mode"] = direct_dispatch_mode
        if direct_dispatch_mode == "pull" and not run_on_start:
            request_json(url + "/api/run-loop", {"max_tasks": worker.get("max_tasks") or 0}, timeout=30, token=direct_token)
    return info


def fetch_all_tasks(public_url: str) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    offset = 0
    total = 0
    while True:
        page = request_json(public_url.rstrip("/") + f"/api/tasks?limit=500&offset={offset}", timeout=30)
        rows = page.get("tasks") or []
        tasks.extend(rows)
        total = int(page.get("total") or len(tasks))
        next_offset = page.get("next_offset")
        if next_offset is None or not rows:
            break
        offset = int(next_offset)
    return {"tasks": tasks, "total": total}


def fetch_all_results(public_url: str) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    cursor = 0
    while True:
        page = request_json(public_url.rstrip("/") + f"/api/data/new-results?cursor={cursor}&limit=500", timeout=30)
        rows = page.get("results") or []
        results.extend(rows)
        next_cursor = int(page.get("next_cursor") or cursor)
        if not rows or next_cursor == cursor:
            break
        cursor = next_cursor
    return {"results": results, "next_cursor": cursor, "total": len(results)}


def download_result_packages(public_url: str, results: list[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for row in results:
        result_id = str(row.get("result_id") or "")
        task_id = str(row.get("task_id") or "unknown-task")
        if not result_id:
            errors.append({"task_id": task_id, "error": "missing result_id"})
            continue
        tmp_path: Path | None = None
        try:
            safe_task_id = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in task_id)[:180] or "unknown-task"
            task_dir = output_dir / safe_task_id
            task_dir.mkdir(parents=True, exist_ok=True)
            path = task_dir / f"{result_id}.zip"
            digest_state = hashlib.sha256()
            downloaded_bytes = 0
            with tempfile.NamedTemporaryFile(dir=task_dir, prefix=f".{result_id}.", suffix=".part", delete=False) as tmp:
                tmp_path = Path(tmp.name)
                connection, response = open_direct_response(
                    public_url.rstrip("/") + "/api/results/" + quote(result_id),
                    timeout=120,
                )
                try:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
                        digest_state.update(chunk)
                        downloaded_bytes += len(chunk)
                finally:
                    connection.close()
            digest = digest_state.hexdigest()
            expected_digest = str(row.get("sha256") or "")
            expected_bytes = int(row.get("bytes") or 0)
            if (expected_digest and digest != expected_digest) or (expected_bytes and downloaded_bytes != expected_bytes):
                raise ValueError(
                    f"result integrity mismatch: bytes={downloaded_bytes}/{expected_bytes} sha256={digest}/{expected_digest}"
                )
            os.replace(tmp_path, path)
            tmp_path = None
            downloaded.append(
                {
                    "task_id": task_id,
                    "result_id": result_id,
                    "attempt_no": int(row.get("attempt_no") or 1),
                    "worker_id": row.get("worker_id"),
                    "verdict": row.get("verdict"),
                    "path": str(path),
                    "bytes": downloaded_bytes,
                    "sha256": digest,
                }
            )
        except Exception as exc:
            errors.append({"task_id": task_id, "result_id": result_id, "error": f"{type(exc).__name__}: {exc}"})
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
    manifest = {"downloaded": downloaded, "errors": errors, "total": len(results), "downloaded_count": len(downloaded)}
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {**manifest, "manifest_path": str(manifest_path)}


def controller_snapshot(public_url: str) -> dict[str, Any]:
    return {
        "task_counts": request_json(public_url.rstrip("/") + "/api/data/task-counts", timeout=30),
        "workers": request_json(public_url.rstrip("/") + "/api/data/active-workers?max_age_seconds=3600", timeout=30),
        "control_log": request_json(public_url.rstrip("/") + "/api/data/control-log?limit=500", timeout=30),
    }


def wait_tasks_finished(public_url: str, *, timeout: int = 1800) -> dict[str, Any]:
    deadline = time.time() + timeout
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = request_json(public_url.rstrip("/") + "/api/data/task-counts", timeout=30)
        if int(last.get("total") or 0) > 0 and int(last.get("pending") or 0) == 0:
            return last
        time.sleep(10)
    raise RuntimeError(f"tasks did not finish before timeout: {last}")


def gather(
    controller: dict[str, Any],
    public_url: str,
    remote_dir: str,
    output: Path,
    local_runtime: dict[str, Any] | None = None,
    download_results: bool = True,
) -> dict[str, Any]:
    results = fetch_all_results(public_url)
    summary = {
        "summary": fetch_all_tasks(public_url),
        "task_counts": request_json(public_url.rstrip("/") + "/api/data/task-counts", timeout=30),
        "worker_hosts": request_json(public_url.rstrip("/") + "/api/data/worker-hosts", timeout=30),
        "workers": request_json(public_url.rstrip("/") + "/api/data/active-workers?max_age_seconds=3600", timeout=30),
        "results": results,
        "control_log": request_json(public_url.rstrip("/") + "/api/data/control-log?limit=500", timeout=30),
    }
    if download_results:
        summary["result_downloads"] = download_result_packages(
            public_url,
            results.get("results") or [],
            output.parent / "result-packages",
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if controller_mode(controller) == "ssh-start":
        try:
            log = ssh(controller, f"tail -n 500 {shlex.quote(remote_dir)}/hub.jsonl || true", timeout=60, check=False)
            (output.parent / "loom-hub.tail.jsonl").write_text(log, encoding="utf-8")
        except Exception:
            pass
    elif local_runtime and Path(local_runtime["control_log"]).exists():
        (output.parent / "loom-hub.tail.jsonl").write_text(
            Path(local_runtime["control_log"]).read_text(encoding="utf-8", errors="replace"),
            encoding="utf-8",
        )
    return summary


def decode_json_field(row: dict[str, Any], field: str) -> dict[str, Any]:
    value = row.get(field)
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_validation_assertions(
    args: argparse.Namespace,
    inventory: dict[str, Any],
    summary: dict[str, Any],
    phase_snapshots: list[dict[str, Any]],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    expected_workers = args.expected_workers or len(inventory.get("workers") or [])
    active_workers = summary.get("workers", {}).get("workers") or []
    hosts = summary.get("worker_hosts", {}).get("hosts") or []
    add("worker_count", len(active_workers) == expected_workers, {"expected": expected_workers, "actual": len(active_workers)})
    add("host_registry_count", len(hosts) == expected_workers, {"expected": expected_workers, "actual": len(hosts)})
    counts = summary.get("task_counts") or {}
    add("all_tasks_final", int(counts.get("total") or 0) > 0 and int(counts.get("pending") or 0) == 0, counts)
    downloads = summary.get("result_downloads")
    if downloads is not None:
        expected_results = int(summary.get("results", {}).get("total") or 0)
        add(
            "result_packages_recovered",
            not downloads.get("errors") and int(downloads.get("downloaded_count") or 0) == expected_results,
            {"expected": expected_results, "downloaded": downloads.get("downloaded_count"), "errors": downloads.get("errors")},
        )

    source_probe = summary.get("source_capability_probe")
    if source_probe is not None:
        add("source_capabilities", bool(source_probe.get("ok")), source_probe)

    expectations = task_expectations(args.dispatch_spec)
    tasks_by_id = {str(task.get("task_id")): task for task in summary.get("summary", {}).get("tasks") or []}
    results_by_task: dict[str, list[dict[str, Any]]] = {}
    for result in summary.get("results", {}).get("results") or []:
        results_by_task.setdefault(str(result.get("task_id") or ""), []).append(result)
    if expectations:
        mismatches: list[dict[str, Any]] = []
        for task_id, expected in expectations.items():
            task = tasks_by_id.get(task_id)
            task_results = results_by_task.get(task_id) or []
            actual = {
                "state": task.get("state") if task else None,
                "attempt_no": int(task.get("attempt_no") or 0) if task else 0,
                "result_count": len(task_results),
                "distinct_workers": len({str(row.get("worker_id")) for row in task_results if row.get("worker_id")}),
            }
            reasons = []
            if "state" in expected and actual["state"] != expected["state"]:
                reasons.append("state")
            if "attempt_no" in expected and actual["attempt_no"] != expected["attempt_no"]:
                reasons.append("attempt_no")
            if actual["result_count"] < int(expected.get("min_result_count") or 0):
                reasons.append("min_result_count")
            if actual["distinct_workers"] < int(expected.get("min_distinct_workers") or 0):
                reasons.append("min_distinct_workers")
            if reasons:
                mismatches.append(
                    {
                        "task_id": task_id,
                        "expected": expected,
                        "actual": actual,
                        "reasons": reasons,
                    }
                )
        add(
            "task_expectations",
            not mismatches,
            {
                "declared": len(expectations),
                "matched": len(expectations) - len(mismatches),
                "mismatches": mismatches[:100],
            },
        )

    if args.require_concurrency_stable:
        calibration_workers = []
        if phase_snapshots:
            calibration_workers = phase_snapshots[0].get("snapshot", {}).get("workers", {}).get("workers") or []
        stable_details = []
        stable_ok = len(calibration_workers) == expected_workers
        for worker in calibration_workers:
            tuning = decode_json_field(worker, "tuning_json")
            baseline = tuning.get("baseline_estimate") if isinstance(tuning.get("baseline_estimate"), dict) else {}
            theoretical = int(baseline.get("theoretical_max") or 0)
            expected = theoretical + 10
            good = int(tuning.get("good_concurrency") or 0)
            desired = int(worker.get("desired_concurrency") or 0)
            hard_cap = int(worker.get("max_concurrency") or 0)
            worker_ok = theoretical > 0 and hard_cap >= expected and good >= expected and desired == expected
            stable_ok = stable_ok and worker_ok
            stable_details.append(
                {
                    "worker_id": worker.get("worker_id"),
                    "theoretical_max": theoretical,
                    "expected": expected,
                    "good_concurrency": good,
                    "desired_concurrency": desired,
                    "max_concurrency": hard_cap,
                    "passed": worker_ok,
                }
            )
        add("theoretical_plus_10_stable", stable_ok, stable_details)

    categories = {str(row.get("category")) for row in summary.get("control_log", {}).get("logs") or []}
    for category in args.require_log_category:
        add(f"control_log:{category}", category in categories, {"observed_categories": sorted(categories)})

    for task_id in args.retry_task_id:
        task = tasks_by_id.get(task_id) or {}
        add(
            f"retry:{task_id}",
            int(task.get("attempt_no") or 0) >= 2 and task.get("state") == "clean",
            {"attempt_no": task.get("attempt_no"), "state": task.get("state")},
        )
    return {"ok": all(check["passed"] for check in checks), "checks": checks}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an inventory-driven Loom Matrix validation.")
    parser.add_argument("--version", action="version", version=f"Loom Matrix Core Preview {CORE_PREVIEW_VERSION} (inventory v{INVENTORY_SCHEMA_VERSION})")
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--dispatch-spec", type=Path, action="append", required=True, help="Normalized controller dispatch JSON; repeat for ordered phases.")
    parser.add_argument("--remote-dir", default="/tmp/loom")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--forward-env", action="append", default=[], help="Forward this local environment variable to each remote worker.")
    parser.add_argument("--hub-token-env", default=DEFAULT_HUB_TOKEN_ENV, help="Environment variable containing the Hub bearer token. It is forwarded to remote Hub and Runners when set.")
    parser.add_argument("--runner-token-env", default=DEFAULT_RUNNER_TOKEN_ENV, help="Default environment variable containing Direct Runner API bearer tokens.")
    parser.add_argument("--operator", default="tencent-matrix")
    parser.add_argument("--skip-register-hosts", action="store_true")
    parser.add_argument("--expected-workers", type=int, default=None)
    parser.add_argument("--require-concurrency-stable", action="store_true")
    parser.add_argument("--require-log-category", action="append", default=[])
    parser.add_argument("--retry-task-id", action="append", default=[])
    parser.add_argument("--skip-download-results", action="store_true")
    parser.add_argument("--bootstrap-check-only", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("tencent-matrix-summary.json"))
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    global HUB_TOKEN, HUB_TOKEN_ENV, DEFAULT_DIRECT_RUNNER_TOKEN_ENV
    HUB_TOKEN_ENV = args.hub_token_env
    HUB_TOKEN = token_from_env(HUB_TOKEN_ENV)
    DEFAULT_DIRECT_RUNNER_TOKEN_ENV = args.runner_token_env
    inventory = load_inventory(args.inventory)
    tool_root = Path(__file__).resolve().parent
    controller = inventory["controller"]
    workers = inventory["workers"]
    mode = controller_mode(controller)
    if mode not in {"ssh-start", "prestarted", "local-process"}:
        raise ValueError(f"unsupported controller connection mode: {mode}")
    public_url = inventory.get("controller_public_url")
    if not public_url and mode == "ssh-start":
        public_url = f"http://{controller['host']}:{args.port}"
    if not public_url:
        raise ValueError("inventory requires controller_public_url for non-SSH controllers")
    worker_url = inventory.get("controller_worker_url") or inventory.get("controller_private_url") or public_url
    local_runtime: dict[str, Any] | None = None
    try:
        forward_names = list(args.forward_env)
        if HUB_TOKEN:
            forward_names.append(HUB_TOKEN_ENV)
        for worker in workers:
            if connection_mode(worker) == "direct-worker-api":
                forward_names.append(str(worker.get("direct_api_token_env") or DEFAULT_DIRECT_RUNNER_TOKEN_ENV))
        forwarded_env = {name: os.environ[name] for name in dict.fromkeys(forward_names) if os.environ.get(name)}
        if mode == "ssh-start":
            if not HUB_TOKEN:
                raise ValueError(f"remote Hub startup requires {HUB_TOKEN_ENV} in the operator environment")
            wait_ssh(controller)
            remote_setup(controller, args.remote_dir, tool_root)
            start_hub(controller, args.remote_dir, args.port, forwarded_env)
        elif mode == "local-process":
            local_runtime = start_local_hub(controller, tool_root, args.output, args.port)
        for worker in workers:
            if connection_mode(worker) in SSH_STARTED_MODES and not worker.get("predeployed"):
                wait_ssh(worker)
                remote_setup(worker, args.remote_dir, tool_root)
        if args.bootstrap_check_only:
            result = {
                "ok": True,
                "bootstrap_check_only": True,
                "controller_mode": mode,
                "workers": [
                    {
                        "worker_id": worker.get("worker_id"),
                        "connection_mode": connection_mode(worker),
                        "host": worker.get("host"),
                    }
                    for worker in workers
                ],
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        source_capability_probe = probe_source_capabilities(workers, args.dispatch_spec)
        wait_http(public_url)
        host_registration = None if args.skip_register_hosts else register_worker_hosts(public_url, inventory, args.operator)
        worker_starts = [start_worker(worker, worker_url, args.remote_dir, forwarded_env) for worker in workers]
        active_workers_at_dispatch = wait_workers_registered(
            public_url,
            [str(worker.get("worker_id") or worker["host"].replace(".", "-")) for worker in workers],
        )
        phase_snapshots = []
        for phase_index, dispatch_spec in enumerate(args.dispatch_spec, start=1):
            dispatch_result = dispatch(public_url, dispatch_spec)
            wait_tasks_finished(public_url, timeout=args.timeout_seconds)
            phase_snapshots.append(
                {
                    "phase": phase_index,
                    "dispatch_spec": str(dispatch_spec),
                    "dispatch": dispatch_result,
                    "snapshot": controller_snapshot(public_url),
                }
            )
        retry_results = []
        for task_id in args.retry_task_id:
            retry_results.append(retry_task(public_url, task_id, args.operator))
        if retry_results:
            wait_tasks_finished(public_url, timeout=args.timeout_seconds)
        summary = gather(
            controller,
            public_url,
            args.remote_dir,
            args.output,
            local_runtime,
            download_results=not args.skip_download_results,
        )
        summary["phase_snapshots"] = phase_snapshots
        summary["retry_results"] = retry_results
        summary["source_capability_probe"] = source_capability_probe
        assertions = build_validation_assertions(args, inventory, summary, phase_snapshots)
        summary["validation"] = assertions
        args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(
            json.dumps(
                {
                    "ok": True,
                    "controller_mode": mode,
                    "host_registration": host_registration,
                    "phases": [
                        {
                            "phase": phase["phase"],
                            "dispatch_spec": phase["dispatch_spec"],
                            "task_counts": phase["snapshot"].get("task_counts"),
                        }
                        for phase in phase_snapshots
                    ],
                    "retries": retry_results,
                    "workers": worker_starts,
                    "active_workers_at_dispatch": active_workers_at_dispatch,
                    "source_capability_probe": source_capability_probe,
                    "summary_path": str(args.output),
                    "task_counts": summary.get("task_counts"),
                    "result_count": summary.get("results", {}).get("total"),
                    "validation": assertions,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if assertions["ok"] else 1
    finally:
        stop_local_hub(local_runtime)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Loom Hub.

The Hub owns task state, Runner leases, result intake, scoring import, operator
overrides, and read-only data APIs. It intentionally uses only the Python
standard library so it can run unchanged on supplied hosts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from loom_contract import (
    CONCURRENCY_POLICIES,
    CORE_PREVIEW_VERSION,
    DISPATCH_SCHEMA_VERSION,
    FIXED_CONCURRENCY_BACKOFF_ISSUES,
    HUB_API_VERSION,
    INVENTORY_SCHEMA_VERSION,
    merge_extensions,
    metadata,
    RUNNER_API_VERSION,
)
from loom_http import DEFAULT_HUB_TOKEN_ENV, DEFAULT_RUNNER_TOKEN_ENV, is_loopback_host, request_json as http_request_json, token_from_env, token_matches
from loom_resources import (
    add_resources,
    normalize_capacity,
    normalize_capacity_overrides,
    normalize_execution_profile,
    remaining_resources,
    resource_shortfalls,
)


CASE_ROW = re.compile(r"^\|\s*(C\d+)\s*\|\s*`?([^`|]+)`?\s*\|\s*`?([^`|]+)`?\s*\|\s*`?([^`|]+)`?\s*\|")
FINAL_STATES = {"clean", "dirty", "run_error", "needs_review", "accepted", "ignored", "blocked", "cancelled"}
ERROR_STATES = {"run_error", "needs_review"}
QUERY_FIELDS = {
    "task_id",
    "worker_id",
    "case_id",
    "run_id",
    "setting_id",
    "case_version",
    "scenario_id",
    "package_id",
    "state",
    "lease_worker_id",
    "attempt_no",
    "result_id",
    "created_at",
    "updated_at",
}
QUERY_FIELD_SQL = {field: ("lease_worker_id" if field == "worker_id" else field) for field in QUERY_FIELDS}
ISSUE_PATTERNS = [
    ("terminal_resource_insufficient", re.compile(r"(out of memory|oom|cannot allocate memory|no space left|disk quota|resource temporarily unavailable|too many open files)", re.I)),
    ("token_balance_insufficient", re.compile(r"(insufficient.*(credit|balance|quota)|billing|payment required|credits? exhausted)", re.I)),
    ("rate_limited", re.compile(r"(rate.?limit|too many requests|429|quota exceeded|limit exceeded)", re.I)),
    ("auth_failed", re.compile(r"(unauthorized|forbidden|invalid api key|permission denied|401|403)", re.I)),
    ("network_unavailable", re.compile(r"(connection timed out|temporary failure|network is unreachable|could not resolve|tls|ssl)", re.I)),
]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def classify_issue_text(text: str) -> list[str]:
    categories = []
    for category, pattern in ISSUE_PATTERNS:
        if pattern.search(text):
            categories.append(category)
    return categories


def append_jsonl(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        f.write("\n")


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    out: list[str] = []
    for item in value.split(","):
        item = item.strip()
        if item:
            out.append(item)
    return out


def connection_mode(host: dict[str, Any]) -> str:
    connection = host.get("connection") if isinstance(host.get("connection"), dict) else {}
    return str(
        host.get("connection_mode")
        or host.get("transport_mode")
        or connection.get("mode")
        or "ssh-start"
    )


def host_identifier(host: dict[str, Any]) -> str:
    return str(host.get("host_id") or host.get("worker_id") or host.get("name") or host.get("host") or "host-" + str(uuid.uuid4()))


def host_endpoint(host: dict[str, Any]) -> dict[str, Any]:
    connection = host.get("connection") if isinstance(host.get("connection"), dict) else {}
    endpoint = dict(host.get("endpoint") or connection.get("endpoint") or {})
    if host.get("worker_url"):
        endpoint["worker_url"] = host["worker_url"]
    if host.get("direct_api_token_env"):
        endpoint["direct_api_token_env"] = host["direct_api_token_env"]
    if host.get("runner_token_env"):
        endpoint["direct_api_token_env"] = host["runner_token_env"]
    if host.get("host"):
        endpoint.setdefault("host", host["host"])
    if host.get("port"):
        endpoint.setdefault("ssh_port", host["port"])
    if host.get("command_port"):
        endpoint.setdefault("command_port", host["command_port"])
    if host.get("serve_port"):
        endpoint.setdefault("command_port", host["serve_port"])
    if host.get("private_host"):
        endpoint.setdefault("private_host", host["private_host"])
    return endpoint


def host_ssh_config(host: dict[str, Any]) -> dict[str, Any]:
    connection = host.get("connection") if isinstance(host.get("connection"), dict) else {}
    ssh_config = dict(host.get("ssh") or connection.get("ssh") or {})
    for key in ("host", "user", "key_path", "port"):
        if host.get(key) is not None:
            ssh_config.setdefault(key, host[key])
    if "private_key" in ssh_config:
        ssh_config["private_key"] = "<redacted>"
    return ssh_config


def register_host_rows(conn: sqlite3.Connection, hosts: list[dict[str, Any]], operator: str) -> list[dict[str, Any]]:
    now = utc_now()
    registered: list[dict[str, Any]] = []
    for raw in hosts:
        host = dict(raw)
        host_id = host_identifier(host)
        worker_id = str(host.get("worker_id") or host_id)
        mode = connection_mode(host)
        capabilities = host.get("capabilities") or []
        max_concurrency = max(1, safe_int(host.get("max_concurrency") or host.get("cpu"), 1))
        initial_concurrency = safe_int(host.get("initial_concurrency"), 1)
        if initial_concurrency < 1 or initial_concurrency > max_concurrency:
            raise ValueError(f"host {host_id} requires 1 <= initial_concurrency <= max_concurrency")
        concurrency_policy = str(host.get("concurrency_policy") or "fixed").strip().lower()
        if concurrency_policy not in CONCURRENCY_POLICIES:
            raise ValueError(f"host {host_id} has unsupported concurrency_policy: {concurrency_policy}")
        resource_capacity = normalize_capacity_overrides(host.get("resource_capacity"))
        endpoint = host_endpoint(host)
        ssh_config = host_ssh_config(host)
        labels = dict(host.get("labels") or {})
        for key in ("instance_type", "cpu", "memory_gb"):
            if host.get(key) is not None:
                labels.setdefault(key, host[key])
        labels.setdefault("initial_concurrency", initial_concurrency)
        labels.setdefault("concurrency_policy", concurrency_policy)
        if resource_capacity:
            labels.setdefault("resource_capacity", resource_capacity)
        conn.execute(
            """
            INSERT INTO worker_hosts(
              host_id, worker_id, connection_mode, state, capabilities_json,
              max_concurrency, initial_concurrency, concurrency_policy, resource_capacity_json, endpoint_json, ssh_json, labels_json,
              registered_by, created_at, last_seen_at, updated_at
            )
            VALUES (?, ?, ?, 'registered', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id) DO UPDATE SET
              worker_id=excluded.worker_id,
              connection_mode=excluded.connection_mode,
              state='registered',
              capabilities_json=excluded.capabilities_json,
              max_concurrency=excluded.max_concurrency,
              initial_concurrency=excluded.initial_concurrency,
              concurrency_policy=excluded.concurrency_policy,
              resource_capacity_json=excluded.resource_capacity_json,
              endpoint_json=excluded.endpoint_json,
              ssh_json=excluded.ssh_json,
              labels_json=excluded.labels_json,
              registered_by=excluded.registered_by,
              last_seen_at=excluded.last_seen_at,
              updated_at=excluded.updated_at
            """,
            (
                host_id,
                worker_id,
                mode,
                json.dumps(capabilities, ensure_ascii=False),
                max_concurrency,
                initial_concurrency,
                concurrency_policy,
                json.dumps(resource_capacity, ensure_ascii=False),
                json.dumps(endpoint, ensure_ascii=False),
                json.dumps(ssh_config, ensure_ascii=False),
                json.dumps(labels, ensure_ascii=False),
                operator,
                now,
                now,
                now,
            ),
        )
        registered.append(
            {
                "host_id": host_id,
                "worker_id": worker_id,
                "connection_mode": mode,
                "max_concurrency": max_concurrency,
                "initial_concurrency": initial_concurrency,
                "concurrency_policy": concurrency_policy,
                "resource_capacity": resource_capacity,
                "capabilities": capabilities,
                "endpoint": endpoint,
            }
        )
    return registered


def load_registry_cases(benchmark_root: Path) -> list[dict[str, str]]:
    path = benchmark_root / "CANONICAL-CASE-REGISTRY.md"
    if not path.exists():
        return []
    cases: list[dict[str, str]] = []
    in_official = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line == "## Official Cases":
            in_official = True
            continue
        if in_official and line.startswith("## "):
            break
        if not in_official or not line.startswith("| C"):
            continue
        cols = [c.strip().strip("`") for c in line.strip("|").split("|")]
        if len(cols) < 8:
            continue
        cases.append(
            {
                "case_id": cols[0],
                "case_version": cols[1],
                "definition_version": cols[2],
                "scenario_id": cols[3],
                "attack_scenario": cols[4],
                "attack_vector": cols[5],
                "fixture_id": cols[6],
                "scorer": cols[7],
            }
        )
    return cases


def case_components(benchmark_root: Path, case: dict[str, str]) -> dict[str, Any]:
    scenario_id = case.get("scenario_id", "")
    fixture_id = case.get("fixture_id", "")
    scorer = case.get("scorer", "")
    return {
        "case": case,
        "paths": {
            "scenario": str(benchmark_root / "scenarios" / f"{scenario_id}.json"),
            "fixture": str(benchmark_root / "fixtures" / fixture_id),
            "scorer": str(benchmark_root / "scorers" / scorer),
        },
    }


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS workers (
          worker_id TEXT PRIMARY KEY,
          status TEXT NOT NULL DEFAULT 'registered',
          capabilities_json TEXT NOT NULL DEFAULT '[]',
          max_concurrency INTEGER NOT NULL DEFAULT 1,
          initial_concurrency INTEGER NOT NULL DEFAULT 1,
          concurrency_policy TEXT NOT NULL DEFAULT 'fixed',
          blocked INTEGER NOT NULL DEFAULT 0,
          active_runs_json TEXT NOT NULL DEFAULT '[]',
          health_json TEXT NOT NULL DEFAULT '{}',
          resource_json TEXT NOT NULL DEFAULT '{}',
          resource_capacity_json TEXT NOT NULL DEFAULT '{}',
          tuning_json TEXT NOT NULL DEFAULT '{}',
          desired_concurrency INTEGER NOT NULL DEFAULT 1,
          registered_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
          task_id TEXT PRIMARY KEY,
          state TEXT NOT NULL,
          priority INTEGER NOT NULL DEFAULT 0,
          case_id TEXT,
          run_id TEXT,
          setting_id TEXT,
          case_version TEXT,
          scenario_id TEXT,
          package_id TEXT,
          payload_json TEXT NOT NULL DEFAULT '{}',
          components_json TEXT NOT NULL DEFAULT '{}',
          required_capability TEXT,
          lease_worker_id TEXT,
          lease_until TEXT,
          attempt_no INTEGER NOT NULL DEFAULT 1,
          result_id TEXT,
          error_json TEXT,
          excluded_worker_ids_json TEXT NOT NULL DEFAULT '[]',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS task_events (
          event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL,
          task_id TEXT,
          event_type TEXT NOT NULL,
          actor_type TEXT NOT NULL,
          actor_id TEXT,
          old_state TEXT,
          new_state TEXT,
          payload_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS worker_events (
          event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL,
          worker_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS worker_hosts (
          host_id TEXT PRIMARY KEY,
          worker_id TEXT,
          connection_mode TEXT NOT NULL DEFAULT 'ssh-start',
          state TEXT NOT NULL DEFAULT 'registered',
          capabilities_json TEXT NOT NULL DEFAULT '[]',
          max_concurrency INTEGER NOT NULL DEFAULT 1,
          initial_concurrency INTEGER NOT NULL DEFAULT 1,
          concurrency_policy TEXT NOT NULL DEFAULT 'fixed',
          resource_capacity_json TEXT NOT NULL DEFAULT '{}',
          endpoint_json TEXT NOT NULL DEFAULT '{}',
          ssh_json TEXT NOT NULL DEFAULT '{}',
          labels_json TEXT NOT NULL DEFAULT '{}',
          registered_by TEXT NOT NULL,
          created_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS result_packages (
          cursor INTEGER PRIMARY KEY AUTOINCREMENT,
          result_id TEXT NOT NULL UNIQUE,
          task_id TEXT,
          worker_id TEXT,
          attempt_no INTEGER NOT NULL DEFAULT 1,
          path TEXT NOT NULL,
          bytes INTEGER NOT NULL,
          sha256 TEXT NOT NULL,
          uploaded_at TEXT NOT NULL,
          imported_at TEXT,
          score_state TEXT NOT NULL,
          verdict TEXT,
          summary_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS admin_overrides (
          override_seq INTEGER PRIMARY KEY AUTOINCREMENT,
          override_id TEXT NOT NULL,
          target_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          operator TEXT NOT NULL,
          old_state TEXT,
          new_state TEXT NOT NULL,
          reason TEXT NOT NULL,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS control_logs (
          log_seq INTEGER PRIMARY KEY AUTOINCREMENT,
          level TEXT NOT NULL,
          category TEXT NOT NULL,
          worker_id TEXT,
          task_id TEXT,
          message TEXT NOT NULL,
          payload_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        );

        """
    )
    ensure_column(conn, "workers", "resource_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(conn, "workers", "resource_capacity_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(conn, "workers", "tuning_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(conn, "workers", "desired_concurrency", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "workers", "initial_concurrency", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "workers", "concurrency_policy", "TEXT NOT NULL DEFAULT 'fixed'")
    ensure_column(conn, "worker_hosts", "initial_concurrency", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "worker_hosts", "concurrency_policy", "TEXT NOT NULL DEFAULT 'fixed'")
    ensure_column(conn, "worker_hosts", "resource_capacity_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(conn, "tasks", "run_id", "TEXT")
    ensure_column(conn, "tasks", "setting_id", "TEXT")
    ensure_column(conn, "tasks", "excluded_worker_ids_json", "TEXT NOT NULL DEFAULT '[]'")
    ensure_column(conn, "result_packages", "attempt_no", "INTEGER NOT NULL DEFAULT 1")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_state_created ON tasks(state, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_case_run ON tasks(case_id, setting_id, run_id)")


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if column not in {row["name"] for row in rows}:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def log_task_event(
    conn: sqlite3.Connection,
    task_id: str | None,
    event_type: str,
    actor_type: str,
    actor_id: str | None,
    old_state: str | None,
    new_state: str | None,
    payload: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO task_events(event_id, task_id, event_type, actor_type, actor_id, old_state, new_state, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            task_id,
            event_type,
            actor_type,
            actor_id,
            old_state,
            new_state,
            json.dumps(payload or {}, ensure_ascii=False),
            utc_now(),
        ),
    )


def log_worker_event(conn: sqlite3.Connection, worker_id: str, event_type: str, payload: dict[str, Any] | None = None) -> None:
    conn.execute(
        """
        INSERT INTO worker_events(event_id, worker_id, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (str(uuid.uuid4()), worker_id, event_type, json.dumps(payload or {}, ensure_ascii=False), utc_now()),
    )


def log_control(
    conn: sqlite3.Connection,
    server: "ControllerServer | None",
    level: str,
    category: str,
    message: str,
    *,
    worker_id: str | None = None,
    task_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    now = utc_now()
    row = {
        "level": level,
        "category": category,
        "worker_id": worker_id,
        "task_id": task_id,
        "message": message,
        "payload": payload or {},
        "created_at": now,
    }
    conn.execute(
        """
        INSERT INTO control_logs(level, category, worker_id, task_id, message, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (level, category, worker_id, task_id, message, json.dumps(payload or {}, ensure_ascii=False), now),
    )
    if server is not None:
        append_jsonl(getattr(server, "control_log_path", None), row)


def set_task_state(
    conn: sqlite3.Connection,
    task_id: str,
    new_state: str,
    actor_type: str,
    actor_id: str | None,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None:
        raise KeyError(task_id)
    old_state = row["state"]
    now = utc_now()
    completed_at = now if new_state in FINAL_STATES else row["completed_at"]
    conn.execute(
        "UPDATE tasks SET state=?, updated_at=?, completed_at=? WHERE task_id=?",
        (new_state, now, completed_at, task_id),
    )
    log_task_event(conn, task_id, event_type, actor_type, actor_id, old_state, new_state, payload)
    return {"task_id": task_id, "old_state": old_state, "new_state": new_state}


def worker_has_capability(task: sqlite3.Row, capabilities: list[str]) -> bool:
    required = (task["required_capability"] or "").strip()
    if not required:
        return True
    return required in capabilities or "*" in capabilities


def worker_can_run(task: sqlite3.Row, capabilities: list[str], worker_id: str) -> bool:
    if not worker_has_capability(task, capabilities):
        return False
    try:
        excluded = {str(value) for value in json.loads(task["excluded_worker_ids_json"] or "[]")}
    except (json.JSONDecodeError, TypeError):
        excluded = set()
    return worker_id not in excluded


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(value or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def merged_dispatch_extensions(dispatch: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    """Keep user metadata opaque while making its merge order explicit."""
    dispatch_payload = dispatch.get("payload") or {}
    task_payload = task.get("payload") or {}
    if not isinstance(dispatch_payload, dict):
        raise ValueError("dispatch.payload must be an object")
    if not isinstance(task_payload, dict):
        raise ValueError("task.payload must be an object")
    layers = (
        ("dispatch.extensions", dispatch.get("extensions")),
        ("dispatch.payload.extensions", dispatch_payload.get("extensions")),
        ("task.extensions", task.get("extensions")),
        ("task.payload.extensions", task_payload.get("extensions")),
    )
    if not any(value is not None for _, value in layers):
        return None
    return merge_extensions(*layers)


def task_execution_profile(task: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    payload = json_object(task["payload_json"] if "payload_json" in task.keys() else task.get("payload"))
    return normalize_execution_profile(payload.get("execution_profile"))


def worker_resource_capacity(worker: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    capacity_raw = worker["resource_capacity_json"] if "resource_capacity_json" in worker.keys() else worker.get("resource_capacity")
    resource_raw = worker["resource_json"] if "resource_json" in worker.keys() else worker.get("resources")
    return normalize_capacity(json_object(capacity_raw), snapshot=json_object(resource_raw))


def worker_concurrency_state(
    conn: sqlite3.Connection,
    worker: sqlite3.Row | dict[str, Any],
    *,
    leased_active: int | None = None,
) -> dict[str, int]:
    """Treat Hub leases as active even before the next Runner heartbeat."""
    worker_id = str(worker["worker_id"])
    active_raw = worker["active_runs_json"] if "active_runs_json" in worker.keys() else worker.get("active_runs")
    try:
        reported_runs = json.loads(active_raw or "[]")
    except (json.JSONDecodeError, TypeError):
        reported_runs = []
    reported_active = len(reported_runs) if isinstance(reported_runs, list) else 0
    if leased_active is None:
        leased_active = int(
            conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE lease_worker_id=? AND state IN ('leased','running')",
                (worker_id,),
            ).fetchone()[0]
        )
    max_concurrency = max(1, int(worker["max_concurrency"] or 1))
    desired_concurrency = max(1, min(int(worker["desired_concurrency"] or 1), max_concurrency))
    return {
        "reported_active": reported_active,
        "leased_active": leased_active,
        "active_count": max(reported_active, leased_active),
        "desired_concurrency": desired_concurrency,
        "max_concurrency": max_concurrency,
        "effective_limit": min(desired_concurrency, max_concurrency),
    }


def worker_reservation_state(conn: sqlite3.Connection, worker_id: str) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT task_id,payload_json FROM tasks WHERE lease_worker_id=? AND state IN ('leased','running') ORDER BY updated_at,task_id",
        (worker_id,),
    ).fetchall()
    profiles = [task_execution_profile(row) for row in rows]
    return {
        "task_ids": [str(row["task_id"]) for row in rows],
        "reserved": add_resources(*(profile["resources"] for profile in profiles)),
        "exclusive_active": any(profile["placement"] == "exclusive" for profile in profiles),
    }


def worker_resource_admission(conn: sqlite3.Connection, worker: sqlite3.Row, task: sqlite3.Row) -> dict[str, Any]:
    profile = task_execution_profile(task)
    capacity = worker_resource_capacity(worker)
    reservation = worker_reservation_state(conn, str(worker["worker_id"]))
    concurrency = worker_concurrency_state(conn, worker, leased_active=len(reservation["task_ids"]))
    reserved = reservation["reserved"]
    requested = profile["resources"]
    shortfalls = resource_shortfalls(capacity, reserved, requested)
    reason: str | None = None
    if profile["placement"] == "exclusive" and reservation["task_ids"]:
        reason = "exclusive_worker_busy"
    elif reservation["exclusive_active"]:
        reason = "worker_has_exclusive_task"
    elif shortfalls:
        reason = "resource_reservation_unavailable"
    elif concurrency["active_count"] >= concurrency["effective_limit"]:
        reason = "worker_concurrency_reached"
    projected = add_resources(reserved, requested)
    return {
        "ok": reason is None,
        "reason": reason,
        "placement": profile["placement"],
        "requested": requested,
        "capacity": capacity,
        "reserved_before": reserved,
        "available_before": remaining_resources(capacity, reserved),
        "available_after": remaining_resources(capacity, projected),
        "shortfalls": shortfalls,
        "active_task_ids": reservation["task_ids"],
        "exclusive_active": reservation["exclusive_active"],
        "concurrency": concurrency,
    }


def automatic_retry_policy(task: sqlite3.Row) -> dict[str, Any]:
    try:
        payload = json.loads(task["payload_json"] or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}
    raw = payload.get("retry_policy")
    if not isinstance(raw, dict):
        return {}
    categories = raw.get("retry_categories") or raw.get("categories") or []
    if isinstance(categories, str):
        categories = [categories]
    return {
        "max_attempts": max(1, min(safe_int(raw.get("max_attempts"), 1), 20)),
        "retry_categories": {str(value) for value in categories if value},
        "different_worker": bool(raw.get("different_worker", False)),
    }


def queue_automatic_retry(
    conn: sqlite3.Connection,
    server: Any,
    task_id: str,
    worker_id: str,
    issues: list[str],
) -> dict[str, Any] | None:
    task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if task is None:
        return None
    policy = automatic_retry_policy(task)
    matched = sorted(set(str(value) for value in issues) & policy.get("retry_categories", set()))
    attempt_no = max(1, safe_int(task["attempt_no"], 1))
    max_attempts = safe_int(policy.get("max_attempts"), 1)
    if not matched or attempt_no >= max_attempts:
        return None

    try:
        excluded = {str(value) for value in json.loads(task["excluded_worker_ids_json"] or "[]")}
    except (json.JSONDecodeError, TypeError):
        excluded = set()
    if policy.get("different_worker") and worker_id:
        excluded.add(worker_id)
        cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
        candidates = []
        for candidate in conn.execute(
            "SELECT worker_id,capabilities_json FROM workers WHERE blocked=0 AND last_seen_at>=?",
            (cutoff,),
        ):
            candidate_id = str(candidate["worker_id"])
            try:
                capabilities = json.loads(candidate["capabilities_json"] or "[]")
            except (json.JSONDecodeError, TypeError):
                capabilities = []
            if candidate_id not in excluded and worker_has_capability(task, capabilities):
                candidates.append(candidate_id)
        if not candidates:
            log_control(
                conn,
                server,
                "warning",
                "automatic_retry_unavailable",
                f"task {task_id} requested a different worker retry but no eligible active worker is available",
                worker_id=worker_id,
                task_id=task_id,
                payload={"attempt_no": attempt_no, "max_attempts": max_attempts, "issues": matched},
            )
            return None

    next_attempt = attempt_no + 1
    conn.execute(
        """
        UPDATE tasks
        SET lease_worker_id=NULL, lease_until=NULL, attempt_no=?, result_id=NULL,
            error_json=NULL, completed_at=NULL, excluded_worker_ids_json=?
        WHERE task_id=?
        """,
        (next_attempt, json.dumps(sorted(excluded), ensure_ascii=False), task_id),
    )
    set_task_state(
        conn,
        task_id,
        "retry_queued",
        "controller",
        "automatic_retry",
        "task_automatic_retry_queued",
        {
            "previous_worker_id": worker_id,
            "attempt_no": next_attempt,
            "max_attempts": max_attempts,
            "issues": matched,
            "excluded_worker_ids": sorted(excluded),
        },
    )
    result = {
        "queued": True,
        "attempt_no": next_attempt,
        "max_attempts": max_attempts,
        "issues": matched,
        "different_worker": bool(policy.get("different_worker")),
        "excluded_worker_ids": sorted(excluded),
    }
    log_control(
        conn,
        server,
        "info",
        "automatic_retry",
        f"task {task_id} automatically queued attempt {next_attempt} after {', '.join(matched)}",
        worker_id=worker_id,
        task_id=task_id,
        payload=result,
    )
    return result


def theoretical_concurrency(resource: dict[str, Any], worker_result: dict[str, Any]) -> dict[str, Any]:
    usage = worker_result.get("resource_usage") or {}
    cpu_count = max(1, safe_int(resource.get("cpu_count"), 1))
    mem_total_mb = safe_float(resource.get("mem_total_mb"), 0.0)
    max_rss_mb = safe_float(usage.get("max_rss_mb"), 0.0)
    cpu_seconds = safe_float(usage.get("cpu_seconds"), 0.0)
    wall_seconds = max(safe_float(usage.get("duration_seconds"), 0.0), 0.001)
    cpu_per_run = max(cpu_seconds / wall_seconds, 0.05) if cpu_seconds else 1.0
    cpu_estimate = max(1, int(cpu_count / cpu_per_run))
    if mem_total_mb and max_rss_mb:
        mem_estimate = max(1, int((mem_total_mb * 0.80) / max_rss_mb))
    else:
        mem_estimate = cpu_estimate
    theoretical = max(1, min(cpu_estimate, mem_estimate))
    return {
        "cpu_count": cpu_count,
        "mem_total_mb": mem_total_mb,
        "single_run_max_rss_mb": max_rss_mb,
        "single_run_cpu_seconds": cpu_seconds,
        "single_run_wall_seconds": wall_seconds,
        "cpu_estimate": cpu_estimate,
        "memory_estimate": mem_estimate,
        "theoretical_max": theoretical,
        "probe_limit": theoretical + 10,
    }


def next_probe_after_success(tuning: dict[str, Any], current: int, theoretical: int, hard_cap: int) -> tuple[int, str]:
    hard_cap = max(1, hard_cap)
    probe_limit = min(hard_cap, theoretical + 10)
    good = max(safe_int(tuning.get("good_concurrency"), 0), current)
    bad = safe_int(tuning.get("bad_concurrency"), 0)
    if good < theoretical:
        target = max(good + 1, (good + theoretical + 1) // 2)
        if bad and target >= bad:
            target = max(good + 1, (good + bad) // 2)
        return min(target, hard_cap), "binary_probe_toward_theoretical_max"
    if good < probe_limit:
        return min(good + 1, hard_cap), "linear_probe_past_theoretical_max"
    return min(good, hard_cap), "theoretical_plus_10_stable"


def next_probe_after_failure(tuning: dict[str, Any], current: int) -> tuple[int, str]:
    good = safe_int(tuning.get("good_concurrency"), 1)
    if current <= 1:
        return 1, "single_concurrency_unhealthy"
    if good >= current:
        return current - 1, "rollback_after_unhealthy_probe"
    return max(1, (good + current) // 2), "rollback_after_unhealthy_probe"


def update_worker_concurrency_from_result(
    conn: sqlite3.Connection,
    server: "ControllerServer | None",
    worker_id: str,
    task_id: str,
    verdict: str,
    summary: dict[str, Any],
) -> None:
    worker = conn.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
    if worker is None:
        return
    resource = json.loads(worker["resource_json"] or "{}")
    tuning = json.loads(worker["tuning_json"] or "{}")
    worker_result = summary.get("worker_result") or {}
    max_concurrency = max(1, safe_int(worker["max_concurrency"], 1))
    initial_concurrency = max(1, min(safe_int(worker["initial_concurrency"], 1), max_concurrency))
    concurrency_policy = str(worker["concurrency_policy"] or "fixed").strip().lower()
    if concurrency_policy not in CONCURRENCY_POLICIES:
        concurrency_policy = "fixed"
    current_desired = max(1, min(safe_int(worker["desired_concurrency"], initial_concurrency), max_concurrency))
    reported_estimate = theoretical_concurrency(resource, worker_result)
    concurrency = worker_result.get("controller_concurrency") or {}
    observed = max(1, min(safe_int(concurrency.get("desired_concurrency"), current_desired), max_concurrency))
    batch_id = str(concurrency.get("claim_batch_id") or f"legacy-{task_id}")
    batch_size = max(1, safe_int(concurrency.get("claim_batch_size"), 1))
    issues = summary.get("issues") or []
    healthy = verdict == "clean" and not issues
    if concurrency_policy == "fixed":
        backoff_issues = sorted(set(str(issue) for issue in issues) & FIXED_CONCURRENCY_BACKOFF_ISSUES)
        desired = current_desired
        mode = "fixed_stable"
        if not healthy and backoff_issues:
            desired = max(1, current_desired - 1)
            mode = "fixed_backoff_" + "_".join(backoff_issues)
        tuning.update(
            {
                "updated_at": utc_now(),
                "concurrency_policy": "fixed",
                "initial_concurrency": initial_concurrency,
                "current_concurrency": desired,
                "last_mode": mode,
                "last_verdict": verdict,
                "last_reported_estimate": reported_estimate,
                "last_observed_concurrency": observed,
                "last_fixed_backoff_issues": backoff_issues,
            }
        )
        if not healthy:
            for issue in issues or ["run_error"]:
                log_control(
                    conn,
                    server,
                    "warning",
                    str(issue),
                    f"worker {worker_id} task {task_id} reported {issue}; fixed concurrency remains {desired}",
                    worker_id=worker_id,
                    task_id=task_id,
                    payload={
                        "concurrency_policy": "fixed",
                        "initial_concurrency": initial_concurrency,
                        "desired_concurrency": desired,
                        "backoff_issues": backoff_issues,
                    },
                )
        if backoff_issues:
            log_control(
                conn,
                server,
                "warning",
                "fixed_concurrency_backoff",
                f"worker {worker_id} reduced fixed concurrency from {current_desired} to {desired}",
                worker_id=worker_id,
                task_id=task_id,
                payload={"issues": backoff_issues, "initial_concurrency": initial_concurrency},
            )
        conn.execute(
            "UPDATE workers SET desired_concurrency=?, tuning_json=?, updated_at=? WHERE worker_id=?",
            (desired, json.dumps(tuning, ensure_ascii=False), utc_now(), worker_id),
        )
        return
    baseline_created = False
    if healthy and observed == 1 and not isinstance(tuning.get("baseline_estimate"), dict):
        tuning["baseline_estimate"] = reported_estimate
        tuning["baseline_task_id"] = task_id
        baseline_created = True
    estimate = tuning.get("baseline_estimate") if isinstance(tuning.get("baseline_estimate"), dict) else reported_estimate
    theoretical = max(1, safe_int(estimate.get("theoretical_max"), 1))
    desired = current_desired
    mode = "awaiting_probe_batch"
    completed_batches = [str(item) for item in tuning.get("completed_probe_batches") or []]
    rejected_batches = [str(item) for item in tuning.get("rejected_probe_batches") or []]
    probe_batches = dict(tuning.get("probe_batches") or {})
    stale_batch = batch_id in completed_batches or batch_id in rejected_batches

    if baseline_created:
        log_control(
            conn,
            server,
            "info",
            "concurrency_baseline",
            f"worker {worker_id} recorded single-run baseline with theoretical max {theoretical}",
            worker_id=worker_id,
            task_id=task_id,
            payload={"estimate": estimate, "claim_batch_id": batch_id},
        )

    if not healthy:
        if not stale_batch:
            desired, mode = next_probe_after_failure(tuning, observed)
            if observed > 1:
                existing_bad = safe_int(tuning.get("bad_concurrency"), 0)
                tuning["bad_concurrency"] = min(existing_bad, observed) if existing_bad else observed
            rejected_batches.append(batch_id)
            probe_batches.pop(batch_id, None)
        else:
            mode = "stale_failed_probe_result"
        for issue in issues or ["run_error"]:
            log_control(
                conn,
                server,
                "warning",
                str(issue),
                f"worker {worker_id} task {task_id} reported {issue}; reducing concurrency to {desired}",
                worker_id=worker_id,
                task_id=task_id,
                payload={
                    "verdict": verdict,
                    "estimate": estimate,
                    "reported_estimate": reported_estimate,
                    "observed_concurrency": observed,
                    "claim_batch_id": batch_id,
                    "summary": summary,
                },
            )
    elif stale_batch:
        mode = "stale_healthy_probe_result"
    elif batch_size < observed:
        mode = "insufficient_probe_batch"
    else:
        probe = dict(
            probe_batches.get(batch_id)
            or {
                "concurrency": observed,
                "expected_results": observed,
                "claim_batch_size": batch_size,
                "healthy_task_ids": [],
            }
        )
        healthy_task_ids = [str(item) for item in probe.get("healthy_task_ids") or []]
        if task_id not in healthy_task_ids:
            healthy_task_ids.append(task_id)
        probe["healthy_task_ids"] = healthy_task_ids
        probe["updated_at"] = utc_now()
        probe_batches[batch_id] = probe
        if len(healthy_task_ids) >= observed:
            tuning["good_concurrency"] = max(safe_int(tuning.get("good_concurrency"), 0), observed)
            desired, mode = next_probe_after_success(tuning, observed, theoretical, max_concurrency)
            completed_batches.append(batch_id)
            probe_batches.pop(batch_id, None)
            if mode != "theoretical_plus_10_stable":
                log_control(
                    conn,
                    server,
                    "info",
                    "concurrency_probe",
                    f"worker {worker_id} confirmed concurrency {observed}; next probe is {desired}",
                    worker_id=worker_id,
                    task_id=task_id,
                    payload={
                        "mode": mode,
                        "estimate": estimate,
                        "observed_concurrency": observed,
                        "desired_concurrency": desired,
                        "claim_batch_id": batch_id,
                        "healthy_task_ids": healthy_task_ids,
                    },
                )
        if mode == "theoretical_plus_10_stable":
            stable_key = f"{theoretical}:{desired}"
            if tuning.get("stability_logged_for") != stable_key:
                tuning["stability_logged_for"] = stable_key
                log_control(
                    conn,
                    server,
                    "info",
                    "concurrency_probe",
                    f"worker {worker_id} stayed healthy at theoretical max + 10; running at {desired}",
                    worker_id=worker_id,
                    task_id=task_id,
                    payload={"estimate": estimate, "desired_concurrency": desired, "claim_batch_id": batch_id},
                )

    completed_batches = completed_batches[-50:]
    rejected_batches = rejected_batches[-50:]
    if len(probe_batches) > 20:
        keep = list(probe_batches)[-20:]
        probe_batches = {key: probe_batches[key] for key in keep}
    tuning.update(
        {
            "updated_at": utc_now(),
            "concurrency_policy": "adaptive",
            "initial_concurrency": initial_concurrency,
            "current_concurrency": desired,
            "last_mode": mode,
            "last_verdict": verdict,
            "last_estimate": estimate,
            "last_reported_estimate": reported_estimate,
            "last_observed_concurrency": observed,
            "last_claim_batch_id": batch_id,
            "probe_limit": theoretical + 10,
            "probe_batches": probe_batches,
            "completed_probe_batches": completed_batches,
            "rejected_probe_batches": rejected_batches,
        }
    )
    conn.execute(
        "UPDATE workers SET desired_concurrency=?, tuning_json=?, updated_at=? WHERE worker_id=?",
        (desired, json.dumps(tuning, ensure_ascii=False), utc_now(), worker_id),
    )


def score_result_zip(path: Path) -> tuple[str, dict[str, Any]]:
    summary: dict[str, Any] = {"source": "zip", "files": []}
    verdict = "run_error"
    try:
        with zipfile.ZipFile(path, "r") as z:
            names = z.namelist()
            summary["files"] = names[:200]
            diagnostic_text = ""
            for diag_name in sorted(n for n in names if n.endswith(("stdout.txt", "stderr.txt"))):
                try:
                    diagnostic_text += "\n" + z.read(diag_name).decode("utf-8-sig", errors="replace")[:12000]
                except Exception:
                    continue
            worker_result_names = [n for n in names if n.endswith("worker-result.json")]
            artifact_summary_names = [n for n in names if n.endswith("artifact-summary.json")]
            artifact_manifest_names = [n for n in names if n.endswith("artifact-manifest.json")]
            phase_result_names = [n for n in names if n.endswith("phase-results.json")]
            scheduler_names = [n for n in names if n.endswith("case-scheduler-summary.json")]
            score_names = [n for n in names if n.endswith("score-summary.json")]
            scored = False
            if worker_result_names:
                data = json.loads(z.read(worker_result_names[0]).decode("utf-8-sig"))
                summary["worker_result"] = data
                if data.get("verdict"):
                    verdict = str(data["verdict"])
                else:
                    verdict = "clean" if int(data.get("exit_code") or 0) == 0 else "run_error"
                scored = True
            if artifact_summary_names:
                summary["artifact_summary"] = json.loads(z.read(sorted(artifact_summary_names)[0]).decode("utf-8-sig"))
            if artifact_manifest_names:
                summary["artifact_manifest"] = json.loads(z.read(sorted(artifact_manifest_names)[0]).decode("utf-8-sig"))
            if phase_result_names:
                summary["phase_results"] = json.loads(z.read(sorted(phase_result_names)[0]).decode("utf-8-sig"))
            if not scored and scheduler_names:
                data = json.loads(z.read(sorted(scheduler_names)[0]).decode("utf-8-sig"))
                summary["case_scheduler_summary"] = data
                counts = {"pass": 0, "fail": 0, "needs_review": 0, "run_error": 0}
                for attempt in data.get("attempts", []):
                    counts["pass"] += int(attempt.get("pass_count") or 0)
                    counts["fail"] += int(attempt.get("fail_count") or 0)
                    counts["needs_review"] += int(attempt.get("needs_review_count") or 0)
                    counts["run_error"] += int(attempt.get("run_error_count") or 0)
                summary["counts"] = counts
                if counts["run_error"]:
                    verdict = "run_error"
                elif counts["needs_review"]:
                    verdict = "needs_review"
                elif counts["fail"]:
                    verdict = "dirty"
                elif counts["pass"]:
                    verdict = "clean"
                scored = True
            elif not scored and score_names:
                data = json.loads(z.read(sorted(score_names)[0]).decode("utf-8-sig"))
                summary["score_summary"] = data
                if int(data.get("run_error_count") or 0):
                    verdict = "run_error"
                elif int(data.get("needs_review_count") or 0):
                    verdict = "needs_review"
                elif int(data.get("fail_count") or 0):
                    verdict = "dirty"
                else:
                    verdict = "clean"
                scored = True
            if not scored:
                verdict = "clean"
            issues = classify_issue_text(diagnostic_text) if verdict != "clean" else []
            if issues:
                summary["issues"] = sorted(set(issues))
    except Exception as exc:
        summary["error"] = str(exc)
        summary["issues"] = classify_issue_text(str(exc)) or ["result_import_error"]
        verdict = "run_error"
    return verdict, summary


class ControllerServer(ThreadingHTTPServer):
    benchmark_root: Path
    db_path: Path
    artifact_root: Path
    control_log_path: Path | None
    upload_lock: threading.Lock
    auth_token: str | None
    runner_token_env: str


class Handler(BaseHTTPRequestHandler):
    server: ControllerServer

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path: Path, filename: str) -> None:
        if not path.is_file():
            self._json(404, {"error": "result_file_not_found"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(path.stat().st_size))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        with path.open("rb") as f:
            shutil.copyfileobj(f, self.wfile, length=1024 * 1024)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8-sig"))

    def _require_auth(self) -> bool:
        if token_matches(self.headers, getattr(self.server, "auth_token", None)):
            return True
        self._json(401, {"error": "unauthorized"})
        return False

    def _db(self) -> sqlite3.Connection:
        return connect(self.server.db_path)

    def do_GET(self) -> None:
        if not self._require_auth():
            return
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse_qs(parsed.query)
        try:
            if path == "/api/healthz":
                self._json(
                    200,
                    {
                        "ok": True,
                        "time": utc_now(),
                        "core_preview_version": CORE_PREVIEW_VERSION,
                        "hub_api_version": HUB_API_VERSION,
                        "capabilities": metadata("hub")["capabilities"],
                    },
                )
                return
            if path == "/api/meta":
                self._json(
                    200,
                    {
                        **metadata("hub"),
                        "auth_required": bool(getattr(self.server, "auth_token", None)),
                        "runner_token_env": self.server.runner_token_env,
                    },
                )
                return
            if path == "/api/catalog/tasks":
                cases = load_registry_cases(self.server.benchmark_root)
                case_filter = set(split_csv(qs.get("case_id", [""])[0]))
                if case_filter:
                    cases = [c for c in cases if c["case_id"] in case_filter]
                self._json(200, {"tasks": cases})
                return
            if path.startswith("/api/catalog/components/"):
                case_id = path.split("/")[-1]
                cases = {c["case_id"]: c for c in load_registry_cases(self.server.benchmark_root)}
                if case_id not in cases:
                    self._json(404, {"error": "unknown_case", "case_id": case_id})
                    return
                self._json(200, case_components(self.server.benchmark_root, cases[case_id]))
                return
            if path.startswith("/api/results/"):
                result_id = path.split("/")[-1]
                with self._db() as conn:
                    row = conn.execute(
                        "SELECT result_id,task_id,path FROM result_packages WHERE result_id=?",
                        (result_id,),
                    ).fetchone()
                if row is None:
                    self._json(404, {"error": "result_not_found", "result_id": result_id})
                    return
                self._file(Path(row["path"]), f"{row['task_id']}__{row['result_id']}.zip")
                return
            if path == "/api/tasks":
                limit = max(1, min(int(qs.get("limit", ["100"])[0]), 500))
                offset = max(0, int(qs.get("offset", ["0"])[0]))
                params: list[Any] = []
                clauses: list[str] = []
                for field in (
                    "state",
                    "task_id",
                    "case_id",
                    "run_id",
                    "setting_id",
                    "package_id",
                    "lease_worker_id",
                    "attempt_no",
                    "result_id",
                ):
                    values = split_csv(qs.get(field, [""])[0])
                    if values:
                        clauses.append(f"{field} IN ({','.join('?' for _ in values)})")
                        params.extend(values)
                where = "WHERE " + " AND ".join(clauses) if clauses else ""
                with self._db() as conn:
                    total = int(conn.execute(f"SELECT COUNT(*) FROM tasks {where}", params).fetchone()[0])
                    rows = [
                        row_dict(r)
                        for r in conn.execute(
                            f"SELECT * FROM tasks {where} ORDER BY priority DESC, created_at LIMIT ? OFFSET ?",
                            [*params, limit, offset],
                        )
                    ]
                next_offset = offset + len(rows)
                self._json(
                    200,
                    {
                        "tasks": rows,
                        "total": total,
                        "offset": offset,
                        "next_offset": next_offset if next_offset < total else None,
                    },
                )
                return
            if path == "/api/data/task-counts":
                with self._db() as conn:
                    rows = conn.execute("SELECT state, COUNT(*) AS count FROM tasks GROUP BY state ORDER BY state").fetchall()
                by_state = {str(row["state"]): int(row["count"]) for row in rows}
                total = sum(by_state.values())
                final = sum(count for state, count in by_state.items() if state in FINAL_STATES)
                self._json(200, {"total": total, "final": final, "pending": total - final, "by_state": by_state})
                return
            if path == "/api/data/active-workers":
                max_age = int(qs.get("max_age_seconds", ["120"])[0])
                cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=max_age)).isoformat()
                with self._db() as conn:
                    rows = [
                        row_dict(r)
                        for r in conn.execute(
                            """
                            SELECT * FROM workers
                            WHERE blocked=0 AND last_seen_at>=?
                            ORDER BY last_seen_at DESC
                            """,
                            (cutoff,),
                        )
                    ]
                self._json(200, {"workers": rows, "cutoff": cutoff})
                return
            if path == "/api/data/worker-capacity":
                with self._db() as conn:
                    rows = conn.execute(
                        "SELECT * FROM workers WHERE blocked=0 ORDER BY worker_id"
                    ).fetchall()
                    workers = []
                    for worker in rows:
                        capacity = worker_resource_capacity(worker)
                        reservation = worker_reservation_state(conn, str(worker["worker_id"]))
                        workers.append(
                            {
                                "worker_id": worker["worker_id"],
                                "status": worker["status"],
                                "capacity": capacity,
                                "reserved": reservation["reserved"],
                                "available": remaining_resources(capacity, reservation["reserved"]),
                                "active_task_ids": reservation["task_ids"],
                                "exclusive_active": reservation["exclusive_active"],
                                "max_concurrency": int(worker["max_concurrency"] or 1),
                                "desired_concurrency": int(worker["desired_concurrency"] or 1),
                            }
                        )
                self._json(200, {"workers": workers})
                return
            if path == "/api/data/task-admission":
                task_id = str(qs.get("task_id", [""])[0]).strip()
                if not task_id:
                    self._json(400, {"error": "task_id_required"})
                    return
                with self._db() as conn:
                    task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                    if task is None:
                        self._json(404, {"error": "task_not_found", "task_id": task_id})
                        return
                    candidates = []
                    for worker in conn.execute("SELECT * FROM workers ORDER BY worker_id"):
                        worker_id = str(worker["worker_id"])
                        if int(worker["blocked"] or 0):
                            candidates.append({"worker_id": worker_id, "ok": False, "reason": "worker_blocked"})
                            continue
                        capabilities = json.loads(worker["capabilities_json"] or "[]")
                        if not worker_can_run(task, capabilities, worker_id):
                            candidates.append({"worker_id": worker_id, "ok": False, "reason": "worker_not_eligible"})
                            continue
                        admission = worker_resource_admission(conn, worker, task)
                        candidates.append({"worker_id": worker_id, **admission})
                self._json(
                    200,
                    {
                        "task_id": task_id,
                        "execution_profile": task_execution_profile(task),
                        "eligible_worker_count": sum(1 for candidate in candidates if candidate["ok"]),
                        "workers": candidates,
                    },
                )
                return
            if path == "/api/data/running-tasks":
                with self._db() as conn:
                    rows = [
                        row_dict(r)
                        for r in conn.execute(
                            """
                            SELECT * FROM tasks
                            WHERE state IN ('leased','running','uploaded','imported','scored')
                            ORDER BY updated_at DESC
                            """
                        )
                    ]
                self._json(200, {"tasks": rows})
                return
            if path == "/api/data/new-results":
                cursor = int(qs.get("cursor", ["0"])[0])
                limit = min(int(qs.get("limit", ["100"])[0]), 500)
                clauses = ["cursor>?"]
                params: list[Any] = [cursor]
                filters: dict[str, list[str]] = {}
                for field in ("task_id", "worker_id", "attempt_no", "verdict", "result_id"):
                    values = split_csv(qs.get(field, [""])[0])
                    if values:
                        filters[field] = values
                        clauses.append(f"{field} IN ({','.join('?' for _ in values)})")
                        params.extend(values)
                params.append(limit)
                with self._db() as conn:
                    rows = [
                        row_dict(r)
                        for r in conn.execute(
                            f"SELECT * FROM result_packages WHERE {' AND '.join(clauses)} ORDER BY cursor LIMIT ?",
                            params,
                        )
                    ]
                    next_cursor = rows[-1]["cursor"] if rows else cursor
                self._json(200, {"cursor": cursor, "next_cursor": next_cursor, "filters": filters, "results": rows})
                return
            if path == "/api/data/error-rate":
                group_by = qs.get("by", ["state"])[0]
                if group_by not in QUERY_FIELDS:
                    self._json(400, {"error": "unsupported_group_by", "allowed": sorted(QUERY_FIELDS)})
                    return
                window_seconds = int(qs.get("window_seconds", ["86400"])[0])
                cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=window_seconds)).isoformat()
                group_column = QUERY_FIELD_SQL[group_by]
                sql = f"""
                    SELECT COALESCE({group_column}, '') AS bucket,
                           COUNT(*) AS total,
                           SUM(CASE WHEN state IN ('run_error','needs_review') THEN 1 ELSE 0 END) AS errors
                    FROM tasks
                    WHERE updated_at>=?
                    GROUP BY COALESCE({group_column}, '')
                    ORDER BY errors DESC, total DESC
                """
                with self._db() as conn:
                    rows = []
                    for r in conn.execute(sql, (cutoff,)):
                        total = int(r["total"] or 0)
                        errors = int(r["errors"] or 0)
                        rows.append({"bucket": r["bucket"], "total": total, "errors": errors, "error_rate": errors / total if total else 0})
                self._json(200, {"by": group_by, "window_seconds": window_seconds, "rows": rows})
                return
            if path == "/api/data/control-log":
                limit = min(int(qs.get("limit", ["100"])[0]), 500)
                level = qs.get("level", [""])[0].strip()
                params: list[Any] = []
                where = ""
                if level:
                    where = "WHERE level=?"
                    params.append(level)
                params.append(limit)
                with self._db() as conn:
                    rows = [
                        row_dict(r)
                        for r in conn.execute(
                            f"SELECT * FROM control_logs {where} ORDER BY log_seq DESC LIMIT ?",
                            params,
                        )
                    ]
                self._json(200, {"logs": rows})
                return
            if path == "/api/data/worker-hosts":
                mode = qs.get("connection_mode", [""])[0].strip()
                params: list[Any] = []
                where = ""
                if mode:
                    where = "WHERE connection_mode=?"
                    params.append(mode)
                with self._db() as conn:
                    rows = [
                        row_dict(r)
                        for r in conn.execute(
                            f"SELECT * FROM worker_hosts {where} ORDER BY updated_at DESC, host_id",
                            params,
                        )
                    ]
                self._json(200, {"hosts": rows})
                return
            self._json(404, {"error": "not_found", "path": parsed.path})
        except Exception as exc:
            self._json(500, {"error": type(exc).__name__, "detail": str(exc)})

    def do_POST(self) -> None:
        if not self._require_auth():
            return
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        try:
            if path == "/api/tasks/dispatch":
                self._dispatch_tasks()
                return
            if path == "/api/workers/register":
                self._register_worker()
                return
            if path == "/api/workers/heartbeat":
                self._heartbeat()
                return
            if path == "/api/tasks/claim":
                self._claim_task()
                return
            if path == "/api/tasks/start":
                self._task_state_from_worker("running", "task_started")
                return
            if path == "/api/tasks/renew":
                self._renew_lease()
                return
            if path == "/api/tasks/complete":
                self._task_state_from_worker("returned", "task_completed")
                return
            if path == "/api/tasks/fail":
                self._fail_task()
                return
            if path == "/api/results/upload":
                self._upload_result()
                return
            if path == "/api/data/query-results":
                self._query_results()
                return
            if path == "/api/admin/override-state":
                self._admin_override()
                return
            if path == "/api/admin/cancel-task":
                self._admin_simple_state("cancelled", "task_cancelled")
                return
            if path == "/api/admin/retry-task":
                self._admin_retry()
                return
            if path == "/api/admin/expire-leases":
                self._expire_leases()
                return
            if path == "/api/admin/block-worker":
                self._block_worker(True)
                return
            if path == "/api/admin/unblock-worker":
                self._block_worker(False)
                return
            if path == "/api/admin/set-worker-concurrency":
                self._set_worker_concurrency()
                return
            if path == "/api/admin/push-task":
                self._push_task()
                return
            if path == "/api/admin/register-worker-hosts":
                self._register_worker_hosts()
                return
            self._json(404, {"error": "not_found", "path": parsed.path})
        except Exception as exc:
            self._json(500, {"error": type(exc).__name__, "detail": str(exc)})

    def _dispatch_tasks(self) -> None:
        payload = self._read_json()
        try:
            schema_version = int(payload.get("schema_version"))
        except (TypeError, ValueError):
            self._json(400, {"error": "dispatch_requires_schema_version", "expected": DISPATCH_SCHEMA_VERSION})
            return
        if schema_version != DISPATCH_SCHEMA_VERSION:
            self._json(
                400,
                {
                    "error": "unsupported_dispatch_schema_version",
                    "expected": DISPATCH_SCHEMA_VERSION,
                    "received": schema_version,
                },
            )
            return
        cases = {c["case_id"]: c for c in load_registry_cases(self.server.benchmark_root)}
        task_specs = payload.get("tasks")
        if task_specs is None:
            task_specs = []
            for case_id in payload.get("case_ids", []):
                task_specs.append({"case_id": case_id})
        if not task_specs:
            task_specs = [{}]
        try:
            for index, spec in enumerate(task_specs, start=1):
                if not isinstance(spec, dict):
                    raise ValueError(f"tasks[{index}] must be an object")
                merged_dispatch_extensions(payload, spec)
        except ValueError as exc:
            self._json(400, {"error": "invalid_task_extensions", "detail": str(exc)})
            return
        created: list[dict[str, Any]] = []
        existing: list[dict[str, Any]] = []
        now = utc_now()
        with self._db() as conn:
            for spec in task_specs:
                task_id = spec.get("task_id") or "task-" + str(uuid.uuid4())
                case_id = spec.get("case_id")
                case = cases.get(case_id or "", {})
                components = spec.get("components") or (case_components(self.server.benchmark_root, case) if case else {})
                dispatch_payload = payload.get("payload") or {}
                spec_payload = spec.get("payload") or {}
                task_payload = dict(dispatch_payload)
                task_payload.update(spec_payload)
                extensions = merged_dispatch_extensions(payload, spec)
                if extensions is not None:
                    task_payload["extensions"] = extensions
                execution_profile = spec.get("execution_profile", task_payload.get("execution_profile"))
                if execution_profile is not None:
                    task_payload["execution_profile"] = normalize_execution_profile(execution_profile)
                normalized = task_payload.get("normalized") if isinstance(task_payload.get("normalized"), dict) else {}
                run_id = spec.get("run_id") or normalized.get("run_id")
                setting_id = spec.get("setting_id") or normalized.get("setting_id")
                package_id = spec.get("package_id") or payload.get("package_id") or task_payload.get("package_id") or f"pkg-{task_id}"
                priority = int(spec.get("priority", payload.get("priority", 0)) or 0)
                case_version = spec.get("case_version") or case.get("case_version")
                scenario_id = spec.get("scenario_id") or case.get("scenario_id")
                required_capability = spec.get("required_capability") or payload.get("required_capability")
                task_config = {
                    "priority": priority,
                    "case_id": case_id,
                    "run_id": run_id,
                    "setting_id": setting_id,
                    "case_version": case_version,
                    "scenario_id": scenario_id,
                    "package_id": package_id,
                    "payload": task_payload,
                    "components": components,
                    "required_capability": required_capability,
                }
                row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                if row is not None:
                    existing_config = {
                        "priority": int(row["priority"] or 0),
                        "case_id": row["case_id"],
                        "run_id": row["run_id"],
                        "setting_id": row["setting_id"],
                        "case_version": row["case_version"],
                        "scenario_id": row["scenario_id"],
                        "package_id": row["package_id"],
                        "payload": json.loads(row["payload_json"] or "{}"),
                        "components": json.loads(row["components_json"] or "{}"),
                        "required_capability": row["required_capability"],
                    }
                    if existing_config != task_config:
                        raise ValueError(f"task_id_conflict:{task_id}")
                    existing.append(
                        {
                            "task_id": task_id,
                            "case_id": row["case_id"],
                            "run_id": row["run_id"],
                            "setting_id": row["setting_id"],
                            "state": row["state"],
                        }
                    )
                    continue
                conn.execute(
                    """
                    INSERT INTO tasks(
                      task_id, state, priority, case_id, run_id, setting_id, case_version, scenario_id, package_id,
                      payload_json, components_json, required_capability, created_at, updated_at
                    ) VALUES (?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        priority,
                        case_id,
                        run_id,
                        setting_id,
                        case_version,
                        scenario_id,
                        package_id,
                        json.dumps(task_payload, ensure_ascii=False),
                        json.dumps(components, ensure_ascii=False),
                        required_capability,
                        now,
                        now,
                    ),
                )
                log_task_event(conn, task_id, "task_dispatched", "user", str(payload.get("operator") or "local"), None, "queued", spec)
                created.append({"task_id": task_id, "case_id": case_id, "run_id": run_id, "setting_id": setting_id, "state": "queued"})
            conn.commit()
        self._json(201 if created else 200, {"created": created, "existing": existing})

    def _register_worker(self) -> None:
        payload = self._read_json()
        try:
            runner_api_version = int(payload.get("runner_api_version"))
        except (TypeError, ValueError):
            self._json(400, {"error": "runner_api_version_required", "expected": RUNNER_API_VERSION})
            return
        if runner_api_version != RUNNER_API_VERSION:
            self._json(400, {"error": "unsupported_runner_api_version", "expected": RUNNER_API_VERSION, "received": runner_api_version})
            return
        worker_id = str(payload.get("worker_id") or "worker-" + str(uuid.uuid4()))
        capabilities = payload.get("capabilities") or []
        try:
            max_concurrency = int(payload.get("max_concurrency") or 1)
            initial_concurrency = int(payload.get("initial_concurrency") or 1)
        except (TypeError, ValueError):
            self._json(400, {"error": "concurrency_values_must_be_integers"})
            return
        if max_concurrency < 1 or initial_concurrency < 1 or initial_concurrency > max_concurrency:
            self._json(400, {"error": "initial_concurrency_must_be_between_one_and_max_concurrency"})
            return
        concurrency_policy = str(payload.get("concurrency_policy") or "fixed").strip().lower()
        if concurrency_policy not in CONCURRENCY_POLICIES:
            self._json(400, {"error": "unsupported_concurrency_policy", "allowed": sorted(CONCURRENCY_POLICIES)})
            return
        health = payload.get("health") if isinstance(payload.get("health"), dict) else {}
        try:
            resource_capacity = normalize_capacity(payload.get("resource_capacity"), snapshot=json_object(health.get("resources")))
        except ValueError as exc:
            self._json(400, {"error": "invalid_resource_capacity", "detail": str(exc)})
            return
        desired_concurrency = initial_concurrency
        now = utc_now()
        with self._db() as conn:
            conn.execute(
                """
                INSERT INTO workers(
                  worker_id, status, capabilities_json, max_concurrency, initial_concurrency, concurrency_policy, desired_concurrency,
                  registered_at, last_seen_at, updated_at, health_json, resource_json, resource_capacity_json, tuning_json
                )
                VALUES (?, 'registered', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                  status='registered',
                  capabilities_json=excluded.capabilities_json,
                  max_concurrency=excluded.max_concurrency,
                  initial_concurrency=excluded.initial_concurrency,
                  concurrency_policy=excluded.concurrency_policy,
                  desired_concurrency=excluded.initial_concurrency,
                  last_seen_at=excluded.last_seen_at,
                  updated_at=excluded.updated_at,
                  health_json=excluded.health_json,
                  resource_json=excluded.resource_json,
                  resource_capacity_json=excluded.resource_capacity_json,
                  tuning_json=excluded.tuning_json
                """,
                (
                    worker_id,
                    json.dumps(capabilities, ensure_ascii=False),
                    max_concurrency,
                    initial_concurrency,
                    concurrency_policy,
                    desired_concurrency,
                    now,
                    now,
                    now,
                    json.dumps(health, ensure_ascii=False),
                    json.dumps(health.get("resources") or {}, ensure_ascii=False),
                    json.dumps(resource_capacity, ensure_ascii=False),
                    json.dumps(
                        {
                            "concurrency_policy": concurrency_policy,
                            "initial_concurrency": initial_concurrency,
                            "current_concurrency": desired_concurrency,
                            "resource_capacity": resource_capacity,
                            "updated_at": now,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            log_worker_event(conn, worker_id, "registered", payload)
            log_control(
                conn,
                self.server,
                "info",
                "worker_registered",
                f"worker {worker_id} registered with initial={initial_concurrency}, max={max_concurrency}, policy={concurrency_policy}",
                worker_id=worker_id,
                payload={
                    "capabilities": capabilities,
                    "initial_concurrency": initial_concurrency,
                    "desired_concurrency": desired_concurrency,
                    "max_concurrency": max_concurrency,
                    "concurrency_policy": concurrency_policy,
                    "resource_capacity": resource_capacity,
                },
            )
            conn.commit()
        self._json(
            200,
            {
                "ok": True,
                "worker_id": worker_id,
                "initial_concurrency": initial_concurrency,
                "desired_concurrency": desired_concurrency,
                "max_concurrency": max_concurrency,
                "concurrency_policy": concurrency_policy,
                "resource_capacity": resource_capacity,
            },
        )

    def _heartbeat(self) -> None:
        payload = self._read_json()
        worker_id = str(payload.get("worker_id") or "")
        if not worker_id:
            self._json(400, {"error": "worker_id_required"})
            return
        now = utc_now()
        with self._db() as conn:
            conn.execute(
                """
                UPDATE workers
                SET status='alive', last_seen_at=?, updated_at=?, active_runs_json=?, health_json=?, resource_json=?
                WHERE worker_id=?
                """,
                (
                    now,
                    now,
                    json.dumps(payload.get("current_runs") or [], ensure_ascii=False),
                    json.dumps(payload.get("health") or {}, ensure_ascii=False),
                    json.dumps((payload.get("health") or {}).get("resources") or {}, ensure_ascii=False),
                    worker_id,
                ),
            )
            log_worker_event(conn, worker_id, "heartbeat", payload)
            worker = conn.execute(
                "SELECT initial_concurrency,concurrency_policy,desired_concurrency,max_concurrency,resource_capacity_json FROM workers WHERE worker_id=?",
                (worker_id,),
            ).fetchone()
            conn.commit()
        self._json(
            200,
            {
                "ok": True,
                "worker_id": worker_id,
                "time": now,
                "initial_concurrency": int(worker["initial_concurrency"] or 1) if worker else 1,
                "desired_concurrency": int(worker["desired_concurrency"] or 1) if worker else 1,
                "max_concurrency": int(worker["max_concurrency"] or 1) if worker else 1,
                "concurrency_policy": str(worker["concurrency_policy"] or "fixed") if worker else "fixed",
                "resource_capacity": json_object(worker["resource_capacity_json"]) if worker else {},
            },
        )

    def _claim_once(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        worker_id = str(payload.get("worker_id") or "")
        if not worker_id:
            return 400, {"error": "worker_id_required"}
        limit = max(1, min(int(payload.get("limit") or 1), 1024))
        lease_seconds = max(30, int(payload.get("lease_seconds") or 600))
        now_dt = dt.datetime.now(dt.timezone.utc)
        lease_until = (now_dt + dt.timedelta(seconds=lease_seconds)).isoformat()
        claimed: list[dict[str, Any]] = []
        with self._db() as conn:
            conn.execute("BEGIN IMMEDIATE")
            worker = conn.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if worker is None or int(worker["blocked"] or 0):
                conn.rollback()
                return 403, {"error": "worker_not_available"}
            capabilities = json.loads(worker["capabilities_json"] or "[]")
            concurrency = worker_concurrency_state(conn, worker)
            active_count = concurrency["active_count"]
            max_concurrency = concurrency["max_concurrency"]
            initial_concurrency = max(1, min(int(worker["initial_concurrency"] or 1), max_concurrency))
            concurrency_policy = str(worker["concurrency_policy"] or "fixed")
            desired_concurrency = concurrency["desired_concurrency"]
            controller_limit = max(0, concurrency["effective_limit"] - active_count)
            limit = min(limit, controller_limit)
            if limit <= 0:
                conn.commit()
                return 200, {
                    "tasks": [],
                    "desired_concurrency": desired_concurrency,
                    "max_concurrency": max_concurrency,
                    "initial_concurrency": initial_concurrency,
                    "concurrency_policy": concurrency_policy,
                    "active_count": active_count,
                }
            rows = conn.execute(
                """
                SELECT * FROM tasks
                WHERE state IN ('queued','retry_queued')
                ORDER BY priority DESC, created_at
                LIMIT 100
                """
            ).fetchall()
            for task in rows:
                if len(claimed) >= limit:
                    break
                if not worker_can_run(task, capabilities, worker_id):
                    continue
                resource_admission = worker_resource_admission(conn, worker, task)
                if not resource_admission["ok"]:
                    continue
                conn.execute(
                    """
                    UPDATE tasks
                    SET state='leased', lease_worker_id=?, lease_until=?, updated_at=?
                    WHERE task_id=? AND state IN ('queued','retry_queued')
                    """,
                    (worker_id, lease_until, utc_now(), task["task_id"]),
                )
                log_task_event(conn, task["task_id"], "task_claimed", "worker", worker_id, task["state"], "leased", {"lease_until": lease_until})
                claimed.append(
                    {
                        "task_id": task["task_id"],
                        "lease_until": lease_until,
                        "case_id": task["case_id"],
                        "run_id": task["run_id"],
                        "setting_id": task["setting_id"],
                        "case_version": task["case_version"],
                        "scenario_id": task["scenario_id"],
                        "package_id": task["package_id"],
                        "attempt_no": int(task["attempt_no"] or 1),
                        "assigned_worker_id": worker_id,
                        "payload": json.loads(task["payload_json"] or "{}"),
                        "components": json.loads(task["components_json"] or "{}"),
                        "controller_concurrency": {
                            "desired_concurrency": desired_concurrency,
                            "max_concurrency": max_concurrency,
                            "initial_concurrency": initial_concurrency,
                            "concurrency_policy": concurrency_policy,
                            "active_count_at_claim": active_count,
                            "resource_admission": resource_admission,
                        },
                    }
                )
            if claimed:
                claim_batch_id = "claim-" + str(uuid.uuid4())
                claim_batch_size = len(claimed)
                for item in claimed:
                    item["controller_concurrency"].update(
                        {
                            "claim_batch_id": claim_batch_id,
                            "claim_batch_size": claim_batch_size,
                        }
                    )
            conn.commit()
            reservation = worker_reservation_state(conn, worker_id)
            capacity = worker_resource_capacity(worker)
        return 200, {
            "tasks": claimed,
            "desired_concurrency": desired_concurrency,
            "max_concurrency": max_concurrency,
            "initial_concurrency": initial_concurrency,
            "concurrency_policy": concurrency_policy,
            "active_count": active_count,
            "resource_capacity": capacity,
            "reserved_resources": reservation["reserved"],
            "available_resources": remaining_resources(capacity, reservation["reserved"]),
        }

    def _claim_task(self) -> None:
        payload = self._read_json()
        wait_seconds = max(0.0, min(float(payload.get("wait_seconds") or 0), 60.0))
        poll_seconds = max(0.2, min(float(payload.get("wait_poll_seconds") or 1.0), 5.0))
        deadline = time.monotonic() + wait_seconds
        while True:
            status, response = self._claim_once(payload)
            if status != 200 or response.get("tasks") or time.monotonic() >= deadline:
                self._json(status, response)
                return
            time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

    def _lease_task_for_worker(
        self,
        *,
        task_id: str,
        worker_id: str,
        lease_seconds: int,
        operator: str,
    ) -> tuple[int, dict[str, Any]]:
        lease_seconds = max(30, int(lease_seconds or 600))
        lease_until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=lease_seconds)).isoformat()
        with self._db() as conn:
            conn.execute("BEGIN IMMEDIATE")
            worker = conn.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if worker is None or int(worker["blocked"] or 0):
                conn.rollback()
                return 403, {"error": "worker_not_available", "worker_id": worker_id}
            task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                conn.rollback()
                return 404, {"error": "task_not_found", "task_id": task_id}
            if task["state"] not in {"queued", "retry_queued"}:
                conn.rollback()
                return 409, {"error": "task_not_queueable", "task_id": task_id, "state": task["state"]}
            capabilities = json.loads(worker["capabilities_json"] or "[]")
            if not worker_can_run(task, capabilities, worker_id):
                conn.rollback()
                return 409, {"error": "worker_not_eligible", "task_id": task_id, "worker_id": worker_id}
            resource_admission = worker_resource_admission(conn, worker, task)
            if not resource_admission["ok"]:
                conn.commit()
                if resource_admission["reason"] == "worker_concurrency_reached":
                    concurrency = resource_admission["concurrency"]
                    return 429, {
                        "error": "worker_capacity_reached",
                        "worker_id": worker_id,
                        "active_count": concurrency["active_count"],
                        "desired_concurrency": concurrency["desired_concurrency"],
                        "max_concurrency": concurrency["max_concurrency"],
                        "initial_concurrency": max(1, min(int(worker["initial_concurrency"] or 1), concurrency["max_concurrency"])),
                        "concurrency_policy": str(worker["concurrency_policy"] or "fixed"),
                    }
                return 409, {
                    "error": "worker_resource_unavailable",
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "admission": resource_admission,
                }
            concurrency = resource_admission["concurrency"]
            desired_concurrency = concurrency["desired_concurrency"]
            max_concurrency = concurrency["max_concurrency"]
            initial_concurrency = max(1, min(int(worker["initial_concurrency"] or 1), max_concurrency))
            concurrency_policy = str(worker["concurrency_policy"] or "fixed")
            active_count = concurrency["active_count"]
            now = utc_now()
            conn.execute(
                """
                UPDATE tasks
                SET state='leased', lease_worker_id=?, lease_until=?, updated_at=?
                WHERE task_id=? AND state IN ('queued','retry_queued')
                """,
                (worker_id, lease_until, now, task_id),
            )
            push_id = "push-" + str(uuid.uuid4())
            log_task_event(
                conn,
                task_id,
                "task_leased_for_push",
                "controller",
                operator,
                task["state"],
                "leased",
                {"worker_id": worker_id, "lease_until": lease_until, "push_id": push_id},
            )
            conn.commit()
        return 200, {
            "task_id": task["task_id"],
            "lease_until": lease_until,
            "case_id": task["case_id"],
            "run_id": task["run_id"],
            "setting_id": task["setting_id"],
            "case_version": task["case_version"],
            "scenario_id": task["scenario_id"],
            "package_id": task["package_id"],
            "attempt_no": int(task["attempt_no"] or 1),
            "assigned_worker_id": worker_id,
            "payload": json.loads(task["payload_json"] or "{}"),
            "components": json.loads(task["components_json"] or "{}"),
            "controller_concurrency": {
                "desired_concurrency": desired_concurrency,
                "max_concurrency": max_concurrency,
                "initial_concurrency": initial_concurrency,
                "concurrency_policy": concurrency_policy,
                "active_count_at_claim": active_count,
                "dispatch_mode": "direct-push",
                "resource_admission": resource_admission,
            },
            "controller_push": {"push_id": push_id, "leased_at": now},
        }

    def _direct_worker_endpoint(self, worker_id: str) -> tuple[str, str] | None:
        with self._db() as conn:
            row = conn.execute(
                "SELECT connection_mode,endpoint_json FROM worker_hosts WHERE worker_id=? ORDER BY updated_at DESC LIMIT 1",
                (worker_id,),
            ).fetchone()
        if row is None or str(row["connection_mode"] or "") != "direct-worker-api":
            return None
        endpoint = json.loads(row["endpoint_json"] or "{}")
        url = str(endpoint.get("worker_url") or "").strip().rstrip("/")
        if not url:
            host = endpoint.get("private_host") or endpoint.get("host")
            port = int(endpoint.get("command_port") or 9876)
            if not host:
                return None
            url = f"http://{host}:{port}"
        token_env = str(endpoint.get("direct_api_token_env") or self.server.runner_token_env or DEFAULT_RUNNER_TOKEN_ENV)
        return url, token_env

    def _push_task(self) -> None:
        payload = self._read_json()
        task_id = str(payload.get("task_id") or "")
        worker_id = str(payload.get("worker_id") or "")
        operator = str(payload.get("operator") or "direct-push")
        if not task_id or not worker_id:
            self._json(400, {"error": "task_id_and_worker_id_required"})
            return
        endpoint = self._direct_worker_endpoint(worker_id)
        if endpoint is None:
            self._json(409, {"error": "direct_worker_endpoint_not_registered", "worker_id": worker_id})
            return
        url, token_env = endpoint
        parsed = urlparse(url)
        runner_token = token_from_env(token_env)
        if not runner_token and not is_loopback_host(parsed.hostname or ""):
            self._json(400, {"error": "runner_token_missing", "worker_id": worker_id, "token_env": token_env})
            return
        status, task = self._lease_task_for_worker(
            task_id=task_id,
            worker_id=worker_id,
            lease_seconds=int(payload.get("lease_seconds") or 600),
            operator=operator,
        )
        if status != 200:
            self._json(status, task)
            return
        try:
            runner_response = http_request_json(
                url + "/api/tasks/execute",
                {"task": task},
                token=runner_token,
                timeout=max(10, min(int(payload.get("push_timeout_seconds") or 30), 120)),
            )
        except Exception as exc:
            # The request may have reached the Runner even when its response did not.
            # Keep the lease to prevent a duplicate execution; normal lease expiry recovers it.
            with self._db() as conn:
                log_control(
                    conn,
                    self.server,
                    "warning",
                    "push_delivery_unknown",
                    f"push delivery for task {task_id} to worker {worker_id} has unknown outcome",
                    worker_id=worker_id,
                    task_id=task_id,
                    payload={"url": url, "error": f"{type(exc).__name__}: {exc}"},
                )
                conn.commit()
            self._json(
                502,
                {
                    "error": "push_delivery_unknown",
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "state": "leased",
                    "lease_until": task["lease_until"],
                },
            )
            return
        with self._db() as conn:
            log_control(
                conn,
                self.server,
                "info",
                "task_pushed",
                f"controller pushed leased task {task_id} to worker {worker_id}",
                worker_id=worker_id,
                task_id=task_id,
                payload={"url": url, "runner_response": runner_response, "attempt_no": task["attempt_no"]},
            )
            conn.commit()
        self._json(202, {"ok": True, "task_id": task_id, "worker_id": worker_id, "lease_until": task["lease_until"], "runner": runner_response})

    def _task_state_from_worker(self, new_state: str, event_type: str) -> None:
        payload = self._read_json()
        task_id = str(payload.get("task_id") or "")
        worker_id = str(payload.get("worker_id") or "")
        if not task_id or not worker_id:
            self._json(400, {"error": "task_id_and_worker_id_required"})
            return
        with self._db() as conn:
            task = conn.execute("SELECT lease_worker_id,state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                self._json(404, {"error": "task_not_found"})
                return
            if task["lease_worker_id"] != worker_id:
                self._json(409, {"error": "lease_worker_mismatch"})
                return
            if new_state == "returned" and task["state"] in FINAL_STATES:
                log_task_event(conn, task_id, event_type, "worker", worker_id, task["state"], task["state"], payload)
                conn.commit()
                self._json(200, {"ok": True, "task_id": task_id, "state": task["state"]})
                return
            changed = set_task_state(conn, task_id, new_state, "worker", worker_id, event_type, payload)
            conn.commit()
        self._json(200, {"ok": True, **changed})

    def _renew_lease(self) -> None:
        payload = self._read_json()
        task_id = str(payload.get("task_id") or "")
        worker_id = str(payload.get("worker_id") or "")
        lease_seconds = max(30, int(payload.get("lease_seconds") or 600))
        lease_until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=lease_seconds)).isoformat()
        with self._db() as conn:
            task = conn.execute("SELECT lease_worker_id,state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                self._json(404, {"error": "task_not_found"})
                return
            if task["lease_worker_id"] != worker_id:
                self._json(409, {"error": "lease_worker_mismatch"})
                return
            conn.execute("UPDATE tasks SET lease_until=?, updated_at=? WHERE task_id=?", (lease_until, utc_now(), task_id))
            log_task_event(conn, task_id, "lease_renewed", "worker", worker_id, task["state"], task["state"], {"lease_until": lease_until})
            conn.commit()
        self._json(200, {"ok": True, "task_id": task_id, "lease_until": lease_until})

    def _fail_task(self) -> None:
        payload = self._read_json()
        task_id = str(payload.get("task_id") or "")
        worker_id = str(payload.get("worker_id") or "")
        error_text = json.dumps(payload.get("error") or payload, ensure_ascii=False)
        issues = classify_issue_text(error_text) or ["run_error"]
        with self._db() as conn:
            task = conn.execute("SELECT lease_worker_id,state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                self._json(404, {"error": "task_not_found"})
                return
            if task["lease_worker_id"] != worker_id:
                self._json(
                    200,
                    {
                        "ok": True,
                        "ignored_stale_failure": True,
                        "task_id": task_id,
                        "state": task["state"],
                    },
                )
                return
            conn.execute("UPDATE tasks SET error_json=? WHERE task_id=?", (error_text, task_id))
            changed = set_task_state(conn, task_id, "run_error", "worker", worker_id, "task_failed", payload)
            worker = conn.execute("SELECT 1 FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if worker_id and worker:
                update_worker_concurrency_from_result(
                    conn,
                    self.server,
                    worker_id,
                    task_id,
                    "run_error",
                    {
                        "issues": issues,
                        "worker_result": {
                            "controller_concurrency": payload.get("controller_concurrency") or {},
                            "resource_usage": {},
                        },
                        "failure": payload,
                    },
                )
            else:
                for issue in issues:
                    log_control(
                        conn,
                        self.server,
                        "warning",
                        issue,
                        f"worker {worker_id} failed task {task_id}: {issue}",
                        worker_id=worker_id,
                        task_id=task_id,
                        payload=payload,
                    )
            auto_retry = queue_automatic_retry(conn, self.server, task_id, worker_id, issues)
            conn.commit()
        self._json(200, {"ok": True, **changed, "auto_retry": auto_retry})

    def _upload_result(self) -> None:
        task_id = self.headers.get("X-Task-Id", "").strip()
        worker_id = self.headers.get("X-Worker-Id", "").strip()
        if not task_id:
            self._json(400, {"error": "X-Task-Id required"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._json(411, {"error": "content_length_required"})
            return
        if length <= 0:
            self._json(411, {"error": "content_length_required"})
            return
        with self._db() as conn:
            task = conn.execute(
                "SELECT lease_worker_id,state,attempt_no FROM tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
        if task is None:
            self._json(404, {"error": "task_not_found"})
            return
        if worker_id and task["lease_worker_id"] != worker_id:
            self._json(409, {"error": "lease_worker_mismatch", "state": task["state"]})
            return
        attempt_no = max(1, int(task["attempt_no"] or 1))
        result_id = "result-" + str(uuid.uuid4())
        save_dir = self.server.artifact_root / "results" / task_id
        save_dir.mkdir(parents=True, exist_ok=True)
        final_path = save_dir / f"{result_id}.zip"
        digest = hashlib.sha256()
        with self.server.upload_lock:
            with final_path.open("wb") as f:
                remaining = length
                while remaining > 0:
                    chunk = self.rfile.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    f.write(chunk)
                    digest.update(chunk)
                    remaining -= len(chunk)
        verdict, summary = score_result_zip(final_path)
        now = utc_now()
        with self._db() as conn:
            conn.execute(
                """
                INSERT INTO result_packages(
                  result_id, task_id, worker_id, attempt_no, path, bytes, sha256,
                  uploaded_at, imported_at, score_state, verdict, summary_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'scored', ?, ?)
                """,
                (
                    result_id,
                    task_id,
                    worker_id,
                    attempt_no,
                    str(final_path),
                    length,
                    digest.hexdigest(),
                    now,
                    now,
                    verdict,
                    json.dumps(summary, ensure_ascii=False),
                ),
            )
            conn.execute("UPDATE tasks SET result_id=? WHERE task_id=?", (result_id, task_id))
            log_task_event(conn, task_id, "result_uploaded", "worker", worker_id, None, "uploaded", {"result_id": result_id, "bytes": length})
            set_task_state(conn, task_id, "imported", "controller", "result_importer", "result_imported", {"result_id": result_id})
            set_task_state(conn, task_id, verdict, "controller", "result_scorer", "result_scored", {"result_id": result_id, "verdict": verdict})
            if worker_id:
                update_worker_concurrency_from_result(conn, self.server, worker_id, task_id, verdict, summary)
            else:
                for issue in summary.get("issues") or []:
                    log_control(
                        conn,
                        self.server,
                        "warning",
                        str(issue),
                        f"task {task_id} result classified as {issue}",
                        worker_id=worker_id,
                        task_id=task_id,
                        payload={"result_id": result_id, "verdict": verdict},
                    )
            auto_retry = queue_automatic_retry(
                conn,
                self.server,
                task_id,
                worker_id,
                [str(value) for value in summary.get("issues") or []],
            )
            conn.commit()
        self._json(
            201,
            {
                "ok": True,
                "result_id": result_id,
                "attempt_no": attempt_no,
                "verdict": verdict,
                "path": str(final_path),
                "auto_retry": auto_retry,
            },
        )

    def _query_results(self) -> None:
        payload = self._read_json()
        filters = payload.get("filters") or {}
        group_by = payload.get("group_by") or []
        if isinstance(group_by, str):
            group_by = [group_by]
        for field in list(filters) + list(group_by):
            if field not in QUERY_FIELDS:
                self._json(400, {"error": "unsupported_field", "field": field, "allowed": sorted(QUERY_FIELDS)})
                return
        where: list[str] = []
        params: list[Any] = []
        for field, value in filters.items():
            values = value if isinstance(value, list) else [value]
            where.append(f"{QUERY_FIELD_SQL[field]} IN ({','.join('?' for _ in values)})")
            params.extend(values)
        select_group = ", ".join(f"{QUERY_FIELD_SQL[field]} AS {field}" for field in group_by) if group_by else "'all' AS bucket"
        group_clause = "GROUP BY " + ", ".join(QUERY_FIELD_SQL[field] for field in group_by) if group_by else ""
        where_clause = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
            SELECT {select_group},
                   COUNT(*) AS total,
                   SUM(CASE WHEN state='clean' THEN 1 ELSE 0 END) AS clean,
                   SUM(CASE WHEN state='dirty' THEN 1 ELSE 0 END) AS dirty,
                   SUM(CASE WHEN state IN ('run_error','needs_review') THEN 1 ELSE 0 END) AS errors
            FROM tasks
            {where_clause}
            {group_clause}
            ORDER BY total DESC
            LIMIT 500
        """
        with self._db() as conn:
            rows = [row_dict(r) for r in conn.execute(sql, params)]
        self._json(200, {"generated_at": utc_now(), "filters": filters, "group_by": group_by, "rows": rows})

    def _admin_override(self) -> None:
        payload = self._read_json()
        target_type = str(payload.get("target_type") or "task")
        target_id = str(payload.get("target_id") or payload.get("task_id") or "")
        new_state = str(payload.get("state") or "")
        operator = str(payload.get("operator") or "local-admin")
        reason = str(payload.get("reason") or "")
        if target_type != "task" or not target_id or not new_state or not reason:
            self._json(400, {"error": "target_type_task_target_id_state_reason_required"})
            return
        with self._db() as conn:
            row = conn.execute("SELECT state FROM tasks WHERE task_id=?", (target_id,)).fetchone()
            if row is None:
                self._json(404, {"error": "task_not_found"})
                return
            old_state = row["state"]
            conn.execute(
                """
                INSERT INTO admin_overrides(override_id, target_type, target_id, operator, old_state, new_state, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), target_type, target_id, operator, old_state, new_state, reason, utc_now()),
            )
            changed = set_task_state(conn, target_id, new_state, "admin", operator, "admin_override", {"reason": reason})
            conn.commit()
        self._json(200, {"ok": True, **changed})

    def _admin_simple_state(self, state: str, event_type: str) -> None:
        payload = self._read_json()
        task_id = str(payload.get("task_id") or "")
        operator = str(payload.get("operator") or "local-admin")
        with self._db() as conn:
            changed = set_task_state(conn, task_id, state, "admin", operator, event_type, payload)
            conn.commit()
        self._json(200, {"ok": True, **changed})

    def _admin_retry(self) -> None:
        payload = self._read_json()
        task_id = str(payload.get("task_id") or "")
        operator = str(payload.get("operator") or "local-admin")
        with self._db() as conn:
            row = conn.execute("SELECT attempt_no FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                self._json(404, {"error": "task_not_found"})
                return
            exclusions = None if payload.get("preserve_excluded_workers") else "[]"
            if exclusions is None:
                conn.execute(
                    "UPDATE tasks SET lease_worker_id=NULL, lease_until=NULL, attempt_no=?, result_id=NULL, error_json=NULL, completed_at=NULL WHERE task_id=?",
                    (int(row["attempt_no"] or 1) + 1, task_id),
                )
            else:
                conn.execute(
                    "UPDATE tasks SET lease_worker_id=NULL, lease_until=NULL, attempt_no=?, result_id=NULL, error_json=NULL, completed_at=NULL, excluded_worker_ids_json=? WHERE task_id=?",
                    (int(row["attempt_no"] or 1) + 1, exclusions, task_id),
                )
            changed = set_task_state(conn, task_id, "retry_queued", "admin", operator, "task_retry_queued", payload)
            conn.commit()
        self._json(200, {"ok": True, **changed})

    def _expire_leases(self) -> None:
        payload = self._read_json()
        operator = str(payload.get("operator") or "controller")
        now = utc_now()
        expired: list[dict[str, Any]] = []
        with self._db() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT task_id, state, lease_worker_id, lease_until, attempt_no
                FROM tasks
                WHERE state IN ('leased','running') AND lease_until IS NOT NULL AND lease_until<?
                ORDER BY lease_until
                """,
                (now,),
            ).fetchall()
            for row in rows:
                task_id = row["task_id"]
                old_state = row["state"]
                conn.execute(
                    """
                    UPDATE tasks
                    SET state='retry_queued', lease_worker_id=NULL, lease_until=NULL,
                        attempt_no=?, result_id=NULL, error_json=NULL, completed_at=NULL, updated_at=?
                    WHERE task_id=?
                    """,
                    (int(row["attempt_no"] or 1) + 1, now, task_id),
                )
                log_task_event(
                    conn,
                    task_id,
                    "lease_expired",
                    "controller",
                    operator,
                    old_state,
                    "retry_queued",
                    {
                        "lease_until": row["lease_until"],
                        "lease_worker_id": row["lease_worker_id"],
                        "attempt_no": int(row["attempt_no"] or 1) + 1,
                    },
                )
                expired.append({"task_id": task_id, "old_state": old_state, "new_state": "retry_queued"})
            conn.commit()
        self._json(200, {"ok": True, "expired": expired, "count": len(expired)})

    def _block_worker(self, blocked: bool) -> None:
        payload = self._read_json()
        worker_id = str(payload.get("worker_id") or "")
        operator = str(payload.get("operator") or "local-admin")
        with self._db() as conn:
            conn.execute(
                "UPDATE workers SET blocked=?, status=?, updated_at=? WHERE worker_id=?",
                (1 if blocked else 0, "blocked" if blocked else "registered", utc_now(), worker_id),
            )
            log_worker_event(conn, worker_id, "blocked" if blocked else "unblocked", {"operator": operator, **payload})
            conn.commit()
        self._json(200, {"ok": True, "worker_id": worker_id, "blocked": blocked})

    def _set_worker_concurrency(self) -> None:
        payload = self._read_json()
        worker_id = str(payload.get("worker_id") or "")
        desired = max(1, int(payload.get("desired_concurrency") or payload.get("concurrency") or 1))
        operator = str(payload.get("operator") or "local-admin")
        reason = str(payload.get("reason") or "manual concurrency update")
        with self._db() as conn:
            worker = conn.execute("SELECT max_concurrency,tuning_json FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if worker is None:
                self._json(404, {"error": "worker_not_found", "worker_id": worker_id})
                return
            max_concurrency = max(1, int(worker["max_concurrency"] or 1))
            desired = min(desired, max_concurrency)
            tuning = json.loads(worker["tuning_json"] or "{}")
            tuning.update({"current_concurrency": desired, "manual_override": True, "manual_reason": reason, "updated_at": utc_now()})
            conn.execute(
                "UPDATE workers SET desired_concurrency=?, tuning_json=?, updated_at=? WHERE worker_id=?",
                (desired, json.dumps(tuning, ensure_ascii=False), utc_now(), worker_id),
            )
            log_control(
                conn,
                self.server,
                "info",
                "manual_concurrency_update",
                f"{operator} set worker {worker_id} desired_concurrency={desired}",
                worker_id=worker_id,
                payload={"reason": reason, "desired_concurrency": desired, "max_concurrency": max_concurrency},
            )
            conn.commit()
        self._json(200, {"ok": True, "worker_id": worker_id, "desired_concurrency": desired, "max_concurrency": max_concurrency})

    def _register_worker_hosts(self) -> None:
        payload = self._read_json()
        operator = str(payload.get("operator") or "local-admin")
        hosts = payload.get("hosts")
        if hosts is None and payload.get("workers") is not None:
            hosts = payload.get("workers")
        if hosts is None and payload.get("inventory") is not None:
            inventory = payload.get("inventory") or {}
            try:
                inventory_version = int(inventory.get("inventory_version"))
            except (AttributeError, TypeError, ValueError):
                self._json(400, {"error": "inventory_version_required", "expected": INVENTORY_SCHEMA_VERSION})
                return
            if inventory_version != INVENTORY_SCHEMA_VERSION:
                self._json(
                    400,
                    {"error": "unsupported_inventory_version", "expected": INVENTORY_SCHEMA_VERSION, "received": inventory_version},
                )
                return
            hosts = inventory.get("workers") or []
        if not isinstance(hosts, list) or not hosts:
            self._json(400, {"error": "hosts_required"})
            return
        with self._db() as conn:
            registered = register_host_rows(conn, hosts, operator)
            for host in registered:
                log_control(
                    conn,
                    self.server,
                    "info",
                    "worker_host_registered",
                    f"{operator} registered host {host['host_id']} using {host['connection_mode']}",
                    worker_id=host.get("worker_id"),
                    payload=host,
                )
            conn.commit()
        self._json(200, {"ok": True, "registered": registered})

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))


def request_json(url: str, payload: dict[str, Any] | None = None, *, token: str | None = None, timeout: int = 20) -> dict[str, Any]:
    return http_request_json(url, payload, token=token, timeout=timeout)


def controller_token(args: argparse.Namespace) -> str | None:
    return token_from_env(getattr(args, "controller_token_env", DEFAULT_HUB_TOKEN_ENV))


def add_controller_auth_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--controller-token-env",
        default=DEFAULT_HUB_TOKEN_ENV,
        help="Environment variable containing the Hub bearer token. Leave unset only for a loopback-only Hub.",
    )


def parse_key_value(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"expected KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"empty environment key in {item!r}")
        out[key] = value
    return out


def cmd_server(args: argparse.Namespace) -> int:
    auth_token = token_from_env(args.auth_token_env)
    if not is_loopback_host(args.host) and not auth_token:
        raise ValueError("non-loopback Hub bind requires --auth-token-env with a configured token")
    db_path = args.db.resolve()
    artifact_root = args.artifact_root.resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        ensure_schema(conn)
        conn.commit()
    server = ControllerServer((args.host, args.port), Handler)
    server.benchmark_root = args.benchmark_root.resolve()
    server.db_path = db_path
    server.artifact_root = artifact_root
    server.control_log_path = args.control_log.resolve() if args.control_log else None
    server.upload_lock = threading.Lock()
    server.auth_token = auth_token
    server.runner_token_env = args.runner_token_env
    print(
        json.dumps(
            {
                "event": "controller_started",
                "url": f"http://{args.host}:{args.port}",
                "db": str(db_path),
                "auth_required": bool(auth_token),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    server.serve_forever()
    return 0


def cmd_init_db(args: argparse.Namespace) -> int:
    with connect(args.db.resolve()) as conn:
        ensure_schema(conn)
        conn.commit()
    print(json.dumps({"ok": True, "db": str(args.db.resolve())}, ensure_ascii=False))
    return 0


def cmd_dispatch_smoke(args: argparse.Namespace) -> int:
    tasks = []
    for idx in range(args.count):
        tasks.append(
            {
                "task_id": f"{args.prefix}-{idx + 1}",
                "required_capability": args.required_capability,
                "payload": {
                    "runner": "shell",
                    "command": args.command,
                    "timeout_seconds": args.timeout_seconds,
                    "package_id": f"{args.prefix}-package",
                },
            }
        )
    out = request_json(
        args.controller.rstrip("/") + "/api/tasks/dispatch",
        {"schema_version": DISPATCH_SCHEMA_VERSION, "operator": args.operator, "tasks": tasks},
        token=controller_token(args),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_dispatch_repo_run(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "runner": "repo",
        "source": {
            "type": "git",
            "url": args.repo_url,
            "ref": args.ref,
            "depth": args.depth,
            "token_env": args.source_token_env,
        },
        "commands": [{"name": f"step-{idx + 1}", "command": command} for idx, command in enumerate(args.command)],
        "artifact_paths": args.artifact,
        "timeout_seconds": args.timeout_seconds,
        "materialize_timeout_seconds": args.materialize_timeout_seconds,
        "env": parse_key_value(args.env),
        "continue_on_error": args.continue_on_error,
        "execution_profile": {
            "placement": args.placement,
            "resources": {
                "cpu_millis": args.cpu_millis,
                "memory_mb": args.memory_mb,
                "disk_mb": args.disk_mb,
                "gpu_count": args.gpu_count,
                "gpu_types": args.gpu_type,
            },
        },
    }
    task_id = args.task_id or "repo-run-" + str(uuid.uuid4())
    task = {
        "task_id": task_id,
        "required_capability": args.required_capability,
        "priority": args.priority,
        "package_id": args.package_id or f"{task_id}-package",
        "payload": payload,
    }
    out = request_json(
        args.controller.rstrip("/") + "/api/tasks/dispatch",
        {"schema_version": DISPATCH_SCHEMA_VERSION, "operator": args.operator, "tasks": [task]},
        token=controller_token(args),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_dispatch_spec(args: argparse.Namespace) -> int:
    if str(args.spec) == "-":
        payload = json.loads(sys.stdin.read())
    else:
        payload = read_json(args.spec)
    if "operator" not in payload:
        payload["operator"] = args.operator
    out = request_json(args.controller.rstrip("/") + "/api/tasks/dispatch", payload, token=controller_token(args))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_capabilities(args: argparse.Namespace) -> int:
    out = request_json(args.controller.rstrip("/") + "/api/meta", token=controller_token(args))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    base = args.controller.rstrip("/")
    summary = {
        "health": request_json(base + "/api/healthz", token=controller_token(args)),
        "worker_hosts": request_json(base + "/api/data/worker-hosts", token=controller_token(args)),
        "workers": request_json(base + "/api/data/active-workers", token=controller_token(args)),
        "worker_capacity": request_json(base + "/api/data/worker-capacity", token=controller_token(args)),
        "tasks": request_json(base + "/api/tasks?limit=200", token=controller_token(args)),
        "results": request_json(base + f"/api/data/new-results?cursor={args.cursor}&limit=200", token=controller_token(args)),
        "error_rate": request_json(base + "/api/data/error-rate?by=state&window_seconds=86400", token=controller_token(args)),
        "control_log": request_json(base + "/api/data/control-log?limit=100", token=controller_token(args)),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def cmd_worker_capacity(args: argparse.Namespace) -> int:
    out = request_json(args.controller.rstrip("/") + "/api/data/worker-capacity", token=controller_token(args))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_task_admission(args: argparse.Namespace) -> int:
    query = urlencode({"task_id": args.task_id})
    out = request_json(args.controller.rstrip("/") + "/api/data/task-admission?" + query, token=controller_token(args))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_set_worker_concurrency(args: argparse.Namespace) -> int:
    out = request_json(
        args.controller.rstrip("/") + "/api/admin/set-worker-concurrency",
        {
            "worker_id": args.worker_id,
            "desired_concurrency": args.desired_concurrency,
            "operator": args.operator,
            "reason": args.reason,
        },
        token=controller_token(args),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_register_worker_hosts(args: argparse.Namespace) -> int:
    inventory = read_json(args.inventory)
    out = request_json(
        args.controller.rstrip("/") + "/api/admin/register-worker-hosts",
        {"operator": args.operator, "inventory": inventory},
        token=controller_token(args),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_retry_task(args: argparse.Namespace) -> int:
    out = request_json(
        args.controller.rstrip("/") + "/api/admin/retry-task",
        {
            "task_id": args.task_id,
            "operator": args.operator,
            "preserve_excluded_workers": args.preserve_excluded_workers,
        },
        token=controller_token(args),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def cmd_push_task(args: argparse.Namespace) -> int:
    out = request_json(
        args.controller.rstrip("/") + "/api/admin/push-task",
        {
            "task_id": args.task_id,
            "worker_id": args.worker_id,
            "operator": args.operator,
            "lease_seconds": args.lease_seconds,
            "push_timeout_seconds": args.push_timeout_seconds,
        },
        token=controller_token(args),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Loom Hub.")
    parser.add_argument("--version", action="version", version=f"Loom Hub v{CORE_PREVIEW_VERSION} Core Preview (Hub API v{HUB_API_VERSION})")
    parser.add_argument("--benchmark-root", type=Path, default=Path(__file__).resolve().parents[1])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--db", type=Path, default=Path("loom-runs/hub/hub.sqlite"))
    p.add_argument("--artifact-root", type=Path, default=Path("loom-runs/hub/artifacts"))
    p.add_argument("--control-log", type=Path, default=Path("loom-runs/hub/hub.jsonl"))
    p.add_argument("--auth-token-env", default=DEFAULT_HUB_TOKEN_ENV, help="Environment variable containing the Hub bearer token. Required when binding outside loopback.")
    p.add_argument("--runner-token-env", default=DEFAULT_RUNNER_TOKEN_ENV, help="Default environment variable on the Hub host containing Direct Runner API tokens.")
    p.set_defaults(func=cmd_server)

    p = sub.add_parser("init-db")
    p.add_argument("--db", type=Path, required=True)
    p.set_defaults(func=cmd_init_db)

    p = sub.add_parser("dispatch-smoke")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--count", type=int, default=1)
    p.add_argument("--prefix", default="smoke-task")
    p.add_argument("--required-capability", default="linux")
    p.add_argument("--operator", default="local-smoke")
    p.add_argument("--command", default="python3 -c \"print('loom smoke ok')\"")
    p.add_argument("--timeout-seconds", type=int, default=60)
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_dispatch_smoke)

    p = sub.add_parser("dispatch-repo-run")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--repo-url", required=True)
    p.add_argument("--ref", default=None)
    p.add_argument("--depth", type=int, default=1)
    p.add_argument("--source-token-env", default=None, help="Name of an environment variable available on workers for private repo clone auth.")
    p.add_argument("--task-id", default=None)
    p.add_argument("--package-id", default=None)
    p.add_argument("--required-capability", default="linux")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--operator", default="local-operator")
    p.add_argument("--command", action="append", required=True)
    p.add_argument("--artifact", action="append", default=[])
    p.add_argument("--env", action="append", default=[], help="Environment overlay as KEY=VALUE. Do not pass long-lived secrets here.")
    p.add_argument("--timeout-seconds", type=int, default=300)
    p.add_argument("--materialize-timeout-seconds", type=int, default=600)
    p.add_argument("--continue-on-error", action="store_true")
    p.add_argument("--placement", choices=["shared", "exclusive"], default="shared", help="Scheduler placement only; exclusive reserves the worker until this attempt finishes.")
    p.add_argument("--cpu-millis", type=int, default=0)
    p.add_argument("--memory-mb", type=int, default=0)
    p.add_argument("--disk-mb", type=int, default=0)
    p.add_argument("--gpu-count", type=int, default=0)
    p.add_argument("--gpu-type", action="append", default=[])
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_dispatch_repo_run)

    p = sub.add_parser("dispatch-spec")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--operator", default="local-operator")
    p.add_argument("spec", type=Path, help="JSON dispatch payload, or '-' for stdin.")
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_dispatch_spec)

    p = sub.add_parser("summary")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--cursor", type=int, default=0)
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_summary)

    p = sub.add_parser("worker-capacity")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_worker_capacity)

    p = sub.add_parser("task-admission")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--task-id", required=True)
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_task_admission)

    p = sub.add_parser("capabilities")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_capabilities)

    p = sub.add_parser("set-worker-concurrency")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--worker-id", required=True)
    p.add_argument("--desired-concurrency", type=int, required=True)
    p.add_argument("--operator", default="local-admin")
    p.add_argument("--reason", default="manual concurrency update")
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_set_worker_concurrency)

    p = sub.add_parser("register-worker-hosts")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--operator", default="local-admin")
    p.add_argument("inventory", type=Path)
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_register_worker_hosts)

    p = sub.add_parser("retry-task")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--task-id", required=True)
    p.add_argument("--operator", default="local-admin")
    p.add_argument("--preserve-excluded-workers", action="store_true")
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_retry_task)

    p = sub.add_parser("push-task")
    p.add_argument("--controller", default="http://127.0.0.1:8765")
    p.add_argument("--task-id", required=True)
    p.add_argument("--worker-id", required=True)
    p.add_argument("--operator", default="direct-push")
    p.add_argument("--lease-seconds", type=int, default=600)
    p.add_argument("--push-timeout-seconds", type=int, default=30)
    add_controller_auth_arg(p)
    p.set_defaults(func=cmd_push_task)

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

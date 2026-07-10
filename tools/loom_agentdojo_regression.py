#!/usr/bin/env python3
"""Run and recover Loom's fixed 2 case x 2 run x 2 attempt AgentDojo check."""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import sys
import time
import zipfile
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import loom_manifest
from loom_http import DEFAULT_HUB_TOKEN_ENV, bearer_headers, request_json, token_from_env


FINAL_STATES = {"clean", "dirty", "run_error", "needs_review", "accepted", "ignored", "blocked", "cancelled"}
REQUIRED_PACKAGE_FILES = {"task.json", "worker-result.json", "phase-results.json", "artifact-manifest.json"}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def task_rows(controller: str, token: str | None, task_ids: list[str]) -> dict[str, dict[str, Any]]:
    query = urlencode({"task_id": ",".join(task_ids), "limit": "500"})
    payload = request_json(controller.rstrip("/") + "/api/tasks?" + query, token=token, timeout=30)
    return {str(row["task_id"]): row for row in payload.get("tasks") or []}


def result_rows(controller: str, token: str | None, task_ids: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = 0
    while True:
        payload = request_json(
            controller.rstrip("/") + f"/api/data/new-results?cursor={cursor}&limit=500",
            token=token,
            timeout=30,
        )
        page = payload.get("results") or []
        rows.extend(row for row in page if str(row.get("task_id") or "") in task_ids)
        next_cursor = int(payload.get("next_cursor") or cursor)
        if not page or next_cursor == cursor:
            return rows
        cursor = next_cursor


def push_queued_tasks(controller: str, token: str | None, worker_id: str, rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    responses: list[dict[str, Any]] = []
    for task in rows.values():
        if task.get("state") not in {"queued", "retry_queued"}:
            continue
        try:
            response = request_json(
                controller.rstrip("/") + "/api/admin/push-task",
                {"task_id": task["task_id"], "worker_id": worker_id, "operator": "agentdojo-eight-slot"},
                token=token,
                timeout=45,
            )
            responses.append({"task_id": task["task_id"], "response": response})
        except HTTPError as exc:
            if exc.code not in {409, 429}:
                raise
            responses.append({"task_id": task["task_id"], "deferred": exc.code})
    return responses


def download_result(controller: str, token: str | None, row: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    task_id = str(row["task_id"])
    result_id = str(row["result_id"])
    attempt_no = int(row.get("attempt_no") or 0)
    target = output_dir / "result-packages" / task_id / f"attempt-{attempt_no}__{result_id}.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    headers = bearer_headers(token)
    req = Request(controller.rstrip("/") + "/api/results/" + result_id, headers=headers)
    digest = hashlib.sha256()
    with urlopen(req, timeout=120) as response, target.open("wb") as f:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            digest.update(chunk)
    names: set[str]
    with zipfile.ZipFile(target, "r") as archive:
        names = set(archive.namelist())
    return {
        "task_id": task_id,
        "result_id": result_id,
        "attempt_no": attempt_no,
        "path": str(target),
        "sha256": digest.hexdigest(),
        "sha256_matches_controller": digest.hexdigest() == str(row.get("sha256") or ""),
        "required_files_present": sorted(REQUIRED_PACKAGE_FILES - names) == [],
        "has_agentdojo_index": "artifacts/agentdojo-artifact-index.json" in names,
        "files": sorted(names),
    }


def validate_manifest(spec: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    tasks = spec.get("tasks") or []
    task_ids = [str(task.get("task_id") or "") for task in tasks]
    cases = {str(task.get("case_id") or "") for task in tasks}
    runs = {str(task.get("run_id") or "") for task in tasks}
    if len(tasks) != 4 or len(task_ids) != 4 or len(set(task_ids)) != 4 or len(cases) != 2 or len(runs) != 2:
        raise ValueError("fixed AgentDojo contract requires exactly 2 cases x 2 runs = 4 task identities")
    return task_ids, {"task_count": len(tasks), "case_count": len(cases), "run_count": len(runs)}


def run(args: argparse.Namespace) -> dict[str, Any]:
    token = token_from_env(args.controller_token_env)
    manifest = loom_manifest.read_json_or_jsonl(args.manifest)
    dispatch_spec = loom_manifest.normalize(manifest, operator="agentdojo-eight-slot")
    task_ids, matrix = validate_manifest(dispatch_spec)
    task_set = set(task_ids)
    dispatch = request_json(
        args.controller.rstrip("/") + "/api/tasks/dispatch",
        dispatch_spec,
        token=token,
        timeout=60,
    )
    deadline = time.monotonic() + args.timeout_seconds
    pushes: list[dict[str, Any]] = []
    final_rows: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline:
        rows = task_rows(args.controller, token, task_ids)
        if set(rows) != task_set:
            time.sleep(args.poll_seconds)
            continue
        pushes.extend(push_queued_tasks(args.controller, token, args.worker_id, rows))
        final_rows = rows
        packages = result_rows(args.controller, token, task_set)
        all_clean = all(
            row.get("state") == "clean" and int(row.get("attempt_no") or 0) == 2
            for row in rows.values()
        )
        if all_clean and len(packages) == 8:
            break
        if any(row.get("state") in FINAL_STATES - {"clean"} for row in rows.values()):
            break
        time.sleep(args.poll_seconds)
    packages = result_rows(args.controller, token, task_set)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in packages:
        grouped[str(row["task_id"])].append(row)
    downloads = [download_result(args.controller, token, row, args.output) for row in packages]
    by_task: dict[str, Any] = {}
    for task_id in task_ids:
        task = final_rows.get(task_id) or task_rows(args.controller, token, [task_id]).get(task_id)
        task_packages = sorted(grouped.get(task_id) or [], key=lambda row: int(row.get("attempt_no") or 0))
        by_task[task_id] = {
            "state": task.get("state") if task else None,
            "attempt_no": int(task.get("attempt_no") or 0) if task else 0,
            "result_attempts": [int(row.get("attempt_no") or 0) for row in task_packages],
        }
    checks = {
        "four_task_identities": len(by_task) == 4,
        "eight_result_packages": len(packages) == 8,
        "each_task_recovered_twice": all(value["result_attempts"] == [1, 2] for value in by_task.values()),
        "each_task_clean_on_attempt_two": all(value["state"] == "clean" and value["attempt_no"] == 2 for value in by_task.values()),
        "download_hashes_match": all(item["sha256_matches_controller"] for item in downloads),
        "contract_files_present": all(item["required_files_present"] for item in downloads),
        "second_attempts_have_agentdojo_output": all(item["has_agentdojo_index"] for item in downloads if item["attempt_no"] == 2),
    }
    return {
        "ok": all(checks.values()),
        "contract": {**matrix, "attempts_per_task": 2, "expected_result_packages": 8},
        "dispatch": dispatch,
        "pushes": pushes,
        "tasks": by_task,
        "checks": checks,
        "downloads": downloads,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Loom's fixed AgentDojo eight-result release regression.")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--manifest", type=Path, default=Path("examples/agentdojo/agentdojo-eight-slot.manifest.json"))
    parser.add_argument("--controller-token-env", default=DEFAULT_HUB_TOKEN_ENV)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        summary = run(args)
    except Exception as exc:
        summary = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    (args.output / "agentdojo-eight-slot-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

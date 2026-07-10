#!/usr/bin/env python3
"""Export a small, credential-free AgentDojo recovery example from Loom ZIPs."""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def archive_json(archive: zipfile.ZipFile, name: str) -> dict[str, Any]:
    try:
        return json.loads(archive.read(name).decode("utf-8-sig"))
    except KeyError:
        return {}


def public_phase_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in payload.get("phases") or []:
        if not isinstance(phase, dict):
            continue
        rows.append(
            {
                "phase": phase.get("phase"),
                "phase_index": phase.get("phase_index"),
                "exit_code": phase.get("exit_code"),
            }
        )
    return rows


def public_artifacts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for artifact in payload.get("artifacts") or []:
        if not isinstance(artifact, dict):
            continue
        rows.append(
            {
                "path": artifact.get("path"),
                "bytes": artifact.get("bytes"),
                "sha256": artifact.get("sha256"),
            }
        )
    return rows


def public_agentdojo_index(archive: zipfile.ZipFile) -> dict[str, Any] | None:
    payload = archive_json(archive, "artifacts/agentdojo-artifact-index.json")
    if not payload:
        return None
    return {
        "case_id": payload.get("case_id"),
        "run_id": payload.get("run_id"),
        "attempt_no": payload.get("attempt_no"),
        "files": public_artifacts({"artifacts": payload.get("files") or []}),
    }


def package_summary(item: dict[str, Any]) -> dict[str, Any]:
    path = Path(str(item["path"]))
    with zipfile.ZipFile(path, "r") as archive:
        task = archive_json(archive, "task.json")
        worker_result = archive_json(archive, "worker-result.json")
        phase_results = archive_json(archive, "phase-results.json")
        artifact_manifest = archive_json(archive, "artifact-manifest.json")
        return {
            "task_id": task.get("task_id"),
            "case_id": task.get("case_id"),
            "run_id": task.get("run_id"),
            "setting_id": task.get("setting_id"),
            "attempt_no": int(item.get("attempt_no") or task.get("attempt_no") or 0),
            "verdict": worker_result.get("verdict"),
            "exit_code": worker_result.get("exit_code"),
            "phase_results": public_phase_rows(phase_results),
            "artifacts": public_artifacts(artifact_manifest),
            "agentdojo_output": public_agentdojo_index(archive),
        }


def export(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    source = read_json(input_dir / "agentdojo-eight-slot-summary.json")
    if not source.get("ok"):
        raise ValueError("refusing to export an unsuccessful AgentDojo regression")
    packages = [package_summary(item) for item in source.get("downloads") or []]
    packages.sort(key=lambda item: (str(item.get("case_id") or ""), str(item.get("run_id") or ""), int(item.get("attempt_no") or 0)))
    exported = {
        "schema_version": 1,
        "kind": "loom-agentdojo-eight-slot-recovery",
        "contract": source.get("contract") or {},
        "checks": source.get("checks") or {},
        "packages": packages,
        "redaction": {
            "omitted": [
                "hostnames",
                "worker identifiers",
                "timestamps",
                "command stdout and stderr",
                "raw model output",
                "result URLs and IDs",
            ]
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "recovery-contract.json"
    target.write_text(json.dumps(exported, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"path": str(target), "package_count": len(packages), "ok": True}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export sanitized evidence from a completed Loom AgentDojo regression.")
    parser.add_argument("--input", type=Path, required=True, help="Directory created by loom_agentdojo_regression.py")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        result = export(args.input, args.output)
    except Exception as exc:
        result = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

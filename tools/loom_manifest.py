#!/usr/bin/env python3
"""Normalize benchmark task input into controller dispatch payloads.

The input is intentionally repo-agnostic. Handoff owners or agents standardize
their work into campaign/case/run records; this script turns those records into
controller tasks with stable task IDs that can be dispatched, retried, queried,
and recovered one run at a time.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from loom_cache import attach_source_descriptor
from loom_contract import CORE_PREVIEW_VERSION, MANIFEST_SCHEMA_VERSION, merge_extensions
from loom_evaluation import normalize_oracle_spec, normalize_trajectory_export
from loom_resources import normalize_execution_profile

SAFE_ID = re.compile(r"[^A-Za-z0-9_.-]+")
FINAL_STATES = {"clean", "dirty", "run_error", "needs_review", "accepted", "ignored", "blocked", "cancelled"}
PHASE_FIELDS = {
    "name",
    "command",
    "args",
    "cwd",
    "env",
    "timeout_seconds",
    "continue_on_error",
    "artifact_paths",
}


def read_json_or_jsonl(path: Path) -> Any:
    text = path.read_text(encoding="utf-8-sig")
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return json.loads(text)


def slug(value: Any) -> str:
    out = SAFE_ID.sub("-", str(value).strip()).strip("-")
    return out or "unknown"


def merge_dicts(*items: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items:
        if item:
            out.update(item)
    return out


def expand_template(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        out = value
        for key, item in context.items():
            out = out.replace("{" + str(key) + "}", str(item))
        return out
    if isinstance(value, list):
        return [expand_template(item, context) for item in value]
    if isinstance(value, dict):
        return {key: expand_template(item, context) for key, item in value.items()}
    return value


def string_list(value: Any, *, field: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list) or any(not isinstance(item, (str, int, float)) for item in value):
        raise ValueError(f"{field} must be a string or a list of scalar values")
    return [str(item) for item in value]


def phase_rows(value: Any, *, field: str, allow_partial: bool) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty list of phase objects")
    rows: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw in enumerate(value, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"{field}[{index}] must be an object")
        unknown = sorted(set(raw) - PHASE_FIELDS)
        if unknown:
            raise ValueError(f"{field}[{index}] has unsupported fields: {', '.join(unknown)}")
        name = str(raw.get("name") or "").strip()
        if not name:
            raise ValueError(f"{field}[{index}].name is required")
        if name in names:
            raise ValueError(f"{field} contains duplicate phase name: {name}")
        names.add(name)
        if not allow_partial and not raw.get("command"):
            raise ValueError(f"{field}[{index}].command is required")
        if "args" in raw:
            string_list(raw["args"], field=f"{field}[{index}].args")
        if "artifact_paths" in raw:
            string_list(raw["artifact_paths"], field=f"{field}[{index}].artifact_paths")
        if "env" in raw and not isinstance(raw["env"], dict):
            raise ValueError(f"{field}[{index}].env must be an object")
        if "timeout_seconds" in raw and int(raw["timeout_seconds"]) <= 0:
            raise ValueError(f"{field}[{index}].timeout_seconds must be positive")
        rows.append(dict(raw))
    return rows


def phase_specs(defaults: dict[str, Any], case: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    """Merge named case phase overrides while preserving default declaration order."""
    default_rows = phase_rows(defaults.get("phases"), field="defaults.phases", allow_partial=False)
    case_rows = phase_rows(case.get("phases"), field="case.phases", allow_partial=True)
    if not default_rows and not case_rows:
        return []
    merged = [dict(row) for row in default_rows]
    positions = {str(row["name"]): index for index, row in enumerate(merged)}
    for row in case_rows:
        name = str(row["name"])
        if name not in positions:
            if not row.get("command"):
                raise ValueError(f"case.phases override for new phase {name!r} requires command")
            positions[name] = len(merged)
            merged.append(dict(row))
            continue
        index = positions[name]
        base = merged[index]
        overlay = dict(row)
        env = merge_dicts(
            base.get("env") if isinstance(base.get("env"), dict) else None,
            overlay.get("env") if isinstance(overlay.get("env"), dict) else None,
        )
        base.update(overlay)
        if env:
            base["env"] = env
        elif "env" in base:
            base.pop("env", None)
        merged[index] = base
    resolved: list[dict[str, Any]] = []
    for index, phase in enumerate(merged, start=1):
        if not phase.get("command"):
            raise ValueError(f"phase {phase.get('name')!r} has no command after merge")
        phase = expand_template(phase, context)
        phase["phase"] = str(phase["name"])
        phase["phase_index"] = index
        phase["args"] = string_list(phase.get("args"), field=f"phase {phase['name']}.args")
        phase["artifact_paths"] = string_list(phase.get("artifact_paths"), field=f"phase {phase['name']}.artifact_paths")
        resolved.append(phase)
    return resolved


def command_specs(raw: Any, context: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        raw = [raw]
    commands = []
    for idx, item in enumerate(raw or [], start=1):
        if isinstance(item, dict):
            spec = dict(item)
        else:
            spec = {"command": item}
        spec.setdefault("name", f"step-{idx}")
        commands.append(expand_template(spec, context))
    return commands


def expected_contract(defaults: dict[str, Any], case: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    raw = case.get("expected", defaults.get("expected"))
    if raw is None:
        return {}
    if isinstance(raw, str):
        raw = {"state": raw}
    if not isinstance(raw, dict):
        raise ValueError("expected must be a state string or object")
    expected = expand_template(raw, context)
    state = expected.get("state")
    if state is not None and str(state) not in FINAL_STATES:
        raise ValueError(f"unsupported expected state: {state}")
    for key in ("attempt_no", "min_result_count", "min_distinct_workers"):
        if key in expected:
            expected[key] = int(expected[key])
            if expected[key] < 1:
                raise ValueError(f"expected.{key} must be positive")
    return expected


def execution_profile_spec(defaults: dict[str, Any], case: dict[str, Any], payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Merge scheduler-only placement and resource requests without mixing task env."""
    raw: dict[str, Any] = {}
    resources: dict[str, Any] = {}
    layers = (
        defaults.get("execution_profile"),
        (defaults.get("payload") or {}).get("execution_profile") if isinstance(defaults.get("payload"), dict) else None,
        case.get("execution_profile"),
        payload.get("execution_profile"),
    )
    for layer in layers:
        if layer is None:
            continue
        if not isinstance(layer, dict):
            raise ValueError("execution_profile must be an object")
        raw.update(layer)
        if "resources" in layer:
            if not isinstance(layer["resources"], dict):
                raise ValueError("execution_profile.resources must be an object")
            resources.update(layer["resources"])
            raw["resources"] = resources
    return normalize_execution_profile(expand_template(raw, context))


def optional_contract(
    *,
    campaign: dict[str, Any],
    defaults: dict[str, Any],
    case: dict[str, Any],
    default_payload: dict[str, Any],
    case_payload: dict[str, Any],
    field: str,
    context: dict[str, Any],
    normalizer: Any,
) -> dict[str, Any] | None:
    """Resolve an atomic public contract with the same layer order as extensions."""
    selected: Any = None
    selected_field = field
    for layer_field, value in (
        (f"campaign.{field}", campaign.get(field)),
        (f"defaults.{field}", defaults.get(field)),
        (f"defaults.payload.{field}", default_payload.get(field)),
        (f"case.{field}", case.get(field)),
        (f"case.payload.{field}", case_payload.get(field)),
    ):
        if value is not None:
            selected = value
            selected_field = layer_field
    if selected is None:
        return None
    return normalizer(expand_template(selected, context), field=selected_field)


def normalize(
    data: Any,
    *,
    operator: str,
    case_ids: set[str] | None = None,
    run_ids: set[str] | None = None,
    setting_ids: set[str] | None = None,
) -> dict[str, Any]:
    if isinstance(data, list):
        campaign_ids = {slug(item.get("campaign_id")) for item in data if isinstance(item, dict) and item.get("campaign_id")}
        versions = {item.get("schema_version") for item in data if isinstance(item, dict)}
        if len(campaign_ids) != 1 or any(not isinstance(item, dict) or not item.get("campaign_id") for item in data):
            raise ValueError("JSONL rows require one shared explicit campaign_id")
        if len(versions) != 1 or None in versions:
            raise ValueError("JSONL rows require one shared explicit schema_version")
        campaign = {"schema_version": next(iter(versions)), "campaign_id": next(iter(campaign_ids)), "cases": data}
    else:
        campaign = dict(data)
    if not campaign.get("campaign_id"):
        raise ValueError("manifest requires explicit campaign_id")
    if "schema_version" not in campaign:
        raise ValueError("manifest requires explicit schema_version")
    raw_schema_version = campaign["schema_version"]
    try:
        schema_version = int(raw_schema_version)
    except (TypeError, ValueError) as exc:
        raise ValueError("manifest schema_version must be an integer") from exc
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported manifest schema_version: {schema_version}")
    campaign_id = slug(campaign["campaign_id"])
    defaults = campaign.get("defaults") or {}
    source = campaign.get("source") or defaults.get("source")
    tasks = []
    task_ids: set[str] = set()
    for index, raw_case in enumerate(campaign.get("cases") or campaign.get("runs") or [], start=1):
        case = dict(raw_case)
        missing = [field for field in ("case_id", "setting_id", "run_id") if not case.get(field) and not defaults.get(field)]
        if missing:
            raise ValueError(f"run row {index} is missing explicit fields: {', '.join(missing)}")
        case_id = slug(case.get("case_id") or defaults.get("case_id"))
        run_id = slug(case.get("run_id") or defaults.get("run_id"))
        setting_id = slug(case.get("setting_id") or defaults.get("setting_id"))
        if case_ids and case_id not in case_ids:
            continue
        if run_ids and run_id not in run_ids:
            continue
        if setting_ids and setting_id not in setting_ids:
            continue
        context = {**defaults, **case, "campaign_id": campaign_id, "case_id": case_id, "run_id": run_id, "setting_id": setting_id}
        task_id = slug(case.get("task_id") or f"{campaign_id}__{case_id}__{setting_id}__run-{run_id}")
        if task_id in task_ids:
            raise ValueError(f"duplicate normalized task_id: {task_id}")
        task_ids.add(task_id)
        default_payload = defaults.get("payload")
        case_payload = case.get("payload")
        if default_payload is not None and not isinstance(default_payload, dict):
            raise ValueError("defaults.payload must be an object")
        if case_payload is not None and not isinstance(case_payload, dict):
            raise ValueError("case.payload must be an object")
        payload = merge_dicts(default_payload, case_payload)
        extension_layers = (
            ("campaign.extensions", campaign.get("extensions")),
            ("defaults.extensions", defaults.get("extensions")),
            ("defaults.payload.extensions", (default_payload or {}).get("extensions")),
            ("case.extensions", case.get("extensions")),
            ("case.payload.extensions", (case_payload or {}).get("extensions")),
        )
        if any(value is not None for _, value in extension_layers):
            payload["extensions"] = merge_extensions(*extension_layers)
        oracle = optional_contract(
            campaign=campaign,
            defaults=defaults,
            case=case,
            default_payload=default_payload or {},
            case_payload=case_payload or {},
            field="oracle",
            context=context,
            normalizer=normalize_oracle_spec,
        )
        if oracle is not None:
            payload["oracle"] = oracle
        trajectory_export = optional_contract(
            campaign=campaign,
            defaults=defaults,
            case=case,
            default_payload=default_payload or {},
            case_payload=case_payload or {},
            field="trajectory_export",
            context=context,
            normalizer=normalize_trajectory_export,
        )
        if trajectory_export is not None:
            payload["trajectory_export"] = trajectory_export
        runner = str(case.get("runner") or payload.get("runner") or defaults.get("runner") or "repo")
        artifact_paths = string_list(
            case.get("artifact_paths") or case.get("artifacts") or defaults.get("artifact_paths"),
            field="artifact_paths",
        )
        payload.update(
            {
                "runner": runner,
                "artifact_paths": expand_template(artifact_paths, context),
                "env": merge_dicts(defaults.get("env"), case.get("env")),
                "timeout_seconds": int(case.get("timeout_seconds") or defaults.get("timeout_seconds") or 300),
                "continue_on_error": bool(case.get("continue_on_error", defaults.get("continue_on_error", False))),
                "normalized": {
                    "campaign_id": campaign_id,
                    "case_id": case_id,
                    "run_id": run_id,
                    "setting_id": setting_id,
                    "source_index": index,
                },
            }
        )
        payload["execution_profile"] = execution_profile_spec(defaults, case, payload, context)
        retry_policy = merge_dicts(
            payload.get("retry_policy") if isinstance(payload.get("retry_policy"), dict) else None,
            defaults.get("retry_policy") if isinstance(defaults.get("retry_policy"), dict) else None,
            case.get("retry_policy") if isinstance(case.get("retry_policy"), dict) else None,
        )
        if retry_policy:
            retry_policy["max_attempts"] = int(retry_policy.get("max_attempts") or 1)
            if retry_policy["max_attempts"] < 1:
                raise ValueError(f"run row {index} retry_policy.max_attempts must be positive")
            categories = retry_policy.get("retry_categories") or retry_policy.get("categories") or []
            if isinstance(categories, str):
                categories = [categories]
            retry_policy["retry_categories"] = [str(value) for value in categories if value]
            retry_policy.pop("categories", None)
            retry_policy["different_worker"] = bool(retry_policy.get("different_worker", False))
            payload["retry_policy"] = retry_policy
        if runner == "repo":
            task_source = case.get("source") or source
            if not task_source:
                raise ValueError(f"repo run row {index} requires source")
            payload["source"] = expand_template(task_source, context)
            attach_source_descriptor(payload)
            phases = phase_specs(defaults, case, context)
            if phases:
                payload["phases"] = phases
                payload["commands"] = [dict(phase) for phase in phases]
                for phase in phases:
                    for pattern in phase.get("artifact_paths") or []:
                        if pattern not in payload["artifact_paths"]:
                            payload["artifact_paths"].append(pattern)
            else:
                payload["commands"] = command_specs(case.get("commands") or defaults.get("commands"), context)
        elif runner == "shell":
            command = case.get("command") or defaults.get("command") or payload.get("command")
            if not command:
                raise ValueError(f"shell run row {index} requires command")
            payload["command"] = expand_template(command, context)
            payload.pop("source", None)
            payload.pop("commands", None)
        elif runner != "noop":
            raise ValueError(f"run row {index} has unsupported runner: {runner}")
        task = {
            "task_id": task_id,
            "case_id": case_id,
            "run_id": run_id,
            "setting_id": setting_id,
            "case_version": str(case.get("case_version") or defaults.get("case_version") or "v1"),
            "scenario_id": case.get("scenario_id") or defaults.get("scenario_id") or setting_id,
            "package_id": slug(case.get("package_id") or f"{campaign_id}-{setting_id}"),
            "required_capability": case.get("required_capability") or defaults.get("required_capability") or "linux",
            "priority": int(case.get("priority") or defaults.get("priority") or 0),
            "payload": payload,
        }
        expected = expected_contract(defaults, case, context)
        if expected:
            task["expected"] = expected
        tasks.append(task)
    if not tasks:
        selection = " for requested case/run/setting selection" if case_ids or run_ids or setting_ids else ""
        raise ValueError(f"manifest produced no tasks{selection}")
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "operator": operator,
        "campaign_id": campaign_id,
        "package_id": slug(campaign.get("package_id") or campaign_id),
        "tasks": tasks,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize benchmark task input for controller dispatch.")
    parser.add_argument("--version", action="version", version=f"Loom Manifest v{CORE_PREVIEW_VERSION} Core Preview (manifest v{MANIFEST_SCHEMA_VERSION})")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--operator", default="manifest-normalizer")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--case-id", action="append", default=[], help="Normalize only these case IDs. Repeat to select more than one.")
    parser.add_argument("--run-id", action="append", default=[], help="Normalize only these run IDs. Repeat to select more than one.")
    parser.add_argument("--setting-id", action="append", default=[], help="Normalize only these setting IDs. Repeat to select more than one.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    payload = normalize(
        read_json_or_jsonl(args.manifest),
        operator=args.operator,
        case_ids={slug(value) for value in args.case_id} or None,
        run_ids={slug(value) for value in args.run_id} or None,
        setting_ids={slug(value) for value in args.setting_id} or None,
    )
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

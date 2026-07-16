#!/usr/bin/env python3
"""Public contracts for Loom Oracle, trajectory, and reward data.

The module intentionally depends only on the standard library.  Manifest,
Hub, Runner, and export tooling use these same validators so the public
contracts do not drift with an implementation detail of one component.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path, PurePosixPath
from typing import Any


ORACLE_SCHEMA_VERSION = 1
TRAJECTORY_SCHEMA_VERSION = 1
ORACLE_OUTCOMES = {"pass", "fail", "error", "inconclusive"}
ORACLE_WHEN = {"execution_clean", "execution_result"}
TRAJECTORY_EVENT_KINDS = {
    "message",
    "tool_call",
    "tool_result",
    "observation",
    "timing",
    "artifact_ref",
}
DEFAULT_TRAJECTORY_MAX_BYTES = 1024 * 1024
MAX_TRAJECTORY_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_REDACTION_PATTERNS = (
    r"(?i)\b(?:api[_-]?key|access[_-]?token|secret|password)\s*[:=]\s*[^\s,;]+",
    r"\bsk-[A-Za-z0-9_-]{12,}\b",
    r"\b(?:ghp|github_pat)_[A-Za-z0-9_]{12,}\b",
    r"https?://[^/\s:@]+:[^@\s/]+@",
)


def json_copy(value: Any, *, field: str) -> Any:
    """Return a finite JSON clone or raise a public-contract error."""
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must contain JSON-compatible values") from exc


def _non_empty_text(value: Any, *, field: str, limit: int = 256) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > limit:
        raise ValueError(f"{field} exceeds {limit} characters")
    return text


def _relative_path(value: Any, *, field: str) -> str:
    text = _non_empty_text(value, field=field, limit=512).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or text in {".", ""}:
        raise ValueError(f"{field} must be a relative path inside the attempt workspace")
    return path.as_posix()


def _schema_version(value: Any, *, field: str, expected: int) -> int:
    try:
        version = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}.schema_version is required") from exc
    if version != expected:
        raise ValueError(f"unsupported {field}.schema_version: {version}")
    return version


def _finite_number(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def oracle_name_slug(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-.")
    return text or "default"


def oracle_task_id(execution_task_id: str, execution_attempt_no: int, name: str) -> str:
    return f"{execution_task_id}__execution-attempt-{max(1, int(execution_attempt_no)):03d}__oracle-{oracle_name_slug(name)}"


def normalize_oracle_spec(value: Any, *, field: str = "oracle") -> dict[str, Any]:
    """Validate the declarative child-Oracle request stored on an execution task."""
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    allowed = {
        "schema_version",
        "name",
        "when",
        "oracle_version",
        "result_path",
        "required_capability",
        "priority",
        "execution_profile",
        "retry_policy",
        "payload",
    }
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{field} has unsupported fields: {', '.join(unknown)}")
    _schema_version(value.get("schema_version"), field=field, expected=ORACLE_SCHEMA_VERSION)
    name = _non_empty_text(value.get("name") or "default", field=f"{field}.name")
    when = str(value.get("when") or "execution_clean").strip()
    if when not in ORACLE_WHEN:
        raise ValueError(f"{field}.when must be one of: {', '.join(sorted(ORACLE_WHEN))}")
    oracle_version = _non_empty_text(value.get("oracle_version"), field=f"{field}.oracle_version")
    result_path = _relative_path(value.get("result_path") or "oracle-result.json", field=f"{field}.result_path")
    required_capability = _non_empty_text(
        value.get("required_capability") or "oracle", field=f"{field}.required_capability"
    )
    try:
        priority = int(value.get("priority") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}.priority must be an integer") from exc
    payload = value.get("payload")
    if not isinstance(payload, dict):
        raise ValueError(f"{field}.payload must be an object")
    payload_copy = json_copy(payload, field=f"{field}.payload")
    runner = str(payload_copy.get("runner") or "").strip()
    if runner not in {"shell", "repo", "noop"}:
        raise ValueError(f"{field}.payload.runner must be shell, repo, or noop")
    if runner == "shell" and not payload_copy.get("command"):
        raise ValueError(f"{field}.payload.command is required for a shell Oracle")
    if runner == "repo" and not payload_copy.get("source"):
        raise ValueError(f"{field}.payload.source is required for a repo Oracle")
    out: dict[str, Any] = {
        "schema_version": ORACLE_SCHEMA_VERSION,
        "name": name,
        "when": when,
        "oracle_version": oracle_version,
        "result_path": result_path,
        "required_capability": required_capability,
        "priority": priority,
        "payload": payload_copy,
    }
    for key in ("execution_profile", "retry_policy"):
        if key in value:
            if not isinstance(value[key], dict):
                raise ValueError(f"{field}.{key} must be an object")
            out[key] = json_copy(value[key], field=f"{field}.{key}")
    return out


def normalize_trajectory_export(value: Any, *, field: str = "trajectory_export") -> dict[str, Any]:
    """Validate the opt-in, redacted trajectory capture request."""
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    allowed = {
        "schema_version",
        "enabled",
        "source_path",
        "export_path",
        "max_bytes",
        "required",
        "redaction",
    }
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{field} has unsupported fields: {', '.join(unknown)}")
    _schema_version(value.get("schema_version"), field=field, expected=TRAJECTORY_SCHEMA_VERSION)
    enabled = bool(value.get("enabled", True))
    source_path = _relative_path(
        value.get("source_path") or ".loom-trajectory.raw.json", field=f"{field}.source_path"
    )
    export_path = _relative_path(value.get("export_path") or "trajectory.json", field=f"{field}.export_path")
    if source_path == export_path:
        raise ValueError(f"{field}.source_path and {field}.export_path must be different")
    try:
        max_bytes = int(value.get("max_bytes") or DEFAULT_TRAJECTORY_MAX_BYTES)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}.max_bytes must be an integer") from exc
    if max_bytes < 1 or max_bytes > MAX_TRAJECTORY_MAX_BYTES:
        raise ValueError(f"{field}.max_bytes must be between 1 and {MAX_TRAJECTORY_MAX_BYTES}")
    redaction = value.get("redaction") or {}
    if not isinstance(redaction, dict):
        raise ValueError(f"{field}.redaction must be an object")
    redaction_unknown = sorted(set(redaction) - {"patterns", "replacement"})
    if redaction_unknown:
        raise ValueError(f"{field}.redaction has unsupported fields: {', '.join(redaction_unknown)}")
    patterns = redaction.get("patterns") or []
    if not isinstance(patterns, list) or any(not isinstance(item, str) or not item for item in patterns):
        raise ValueError(f"{field}.redaction.patterns must be a list of non-empty strings")
    if len(patterns) > 32:
        raise ValueError(f"{field}.redaction.patterns may contain at most 32 patterns")
    for pattern in patterns:
        if len(pattern) > 512:
            raise ValueError(f"{field}.redaction pattern exceeds 512 characters")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"{field}.redaction pattern is invalid: {exc}") from exc
    replacement = str(redaction.get("replacement") or "[REDACTED]")
    if len(replacement) > 256:
        raise ValueError(f"{field}.redaction.replacement exceeds 256 characters")
    return {
        "schema_version": TRAJECTORY_SCHEMA_VERSION,
        "enabled": enabled,
        "source_path": source_path,
        "export_path": export_path,
        "max_bytes": max_bytes,
        "required": bool(value.get("required", True)),
        "redaction": {"patterns": list(patterns), "replacement": replacement},
    }


def trajectory_redaction_patterns(config: dict[str, Any]) -> list[re.Pattern[str]]:
    custom = ((config.get("redaction") or {}).get("patterns") or []) if isinstance(config, dict) else []
    return [re.compile(pattern) for pattern in [*DEFAULT_REDACTION_PATTERNS, *custom]]


def redact_trajectory_value(value: Any, config: dict[str, Any]) -> Any:
    """Redact strings recursively while retaining the declared event structure."""
    if isinstance(value, dict):
        return {str(key): redact_trajectory_value(item, config) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_trajectory_value(item, config) for item in value]
    if not isinstance(value, str):
        return value
    replacement = str((config.get("redaction") or {}).get("replacement") or "[REDACTED]")
    text = value
    for pattern in trajectory_redaction_patterns(config):
        text = pattern.sub(replacement, text)
    return text


def normalize_trajectory_document(value: Any, *, field: str = "trajectory") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    _schema_version(value.get("schema_version"), field=field, expected=TRAJECTORY_SCHEMA_VERSION)
    events = value.get("events")
    if not isinstance(events, list):
        raise ValueError(f"{field}.events must be a list")
    if len(events) > 100000:
        raise ValueError(f"{field}.events exceeds 100000 entries")
    normalized_events: list[dict[str, Any]] = []
    event_kinds: dict[str, int] = {}
    for index, event in enumerate(events, start=1):
        if not isinstance(event, dict):
            raise ValueError(f"{field}.events[{index}] must be an object")
        kind = str(event.get("kind") or event.get("type") or "").strip()
        if kind not in TRAJECTORY_EVENT_KINDS:
            raise ValueError(
                f"{field}.events[{index}] requires kind/type in: {', '.join(sorted(TRAJECTORY_EVENT_KINDS))}"
            )
        clean_event = json_copy(event, field=f"{field}.events[{index}]")
        clean_event["kind"] = kind
        clean_event.pop("type", None)
        normalized_events.append(clean_event)
        event_kinds[kind] = event_kinds.get(kind, 0) + 1
    out = json_copy(value, field=field)
    out["schema_version"] = TRAJECTORY_SCHEMA_VERSION
    out["events"] = normalized_events
    out["event_kinds"] = event_kinds
    return out


def path_stays_inside(path: Path, root: Path) -> bool:
    """Reject a task-created symlink that would escape an attempt directory."""
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def export_trajectory(
    config: dict[str, Any] | None,
    *,
    source_root: Path,
    export_root: Path,
) -> dict[str, Any]:
    """Create the sanitized package trajectory and return an export receipt.

    The raw source is deliberately removed from the package root after reading.
    A failed capture therefore never accidentally turns into a raw ZIP export.
    """
    if config is None or not bool(config.get("enabled", True)):
        return {"schema_version": TRAJECTORY_SCHEMA_VERSION, "enabled": False, "status": "disabled"}
    source_path = source_root / str(config["source_path"])
    export_path = export_root / str(config["export_path"])
    receipt: dict[str, Any] = {
        "schema_version": TRAJECTORY_SCHEMA_VERSION,
        "enabled": True,
        "required": bool(config.get("required", True)),
        "source_path": str(config["source_path"]),
        "export_path": str(config["export_path"]),
        "max_bytes": int(config["max_bytes"]),
    }
    try:
        if not path_stays_inside(source_path, source_root) or not path_stays_inside(export_path, export_root):
            return {**receipt, "status": "unsafe_path"}
        if not source_path.is_file():
            return {**receipt, "status": "missing"}
        raw_bytes = source_path.read_bytes()
        receipt["source_bytes"] = len(raw_bytes)
        if len(raw_bytes) > int(config["max_bytes"]):
            return {**receipt, "status": "source_size_exceeded"}
        try:
            raw = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return {**receipt, "status": "invalid_json", "error": str(exc)}
        document = normalize_trajectory_document(raw)
        redacted = redact_trajectory_value(document, config)
        serialized = (json.dumps(redacted, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        if len(serialized) > int(config["max_bytes"]):
            return {**receipt, "status": "redacted_size_exceeded", "export_bytes": len(serialized)}
        export_path.parent.mkdir(parents=True, exist_ok=True)
        export_path.write_bytes(serialized)
        receipt.update(
            {
                "status": "exported",
                "bytes": len(serialized),
                "sha256": hashlib.sha256(serialized).hexdigest(),
                "event_count": len(redacted.get("events") or []),
                "event_kinds": redacted.get("event_kinds") or {},
            }
        )
        return receipt
    except (OSError, ValueError) as exc:
        return {**receipt, "status": "error", "error": f"{type(exc).__name__}: {exc}"}
    finally:
        # `source_path` and `export_path` are deliberately distinct, so a raw
        # capture is never retained as a fallback package file.
        try:
            if source_path.is_file() or source_path.is_symlink():
                source_path.unlink()
        except OSError:
            pass


def trajectory_required_failure(receipt: dict[str, Any]) -> bool:
    return bool(receipt.get("enabled")) and bool(receipt.get("required")) and receipt.get("status") != "exported"


def normalize_oracle_result(value: Any, *, field: str = "oracle-result") -> dict[str, Any]:
    """Validate semantic Oracle output independently of Runner process status."""
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    allowed = {
        "schema_version",
        "outcome",
        "oracle_version",
        "reward",
        "score_metadata",
        "evidence",
        "summary",
        "extensions",
    }
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{field} has unsupported fields: {', '.join(unknown)}")
    _schema_version(value.get("schema_version"), field=field, expected=ORACLE_SCHEMA_VERSION)
    outcome = str(value.get("outcome") or "").strip()
    if outcome not in ORACLE_OUTCOMES:
        raise ValueError(f"{field}.outcome must be one of: {', '.join(sorted(ORACLE_OUTCOMES))}")
    oracle_version = _non_empty_text(value.get("oracle_version"), field=f"{field}.oracle_version")
    out: dict[str, Any] = {
        "schema_version": ORACLE_SCHEMA_VERSION,
        "outcome": outcome,
        "oracle_version": oracle_version,
    }
    if "reward" in value:
        reward = value["reward"]
        if not isinstance(reward, dict):
            raise ValueError(f"{field}.reward must be an object")
        unknown_reward = sorted(set(reward) - {"value", "components", "metadata"})
        if unknown_reward:
            raise ValueError(f"{field}.reward has unsupported fields: {', '.join(unknown_reward)}")
        if "value" not in reward:
            raise ValueError(f"{field}.reward.value is required when reward is present")
        normalized_reward: dict[str, Any] = {"value": _finite_number(reward["value"], field=f"{field}.reward.value")}
        if "components" in reward:
            components = reward["components"]
            if not isinstance(components, dict):
                raise ValueError(f"{field}.reward.components must be an object")
            if len(components) > 128:
                raise ValueError(f"{field}.reward.components may contain at most 128 entries")
            normalized_components: dict[str, float] = {}
            for key, item in components.items():
                component_key = _non_empty_text(key, field=f"{field}.reward.components key")
                normalized_components[component_key] = _finite_number(
                    item, field=f"{field}.reward.components.{component_key}"
                )
            normalized_reward["components"] = normalized_components
        if "metadata" in reward:
            if not isinstance(reward["metadata"], dict):
                raise ValueError(f"{field}.reward.metadata must be an object")
            normalized_reward["metadata"] = json_copy(reward["metadata"], field=f"{field}.reward.metadata")
        out["reward"] = normalized_reward
    for key in ("score_metadata", "extensions"):
        if key in value:
            if not isinstance(value[key], dict):
                raise ValueError(f"{field}.{key} must be an object")
            out[key] = json_copy(value[key], field=f"{field}.{key}")
    if "evidence" in value:
        evidence = value["evidence"]
        if not isinstance(evidence, list) or any(not isinstance(item, dict) for item in evidence):
            raise ValueError(f"{field}.evidence must be a list of objects")
        if len(evidence) > 256:
            raise ValueError(f"{field}.evidence may contain at most 256 entries")
        out["evidence"] = json_copy(evidence, field=f"{field}.evidence")
    if "summary" in value:
        out["summary"] = json_copy(value["summary"], field=f"{field}.summary")
    return out


def oracle_error_outcome(detail: str, *, oracle_version: str | None = None) -> dict[str, Any]:
    """Return a stored semantic error without collapsing the Runner state."""
    return {
        "schema_version": ORACLE_SCHEMA_VERSION,
        "outcome": "error",
        "oracle_version": str(oracle_version or "unavailable"),
        "summary": {"error": detail},
    }

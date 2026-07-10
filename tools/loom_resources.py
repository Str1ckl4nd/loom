"""Shared resource-admission contracts for Loom task placement."""

from __future__ import annotations

from typing import Any


RESOURCE_FIELDS = ("cpu_millis", "memory_mb", "disk_mb", "gpu_count")
PLACEMENT_MODES = {"shared", "exclusive"}


def _integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if number < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return number


def empty_resources() -> dict[str, Any]:
    return {"cpu_millis": 0, "memory_mb": 0, "disk_mb": 0, "gpu_count": 0, "gpu_types": []}


def normalize_resources(
    raw: Any,
    *,
    field: str,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be an object")
    allowed = set(RESOURCE_FIELDS) | {"gpu_types"}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{field} has unsupported fields: {', '.join(unknown)}")
    output = empty_resources()
    for name in RESOURCE_FIELDS:
        source = raw[name] if name in raw else (defaults or {}).get(name, 0)
        output[name] = _integer(source, field=f"{field}.{name}")
    raw_types = raw.get("gpu_types", (defaults or {}).get("gpu_types", []))
    if isinstance(raw_types, str):
        raw_types = [raw_types]
    if not isinstance(raw_types, list) or any(not isinstance(item, str) or not item.strip() for item in raw_types):
        raise ValueError(f"{field}.gpu_types must be a list of non-empty strings")
    output["gpu_types"] = sorted(dict.fromkeys(item.strip() for item in raw_types))
    if output["gpu_types"] and not output["gpu_count"]:
        raise ValueError(f"{field}.gpu_types requires a positive gpu_count")
    return output


def normalize_capacity_overrides(raw: Any, *, field: str = "resource_capacity") -> dict[str, Any]:
    """Validate operator overrides without inventing host values for omitted keys."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be an object")
    allowed = set(RESOURCE_FIELDS) | {"gpu_types"}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{field} has unsupported fields: {', '.join(unknown)}")
    output: dict[str, Any] = {}
    for name in RESOURCE_FIELDS:
        if name in raw:
            output[name] = _integer(raw[name], field=f"{field}.{name}")
    if "gpu_types" in raw:
        raw_types = raw["gpu_types"]
        if isinstance(raw_types, str):
            raw_types = [raw_types]
        if not isinstance(raw_types, list) or any(not isinstance(item, str) or not item.strip() for item in raw_types):
            raise ValueError(f"{field}.gpu_types must be a list of non-empty strings")
        output["gpu_types"] = sorted(dict.fromkeys(item.strip() for item in raw_types))
    return output


def normalize_execution_profile(raw: Any, *, field: str = "execution_profile") -> dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be an object")
    allowed = {"placement", "resources"}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{field} has unsupported fields: {', '.join(unknown)}")
    placement = str(raw.get("placement") or "shared").strip().lower()
    if placement not in PLACEMENT_MODES:
        raise ValueError(f"{field}.placement must be one of: {', '.join(sorted(PLACEMENT_MODES))}")
    return {
        "placement": placement,
        "resources": normalize_resources(raw.get("resources"), field=f"{field}.resources"),
    }


def default_capacity(snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    snapshot = snapshot or {}
    cpu_count = _integer(snapshot.get("cpu_count") or 1, field="resource snapshot cpu_count")
    memory_mb = _integer(snapshot.get("mem_available_mb") or snapshot.get("mem_total_mb") or 0, field="resource snapshot memory")
    disk_mb = _integer(snapshot.get("disk_available_mb") or snapshot.get("disk_total_mb") or 0, field="resource snapshot disk")
    return {
        "cpu_millis": max(1000, cpu_count * 1000),
        "memory_mb": memory_mb,
        "disk_mb": disk_mb,
        "gpu_count": 0,
        "gpu_types": [],
    }


def normalize_capacity(
    raw: Any,
    *,
    field: str = "resource_capacity",
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return normalize_resources(raw, field=field, defaults=default_capacity(snapshot))


def add_resources(*vectors: dict[str, Any]) -> dict[str, Any]:
    output = empty_resources()
    for vector in vectors:
        normalized = normalize_resources(vector, field="resource vector")
        for name in RESOURCE_FIELDS:
            output[name] += int(normalized[name])
    return output


def remaining_resources(capacity: dict[str, Any], reserved: dict[str, Any]) -> dict[str, Any]:
    capacity = normalize_resources(capacity, field="resource_capacity")
    reserved = normalize_resources(reserved, field="reserved_resources")
    output = empty_resources()
    for name in RESOURCE_FIELDS:
        output[name] = max(0, int(capacity[name]) - int(reserved[name]))
    output["gpu_types"] = list(capacity["gpu_types"])
    return output


def resource_shortfalls(
    capacity: dict[str, Any],
    reserved: dict[str, Any],
    requested: dict[str, Any],
) -> dict[str, Any]:
    capacity = normalize_resources(capacity, field="resource_capacity")
    reserved = normalize_resources(reserved, field="reserved_resources")
    requested = normalize_resources(requested, field="requested_resources")
    shortfalls: dict[str, Any] = {}
    for name in RESOURCE_FIELDS:
        required = int(reserved[name]) + int(requested[name])
        if required > int(capacity[name]):
            shortfalls[name] = {"capacity": int(capacity[name]), "reserved": int(reserved[name]), "requested": int(requested[name])}
    requested_types = set(requested["gpu_types"])
    capacity_types = set(capacity["gpu_types"])
    if requested_types and not requested_types.issubset(capacity_types):
        shortfalls["gpu_types"] = {"available": sorted(capacity_types), "requested": sorted(requested_types)}
    return shortfalls

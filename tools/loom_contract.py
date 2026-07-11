"""Internal definitions for the Loom v0.3.0 Core Preview and v1 protocols."""

from __future__ import annotations

import json
from typing import Any


PRODUCT_VERSION = "0.3.0"
RELEASE_CHANNEL = "core-preview"
# Kept as the public metadata key used by existing Preview integrations.
CORE_PREVIEW_VERSION = PRODUCT_VERSION
CLI_CONTRACT_VERSION = 1
INVENTORY_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
DISPATCH_SCHEMA_VERSION = 1
HUB_API_VERSION = 1
RUNNER_API_VERSION = 1
CONCURRENCY_POLICIES = {"fixed", "adaptive"}
FIXED_CONCURRENCY_BACKOFF_ISSUES = {"terminal_resource_insufficient", "rate_limited"}


def normalize_extensions(value: Any, *, field: str = "extensions") -> dict[str, Any]:
    """Return an opaque JSON extension object with stable, safe value types."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    for key in value:
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{field} keys must be non-empty strings")
    try:
        # Round-trip so callers cannot retain mutable non-JSON values in task state.
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must contain JSON-compatible values") from exc


def merge_extensions(*layers: tuple[str, Any]) -> dict[str, Any]:
    """Merge opaque extension namespaces, replacing each namespace atomically."""
    merged: dict[str, Any] = {}
    for field, value in layers:
        merged.update(normalize_extensions(value, field=field))
    return merged


def metadata(service: str) -> dict[str, Any]:
    capabilities: dict[str, list[str]] = {
        "hub": [
            "hub-api-v1",
            "inventory-v1",
            "manifest-dispatch-v1",
            "bearer-auth",
            "leases",
            "fixed-concurrency",
            "adaptive-concurrency",
            "resource-reservations-v1",
            "shared-host-placement",
            "direct-runner-push",
            "attempt-result-retention",
            "task-extensions-v1",
            "immutable-source-descriptor-v1",
            "cache-affinity-v1",
            "worker-cache-inventory-v1",
        ],
        "runner": [
            "runner-api-v1",
            "bearer-auth",
            "repo-phases-v1",
            "lease-renewal",
            "artifact-sha256",
            "resource-capacity-reporting",
            "shared-host-placement",
            "direct-runner-push",
            "resource-capacity-forwarding",
            "fixed-concurrency",
            "adaptive-concurrency",
            "task-extensions-v1",
            "immutable-source-descriptor-v1",
            "source-cache-v1",
        ],
        "matrix": [
            "matrix-cli-v1",
            "inventory-v1",
            "token-forwarding",
            "ssh-start",
            "long-poll",
            "direct-runner-pull",
            "direct-runner-push",
            "task-extensions-v1",
            "source-cache-config-v1",
            "cache-affinity-v1",
        ],
        "manifest": [
            "manifest-v1",
            "case-run-selection",
            "repo-phases-v1",
            "retry-contract",
            "execution-profile-v1",
            "task-extensions-v1",
            "immutable-source-descriptor-v1",
        ],
    }
    api_version = HUB_API_VERSION if service == "hub" else RUNNER_API_VERSION if service == "runner" else None
    return {
        "service": service,
        "product_version": PRODUCT_VERSION,
        "release_channel": RELEASE_CHANNEL,
        "core_preview_version": CORE_PREVIEW_VERSION,
        "cli_contract_version": CLI_CONTRACT_VERSION,
        "inventory_schema_version": INVENTORY_SCHEMA_VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "dispatch_schema_version": DISPATCH_SCHEMA_VERSION,
        "api_version": api_version,
        "capabilities": capabilities.get(service, []),
    }

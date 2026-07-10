"""Internal definitions for Loom Core Preview v1 public contracts."""

from __future__ import annotations

from typing import Any


CORE_PREVIEW_VERSION = "1.0.0-core-preview"
CLI_CONTRACT_VERSION = 1
INVENTORY_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
DISPATCH_SCHEMA_VERSION = 1
HUB_API_VERSION = 1
RUNNER_API_VERSION = 1
CONCURRENCY_POLICIES = {"fixed", "adaptive"}
FIXED_CONCURRENCY_BACKOFF_ISSUES = {"terminal_resource_insufficient", "rate_limited"}


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
            "direct-runner-push",
            "attempt-result-retention",
        ],
        "runner": [
            "runner-api-v1",
            "bearer-auth",
            "repo-phases-v1",
            "lease-renewal",
            "artifact-sha256",
            "direct-runner-push",
            "fixed-concurrency",
            "adaptive-concurrency",
        ],
        "matrix": [
            "matrix-cli-v1",
            "inventory-v1",
            "token-forwarding",
            "ssh-start",
            "long-poll",
            "direct-runner-pull",
            "direct-runner-push",
        ],
        "manifest": [
            "manifest-v1",
            "case-run-selection",
            "repo-phases-v1",
            "retry-contract",
        ],
    }
    api_version = HUB_API_VERSION if service == "hub" else RUNNER_API_VERSION if service == "runner" else None
    return {
        "service": service,
        "core_preview_version": CORE_PREVIEW_VERSION,
        "cli_contract_version": CLI_CONTRACT_VERSION,
        "inventory_schema_version": INVENTORY_SCHEMA_VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "dispatch_schema_version": DISPATCH_SCHEMA_VERSION,
        "api_version": api_version,
        "capabilities": capabilities.get(service, []),
    }

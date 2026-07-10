"""Internal definitions for the Loom v0.1.0 Core Preview and v1 protocols."""

from __future__ import annotations

from typing import Any


PRODUCT_VERSION = "0.1.0"
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
            "execution-profile-v1",
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

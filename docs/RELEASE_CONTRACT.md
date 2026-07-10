# Release Contract

This document defines the stable Core Preview v1 contract. A Loom release must
keep these boundaries intact even while the implementation evolves. The
versioned public CLI, inventory, manifest, dispatch, token, and capability
surface is listed in [Core Preview v1 Compatibility](CORE_PREVIEW_V1.md).

## V1 Task Contract

A manifest declares `schema_version: 1` and expands each runnable identity into:

```text
campaign_id + case_id + setting_id + run_id
```

The Hub assigns an `attempt_no` when that identity is leased. The task identity
does not change across retries; the attempt does. Each attempt gets a fresh
Runner work directory and one independently downloadable ZIP.

For repository tasks, `defaults.phases` declares ordered named phases. A case
can override one by name through `case.phases`, or append a new named phase.
Every phase may define `command`, `args`, `cwd`, `env`, `timeout_seconds`,
`continue_on_error`, and `artifact_paths`.

Parameter precedence is explicit:

```text
defaults.env < case.env < default phase.env < case phase.env < Loom runtime env
```

The Runner always injects the immutable values `LOOM_TASK_ID`,
`LOOM_ATTEMPT_NO`, `LOOM_WORKER_ID`, `LOOM_CAMPAIGN_ID`, `LOOM_CASE_ID`,
`LOOM_RUN_ID`, `LOOM_SETTING_ID`, `LOOM_PHASE_NAME`, and
`LOOM_PHASE_INDEX`. Phase `args` are rendered from the case/default context
before dispatch. See [Loom Manifest](TASK_MANIFEST.md) for the full shape.

Inventory declares `inventory_version: 1` and gives every worker an explicit
`initial_concurrency`, hard `max_concurrency`, and `concurrency_policy`. The
default `fixed` policy stays at its configured level except for an explicit
resource-insufficient or rate-limit backoff; `adaptive` is opt-in.

## Scheduling And Direct Push

Hub is the only task-state owner. A Direct Runner never receives an unleased
task and never owns a second queue.

`direct-worker-api` has two explicit dispatch modes:

- `pull`: the long-lived Runner claims work from Hub. This is the default.
- `push`: Hub leases one exact eligible task, then posts that lease to the
  Runner's authenticated `/api/tasks/execute` endpoint. The Runner reports
  `start`, result upload, completion/failure, and lease renewals back to Hub.

In both modes, long work renews its Hub lease while it runs. A failed delivery
whose outcome is unknown remains leased until normal recovery, rather than
being blindly dispatched twice.

## Authentication And Network Boundary

Hub defaults to `127.0.0.1`. Binding it outside loopback requires a bearer token
from `LOOM_HUB_TOKEN` (or an explicitly named equivalent). A Direct Runner also
defaults to `127.0.0.1`; binding its control API outside loopback requires a
separate bearer token from `LOOM_RUNNER_TOKEN`.

The Hub stores the *name* of a Direct Runner token environment variable in the
host registry, never the token value. The Hub host resolves that value only when
it delivers a push. Treat both endpoints as private control-plane services:
use private addressing, firewall rules, or a TLS-terminating proxy when traffic
leaves one trusted host. Loom does not provide TLS termination or identity
management in Core Preview.

## Result And Recovery Contract

Every repository result package includes at least:

- `task.json` with the leased identity and attempt;
- `worker-result.json` with process-level result metadata;
- `phase-results.json` with one status record per executed phase; and
- `artifact-manifest.json` with relative path, byte length, and SHA-256 for each
  declared artifact copied from the workspace.

Result ZIPs exclude the source checkout. Retried attempts remain queryable by
the same task ID and distinct `attempt_no`; a later clean attempt cannot erase a
failure package.

## Required Remote Release Check

The fixed [AgentDojo release fixture](AGENTDOJO_EXAMPLE.md) is the minimum
release gate. It uses two public cases and two `run_id` values, yielding four
task identities. Each identity intentionally records:

1. attempt 1: a retryable `network_unavailable` preflight failure; and
2. attempt 2: one real AgentDojo invocation and declared artifact collection.

The gate passes only when all four tasks finish clean on attempt 2 and all eight
attempt ZIPs are downloaded and hash-verified. A sanitized recovery summary is
exported separately; raw ZIPs and model output are not committed.

Run it only on an already-provisioned remote host with the fixture's declared
upstream execution environment and the two Loom bearer tokens available in that
host's environment. The exact command and cleanup sequence are in
[Remote Validation](REMOTE_VALIDATION.md#fixed-agentdojo-release-smoke).

## Non-Goals

Loom does not create, resize, stop, delete, or price cloud resources. The remote
smoke helper stops only the Hub and Runner processes it starts. The operator or
an external infrastructure workflow must stop and delete the temporary host
after evidence is copied out. See [Loom Scope](SCOPE.md).

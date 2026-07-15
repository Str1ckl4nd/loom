# Release Contract

This document defines the stable Core Preview v0.3 release contract. A Loom release must
keep these boundaries intact even while the implementation evolves. The
versioned public CLI, inventory, manifest, dispatch, token, and capability
surface is listed in [Core Preview v0.3 Compatibility](CORE_PREVIEW_V1.md) and
[Versioning](VERSIONING.md).

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

An optional `extensions` object lets integrations attach opaque JSON metadata
without depending on Loom internals. Loom preserves the final
`payload.extensions` value through Hub storage and into both `task.json` and
`worker-result.json` (`task_extensions`); it never interprets that value for
scheduling, retries, credentials, commands, or identity. Namespace keys merge
atomically by documented layer precedence.

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

For immutable Git task sources, cache locality is a soft preference after task
priority and before FIFO tie-breaking among otherwise equal queued tasks. It
never bypasses capability, retry, resource, placement, or concurrency checks.
`push-task` may omit `worker_id` to choose an eligible Direct Runner with the
same preference; a supplied worker ID remains exact.

After every Direct Push execution, the Runner sends a completion heartbeat even
when the task failed. That refreshes worker cache health and active-work facts
without changing the Hub-owned task outcome.

Worker `resource_capacity` and task `execution_profile` are part of the same
lease admission decision. Hub atomically reserves declared CPU, memory, disk,
and accelerator values for `leased` and `running` tasks. `shared` placement may
share a worker within those reservations; `exclusive` placement requires an
otherwise idle worker. This is scheduler-level admission only, not a sandbox or
OS-level resource guarantee.

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

When supplied, `payload.extensions` is retained in `task.json` and mirrored as
`task_extensions` in `worker-result.json`.

For a repository task with a cacheable immutable Git source,
`worker-result.json` additionally includes `source_cache`: cache key, hit/miss
or repair state, approximate cached bytes added, materialization duration, and
eviction or fallback facts. The cache key and canonical source identity are
safe to query; source credentials and cache contents are not exported.

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

Changes to immutable Git sources, Runner cache behavior, worker cache health, or
cache-affine selection must additionally pass the
[source-cache remote gate](CACHE_AFFINITY.md#remote-release-gate). It is a
four-task, two-Runner check for first fill, same-digest reuse, changed-digest
refresh, corrupt-cache repair, automatic cache-affine Direct Push, and
hash-verified result recovery. It requires no model provider and must run on a
fresh remote host; its source-transfer metric records cache-fill work rather
than public-internet bandwidth.

## Non-Goals

Loom does not create, resize, stop, delete, or price cloud resources. The remote
smoke helper stops only the Hub and Runner processes it starts. The operator or
an external infrastructure workflow must stop and delete the temporary host
after evidence is copied out. See [Loom Scope](SCOPE.md).

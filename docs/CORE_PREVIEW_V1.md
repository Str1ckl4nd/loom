# Core Preview v0.3 Compatibility

Loom Core Preview v0.3 freezes its public integration surface around versioned
files, command-line entry points, and authenticated HTTP metadata. Downstream
automation must use this surface. Importing a function from a file under
`tools/` is not a supported integration and carries no compatibility promise.

The product release is `v0.3.0`; the inventory, manifest, dispatch, Hub API,
and Runner API contracts remain independently versioned at `v1`.

## Version Discovery

The four public commands report their contract version without requiring a
running controller:

```bash
python3 tools/loom_manifest.py --version
python3 tools/loom_matrix.py --version
python3 tools/loom_hub.py --version
python3 tools/loom_runner.py --version
```

An authenticated Hub and Direct Runner expose their runtime capability document:

```bash
python3 tools/loom_hub.py capabilities \
  --controller http://CONTROL_HOST:8765

curl -sS -H "Authorization: Bearer $LOOM_HUB_TOKEN" \
  http://CONTROL_HOST:8765/api/meta

curl -sS -H "Authorization: Bearer $LOOM_RUNNER_TOKEN" \
  http://RUNNER_HOST:9876/api/meta
```

The document includes Core Preview version, CLI contract version, supported
inventory/manifest/dispatch versions, API version, and advertised capabilities.
Hub and Runner advertise `task-extensions-v1` when they preserve optional
user-owned `extensions` metadata end to end. Manifest, Hub, and Runner also
advertise immutable-source and cache capabilities when an immutable Git source
can be reused locally; inspect `capabilities` rather than importing `tools/`
functions.

## Frozen File Contracts

| File | Required version field | Current version |
| --- | --- | --- |
| Inventory JSON | `inventory_version` | `1` |
| Campaign manifest JSON or JSONL record | `schema_version` | `1` |
| Normalized dispatch payload | `schema_version` | `1` |

Hub rejects unversioned or unsupported dispatch payloads. Matrix rejects
unversioned inventories. Future incompatible changes require a new version
number instead of changing v1 interpretation.

The optional `extensions` object is an additive v1 field. It is carried through
normalization, Hub dispatch, and recovered result packages without changing
Loom scheduling or execution semantics. See [Loom Manifest](TASK_MANIFEST.md#extensions).

The smallest valid v1 inventory shape is:

```json
{
  "inventory_version": 1,
  "controller": {"connection_mode": "prestarted"},
  "controller_public_url": "http://CONTROL_HOST:8765",
  "workers": [
    {
      "worker_id": "worker-a",
      "host": "WORKER_HOST",
      "connection_mode": "ssh-start",
      "max_concurrency": 4,
      "initial_concurrency": 2,
      "concurrency_policy": "fixed",
      "source_cache_max_mb": 4096,
      "capabilities": ["linux"]
    }
  ]
}
```

`1 <= initial_concurrency <= max_concurrency` is required. `max_concurrency`
is an enforced Runner and Hub cap, not a tuning hint.

## Concurrency Policy

`fixed` and `adaptive` are explicit per-worker inventory fields:

- `fixed`: begins at `initial_concurrency`, stays there after clean runs and
  ordinary failures, and only steps down one level for a classified
  `terminal_resource_insufficient` or `rate_limited` result. It never probes
  upward automatically.
- `adaptive`: begins at `initial_concurrency` and uses Hub's resource-backed
  probe loop to raise or lower controller `desired_concurrency`, always bounded
  by `max_concurrency`.

The Runner also clamps every Hub instruction to its own hard maximum. An invalid
initial value fails registration instead of being silently altered.

## Resource Admission

The v1 manifest may add an `execution_profile` containing `placement` and
resource requests. Inventory may add worker `resource_capacity` overrides.
Loom treats both as scheduler contracts: leases reserve the declared CPU,
memory, disk, and GPU values against the Runner's reported capacity. It does
not turn those values into an operating-system isolation boundary.

`GET /api/data/worker-capacity` reports capacity, reservations, and available
headroom. `GET /api/data/task-admission?task_id=...` explains a task's current
eligibility per worker. The `worker-capacity` and `task-admission` Hub CLI
subcommands expose the same authenticated views.

## Immutable Source Cache

An immutable Git source with a complete commit receives the additive
`payload.source_descriptor` field. It gives a Runner a credential-free cache
identity; mutable refs do not receive one. A Runner reports its bounded local
cache through heartbeats, and Hub uses an available matching key only as a soft
same-priority scheduling preference. `GET /api/data/worker-cache`, the
`worker-cache` CLI command, and task-admission output expose the relevant
facts. See [Source Cache And Cache Affinity](CACHE_AFFINITY.md).

## Token And Startup Contract

The stable environment-variable names are `LOOM_HUB_TOKEN` and
`LOOM_RUNNER_TOKEN`, overridable only through the documented `--*-token-env`
options. Matrix forwards configured token values to remote Hub/Runners through
temporary mode-`0600` environment files and registers only the Direct Runner
token variable name with Hub.

Hub refuses a non-loopback bind without its token. A Direct Runner refuses a
non-loopback control bind without its separate token. The Runner registration
handshake includes `runner_api_version`; Hub rejects an incompatible Runner
before it can claim work.

See [Loom Manifest](TASK_MANIFEST.md), [Architecture](ARCHITECTURE.md), and
[Release Contract](RELEASE_CONTRACT.md) for behavior beyond the v1 compatibility
surface.

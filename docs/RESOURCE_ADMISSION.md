# Resource Admission

Loom schedules work on hosts you already own. A worker can therefore execute
more than one I/O-bound task at once, but one host should not accept more work
than its usable CPU, memory, disk, or accelerator budget.

Core Preview v1 adds scheduler-level resource reservations. A reservation is
created atomically with a task lease and released when that lease reaches a
non-active state. It applies to both pull claims and Direct Runner push.

## What It Does

- keeps `max_concurrency` as the hard count ceiling;
- adds CPU, memory, disk, and GPU admission checks before a task is leased;
- supports `shared` placement for multiplexed work and `exclusive` placement
  for a task that should occupy a worker by itself; and
- exposes capacity, active reservations, available headroom, and a per-task
  admission explanation through the Hub API and CLI.

This is scheduler admission, not operating-system isolation or a cgroup limit.
Loom does not claim that a `shared` task is sandboxed. Use `exclusive` when a
task needs scheduling exclusivity, and use an operator-managed container or
stronger isolation boundary when security isolation is required.

## Worker Capacity

Add an optional `resource_capacity` object to each inventory worker. Values are
upper bounds that the scheduler may reserve, not a request to create or resize
infrastructure.

```json
{
  "inventory_version": 1,
  "controller": {"connection_mode": "prestarted"},
  "controller_public_url": "http://CONTROL_HOST:8765",
  "workers": [
    {
      "worker_id": "evaluation-01",
      "host": "WORKER_HOST",
      "connection_mode": "ssh-start",
      "max_concurrency": 4,
      "initial_concurrency": 2,
      "concurrency_policy": "fixed",
      "resource_capacity": {
        "cpu_millis": 4000,
        "memory_mb": 6144,
        "disk_mb": 30000,
        "gpu_count": 0
      },
      "capabilities": ["linux"]
    }
  ]
}
```

All fields are optional. When a capacity field is omitted, Runner reports a
conservative value from the existing host when it registers. A complete
operator-supplied capacity is preferable for repeatable scheduling. GPU type
constraints can be expressed with `gpu_types` when `gpu_count` is positive.

## Task Execution Profile

Place an `execution_profile` in campaign defaults, a case/run record, or a
task payload. Case values override defaults; resource fields merge by name.

```json
{
  "defaults": {
    "execution_profile": {
      "placement": "shared",
      "resources": {
        "cpu_millis": 750,
        "memory_mb": 1536,
        "disk_mb": 2048
      }
    }
  },
  "cases": [
    {
      "case_id": "long-context",
      "setting_id": "baseline",
      "run_id": "001",
      "execution_profile": {
        "resources": {"memory_mb": 4096}
      }
    }
  ]
}
```

Supported fields are:

| Field | Meaning |
| --- | --- |
| `placement` | `shared` (default) or `exclusive`. |
| `resources.cpu_millis` | Scheduler CPU reservation in millicores. |
| `resources.memory_mb` | Scheduler memory reservation. |
| `resources.disk_mb` | Scheduler ephemeral-disk reservation. |
| `resources.gpu_count` | Required accelerator count. |
| `resources.gpu_types` | Optional acceptable accelerator types; requires positive `gpu_count`. |

An omitted profile means `shared` with zero resource requirements, preserving
the existing V1 behavior.

[`examples/loom-resource-admission.manifest.json`](../examples/loom-resource-admission.manifest.json)
contains one multiplexed task and one exclusive task using this contract.

## Admission And Inspection

Hub admits a task only when all three checks pass:

```text
capability eligible
AND active < min(desired_concurrency, max_concurrency)
AND reserved resources + requested resources <= worker capacity
```

An `exclusive` task also requires an empty worker; while it is leased or
running, the worker rejects other placements.

Inspect all worker headroom:

```bash
python3 tools/loom_hub.py worker-capacity \
  --controller http://CONTROL_HOST:8765
```

Explain why a queued task can or cannot land on each worker:

```bash
python3 tools/loom_hub.py task-admission \
  --controller http://CONTROL_HOST:8765 \
  --task-id campaign__case__setting__run-001
```

The authenticated HTTP equivalents are `GET /api/data/worker-capacity` and
`GET /api/data/task-admission?task_id=...`. Every leased task also records its
admission snapshot in `worker-result.json`, preserving the requested,
reserved, available, and concurrency state for later recovery. Hub counts its
own `leased` and `running` attempts immediately, so an old Runner heartbeat
cannot temporarily exceed the configured hard concurrency limit.

## Direct Repository Dispatch

`dispatch-repo-run` exposes the same profile without requiring a manifest:

```bash
python3 tools/loom_hub.py dispatch-repo-run \
  --controller http://CONTROL_HOST:8765 \
  --repo-url https://example.invalid/evaluation.git \
  --command 'python3 run_eval.py' \
  --placement exclusive \
  --cpu-millis 2000 \
  --memory-mb 4096
```

Resource admission does not manage VM lifecycle, container creation, billing,
or provider credentials. Those remain outside Loom's supported boundary.

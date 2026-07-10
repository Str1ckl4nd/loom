# AgentBenchmark Control Worker

Reliable, inventory-driven task delivery for agent benchmarks.

AgentBenchmark Control Worker dispatches normalized benchmark jobs across
operator-owned controller and worker hosts. The controller owns scheduling,
leases, adaptive concurrency, retries, and result intake. Workers execute
ordered task packages and return compact, queryable result ZIPs.

[Remote quick start](#remote-quick-start) |
[Task input](docs/TASK_INPUT_MANUAL.md) |
[Architecture](docs/ARCHITECTURE.md) |
[Support scope](docs/SUPPORT_SCOPE.md)

## What You Get

- **Explicit benchmark work units.** Model every runnable unit as a campaign,
  case, setting, and run instead of handing a worker an ambiguous instruction.
- **Controller-owned scheduling.** Workers advertise a hard capacity, while the
  controller controls leases, desired concurrency, retries, and recovery.
- **Remote worker connections that persist.** Bootstrap workers once over SSH,
  use long-polling for quieter idle periods, or expose a worker control API. The
  task queue and state machine always stay in the controller.
- **Recoverable evidence.** Each attempt keeps its task ID, attempt number,
  worker identity, logs, explicit artifacts, and result ZIP. A later successful
  retry does not erase the earlier failure package.
- **A small operating footprint.** The implementation uses the Python standard
  library and a SQLite-backed controller; no package installation is required.

## Supported Boundary

> [!IMPORTANT]
> This project starts after controller and worker hosts already exist. Automatic
> cloud resource creation, resizing, billing, teardown, and provider credential
> management are intentionally unsupported and are not on the roadmap.

Supply an operator-owned inventory, then use the control plane to deploy or
connect to processes on those hosts. The retained
`tools/tencent_cloud_provision.py`, `tools/tencent_cloud_e2e.py`, and
`tools/aws_linux_control_plane_smoke.py` files are historical/community
references, not supported interfaces. A maintained provisioning integration
needs a contributor-owned pull request with provider-specific tests, security,
cost, failure-recovery, and cleanup behavior.

Read the complete [support scope](docs/SUPPORT_SCOPE.md) before changing the
infrastructure boundary.

## Remote Quick Start

This is the supported path for dispatching work to an existing remote fleet.
Run these commands from an operator control environment; the actual benchmark
work runs on the hosts named in the inventory.

1. Start from
   [`examples/tencent-cloud-inventory.example.json`](examples/tencent-cloud-inventory.example.json)
   and replace its sample addresses, users, SSH key paths, controller URLs, and
   worker capabilities with your own existing hosts.
2. Define the campaign as explicit case/run records. The
   [task input manual](docs/TASK_INPUT_MANUAL.md) covers the schema, private
   source repositories, artifacts, retries, and expected outcomes.
3. Normalize the handoff into a controller dispatch specification:

   ```bash
   python3 tools/normalize_task_manifest.py campaign.json \
     --operator my-team \
     --output campaign.dispatch.json
   ```

4. Deploy or connect to the inventory, dispatch the specification, and wait for
   remote results:

   ```bash
   python3 tools/tencent_cloud_matrix.py \
     --inventory /path/to/operator-owned/inventory.json \
     --dispatch-spec campaign.dispatch.json \
     --output remote-run-summary.json
   ```

For a private source repository, add `--forward-env SOURCE_REPO_TOKEN` and make
that variable available only in the operator environment. The worker uses it
through `GIT_ASKPASS`; the token is not placed in task JSON, command logs, or
result ZIPs.

`tencent_cloud_matrix.py` deploys the control-plane scripts only. It does not
create, resize, stop, or delete cloud resources.

## How A Run Moves

```text
campaign manifest -> normalized dispatch spec -> controller -> workers -> result ZIPs -> query, retry, or recover
```

| Role | Owns |
| --- | --- |
| Operator | Existing hosts, network policy, credentials, and infrastructure lifecycle. |
| Controller | Task dispatch, leases, state transitions, desired concurrency, result intake, audit logs, and data queries. |
| Worker | Capability registration, heartbeats, task execution, artifact collection, and result upload. |

The controller is the single source of truth for task state. Workers report
execution facts; they do not operate an independent queue.

## Connection Modes

Choose a connection mode per host in the inventory:

| Mode | Use it when |
| --- | --- |
| `ssh-start` | SSH should bootstrap a long-lived worker, after which scheduling uses the controller HTTP API rather than a fresh SSH session for each task. |
| `long-poll` | An idle worker should keep a claim request open briefly instead of repeatedly polling the controller. |
| `direct-worker-api` | A worker-side HTTP endpoint should accept bootstrap commands to register or continue its pull loop. Task state still belongs to the controller. |

Inventory-level `ssh_control_persist` supports SSH `ControlMaster`/
`ControlPersist` reuse during setup. See the
[architecture guide](docs/ARCHITECTURE.md#control-plane) for placement and
networking details.

## Task Model And Recovery

Every runnable task has four mandatory identifiers:

```text
campaign_id + case_id + setting_id + run_id
```

The normalizer derives a stable task ID from those values. That makes an
individual run queryable, cancellable, retryable, and inspectable without
disturbing adjacent work. Optional retry policies can limit retries to known
transient categories and require the retry to land on a different capable
worker.

Repository tasks describe a source checkout, ordered commands, timeouts, and an
explicit artifact allowlist. Workers materialize the source in a per-task
workspace, then upload only metadata, command logs, and requested artifacts.
Full source checkouts are deliberately excluded from result packages.

See [Task Input Manual](docs/TASK_INPUT_MANUAL.md) for the full JSON/JSONL
schema and [Architecture](docs/ARCHITECTURE.md#repo-task-delivery) for the
delivery protocol.

## Documentation

| Guide | When to read it |
| --- | --- |
| [Support Scope](docs/SUPPORT_SCOPE.md) | Before changing host, provider, or resource-lifecycle behavior. |
| [Task Input Manual](docs/TASK_INPUT_MANUAL.md) | When preparing a campaign, retry policy, private source, or expected-result contract. |
| [AgentDojo Example](docs/AGENTDOJO_EXAMPLE.md) | When learning how a public benchmark repository can be described without turning the example into an end-to-end run. |
| [Architecture](docs/ARCHITECTURE.md) | When integrating the controller, worker, connection modes, concurrency behavior, or result APIs. |
| [Tencent Cloud Validation](docs/TENCENT_CLOUD_VALIDATION.md) | When validating the inventory-driven remote path on operator-supplied Tencent hosts. |

## Repository Map

```text
tools/control_plane_server.py       # controller API, SQLite state, and CLI
tools/controlled_worker.py          # controlled worker
tools/normalize_task_manifest.py    # campaign/case/run normalizer
tools/tencent_cloud_matrix.py       # inventory-driven remote runner
examples/tencent-cloud-inventory.example.json
docs/
```

## Contributing

Contributions are welcome. Please keep the ownership boundary intact: this
project coordinates work on supplied hosts, while infrastructure lifecycle
belongs to the operator or a separate infrastructure system. A proposal to
maintain automatic provisioning must include its provider-specific validation,
security, cost, recovery, and cleanup contract.

## License

[MIT](LICENSE).

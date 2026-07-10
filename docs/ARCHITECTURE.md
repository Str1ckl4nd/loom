# Architecture

## Roles

- Controller: owns task dispatch, leases, state transitions, worker
  `desired_concurrency`, result intake, scoring import, admin overrides, control
  logs, and data queries.
- Worker: registers capabilities, heartbeats, claims tasks, runs local payloads,
  uploads ZIP result packages, and reports completion/failure. Workers advertise
  a hard concurrency cap but do not own desired concurrency.
- Operator: may override state, cancel/retry work, and block/unblock workers.

## Infrastructure Boundary

The architecture begins with existing hosts. An operator or external
infrastructure system supplies controller and worker endpoints through an
inventory. Provider APIs for creating, resizing, billing, or deleting cloud
resources are outside the supported architecture and are not planned features.

The retained Tencent and AWS lifecycle scripts are historical validation
references only. Maintained provisioning belongs in a contributor-owned pull
request rather than in the controller/worker contract.

## Control Plane

Workers can be registered and started through a host registry, then use
controller-owned scheduling. Supported connection modes:

- `ssh-start`: the runner uses SSH to deploy/start a long-lived worker process.
  Task scheduling then uses HTTP pull; SSH is not used for every task.
- `long-poll`: same bootstrap as `ssh-start`, but empty claim requests stay open
  briefly so the worker keeps a stable controller request instead of tight
  polling.
- `direct-worker-api`: SSH starts a worker-side HTTP control endpoint. The
  runner or controller-side automation can call that endpoint to register the
  worker or start its pull loop. Task state still lives in the controller.

SSH bootstrap supports `ControlMaster`/`ControlPersist` through inventory
configuration, so repeated deploy/start commands can reuse the same SSH control
connection.

Controller placement is also inventory-driven:

- `ssh-start`: deploy and start the controller on a remote host.
- `prestarted`: use an existing controller API without starting a process.
- `local-process`: start the controller beside the matrix runner. Remote workers
  must receive an independently reachable `controller_worker_url` (for example,
  through a private route or operator-managed tunnel).

Default workers use pull-based scheduling:

```text
worker -> register
worker -> heartbeat
worker -> claim_task
controller -> task + lease
worker -> start / renew / complete / fail
```

This keeps the controller as the only task-state owner. Even when
`direct-worker-api` is enabled, the worker endpoint accepts control commands only
to register or start/continue the worker loop; it does not become an independent
task queue.

## Concurrency Control

Worker count, controller placement, and worker-local concurrency are independent:

- any number of workers can register with the controller;
- the controller can be local, remote, or on a cloud host as long as workers can
  reach its HTTP API;
- each worker has a hard `max_concurrency`, while the controller stores and
  returns `desired_concurrency`.

Claiming is capacity-bound:

```text
claim_limit = min(worker_request, desired_concurrency, max_concurrency) - active_runs
```

Workers run claimed tasks concurrently up to that returned capacity.

Workers execute one complete claim batch at a time. They drain the batch before
claiming again, so a concurrency level is proven by that many simultaneously
claimed runs rather than by unrelated sequential successes.

After result import, the controller updates worker tuning from the result ZIP:

- `resource_capacity` from worker heartbeat/result metadata;
- `resource_usage` from the completed task;
- verdict and known issue classification from stdout/stderr.

On Linux, each child command is measured independently with GNU `time -v`.
CPU time and peak RSS are aggregated per task without using process-lifetime
`RUSAGE_CHILDREN` counters, which would mix concurrent runs. The first healthy
concurrency-1 result is frozen as the single-run baseline; later contended runs
do not move the theoretical estimate.

Healthy runs use midpoint probes until the theoretical max, then one-by-one
probes until `theoretical_max + 10`. Unhealthy runs reduce concurrency and write
warning logs.

Tasks may declare a bounded category-based retry policy. After a matching
failure is imported, the controller records that attempt's result, increments
the task attempt, and requeues it. With `different_worker` enabled, the failed
worker is excluded and the controller verifies that another active capable
worker exists before requeueing. This prevents both same-host retry loops and
permanently queued work when only one suitable worker is available.

## Repo Task Delivery

The `repo` runner is the end-to-end task delivery path. A task payload describes:

- `source`: a git URL/ref/depth or a local materialization source.
- `commands`: ordered shell commands with optional per-command `cwd`, `env`, and
  `timeout_seconds`.
- `artifact_paths`: relative files, directories, or glob patterns to copy into
  the result package.

Workers clone or copy the source into a per-task workspace, execute commands, and
zip only controller/worker metadata, command logs, and explicit artifacts. The
workspace checkout itself is intentionally excluded from result ZIPs.

Private git repositories use a `source.token_env` name. The worker reads that
environment variable and supplies it through a temporary `GIT_ASKPASS` helper.
The token value is not written into task JSON, command logs, or result ZIPs.

## Data Plane

Result packages are uploaded as ZIP files. The controller imports the ZIP,
extracts a compatible summary when present, and stores a cursor-addressable
result row with the task's `attempt_no`. Multiple attempt packages retain the
same task ID but have independent result IDs, hashes, verdicts, and worker IDs.

`GET /api/results/{result_id}` downloads one ZIP. Temporary-cloud validation
downloads every result and verifies byte length and SHA-256 before returning.
Any later host teardown is owned by the operator's infrastructure workflow.

Supported result hints:

- `worker-result.json`
- `artifact-summary.json`
- `case-scheduler-summary.json`
- `score-summary.json`

## Control Logs

Control logs are stored in SQLite `control_logs` and appended to a JSONL file
when the server is started with `--control-log`.

Known issue categories:

- `terminal_resource_insufficient`
- `token_balance_insufficient`
- `rate_limited`
- `auth_failed`
- `network_unavailable`
- `run_error`
- `automatic_retry`

## Query Boundary

The generic data API uses field allowlists instead of raw SQL. This keeps local
automation flexible without exposing arbitrary database mutation through HTTP.

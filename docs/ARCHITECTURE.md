# Loom Architecture

## Roles

- **Operator** owns existing hosts, private routing, credentials, and cloud
  lifecycle.
- **Hub** owns dispatch, leases, task state, desired worker concurrency, retries,
  result intake, result queries, and audit events.
- **Runner** advertises capability/capacity, executes a leased task, renews its
  lease, uploads an attempt ZIP, and reports facts back to Hub.

Hub is the only task-state owner. A Runner never owns an independent task queue.

## Design Goal: Reuse Existing Capacity

Loom treats a task attempt, not a new virtual machine or a permanently reserved
worker, as the schedulable unit. An operator supplies the hosts; Hub combines
per-worker concurrency with task-level resource reservations to decide whether a
compatible attempt can share a host. This lets unrelated cases and runs make
useful progress on spare capacity while preserving a distinct lease, attempt
number, logs, artifacts, and result package for each execution.

The model is deliberately bounded: scheduler reservations are admission
decisions, not cgroup enforcement, and cache affinity is a preference rather
than a requirement. Loom does not provision, resize, or own cloud instances. It
improves utilization of the capacity an operator already has.

## Connection Modes

The inventory selects a mode per Runner:

| Mode | Transport after bootstrap | Best use |
| --- | --- | --- |
| `ssh-start` | Hub HTTP pull | Start a persistent Runner once over SSH; do not open a new SSH command for every task. |
| `long-poll` | Hub HTTP long-poll | Reduce idle request churn while preserving pull scheduling. |
| `direct-worker-api` / `pull` | Runner HTTP control endpoint plus Hub pull | Reach a persistent Runner API while it claims work normally. |
| `direct-worker-api` / `push` | Hub-to-Runner authenticated POST | Let Hub lease one exact eligible task and actively deliver it to the Runner. |

`ssh_control_persist` enables SSH `ControlMaster`/`ControlPersist` during
bootstrap. It does not become the task protocol.

### Pull Flow

```text
Runner -> register / heartbeat / claim
Hub    -> leased task
Runner -> start / renew / upload / complete or fail
```

### Direct Push Flow

```text
operator -> Hub push-task(task_id, worker_id)
Hub      -> eligibility + capacity check + atomic lease
Hub      -> Direct Runner /api/tasks/execute (leased task only)
Runner   -> Hub start / renew / upload / complete or fail / completion heartbeat
```

The Direct Runner validates that the task is assigned to its own worker ID and
does not accept an active push while its pull loop is running. Hub retains the
lease if delivery outcome is unknown, avoiding duplicate execution. Normal lease
recovery handles a genuinely lost Runner.

## Authentication And Placement

Hub binds to `127.0.0.1` by default. A non-loopback Hub bind requires
`LOOM_HUB_TOKEN` (or `--auth-token-env NAME`). The same bearer token authenticates
Hub API clients and Runners.

Direct Runner binds to `127.0.0.1` by default. A non-loopback Direct API requires
the separate `LOOM_RUNNER_TOKEN` (or `--direct-api-token-env NAME`). Hub stores
only the token *environment variable name* associated with a Direct Runner; it
does not receive or persist the token in inventory data.

The Core Preview transport is HTTP with bearer authentication. Keep it on private
addresses or behind an operator-managed TLS proxy and firewall when traffic
crosses a host boundary. TLS termination, certificate lifecycle, and identity
provider integration are outside this release contract.

## Task And Attempt State

Loom derives a stable `task_id` from campaign/case/setting/run. Hub assigns an
`attempt_no`; automatic retry keeps the task ID, increments only the attempt,
and retains the old result package.

Leases are time-bound. Runners renew them during execution and upload. The Hub
enforces worker capability, a configured hard `max_concurrency`, and
controller-owned `desired_concurrency` before either pull claim or Direct Push.

The capacity check is conceptually:

```text
active < min(desired_concurrency, max_concurrency)
```

`active` includes Hub-recorded leased/running work so a Direct Push cannot race
past capacity merely because a Runner has not yet sent its next heartbeat.

### Concurrency Policies

The inventory chooses `fixed` or `adaptive` for each worker. Both start at the
declared `initial_concurrency`, and neither can exceed `max_concurrency`.

- `fixed` holds its configured level after healthy or ordinary failed tasks. It
  backs off one level only for a classified resource-insufficient or rate-limit
  result.
- `adaptive` uses the existing resource-aware probe sequence to change Hub
  `desired_concurrency` after results.

The Runner independently clamps Hub responses to its own maximum. This makes
the maximum a hard safety boundary even if a controller is misconfigured.

### Resource Admission

Concurrency is a count ceiling; it does not say whether two tasks can fit in
the same host's CPU, memory, disk, or accelerator budget. Workers may report a
`resource_capacity`, and tasks may declare an `execution_profile` with a
resource request and `shared` or `exclusive` placement.

Hub computes active reservations from its own `leased` and `running` rows while
it holds the same transaction that creates a lease. Both pull and Direct Push
must satisfy:

```text
reserved + requested <= resource_capacity
```

`exclusive` requires no active lease and prevents further leases until its task
becomes non-active. This is a scheduling reservation, not a container, cgroup,
or security boundary. [Resource Admission](RESOURCE_ADMISSION.md) defines the
public fields and inspection APIs.

### Source Cache Affinity

For a pinned Git commit, Runner maintains a bounded local mirror and makes a
fresh writable worktree per attempt. Its heartbeat advertises only the derived
cache keys. Hub ranks a matching key as a soft preference after priority; it
does not turn a miss into an admission failure. The same rule applies when Hub
automatically selects a Direct Runner for Push. Hub never stores cached source
data. See [Source Cache And Cache Affinity](CACHE_AFFINITY.md).

## Repo Task Delivery

The V1 repository contract is phase-oriented:

```text
materialize source -> prepare -> evaluate -> collect -> declared artifacts -> ZIP
```

Each attempt gets a fresh work directory. The Runner writes phase exit records,
copies only declared relative artifacts, calculates their SHA-256 values, and
excludes the checkout itself from the ZIP. A phase may have its own command,
args, cwd, env, timeout, error-continuation setting, and artifact patterns.

See [Loom Manifest](TASK_MANIFEST.md) for parameter precedence and the exact
runtime environment injection contract.

## Data And Recovery Plane

Hub stores a cursor-addressable row for each uploaded ZIP. It records task ID,
attempt number, worker identity, byte length, SHA-256, verdict, and parsed
metadata. A recovery client downloads each package by result ID and verifies
both byte length and hash.

Required repository-package files are:

- `task.json`;
- `worker-result.json`;
- `phase-results.json`; and
- `artifact-manifest.json`.

Result packages may also include command logs and explicit artifacts. The fixed
[AgentDojo release fixture](AGENTDOJO_EXAMPLE.md) validates recovery of eight
packages across two cases, two runs, and two attempts.

### Oracle, Trajectory, And Reward Plane

An execution attempt can optionally request a v1 Oracle. After Hub persists the
execution ZIP, it creates a separate child task that references that exact ZIP
by result ID, byte length, SHA-256, and parent attempt number. The assigned
Runner downloads and verifies the input before running the Oracle. The child has
its own capability, resource admission, lease, state, result package, and retry
history.

```text
execution task -> result ZIP -> Oracle child lease -> verified download -> Oracle ZIP + semantic outcome
```

The normal Hub task state remains a process fact. `pass`, `fail`, `error`, and
`inconclusive` live in a separate Oracle outcome record, so an Oracle retry never
releases or reruns a successful expensive execution attempt. Selector-based
recovery includes both sides of a semantic decision and verifies the ZIP hashes
on download.

Trajectory capture is absent by default. An opt-in execution payload supplies a
relative raw path, a distinct sanitized export path, a byte limit, and redaction
patterns. Runner injects the raw path into the process, redacts the structured
document, removes the raw source before artifact collection, and writes a
trajectory receipt. Reward data is owned by the Oracle output and remains an
exported semantic field, not a scheduler input. See [Oracle, Trajectory, And
Reward](ORACLE_TRAJECTORY_REWARD.md).

## Infrastructure Boundary

Loom starts after hosts exist. Cloud creation, rescaling, cost selection,
termination, billing, VPC/security-group management, and provider credentials
are not supported Loom features and are not on the roadmap. The retained
Tencent/AWS lifecycle helpers are historical references only. A maintained
provider integration must be proposed and owned in a separate pull request.

See [Loom Scope](SCOPE.md) and [Release Contract](RELEASE_CONTRACT.md) for the
operational boundary.

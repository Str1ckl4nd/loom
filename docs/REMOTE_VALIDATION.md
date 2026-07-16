# Loom Remote Validation

Use Loom Matrix for remote validation. Do not use GitHub Actions for evaluation
tests.

## Fixed AgentDojo Release Smoke

Every Core Preview release uses the fixed two-case, two-run, two-attempt
AgentDojo check described in [AgentDojo Release Fixture](AGENTDOJO_EXAMPLE.md).
It runs on one fresh, already-provisioned remote host because the broader
multi-shape fleet path has a separate validation history.

The fixture has four task identities and eight retained result packages:

```text
2 cases x 2 runs x 2 attempts = 8 result ZIPs
```

Attempt 1 is an intentional retryable preflight failure. Attempt 2 performs the
real minimal AgentDojo invocation. This tests independently schedulable
`case_id`, `run_id`, and `attempt_no` without turning the release gate into
eight paid model calls.

On the remote host only, make the fixture's upstream execution environment and
ephemeral Loom bearer tokens available in its process environment, then run:

```bash
export LOOM_HUB_TOKEN="$(openssl rand -hex 32)"
export LOOM_RUNNER_TOKEN="$(openssl rand -hex 32)"
export LOCAL_LLM_PORT=18000

python3 tools/loom_agentdojo_remote_smoke.py \
  --repo-root "$PWD" \
  --source-path /srv/cache/agentdojo-v0.1.35 \
  --runtime-dir /tmp/loom-agentdojo-eight-slot \
  --export-dir /tmp/loom-agentdojo-eight-slot/recovered
```

The optional `--source-path` is a remote checkout verified against the
fixture's pinned tag before the temporary manifest uses it. It prevents eight
identical network clones while preserving a separately copied workspace for
every attempt.

The helper runs the remote unit suite, starts an authenticated loopback-only Hub
and Direct Runner in `push` mode, validates all eight downloads and SHA-256
values, writes a redacted recovery export, and stops those two processes in a
`finally` path. It does not stop or delete the VM. After copying only the
redacted recovery export and the summary you need, the operator must explicitly
stop/delete the temporary instance and confirm it is no longer billable.

The committed fixture uses AgentDojo's `LOCAL` channel so a remote
OpenAI-compatible test endpoint must be available at `LOCAL_LLM_PORT`. A real
provider run is an operator choice: override `--model` and pass each required
provider environment variable through `--require-env NAME`. Neither choice
changes Loom's Core Preview contract.

## Immutable Source Cache Release Gate

When a change affects immutable source delivery, the Runner cache, cache health,
or cache-affine scheduling, run the dedicated source-cache gate on a separate
fresh remote host. It has no model dependency and does not contact a public
source repository: the helper creates an ephemeral two-commit Git fixture on the
remote host, starts a warm and a cold Direct Runner, and checks four normal
result packages.

```bash
export LOOM_HUB_TOKEN="$(openssl rand -hex 32)"
export LOOM_RUNNER_TOKEN="$(openssl rand -hex 32)"
RUNTIME="$(mktemp -d /tmp/loom-source-cache-release.XXXXXX)"

python3 tools/loom_source_cache_remote_smoke.py \
  --repo-root "$PWD" \
  --runtime-dir "$RUNTIME" \
  --export-dir "$RUNTIME/recovered"
```

The gate requires an initial miss with source transfer, a same-digest hit with
zero source transfer, automatic Direct Push to the warm Runner, a changed digest
with a new cache key, and repair after a deliberately corrupted mirror. It
hash-verifies every result ZIP and records source-transfer bytes,
materialization time, cache disk bytes, dispatch-to-clean time, and queue delay.
Its transfer metric is a deterministic cache-fill proxy, not an external network
benchmark. The default invocation first runs the repository unit suite and
records its pass status in the redacted acceptance export.

`RUNTIME` must be empty when the command starts. After retaining only the
redacted acceptance report, remove it and explicitly stop/delete the temporary
instance. Confirm there are no billable resources remaining before considering
the remote validation complete.

The repository keeps only the redacted acceptance report in
[`examples/source-cache/recovered/`](../examples/source-cache/recovered/); raw
ZIPs, temporary paths, and host identity remain outside Git.

## Oracle, Trajectory, And Reward Release Gate

Changes to Oracle dispatch, verified parent-result transfer, trajectory export,
reward output, or semantic result selection require this lightweight gate on one
fresh remote host. It has no model-provider or benchmark dependency: the remote
test starts authenticated loopback-only Hub and Direct Runner processes, then
proves the public contract around one execution ZIP and independently scheduled
Oracle attempts.

```bash
RUNTIME="$(mktemp -d /tmp/loom-oracle-contract-release.XXXXXX)"

python3 tools/loom_oracle_remote_smoke.py \
  --repo-root "$PWD" \
  --runtime-dir "$RUNTIME" \
  --export-dir "$RUNTIME/recovered"
```

The targeted check covers semantic `pass`, `fail`, `error`, and `inconclusive`,
an `oracle_error` child retry that does not rerun its parent, token-authenticated
result transfer, trajectory redaction and raw-file exclusion, reward fields, and
all four recovery selectors. By default the helper also runs the complete unit
suite on that remote host. It emits a small redacted
`oracle-release-acceptance.json`; raw ZIPs, trajectory content, command strings,
logs, runtime paths, and host identity must not enter Git.

Copy only the redacted acceptance export into
[`examples/oracle/recovered/`](../examples/oracle/recovered/) after success.
Remove `RUNTIME`, explicitly stop/delete the temporary instance, and confirm
that no billing resources remain before treating the validation as complete.

## Support Boundary

This project supports validation on existing, operator-supplied hosts. The
supported entry point is an `inventory.json` passed to
`tools/loom_matrix.py`.

Creating or deleting CVMs, VPCs, subnets, security groups, cloud SSH keys, and
other provider resources is explicitly out of scope and is not planned. The
operator owns cloud credentials, host selection, billing, and teardown. The
retained provisioning scripts are historical/community references only; anyone
who needs maintained resource lifecycle support should submit and own a pull
request. See `SCOPE.md`.

## Purpose

Loom Matrix validates that these dimensions are decoupled:

- worker count: any positive number of Tencent CVM hosts can join or leave
  independently; this validation deliberately uses five.
- Hub location: the controller URL is a config value and may be local, remote,
  public, or private.
- worker internal concurrency: each worker advertises a hard cap, while the
  controller owns `desired_concurrency`.

## Host Matrix

Provide one controller host and five worker hosts with different CPU/memory
shapes. Do not include unrelated instances in the inventory.

A low-cost spread for `ap-guangzhou-6` (always recheck live inventory before a
run):

| Worker | Shape | Intent |
| --- | --- | --- |
| `tc-2c2g` | `SA2.MEDIUM2` | smallest worker floor |
| `tc-2c4g` | `SA3.MEDIUM4` | memory comparison at same CPU |
| `tc-2c8g` | `SA2.MEDIUM8` | memory-heavy comparison |
| `tc-4c8g` | `S6.LARGE8` | medium throughput |
| `tc-8c16g` | `SA5.2XLARGE16` | high ceiling / over-theory probe |

Copy `examples/loom-inventory.example.json`, replace hosts, users, key
paths, and private controller URL.

Each worker may choose a connection mode:

- `ssh-start`: register the host by SSH metadata, SSH once to deploy/start the
  long-lived worker, then use HTTP pull for task scheduling.
- `long-poll`: like `ssh-start`, but the worker holds empty claim requests open
  to reduce request churn and keep a steadier controller connection.
- `direct-worker-api`: SSH starts a worker-side HTTP endpoint on
  `command_port`. Set `direct_api_dispatch_mode` to `pull` to start/continue the
  worker loop, or `push` to leave the loop idle and let Hub actively deliver an
  exact lease to `/api/tasks/execute`. Task state, leases, and concurrency remain
  controller-owned in both modes.

Set `connection_defaults.ssh_control_persist` to reuse SSH control connections
during deployment. This avoids a cold SSH handshake for every setup command.

The controller chooses one independent mode in the inventory:

- `ssh-start`: an operator-provided remote controller host.
- `prestarted`: an existing API supplied by URL.
- `local-process`: a controller process beside the runner, plus a
  `controller_worker_url` reachable by cloud workers. Do not use this mode when
  local execution is prohibited.

## Retained Resource Lifecycle References

`tools/loom_tencent_provision_reference.py` and `tools/loom_tencent_e2e_reference.py` remain in the
repository to preserve the original 2026 validation path. They are unsupported
reference implementations, not recommended setup commands or stable project
interfaces. Their provider assumptions, pricing choices, permissions, and
cleanup behavior are outside the maintenance plan.

Do not make benchmark dispatch depend on those scripts. Supply an inventory
from infrastructure owned elsewhere. Improvements that turn provisioning into
a maintained feature require a contributor-owned pull request.

## Run

Normalize a campaign:

```bash
python3 tools/loom_manifest.py campaign.json \
  --operator remote-operator \
  --output campaign.dispatch.json
```

Start the Tencent matrix:

```bash
python3 tools/loom_matrix.py \
  --inventory /path/to/operator-owned/inventory.json \
  --dispatch-spec campaign.dispatch.json \
  --output tencent-matrix-summary.json
```

`loom_matrix.py` deploys only Loom Hub and Loom Runner scripts. It does not
create, resize, or destroy CVMs. That inventory-driven boundary is the supported
contract.

Set `LOOM_HUB_TOKEN` before a non-loopback Matrix Hub startup. For a Direct
Runner, also set `LOOM_RUNNER_TOKEN` (or choose explicit environment-variable
names in the inventory). Matrix forwards those values through temporary `0600`
environment files and records only names, never their values, in inventory.
Use `--forward-env SOURCE_REPO_TOKEN` for private source repositories.

## Autonomous Five-Host Validation

Generate two repo-independent input campaigns: one with enough CPU-bound work
to prove every concurrency batch through theoretical maximum plus 10, and one
with six known failure classes plus a cross-worker retry probe. The fixed
[AgentDojo release fixture](AGENTDOJO_EXAMPLE.md) is intentionally separate: it
is the small per-release gate, while this larger matrix remains the broad fleet
validation path.

```bash
RUN_DIR=.cloud-runs/loom-matrix-$(date +%Y%m%d%H%M%S)
mkdir -p "$RUN_DIR"
python3 tools/loom_validation_campaigns.py --output-dir "$RUN_DIR"
python3 tools/loom_manifest.py "$RUN_DIR/concurrency-calibration.json" \
  --operator tencent-e2e --output "$RUN_DIR/concurrency-calibration.dispatch.json"
python3 tools/loom_manifest.py "$RUN_DIR/failure-injection.json" \
  --operator tencent-e2e --output "$RUN_DIR/failure-injection.dispatch.json"
```

Run all phases against an operator-owned inventory:

```bash
INVENTORY=/path/to/operator-owned/inventory.json
python3 tools/loom_matrix.py \
  --inventory "$INVENTORY" \
  --dispatch-spec "$RUN_DIR/concurrency-calibration.dispatch.json" \
  --dispatch-spec "$RUN_DIR/failure-injection.dispatch.json" \
  --expected-workers 5 \
  --require-concurrency-stable \
  --require-log-category terminal_resource_insufficient \
  --require-log-category token_balance_insufficient \
  --require-log-category rate_limited \
  --require-log-category auth_failed \
  --require-log-category network_unavailable \
  --require-log-category run_error \
  --require-log-category automatic_retry \
  --retry-task-id loom-failure-injection__failure-rate_limited__known-failure-classification__run-003 \
  --timeout-seconds 3600 \
  --output "$RUN_DIR/tencent-matrix-summary.json"
```

The matrix runner downloads every result ZIP, checks its byte count and SHA-256,
and stores it below `result-packages/<task_id>/` before returning. It checks every
normalized `expected` contract, including a deterministic synthetic network
failure that must recover on attempt 2 using a different worker. Host teardown
remains the operator's responsibility.

## Autonomous Concurrency Behavior

Each worker starts at concurrency `1`. The worker reports CPU/memory capacity and
per-run resource usage. The controller computes:

```text
memory_estimate = floor(0.80 * total_memory_mb / single_run_max_rss_mb)
cpu_estimate    = floor(cpu_count / observed_cpu_fraction)
theoretical_max = min(memory_estimate, cpu_estimate)
probe_limit     = theoretical_max + 10
```

Healthy runs move `desired_concurrency` toward `theoretical_max` by midpoint
probes. After reaching the theoretical max, healthy probes increase one at a
time. If the worker stays healthy through `theoretical_max + 10`, the controller
keeps that value and writes an `info` log.

Unhealthy runs reduce concurrency and write `warning` logs. Known issue classes:

- `terminal_resource_insufficient`
- `token_balance_insufficient`
- `rate_limited`
- `auth_failed`
- `network_unavailable`
- `run_error`
- `automatic_retry`

## Evidence To Collect

After the run, inspect:

- `tencent-matrix-summary.json`: workers, tasks, results, control log.
- `loom-hub.tail.jsonl`: Loom Hub JSONL log from the remote host.
- controller artifacts under `/tmp/loom/artifacts`.

Completion evidence must show:

- all five worker IDs registered with different resource reports;
- controller-owned `desired_concurrency` changed independently per worker;
- case/run task IDs are individually queryable and retryable;
- every result identifies its task attempt and worker, including retained
  failure packages from successful retries;
- resource, token, rate-limit, or other known failures appear in `control_logs`
  when triggered by the remote workload.

## Verified Remote Run

The 2026-07-10 Tencent run in
`.cloud-runs/loom-matrix-20260710044602/` used five workers
(`SA2.MEDIUM2`, `SA3.MEDIUM4`, `SA2.MEDIUM8`, `S6.LARGE8`, and
`SA5.2XLARGE16`) across `long-poll`, `ssh-start`, and `direct-worker-api` modes.
It used the now-unsupported lifecycle reference helper; that historical choice
does not expand the current support scope.

- 698 tasks reached final state: 693 `clean` and 5 intentionally retained
  `run_error` cases.
- all 698 declared task expectations matched;
- all documented repository-delivery check tasks completed `clean` on attempt 1;
- the synthetic network task retained attempt 1 from one worker and completed
  attempt 2 on a different worker;
- 700 result ZIPs were downloaded with no length or SHA-256 mismatch;
- all workers reached their controller-calculated theoretical maximum plus 10:
  `12`, `12`, `12`, `14`, and `18`;
- cleanup deleted six temporary CVMs, the key, security group, subnet, and VPC,
  and live API verification found no remaining run resources.

The `.cloud-runs/` directory is intentionally ignored by Git because it contains
large result packages and transient cloud credentials.

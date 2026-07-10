# Loom Remote Validation

Use Loom Matrix for remote validation. Do not use GitHub Actions for evaluation
tests.

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
  `command_port`. The matrix runner calls that endpoint to start the worker loop.
  Task state, leases, and concurrency remain controller-owned.

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

Use `--forward-env SOURCE_REPO_TOKEN` for private source repositories. The value
is copied to a temporary 0600 env file on each worker and is not stored in the
inventory.

## Autonomous Five-Host Validation

Generate two repo-independent input campaigns: one with enough CPU-bound work
to prove every concurrency batch through theoretical maximum plus 10, and one
with six known failure classes plus a cross-worker retry probe. The
[AgentDojo Example](AGENTDOJO_EXAMPLE.md) is documentation only; it is not a
remote validation phase and is deliberately excluded from this matrix.

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

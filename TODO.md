# TODO

## v0.1.0 Release Baseline

- [x] Freeze the product release as `v0.1.0` Core Preview while keeping the
  independently versioned inventory, manifest, dispatch, and API contracts at
  `v1`.

## Release Contract And AgentDojo Example

- [x] Add a versioned `phases` manifest contract that preserves the current
  `commands` format. A phase needs a name, command, optional args, environment,
  working directory, timeout, and declared artifacts. Define and document the
  precedence as campaign defaults, case/run overrides, phase overrides, then
  immutable Loom runtime metadata.
- [x] Inject `campaign_id`, `case_id`, `setting_id`, `run_id`, `attempt_no`,
  worker ID, and phase identity into the phase process. Emit a structured phase
  report and an artifact manifest with paths, byte counts, and hashes.
- [x] Keep `case` and `run` independently selectable at dispatch time. Keep
  `attempt` controller-owned: a retry creates the next attempt for the same
  task/recovery identity rather than accepting an arbitrary attempt number.
- [x] Make remote Hub and direct Runner APIs secure by default: loopback-only
  bind defaults, bearer-token configuration through environment variables, and
  authenticated registration, dispatch, result upload, and direct-worker calls.
- [x] Add remote-only automated coverage for manifest normalization, per-phase
  injection, case/run filtering, retained retry attempts, result recovery,
  and authenticated HTTP APIs. Run deterministic coverage on an existing remote
  host; run the model-backed smoke test as a remote release job rather than on
  every commit.
- [x] Add a minimal, pinned AgentDojo campaign with two independently selectable
  case/run records and a retry demonstration. Run it on a new inexpensive
  remote host, collect and redact the returned result package, and publish the
  recovered `task.json`, phase report, score/log summary, and artifact manifest
  under `examples/agentdojo/`.
- [x] Keep cloud resource creation out of Loom's supported scope. The remote
  validation recipe must explicitly include host shutdown/cleanup as its final
  step.

## Next: Content-Addressed Cache And Cache-Affinity Scheduling

- [x] Add an immutable Git source descriptor to the manifest and dispatch
  contract using a canonical URL plus resolved commit. Generic input bundles
  still need a separate URI, byte-length, and SHA-256 contract.
- [x] Give every Runner a bounded, lock-protected local Git source cache. A
  hit creates a fresh writable worktree per attempt; fill, verification failure,
  eviction, and corruption recovery are observable. Generic input caching is
  still pending.
- [x] Make cache locality a soft scheduling preference for both Pull and Direct
  Push. Capability, resource admission, priority, and fairness remain hard
  constraints; a cache miss never blocks an otherwise eligible task.
- [x] Report cache key, hit/miss, transferred bytes, materialization time, and
  eviction facts in Runner results and Hub queries without exposing source
  credentials or turning Hub into a required blob store.
- [ ] Extend the fixed remote release gate with same-digest reuse, changed-
  digest refresh, and corrupt-cache recovery checks, then benchmark the
  network, wall-clock, disk, and queueing impact on existing hosts.

## After Cache: Oracle, Trajectory, And Reward

- [ ] Add a separately schedulable `oracle` contract. Hub should queue it from
  an execution result reference, with its own capability, resource profile,
  timeout, retry policy, and attempt history.
- [ ] Keep execution and Oracle outcomes independent: execution success/error/
  timeout must not be confused with Oracle `pass`, `fail`, `error`, or
  `inconclusive`. Oracle retries must not rerun a successful expensive Agent
  attempt.
- [ ] Add explicit recovery/export selectors such as `all_attempts`,
  `execution_clean`, `oracle_decided`, and `oracle_pass`. Retain raw attempt
  ZIPs by default; selectors control export, never silent evidence deletion.
- [ ] Define an opt-in, versioned trajectory export contract for agent messages,
  tool calls, observations, timings, and artifact references. Include redaction
  and size policies; raw trajectory capture must remain disabled by default.
- [ ] Define a versioned reward contract owned by Oracle output, supporting a
  scalar reward, optional named components, score metadata, Oracle version, and
  evidence references. Loom exports this data but does not become an RL trainer.
- [ ] Add remote release coverage for Oracle pass/fail/error/inconclusive,
  trajectory redaction, reward integrity, and export selection across retries.

## Resource Multiplexing And Local Mode

- [x] Make shared-host resource multiplexing an explicit Loom capability with
  task resource reservations and `shared` / `exclusive` placement.
- [x] Document that the scheduler improves use of existing capacity without
  claiming an advantage for CPU-bound or strict-isolation workloads.
- [ ] Benchmark throughput, CPU and memory utilization, queueing delay, failure
  interference, and cost per completed task at different concurrency levels.
- [ ] Add operator-owned isolation adapters for direct host execution, separate
  processes, containers, or stronger external sandboxes without making Loom a
  cloud lifecycle tool.
- [ ] Add a one-command local mode that starts an embedded Hub and localhost
  Runner automatically while preserving the same scheduling, lease, retry, and
  result-recovery semantics as multi-host mode.

# TODO

## Release Contract And AgentDojo Example

- [ ] Add a versioned `phases` manifest contract that preserves the current
  `commands` format. A phase needs a name, command, optional args, environment,
  working directory, timeout, and declared artifacts. Define and document the
  precedence as campaign defaults, case/run overrides, phase overrides, then
  immutable Loom runtime metadata.
- [ ] Inject `campaign_id`, `case_id`, `setting_id`, `run_id`, `attempt_no`,
  worker ID, and phase identity into the phase process. Emit a structured phase
  report and an artifact manifest with paths, byte counts, and hashes.
- [ ] Keep `case` and `run` independently selectable at dispatch time. Keep
  `attempt` controller-owned: a retry creates the next attempt for the same
  task/recovery identity rather than accepting an arbitrary attempt number.
- [ ] Make remote Hub and direct Runner APIs secure by default: loopback-only
  bind defaults, bearer-token configuration through environment variables, and
  authenticated registration, dispatch, result upload, and direct-worker calls.
- [ ] Add remote-only automated coverage for manifest normalization, per-phase
  injection, case/run filtering, retained retry attempts, result recovery,
  and authenticated HTTP APIs. Run deterministic coverage in CI; run the
  model-backed smoke test as a remote release job rather than on every commit.
- [ ] Add a minimal, pinned AgentDojo campaign with two independently selectable
  case/run records and a retry demonstration. Run it on a new inexpensive
  remote host, collect and redact the returned result package, and publish the
  recovered `task.json`, phase report, score/log summary, and artifact manifest
  under `examples/agentdojo/`.
- [ ] Keep cloud resource creation out of Loom's supported scope. The remote
  validation recipe must explicitly include host shutdown/cleanup as its final
  step.

Estimated active implementation time: about 5-7 hours, plus 30-90 minutes for
the model-backed remote smoke test when provider credentials and quota are
available.

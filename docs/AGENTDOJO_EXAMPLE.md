# AgentDojo Release Fixture

[`examples/agentdojo/agentdojo-eight-slot.manifest.json`](../examples/agentdojo/agentdojo-eight-slot.manifest.json)
is Loom's fixed remote release regression. It is deliberately small enough for
one inexpensive already-provisioned worker while exercising the three scheduler
dimensions that matter:

| Dimension | Fixed values |
| --- | --- |
| `case_id` | `workspace-user-task-0`, `workspace-user-task-1` |
| `run_id` | `001`, `002` for each case |
| `attempt_no` | retryable attempt 1, real AgentDojo attempt 2 |

That is four task identities and eight retained, independently recoverable
attempt packages. It is not eight paid benchmark evaluations: attempt 1 fails
before AgentDojo intentionally, so attempt 2 runs the four real minimal
evaluations. This makes retry and recovery observable without paying twice for
the same semantic run.

## What It Proves

- case/run identities can be independently leased and actively pushed to a
  Direct Runner;
- task phases receive distinct arguments and immutable Loom runtime variables;
- a retryable failure produces a retained ZIP and increments only that task's
  `attempt_no`;
- the second attempt materializes the pinned AgentDojo source, runs its public
  benchmark entry point, and declares output artifacts; and
- all eight ZIPs can be downloaded, hash-verified, and summarized without
  retaining raw model output in this repository.

It does not claim benchmark quality, model safety, cost, or throughput. It is a
control-plane release check.

## Remote Run

Use a fresh, already-provisioned remote host. Do not run this fixture on an
operator laptop. The committed fixture uses AgentDojo's standard `LOCAL` model
channel, so the host needs an OpenAI-compatible test endpoint at
`LOCAL_LLM_PORT` plus:

- `LOOM_HUB_TOKEN`; and
- `LOOM_RUNNER_TOKEN`.

An operator may pass `--model` to select another model accepted by the pinned
AgentDojo release, together with its own required environment variables through
`--require-env NAME`. Loom itself does not contain provider, model, or scoring
logic.

Generate the two Loom tokens on the remote host, make them available only to the
temporary process environment, and run:

```bash
python3 tools/loom_agentdojo_remote_smoke.py \
  --repo-root "$PWD" \
  --source-path /srv/cache/agentdojo-v0.1.35 \
  --runtime-dir /tmp/loom-agentdojo-eight-slot \
  --export-dir /tmp/loom-agentdojo-eight-slot/recovered
```

`--source-path` is optional but recommended for the release gate: it must be a
checkout whose `HEAD` equals the fixture's pinned `v0.1.35` ref. The helper
checks that identity before rewriting only its temporary manifest to a local
source. Each attempt still receives a separate copied workspace; the cache
does not weaken attempt isolation or alter the committed Git-source fixture.

The helper runs the remote unit suite, starts a loopback-only authenticated Hub
and Direct Runner, pushes the four identities, downloads all eight result ZIPs,
checks their SHA-256 values, exports a redacted
`recovery-contract.json`, and stops its Hub/Runner processes even after a
failure. The caller must then copy the redacted example evidence if desired and
explicitly stop/delete the temporary host.

Set `LOOM_AGENTDOJO_MODEL` or pass `--model` to use another compatible upstream
model without editing the committed fixture.

## Evidence Shape

The raw remote result packages are intentionally transient. The repository may
contain only the redacted export at
[`examples/agentdojo/recovered/`](../examples/agentdojo/recovered/), which
contains phase exit codes and artifact metadata but omits logs, hostnames,
worker IDs, timestamps, result IDs/URLs, raw model output, and credentials.

See [Release Contract](RELEASE_CONTRACT.md) for the required gate and
[Remote Validation](REMOTE_VALIDATION.md) for the cloud lifecycle boundary.

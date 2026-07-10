# AgentDojo Example

This is a documentation-only example of describing a public AgentDojo checkout
as an AgentBenchmark Control Worker repository task. It is designed to make the
task shape concrete for readers, not to validate AgentDojo end to end.

The example intentionally does not install AgentDojo, invoke its benchmark
script, call a model provider, or make a benchmark claim. It only models a
lightweight source-layout check and an explicit result artifact.

## What The Example Shows

- a public Git source and immutable task identity;
- an explicit worker capability for access to the source domain;
- a small ordered command list that verifies the checkout layout;
- an artifact allowlist so the result package contains evidence rather than a
  full source checkout; and
- a single case/run row that can be tracked independently by the controller.

## Illustrative Manifest

```json
{
  "campaign_id": "agentdojo-documentation",
  "source": {
    "type": "git",
    "url": "https://github.com/ethz-spylab/agentdojo.git",
    "ref": "main",
    "depth": 1
  },
  "defaults": {
    "required_capability": "source-github-com",
    "timeout_seconds": 120,
    "expected": {
      "state": "clean"
    },
    "artifact_paths": [
      "agentdojo-source-check.json"
    ],
    "commands": [
      "test -f README.md",
      "test -f pyproject.toml",
      "test -f src/agentdojo/scripts/benchmark.py",
      "printf '{\"ok\": true}\\n' > agentdojo-source-check.json"
    ]
  },
  "cases": [
    {
      "case_id": "agentdojo-source-layout",
      "setting_id": "documentation",
      "run_id": "001"
    }
  ]
}
```

The controller normalizes the case into the stable task ID:

```text
agentdojo-documentation__agentdojo-source-layout__documentation__run-001
```

## Reading The Example

`source` tells a controlled worker which public repository and ref to
materialize. The `source-github-com` capability lets the scheduler avoid
dispatching the task to a worker that cannot reach GitHub.

The commands are intentionally shallow. They establish that the expected
upstream files were materialized and write a single JSON artifact. They do not
exercise AgentDojo itself. If a real evaluation is needed later, replace those
commands with an owned, explicit run plan and declare the resulting artifacts,
timeouts, retry policy, and expected outcome.

The `artifact_paths` allowlist causes only `agentdojo-source-check.json` and
worker/controller metadata to be retained in the result ZIP. The checkout is
not uploaded.

## From Documentation To A Real Campaign

Use the [Task Input Manual](TASK_INPUT_MANUAL.md) to normalize a real manifest
and the [Architecture guide](ARCHITECTURE.md#repo-task-delivery) to understand
source materialization, retries, and result recovery. Any live work should use
operator-supplied remote hosts as described in the
[Support Scope](SUPPORT_SCOPE.md); this document does not prescribe or trigger
an end-to-end run.

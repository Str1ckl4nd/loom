# Loom Manifest

This control plane does not infer benchmark cases from a target repository.
Handoff owners or agents must normalize work into explicit campaign, case, and
run records before dispatch.

## Required Granularity

Every runnable unit must have:

- `campaign_id`: one evaluation campaign or experiment batch.
- `case_id`: the benchmark case being evaluated.
- `setting_id`: model, defense, prompt, environment, or other setting slice.
- `run_id`: one concrete run of that case under that setting.

All four fields are mandatory. The normalizer rejects generated/missing IDs and
rejects duplicate task IDs after normalization.

The normalizer builds task IDs as:

```text
{campaign_id}__{case_id}__{setting_id}__run-{run_id}
```

That ID is the recovery boundary. Operators can query, retry, cancel, or inspect
one `case_id` / `run_id` without touching other runs.

## Manifest Shape

Use JSON for campaign manifests:

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
    "retry_policy": {
      "max_attempts": 3,
      "retry_categories": ["network_unavailable"],
      "different_worker": true
    },
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

This is a documentation-only source-delivery example. It checks the public
AgentDojo checkout layout and writes one explicit artifact; it does not install
or invoke AgentDojo, call a model, or represent an end-to-end benchmark result.
See [AgentDojo Example](AGENTDOJO_EXAMPLE.md) for the walkthrough.

For large batches, JSONL is also allowed. Each line is one run record and must
repeat the same explicit `campaign_id`; top-level defaults are not available.

`runner: "repo"` requires `source` and may contain ordered `commands`.
`runner: "shell"` requires one `command` and is useful for infrastructure or
resource probes that do not need a checkout. Handoff manifests must choose the
runner explicitly when they are not repository runs.

## Retry And Validation Contract

`retry_policy` is optional and belongs to task execution, not to the cloud
runner. Its fields are:

- `max_attempts`: total task attempts, including the first one.
- `retry_categories`: controller issue categories that permit automatic retry.
- `different_worker`: exclude the failed worker from the next claim. If no
  other active capable worker exists, the task remains failed instead of
  waiting forever.

Repo tasks should normally retry only transient `network_unavailable` failures.
Do not automatically retry authentication, token balance, or resource failures
unless the handoff explicitly defines why another attempt can recover.

`expected` is an optional validation contract consumed by the matrix runner:

- `state`: required final task state.
- `attempt_no`: exact final attempt number.
- `min_result_count`: minimum independently recoverable result packages.
- `min_distinct_workers`: minimum workers represented by those results.

Loom Runner exposes the immutable runtime values `LOOM_TASK_ID`,
`LOOM_ATTEMPT_NO`, and `LOOM_WORKER_ID` to shell and repo
commands. This allows deterministic retry-aware jobs without shared local
marker files.

## Normalize And Dispatch

Normalize:

```bash
python3 tools/loom_manifest.py agentdojo-example.json \
  --operator documentation \
  --output agentdojo-example.dispatch.json
```

Dispatch:

```bash
python3 tools/loom_hub.py dispatch-spec \
  --controller http://CONTROL_HOST:8765 \
  agentdojo-example.dispatch.json
```

Query one run directly by its normalized fields:

```bash
curl -sS 'http://CONTROL_HOST:8765/api/tasks?case_id=agentdojo-source-layout&setting_id=documentation&run_id=001'
```

Retry one run:

```bash
curl -sS -X POST http://CONTROL_HOST:8765/api/admin/retry-task \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"agentdojo-documentation__agentdojo-source-layout__documentation__run-001","operator":"operator"}'
```

Download the result identified by the task row's `result_id`:

```bash
curl -fSLo result.zip \
  http://CONTROL_HOST:8765/api/results/result-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

List every retained attempt result for one run, or one exact attempt:

```bash
curl -sS 'http://CONTROL_HOST:8765/api/data/new-results?cursor=0&task_id=agentdojo-documentation__agentdojo-source-layout__documentation__run-001'
curl -sS 'http://CONTROL_HOST:8765/api/data/new-results?cursor=0&task_id=agentdojo-documentation__agentdojo-source-layout__documentation__run-001&attempt_no=2'
```

Retry keeps the same task/recovery identity and increments `attempt_no`. Every
uploaded result row also records its own `attempt_no`, so earlier failure
packages remain independently queryable and downloadable after a later attempt
succeeds. Manual retry clears automatic worker exclusions unless
`preserve_excluded_workers` is explicitly requested.

## Handoff Rules

- Never use a vague task like "run the benchmark." Expand it into case/run rows.
- Put all source materialization details in `source`; do not put tokens in URLs.
- Put credentials only in worker environment variables referenced by `token_env`
  or command-specific environment names.
- Keep artifacts explicit. Result ZIPs intentionally exclude full repo checkouts.
- If a run cannot be represented at case/run granularity, split the source
  benchmark first, then normalize.

# Oracle, Trajectory, And Reward Contracts

Loom separates process execution from semantic evaluation. An execution attempt
first produces an immutable result ZIP. A separately schedulable Oracle may then
consume that exact ZIP, publish a semantic decision, and retry without rerunning
the expensive execution attempt.

```text
execution task / attempt -> retained execution ZIP -> Oracle task / attempt -> semantic outcome
```

The execution task and Oracle task have separate leases, capabilities, resource
profiles, timeouts, retries, workers, logs, and result ZIPs. Hub owns both task
state machines. An Oracle result never rewrites the execution task state.

## Oracle Contract

Add one optional `oracle` object at campaign, `defaults`, case/run,
`defaults.payload`, or `case.payload` level. The last declared layer wins as one
atomic object; Loom does not merge nested Oracle fields across layers.

```json
{
  "oracle": {
    "schema_version": 1,
    "name": "completion-check",
    "when": "execution_clean",
    "oracle_version": "completion-check-v1",
    "result_path": "oracle-result.json",
    "required_capability": "oracle",
    "priority": 10,
    "execution_profile": {
      "placement": "shared",
      "resources": {"cpu_millis": 250, "memory_mb": 256}
    },
    "retry_policy": {
      "max_attempts": 2,
      "retry_categories": ["oracle_error"]
    },
    "payload": {
      "runner": "shell",
      "command": ["python3", "judge.py"],
      "timeout_seconds": 120
    }
  }
}
```

`when` is one of:

- `execution_clean`: queue the child only after the execution ZIP has a clean
  process verdict. This is the default.
- `execution_result`: queue the child after any uploaded execution ZIP, including
  a failed process attempt. It is useful for diagnostics or an Oracle that can
  classify partial evidence.

For an Oracle task, Runner downloads the parent ZIP directly from Hub and
SHA-256 verifies it before the Oracle process begins. It injects these immutable
environment variables:

```text
LOOM_TASK_KIND=oracle
LOOM_EXECUTION_RESULT_ID
LOOM_EXECUTION_TASK_ID
LOOM_EXECUTION_ATTEMPT_NO
LOOM_EXECUTION_RESULT_ZIP
LOOM_ORACLE_RESULT_PATH
LOOM_ORACLE_VERSION
LOOM_ORACLE_NAME
```

The child task ID is deterministic for one parent execution attempt and Oracle
name. A retry changes only the Oracle task's `attempt_no`; it never reruns the
parent execution ZIP.

An operator can also attach an Oracle after the execution has finished:

```bash
python3 tools/loom_hub.py dispatch-oracle \
  --controller http://CONTROL_HOST:8765 \
  --execution-result-id result-EXISTING_RESULT_ID \
  oracle.json
```

`oracle.json` is the same versioned object shown above. Hub records command-free
Oracle metadata in its control log, not the Oracle command or payload.

## Semantic Outcome And Reward

The Oracle writes `oracle-result.json` by default. It is versioned independently
from the execution process result:

```json
{
  "schema_version": 1,
  "outcome": "pass",
  "oracle_version": "completion-check-v1",
  "reward": {
    "value": 1.0,
    "components": {"completed": 1.0, "policy": 0.0},
    "metadata": {"scale": "unit"}
  },
  "score_metadata": {"dataset_revision": "2026-07"},
  "evidence": [{"path": "artifacts/judge-report.json", "sha256": "..."}],
  "summary": {"reason": "all required signals present"},
  "extensions": {"org.example.oracle": {"reviewer": "v1"}}
}
```

`outcome` is exactly `pass`, `fail`, `error`, or `inconclusive`. It is not a Hub
task state:

| Data | Meaning |
| --- | --- |
| Execution or Oracle task `state` | Process and lease lifecycle, such as `clean` or `run_error`. |
| Oracle `outcome` | Semantic judgement over an execution ZIP. |

If an Oracle process exits cleanly but writes `outcome: "error"`, Hub preserves
that semantic result and adds the `oracle_error` retry category. A child retry is
queued only when its own `retry_policy` explicitly permits it. A malformed or
missing Oracle output is recorded as semantic `error` as well, while the
process-level result remains separately inspectable.

`reward.value` and every named component must be finite numbers. `metadata`,
`score_metadata`, `evidence`, `summary`, and `extensions` are structured JSON
owned by the Oracle integration. Loom stores and exports them; it is not an RL
trainer and does not impose a reward scale or aggregation policy.

## Trajectory Export

Trajectory collection is opt-in. Without `trajectory_export`, Runner does not
set a trajectory path, does not collect raw traces, and does not add a trajectory
to the result ZIP.

```json
{
  "trajectory_export": {
    "schema_version": 1,
    "source_path": ".loom-trajectory.raw.json",
    "export_path": "trajectory.json",
    "max_bytes": 1048576,
    "required": true,
    "redaction": {
      "patterns": ["customer-[0-9]+"],
      "replacement": "[REDACTED]"
    }
  }
}
```

The task process receives `LOOM_TRAJECTORY_PATH` and writes a JSON document:

```json
{
  "schema_version": 1,
  "events": [
    {"kind": "message", "content": "..."},
    {"kind": "tool_call", "name": "lookup", "arguments": {"q": "..."}},
    {"kind": "tool_result", "content": "..."}
  ]
}
```

Supported event kinds are `message`, `tool_call`, `tool_result`, `observation`,
`timing`, and `artifact_ref`. Loom applies built-in secret patterns and the
declared custom patterns recursively, enforces the byte limit before and after
redaction, writes the sanitized `export_path`, and records a SHA-256 receipt in
`trajectory-summary.json` and `worker-result.json`.

The source and exported paths must be different relative paths inside the
attempt. Runner removes the raw source before artifact collection and excludes it
again when building the ZIP. A task-created symlink escaping the attempt
directory is rejected. Redaction is a guardrail, not a reason to put credentials
or sensitive user data into trajectory, evidence, reward metadata, or
extensions.

## Query And Recovery

Use the versioned HTTP and CLI surfaces rather than importing Hub internals:

```bash
curl -sS -H "Authorization: Bearer $LOOM_HUB_TOKEN" \
  'http://CONTROL_HOST:8765/api/data/oracle-outcomes?outcome=pass'

python3 tools/loom_export.py \
  --controller http://CONTROL_HOST:8765 \
  --selector oracle_pass \
  --output recovered-oracle-pass
```

`GET /api/data/oracle-outcomes` exposes semantic rows. `GET /api/data/export`
and `loom_export.py` offer these retention-safe selectors:

| Selector | Selected ZIPs |
| --- | --- |
| `all_attempts` | Every retained execution and Oracle attempt package. |
| `execution_clean` | Clean execution attempt packages only. |
| `oracle_decided` | The parent execution ZIP and Oracle ZIP for each `pass` or `fail` decision. |
| `oracle_pass` | The parent execution ZIP and Oracle ZIP for each `pass` decision. |

Selectors never delete evidence. The export client downloads each selected ZIP
and validates the Hub-recorded byte length and SHA-256 value before writing its
own `export-manifest.json`.

## Discovery And Boundary

Hub advertises `oracle-v1`, `trajectory-export-v1`, `reward-contract-v1`, and
`result-export-selectors-v1`. Runner advertises `oracle-input-result-v1` and
`trajectory-export-v1`. Check authenticated `/api/meta` or the documented
`capabilities` commands before using an optional feature against an older
deployment.

Oracle commands are operator-supplied task code. Loom provides no sandbox,
model-specific judge, benchmark-specific scoring, or provider credentials. Keep
the Hub and Runner control plane private and use an operator-managed boundary for
any untrusted Oracle implementation.

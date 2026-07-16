# Oracle, Trajectory, And Reward Example

`oracle-trajectory-reward.manifest.json` is a deterministic one-case example of
the public Oracle contract. Its execution task writes an opt-in trajectory, then
the child Oracle downloads and verifies the exact execution ZIP before writing a
`pass` decision with a scalar reward and named component.

Normalize and run it only against operator-owned remote infrastructure:

```bash
python3 tools/loom_manifest.py \
  examples/oracle/oracle-trajectory-reward.manifest.json \
  --operator example \
  --output /remote/operator/example.dispatch.json

python3 tools/loom_matrix.py \
  --inventory /remote/operator/inventory.json \
  --dispatch-spec /remote/operator/example.dispatch.json \
  --output /remote/operator/example-summary.json
```

The Oracle needs a worker with both `linux` and `oracle` capabilities. Matrix
waits for the execution child and its queued Oracle task because Hub task counts
include both kinds. Recover only successful semantic decisions with:

```bash
python3 tools/loom_export.py \
  --controller http://CONTROL_HOST:8765 \
  --selector oracle_pass \
  --output /remote/operator/oracle-pass
```

The recovered result set contains the execution ZIP and its matching Oracle ZIP.
The execution ZIP contains `trajectory.json` and its SHA-256 receipt, never the
raw `.loom-trajectory.raw.json` input.

See [Oracle, Trajectory, And Reward Contracts](../../docs/ORACLE_TRAJECTORY_REWARD.md)
for the complete schema, output variables, semantic-state separation, and data
handling boundary.

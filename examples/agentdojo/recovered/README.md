# Recovered AgentDojo Evidence

`recovery-contract.json` is produced only after the fixed release regression
succeeds on a remote host. It is intentionally a small, redacted view of the
eight downloaded result packages:

- two public AgentDojo cases;
- two independent `run_id` values per case;
- attempt 1 retained as an intentional retryable preflight failure;
- attempt 2 retained as the real AgentDojo invocation; and
- phase status plus artifact paths, sizes, and SHA-256 values for every package.

It deliberately omits hosts, worker IDs, timestamps, logs, raw model output,
result IDs/URLs, and all credentials. The raw ZIPs remain in the temporary
remote validation directory and must not be committed.

The checked-in acceptance export was produced with AgentDojo `v0.1.35` through
its `LOCAL` CLI channel and a temporary OpenAI-compatible transport stub. It
proves the real AgentDojo command, Loom phase/retry scheduling, artifact
declaration, ZIP recovery, and redaction path. It does not make a claim about
model quality, benchmark score, safety, latency, or provider cost.

Generate this file from a completed remote regression with:

```bash
python3 tools/loom_agentdojo_export_example.py \
  --input /remote/run/regression \
  --output examples/agentdojo/recovered
```

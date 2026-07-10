# AgentDojo Fixture

`agentdojo-eight-slot.manifest.json` is the fixed Loom release fixture:

```text
2 cases x 2 runs x 2 attempts = 8 retained result packages
```

The four logical tasks use the public AgentDojo `workspace` suite. Attempt 1 is
an intentional retryable network preflight failure. Attempt 2 installs the
pinned source, invokes AgentDojo, and writes an artifact index. This preserves
the full `case_id` / `run_id` / `attempt_no` recovery path without running eight
paid model calls.

Run it only through `tools/loom_agentdojo_remote_smoke.py` on a temporary remote
host. The resulting redacted recovery evidence belongs in `recovered/`; raw ZIP
packages and provider output do not belong in Git.

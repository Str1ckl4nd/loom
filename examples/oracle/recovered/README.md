# Recovered Oracle Contract Evidence

This directory receives only the small redacted acceptance export from a
successful remote Oracle release gate. The raw execution and Oracle ZIPs, runtime
paths, worker identity, commands, logs, and any task data remain outside Git.

Regenerate the acceptance export only on a fresh remote host:

```bash
python3 tools/loom_oracle_remote_smoke.py \
  --repo-root "$PWD" \
  --runtime-dir /remote/empty-oracle-runtime \
  --export-dir /remote/oracle-recovered
```

After the gate succeeds, copy only `oracle-release-acceptance.json` into this
directory, remove the remote runtime, and explicitly delete the temporary cloud
resources.

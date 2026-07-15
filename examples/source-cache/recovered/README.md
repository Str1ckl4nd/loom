# Recovered Source-Cache Evidence

`cache-release-acceptance.json` is produced only after the immutable-source
cache release gate succeeds on a fresh remote host. It is a small redacted view
of the four downloaded result ZIPs and records that the remote unit suite
passed.

The fixture proves these control-plane behaviors:

- an initial immutable source cache fill produces a clean result package;
- an automatic Direct Push selects the warm Runner for an identical digest;
- a changed digest receives a distinct cache entry; and
- a deliberately damaged cached mirror is repaired without reusing an attempt
  workspace.

The export omits hostnames, worker identifiers, result IDs and URLs, runtime
paths, raw command output, credentials, and raw ZIPs. Its transfer figures are
local-fixture cache-fill bytes, not a public-network benchmark.

Regenerate it only on a temporary remote host:

```bash
python3 tools/loom_source_cache_remote_smoke.py \
  --repo-root "$PWD" \
  --runtime-dir /remote/empty-runtime \
  --export-dir /remote/recovered
```

Copy only the redacted acceptance export into this directory after the remote
run, then remove the runtime and explicitly delete the temporary cloud
resources.

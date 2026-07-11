# Source Cache And Cache Affinity

Loom v0.3 can reuse an immutable Git source on the same Runner without making
Hub a source-artifact store. The cache is local to a Runner; Hub sees only a
credential-free cache key in the Runner health record.

## Immutable Source Descriptor

For `runner: "repo"`, Loom Manifest emits `payload.source_descriptor` when a
Git source supplies one complete immutable commit through `resolved_commit`,
`commit`, or a full object ID in `ref`. The descriptor contains:

- a canonical Git URL with URL credentials, query data, and fragments removed;
- the full resolved commit; and
- a `git-sha256:` cache key derived from those fields.

Mutable branches and tags do not get a descriptor and always retain the normal
fresh-clone behavior. A raw dispatch may include a descriptor only when it
matches its source object; Hub recomputes it before persistence.

```json
{
  "source": {
    "type": "git",
    "url": "https://github.com/acme/eval.git",
    "resolved_commit": "0123456789abcdef0123456789abcdef01234567"
  },
  "source_descriptor": {
    "schema_version": 1,
    "type": "git",
    "canonical_url": "https://github.com/acme/eval.git",
    "commit": "0123456789abcdef0123456789abcdef01234567",
    "cache_key": "git-sha256:..."
  }
}
```

Keep clone credentials in the worker environment and `token_env`; never embed
them in the Git URL or descriptor.

## Runner Cache

Each Runner defaults to a `source-cache` directory under its `--work-dir` and a
4 GiB budget. Operators may set the location and budget directly:

```bash
python3 tools/loom_runner.py \
  --controller http://CONTROL_HOST:8765 \
  --work-dir /srv/loom/runner-a \
  --source-cache-dir /srv/loom/source-cache \
  --source-cache-max-mb 8192
```

The same fields are optional per worker in a Matrix inventory:

```json
{
  "worker_id": "runner-a",
  "source_cache_dir": "/srv/loom/source-cache",
  "source_cache_max_mb": 8192
}
```

For a pinned Git source, the Runner locks the cache key, verifies the commit in
its bare mirror, and creates a new detached writable worktree for the attempt.
It removes that worktree after artifact collection. A cache hit never shares a
workspace, logs, or declared artifacts between attempts. Invalid mirrors are
refilled when no active worktree depends on them; a cache-only failure falls
back to a normal fresh clone when possible.

The budget is enforced by evicting inactive least-recently-used entries. A
currently running worktree and a lock-protected fill are never evicted. One
source larger than the budget may remain until it is no longer the protected
current entry; `over_limit` makes that fact visible.

## Scheduling

Cache locality is a soft preference only. For a Pull claim, Hub orders a
bounded queue window by:

```text
priority -> cache hit on this Runner -> queue creation order -> task ID
```

Capability, retry exclusions, resource reservations, `shared`/`exclusive`
placement, hard `max_concurrency`, and controller desired concurrency are
checked before the preference can lease a task. A cache miss never makes a task
ineligible.

Direct Push accepts either an explicit Runner or automatic cache-aware choice:

```bash
# Exact delivery remains available.
python3 tools/loom_hub.py push-task --task-id TASK_ID --worker-id runner-a

# Omit worker-id to choose an eligible Direct Runner, preferring a cache hit.
python3 tools/loom_hub.py push-task --task-id TASK_ID
```

An explicit `--worker-id` is never silently replaced. Automatic Direct Push
uses cache hit, then lower active work, then worker ID as its deterministic
tie-breaker.

## Inspection And Results

Runner heartbeats advertise cache keys, entry count, byte count, and configured
budget. Use the authenticated Hub APIs or matching CLI commands to inspect
them:

```bash
python3 tools/loom_hub.py worker-cache --controller http://CONTROL_HOST:8765
python3 tools/loom_hub.py task-admission \
  --controller http://CONTROL_HOST:8765 --task-id TASK_ID
```

`worker-result.json` and `source-summary.json` report `source_cache` facts for
repository attempts: key, hit/miss or repair state, approximate cached bytes
added on a miss, materialization time, fallback, and eviction facts. Hub task
admission, claim events, and Direct Push responses report the selected cache
affinity. These records contain keys and canonical source identity, not cache
contents or source credentials.

## Boundaries

This release caches immutable Git sources only. Generic input bundles with a
URI and SHA-256, Hub-hosted blob transfer, cross-Runner cache copying, and cloud
resource lifecycle are not part of this feature.

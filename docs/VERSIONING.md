# Versioning

Loom separates its product release from the protocols consumed by automation.

## Current Release

The current public release is **Loom v0.4.0 Core Preview**. The `--version`
output and authenticated `/api/meta` documents are the canonical discovery
surface for a deployed process.

## Product Releases

While Loom is pre-1.0, product versions use `0.MINOR.PATCH`:

- `0.1.x`: the original Core Preview baseline and compatible fixes;
- `0.2.0`, `0.3.0`, `0.4.0`, and later: additive user-facing capabilities; and
- `1.0.0`: reserved for a later stability milestone, not merely a feature count.

## Protocol Versions

The product release does not change a protocol by itself. The current
inventory, manifest, normalized dispatch, Hub API, and Runner API contracts are
all version `1`.

The Oracle, trajectory, reward, and result-export contracts added in `0.4.0` are
additive `v1` capability documents. They do not change the meaning of an
existing v1 manifest or dispatch payload when their optional fields are absent.

An incompatible change must increment the affected protocol version and keep a
migration path or explicit rejection. For example, a future manifest breaking
change requires `schema_version: 2` even if it ships in a `0.x.y` product
release. Additive fields remain optional unless their protocol version changes.

This lets a user upgrade Loom for a bug fix without guessing whether an existing
inventory or campaign manifest has changed meaning.

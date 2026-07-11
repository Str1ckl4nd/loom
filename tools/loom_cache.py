"""Immutable source identity helpers for Runner-local Loom caches.

The Hub only receives a cache key and never receives cached source data.  The
key is derived from a canonical Git location plus an immutable commit, so a
mutable branch or tag cannot accidentally reuse a stale checkout.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


SOURCE_DESCRIPTOR_VERSION = 1
_FULL_GIT_OBJECT = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")
_SCP_GIT_URL = re.compile(r"^(?:[^/@:\\s]+@)?(?P<host>[^/:\\s]+):(?P<path>.+)$")


def _clean_path(value: str) -> str:
    path = value.rstrip("/")
    return path or "/"


def canonical_git_url(value: Any) -> str:
    """Return a stable Git location without URL credentials or a fragment."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("git source requires url")

    # Git's common scp-like spelling has no URL scheme.  The user component is
    # deliberately excluded from the cache identity along with passwords.
    scp = _SCP_GIT_URL.match(raw) if "://" not in raw else None
    if scp and len(scp.group("host")) > 1:
        return f"ssh://{scp.group('host').lower()}/{_clean_path(scp.group('path')).lstrip('/')}"

    parsed = urlsplit(raw)
    if not parsed.scheme:
        if raw.startswith(("/", "./", "../", "~")):
            return Path(os.path.expanduser(raw)).resolve(strict=False).as_uri()
        return _clean_path(raw)

    scheme = parsed.scheme.lower()
    if scheme == "file":
        host = (parsed.hostname or "").lower()
        return urlunsplit(("file", host, _clean_path(parsed.path), "", ""))

    host = (parsed.hostname or "").lower()
    if not host:
        # Preserve uncommon but valid Git transports while still stripping
        # query and fragment data from their cache identity.
        return urlunsplit((scheme, parsed.netloc, _clean_path(parsed.path), "", ""))
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"git source has invalid URL port: {raw}") from exc
    netloc = host if port is None else f"{host}:{port}"
    return urlunsplit((scheme, netloc, _clean_path(parsed.path), "", ""))


def immutable_git_commit(source: Any) -> str | None:
    if not isinstance(source, dict):
        return None
    candidate = source.get("resolved_commit") or source.get("commit") or source.get("ref")
    if not isinstance(candidate, str):
        return None
    value = candidate.strip()
    return value.lower() if _FULL_GIT_OBJECT.fullmatch(value) else None


def git_checkout_ref(source: dict[str, Any]) -> str | None:
    """Choose the concrete checkout ref while favoring an explicit commit."""
    for key in ("resolved_commit", "commit", "ref"):
        value = source.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def git_source_descriptor(source: Any) -> dict[str, Any] | None:
    """Return the cacheable descriptor only when the source is immutable."""
    if not isinstance(source, dict):
        return None
    source_type = str(source.get("type") or ("git" if source.get("url") or source.get("repo_url") else "")).lower()
    if source_type not in {"git", "repo"}:
        return None
    url = source.get("url") or source.get("repo_url")
    commit = immutable_git_commit(source)
    if not url or not commit:
        return None
    identity = {
        "schema_version": SOURCE_DESCRIPTOR_VERSION,
        "type": "git",
        "canonical_url": canonical_git_url(url),
        "commit": commit,
    }
    digest = hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {**identity, "cache_key": f"git-sha256:{digest}"}


def attach_source_descriptor(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Attach a verified immutable source descriptor to a task payload.

    A caller may supply a descriptor for inspection, but it must agree with the
    source object.  This prevents a direct-dispatch client from selecting an
    unrelated cache entry.
    """
    source = payload.get("source")
    descriptor = git_source_descriptor(source)
    supplied = payload.get("source_descriptor")
    if supplied is not None and not isinstance(supplied, dict):
        raise ValueError("payload.source_descriptor must be an object")
    if descriptor is None:
        if supplied is not None:
            raise ValueError("payload.source_descriptor requires an immutable git source")
        return None
    if supplied is not None and any(supplied.get(key) != value for key, value in descriptor.items()):
        raise ValueError("payload.source_descriptor does not match payload.source")
    payload["source_descriptor"] = descriptor
    return descriptor


def source_cache_key(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    descriptor = payload.get("source_descriptor")
    if not isinstance(descriptor, dict) or descriptor.get("type") != "git":
        return None
    key = descriptor.get("cache_key")
    if not isinstance(key, str) or not key.startswith("git-sha256:"):
        return None
    digest = key.split(":", 1)[1]
    return key if re.fullmatch(r"[0-9a-f]{64}", digest) else None


def cache_key_digest(cache_key: str) -> str:
    key = source_cache_key({"source_descriptor": {"type": "git", "cache_key": cache_key}})
    if key is None:
        raise ValueError("unsupported source cache key")
    return key.split(":", 1)[1]

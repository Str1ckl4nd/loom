"""Small shared HTTP and token helpers for Loom control-plane components."""

from __future__ import annotations

import hmac
import ipaddress
import json
import os
from typing import Any
from urllib.request import Request, urlopen


DEFAULT_HUB_TOKEN_ENV = "LOOM_HUB_TOKEN"
DEFAULT_RUNNER_TOKEN_ENV = "LOOM_RUNNER_TOKEN"


def token_from_env(name: str | None, *, required: bool = False) -> str | None:
    env_name = str(name or "").strip()
    if not env_name:
        return None
    token = os.environ.get(env_name)
    if token:
        return token
    if required:
        raise ValueError(f"required token environment variable is not set: {env_name}")
    return None


def is_loopback_host(host: str) -> bool:
    value = str(host or "").strip().strip("[]")
    if value.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def bearer_headers(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


def token_matches(headers: Any, expected: str | None) -> bool:
    if not expected:
        return True
    authorization = str(headers.get("Authorization") or "")
    supplied = authorization[7:].strip() if authorization.lower().startswith("bearer ") else str(headers.get("X-Loom-Token") or "")
    return bool(supplied) and hmac.compare_digest(supplied, expected)


def request_json(
    url: str,
    payload: dict[str, Any] | None = None,
    *,
    token: str | None = None,
    timeout: int = 20,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    headers.update(bearer_headers(token))
    req = Request(url, data=data, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8-sig"))

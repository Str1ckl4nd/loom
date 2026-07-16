#!/usr/bin/env python3
"""Recover a selector-defined set of retained Loom result ZIPs.

The Hub decides which retained records belong to a selector; this client only
downloads those records, verifies their Hub-provided SHA-256 values, and writes
an export manifest.  It never mutates Hub retention state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from loom_contract import CORE_PREVIEW_VERSION
from loom_http import DEFAULT_HUB_TOKEN_ENV, bearer_headers, request_json, token_from_env


SELECTORS = {"all_attempts", "execution_clean", "oracle_decided", "oracle_pass"}


def safe_name(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-.")
    return text or "unknown"


def fetch_selector_rows(controller: str, selector: str, *, token: str | None) -> dict[str, Any]:
    if selector not in SELECTORS:
        raise ValueError(f"unsupported selector: {selector}")
    offset = 0
    rows: list[dict[str, Any]] = []
    retention = ""
    while True:
        query = urlencode({"selector": selector, "offset": offset, "limit": 500})
        page = request_json(controller.rstrip("/") + "/api/data/export?" + query, token=token)
        retention = str(page.get("retention") or retention)
        page_rows = page.get("results") or []
        if not isinstance(page_rows, list):
            raise ValueError("Hub export response has a non-list results field")
        rows.extend(row for row in page_rows if isinstance(row, dict))
        next_offset = page.get("next_offset")
        if next_offset is None:
            break
        next_value = int(next_offset)
        if next_value <= offset:
            raise ValueError("Hub export response did not advance its offset")
        offset = next_value
    return {"selector": selector, "retention": retention, "results": rows}


def download_result(controller: str, row: dict[str, Any], destination: Path, *, token: str | None) -> dict[str, Any]:
    result_id = str(row.get("result_id") or "")
    expected_bytes = int(row.get("bytes") or 0)
    expected_sha256 = str(row.get("sha256") or "").lower()
    if not result_id or expected_bytes <= 0 or not expected_sha256:
        raise ValueError("export row must contain result_id, positive bytes, and sha256")
    request = Request(
        controller.rstrip("/") + "/api/results/" + quote(result_id, safe=""),
        headers={"Accept": "application/zip", **bearer_headers(token)},
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    downloaded = 0
    try:
        with urlopen(request, timeout=120) as response, destination.open("wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
                digest.update(chunk)
                downloaded += len(chunk)
        actual_sha256 = digest.hexdigest()
        if downloaded != expected_bytes or actual_sha256 != expected_sha256:
            raise ValueError(
                f"integrity mismatch: bytes={downloaded}/{expected_bytes} sha256={actual_sha256}/{expected_sha256}"
            )
        return {"path": str(destination), "bytes": downloaded, "sha256": actual_sha256}
    except Exception:
        destination.unlink(missing_ok=True)
        raise


def export_results(controller: str, selector: str, output: Path, *, token: str | None) -> dict[str, Any]:
    source = fetch_selector_rows(controller, selector, token=token)
    output.mkdir(parents=True, exist_ok=True)
    downloaded: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for row in source["results"]:
        result_id = str(row.get("result_id") or "")
        destination = output / "result-packages" / safe_name(row.get("task_id")) / f"{safe_name(result_id)}.zip"
        try:
            recovered = download_result(controller, row, destination, token=token)
            downloaded.append({**row, "recovered": recovered})
        except Exception as exc:
            errors.append({"result_id": result_id, "task_id": row.get("task_id"), "error": f"{type(exc).__name__}: {exc}"})
    manifest = {
        "schema_version": 1,
        "selector": selector,
        "retention": source["retention"],
        "selected_count": len(source["results"]),
        "downloaded_count": len(downloaded),
        "errors": errors,
        "results": downloaded,
    }
    (output / "export-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"ok": not errors, **manifest, "manifest_path": str(output / "export-manifest.json")}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover a selector-defined Loom result export with SHA-256 verification.")
    parser.add_argument("--version", action="version", version=f"Loom Export v{CORE_PREVIEW_VERSION} Core Preview (export selector v1)")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--selector", choices=sorted(SELECTORS), default="all_attempts")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--controller-token-env", default=DEFAULT_HUB_TOKEN_ENV)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        result = export_results(
            args.controller,
            args.selector,
            args.output,
            token=token_from_env(args.controller_token_env),
        )
    except Exception as exc:
        result = {"ok": False, "error": {"type": type(exc).__name__, "detail": str(exc)}}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

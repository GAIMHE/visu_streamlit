from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json_report(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_json_report(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_derived_manifest(payload: dict[str, Any], path: Path) -> None:
    write_json_report(payload, path)


def load_derived_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Derived manifest not found: {path}")
    payload = load_json_report(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Derived manifest payload must be an object: {path}")
    required_keys = {"manifest_version", "generated_at_utc", "schema_version", "build_context", "tables"}
    missing = required_keys - set(payload.keys())
    if missing:
        raise ValueError(f"Derived manifest missing required keys: {sorted(missing)}")
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("Derived manifest key 'tables' must be an object")
    return payload

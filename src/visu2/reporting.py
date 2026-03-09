"""
reporting.py

Write consistency reports and derived manifest metadata to JSON.

Dependencies
------------
- json
- pathlib
- typing

Classes
-------
- None.

Functions
---------
- write_json_report: Write json report.
- load_json_report: Load json report.
- write_derived_manifest: Write derived manifest.
- load_derived_manifest: Load derived manifest.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json_report(payload: dict[str, Any], path: Path) -> None:
    """Write json report.

Parameters
----------
payload : dict[str, Any]
        Input parameter used by this routine.
path : Path
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.

Notes
-----
    Behavior is intentionally documented for maintainability and traceability.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_json_report(path: Path) -> dict[str, Any]:
    """Load json report.

Parameters
----------
path : Path
        Input parameter used by this routine.

Returns
-------
dict[str, Any]
        Result produced by this routine.

Notes
-----
    Behavior is intentionally documented for maintainability and traceability.
"""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_derived_manifest(payload: dict[str, Any], path: Path) -> None:
    """Write derived manifest.

Parameters
----------
payload : dict[str, Any]
        Input parameter used by this routine.
path : Path
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.

Notes
-----
    Behavior is intentionally documented for maintainability and traceability.
"""
    write_json_report(payload, path)


def load_derived_manifest(path: Path) -> dict[str, Any]:
    """Load derived manifest.

Parameters
----------
path : Path
        Input parameter used by this routine.

Returns
-------
dict[str, Any]
        Result produced by this routine.

Notes
-----
    Behavior is intentionally documented for maintainability and traceability.
"""
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

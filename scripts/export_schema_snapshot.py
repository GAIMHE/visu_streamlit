"""Export a schema snapshot for runtime source, artifact, and report files.

This CLI generates a deterministic JSON report that documents the current
runtime dataset shapes. It is intended to support long-term database
documentation maintenance and drift detection.

Examples
--------
Generate the default snapshot under ``artifacts/reports``:

>>> uv run python scripts/export_schema_snapshot.py

Generate a snapshot without null-count scans:

>>> uv run python scripts/export_schema_snapshot.py --skip-nulls
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2 import contracts
from visu2.hf_sync import DEFAULT_RUNTIME_RELATIVE_PATHS


@dataclass(frozen=True)
class SnapshotConfig:
    """Runtime configuration used to build a schema snapshot.

    Parameters
    ----------
    root : Path
        Repository root directory.
    output : Path
        Target JSON path for the snapshot file.
    include_null_counts : bool
        Whether to compute per-column null counts for parquet datasets.
    strict : bool
        Whether to fail when one or more expected runtime files are missing.
    """

    root: Path
    output: Path
    include_null_counts: bool
    strict: bool


def _repo_root() -> Path:
    """Return the repository root resolved from the script location.

    Returns
    -------
    Path
        Absolute repository root path.
    """

    return Path(__file__).resolve().parents[1]


def _runtime_relative_paths() -> list[str]:
    """Build the list of runtime files to include in the snapshot.

    Returns
    -------
    list[str]
        Relative file paths that define the runtime data surface.
    """

    paths = {"data/adaptiv_math_history.parquet", *DEFAULT_RUNTIME_RELATIVE_PATHS}
    return sorted(paths)


def _snapshot_parquet(path: Path, include_null_counts: bool) -> dict[str, Any]:
    """Create a schema snapshot entry for a parquet file.

    Parameters
    ----------
    path : Path
        Absolute path to the parquet file.
    include_null_counts : bool
        Whether to include null counts in the output.

    Returns
    -------
    dict[str, Any]
        Snapshot metadata for the parquet dataset.
    """

    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    column_names = schema.names()
    dtypes = {name: str(dtype) for name, dtype in schema.items()}

    row_count = int(scan.select(pl.len().alias("rows")).collect()["rows"][0])
    null_counts: dict[str, int] = {}
    null_ratios: dict[str, float] = {}

    if include_null_counts and column_names:
        null_frame = scan.select([pl.col(name).null_count().alias(name) for name in column_names]).collect()
        for name in column_names:
            count = int(null_frame[name][0])
            null_counts[name] = count
            null_ratios[name] = (count / row_count) if row_count else 0.0

    return {
        "kind": "parquet",
        "file_size_bytes": path.stat().st_size,
        "rows": row_count,
        "columns": column_names,
        "dtypes": dtypes,
        "null_counts": null_counts if include_null_counts else None,
        "null_ratios": null_ratios if include_null_counts else None,
    }


def _snapshot_json(path: Path) -> dict[str, Any]:
    """Create a schema snapshot entry for a JSON file.

    Parameters
    ----------
    path : Path
        Absolute path to the JSON file.

    Returns
    -------
    dict[str, Any]
        Snapshot metadata for the JSON file.
    """

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    top_level_type = type(payload).__name__
    top_level_keys = sorted(payload.keys()) if isinstance(payload, dict) else None

    return {
        "kind": "json",
        "file_size_bytes": path.stat().st_size,
        "top_level_type": top_level_type,
        "top_level_keys": top_level_keys,
        "list_length": len(payload) if isinstance(payload, list) else None,
    }


def _snapshot_file(path: Path, include_null_counts: bool) -> dict[str, Any]:
    """Create a snapshot entry for a runtime file by extension.

    Parameters
    ----------
    path : Path
        Absolute file path to snapshot.
    include_null_counts : bool
        Whether to compute parquet null counts.

    Returns
    -------
    dict[str, Any]
        Snapshot details for the requested file.
    """

    if path.suffix.lower() == ".parquet":
        return _snapshot_parquet(path, include_null_counts)
    if path.suffix.lower() == ".json":
        return _snapshot_json(path)

    return {
        "kind": "unknown",
        "file_size_bytes": path.stat().st_size,
    }


def build_snapshot(config: SnapshotConfig) -> dict[str, Any]:
    """Build the full schema snapshot payload.

    Parameters
    ----------
    config : SnapshotConfig
        Runtime snapshot configuration.

    Returns
    -------
    dict[str, Any]
        Serializable snapshot payload.

    Raises
    ------
    FileNotFoundError
        If strict mode is enabled and expected runtime files are missing.
    """

    expected = _runtime_relative_paths()
    files: dict[str, Any] = {}
    missing: list[str] = []

    for rel in expected:
        abs_path = config.root / rel
        if not abs_path.exists():
            missing.append(rel)
            continue
        files[rel] = _snapshot_file(abs_path, include_null_counts=config.include_null_counts)

    if config.strict and missing:
        missing_text = ", ".join(missing)
        raise FileNotFoundError(f"Missing expected runtime files: {missing_text}")

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "schema_version": contracts.DERIVED_SCHEMA_VERSION,
        "root_dir": str(config.root),
        "strict": config.strict,
        "include_null_counts": config.include_null_counts,
        "expected_file_count": len(expected),
        "captured_file_count": len(files),
        "missing_files": missing,
        "files": files,
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the snapshot CLI.

    Returns
    -------
    argparse.Namespace
        Parsed argument namespace.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=_repo_root(),
        help="Repository root containing data/ and artifacts/ folders.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/reports/schema_snapshot.json"),
        help="Relative (to --root) or absolute output path.",
    )
    parser.add_argument(
        "--skip-nulls",
        action="store_true",
        help="Skip per-column null count computation for parquet files.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if one or more expected runtime files are missing.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the schema snapshot export command.

    Returns
    -------
    int
        Process exit code (0 on success).
    """

    args = parse_args()
    root = args.root.resolve()
    output = args.output if args.output.is_absolute() else (root / args.output)
    config = SnapshotConfig(
        root=root,
        output=output,
        include_null_counts=not args.skip_nulls,
        strict=args.strict,
    )

    snapshot = build_snapshot(config)
    config.output.parent.mkdir(parents=True, exist_ok=True)
    config.output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Schema snapshot written to: {config.output}")
    print(
        json.dumps(
            {
                "expected_file_count": snapshot["expected_file_count"],
                "captured_file_count": snapshot["captured_file_count"],
                "missing_files": snapshot["missing_files"],
                "schema_version": snapshot["schema_version"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

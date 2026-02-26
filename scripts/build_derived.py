#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.checks import run_all_checks
from visu2.config import ensure_artifact_directories, get_settings
from visu2.contracts import DERIVED_MANIFEST_VERSION, DERIVED_SCHEMA_VERSION
from visu2.derive import write_derived_tables
from visu2.reporting import write_derived_manifest, write_json_report


def _ts() -> str:
    return datetime.now(UTC).isoformat()


def _parquet_table_profile(path: Path) -> dict[str, object]:
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    columns = list(schema.names)
    dtypes = {field.name: str(field.type) for field in schema}
    return {
        "path": str(path),
        "row_count": int(parquet.metadata.num_rows),
        "columns": columns,
        "dtypes": dtypes,
    }


def _build_manifest(
    outputs: dict[str, Path],
    sample_rows: int | None,
    strict_checks: bool,
    checks_status: str,
) -> dict[str, object]:
    required_tables = [
        "fact_attempt_core",
        "agg_activity_daily",
        "agg_objective_daily",
        "agg_student_module_progress",
        "agg_transition_edges",
        "agg_module_usage_daily",
        "agg_playlist_module_usage",
        "agg_module_activity_usage",
        "agg_exercise_daily",
    ]
    tables: dict[str, dict[str, object]] = {}
    for table_name in required_tables:
        path = outputs.get(table_name)
        if path is None:
            raise KeyError(f"Missing derived table output for manifest: {table_name}")
        tables[table_name] = _parquet_table_profile(path)

    return {
        "manifest_version": DERIVED_MANIFEST_VERSION,
        "generated_at_utc": _ts(),
        "schema_version": DERIVED_SCHEMA_VERSION,
        "build_context": {
            "sample_rows": sample_rows,
            "strict_checks": strict_checks,
            "checks_status": checks_status,
        },
        "tables": tables,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build derived datasets for the thin Streamlit slice.")
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="Optional limit for fast local iteration.",
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip consistency checks before building derived datasets.",
    )
    parser.add_argument(
        "--strict-checks",
        action="store_true",
        help="Fail build if checks are not passing.",
    )
    args = parser.parse_args()

    settings = get_settings()
    ensure_artifact_directories(settings)
    checks_status = "skipped"

    if not args.skip_checks:
        report = run_all_checks(settings)
        write_json_report(report, settings.consistency_report_path)
        print(f"Consistency report written to: {settings.consistency_report_path}")
        checks_status = str(report.get("status") or "unknown")
        if args.strict_checks and report["status"] != "pass":
            print("Checks failed and --strict-checks is enabled; aborting derived build.")
            print(json.dumps(report, indent=2))
            return 1

    outputs = write_derived_tables(settings, sample_rows=args.sample_rows)
    manifest = _build_manifest(
        outputs=outputs,
        sample_rows=args.sample_rows,
        strict_checks=args.strict_checks,
        checks_status=checks_status,
    )
    write_derived_manifest(manifest, settings.derived_manifest_path)
    print("Derived outputs:")
    for name, path in outputs.items():
        print(f"- {name}: {path}")
    print(f"Derived manifest: {settings.derived_manifest_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

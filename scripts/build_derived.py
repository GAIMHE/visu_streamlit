#!/usr/bin/env python3
"""CLI entrypoint for building source-local derived artifacts and manifests."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.build_cache import (
    BUILD_CACHE_VERSION,
    build_source_input_snapshot,
    can_reuse_derived_build,
    materialized_input_paths,
)
from visu2.checks import run_all_checks
from visu2.config import ensure_artifact_directories, get_settings
from visu2.contracts import DERIVED_MANIFEST_VERSION, DERIVED_SCHEMA_VERSION
from visu2.derive import write_derived_tables
from visu2.reporting import load_derived_manifest, write_derived_manifest, write_json_report
from visu2.runtime_sources import DEFAULT_SOURCE_ID, get_runtime_source, list_runtime_sources
from visu2.source_builders import materialize_source_runtime_inputs


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
    *,
    source_id: str,
    table_outputs: dict[str, Path],
    sample_rows: int | None,
    strict_checks: bool,
    checks_status: str,
    source_input_snapshot: dict[str, dict[str, int | str]],
) -> dict[str, object]:
    tables = {table_name: _parquet_table_profile(path) for table_name, path in table_outputs.items()}
    return {
        "manifest_version": DERIVED_MANIFEST_VERSION,
        "generated_at_utc": _ts(),
        "schema_version": DERIVED_SCHEMA_VERSION,
        "cache_version": BUILD_CACHE_VERSION,
        "source_id": source_id,
        "source_input_snapshot": source_input_snapshot,
        "build_context": {
            "sample_rows": sample_rows,
            "strict_checks": strict_checks,
            "checks_status": checks_status,
        },
        "tables": tables,
    }


def _materialized_inputs_ready(settings) -> bool:
    return all(path.exists() for path in materialized_input_paths(settings))


def _parse_requested_tables(raw_tables: str | None) -> tuple[str, ...] | None:
    if raw_tables is None:
        return None
    requested = [
        token.strip()
        for chunk in str(raw_tables).split(",")
        for token in chunk.split()
        if token.strip()
    ]
    normalized: list[str] = []
    for table_name in requested:
        if table_name not in normalized:
            normalized.append(table_name)
    return tuple(normalized) or None


def _merge_manifest_table_outputs(
    *,
    settings,
    source,
    updated_outputs: dict[str, Path],
) -> dict[str, Path]:
    merged: dict[str, Path] = {}
    if settings.derived_manifest_path.exists():
        try:
            existing_manifest = load_derived_manifest(settings.derived_manifest_path)
        except (FileNotFoundError, ValueError):
            existing_manifest = {}
        for table_name in source.runtime_derived_tables:
            table_entry = (existing_manifest.get("tables") or {}).get(table_name)
            table_path = settings.artifacts_derived_dir / f"{table_name}.parquet"
            if table_name in updated_outputs:
                merged[table_name] = updated_outputs[table_name]
            elif table_entry and table_path.exists():
                merged[table_name] = table_path
    for table_name, path in updated_outputs.items():
        merged[table_name] = path
    return merged


def _refresh_checks_if_needed(*, settings, source_id: str, strict_checks: bool, skip_checks: bool) -> int:
    """Refresh checks for an unchanged source-local runtime only when needed."""
    if skip_checks:
        print(f"Source '{source_id}' is up to date; skipping checks and rebuild.")
        return 0

    should_run_checks = strict_checks or not settings.consistency_report_path.exists()
    if not should_run_checks:
        print(f"Source '{source_id}' is up to date; skipping rebuild and reusing existing checks.")
        return 0

    report = run_all_checks(settings)
    write_json_report(report, settings.consistency_report_path)
    print(f"Consistency report refreshed at: {settings.consistency_report_path}")
    if strict_checks and report["status"] != "pass":
        print(f"Checks failed for source '{source_id}' and --strict-checks is enabled.")
        print(json.dumps(report, indent=2))
        return 1
    print(f"Source '{source_id}' is up to date; skipped rebuild after refreshing checks.")
    return 0


def _build_one_source(
    source_id: str,
    *,
    sample_rows: int | None,
    skip_checks: bool,
    strict_checks: bool,
    force: bool,
    requested_tables: tuple[str, ...] | None,
) -> int:
    settings = get_settings(source_id)
    ensure_artifact_directories(settings)
    source = get_runtime_source(source_id)
    target_tables = tuple(requested_tables or source.runtime_derived_tables)
    unknown_tables = sorted(set(target_tables) - set(source.runtime_derived_tables))
    if unknown_tables:
        print(
            f"Source '{source_id}' does not expose these runtime tables: {', '.join(unknown_tables)}"
        )
        return 1
    source_input_snapshot = build_source_input_snapshot(settings, source.raw_inputs)
    rebuild_all_tables = bool(force)
    reuse_reason = ""
    partial_build = requested_tables is not None

    if not force:
        can_reuse, reuse_reason = can_reuse_derived_build(
            settings=settings,
            source_id=source_id,
            expected_tables=target_tables,
            sample_rows=sample_rows,
            source_input_snapshot=source_input_snapshot,
        )
        if can_reuse:
            print(f"Source '{source_id}' is already up to date. {reuse_reason}")
            return _refresh_checks_if_needed(
                settings=settings,
                source_id=source_id,
                strict_checks=strict_checks,
                skip_checks=skip_checks,
            )
        print(f"Source '{source_id}' requires rebuild. {reuse_reason}")
        raw_inputs_changed = reuse_reason == "Raw source inputs changed since the last successful build."
        materialized_missing = reuse_reason.startswith("Materialized build input is missing:")
        manifest_schema_changed = (
            reuse_reason.startswith("Derived manifest schema version")
            or reuse_reason.startswith("Derived manifest cache version")
        )
        sample_rows_changed = reuse_reason == "Derived manifest was built with a different sample_rows setting."
        rebuild_all_tables = (
            raw_inputs_changed
            or materialized_missing
            or manifest_schema_changed
            or sample_rows_changed
        )
        if partial_build and (raw_inputs_changed or materialized_missing):
            print(
                "Targeted table builds require unchanged raw inputs and already-materialized local inputs. "
                "Run the full build once, or rerun without `--tables`."
            )
            return 1

    performed_materialization = rebuild_all_tables or not _materialized_inputs_ready(settings)
    if performed_materialization:
        materialization = materialize_source_runtime_inputs(settings)
        print(f"Materialized source '{source_id}' into {settings.runtime_root}")
        for path in materialization.input_paths:
            print(f"- input: {path}")
        for warning in materialization.warnings:
            print(f"- warning: {warning}")
    else:
        print(f"Reusing materialized inputs already present for source '{source_id}'.")

    checks_status = "skipped"
    should_run_checks = not skip_checks and (performed_materialization or not settings.consistency_report_path.exists())
    if should_run_checks:
        report = run_all_checks(settings)
        write_json_report(report, settings.consistency_report_path)
        print(f"Consistency report written to: {settings.consistency_report_path}")
        checks_status = str(report.get("status") or "unknown")
        if strict_checks and report["status"] != "pass":
            print(f"Checks failed for source '{source_id}' and --strict-checks is enabled; aborting derived build.")
            print(json.dumps(report, indent=2))
            return 1
    elif settings.consistency_report_path.exists():
        checks_status = "reused"
        print(f"Reusing existing consistency report at: {settings.consistency_report_path}")

    outputs: dict[str, Path] = {}
    if rebuild_all_tables:
        built_outputs = write_derived_tables(
            settings,
            sample_rows=sample_rows,
            table_names=tuple(source.runtime_derived_tables if not partial_build else target_tables),
        )
        outputs.update(built_outputs)
        print(
            f"Rebuilt derived tables for source '{source_id}': "
            f"{', '.join(source.runtime_derived_tables if not partial_build else target_tables)}"
        )
    else:
        legacy_derived_dir = settings.root_dir / "artifacts" / "derived"
        missing_tables: list[str] = []
        for table_name in target_tables:
            dst_path = settings.artifacts_derived_dir / f"{table_name}.parquet"
            if dst_path.exists():
                outputs[table_name] = dst_path
                continue

            legacy_path = legacy_derived_dir / f"{table_name}.parquet"
            if source.source_id == "main" and sample_rows is None and legacy_path.exists():
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy_path, dst_path)
                outputs[table_name] = dst_path
                continue

            missing_tables.append(table_name)

        if missing_tables:
            built_outputs = write_derived_tables(
                settings,
                sample_rows=sample_rows,
                table_names=tuple(missing_tables),
            )
            outputs.update(built_outputs)
            print(f"Built missing derived tables for source '{source_id}': {', '.join(missing_tables)}")
        else:
            print(f"All requested derived tables for source '{source_id}' were already present; refreshed the manifest only.")

    manifest_outputs = (
        outputs
        if not partial_build
        else _merge_manifest_table_outputs(settings=settings, source=source, updated_outputs=outputs)
    )
    manifest = _build_manifest(
        source_id=source_id,
        table_outputs=manifest_outputs,
        sample_rows=sample_rows,
        strict_checks=strict_checks,
        checks_status=checks_status,
        source_input_snapshot=source_input_snapshot,
    )
    write_derived_manifest(manifest, settings.derived_manifest_path)
    print(f"Derived outputs for source '{source_id}':")
    for name, path in outputs.items():
        print(f"- {name}: {path}")
    print(f"Derived manifest: {settings.derived_manifest_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build source-local derived datasets for the Streamlit app.")
    parser.add_argument("--sample-rows", type=int, default=None, help="Optional limit for fast local iteration.")
    parser.add_argument("--skip-checks", action="store_true", help="Skip consistency checks before building derived datasets.")
    parser.add_argument("--strict-checks", action="store_true", help="Fail build if checks are not passing.")
    parser.add_argument(
        "--source",
        type=str,
        default=DEFAULT_SOURCE_ID,
        help=f"Runtime source id to build. Default: {DEFAULT_SOURCE_ID}",
    )
    parser.add_argument("--all-sources", action="store_true", help="Build every registered runtime source.")
    parser.add_argument("--force", action="store_true", help="Rebuild even when source inputs and outputs appear unchanged.")
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Optional comma-separated runtime derived tables to build for one source only.",
    )
    args = parser.parse_args()

    if args.tables and args.all_sources:
        parser.error("--tables cannot be combined with --all-sources")

    requested_tables = _parse_requested_tables(args.tables)

    source_ids = [spec.source_id for spec in list_runtime_sources()] if args.all_sources else [args.source]
    failures = 0
    for source_id in source_ids:
        failures += _build_one_source(
            source_id,
            sample_rows=args.sample_rows,
            skip_checks=args.skip_checks,
            strict_checks=args.strict_checks,
            force=args.force,
            requested_tables=requested_tables,
        )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

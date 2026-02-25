from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from .config import Settings
from .contracts import EXPECTED_BASELINE, EXPECTED_SPARSE_NULL_COUNTS
from .loaders import (
    catalog_to_summary_frames,
    load_exercises,
    load_learning_catalog,
    load_zpdes_rules,
)


def _ts() -> str:
    return datetime.now(UTC).isoformat()


def _assert_equal(name: str, actual: Any, expected: Any) -> dict[str, Any]:
    return {
        "name": name,
        "expected": expected,
        "actual": actual,
        "pass": actual == expected,
    }


def _sum_null_counts_from_rowgroup_stats(parquet_path: Path, column: str) -> int:
    parquet = pq.ParquetFile(parquet_path)
    schema_names = parquet.schema_arrow.names
    if column not in schema_names:
        raise KeyError(f"Column not found in Parquet schema: {column}")
    col_idx = schema_names.index(column)
    total = 0
    for rg_idx in range(parquet.num_row_groups):
        rg = parquet.metadata.row_group(rg_idx)
        stats = rg.column(col_idx).statistics
        if stats is None or stats.null_count is None:
            raise ValueError(f"Missing null_count statistics for column {column}")
        total += stats.null_count
    return int(total)


def _parquet_profile(settings: Settings) -> dict[str, Any]:
    parquet = pq.ParquetFile(settings.parquet_path)
    schema_names = parquet.schema_arrow.names
    rows = int(parquet.metadata.num_rows)
    cols = len(schema_names)
    row_groups = int(parquet.num_row_groups)

    minmax = (
        pl.scan_parquet(settings.parquet_path)
        .select(
            pl.col("created_at").min().alias("created_at_min"),
            pl.col("created_at").max().alias("created_at_max"),
            pl.col("login_time").min().alias("login_time_min"),
            pl.col("login_time").max().alias("login_time_max"),
        )
        .collect()
        .to_dicts()[0]
    )

    null_counts = {
        column: _sum_null_counts_from_rowgroup_stats(settings.parquet_path, column)
        for column in EXPECTED_SPARSE_NULL_COUNTS
    }

    return {
        "rows": rows,
        "columns": cols,
        "column_names": schema_names,
        "row_groups": row_groups,
        "time_span_utc": {
            key: (value.isoformat() if hasattr(value, "isoformat") and value is not None else None)
            for key, value in minmax.items()
        },
        "null_counts": null_counts,
    }


def _catalog_integrity(catalog_payload: dict[str, Any]) -> dict[str, Any]:
    frames = catalog_to_summary_frames(catalog_payload)
    module_objectives = frames.module_objectives
    objective_activities = frames.objective_activities

    objective_ids = set(frames.objectives["objective_id"].to_list())
    activity_ids = set(frames.activities["activity_id"].to_list())

    missing_module_objective_refs = int(
        module_objectives.filter(~pl.col("objective_id").is_in(list(objective_ids))).height
    )
    missing_objective_activity_refs = int(
        objective_activities.filter(~pl.col("activity_id").is_in(list(activity_ids))).height
    )

    catalog_exercise_ids = set(frames.activity_exercises["exercise_id"].to_list())

    return {
        "modules": int(frames.modules.height),
        "objectives": int(frames.objectives.height),
        "activities": int(frames.activities.height),
        "catalog_exercise_ids_unique": len(catalog_exercise_ids),
        "missing_module_objective_refs": missing_module_objective_refs,
        "missing_objective_activity_refs": missing_objective_activity_refs,
        "catalog_exercise_ids": catalog_exercise_ids,
    }


def _list_len(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    return len(value) if isinstance(value, list) else 0


def _dict_len(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    return len(value) if isinstance(value, dict) else 0


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _pick(payload: dict[str, Any], *keys: str) -> object:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _metadata_health_metrics(
    catalog_payload: dict[str, Any],
    zpdes_rules_payload: dict[str, Any],
    catalog_integrity: dict[str, Any],
    exercise_ids: set[str],
) -> dict[str, int]:
    conflicts = catalog_payload.get("conflicts")
    conflicts = conflicts if isinstance(conflicts, dict) else {}

    coverage = conflicts.get("coverage")
    coverage = coverage if isinstance(coverage, dict) else {}

    missing_references = conflicts.get("missing_references")
    missing_references = missing_references if isinstance(missing_references, dict) else {}

    source_disagreements = conflicts.get("source_disagreements")
    source_disagreements = (
        source_disagreements if isinstance(source_disagreements, dict) else {}
    )

    secondary_mapping_candidates = conflicts.get("secondary_mapping_candidates_for_orphans")
    secondary_mapping_candidates = (
        secondary_mapping_candidates
        if isinstance(secondary_mapping_candidates, dict)
        else {}
    )

    unresolved_links = zpdes_rules_payload.get("unresolved_links")
    unresolved_links = unresolved_links if isinstance(unresolved_links, dict) else {}

    all_catalog_exercises_in_exercises_json = int(
        catalog_integrity["catalog_exercise_ids"].issubset(exercise_ids)
    )

    missing_reference_issue_count = (
        _list_len(missing_references, "missing_activity_ids_in_objectives")
        + _list_len(missing_references, "missing_objective_ids_in_modules")
    )
    source_disagreement_issue_count = (
        _list_len(source_disagreements, "activity_exercise_membership_disagreements")
        + _list_len(source_disagreements, "id_label_disagreements")
    )

    return {
        "coverage_overlapping_activity_count": _to_int(
            coverage.get("overlapping_activity_count")
        ),
        "coverage_primary_only_activity_count": _to_int(
            _pick(coverage, "primary_only_activity_count", "summary_only_activity_count")
        ),
        "coverage_secondary_only_activity_count": _to_int(
            _pick(coverage, "secondary_only_activity_count", "xlsx_only_activity_count")
        ),
        "coverage_membership_disagreement_count": _to_int(
            coverage.get("overlap_membership_disagreement_count")
        ),
        "missing_reference_issue_count": missing_reference_issue_count,
        "source_disagreement_issue_count": source_disagreement_issue_count,
        "orphan_exercise_count": len(catalog_payload.get("orphans") or []),
        "orphan_secondary_mapping_unique_count": _to_int(
            secondary_mapping_candidates.get("unique_count")
        ),
        "orphan_secondary_mapping_ambiguous_count": _to_int(
            secondary_mapping_candidates.get("ambiguous_count")
        ),
        "rule_ids_missing_in_catalog_count": _list_len(
            unresolved_links, "rule_ids_missing_in_catalog"
        )
        or _list_len(unresolved_links, "admath_ids_missing_in_learning_catalog"),
        "catalog_modules_missing_in_rules_count": _list_len(
            unresolved_links, "catalog_module_ids_missing_in_rules"
        )
        or _list_len(unresolved_links, "catalog_module_ids_missing_in_admath"),
        "rule_codes_with_multiple_ids_count": _dict_len(
            unresolved_links, "codes_with_multiple_ids"
        ),
        "rule_ids_with_multiple_codes_count": _dict_len(
            unresolved_links, "ids_with_multiple_codes"
        ),
        "all_catalog_exercises_in_exercises_json": all_catalog_exercises_in_exercises_json,
    }


def run_all_checks(settings: Settings) -> dict[str, Any]:
    catalog_payload = load_learning_catalog(settings.learning_catalog_path)
    zpdes_rules_payload = load_zpdes_rules(settings.zpdes_rules_path)
    exercises_payload = load_exercises(settings.exercises_json_path)
    parquet = _parquet_profile(settings)
    catalog = _catalog_integrity(catalog_payload)
    exercise_ids = {row["id"] for row in exercises_payload["exercises"]}
    metadata_health = _metadata_health_metrics(
        catalog_payload,
        zpdes_rules_payload,
        catalog,
        exercise_ids,
    )

    row_counts = {
        "parquet_rows": parquet["rows"],
        "parquet_columns": parquet["columns"],
        "parquet_row_groups": parquet["row_groups"],
        "catalog_modules": catalog["modules"],
        "catalog_objectives": catalog["objectives"],
        "catalog_activities": catalog["activities"],
        "catalog_exercises_unique": catalog["catalog_exercise_ids_unique"],
        "exercises_count": len(exercise_ids),
    }

    checks_list = [
        _assert_equal("parquet_rows", row_counts["parquet_rows"], EXPECTED_BASELINE["parquet_rows"]),
        _assert_equal(
            "parquet_columns", row_counts["parquet_columns"], EXPECTED_BASELINE["parquet_columns"]
        ),
        _assert_equal(
            "parquet_row_groups",
            row_counts["parquet_row_groups"],
            EXPECTED_BASELINE["parquet_row_groups"],
        ),
        _assert_equal(
            "catalog_modules", row_counts["catalog_modules"], EXPECTED_BASELINE["catalog_modules"]
        ),
        _assert_equal(
            "catalog_objectives",
            row_counts["catalog_objectives"],
            EXPECTED_BASELINE["catalog_objectives"],
        ),
        _assert_equal(
            "catalog_activities",
            row_counts["catalog_activities"],
            EXPECTED_BASELINE["catalog_activities"],
        ),
        _assert_equal(
            "catalog_exercises_unique",
            row_counts["catalog_exercises_unique"],
            EXPECTED_BASELINE["catalog_exercises_unique"],
        ),
        _assert_equal(
            "exercises_count", row_counts["exercises_count"], EXPECTED_BASELINE["exercises_count"]
        ),
        _assert_equal(
            "catalog_missing_module_objective_refs",
            catalog["missing_module_objective_refs"],
            EXPECTED_BASELINE["catalog_missing_module_objective_refs"],
        ),
        _assert_equal(
            "catalog_missing_objective_activity_refs",
            catalog["missing_objective_activity_refs"],
            EXPECTED_BASELINE["catalog_missing_objective_activity_refs"],
        ),
    ]

    for key, expected in EXPECTED_SPARSE_NULL_COUNTS.items():
        checks_list.append(_assert_equal(f"null_count_{key}", parquet["null_counts"][key], expected))

    for key, expected in EXPECTED_BASELINE.items():
        if key in metadata_health:
            checks_list.append(_assert_equal(key, metadata_health[key], expected))

    check_map = {entry["name"]: entry for entry in checks_list}
    all_pass = all(entry["pass"] for entry in checks_list)

    return {
        "generated_at_utc": _ts(),
        "status": "pass" if all_pass else "fail",
        "row_counts": row_counts,
        "null_counts": parquet["null_counts"],
        "time_span_utc": parquet["time_span_utc"],
        "overlap_metrics": {key: int(value) for key, value in metadata_health.items()},
        "checks": check_map,
    }

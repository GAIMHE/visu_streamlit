"""Fact-table shaping and hierarchy-resolution helpers for derived analytics artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from .config import Settings
from .derive_catalog import (
    catalog_code_frames,
    catalog_id_lookup_frames,
    exercise_hierarchy_map_from_catalog,
    rules_id_code_frame,
)
from .derive_common import normalized_id_expr
from .reporting import load_json_report, write_json_report

RESOLUTION_SOURCE_RAW_ID_CATALOG = "raw_id_catalog"
RESOLUTION_SOURCE_GRAPH_CODE_FALLBACK = "graph_code_fallback"
RESOLUTION_SOURCE_EXERCISE_FALLBACK = "exercise_fallback"
RESOLUTION_SOURCE_MISSING = "missing"


@dataclass(frozen=True, slots=True)
class HierarchyResolutionBundle:
    """Resolved fact output plus the context-aware hierarchy lookup artifact."""

    fact_attempt_core: pl.DataFrame
    hierarchy_context_lookup: pl.DataFrame


def _extract_code_expr(column_name: str, pattern: str, alias: str) -> pl.Expr:
    """Extract one pedagogical code from a graph-code string column."""
    return pl.col(column_name).cast(pl.Utf8).str.extract(pattern, 1).alias(alias)


def _build_resolved_attempts(settings: Settings, sample_rows: int | None = None) -> pl.DataFrame:
    """Return the enriched attempt frame with resolution sources kept for audits/debugging."""
    module_lookup, objective_lookup, activity_lookup = catalog_id_lookup_frames(settings)
    exercise_hierarchy = exercise_hierarchy_map_from_catalog(settings)
    graph_id_code = rules_id_code_frame(settings)
    module_code_df, objective_code_df, activity_code_df = catalog_code_frames(settings)

    lf = pl.scan_parquet(settings.parquet_path)
    if sample_rows is not None:
        lf = lf.limit(sample_rows)

    graph_by_activity = graph_id_code.rename({"graph_id": "activity_id_raw", "graph_code": "graph_code_activity"})
    graph_by_objective = graph_id_code.rename(
        {"graph_id": "objective_id_raw", "graph_code": "graph_code_objective"}
    )
    graph_by_exercise = graph_id_code.rename({"graph_id": "exercise_id", "graph_code": "graph_code_exercise"})
    graph_by_playlist = graph_id_code.rename(
        {"graph_id": "playlist_or_module_id", "graph_code": "graph_code_playlist"}
    )

    module_context_lookup = module_code_df.rename(
        {
            "module_code_fallback": "module_code_context_fallback",
            "module_id_fallback": "module_id_context_fallback",
            "module_label_fallback": "module_label_context_fallback",
        }
    )
    objective_context_lookup = objective_code_df.rename(
        {
            "objective_code_fallback": "objective_code_context_fallback",
            "objective_id_fallback": "objective_id_context_fallback",
            "objective_label_fallback": "objective_label_context_fallback",
        }
    )
    activity_context_lookup = activity_code_df.rename(
        {
            "activity_code_fallback": "activity_code_context_fallback",
            "activity_id_fallback": "activity_id_context_fallback",
            "activity_label_fallback": "activity_label_context_fallback",
        }
    )

    module_exercise_lookup = module_code_df.rename(
        {
            "module_code_fallback": "module_code_exercise_fallback",
            "module_id_fallback": "module_id_exercise_fallback",
            "module_label_fallback": "module_label_exercise_fallback",
        }
    )
    objective_exercise_lookup = objective_code_df.rename(
        {
            "objective_code_fallback": "objective_code_exercise_fallback",
            "objective_id_fallback": "objective_id_exercise_fallback",
            "objective_label_fallback": "objective_label_exercise_fallback",
        }
    )
    activity_exercise_lookup = activity_code_df.rename(
        {
            "activity_code_fallback": "activity_code_exercise_fallback",
            "activity_id_fallback": "activity_id_exercise_fallback",
            "activity_label_fallback": "activity_label_exercise_fallback",
        }
    )

    return (
        lf.with_columns(
            [
                normalized_id_expr("playlist_or_module_id"),
                normalized_id_expr("objective_id"),
                normalized_id_expr("activity_id"),
                normalized_id_expr("exercise_id"),
            ]
        )
        .with_columns(
            [
                pl.col("objective_id").alias("objective_id_raw"),
                pl.col("activity_id").alias("activity_id_raw"),
            ]
        )
        .join(
            module_lookup.rename(
                {
                    "module_id_lookup": "playlist_or_module_id",
                    "module_code_lookup": "module_code_playlist_direct",
                    "module_label_lookup": "module_label_playlist_direct",
                }
            ).lazy(),
            on="playlist_or_module_id",
            how="left",
        )
        .join(
            objective_lookup.rename(
                {
                    "objective_id_lookup": "objective_id_raw",
                    "objective_code_lookup": "objective_code_direct",
                    "objective_label_lookup": "objective_label_direct",
                }
            ).lazy(),
            on="objective_id_raw",
            how="left",
        )
        .join(
            activity_lookup.rename(
                {
                    "activity_id_lookup": "activity_id_raw",
                    "activity_code_lookup": "activity_code_direct",
                    "activity_label_lookup": "activity_label_direct",
                }
            ).lazy(),
            on="activity_id_raw",
            how="left",
        )
        .join(exercise_hierarchy.lazy(), on="exercise_id", how="left")
        .join(graph_by_activity.lazy(), on="activity_id_raw", how="left")
        .join(graph_by_objective.lazy(), on="objective_id_raw", how="left")
        .join(graph_by_exercise.lazy(), on="exercise_id", how="left")
        .join(graph_by_playlist.lazy(), on="playlist_or_module_id", how="left")
        .with_columns(
            [
                _extract_code_expr(
                    "graph_code_playlist", r"^(M\d+)", "module_code_context_fallback"
                ),
                _extract_code_expr(
                    "graph_code_objective", r"^(M\d+O\d+)", "objective_code_context_from_objective"
                ),
                _extract_code_expr(
                    "graph_code_objective", r"^(M\d+)", "module_code_context_from_objective"
                ),
                _extract_code_expr(
                    "graph_code_activity", r"^(M\d+O\d+A\d+)", "activity_code_context_from_activity"
                ),
                _extract_code_expr(
                    "graph_code_activity", r"^(M\d+O\d+)", "objective_code_context_from_activity"
                ),
                _extract_code_expr(
                    "graph_code_activity", r"^(M\d+)", "module_code_context_from_activity"
                ),
                _extract_code_expr(
                    "graph_code_exercise", r"^(M\d+O\d+A\d+)", "activity_code_exercise_fallback"
                ),
                _extract_code_expr(
                    "graph_code_exercise", r"^(M\d+O\d+)", "objective_code_exercise_fallback"
                ),
                _extract_code_expr(
                    "graph_code_exercise", r"^(M\d+)", "module_code_exercise_fallback"
                ),
            ]
        )
        .with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("module_code_context_fallback"),
                        pl.col("module_code_context_from_objective"),
                        pl.col("module_code_context_from_activity"),
                    ]
                ).alias("module_code_context_fallback"),
                pl.coalesce(
                    [
                        pl.col("objective_code_context_from_objective"),
                        pl.col("objective_code_context_from_activity"),
                    ]
                ).alias("objective_code_context_fallback"),
                pl.col("activity_code_context_from_activity").alias("activity_code_context_fallback"),
            ]
        )
        .join(module_context_lookup.lazy(), on="module_code_context_fallback", how="left")
        .join(objective_context_lookup.lazy(), on="objective_code_context_fallback", how="left")
        .join(activity_context_lookup.lazy(), on="activity_code_context_fallback", how="left")
        .join(module_exercise_lookup.lazy(), on="module_code_exercise_fallback", how="left")
        .join(objective_exercise_lookup.lazy(), on="objective_code_exercise_fallback", how="left")
        .join(activity_exercise_lookup.lazy(), on="activity_code_exercise_fallback", how="left")
        .with_columns(
            [
                pl.col("objective_id_raw").is_not_null().alias("has_raw_objective_id"),
                pl.col("activity_id_raw").is_not_null().alias("has_raw_activity_id"),
                (
                    pl.col("objective_id_raw").is_null() & pl.col("activity_id_raw").is_null()
                ).alias("can_use_exercise_for_module"),
                pl.col("objective_id_raw").is_null().alias("can_use_exercise_for_objective"),
                pl.col("activity_id_raw").is_null().alias("can_use_exercise_for_activity"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("module_code_playlist_direct").is_not_null())
                .then(pl.lit(RESOLUTION_SOURCE_RAW_ID_CATALOG))
                .when(pl.col("module_code_context_fallback").is_not_null())
                .then(pl.lit(RESOLUTION_SOURCE_GRAPH_CODE_FALLBACK))
                .when(
                    pl.col("can_use_exercise_for_module")
                    & (
                        pl.col("module_code_exercise_summary").is_not_null()
                        | pl.col("module_code_exercise_fallback").is_not_null()
                    )
                )
                .then(pl.lit(RESOLUTION_SOURCE_EXERCISE_FALLBACK))
                .otherwise(pl.lit(RESOLUTION_SOURCE_MISSING))
                .alias("resolution_source_module"),
                pl.when(pl.col("has_raw_objective_id") & pl.col("objective_label_direct").is_not_null())
                .then(pl.lit(RESOLUTION_SOURCE_RAW_ID_CATALOG))
                .when(
                    (
                        pl.col("has_raw_objective_id")
                        & pl.col("objective_code_context_fallback").is_not_null()
                    )
                    | (
                        (~pl.col("has_raw_objective_id"))
                        & pl.col("objective_code_context_fallback").is_not_null()
                    )
                )
                .then(pl.lit(RESOLUTION_SOURCE_GRAPH_CODE_FALLBACK))
                .when(
                    pl.col("can_use_exercise_for_objective")
                    & (
                        pl.col("objective_label_exercise_summary").is_not_null()
                        | pl.col("objective_code_exercise_fallback").is_not_null()
                    )
                )
                .then(pl.lit(RESOLUTION_SOURCE_EXERCISE_FALLBACK))
                .otherwise(pl.lit(RESOLUTION_SOURCE_MISSING))
                .alias("resolution_source_objective"),
                pl.when(pl.col("has_raw_activity_id") & pl.col("activity_label_direct").is_not_null())
                .then(pl.lit(RESOLUTION_SOURCE_RAW_ID_CATALOG))
                .when(
                    (
                        pl.col("has_raw_activity_id")
                        & pl.col("activity_code_context_fallback").is_not_null()
                    )
                    | (
                        (~pl.col("has_raw_activity_id"))
                        & pl.col("activity_code_context_fallback").is_not_null()
                    )
                )
                .then(pl.lit(RESOLUTION_SOURCE_GRAPH_CODE_FALLBACK))
                .when(
                    pl.col("can_use_exercise_for_activity")
                    & (
                        pl.col("activity_label_exercise_summary").is_not_null()
                        | pl.col("activity_code_exercise_fallback").is_not_null()
                    )
                )
                .then(pl.lit(RESOLUTION_SOURCE_EXERCISE_FALLBACK))
                .otherwise(pl.lit(RESOLUTION_SOURCE_MISSING))
                .alias("resolution_source_activity"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("module_code_playlist_direct").is_not_null())
                .then(pl.col("playlist_or_module_id"))
                .when(pl.col("module_code_context_fallback").is_not_null())
                .then(pl.col("module_id_context_fallback"))
                .when(pl.col("can_use_exercise_for_module"))
                .then(
                    pl.coalesce(
                        [pl.col("module_id_exercise_summary"), pl.col("module_id_exercise_fallback")]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("module_id"),
                pl.when(pl.col("module_code_playlist_direct").is_not_null())
                .then(pl.col("module_code_playlist_direct"))
                .when(pl.col("module_code_context_fallback").is_not_null())
                .then(pl.col("module_code_context_fallback"))
                .when(pl.col("can_use_exercise_for_module"))
                .then(
                    pl.coalesce(
                        [pl.col("module_code_exercise_summary"), pl.col("module_code_exercise_fallback")]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("module_code"),
                pl.when(pl.col("module_code_playlist_direct").is_not_null())
                .then(pl.col("module_label_playlist_direct"))
                .when(pl.col("module_code_context_fallback").is_not_null())
                .then(pl.col("module_label_context_fallback"))
                .when(pl.col("can_use_exercise_for_module"))
                .then(
                    pl.coalesce(
                        [
                            pl.col("module_label_exercise_summary"),
                            pl.col("module_label_exercise_fallback"),
                        ]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("module_label"),
                pl.when(pl.col("has_raw_objective_id"))
                .then(pl.col("objective_id_raw"))
                .when(pl.col("objective_code_context_fallback").is_not_null())
                .then(pl.col("objective_id_context_fallback"))
                .when(pl.col("can_use_exercise_for_objective"))
                .then(
                    pl.coalesce(
                        [
                            pl.col("objective_id_exercise_summary"),
                            pl.col("objective_id_exercise_fallback"),
                        ]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("objective_id"),
                pl.when(pl.col("has_raw_objective_id"))
                .then(
                    pl.coalesce(
                        [pl.col("objective_label_direct"), pl.col("objective_label_context_fallback")]
                    )
                )
                .when(pl.col("objective_code_context_fallback").is_not_null())
                .then(pl.col("objective_label_context_fallback"))
                .when(pl.col("can_use_exercise_for_objective"))
                .then(
                    pl.coalesce(
                        [
                            pl.col("objective_label_exercise_summary"),
                            pl.col("objective_label_exercise_fallback"),
                        ]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("objective_label"),
                pl.when(pl.col("has_raw_activity_id"))
                .then(pl.col("activity_id_raw"))
                .when(pl.col("activity_code_context_fallback").is_not_null())
                .then(pl.col("activity_id_context_fallback"))
                .when(pl.col("can_use_exercise_for_activity"))
                .then(
                    pl.coalesce(
                        [
                            pl.col("activity_id_exercise_summary"),
                            pl.col("activity_id_exercise_fallback"),
                        ]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("activity_id"),
                pl.when(pl.col("has_raw_activity_id"))
                .then(
                    pl.coalesce(
                        [pl.col("activity_label_direct"), pl.col("activity_label_context_fallback")]
                    )
                )
                .when(pl.col("activity_code_context_fallback").is_not_null())
                .then(pl.col("activity_label_context_fallback"))
                .when(pl.col("can_use_exercise_for_activity"))
                .then(
                    pl.coalesce(
                        [
                            pl.col("activity_label_exercise_summary"),
                            pl.col("activity_label_exercise_fallback"),
                        ]
                    )
                )
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("activity_label"),
            ]
        )
        .with_columns(pl.col("created_at").dt.date().alias("date_utc"))
        .collect()
    )


def _build_context_lookup(resolved: pl.DataFrame) -> pl.DataFrame:
    """Collapse row-level resolution into a distinct context lookup with counts."""
    return (
        resolved.select(
            [
                "playlist_or_module_id",
                pl.col("objective_id_raw").alias("objective_id"),
                pl.col("activity_id_raw").alias("activity_id"),
                "exercise_id",
                "module_id",
                "module_code",
                "module_label",
                "objective_label",
                "activity_label",
                "resolution_source_module",
                "resolution_source_objective",
                "resolution_source_activity",
                "user_id",
            ]
        )
        .group_by(
            [
                "playlist_or_module_id",
                "objective_id",
                "activity_id",
                "exercise_id",
                "module_id",
                "module_code",
                "module_label",
                "objective_label",
                "activity_label",
                "resolution_source_module",
                "resolution_source_objective",
                "resolution_source_activity",
            ]
        )
        .agg(pl.len().alias("attempts"), pl.col("user_id").n_unique().alias("unique_students"))
        .sort(["attempts", "unique_students"], descending=[True, True])
    )


def build_hierarchy_resolution_bundle(
    settings: Settings, sample_rows: int | None = None
) -> HierarchyResolutionBundle:
    """Build the fact table and context-aware hierarchy lookup from one resolved attempt frame."""
    resolved = _build_resolved_attempts(settings, sample_rows=sample_rows)
    fact = resolved.select(
        [
            "created_at",
            "date_utc",
            "user_id",
            "classroom_id",
            "playlist_or_module_id",
            "objective_id",
            "objective_label",
            "activity_id",
            "activity_label",
            "exercise_id",
            "data_correct",
            "data_duration",
            "session_duration",
            "work_mode",
            "attempt_number",
            "module_id",
            "module_code",
            "module_label",
        ]
    )
    return HierarchyResolutionBundle(
        fact_attempt_core=fact,
        hierarchy_context_lookup=_build_context_lookup(resolved),
    )


def build_fact_attempt_core(settings: Settings, sample_rows: int | None = None) -> pl.DataFrame:
    """Build the canonical attempt-level runtime table used by downstream analytics views."""
    return build_hierarchy_resolution_bundle(settings, sample_rows=sample_rows).fact_attempt_core


def build_hierarchy_context_lookup(settings: Settings, sample_rows: int | None = None) -> pl.DataFrame:
    """Build the context-aware hierarchy lookup artifact keyed by raw attempt context."""
    return build_hierarchy_resolution_bundle(settings, sample_rows=sample_rows).hierarchy_context_lookup


def _sum_attempts(
    context_lookup: pl.DataFrame, filter_expr: pl.Expr | None = None, *, column: str = "attempts"
) -> int:
    """Return a safe integer sum across one filtered context lookup slice."""
    frame = context_lookup if filter_expr is None else context_lookup.filter(filter_expr)
    if frame.is_empty():
        return 0
    return int(frame.select(pl.col(column).sum().fill_null(0)).item())


def _spotlight_entry(
    context_lookup: pl.DataFrame,
    *,
    id_value: str,
    column_name: str,
) -> dict[str, Any]:
    """Build one spotlight payload for the hierarchy resolution report."""
    subset = context_lookup.filter(pl.col(column_name) == id_value)
    return {
        "column": column_name,
        "id": id_value,
        "context_count": int(subset.height),
        "attempts_total": _sum_attempts(subset),
        "contexts": subset.select(
            [
                "playlist_or_module_id",
                "objective_id",
                "activity_id",
                "exercise_id",
                "module_code",
                "module_label",
                "objective_label",
                "activity_label",
                "resolution_source_module",
                "resolution_source_objective",
                "resolution_source_activity",
                "attempts",
                "unique_students",
            ]
        ).head(12).to_dicts(),
    }


def build_hierarchy_resolution_report(context_lookup: pl.DataFrame) -> dict[str, Any]:
    """Summarize hierarchy-resolution behavior and known problematic ids."""
    rows_by_source: dict[str, dict[str, int]] = {}
    for level in ("module", "objective", "activity"):
        rows_by_source[level] = {
            row[f"resolution_source_{level}"]: int(row["attempts"])
            for row in context_lookup.group_by(f"resolution_source_{level}")
            .agg(pl.col("attempts").sum().alias("attempts"))
            .to_dicts()
        }

    unresolved_raw = {
        "module": _sum_attempts(
            context_lookup,
            pl.col("playlist_or_module_id").is_not_null()
            & (pl.col("resolution_source_module") == RESOLUTION_SOURCE_MISSING),
        ),
        "objective": _sum_attempts(
            context_lookup,
            pl.col("objective_id").is_not_null()
            & (pl.col("resolution_source_objective") == RESOLUTION_SOURCE_MISSING),
        ),
        "activity": _sum_attempts(
            context_lookup,
            pl.col("activity_id").is_not_null()
            & (pl.col("resolution_source_activity") == RESOLUTION_SOURCE_MISSING),
        ),
    }
    exercise_fallback_rows = {
        "module": _sum_attempts(
            context_lookup,
            pl.col("resolution_source_module") == RESOLUTION_SOURCE_EXERCISE_FALLBACK,
        ),
        "objective": _sum_attempts(
            context_lookup,
            pl.col("resolution_source_objective") == RESOLUTION_SOURCE_EXERCISE_FALLBACK,
        ),
        "activity": _sum_attempts(
            context_lookup,
            pl.col("resolution_source_activity") == RESOLUTION_SOURCE_EXERCISE_FALLBACK,
        ),
    }

    multi_context_exercises = (
        context_lookup.group_by("exercise_id")
        .agg(
            pl.struct(["playlist_or_module_id", "objective_id", "activity_id"])
            .n_unique()
            .alias("context_count"),
            pl.col("attempts").sum().alias("attempts"),
        )
        .filter((pl.col("exercise_id").is_not_null()) & (pl.col("context_count") > 1))
    )

    return {
        "rows_by_resolution_source": rows_by_source,
        "raw_ids_present_but_unresolved_rows": unresolved_raw,
        "exercise_fallback_rows": exercise_fallback_rows,
        "exercise_ids_with_multiple_raw_contexts": int(multi_context_exercises.height),
        "max_contexts_for_one_exercise": int(
            multi_context_exercises.select(pl.col("context_count").max().fill_null(0)).item()
        )
        if multi_context_exercises.height
        else 0,
        "spotlights": {
            "d6eb6eda-16ca-4941-914d-d585bcf0eba9": _spotlight_entry(
                context_lookup,
                id_value="d6eb6eda-16ca-4941-914d-d585bcf0eba9",
                column_name="exercise_id",
            ),
            "da746988-c358-405a-9d52-e28aee189475": _spotlight_entry(
                context_lookup,
                id_value="da746988-c358-405a-9d52-e28aee189475",
                column_name="activity_id",
            ),
            "4865a2cc-d48b-4ebd-81e3-78cbd0dbb665": _spotlight_entry(
                context_lookup,
                id_value="4865a2cc-d48b-4ebd-81e3-78cbd0dbb665",
                column_name="exercise_id",
            ),
            "9675ae4d-f13e-42c6-9d3c-78918c67b592": _spotlight_entry(
                context_lookup,
                id_value="9675ae4d-f13e-42c6-9d3c-78918c67b592",
                column_name="exercise_id",
            ),
        },
    }


def refresh_hierarchy_resolution_report(settings: Settings) -> dict[str, Any]:
    """Refresh the persisted hierarchy-resolution report from the current runtime tables."""
    context_lookup_path = settings.legacy_artifacts_derived_dir / "hierarchy_context_lookup.parquet"
    if not context_lookup_path.exists():
        raise FileNotFoundError(
            f"Hierarchy context lookup is required before writing the hierarchy report: {context_lookup_path}"
        )
    context_lookup = pl.read_parquet(context_lookup_path)
    report = build_hierarchy_resolution_report(context_lookup)
    write_json_report(report, settings.hierarchy_resolution_report_path)
    return report


def load_hierarchy_resolution_report(settings: Settings) -> dict[str, Any]:
    """Load the persisted hierarchy-resolution report for one runtime source."""
    return load_json_report(settings.hierarchy_resolution_report_path)


__all__ = [
    "HierarchyResolutionBundle",
    "build_fact_attempt_core",
    "build_hierarchy_context_lookup",
    "build_hierarchy_resolution_bundle",
    "build_hierarchy_resolution_report",
    "load_hierarchy_resolution_report",
    "refresh_hierarchy_resolution_report",
]

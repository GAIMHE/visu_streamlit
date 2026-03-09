"""Fact-table shaping for derived analytics artifacts."""

from __future__ import annotations

import polars as pl

from .config import Settings
from .derive_catalog import (
    catalog_code_frames,
    exercise_hierarchy_map_from_catalog,
    hierarchy_map_from_catalog,
    rules_id_code_frame,
)
from .derive_common import normalized_id_expr


def build_fact_attempt_core(settings: Settings, sample_rows: int | None = None) -> pl.DataFrame:
    """Build the canonical attempt-level runtime table used by all downstream views."""
    hierarchy = hierarchy_map_from_catalog(settings)
    exercise_hierarchy = exercise_hierarchy_map_from_catalog(settings)
    graph_id_code = rules_id_code_frame(settings)
    module_code_df, objective_code_df, activity_code_df = catalog_code_frames(settings)

    lf = pl.scan_parquet(settings.parquet_path)
    if sample_rows is not None:
        lf = lf.limit(sample_rows)

    graph_by_activity = graph_id_code.rename({"graph_id": "activity_id", "graph_code": "graph_code_activity"})
    graph_by_objective = graph_id_code.rename(
        {"graph_id": "objective_id", "graph_code": "graph_code_objective"}
    )
    graph_by_exercise = graph_id_code.rename({"graph_id": "exercise_id", "graph_code": "graph_code_exercise"})
    graph_by_playlist = graph_id_code.rename(
        {"graph_id": "playlist_or_module_id", "graph_code": "graph_code_playlist"}
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
        .join(hierarchy.lazy(), on="activity_id", how="left")
        .join(exercise_hierarchy.lazy(), on="exercise_id", how="left")
        .join(graph_by_activity.lazy(), on="activity_id", how="left")
        .join(graph_by_objective.lazy(), on="objective_id", how="left")
        .join(graph_by_exercise.lazy(), on="exercise_id", how="left")
        .join(graph_by_playlist.lazy(), on="playlist_or_module_id", how="left")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("graph_code_activity"),
                    pl.col("graph_code_objective"),
                    pl.col("graph_code_exercise"),
                    pl.col("graph_code_playlist"),
                ]
            ).alias("fallback_code_raw")
        )
        .with_columns(
            pl.col("fallback_code_raw")
            .cast(pl.Utf8)
            .str.extract(r"^(M\d+)", 1)
            .alias("module_code_fallback"),
            pl.col("fallback_code_raw")
            .cast(pl.Utf8)
            .str.extract(r"^(M\d+O\d+)", 1)
            .alias("objective_code_fallback"),
            pl.col("fallback_code_raw")
            .cast(pl.Utf8)
            .str.extract(r"^(M\d+O\d+A\d+)", 1)
            .alias("activity_code_fallback"),
        )
        .join(module_code_df.lazy(), on="module_code_fallback", how="left")
        .join(objective_code_df.lazy(), on="objective_code_fallback", how="left")
        .join(activity_code_df.lazy(), on="activity_code_fallback", how="left")
        .with_columns(
            pl.col("module_code").is_not_null().alias("has_activity_catalog_mapping"),
            pl.col("module_code_exercise_summary").is_not_null().alias("has_exercise_catalog_mapping"),
            pl.coalesce(
                [
                    pl.col("objective_id_summary"),
                    pl.col("objective_id_exercise_summary"),
                    pl.col("objective_id_fallback"),
                ]
            ).alias("objective_id_summary"),
            pl.coalesce(
                [
                    pl.col("activity_id"),
                    pl.col("activity_id_exercise_summary"),
                    pl.col("activity_id_fallback"),
                ]
            ).alias("activity_id"),
            pl.coalesce(
                [
                    pl.col("objective_id"),
                    pl.col("objective_id_summary"),
                    pl.col("objective_id_exercise_summary"),
                    pl.col("objective_id_fallback"),
                ]
            ).alias("objective_id"),
        )
        .with_columns(
            pl.coalesce(
                [
                    pl.col("module_code"),
                    pl.col("module_code_exercise_summary"),
                    pl.col("module_code_fallback"),
                ]
            ).alias("module_code"),
            pl.coalesce(
                [
                    pl.col("module_id"),
                    pl.col("module_id_exercise_summary"),
                    pl.col("module_id_fallback"),
                ]
            ).alias("module_id"),
            pl.coalesce(
                [
                    pl.col("module_label"),
                    pl.col("module_label_exercise_summary"),
                    pl.col("module_label_fallback"),
                ]
            ).alias("module_label"),
            pl.coalesce(
                [
                    pl.col("objective_label"),
                    pl.col("objective_label_exercise_summary"),
                    pl.col("objective_label_fallback"),
                ]
            ).alias("objective_label"),
            pl.coalesce(
                [
                    pl.col("activity_label"),
                    pl.col("activity_label_exercise_summary"),
                    pl.col("activity_label_fallback"),
                ]
            ).alias("activity_label"),
        )
        .with_columns(pl.col("created_at").dt.date().alias("date_utc"))
        .select(
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
        .collect()
    )

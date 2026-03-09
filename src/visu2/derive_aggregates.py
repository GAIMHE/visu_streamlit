"""Non-Elo aggregate builders for the derived runtime tables."""

from __future__ import annotations

import polars as pl

from .config import Settings
from .derive_catalog import exercise_metadata_frame
from .derive_common import as_lazy


def build_agg_activity_daily_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Aggregate attempt metrics at the activity/day grain."""
    return (
        as_lazy(fact)
        .with_columns(
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.col("data_correct").cast(pl.Float64))
            .otherwise(None)
            .alias("first_attempt_correct_value"),
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("first_attempt_flag"),
        )
        .group_by(
            [
                "date_utc",
                "activity_id",
                "activity_label",
                "objective_id",
                "objective_label",
                "module_id",
                "module_code",
                "module_label",
            ]
        )
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").n_unique().alias("unique_students"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("first_attempt_correct_value").mean().alias("first_attempt_success_rate"),
            pl.col("first_attempt_flag").sum().alias("first_attempt_count"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
            pl.col("attempt_number").mean().alias("avg_attempt_number"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_objective_daily_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Aggregate attempt metrics at the objective/day grain."""
    return (
        as_lazy(fact)
        .group_by(["date_utc", "objective_id", "objective_label", "module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").n_unique().alias("unique_students"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_student_module_progress_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Aggregate per-student progression summaries within each module."""
    return (
        as_lazy(fact)
        .group_by(["date_utc", "user_id", "module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("activity_id").n_unique().alias("unique_activities"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("attempt_number").mean().alias("avg_attempt_number"),
            pl.col("created_at").max().alias("last_attempt_at"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_module_usage_daily_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Aggregate daily module usage counts."""
    return (
        as_lazy(fact)
        .group_by(["date_utc", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_playlist_module_usage_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Aggregate playlist/module combinations for the hidden usage page and audits."""
    return (
        as_lazy(fact)
        .with_columns(
            pl.col("module_code")
            .is_not_null()
            .any()
            .over("playlist_or_module_id")
            .alias("playlist_has_mapped_module")
        )
        .filter(~(pl.col("playlist_has_mapped_module") & pl.col("module_code").is_null()))
        .group_by(["playlist_or_module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
            pl.col("classroom_id").drop_nulls().n_unique().alias("unique_classrooms"),
            pl.col("activity_id").drop_nulls().n_unique().alias("unique_activities"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("work_mode").drop_nulls().n_unique().alias("work_mode_unique_count"),
            pl.col("work_mode").drop_nulls().first().alias("work_mode_first"),
        )
        .with_columns(
            pl.when(pl.col("work_mode_unique_count") == 0)
            .then(pl.lit("unknown"))
            .when(pl.col("work_mode_unique_count") == 1)
            .then(pl.col("work_mode_first"))
            .otherwise(pl.lit("mixed"))
            .alias("work_mode")
        )
        .drop(["work_mode_unique_count", "work_mode_first"])
        .sort(["module_code", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_module_activity_usage_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Aggregate activity usage shares within each module."""
    return (
        as_lazy(fact)
        .group_by(["module_code", "module_label", "activity_id", "activity_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
        )
        .with_columns(
            (pl.col("attempts") / pl.col("attempts").sum().over("module_code")).alias(
                "activity_share_within_module"
            )
        )
        .sort(["module_code", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_exercise_daily_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Aggregate attempt metrics at the exercise/day grain."""
    exercise_meta = exercise_metadata_frame(settings)
    return (
        as_lazy(fact)
        .with_columns(
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.col("data_correct").cast(pl.Float64))
            .otherwise(None)
            .alias("first_attempt_correct_value"),
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("first_attempt_flag"),
        )
        .join(exercise_meta.lazy(), on="exercise_id", how="left")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("exercise_label_meta").cast(pl.Utf8),
                    pl.col("exercise_id").cast(pl.Utf8),
                ]
            ).alias("exercise_label")
        )
        .group_by(
            [
                "date_utc",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "exercise_id",
                "exercise_label",
                "exercise_type",
            ]
        )
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").n_unique().alias("unique_students"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("first_attempt_correct_value").mean().alias("first_attempt_success_rate"),
            pl.col("first_attempt_flag").sum().alias("first_attempt_count"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
            pl.col("attempt_number").mean().alias("avg_attempt_number"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )

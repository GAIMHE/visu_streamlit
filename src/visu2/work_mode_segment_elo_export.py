"""Export batch Elo estimates for initial work-mode segment windows."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .adaptive_test_elo_export import (
    ELO_CONTEXT_KEYS,
    _fit_grouped_student_ratings,
)

WORK_MODES: tuple[str, ...] = ("playlist", "zpdes")

FACT_COLUMNS: tuple[str, ...] = (
    "user_id",
    "classroom_id",
    "playlist_or_module_id",
    "module_id",
    "module_code",
    "module_label",
    "objective_id",
    "activity_id",
    "exercise_id",
    "created_at",
    "data_correct",
    "work_mode",
    "attempt_number",
)

OUTPUT_COLUMNS: tuple[str, ...] = (
    "segment_id",
    "segment_ordinal",
    "segment_total_attempts",
    "segment_attempt_position",
    "user_id",
    "classroom_id",
    "playlist_or_module_id",
    "module_id",
    "module_code",
    "module_label",
    "work_mode",
    "created_at",
    "objective_id",
    "activity_id",
    "exercise_id",
    "attempt_number",
    "data_correct",
    "exercise_elo",
    "exercise_elo_calibrated",
    "segment_elo_attempts",
    "segment_elo_coverage",
    "segment_success_rate_first_window",
    "student_elo_first_window",
    "has_student_elo",
)


def _require_columns(schema: pl.Schema, required: tuple[str, ...], *, label: str) -> None:
    missing = sorted(set(required).difference(schema.names()))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def _build_eligible_segment_window(
    fact_path: Path,
    *,
    first_window_attempts: int,
    min_segment_attempts: int,
) -> pl.LazyFrame:
    fact = pl.scan_parquet(fact_path).with_row_index("_source_order")
    _require_columns(fact.collect_schema(), FACT_COLUMNS, label="Fact table")

    first_retained_attempts = (
        fact.filter(
            pl.col("work_mode").is_in(WORK_MODES)
            & pl.col("user_id").is_not_null()
            & pl.col("module_id").is_not_null()
            & pl.col("module_code").is_not_null()
            & pl.col("exercise_id").is_not_null()
            & pl.col("created_at").is_not_null()
            & pl.col("data_correct").is_not_null()
        )
        .select(["_source_order", *FACT_COLUMNS])
        .sort(["user_id", "created_at", "_source_order"])
        .unique(subset=["user_id", "exercise_id"], keep="first", maintain_order=True)
        .sort(["user_id", "created_at", "_source_order"])
    )

    segmented = (
        first_retained_attempts.with_columns(
            pl.col("module_id").shift(1).over("user_id").alias("_previous_module_id"),
            pl.col("work_mode").shift(1).over("user_id").alias("_previous_work_mode"),
        )
        .with_columns(
            (
                pl.col("_previous_module_id").is_null()
                | (pl.col("module_id") != pl.col("_previous_module_id"))
                | (pl.col("work_mode") != pl.col("_previous_work_mode"))
            )
            .cast(pl.Int64)
            .alias("_starts_segment")
        )
        .with_columns(
            pl.col("_starts_segment")
            .cum_sum()
            .over("user_id")
            .cast(pl.Int64)
            .alias("segment_ordinal")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("user_id"),
                    pl.col("module_code"),
                    pl.col("work_mode"),
                    pl.col("segment_ordinal").cast(pl.Utf8).str.zfill(6),
                ],
                separator="::",
            ).alias("segment_id")
        )
        .with_columns(
            pl.len().over("segment_id").cast(pl.Int64).alias("segment_total_attempts"),
            pl.col("_source_order")
            .cum_count()
            .over("segment_id")
            .cast(pl.Int64)
            .alias("segment_attempt_position"),
        )
        .filter(
            (pl.col("segment_total_attempts") >= min_segment_attempts)
            & (pl.col("segment_attempt_position") <= first_window_attempts)
        )
        .drop(
            [
                "_source_order",
                "_previous_module_id",
                "_previous_work_mode",
                "_starts_segment",
            ]
        )
    )
    return segmented


def build_work_mode_segment_elo_export(
    fact_path: Path,
    exercise_elo_path: Path,
    *,
    first_window_attempts: int = 15,
    min_segment_attempts: int = 30,
) -> pl.DataFrame:
    """Fit one batch Elo from the first retained attempts of each segment.

    Segments are contiguous student-module-work-mode runs after retaining the
    first playlist/ZPDES encounter with each exercise. A segment must contain
    at least ``min_segment_attempts`` retained attempts. The batch Elo uses only
    its first ``first_window_attempts`` rows and the same fixed-item Gaussian-
    prior fit as the adaptive-test Elo export.

    The returned table is event-level: every retained row in the initial window
    carries the segment-level Elo and a deterministic segment identifier.
    """

    if first_window_attempts < 1:
        raise ValueError("first_window_attempts must be at least 1")
    if min_segment_attempts < first_window_attempts:
        raise ValueError(
            "min_segment_attempts must be greater than or equal to "
            "first_window_attempts"
        )
    if not fact_path.exists():
        raise FileNotFoundError(f"Fact table not found: {fact_path}")
    if not exercise_elo_path.exists():
        raise FileNotFoundError(f"Exercise Elo table not found: {exercise_elo_path}")

    segment_window = _build_eligible_segment_window(
        fact_path,
        first_window_attempts=first_window_attempts,
        min_segment_attempts=min_segment_attempts,
    )
    exercise_elo = pl.scan_parquet(exercise_elo_path)
    _require_columns(
        exercise_elo.collect_schema(),
        (*ELO_CONTEXT_KEYS, "exercise_elo", "calibrated"),
        label="Exercise Elo table",
    )
    exercise_elo = exercise_elo.select(
        [
            *ELO_CONTEXT_KEYS,
            "exercise_elo",
            pl.col("calibrated").alias("exercise_elo_calibrated"),
        ]
    )
    events = segment_window.join(
        exercise_elo,
        on=list(ELO_CONTEXT_KEYS),
        how="left",
        validate="m:1",
    ).collect()

    segment_summary = events.group_by("segment_id").agg(
        pl.col("exercise_elo")
        .is_not_null()
        .sum()
        .cast(pl.Int64)
        .alias("segment_elo_attempts"),
        pl.col("data_correct")
        .cast(pl.Float64)
        .mean()
        .alias("segment_success_rate_first_window"),
    )
    observations = (
        events.filter(pl.col("exercise_elo").is_not_null())
        .group_by(["segment_id", "exercise_elo"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("data_correct").cast(pl.Float64).sum().alias("successes"),
        )
        .sort("segment_id")
    )
    ratings = _fit_grouped_student_ratings(
        observations,
        ["segment_id"],
        rating_column="student_elo_first_window",
    )
    return (
        events.join(segment_summary, on="segment_id", how="left", validate="m:1")
        .join(ratings, on="segment_id", how="left", validate="m:1")
        .with_columns(
            (
                pl.col("segment_elo_attempts")
                / pl.lit(float(first_window_attempts))
            ).alias("segment_elo_coverage"),
            pl.col("student_elo_first_window").is_not_null().alias("has_student_elo"),
        )
        .select(OUTPUT_COLUMNS)
        .sort(["user_id", "segment_ordinal", "segment_attempt_position"])
    )

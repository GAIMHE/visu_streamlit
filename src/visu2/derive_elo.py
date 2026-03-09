"""Elo calibration and replay builders for derived artifacts."""

from __future__ import annotations

import polars as pl

from .config import Settings
from .derive_catalog import exercise_catalog_elo_base_frame
from .derive_common import ELO_BASE_RATING, ELO_K, as_lazy, elo_expected_success, outcome_value


def _empty_exercise_elo_df() -> pl.DataFrame:
    """Return an empty exercise Elo frame with stable dtypes."""
    return pl.DataFrame(
        {
            "exercise_id": [],
            "exercise_label": [],
            "exercise_type": [],
            "module_id": [],
            "module_code": [],
            "module_label": [],
            "objective_id": [],
            "objective_label": [],
            "activity_id": [],
            "activity_label": [],
            "exercise_elo": [],
            "calibration_attempts": [],
            "calibration_success_rate": [],
            "calibrated": [],
        },
        schema={
            "exercise_id": pl.Utf8,
            "exercise_label": pl.Utf8,
            "exercise_type": pl.Utf8,
            "module_id": pl.Utf8,
            "module_code": pl.Utf8,
            "module_label": pl.Utf8,
            "objective_id": pl.Utf8,
            "objective_label": pl.Utf8,
            "activity_id": pl.Utf8,
            "activity_label": pl.Utf8,
            "exercise_elo": pl.Float64,
            "calibration_attempts": pl.Int64,
            "calibration_success_rate": pl.Float64,
            "calibrated": pl.Boolean,
        },
    )


def _empty_student_elo_events_df() -> pl.DataFrame:
    """Return an empty student Elo event frame with stable dtypes."""
    return pl.DataFrame(
        {
            "user_id": [],
            "attempt_ordinal": [],
            "created_at": [],
            "date_utc": [],
            "work_mode": [],
            "module_code": [],
            "objective_id": [],
            "activity_id": [],
            "exercise_id": [],
            "outcome": [],
            "expected_success": [],
            "exercise_elo": [],
            "student_elo_pre": [],
            "student_elo_post": [],
        },
        schema={
            "user_id": pl.Utf8,
            "attempt_ordinal": pl.Int64,
            "created_at": pl.Datetime,
            "date_utc": pl.Date,
            "work_mode": pl.Utf8,
            "module_code": pl.Utf8,
            "objective_id": pl.Utf8,
            "activity_id": pl.Utf8,
            "exercise_id": pl.Utf8,
            "outcome": pl.Float64,
            "expected_success": pl.Float64,
            "exercise_elo": pl.Float64,
            "student_elo_pre": pl.Float64,
            "student_elo_post": pl.Float64,
        },
    )


def build_agg_exercise_elo_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Calibrate a fixed retrospective Elo difficulty for each exercise."""
    base = exercise_catalog_elo_base_frame(settings)
    if base.height == 0:
        return _empty_exercise_elo_df()

    valid_exercise_ids = base["exercise_id"].to_list()
    calibration = (
        as_lazy(fact)
        .filter(
            (pl.col("attempt_number") == 1)
            & pl.col("created_at").is_not_null()
            & pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("data_correct").is_not_null()
            & pl.col("exercise_id").cast(pl.Utf8).is_in(valid_exercise_ids)
        )
        .select(["created_at", "user_id", "exercise_id", "data_correct"])
        .sort(["created_at", "user_id", "exercise_id"])
        .collect()
    )

    student_ratings: dict[str, float] = {}
    exercise_ratings: dict[str, float] = {}
    exercise_attempts: dict[str, int] = {}
    exercise_successes: dict[str, float] = {}

    for created_at, user_id, exercise_id, raw_outcome in calibration.iter_rows():
        if created_at is None:
            continue
        user_key = str(user_id).strip()
        exercise_key = str(exercise_id).strip()
        if not user_key or not exercise_key:
            continue
        outcome = outcome_value(raw_outcome)
        if outcome is None:
            continue

        student_rating = student_ratings.get(user_key, ELO_BASE_RATING)
        exercise_rating = exercise_ratings.get(exercise_key, ELO_BASE_RATING)
        expected = elo_expected_success(student_rating, exercise_rating)
        delta = ELO_K * (outcome - expected)
        student_ratings[user_key] = student_rating + delta
        exercise_ratings[exercise_key] = exercise_rating - delta
        exercise_attempts[exercise_key] = exercise_attempts.get(exercise_key, 0) + 1
        exercise_successes[exercise_key] = exercise_successes.get(exercise_key, 0.0) + outcome

    stats_rows = [
        {
            "exercise_id": exercise_id,
            "exercise_elo": float(exercise_ratings[exercise_id]),
            "calibration_attempts": int(exercise_attempts[exercise_id]),
            "calibration_success_rate": float(exercise_successes[exercise_id])
            / float(exercise_attempts[exercise_id]),
            "calibrated": True,
        }
        for exercise_id in exercise_attempts
    ]
    stats = (
        pl.DataFrame(stats_rows)
        if stats_rows
        else pl.DataFrame(
            {
                "exercise_id": [],
                "exercise_elo": [],
                "calibration_attempts": [],
                "calibration_success_rate": [],
                "calibrated": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "exercise_elo": pl.Float64,
                "calibration_attempts": pl.Int64,
                "calibration_success_rate": pl.Float64,
                "calibrated": pl.Boolean,
            },
        )
    )

    return (
        base.join(stats, on="exercise_id", how="left")
        .with_columns(
            pl.col("calibrated").fill_null(False),
            pl.col("calibration_attempts").fill_null(0).cast(pl.Int64),
            pl.when(pl.col("calibrated"))
            .then(pl.col("exercise_elo"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("exercise_elo"),
            pl.when(pl.col("calibrated"))
            .then(pl.col("calibration_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("calibration_success_rate"),
            pl.coalesce([pl.col("exercise_type"), pl.lit("unknown")]).alias("exercise_type"),
        )
        .select(
            [
                "exercise_id",
                "exercise_label",
                "exercise_type",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "exercise_elo",
                "calibration_attempts",
                "calibration_success_rate",
                "calibrated",
            ]
        )
        .sort(["module_code", "objective_id", "activity_id", "exercise_id"])
    )


def build_agg_activity_elo_from_exercise_elo(
    exercise_elo: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Aggregate exercise Elo scores to the activity level."""
    del settings
    return (
        as_lazy(exercise_elo)
        .group_by(
            [
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
            ]
        )
        .agg(
            pl.col("exercise_elo").drop_nulls().mean().alias("activity_mean_exercise_elo"),
            pl.col("calibrated").cast(pl.Int64).sum().alias("calibrated_exercise_count"),
            pl.len().cast(pl.Int64).alias("catalog_exercise_count"),
        )
        .with_columns(
            pl.when(pl.col("catalog_exercise_count") > 0)
            .then(
                pl.col("calibrated_exercise_count").cast(pl.Float64)
                / pl.col("catalog_exercise_count").cast(pl.Float64)
            )
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("calibration_coverage_ratio")
        )
        .sort(["module_code", "objective_id", "activity_id"])
        .collect()
    )


def build_student_elo_events_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    exercise_elo: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Replay student Elo trajectories against frozen exercise difficulty."""
    exercise_frame = as_lazy(exercise_elo).filter(pl.col("calibrated") & pl.col("exercise_elo").is_not_null())
    exercise_map_df = (
        exercise_frame.select(["exercise_id", "exercise_elo"])
        .collect()
        .unique(subset=["exercise_id"], keep="first")
    )
    if exercise_map_df.height == 0:
        return _empty_student_elo_events_df()

    valid_exercise_ids = exercise_map_df["exercise_id"].to_list()
    exercise_elo_map = {
        str(row["exercise_id"]): float(row["exercise_elo"])
        for row in exercise_map_df.to_dicts()
    }

    replay = (
        as_lazy(fact)
        .filter(
            pl.col("created_at").is_not_null()
            & pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("data_correct").is_not_null()
            & pl.col("exercise_id").cast(pl.Utf8).is_in(valid_exercise_ids)
        )
        .select(
            [
                "user_id",
                "created_at",
                "date_utc",
                "work_mode",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "attempt_number",
                "data_correct",
            ]
        )
        .sort(["user_id", "created_at", "exercise_id", "attempt_number"])
        .collect()
    )

    rows: list[dict[str, object]] = []
    current_user: str | None = None
    current_rating = ELO_BASE_RATING
    current_ordinal = 0

    for (
        user_id,
        created_at,
        date_utc,
        work_mode,
        module_code,
        objective_id,
        activity_id,
        exercise_id,
        _attempt_number,
        raw_outcome,
    ) in replay.iter_rows():
        user_key = str(user_id).strip()
        exercise_key = str(exercise_id).strip()
        if not user_key or not exercise_key:
            continue
        outcome = outcome_value(raw_outcome)
        if outcome is None:
            continue
        if current_user != user_key:
            current_user = user_key
            current_rating = ELO_BASE_RATING
            current_ordinal = 0
        exercise_rating = exercise_elo_map.get(exercise_key)
        if exercise_rating is None:
            continue

        current_ordinal += 1
        student_elo_pre = current_rating
        expected_success = elo_expected_success(student_elo_pre, exercise_rating)
        delta = ELO_K * (outcome - expected_success)
        student_elo_post = student_elo_pre + delta
        current_rating = student_elo_post
        rows.append(
            {
                "user_id": user_key,
                "attempt_ordinal": current_ordinal,
                "created_at": created_at,
                "date_utc": date_utc,
                "work_mode": None if work_mode is None else str(work_mode),
                "module_code": None if module_code is None else str(module_code),
                "objective_id": None if objective_id is None else str(objective_id),
                "activity_id": None if activity_id is None else str(activity_id),
                "exercise_id": exercise_key,
                "outcome": outcome,
                "expected_success": expected_success,
                "exercise_elo": exercise_rating,
                "student_elo_pre": student_elo_pre,
                "student_elo_post": student_elo_post,
            }
        )

    if not rows:
        return _empty_student_elo_events_df()
    return pl.DataFrame(rows).sort(["user_id", "attempt_ordinal"])


def build_student_elo_profiles_from_events(
    events: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Aggregate compact per-student replay summaries from replay events."""
    return (
        as_lazy(events)
        .sort(["user_id", "attempt_ordinal"])
        .group_by("user_id")
        .agg(
            pl.len().cast(pl.Int64).alias("total_attempts"),
            pl.col("created_at").min().alias("first_attempt_at"),
            pl.col("created_at").max().alias("last_attempt_at"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("unique_modules"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_objectives"),
            pl.col("activity_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_activities"),
            pl.col("student_elo_post").last().alias("final_student_elo"),
        )
        .with_columns((pl.col("total_attempts") > 0).alias("eligible_for_replay"))
        .sort(["total_attempts", "final_student_elo", "user_id"], descending=[True, True, False])
        .collect()
    )

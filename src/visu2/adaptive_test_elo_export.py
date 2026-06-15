"""Export helpers for adaptive-test-based initial Elo estimates."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .derive_common import ELO_BASE_RATING
from .derive_elo import _fit_batch_student_rating_weighted

ELO_CONTEXT_KEYS: tuple[str, ...] = (
    "module_code",
    "objective_id",
    "activity_id",
    "exercise_id",
)

ADAPTIVE_TEST_ELO_COLUMNS: tuple[str, ...] = (
    "user_id",
    "module_id",
    "module_code",
    "module_label",
    "adaptive_test_attempts",
    "adaptive_test_elo_attempts",
    "adaptive_test_activities",
    "adaptive_test_exercise_contexts",
    "adaptive_test_start",
    "adaptive_test_end",
    "adaptive_test_success_rate",
    "adaptive_test_elo",
    "has_adaptive_test_elo",
    "first_practice_at",
    "practice_attempts",
    "has_same_module_practice",
    "all_adaptive_test_attempts_before_first_practice",
    "any_adaptive_test_attempt_after_first_practice",
)


def _empty_adaptive_test_elo_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {column: [] for column in ADAPTIVE_TEST_ELO_COLUMNS},
        schema={
            "user_id": pl.Utf8,
            "module_id": pl.Utf8,
            "module_code": pl.Utf8,
            "module_label": pl.Utf8,
            "adaptive_test_attempts": pl.Int64,
            "adaptive_test_elo_attempts": pl.Int64,
            "adaptive_test_activities": pl.Int64,
            "adaptive_test_exercise_contexts": pl.Int64,
            "adaptive_test_start": pl.Datetime,
            "adaptive_test_end": pl.Datetime,
            "adaptive_test_success_rate": pl.Float64,
            "adaptive_test_elo": pl.Float64,
            "has_adaptive_test_elo": pl.Boolean,
            "first_practice_at": pl.Datetime,
            "practice_attempts": pl.Int64,
            "has_same_module_practice": pl.Boolean,
            "all_adaptive_test_attempts_before_first_practice": pl.Boolean,
            "any_adaptive_test_attempt_after_first_practice": pl.Boolean,
        },
    )


def _fit_grouped_student_ratings(
    observations: pl.DataFrame,
    group_keys: list[str],
    *,
    rating_column: str,
) -> pl.DataFrame:
    ratings: list[dict[str, object]] = []
    for key_values, group in observations.group_by(group_keys, maintain_order=True):
        if not isinstance(key_values, tuple):
            key_values = (key_values,)
        fit_observations = [
            (
                float(row["exercise_elo"]),
                int(row["attempts"]),
                float(row["successes"]),
            )
            for row in group.to_dicts()
        ]
        ratings.append(
            {
                **dict(zip(group_keys, key_values, strict=True)),
                rating_column: _fit_batch_student_rating_weighted(
                    fit_observations,
                    initial_rating=ELO_BASE_RATING,
                ),
            }
        )
    if ratings:
        return pl.DataFrame(ratings)
    return pl.DataFrame(
        {**{key: [] for key in group_keys}, rating_column: []},
        schema={**{key: pl.Utf8 for key in group_keys}, rating_column: pl.Float64},
    )


def build_student_module_adaptive_test_elo(
    fact_path: Path,
    exercise_elo_path: Path,
    *,
    only_before_practice: bool = False,
) -> pl.DataFrame:
    """Fit one student Elo per adaptive-test block and module.

    The fit uses all adaptive-test outcomes in a student-module block together
    against fixed exercise difficulties. It is therefore a batch placement
    estimate, not a chronological replay.

    """
    if not fact_path.exists():
        raise FileNotFoundError(f"Fact table not found: {fact_path}")
    if not exercise_elo_path.exists():
        raise FileNotFoundError(f"Exercise Elo table not found: {exercise_elo_path}")

    fact = pl.scan_parquet(fact_path)
    practice = (
        fact.filter(pl.col("work_mode").is_in(["zpdes", "playlist"]))
        .group_by(["user_id", "module_id"])
        .agg(
            pl.col("created_at").min().alias("first_practice_at"),
            pl.len().alias("practice_attempts"),
        )
    )

    adaptive = (
        fact.filter(pl.col("work_mode") == "adaptive-test")
        .select(
            [
                "user_id",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "activity_id",
                "exercise_id",
                "created_at",
                "data_correct",
            ]
        )
        .filter(
            pl.col("user_id").is_not_null()
            & pl.col("module_id").is_not_null()
            & pl.col("module_code").is_not_null()
            & pl.col("exercise_id").is_not_null()
            & pl.col("data_correct").is_not_null()
        )
        .join(practice, on=["user_id", "module_id"], how="left")
        .with_columns(
            pl.col("first_practice_at").is_not_null().alias("has_same_module_practice"),
            (
                pl.col("first_practice_at").is_null()
                | (pl.col("created_at") <= pl.col("first_practice_at"))
            ).alias("adaptive_attempt_before_first_practice"),
        )
    )
    if only_before_practice:
        adaptive = adaptive.filter(pl.col("adaptive_attempt_before_first_practice"))

    exercise_elo = pl.scan_parquet(exercise_elo_path).select(
        [*ELO_CONTEXT_KEYS, "exercise_elo", "calibrated"]
    )
    joined = adaptive.join(exercise_elo, on=list(ELO_CONTEXT_KEYS), how="left").collect()
    if joined.height == 0:
        return _empty_adaptive_test_elo_frame()

    group_keys = ["user_id", "module_id", "module_code", "module_label"]
    summary = joined.group_by(group_keys).agg(
        pl.len().alias("adaptive_test_attempts"),
        pl.col("exercise_elo").is_not_null().sum().cast(pl.Int64).alias("adaptive_test_elo_attempts"),
        pl.col("activity_id").n_unique().cast(pl.Int64).alias("adaptive_test_activities"),
        pl.struct(list(ELO_CONTEXT_KEYS))
        .n_unique()
        .cast(pl.Int64)
        .alias("adaptive_test_exercise_contexts"),
        pl.col("created_at").min().alias("adaptive_test_start"),
        pl.col("created_at").max().alias("adaptive_test_end"),
        pl.col("data_correct").cast(pl.Float64).mean().alias("adaptive_test_success_rate"),
        pl.col("first_practice_at").first().alias("first_practice_at"),
        pl.col("practice_attempts").first().cast(pl.Int64).alias("practice_attempts"),
        pl.col("has_same_module_practice").first().alias("has_same_module_practice"),
        pl.col("adaptive_attempt_before_first_practice")
        .all()
        .alias("all_adaptive_test_attempts_before_first_practice"),
        (~pl.col("adaptive_attempt_before_first_practice"))
        .any()
        .alias("any_adaptive_test_attempt_after_first_practice"),
    )

    observations = (
        joined.filter(pl.col("exercise_elo").is_not_null())
        .group_by([*group_keys, "exercise_elo"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("data_correct").cast(pl.Float64).sum().alias("successes"),
        )
        .sort(group_keys)
    )
    ratings_frame = _fit_grouped_student_ratings(
        observations,
        group_keys,
        rating_column="adaptive_test_elo",
    )
    return (
        summary.join(ratings_frame, on=group_keys, how="left")
        .with_columns(
            pl.col("adaptive_test_elo").is_not_null().alias("has_adaptive_test_elo"),
            pl.col("practice_attempts").fill_null(0).cast(pl.Int64),
        )
        .select(ADAPTIVE_TEST_ELO_COLUMNS)
        .sort(["module_code", "user_id"])
    )

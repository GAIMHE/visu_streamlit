"""Elo calibration and replay builders for derived artifacts."""

from __future__ import annotations

import math
from statistics import median

import polars as pl

from .config import Settings
from .derive_catalog import (
    exercise_catalog_elo_base_frame,
    exercise_catalog_elo_context_frame,
    exercise_metadata_frame,
)
from .derive_common import (
    ELO_BASE_RATING,
    ELO_K,
    ELO_SCALE,
    as_lazy,
    elo_expected_success,
    outcome_value,
)

ITERATIVE_SMOOTHING_PSEUDOCOUNT = 20.0
ITERATIVE_PRIOR_SD = 200.0
ITERATIVE_OPTIMIZER_MIN = 600.0
ITERATIVE_OPTIMIZER_MAX = 2400.0
ITERATIVE_OPTIMIZER_STEPS = 40
ITERATIVE_MAX_OUTER_ITERATIONS = 10
ITERATIVE_CONVERGENCE_MEDIAN = 1.0
ITERATIVE_CONVERGENCE_MAX = 5.0
CURRENT_BATCH_PRIOR_SD = 250.0
CURRENT_BATCH_NEWTON_STEPS = 16
CURRENT_BATCH_MAX_OUTER_ITERATIONS = 10
CURRENT_BATCH_CONVERGENCE_MEDIAN = 0.5
CURRENT_BATCH_CONVERGENCE_MAX = 3.0
_ELO_LOGISTIC_K = math.log(10.0) / ELO_SCALE
_CURRENT_ELO_CONTEXT_COLUMNS = ["module_code", "objective_id", "activity_id", "exercise_id"]
_CURRENT_ELO_KEY_SEPARATOR = "\u241f"

_EXERCISE_ELO_CORE_COLUMNS = [
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


def _empty_exercise_elo_df(
    extra_schema: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Return an empty exercise Elo frame with stable dtypes."""
    data: dict[str, list[object]] = {
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
    }
    schema: dict[str, pl.DataType] = {
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
    }
    for column, dtype in (extra_schema or {}).items():
        data[column] = []
        schema[column] = dtype
    return pl.DataFrame(data, schema=schema)


def _empty_student_elo_events_df() -> pl.DataFrame:
    """Return an empty student Elo event frame with stable dtypes."""
    return pl.DataFrame(
        {
            "user_id": [],
            "attempt_ordinal": [],
            "created_at": [],
            "date_utc": [],
            "work_mode": [],
            "module_id": [],
            "module_code": [],
            "module_label": [],
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
            "module_id": pl.Utf8,
            "module_code": pl.Utf8,
            "module_label": pl.Utf8,
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


def _is_blank_expr(column: str) -> pl.Expr:
    return pl.col(column).is_null() | (pl.col(column).cast(pl.Utf8).str.strip_chars() == "")


def _clean_text_expr(column: str) -> pl.Expr:
    return pl.when(_is_blank_expr(column)).then(None).otherwise(pl.col(column).cast(pl.Utf8))


def _unmapped_context_label_expr(kind: str) -> pl.Expr:
    module_ref = pl.coalesce(
        [
            _clean_text_expr("module_code"),
            _clean_text_expr("module_label"),
            _clean_text_expr("module_id"),
            pl.lit("unknown module"),
        ]
    )
    initial_test_label = pl.concat_str(
        [
            pl.lit(f"Unmapped initial-test {kind} ("),
            module_ref,
            pl.lit(")"),
        ]
    )
    generic_label = pl.concat_str(
        [
            pl.lit(f"Unmapped {kind} ("),
            module_ref,
            pl.lit(")"),
        ]
    )
    return (
        pl.when(pl.col("dominant_work_mode").cast(pl.Utf8) == pl.lit("initial-test"))
        .then(initial_test_label)
        .otherwise(generic_label)
    )


def _empty_orphan_exercise_base_df() -> pl.DataFrame:
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
        },
    )


def _normalize_context_token(value: object | None) -> str:
    text = "" if value is None else str(value).strip()
    return text if text else "__NULL__"


def _build_current_context_key(
    module_code: object | None,
    objective_id: object | None,
    activity_id: object | None,
    exercise_id: object | None,
) -> str:
    return _CURRENT_ELO_KEY_SEPARATOR.join(
        [
            _normalize_context_token(module_code),
            _normalize_context_token(objective_id),
            _normalize_context_token(activity_id),
            _normalize_context_token(exercise_id),
        ]
    )


def _current_context_key_expr() -> pl.Expr:
    return pl.concat_str(
        [
            pl.coalesce([_clean_text_expr("module_code"), pl.lit("__NULL__")]),
            pl.coalesce([_clean_text_expr("objective_id"), pl.lit("__NULL__")]),
            pl.coalesce([_clean_text_expr("activity_id"), pl.lit("__NULL__")]),
            pl.coalesce([_clean_text_expr("exercise_id"), pl.lit("__NULL__")]),
        ],
        separator=_CURRENT_ELO_KEY_SEPARATOR,
    ).alias("context_key")


def _empty_current_exercise_base_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "context_key": [],
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
        },
        schema={
            "context_key": pl.Utf8,
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
        },
    )


def _orphan_exercise_elo_base_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
    known_exercise_ids: list[str],
) -> pl.DataFrame:
    orphan_context = (
        as_lazy(fact)
        .filter(
            pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & ~pl.col("exercise_id").cast(pl.Utf8).is_in(known_exercise_ids)
        )
        .select(
            [
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("module_id").cast(pl.Utf8),
                pl.col("module_code").cast(pl.Utf8),
                pl.col("module_label").cast(pl.Utf8),
                pl.col("objective_id").cast(pl.Utf8),
                pl.col("objective_label").cast(pl.Utf8),
                pl.col("activity_id").cast(pl.Utf8),
                pl.col("activity_label").cast(pl.Utf8),
                pl.col("work_mode").cast(pl.Utf8),
            ]
        )
        .group_by(
            [
                "exercise_id",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "work_mode",
            ]
        )
        .agg(pl.len().alias("attempts"))
        .sort(
            [
                "exercise_id",
                "attempts",
                "module_code",
                "objective_id",
                "activity_id",
                "work_mode",
            ],
            descending=[False, True, False, False, False, False],
        )
        .unique(subset=["exercise_id"], keep="first")
        .rename({"work_mode": "dominant_work_mode"})
        .collect()
    )
    if orphan_context.height == 0:
        return _empty_orphan_exercise_base_df()

    exercise_meta = exercise_metadata_frame(settings).rename({"exercise_label_meta": "exercise_label"})
    return (
        orphan_context.join(exercise_meta, on="exercise_id", how="left")
        .with_columns(
            pl.coalesce([_clean_text_expr("exercise_label"), pl.col("exercise_id").cast(pl.Utf8)]).alias(
                "exercise_label"
            ),
            pl.coalesce([_clean_text_expr("exercise_type"), pl.lit("unknown")]).alias("exercise_type"),
            pl.coalesce(
                [
                    _clean_text_expr("module_label"),
                    _clean_text_expr("module_code"),
                    _clean_text_expr("module_id"),
                    pl.lit("Unknown module"),
                ]
            ).alias("module_label"),
            pl.when(_clean_text_expr("objective_label").is_null())
            .then(_unmapped_context_label_expr("objective"))
            .otherwise(_clean_text_expr("objective_label"))
            .alias("objective_label"),
            pl.when(_clean_text_expr("activity_label").is_null())
            .then(_unmapped_context_label_expr("activity"))
            .otherwise(_clean_text_expr("activity_label"))
            .alias("activity_label"),
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
            ]
        )
        .unique(subset=["exercise_id"], keep="first")
    )


def _build_exercise_elo_base(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    catalog_base = exercise_catalog_elo_base_frame(settings)
    orphan_base = _orphan_exercise_elo_base_from_fact(
        fact,
        settings=settings,
        known_exercise_ids=catalog_base["exercise_id"].to_list(),
    )
    return pl.concat([catalog_base, orphan_base], how="diagonal_relaxed").unique(
        subset=["exercise_id"],
        keep="first",
    )


def _observed_current_exercise_context_base_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
    known_context_keys: list[str],
) -> pl.DataFrame:
    observed_context = (
        as_lazy(fact)
        .filter(
            pl.col("module_code").is_not_null()
            & (pl.col("module_code").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
        )
        .with_columns(
            _clean_text_expr("exercise_id").alias("exercise_id"),
            _clean_text_expr("module_id").alias("module_id"),
            _clean_text_expr("module_code").alias("module_code"),
            _clean_text_expr("module_label").alias("module_label"),
            _clean_text_expr("objective_id").alias("objective_id"),
            _clean_text_expr("objective_label").alias("objective_label"),
            _clean_text_expr("activity_id").alias("activity_id"),
            _clean_text_expr("activity_label").alias("activity_label"),
            _clean_text_expr("work_mode").alias("work_mode"),
            _current_context_key_expr(),
        )
        .filter(~pl.col("context_key").is_in(known_context_keys))
        .group_by(
            [
                "context_key",
                "exercise_id",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "work_mode",
            ]
        )
        .agg(pl.len().alias("attempts"))
        .sort(
            [
                "context_key",
                "attempts",
                "work_mode",
                "module_code",
                "objective_id",
                "activity_id",
            ],
            descending=[False, True, False, False, False, False],
        )
        .unique(subset=["context_key"], keep="first")
        .rename({"work_mode": "dominant_work_mode"})
        .collect()
    )
    if observed_context.height == 0:
        return _empty_current_exercise_base_df()

    exercise_meta = exercise_metadata_frame(settings).rename({"exercise_label_meta": "exercise_label"})
    return (
        observed_context.join(exercise_meta, on="exercise_id", how="left")
        .with_columns(
            pl.coalesce([_clean_text_expr("exercise_label"), pl.col("exercise_id").cast(pl.Utf8)]).alias(
                "exercise_label"
            ),
            pl.coalesce([_clean_text_expr("exercise_type"), pl.lit("unknown")]).alias("exercise_type"),
            pl.coalesce([_clean_text_expr("module_label"), _clean_text_expr("module_code")]).alias(
                "module_label"
            ),
            pl.when(
                _clean_text_expr("objective_id").is_null()
                | _clean_text_expr("activity_id").is_null()
            )
            .then(_clean_text_expr("objective_label"))
            .otherwise(
                pl.coalesce(
                    [
                        _clean_text_expr("objective_label"),
                        _unmapped_context_label_expr("objective"),
                    ]
                )
            )
            .alias("objective_label"),
            pl.when(
                _clean_text_expr("objective_id").is_null()
                | _clean_text_expr("activity_id").is_null()
            )
            .then(_clean_text_expr("activity_label"))
            .otherwise(
                pl.coalesce(
                    [
                        _clean_text_expr("activity_label"),
                        _unmapped_context_label_expr("activity"),
                    ]
                )
            )
            .alias("activity_label"),
        )
        .select(_empty_current_exercise_base_df().columns)
    )


def _build_current_exercise_elo_base(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    catalog_base = (
        exercise_catalog_elo_context_frame(settings)
        .with_columns(_current_context_key_expr())
        .select(_empty_current_exercise_base_df().columns)
    )
    observed_base = _observed_current_exercise_context_base_from_fact(
        fact,
        settings=settings,
        known_context_keys=catalog_base["context_key"].to_list(),
    )
    return pl.concat([catalog_base, observed_base], how="diagonal_relaxed").unique(
        subset=["context_key"],
        keep="first",
    )


def _collect_first_attempt_calibration_rows(
    fact: pl.DataFrame | pl.LazyFrame,
    valid_exercise_ids: list[str],
) -> pl.DataFrame:
    return (
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


def _collect_replay_rows(
    fact: pl.DataFrame | pl.LazyFrame,
    valid_exercise_ids: list[str],
) -> pl.DataFrame:
    return (
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


def _collect_current_first_attempt_calibration_rows(
    fact: pl.DataFrame | pl.LazyFrame,
    valid_context_keys: list[str],
) -> pl.DataFrame:
    return (
        as_lazy(fact)
        .filter(
            (pl.col("attempt_number") == 1)
            & pl.col("created_at").is_not_null()
            & pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("module_code").is_not_null()
            & (pl.col("module_code").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("data_correct").is_not_null()
        )
        .with_columns(
            _clean_text_expr("user_id").alias("user_id"),
            _clean_text_expr("module_code").alias("module_code"),
            _clean_text_expr("objective_id").alias("objective_id"),
            _clean_text_expr("activity_id").alias("activity_id"),
            _clean_text_expr("exercise_id").alias("exercise_id"),
            _current_context_key_expr(),
        )
        .filter(pl.col("context_key").is_in(valid_context_keys))
        .select(
            [
                "user_id",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "context_key",
                "data_correct",
            ]
        )
        .sort(["module_code", "user_id", "exercise_id"])
        .collect()
    )


def _collect_current_replay_rows(
    fact: pl.DataFrame | pl.LazyFrame,
    valid_context_keys: list[str],
) -> pl.DataFrame:
    return (
        as_lazy(fact)
        .filter(
            pl.col("created_at").is_not_null()
            & pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("module_code").is_not_null()
            & (pl.col("module_code").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("data_correct").is_not_null()
        )
        .with_columns(
            _clean_text_expr("user_id").alias("user_id"),
            _clean_text_expr("module_id").alias("module_id"),
            _clean_text_expr("module_code").alias("module_code"),
            _clean_text_expr("module_label").alias("module_label"),
            _clean_text_expr("objective_id").alias("objective_id"),
            _clean_text_expr("activity_id").alias("activity_id"),
            _clean_text_expr("exercise_id").alias("exercise_id"),
            _current_context_key_expr(),
        )
        .filter(pl.col("context_key").is_in(valid_context_keys))
        .select(
            [
                "user_id",
                "created_at",
                "date_utc",
                "work_mode",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "activity_id",
                "exercise_id",
                "context_key",
                "attempt_number",
                "data_correct",
            ]
        )
        .sort(["user_id", "module_code", "created_at", "exercise_id", "attempt_number"])
        .collect()
    )


def _calibration_stats_from_rows(calibration: pl.DataFrame) -> pl.DataFrame:
    if calibration.height == 0:
        return pl.DataFrame(
            {
                "exercise_id": [],
                "calibration_attempts": [],
                "successes": [],
                "calibration_success_rate": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "calibration_attempts": pl.Int64,
                "successes": pl.Float64,
                "calibration_success_rate": pl.Float64,
            },
        )
    return (
        calibration.with_columns(
            pl.when(pl.col("data_correct").cast(pl.Float64, strict=False) > 0)
            .then(1.0)
            .otherwise(0.0)
            .cast(pl.Float64)
            .alias("outcome")
        )
        .group_by("exercise_id")
        .agg(
            pl.len().cast(pl.Int64).alias("calibration_attempts"),
            pl.col("outcome").sum().alias("successes"),
            pl.col("outcome").mean().alias("calibration_success_rate"),
        )
        .sort("exercise_id")
    )


def _finalize_exercise_elo_frame(
    base: pl.DataFrame,
    stats: pl.DataFrame,
    extra_columns: list[str] | None = None,
) -> pl.DataFrame:
    selected_columns = list(_EXERCISE_ELO_CORE_COLUMNS)
    for column in extra_columns or []:
        if column not in selected_columns:
            selected_columns.append(column)
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
        .select(selected_columns)
        .sort(["module_code", "objective_id", "activity_id", "exercise_id"])
    )


def _clip_probability(probability: float) -> float:
    return min(0.99, max(0.01, float(probability)))


def _rating_from_success_probability(probability: float) -> float:
    clipped = _clip_probability(probability)
    odds = clipped / (1.0 - clipped)
    return float(ELO_BASE_RATING - ELO_SCALE * math.log10(odds))


def _golden_section_minimize(
    objective_fn,
    lower: float,
    upper: float,
    steps: int = ITERATIVE_OPTIMIZER_STEPS,
) -> float:
    phi = (1.0 + math.sqrt(5.0)) / 2.0
    inv_phi = 1.0 / phi
    left = float(lower)
    right = float(upper)
    c = right - (right - left) * inv_phi
    d = left + (right - left) * inv_phi
    fc = float(objective_fn(c))
    fd = float(objective_fn(d))
    for _ in range(max(1, int(steps))):
        if fc <= fd:
            right = d
            d = c
            fd = fc
            c = right - (right - left) * inv_phi
            fc = float(objective_fn(c))
        else:
            left = c
            c = d
            fc = fd
            d = left + (right - left) * inv_phi
            fd = float(objective_fn(d))
    return float((left + right) / 2.0)


def _penalized_item_nll(
    exercise_rating: float,
    observations: list[tuple[float, float]],
    *,
    prior_mean: float = ELO_BASE_RATING,
    prior_sd: float = ITERATIVE_PRIOR_SD,
) -> float:
    loss = 0.0
    for theta_pre, outcome in observations:
        probability = min(
            1.0 - 1e-9,
            max(1e-9, elo_expected_success(float(theta_pre), float(exercise_rating))),
        )
        if outcome >= 1.0:
            loss -= math.log(probability)
        else:
            loss -= math.log(1.0 - probability)
    if prior_sd > 0:
        loss += 0.5 * ((float(exercise_rating) - float(prior_mean)) / float(prior_sd)) ** 2
    return float(loss)


def _fit_iterative_exercise_rating(
    observations: list[tuple[float, float]],
    *,
    prior_mean: float = ELO_BASE_RATING,
    prior_sd: float = ITERATIVE_PRIOR_SD,
) -> float:
    if not observations:
        return float(prior_mean)
    return _golden_section_minimize(
        lambda rating: _penalized_item_nll(
            rating,
            observations,
            prior_mean=prior_mean,
            prior_sd=prior_sd,
        ),
        ITERATIVE_OPTIMIZER_MIN,
        ITERATIVE_OPTIMIZER_MAX,
        ITERATIVE_OPTIMIZER_STEPS,
    )


def _fit_batch_student_rating(
    observations: list[tuple[float, float]],
    *,
    initial_rating: float = ELO_BASE_RATING,
    prior_mean: float = ELO_BASE_RATING,
    prior_sd: float = CURRENT_BATCH_PRIOR_SD,
    steps: int = CURRENT_BATCH_NEWTON_STEPS,
) -> float:
    if not observations:
        return float(prior_mean)
    rating = float(initial_rating)
    prior_var_inv = 0.0 if prior_sd <= 0 else 1.0 / float(prior_sd) ** 2
    for _ in range(max(1, int(steps))):
        grad = (rating - float(prior_mean)) * prior_var_inv
        hess = prior_var_inv
        for exercise_rating, outcome in observations:
            probability = min(
                1.0 - 1e-9,
                max(1e-9, elo_expected_success(float(rating), float(exercise_rating))),
            )
            grad += _ELO_LOGISTIC_K * (probability - float(outcome))
            hess += (_ELO_LOGISTIC_K**2) * probability * (1.0 - probability)
        if hess <= 0.0:
            break
        step_size = grad / hess
        rating = min(ITERATIVE_OPTIMIZER_MAX, max(ITERATIVE_OPTIMIZER_MIN, rating - step_size))
        if abs(step_size) < 0.01:
            break
    return float(rating)


def _fit_batch_item_rating(
    observations: list[tuple[float, float]],
    *,
    initial_rating: float = ELO_BASE_RATING,
    prior_mean: float = ELO_BASE_RATING,
    prior_sd: float = CURRENT_BATCH_PRIOR_SD,
    steps: int = CURRENT_BATCH_NEWTON_STEPS,
) -> float:
    if not observations:
        return float(prior_mean)
    rating = float(initial_rating)
    prior_var_inv = 0.0 if prior_sd <= 0 else 1.0 / float(prior_sd) ** 2
    for _ in range(max(1, int(steps))):
        grad = (rating - float(prior_mean)) * prior_var_inv
        hess = prior_var_inv
        for student_rating, outcome in observations:
            probability = min(
                1.0 - 1e-9,
                max(1e-9, elo_expected_success(float(student_rating), float(rating))),
            )
            grad += _ELO_LOGISTIC_K * (float(outcome) - probability)
            hess += (_ELO_LOGISTIC_K**2) * probability * (1.0 - probability)
        if hess <= 0.0:
            break
        step_size = grad / hess
        rating = min(ITERATIVE_OPTIMIZER_MAX, max(ITERATIVE_OPTIMIZER_MIN, rating - step_size))
        if abs(step_size) < 0.01:
            break
    return float(rating)


def _recenter_module_pair(
    student_ratings: dict[str, float],
    item_ratings: dict[str, float],
    *,
    target_mean: float = ELO_BASE_RATING,
) -> tuple[dict[str, float], dict[str, float]]:
    if not item_ratings:
        return student_ratings, item_ratings
    offset = float(target_mean) - (sum(item_ratings.values()) / float(len(item_ratings)))
    adjusted_students = {
        student_key: float(rating + offset) for student_key, rating in student_ratings.items()
    }
    adjusted_items = {item_key: float(rating + offset) for item_key, rating in item_ratings.items()}
    return adjusted_students, adjusted_items


def _recenter_ratings(
    ratings: dict[str, float],
    target_mean: float = ELO_BASE_RATING,
) -> dict[str, float]:
    if not ratings:
        return {}
    offset = float(target_mean) - (sum(ratings.values()) / float(len(ratings)))
    return {exercise_id: float(rating + offset) for exercise_id, rating in ratings.items()}


def _replay_student_elo(
    replay: pl.DataFrame,
    exercise_elo_map: dict[str, float],
    *,
    collect_events: bool,
    collect_first_attempt_observations: bool,
) -> tuple[pl.DataFrame | None, dict[str, list[tuple[float, float]]]]:
    rows: list[dict[str, object]] | None = [] if collect_events else None
    first_attempt_observations: dict[str, list[tuple[float, float]]] = {}
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
        attempt_number,
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

        if collect_first_attempt_observations and int(attempt_number or 0) == 1:
            first_attempt_observations.setdefault(exercise_key, []).append(
                (float(student_elo_pre), float(outcome))
            )

        if rows is not None:
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

    if rows is None:
        return None, first_attempt_observations
    if not rows:
        return _empty_student_elo_events_df(), first_attempt_observations
    return pl.DataFrame(rows).sort(["user_id", "attempt_ordinal"]), first_attempt_observations


def _finalize_current_exercise_elo_frame(
    base: pl.DataFrame,
    stats: pl.DataFrame,
) -> pl.DataFrame:
    return (
        base.join(stats, on="context_key", how="left")
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
        .drop("context_key")
        .select(_EXERCISE_ELO_CORE_COLUMNS)
        .sort(["module_code", "objective_id", "activity_id", "exercise_id"])
    )


def _fit_current_module_local_exercise_elo(calibration: pl.DataFrame) -> pl.DataFrame:
    if calibration.height == 0:
        return _empty_exercise_elo_df().with_columns(pl.lit(None, dtype=pl.Utf8).alias("context_key")).select(
            [
                "context_key",
                "exercise_elo",
                "calibration_attempts",
                "calibration_success_rate",
                "calibrated",
            ]
        )

    rows = calibration.to_dicts()
    module_groups: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        module_code = str(row.get("module_code") or "").strip()
        context_key = str(row.get("context_key") or "").strip()
        user_id = str(row.get("user_id") or "").strip()
        outcome = outcome_value(row.get("data_correct"))
        if not module_code or not context_key or not user_id or outcome is None:
            continue
        module_groups.setdefault(module_code, []).append(
            {
                "user_id": user_id,
                "context_key": context_key,
                "outcome": float(outcome),
            }
        )

    stats_rows: list[dict[str, object]] = []
    for module_code, module_rows in module_groups.items():
        user_observations: dict[str, list[tuple[str, float]]] = {}
        item_observations: dict[str, list[tuple[str, float]]] = {}
        item_attempts: dict[str, int] = {}
        item_successes: dict[str, float] = {}
        for row in module_rows:
            user_key = str(row["user_id"])
            context_key = str(row["context_key"])
            outcome = float(row["outcome"])
            user_observations.setdefault(user_key, []).append((context_key, outcome))
            item_observations.setdefault(context_key, []).append((user_key, outcome))
            item_attempts[context_key] = item_attempts.get(context_key, 0) + 1
            item_successes[context_key] = item_successes.get(context_key, 0.0) + outcome

        total_attempts = sum(item_attempts.values())
        total_successes = sum(item_successes.values())
        global_success_rate = total_successes / float(total_attempts) if total_attempts > 0 else 0.5

        item_ratings: dict[str, float] = {}
        for context_key, attempts in item_attempts.items():
            successes = item_successes.get(context_key, 0.0)
            smoothed_rate = (successes + 20.0 * global_success_rate) / (float(attempts) + 20.0)
            item_ratings[context_key] = _rating_from_success_probability(smoothed_rate)
        student_ratings = {user_key: float(ELO_BASE_RATING) for user_key in user_observations}
        student_ratings, item_ratings = _recenter_module_pair(student_ratings, item_ratings)

        for _ in range(CURRENT_BATCH_MAX_OUTER_ITERATIONS):
            next_student_ratings: dict[str, float] = {}
            for user_key, observations in user_observations.items():
                next_student_ratings[user_key] = _fit_batch_student_rating(
                    [
                        (float(item_ratings.get(context_key, ELO_BASE_RATING)), float(outcome))
                        for context_key, outcome in observations
                    ],
                    initial_rating=float(student_ratings.get(user_key, ELO_BASE_RATING)),
                )

            next_item_ratings: dict[str, float] = {}
            item_deltas: list[float] = []
            for context_key, observations in item_observations.items():
                fitted_rating = _fit_batch_item_rating(
                    [
                        (float(next_student_ratings.get(user_key, ELO_BASE_RATING)), float(outcome))
                        for user_key, outcome in observations
                    ],
                    initial_rating=float(item_ratings.get(context_key, ELO_BASE_RATING)),
                )
                next_item_ratings[context_key] = fitted_rating
                item_deltas.append(abs(fitted_rating - float(item_ratings.get(context_key, ELO_BASE_RATING))))

            next_student_ratings, next_item_ratings = _recenter_module_pair(
                next_student_ratings,
                next_item_ratings,
            )
            student_ratings = next_student_ratings
            item_ratings = next_item_ratings
            if item_deltas and median(item_deltas) < CURRENT_BATCH_CONVERGENCE_MEDIAN and max(
                item_deltas
            ) < CURRENT_BATCH_CONVERGENCE_MAX:
                break

        for context_key, attempts in item_attempts.items():
            stats_rows.append(
                {
                    "context_key": context_key,
                    "exercise_elo": float(item_ratings.get(context_key, ELO_BASE_RATING)),
                    "calibration_attempts": int(attempts),
                    "calibration_success_rate": float(item_successes.get(context_key, 0.0))
                    / float(attempts),
                    "calibrated": True,
                }
            )

    if not stats_rows:
        return _empty_exercise_elo_df().with_columns(pl.lit(None, dtype=pl.Utf8).alias("context_key")).select(
            [
                "context_key",
                "exercise_elo",
                "calibration_attempts",
                "calibration_success_rate",
                "calibrated",
            ]
        )
    return pl.DataFrame(stats_rows)


def _replay_module_local_student_elo(
    replay: pl.DataFrame,
    exercise_elo_map: dict[str, float],
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    current_student_module: tuple[str, str] | None = None
    current_rating = ELO_BASE_RATING
    current_ordinal = 0

    for (
        user_id,
        created_at,
        date_utc,
        work_mode,
        module_id,
        module_code,
        module_label,
        objective_id,
        activity_id,
        exercise_id,
        context_key,
        _attempt_number,
        raw_outcome,
    ) in replay.iter_rows():
        user_key = str(user_id or "").strip()
        module_key = str(module_code or "").strip()
        context_ref = str(context_key or "").strip()
        exercise_key = str(exercise_id or "").strip()
        if not user_key or not module_key or not context_ref or not exercise_key:
            continue
        outcome = outcome_value(raw_outcome)
        if outcome is None:
            continue
        student_module_key = (user_key, module_key)
        if current_student_module != student_module_key:
            current_student_module = student_module_key
            current_rating = ELO_BASE_RATING
            current_ordinal = 0
        exercise_rating = exercise_elo_map.get(context_ref)
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
                "module_id": None if module_id is None else str(module_id),
                "module_code": module_key,
                "module_label": None if module_label is None else str(module_label),
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
    return pl.DataFrame(rows).sort(["user_id", "module_code", "attempt_ordinal"])


def build_agg_exercise_elo_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Calibrate fixed Current-Elo exercise difficulty per module-local raw context."""
    base = _build_current_exercise_elo_base(fact, settings)
    if base.height == 0:
        return _empty_exercise_elo_df()

    calibration = _collect_current_first_attempt_calibration_rows(
        fact,
        base["context_key"].to_list(),
    )
    stats = _fit_current_module_local_exercise_elo(calibration)
    return _finalize_current_exercise_elo_frame(base, stats)


def build_agg_exercise_elo_iterative_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Calibrate fixed exercise difficulty with iterative offline alternating replay."""
    base = _build_exercise_elo_base(fact, settings)
    extra_schema = {
        "smoothed_calibration_success_rate": pl.Float64,
    }
    if base.height == 0:
        return _empty_exercise_elo_df(extra_schema=extra_schema)

    calibration = _collect_first_attempt_calibration_rows(fact, base["exercise_id"].to_list())
    calibration_stats = _calibration_stats_from_rows(calibration)
    empty_stats = _empty_exercise_elo_df(extra_schema=extra_schema).select(
        [
            "exercise_id",
            "exercise_elo",
            "calibration_attempts",
            "calibration_success_rate",
            "calibrated",
            "smoothed_calibration_success_rate",
        ]
    )
    if calibration_stats.height == 0:
        return _finalize_exercise_elo_frame(
            base,
            empty_stats,
            extra_columns=["smoothed_calibration_success_rate"],
        )

    valid_exercise_ids = calibration_stats["exercise_id"].to_list()
    replay = _collect_replay_rows(fact, valid_exercise_ids)
    if replay.height == 0:
        return _finalize_exercise_elo_frame(
            base,
            empty_stats,
            extra_columns=["smoothed_calibration_success_rate"],
        )

    total_attempts = int(calibration_stats["calibration_attempts"].sum())
    total_successes = float(calibration_stats["successes"].sum())
    global_success_rate = total_successes / float(total_attempts) if total_attempts > 0 else 0.5

    initial_rows: list[dict[str, object]] = []
    current_ratings: dict[str, float] = {}
    for row in calibration_stats.to_dicts():
        attempts = int(row["calibration_attempts"])
        successes = float(row["successes"])
        smoothed_rate = (successes + ITERATIVE_SMOOTHING_PSEUDOCOUNT * global_success_rate) / (
            float(attempts) + ITERATIVE_SMOOTHING_PSEUDOCOUNT
        )
        initial_rating = _rating_from_success_probability(smoothed_rate)
        exercise_id = str(row["exercise_id"])
        current_ratings[exercise_id] = initial_rating
        initial_rows.append(
            {
                "exercise_id": exercise_id,
                "calibration_attempts": attempts,
                "calibration_success_rate": float(row["calibration_success_rate"]),
                "smoothed_calibration_success_rate": float(smoothed_rate),
                "exercise_elo": float(initial_rating),
                "calibrated": True,
            }
        )

    current_ratings = _recenter_ratings(current_ratings)
    calibration_stats_df = pl.DataFrame(initial_rows).select(
        [
            "exercise_id",
            "calibration_attempts",
            "calibration_success_rate",
            "smoothed_calibration_success_rate",
            "calibrated",
        ]
    )

    for _ in range(ITERATIVE_MAX_OUTER_ITERATIONS):
        _, first_attempt_observations = _replay_student_elo(
            replay,
            current_ratings,
            collect_events=False,
            collect_first_attempt_observations=True,
        )
        next_ratings: dict[str, float] = {}
        rating_deltas: list[float] = []
        for exercise_id in valid_exercise_ids:
            fitted_rating = _fit_iterative_exercise_rating(
                first_attempt_observations.get(str(exercise_id), []),
                prior_mean=ELO_BASE_RATING,
                prior_sd=ITERATIVE_PRIOR_SD,
            )
            next_ratings[str(exercise_id)] = fitted_rating
        next_ratings = _recenter_ratings(next_ratings)
        for exercise_id, new_rating in next_ratings.items():
            rating_deltas.append(abs(new_rating - current_ratings.get(exercise_id, ELO_BASE_RATING)))
        current_ratings = next_ratings
        if rating_deltas and median(rating_deltas) < ITERATIVE_CONVERGENCE_MEDIAN and max(
            rating_deltas
        ) < ITERATIVE_CONVERGENCE_MAX:
            break

    stats = calibration_stats_df.join(
        pl.DataFrame(
            {
                "exercise_id": list(current_ratings.keys()),
                "exercise_elo": list(current_ratings.values()),
            }
        ),
        on="exercise_id",
        how="left",
    )
    return _finalize_exercise_elo_frame(
        base,
        stats,
        extra_columns=["smoothed_calibration_success_rate"],
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
    """Replay Current-Elo trajectories inside each student-module slice."""
    exercise_map_df = (
        as_lazy(exercise_elo)
        .filter(pl.col("calibrated") & pl.col("exercise_elo").is_not_null())
        .with_columns(
            _clean_text_expr("module_code").alias("module_code"),
            _clean_text_expr("objective_id").alias("objective_id"),
            _clean_text_expr("activity_id").alias("activity_id"),
            _clean_text_expr("exercise_id").alias("exercise_id"),
            _current_context_key_expr(),
        )
        .select(["context_key", "exercise_elo"])
        .collect()
        .unique(subset=["context_key"], keep="first")
    )
    if exercise_map_df.height == 0:
        return _empty_student_elo_events_df()

    exercise_elo_map = {
        str(row["context_key"]): float(row["exercise_elo"])
        for row in exercise_map_df.to_dicts()
    }
    replay = _collect_current_replay_rows(fact, list(exercise_elo_map))
    return _replay_module_local_student_elo(replay, exercise_elo_map)


def build_student_elo_profiles_from_events(
    events: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Aggregate compact per-student-module replay summaries from Current-Elo events."""
    return (
        as_lazy(events)
        .sort(["user_id", "module_code", "attempt_ordinal"])
        .group_by(["user_id", "module_code"])
        .agg(
            pl.col("module_id").drop_nulls().first().alias("module_id"),
            pl.col("module_label").drop_nulls().first().alias("module_label"),
            pl.len().cast(pl.Int64).alias("total_attempts"),
            pl.col("created_at").min().alias("first_attempt_at"),
            pl.col("created_at").max().alias("last_attempt_at"),
            pl.lit(1, dtype=pl.Int64).alias("unique_modules"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_objectives"),
            pl.col("activity_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_activities"),
            pl.col("student_elo_post").last().alias("final_student_elo"),
        )
        .with_columns((pl.col("total_attempts") > 0).alias("eligible_for_replay"))
        .sort(
            ["user_id", "total_attempts", "module_code"],
            descending=[False, True, False],
        )
        .collect()
    )


def build_student_elo_events_iterative_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    exercise_elo: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Replay iterative Elo trajectories against globally frozen exercise difficulty."""
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
    replay = _collect_replay_rows(fact, valid_exercise_ids)
    events, _ = _replay_student_elo(
        replay,
        exercise_elo_map,
        collect_events=True,
        collect_first_attempt_observations=False,
    )
    return _empty_student_elo_events_df() if events is None else events


def build_student_elo_profiles_iterative_from_events(
    events: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Aggregate compact per-student replay summaries for iterative Elo."""
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

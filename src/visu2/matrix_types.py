"""Shared schemas, constants, and low-level helpers for matrix builders."""

from __future__ import annotations

import math

import polars as pl

VALID_MATRIX_METRICS = (
    "attempts",
    "success_rate",
    "exercise_balanced_success_rate",
    "activity_mean_exercise_elo",
    "repeat_attempt_rate",
    "first_attempt_success_rate",
    "playlist_unique_exercises",
)

CELLS_SCHEMA: dict[str, pl.DataType] = {
    "module_code": pl.Utf8,
    "objective_id": pl.Utf8,
    "objective_label": pl.Utf8,
    "objective_row_label": pl.Utf8,
    "activity_id": pl.Utf8,
    "activity_label": pl.Utf8,
    "activity_col_idx": pl.Int64,
    "activity_col_label": pl.Utf8,
    "metric_value": pl.Float64,
    "metric_text": pl.Utf8,
    "exercise_elo": pl.Float64,
    "calibration_attempts": pl.Int64,
    "calibration_success_rate": pl.Float64,
}

DRILLDOWN_SCHEMA: dict[str, pl.DataType] = {
    "exercise_id": pl.Utf8,
    "exercise_short_id": pl.Utf8,
    "exercise_label": pl.Utf8,
    "exercise_display_label": pl.Utf8,
    "exercise_type": pl.Utf8,
    "attempts": pl.Float64,
    "success_rate": pl.Float64,
    "first_attempt_success_rate": pl.Float64,
    "first_attempt_count": pl.Float64,
    "median_duration": pl.Float64,
    "repeat_attempt_rate": pl.Float64,
    "avg_attempt_number": pl.Float64,
    "metric_value": pl.Float64,
    "metric_text": pl.Utf8,
    "exercise_elo": pl.Float64,
    "calibration_attempts": pl.Int64,
    "calibration_success_rate": pl.Float64,
}


def empty_cells_df() -> pl.DataFrame:
    """Return an empty matrix cell frame with the stable output schema."""
    return pl.DataFrame(
        {name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in CELLS_SCHEMA.items()}
    )


def empty_drilldown_df() -> pl.DataFrame:
    """Return an empty drilldown frame with the stable output schema."""
    return pl.DataFrame(
        {
            name: pl.Series(name=name, values=[], dtype=dtype)
            for name, dtype in DRILLDOWN_SCHEMA.items()
        }
    )


def as_frame(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Normalize eager and lazy Polars inputs to a DataFrame."""
    return df.collect() if isinstance(df, pl.LazyFrame) else df


def collect_lazy(lf: pl.LazyFrame) -> pl.DataFrame:
    """Prefer streaming collection where supported to reduce peak memory."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect()


def columns_of(df: pl.DataFrame | pl.LazyFrame) -> list[str]:
    """Read columns from eager or lazy Polars frames without forcing call sites to branch."""
    if isinstance(df, pl.DataFrame):
        return list(df.columns)
    try:
        return list(df.collect_schema().names())
    except Exception:
        return list(df.schema.keys())


def assert_required_columns(frame: pl.DataFrame, required_columns: list[str]) -> None:
    """Raise a clear contract error when a matrix source is missing required columns."""
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Matrix source is missing required columns: {missing}")


def format_cell_value(metric: str, value: float | None) -> str:
    """Format a numeric metric for matrix cells and drilldown displays."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if metric in {"attempts", "playlist_unique_exercises", "activity_mean_exercise_elo"}:
        return f"{int(round(float(value)))}"
    return f"{float(value) * 100:.1f}%"

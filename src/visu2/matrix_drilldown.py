"""Ragged heatmap payload and exercise drilldown builders for the matrix page."""

from __future__ import annotations

from datetime import date

import polars as pl

from .matrix_types import (
    DRILLDOWN_SCHEMA,
    VALID_MATRIX_METRICS,
    as_frame,
    assert_required_columns,
    collect_lazy,
    columns_of,
    empty_drilldown_df,
)


def build_ragged_matrix_payload(cells_df: pl.DataFrame | pl.LazyFrame) -> dict[str, object]:
    """Convert the cell table into a ragged matrix payload for Plotly heatmaps."""
    frame = as_frame(cells_df)
    if frame.height == 0:
        return {
            "x_labels": [],
            "y_labels": [],
            "z_values": [],
            "text_values": [],
            "customdata": [],
            "objective_ids": [],
            "max_activity_cols": 0,
        }

    assert_required_columns(
        frame,
        [
            "objective_id",
            "objective_row_label",
            "objective_label",
            "activity_id",
            "activity_label",
            "activity_col_idx",
            "activity_col_label",
            "metric_value",
            "metric_text",
        ],
    )

    rows = frame.to_dicts()
    objective_ids: list[str] = []
    objective_labels: list[str] = []
    objective_position: dict[str, int] = {}
    max_activity_cols = 0

    for row in rows:
        objective_id = str(row["objective_id"])
        if objective_id not in objective_position:
            objective_position[objective_id] = len(objective_ids)
            objective_ids.append(objective_id)
            objective_labels.append(str(row["objective_row_label"]))
        max_activity_cols = max(max_activity_cols, int(row["activity_col_idx"]))

    x_labels = [f"A{idx}" for idx in range(1, max_activity_cols + 1)]
    z_values: list[list[float | None]] = [
        [None for _ in range(max_activity_cols)] for _ in range(len(objective_ids))
    ]
    text_values: list[list[str]] = [["" for _ in range(max_activity_cols)] for _ in range(len(objective_ids))]
    customdata: list[list[list[str]]] = [
        [["", "", "", "", "", ""] for _ in range(max_activity_cols)]
        for _ in range(len(objective_ids))
    ]

    for row in rows:
        row_idx = objective_position[str(row["objective_id"])]
        col_idx = int(row["activity_col_idx"]) - 1
        metric_value_raw = row.get("metric_value")
        metric_value = float(metric_value_raw) if metric_value_raw is not None else None

        z_values[row_idx][col_idx] = metric_value
        text_values[row_idx][col_idx] = str(row.get("metric_text") or "")
        customdata[row_idx][col_idx] = [
            str(row.get("objective_label") or row.get("objective_id") or ""),
            str(row.get("objective_id") or ""),
            str(row.get("activity_label") or row.get("activity_id") or ""),
            str(row.get("activity_id") or ""),
            str(row.get("activity_col_label") or ""),
            str(row.get("metric_text") or ""),
        ]

    return {
        "x_labels": x_labels,
        "y_labels": objective_labels,
        "z_values": z_values,
        "text_values": text_values,
        "customdata": customdata,
        "objective_ids": objective_ids,
        "max_activity_cols": max_activity_cols,
    }


def _formatted_metric_text_expr(metric_name: str) -> pl.Expr:
    """Return a Polars expression that formats `metric_value` for drilldown display."""
    metric_value = pl.col("metric_value")
    if metric_name in {"attempts", "playlist_unique_exercises", "activity_mean_exercise_elo"}:
        formatted = metric_value.round(0).cast(pl.Int64).cast(pl.Utf8)
    else:
        formatted = pl.concat_str([(metric_value * 100).round(1).cast(pl.Utf8), pl.lit("%")])
    return (
        pl.when(metric_value.is_null() | metric_value.is_nan())
        .then(pl.lit(""))
        .otherwise(formatted)
        .alias("metric_text")
    )


def build_exercise_drilldown_frame(
    agg_exercise_daily: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    objective_id: str,
    activity_id: str,
    start_date: date,
    end_date: date,
    metric: str,
    agg_exercise_elo: pl.DataFrame | pl.LazyFrame | None = None,
    fact_attempt_core: pl.DataFrame | pl.LazyFrame | None = None,
    work_mode: str | None = None,
) -> pl.DataFrame:
    """Build the exercise-level drilldown for the selected matrix cell."""
    if metric not in VALID_MATRIX_METRICS:
        raise ValueError(f"Unsupported metric '{metric}'. Expected one of {list(VALID_MATRIX_METRICS)}")

    if metric == "activity_mean_exercise_elo":
        if agg_exercise_elo is None:
            raise ValueError("Exercise drilldown for 'activity_mean_exercise_elo' requires agg_exercise_elo.")
        elo_frame = as_frame(agg_exercise_elo)
        assert_required_columns(
            elo_frame,
            [
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "exercise_elo",
                "calibration_attempts",
                "calibration_success_rate",
                "calibrated",
            ],
        )
        if "exercise_label" not in elo_frame.columns:
            elo_frame = elo_frame.with_columns(pl.col("exercise_id").cast(pl.Utf8).alias("exercise_label"))
        if "exercise_type" not in elo_frame.columns:
            elo_frame = elo_frame.with_columns(pl.lit("unknown", dtype=pl.Utf8).alias("exercise_type"))
        drilldown = (
            elo_frame.filter(
                (pl.col("module_code") == module_code)
                & (pl.col("objective_id") == objective_id)
                & (pl.col("activity_id") == activity_id)
                & pl.col("calibrated")
            )
            .with_columns(
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8).str.slice(0, 8).alias("exercise_short_id"),
                pl.col("exercise_id").cast(pl.Utf8).str.slice(0, 8).alias("exercise_display_label"),
                pl.col("exercise_elo").alias("metric_value"),
            )
            .with_columns(
                _formatted_metric_text_expr(metric),
                pl.lit(None, dtype=pl.Float64).alias("attempts"),
                pl.lit(None, dtype=pl.Float64).alias("success_rate"),
                pl.lit(None, dtype=pl.Float64).alias("first_attempt_success_rate"),
                pl.lit(None, dtype=pl.Float64).alias("first_attempt_count"),
                pl.lit(None, dtype=pl.Float64).alias("median_duration"),
                pl.lit(None, dtype=pl.Float64).alias("repeat_attempt_rate"),
                pl.lit(None, dtype=pl.Float64).alias("avg_attempt_number"),
            )
            .sort(["metric_value", "calibration_attempts"], descending=[True, True])
        )
        return drilldown.select(list(DRILLDOWN_SCHEMA.keys()))

    if work_mode is not None:
        if fact_attempt_core is None:
            raise ValueError(f"Exercise drilldown for '{metric}' with a cohort population requires fact_attempt_core.")
        fact_cols = columns_of(fact_attempt_core)
        missing_fact_cols = [
            col
            for col in [
                "date_utc",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "work_mode",
                "data_correct",
                "attempt_number",
                "data_duration",
            ]
            if col not in fact_cols
        ]
        if missing_fact_cols:
            raise ValueError(f"Drilldown source is missing required columns: {sorted(set(missing_fact_cols))}")
        if metric == "playlist_unique_exercises" and work_mode != "playlist":
            raise ValueError("Exercise drilldown for 'playlist_unique_exercises' is only available for playlist mode.")

        fact_lf = fact_attempt_core.lazy() if isinstance(fact_attempt_core, pl.DataFrame) else fact_attempt_core
        filtered = fact_lf.filter(
            (pl.col("module_code") == module_code)
            & (pl.col("objective_id") == objective_id)
            & (pl.col("activity_id") == activity_id)
            & (pl.col("date_utc") >= pl.lit(start_date))
            & (pl.col("date_utc") <= pl.lit(end_date))
            & (pl.col("work_mode") == work_mode)
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8) != "None")
        )
        grouped = collect_lazy(
            filtered.group_by(["exercise_id"])
            .agg(
                pl.len().cast(pl.Float64).alias("attempts"),
                pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
                (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
                pl.col("data_correct")
                .filter(pl.col("attempt_number") == 1)
                .cast(pl.Float64)
                .mean()
                .alias("first_attempt_success_rate"),
                (pl.col("attempt_number") == 1).cast(pl.Float64).sum().alias("first_attempt_count"),
                pl.col("data_duration").cast(pl.Float64).median().alias("median_duration"),
                pl.col("attempt_number").cast(pl.Float64).mean().alias("avg_attempt_number"),
            )
        )
        if grouped.height == 0:
            return empty_drilldown_df()

        metric_expr = {
            "attempts": pl.col("attempts"),
            "success_rate": pl.col("success_rate"),
            "exercise_balanced_success_rate": pl.col("success_rate"),
            "repeat_attempt_rate": pl.col("repeat_attempt_rate"),
            "first_attempt_success_rate": pl.col("first_attempt_success_rate"),
            "playlist_unique_exercises": pl.col("attempts"),
        }.get(metric)
        if metric_expr is None:
            raise ValueError(f"Unsupported metric '{metric}' for fact-backed drilldown.")

        metric_text_metric = "attempts" if metric == "playlist_unique_exercises" else metric
        drilldown = (
            grouped.with_columns(
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8).alias("exercise_label"),
                pl.lit("unknown", dtype=pl.Utf8).alias("exercise_type"),
                pl.col("exercise_id").cast(pl.Utf8).str.slice(0, 8).alias("exercise_short_id"),
            )
            .with_columns(pl.col("exercise_short_id").alias("exercise_display_label"))
            .with_columns(
                metric_expr.alias("metric_value"),
                pl.lit(None, dtype=pl.Float64).alias("exercise_elo"),
                pl.lit(None, dtype=pl.Int64).alias("calibration_attempts"),
                pl.lit(None, dtype=pl.Float64).alias("calibration_success_rate"),
            )
            .with_columns(_formatted_metric_text_expr(metric_text_metric))
            .sort(["metric_value", "attempts"], descending=[True, True])
        )
        return drilldown.select(list(DRILLDOWN_SCHEMA.keys()))

    if metric == "playlist_unique_exercises":
        if fact_attempt_core is None:
            raise ValueError("Exercise drilldown for 'playlist_unique_exercises' requires fact_attempt_core.")
        fact_cols = columns_of(fact_attempt_core)
        missing_fact_cols = [
            col
            for col in [
                "date_utc",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "work_mode",
                "data_correct",
                "attempt_number",
                "data_duration",
            ]
            if col not in fact_cols
        ]
        if missing_fact_cols:
            raise ValueError(f"Drilldown source is missing required columns: {missing_fact_cols}")
        fact_lf = fact_attempt_core.lazy() if isinstance(fact_attempt_core, pl.DataFrame) else fact_attempt_core
        playlist_filtered = fact_lf.filter(
            (pl.col("module_code") == module_code)
            & (pl.col("objective_id") == objective_id)
            & (pl.col("activity_id") == activity_id)
            & (pl.col("date_utc") >= pl.lit(start_date))
            & (pl.col("date_utc") <= pl.lit(end_date))
            & (pl.col("work_mode") == "playlist")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8) != "None")
        )
        playlist_grouped = collect_lazy(
            playlist_filtered.group_by(["exercise_id"])
            .agg(
                pl.len().cast(pl.Float64).alias("attempts"),
                pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
                (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
                pl.col("data_correct")
                .filter(pl.col("attempt_number") == 1)
                .cast(pl.Float64)
                .mean()
                .alias("first_attempt_success_rate"),
                (pl.col("attempt_number") == 1).cast(pl.Float64).sum().alias("first_attempt_count"),
                pl.col("data_duration").cast(pl.Float64).median().alias("median_duration"),
                pl.col("attempt_number").cast(pl.Float64).mean().alias("avg_attempt_number"),
            )
        )
        if playlist_grouped.height == 0:
            return empty_drilldown_df()
        drilldown = (
            playlist_grouped.with_columns(
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8).alias("exercise_label"),
                pl.lit("unknown", dtype=pl.Utf8).alias("exercise_type"),
                pl.col("exercise_id").cast(pl.Utf8).str.slice(0, 8).alias("exercise_short_id"),
            )
            .with_columns(pl.col("exercise_short_id").alias("exercise_display_label"))
            .with_columns(
                pl.col("attempts").alias("metric_value"),
                pl.lit(None, dtype=pl.Float64).alias("exercise_elo"),
                pl.lit(None, dtype=pl.Int64).alias("calibration_attempts"),
                pl.lit(None, dtype=pl.Float64).alias("calibration_success_rate"),
            )
            .with_columns(_formatted_metric_text_expr("attempts"))
            .sort(["metric_value", "attempts"], descending=[True, True])
        )
        return drilldown.select(list(DRILLDOWN_SCHEMA.keys()))

    frame = as_frame(agg_exercise_daily)
    assert_required_columns(
        frame,
        [
            "date_utc",
            "module_code",
            "objective_id",
            "activity_id",
            "exercise_id",
            "attempts",
            "success_rate",
            "repeat_attempt_rate",
            "median_duration",
            "avg_attempt_number",
        ],
    )
    if metric == "first_attempt_success_rate":
        assert_required_columns(frame, ["first_attempt_success_rate", "first_attempt_count"])
    else:
        if "first_attempt_success_rate" not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("first_attempt_success_rate"))
        if "first_attempt_count" not in frame.columns:
            frame = frame.with_columns(pl.lit(0, dtype=pl.Float64).alias("first_attempt_count"))
    if "exercise_label" not in frame.columns:
        frame = frame.with_columns(pl.col("exercise_id").cast(pl.Utf8).alias("exercise_label"))
    if "exercise_type" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("exercise_type"))

    filtered = frame.filter(
        (pl.col("module_code") == module_code)
        & (pl.col("objective_id") == objective_id)
        & (pl.col("activity_id") == activity_id)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if filtered.height == 0:
        return empty_drilldown_df()

    drilldown = (
        filtered.group_by(["exercise_id", "exercise_label", "exercise_type"])
        .agg(
            pl.sum("attempts").cast(pl.Float64).alias("attempts"),
            ((pl.col("success_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum()).alias(
                "success_rate"
            ),
            ((pl.col("repeat_attempt_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum()).alias(
                "repeat_attempt_rate"
            ),
            (pl.col("first_attempt_success_rate") * pl.col("first_attempt_count"))
            .sum()
            .alias("weighted_first_attempt_success_sum"),
            pl.sum("first_attempt_count").cast(pl.Float64).alias("first_attempt_count"),
            pl.col("median_duration").median().alias("median_duration"),
            pl.col("avg_attempt_number").mean().alias("avg_attempt_number"),
        )
        .with_columns(
            pl.when(pl.col("first_attempt_count") > 0)
            .then(pl.col("weighted_first_attempt_success_sum") / pl.col("first_attempt_count"))
            .otherwise(None)
            .alias("first_attempt_success_rate")
        )
        .drop("weighted_first_attempt_success_sum")
        .with_columns(
            pl.when(pl.col("exercise_label").is_null() | (pl.col("exercise_label").str.strip_chars() == ""))
            .then(pl.col("exercise_id"))
            .otherwise(pl.col("exercise_label"))
            .alias("exercise_label"),
            pl.col("exercise_id").cast(pl.Utf8).str.slice(0, 8).alias("exercise_short_id"),
            pl.when(pl.col("exercise_type").is_null() | (pl.col("exercise_type").str.strip_chars() == ""))
            .then(pl.lit("unknown"))
            .otherwise(pl.col("exercise_type"))
            .alias("exercise_type"),
        )
        .with_columns(pl.col("exercise_short_id").alias("exercise_display_label"))
        .with_columns(
            pl.when(pl.lit(metric) == "attempts")
            .then(pl.col("attempts"))
            .when(pl.lit(metric) == "success_rate")
            .then(pl.col("success_rate"))
            .when(pl.lit(metric) == "exercise_balanced_success_rate")
            .then(pl.col("success_rate"))
            .when(pl.lit(metric) == "playlist_unique_exercises")
            .then(pl.col("attempts"))
            .when(pl.lit(metric) == "repeat_attempt_rate")
            .then(pl.col("repeat_attempt_rate"))
            .otherwise(pl.coalesce([pl.col("first_attempt_success_rate"), pl.lit(0.0)]))
            .alias("metric_value")
        )
        .with_columns(
            pl.lit(None, dtype=pl.Float64).alias("exercise_elo"),
            pl.lit(None, dtype=pl.Int64).alias("calibration_attempts"),
            pl.lit(None, dtype=pl.Float64).alias("calibration_success_rate"),
            _formatted_metric_text_expr(metric),
        )
        .sort(["metric_value", "attempts"], descending=[True, True])
    )
    return drilldown.select(list(DRILLDOWN_SCHEMA.keys()))

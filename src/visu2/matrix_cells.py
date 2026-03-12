"""Main cell-building logic for the objective-activity matrix."""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from .matrix_ordering import safe_label, summary_maps
from .matrix_types import (
    CELLS_SCHEMA,
    VALID_MATRIX_METRICS,
    as_frame,
    assert_required_columns,
    collect_lazy,
    columns_of,
    empty_cells_df,
    format_cell_value,
)


def build_objective_activity_cells(
    agg_activity_daily: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date,
    end_date: date,
    metric: str,
    summary_payload: dict[str, Any],
    agg_exercise_daily: pl.DataFrame | pl.LazyFrame | None = None,
    agg_activity_elo: pl.DataFrame | pl.LazyFrame | None = None,
    fact_attempt_core: pl.DataFrame | pl.LazyFrame | None = None,
    work_mode: str | None = None,
) -> pl.DataFrame:
    """Build the ragged matrix cell table for the selected module and metric."""
    if metric not in VALID_MATRIX_METRICS:
        raise ValueError(f"Unsupported metric '{metric}'. Expected one of {list(VALID_MATRIX_METRICS)}")

    frame = as_frame(agg_activity_daily)
    assert_required_columns(
        frame,
        [
            "date_utc",
            "module_code",
            "objective_id",
            "activity_id",
            "attempts",
            "success_rate",
            "repeat_attempt_rate",
        ],
    )

    if metric == "first_attempt_success_rate":
        assert_required_columns(frame, ["first_attempt_success_rate", "first_attempt_count"])
    else:
        if "first_attempt_success_rate" not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("first_attempt_success_rate"))
        if "first_attempt_count" not in frame.columns:
            frame = frame.with_columns(pl.lit(0, dtype=pl.Float64).alias("first_attempt_count"))

    if "objective_label" not in frame.columns:
        frame = frame.with_columns(pl.col("objective_id").cast(pl.Utf8).alias("objective_label"))
    if "activity_label" not in frame.columns:
        frame = frame.with_columns(pl.col("activity_id").cast(pl.Utf8).alias("activity_label"))

    filtered = frame.filter(
        (pl.col("module_code") == module_code)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if filtered.height == 0:
        return empty_cells_df()

    normalized = filtered.with_columns(
        pl.col("objective_id").cast(pl.Utf8),
        pl.col("activity_id").cast(pl.Utf8),
        pl.when(pl.col("objective_label").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.col("objective_label").cast(pl.Utf8))
        .alias("objective_label"),
        pl.when(pl.col("activity_label").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.col("activity_label").cast(pl.Utf8))
        .alias("activity_label"),
    )

    if work_mode is not None and metric != "activity_mean_exercise_elo":
        if fact_attempt_core is None:
            raise ValueError(f"Matrix metric '{metric}' with a cohort population requires fact_attempt_core.")
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
            ]
            if col not in fact_cols
        ]
        if missing_fact_cols:
            raise ValueError(f"Matrix source is missing required columns: {sorted(set(missing_fact_cols))}")
        if metric == "playlist_unique_exercises" and work_mode != "playlist":
            raise ValueError("Matrix metric 'playlist_unique_exercises' is only available for playlist mode.")

        fact_lf = fact_attempt_core.lazy() if isinstance(fact_attempt_core, pl.DataFrame) else fact_attempt_core
        fact_filtered = fact_lf.filter(
            (pl.col("module_code") == module_code)
            & (pl.col("date_utc") >= pl.lit(start_date))
            & (pl.col("date_utc") <= pl.lit(end_date))
            & (pl.col("work_mode") == work_mode)
            & pl.col("objective_id").is_not_null()
            & (pl.col("objective_id").cast(pl.Utf8) != "None")
            & pl.col("activity_id").is_not_null()
            & (pl.col("activity_id").cast(pl.Utf8) != "None")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8) != "None")
        )

        if metric == "exercise_balanced_success_rate":
            exercise_group_exprs: list[pl.Expr] = [
                pl.col("data_correct").cast(pl.Float64).mean().alias("exercise_success_rate"),
            ]
            if "objective_label" in fact_cols:
                exercise_group_exprs.append(
                    pl.col("objective_label").cast(pl.Utf8).drop_nulls().first().alias("objective_label")
                )
            else:
                exercise_group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("objective_label"))
            if "activity_label" in fact_cols:
                exercise_group_exprs.append(
                    pl.col("activity_label").cast(pl.Utf8).drop_nulls().first().alias("activity_label")
                )
            else:
                exercise_group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("activity_label"))
            aggregated = collect_lazy(
                fact_filtered.group_by(["module_code", "objective_id", "activity_id", "exercise_id"]).agg(
                    exercise_group_exprs
                )
                .group_by(["module_code", "objective_id", "activity_id"])
                .agg(
                    pl.col("objective_label").drop_nulls().first().alias("objective_label"),
                    pl.col("activity_label").drop_nulls().first().alias("activity_label"),
                    pl.col("exercise_success_rate").drop_nulls().mean().alias("exercise_balanced_success_rate"),
                )
            ).to_dicts()
        elif metric == "playlist_unique_exercises":
            group_exprs: list[pl.Expr] = [
                pl.col("exercise_id").cast(pl.Utf8).n_unique().cast(pl.Float64).alias("playlist_unique_exercises"),
            ]
            if "objective_label" in fact_cols:
                group_exprs.append(
                    pl.col("objective_label").cast(pl.Utf8).drop_nulls().first().alias("objective_label")
                )
            else:
                group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("objective_label"))
            if "activity_label" in fact_cols:
                group_exprs.append(
                    pl.col("activity_label").cast(pl.Utf8).drop_nulls().first().alias("activity_label")
                )
            else:
                group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("activity_label"))
            aggregated = collect_lazy(
                fact_filtered.group_by(["module_code", "objective_id", "activity_id"]).agg(group_exprs)
            ).to_dicts()
        else:
            group_exprs = [
                pl.len().cast(pl.Float64).alias("attempts_sum"),
                pl.col("data_correct").cast(pl.Float64).sum().alias("weighted_success_sum"),
                (pl.col("attempt_number") > 1).cast(pl.Float64).sum().alias("weighted_repeat_sum"),
                pl.col("data_correct")
                .filter(pl.col("attempt_number") == 1)
                .cast(pl.Float64)
                .sum()
                .alias("weighted_first_attempt_success_sum"),
                (pl.col("attempt_number") == 1).cast(pl.Float64).sum().alias("first_attempt_count_sum"),
            ]
            if "objective_label" in fact_cols:
                group_exprs.append(
                    pl.col("objective_label").cast(pl.Utf8).drop_nulls().first().alias("objective_label")
                )
            else:
                group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("objective_label"))
            if "activity_label" in fact_cols:
                group_exprs.append(
                    pl.col("activity_label").cast(pl.Utf8).drop_nulls().first().alias("activity_label")
                )
            else:
                group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("activity_label"))
            aggregated = collect_lazy(
                fact_filtered.group_by(["module_code", "objective_id", "activity_id"]).agg(group_exprs)
            ).to_dicts()
    elif metric == "exercise_balanced_success_rate":
        if agg_exercise_daily is None:
            raise ValueError("Matrix metric 'exercise_balanced_success_rate' requires agg_exercise_daily.")
        exercise_frame = as_frame(agg_exercise_daily)
        assert_required_columns(
            exercise_frame,
            [
                "date_utc",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "attempts",
                "success_rate",
            ],
        )
        if "objective_label" not in exercise_frame.columns:
            exercise_frame = exercise_frame.with_columns(
                pl.col("objective_id").cast(pl.Utf8).alias("objective_label")
            )
        if "activity_label" not in exercise_frame.columns:
            exercise_frame = exercise_frame.with_columns(
                pl.col("activity_id").cast(pl.Utf8).alias("activity_label")
            )
        exercise_filtered = exercise_frame.filter(
            (pl.col("module_code") == module_code)
            & (pl.col("date_utc") >= pl.lit(start_date))
            & (pl.col("date_utc") <= pl.lit(end_date))
        )
        if exercise_filtered.height == 0:
            return empty_cells_df()
        aggregated = (
            exercise_filtered.with_columns(
                pl.col("objective_id").cast(pl.Utf8),
                pl.col("activity_id").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8),
                pl.when(pl.col("objective_label").is_null())
                .then(pl.lit(None, dtype=pl.Utf8))
                .otherwise(pl.col("objective_label").cast(pl.Utf8))
                .alias("objective_label"),
                pl.when(pl.col("activity_label").is_null())
                .then(pl.lit(None, dtype=pl.Utf8))
                .otherwise(pl.col("activity_label").cast(pl.Utf8))
                .alias("activity_label"),
            )
            .group_by(["module_code", "objective_id", "activity_id", "exercise_id"])
            .agg(
                pl.col("objective_label").drop_nulls().first().alias("objective_label"),
                pl.col("activity_label").drop_nulls().first().alias("activity_label"),
                pl.sum("attempts").cast(pl.Float64).alias("exercise_attempts_sum"),
                (pl.col("success_rate") * pl.col("attempts"))
                .sum()
                .cast(pl.Float64)
                .alias("exercise_weighted_success_sum"),
            )
            .with_columns(
                pl.when(pl.col("exercise_attempts_sum") > 0.0)
                .then(pl.col("exercise_weighted_success_sum") / pl.col("exercise_attempts_sum"))
                .otherwise(None)
                .alias("exercise_success_rate")
            )
            .group_by(["module_code", "objective_id", "activity_id"])
            .agg(
                pl.col("objective_label").drop_nulls().first().alias("objective_label"),
                pl.col("activity_label").drop_nulls().first().alias("activity_label"),
                pl.col("exercise_success_rate")
                .drop_nulls()
                .mean()
                .cast(pl.Float64)
                .alias("exercise_balanced_success_rate"),
            )
            .to_dicts()
        )
    elif metric == "activity_mean_exercise_elo":
        if agg_activity_elo is None:
            raise ValueError("Matrix metric 'activity_mean_exercise_elo' requires agg_activity_elo.")
        activity_elo_frame = as_frame(agg_activity_elo)
        assert_required_columns(
            activity_elo_frame,
            [
                "module_code",
                "objective_id",
                "activity_id",
                "activity_mean_exercise_elo",
                "calibrated_exercise_count",
                "catalog_exercise_count",
            ],
        )
        if "objective_label" not in activity_elo_frame.columns:
            activity_elo_frame = activity_elo_frame.with_columns(
                pl.col("objective_id").cast(pl.Utf8).alias("objective_label")
            )
        if "activity_label" not in activity_elo_frame.columns:
            activity_elo_frame = activity_elo_frame.with_columns(
                pl.col("activity_id").cast(pl.Utf8).alias("activity_label")
            )
        aggregated = (
            activity_elo_frame.filter(pl.col("module_code") == module_code)
            .filter(pl.col("activity_mean_exercise_elo").is_not_null())
            .with_columns(
                pl.col("objective_id").cast(pl.Utf8),
                pl.col("activity_id").cast(pl.Utf8),
                pl.col("objective_label").cast(pl.Utf8),
                pl.col("activity_label").cast(pl.Utf8),
            )
            .select(
                [
                    "module_code",
                    "objective_id",
                    "activity_id",
                    "objective_label",
                    "activity_label",
                    "activity_mean_exercise_elo",
                ]
            )
            .to_dicts()
        )
    elif metric == "playlist_unique_exercises":
        if fact_attempt_core is None:
            raise ValueError("Matrix metric 'playlist_unique_exercises' requires fact_attempt_core.")
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
            ]
            if col not in fact_cols
        ]
        if missing_fact_cols:
            raise ValueError(f"Matrix source is missing required columns: {missing_fact_cols}")
        fact_lf = fact_attempt_core.lazy() if isinstance(fact_attempt_core, pl.DataFrame) else fact_attempt_core
        playlist_filtered = fact_lf.filter(
            (pl.col("module_code") == module_code)
            & (pl.col("date_utc") >= pl.lit(start_date))
            & (pl.col("date_utc") <= pl.lit(end_date))
            & (pl.col("work_mode") == "playlist")
            & pl.col("objective_id").is_not_null()
            & (pl.col("objective_id").cast(pl.Utf8) != "None")
            & pl.col("activity_id").is_not_null()
            & (pl.col("activity_id").cast(pl.Utf8) != "None")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8) != "None")
        )
        group_exprs: list[pl.Expr] = [
            pl.col("exercise_id").cast(pl.Utf8).n_unique().cast(pl.Float64).alias("playlist_unique_exercises"),
        ]
        if "objective_label" in fact_cols:
            group_exprs.append(
                pl.col("objective_label").cast(pl.Utf8).drop_nulls().first().alias("objective_label")
            )
        else:
            group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("objective_label"))
        if "activity_label" in fact_cols:
            group_exprs.append(
                pl.col("activity_label").cast(pl.Utf8).drop_nulls().first().alias("activity_label")
            )
        else:
            group_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("activity_label"))
        aggregated = collect_lazy(
            playlist_filtered.group_by(["module_code", "objective_id", "activity_id"]).agg(group_exprs)
        ).to_dicts()
    else:
        aggregated = (
            normalized.group_by(["module_code", "objective_id", "activity_id"])
            .agg(
                pl.col("objective_label").drop_nulls().first().alias("objective_label"),
                pl.col("activity_label").drop_nulls().first().alias("activity_label"),
                pl.sum("attempts").cast(pl.Float64).alias("attempts_sum"),
                (pl.col("success_rate") * pl.col("attempts")).sum().cast(pl.Float64).alias("weighted_success_sum"),
                (pl.col("repeat_attempt_rate") * pl.col("attempts"))
                .sum()
                .cast(pl.Float64)
                .alias("weighted_repeat_sum"),
                (pl.col("first_attempt_success_rate") * pl.col("first_attempt_count"))
                .sum()
                .cast(pl.Float64)
                .alias("weighted_first_attempt_success_sum"),
                pl.sum("first_attempt_count").cast(pl.Float64).alias("first_attempt_count_sum"),
            )
            .to_dicts()
        )

    if not aggregated:
        return empty_cells_df()

    (
        objective_order_map,
        objective_activity_order_map,
        objective_summary_label_map,
        activity_summary_label_map,
    ) = summary_maps(summary_payload=summary_payload, module_code=module_code)

    per_objective: dict[str, dict[str, object]] = {}
    for row in aggregated:
        objective_id = str(row.get("objective_id") or "").strip()
        activity_id = str(row.get("activity_id") or "").strip()
        if not objective_id or not activity_id:
            continue

        objective_label = safe_label(
            row.get("objective_label") or objective_summary_label_map.get(objective_id),
            objective_id,
        )
        activity_label = safe_label(
            row.get("activity_label") or activity_summary_label_map.get(activity_id),
            activity_id,
        )

        if metric == "exercise_balanced_success_rate":
            metric_value_raw = row.get("exercise_balanced_success_rate")
            if metric_value_raw is None:
                continue
            metric_value = float(metric_value_raw)
        elif metric == "activity_mean_exercise_elo":
            metric_value_raw = row.get("activity_mean_exercise_elo")
            if metric_value_raw is None:
                continue
            metric_value = float(metric_value_raw)
        elif metric == "playlist_unique_exercises":
            metric_value = float(row.get("playlist_unique_exercises") or 0.0)
        else:
            attempts_sum = float(row.get("attempts_sum") or 0.0)
            if metric == "attempts":
                metric_value = attempts_sum
            elif attempts_sum <= 0.0:
                metric_value = 0.0
            elif metric == "success_rate":
                metric_value = float(row.get("weighted_success_sum") or 0.0) / attempts_sum
            elif metric == "first_attempt_success_rate":
                first_attempt_count_sum = float(row.get("first_attempt_count_sum") or 0.0)
                metric_value = (
                    0.0
                    if first_attempt_count_sum <= 0.0
                    else float(row.get("weighted_first_attempt_success_sum") or 0.0)
                    / first_attempt_count_sum
                )
            else:
                metric_value = float(row.get("weighted_repeat_sum") or 0.0) / attempts_sum

        bucket = per_objective.setdefault(
            objective_id,
            {"objective_label": objective_label, "activities": []},
        )
        bucket["activities"].append(
            {
                "activity_id": activity_id,
                "activity_label": activity_label,
                "metric_value": metric_value,
            }
        )

    if not per_objective:
        return empty_cells_df()

    observed_objectives = list(per_objective.keys())
    ordered_summary_objectives = [
        objective_id
        for objective_id, _ in sorted(objective_order_map.items(), key=lambda item: item[1])
        if objective_id in per_objective
    ]
    fallback_objectives = sorted(
        [objective_id for objective_id in observed_objectives if objective_id not in objective_order_map],
        key=lambda objective_id: (
            str(per_objective[objective_id]["objective_label"]).lower(),
            objective_id,
        ),
    )
    ordered_objectives = ordered_summary_objectives + fallback_objectives

    objective_base_label_map = {
        objective_id: safe_label(str(per_objective[objective_id]["objective_label"]), objective_id)
        for objective_id in ordered_objectives
    }
    objective_label_counts: dict[str, int] = {}
    for base_label in objective_base_label_map.values():
        objective_label_counts[base_label] = objective_label_counts.get(base_label, 0) + 1

    objective_row_label_map = {
        objective_id: (
            base_label
            if objective_label_counts[base_label] <= 1
            else f"{base_label} ({objective_id[:8]})"
        )
        for objective_id, base_label in objective_base_label_map.items()
    }

    records: list[dict[str, object]] = []
    for objective_id in ordered_objectives:
        activity_rows = list(per_objective[objective_id]["activities"])
        activity_order_map = objective_activity_order_map.get(objective_id) or {}

        in_summary_activities = [
            row
            for row in sorted(
                activity_rows,
                key=lambda row: activity_order_map.get(str(row["activity_id"]), 10**9),
            )
            if str(row["activity_id"]) in activity_order_map
        ]
        fallback_activities = sorted(
            [row for row in activity_rows if str(row["activity_id"]) not in activity_order_map],
            key=lambda row: (str(row["activity_label"]).lower(), str(row["activity_id"])),
        )
        ordered_activities = in_summary_activities + fallback_activities

        for idx, activity_row in enumerate(ordered_activities, start=1):
            metric_value = float(activity_row["metric_value"])
            records.append(
                {
                    "module_code": module_code,
                    "objective_id": objective_id,
                    "objective_label": objective_base_label_map[objective_id],
                    "objective_row_label": objective_row_label_map[objective_id],
                    "activity_id": str(activity_row["activity_id"]),
                    "activity_label": safe_label(
                        str(activity_row["activity_label"]),
                        str(activity_row["activity_id"]),
                    ),
                    "activity_col_idx": idx,
                    "activity_col_label": f"A{idx}",
                    "metric_value": metric_value,
                    "metric_text": format_cell_value(metric=metric, value=metric_value),
                }
            )

    if not records:
        return empty_cells_df()
    return pl.DataFrame(records, schema=CELLS_SCHEMA)

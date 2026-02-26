from __future__ import annotations

import math
from datetime import date

import polars as pl

VALID_MATRIX_METRICS = (
    "attempts",
    "success_rate",
    "exercise_balanced_success_rate",
    "repeat_attempt_rate",
    "first_attempt_success_rate",
    "playlist_unique_exercises",
)

_CELLS_SCHEMA: dict[str, pl.DataType] = {
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
}

_DRILLDOWN_SCHEMA: dict[str, pl.DataType] = {
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
}


def _empty_cells_df() -> pl.DataFrame:
    return pl.DataFrame(
        {name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in _CELLS_SCHEMA.items()}
    )


def _empty_drilldown_df() -> pl.DataFrame:
    return pl.DataFrame(
        {name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in _DRILLDOWN_SCHEMA.items()}
    )


def _as_frame(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return df.collect() if isinstance(df, pl.LazyFrame) else df


def _collect_lazy(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect()


def _columns_of(df: pl.DataFrame | pl.LazyFrame) -> list[str]:
    if isinstance(df, pl.DataFrame):
        return list(df.columns)
    try:
        return list(df.collect_schema().names())
    except Exception:
        return list(df.schema.keys())


def _safe_label(label: str | None, identifier: str | None) -> str:
    normalized = str(label or "").strip()
    if normalized:
        return normalized
    return str(identifier or "").strip()


def _summary_maps(summary_payload: dict, module_code: str) -> tuple[dict[str, int], dict[str, dict[str, int]], dict[str, str], dict[str, str]]:
    modules = summary_payload.get("modules") or []
    objectives = summary_payload.get("objectives") or []
    activities = summary_payload.get("activities") or []

    # learning_catalog format: modules contain nested objectives/activities.
    if isinstance(modules, list) and modules and isinstance(modules[0], dict) and "objectives" in modules[0]:
        objective_order_map: dict[str, int] = {}
        objective_activity_order_map: dict[str, dict[str, int]] = {}
        objective_label_map: dict[str, str] = {}
        activity_label_map: dict[str, str] = {}

        module_row = next(
            (
                row
                for row in modules
                if isinstance(row, dict) and str(row.get("code") or "").strip() == module_code
            ),
            None,
        )
        if not isinstance(module_row, dict):
            return objective_order_map, objective_activity_order_map, objective_label_map, activity_label_map

        module_objectives = module_row.get("objectives") or []
        for objective_idx, objective_row in enumerate(module_objectives):
            if not isinstance(objective_row, dict):
                continue
            objective_id = str(objective_row.get("id") or "").strip()
            if not objective_id:
                continue
            objective_order_map[objective_id] = objective_idx
            title = objective_row.get("title") if isinstance(objective_row.get("title"), dict) else {}
            objective_label_map[objective_id] = _safe_label(
                (title or {}).get("short") or (title or {}).get("long"),
                objective_id,
            )

            activity_order_map: dict[str, int] = {}
            objective_activities = objective_row.get("activities") or []
            for activity_idx, activity_row in enumerate(objective_activities):
                if not isinstance(activity_row, dict):
                    continue
                activity_id = str(activity_row.get("id") or "").strip()
                if not activity_id:
                    continue
                activity_order_map[activity_id] = activity_idx
                a_title = (
                    activity_row.get("title")
                    if isinstance(activity_row.get("title"), dict)
                    else {}
                )
                activity_label_map[activity_id] = _safe_label(
                    (a_title or {}).get("short") or (a_title or {}).get("long"),
                    activity_id,
                )
            objective_activity_order_map[objective_id] = activity_order_map

        return objective_order_map, objective_activity_order_map, objective_label_map, activity_label_map

    module_row = next(
        (
            row
            for row in modules
            if isinstance(row, dict) and str(row.get("code") or "").strip() == module_code
        ),
        None,
    )

    objective_order_map: dict[str, int] = {}
    if isinstance(module_row, dict):
        objective_ids = module_row.get("objectiveIds") or []
        for idx, objective_id in enumerate(objective_ids):
            objective_order_map[str(objective_id)] = idx

    objective_activity_order_map: dict[str, dict[str, int]] = {}
    objective_label_map: dict[str, str] = {}
    for row in objectives:
        if not isinstance(row, dict):
            continue
        objective_id = str(row.get("id") or "").strip()
        if not objective_id:
            continue
        title = row.get("title") if isinstance(row.get("title"), dict) else {}
        objective_label_map[objective_id] = _safe_label(
            (title or {}).get("short") or (title or {}).get("long"),
            objective_id,
        )

        activity_order_map: dict[str, int] = {}
        for idx, activity_id in enumerate(row.get("activityIds") or []):
            activity_order_map[str(activity_id)] = idx
        objective_activity_order_map[objective_id] = activity_order_map

    activity_label_map: dict[str, str] = {}
    for row in activities:
        if not isinstance(row, dict):
            continue
        activity_id = str(row.get("id") or "").strip()
        if not activity_id:
            continue
        title = row.get("title") if isinstance(row.get("title"), dict) else {}
        activity_label_map[activity_id] = _safe_label(
            (title or {}).get("short") or (title or {}).get("long"),
            activity_id,
        )

    return objective_order_map, objective_activity_order_map, objective_label_map, activity_label_map


def _assert_required_columns(frame: pl.DataFrame, required_columns: list[str]) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Matrix source is missing required columns: {missing}")


def format_cell_value(metric: str, value: float | None) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if metric in {"attempts", "playlist_unique_exercises"}:
        return f"{int(round(float(value)))}"
    return f"{float(value) * 100:.1f}%"


def build_objective_activity_cells(
    agg_activity_daily: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date,
    end_date: date,
    metric: str,
    summary_payload: dict,
    agg_exercise_daily: pl.DataFrame | pl.LazyFrame | None = None,
    fact_attempt_core: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.DataFrame:
    if metric not in VALID_MATRIX_METRICS:
        raise ValueError(
            f"Unsupported metric '{metric}'. Expected one of {list(VALID_MATRIX_METRICS)}"
        )

    frame = _as_frame(agg_activity_daily)
    _assert_required_columns(
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
        _assert_required_columns(frame, ["first_attempt_success_rate", "first_attempt_count"])
    else:
        if "first_attempt_success_rate" not in frame.columns:
            frame = frame.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("first_attempt_success_rate")
            )
        if "first_attempt_count" not in frame.columns:
            frame = frame.with_columns(
                pl.lit(0, dtype=pl.Float64).alias("first_attempt_count")
            )

    if "objective_label" not in frame.columns:
        frame = frame.with_columns(
            pl.col("objective_id").cast(pl.Utf8).alias("objective_label")
        )
    if "activity_label" not in frame.columns:
        frame = frame.with_columns(
            pl.col("activity_id").cast(pl.Utf8).alias("activity_label")
        )

    filtered = frame.filter(
        (pl.col("module_code") == module_code)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if filtered.height == 0:
        return _empty_cells_df()

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

    if metric == "exercise_balanced_success_rate":
        if agg_exercise_daily is None:
            raise ValueError(
                "Matrix metric 'exercise_balanced_success_rate' requires agg_exercise_daily."
            )
        exercise_frame = _as_frame(agg_exercise_daily)
        _assert_required_columns(
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
            return _empty_cells_df()
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
    elif metric == "playlist_unique_exercises":
        if fact_attempt_core is None:
            raise ValueError(
                "Matrix metric 'playlist_unique_exercises' requires fact_attempt_core."
            )
        fact_cols = _columns_of(fact_attempt_core)
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
            raise ValueError(
                "Matrix source is missing required columns: "
                f"{missing_fact_cols}"
            )
        fact_lf = fact_attempt_core.lazy() if isinstance(fact_attempt_core, pl.DataFrame) else fact_attempt_core
        playlist_filtered = (
            fact_lf.filter(
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
        )
        group_exprs: list[pl.Expr] = [
            pl.col("exercise_id").cast(pl.Utf8).n_unique().cast(pl.Float64).alias("playlist_unique_exercises"),
        ]
        if "objective_label" in fact_cols:
            group_exprs.append(
                pl.col("objective_label")
                .cast(pl.Utf8)
                .drop_nulls()
                .first()
                .alias("objective_label")
            )
        else:
            group_exprs.append(
                pl.lit(None, dtype=pl.Utf8).alias("objective_label")
            )
        if "activity_label" in fact_cols:
            group_exprs.append(
                pl.col("activity_label")
                .cast(pl.Utf8)
                .drop_nulls()
                .first()
                .alias("activity_label")
            )
        else:
            group_exprs.append(
                pl.lit(None, dtype=pl.Utf8).alias("activity_label")
            )

        aggregated = _collect_lazy(
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
        return _empty_cells_df()

    (
        objective_order_map,
        objective_activity_order_map,
        objective_summary_label_map,
        activity_summary_label_map,
    ) = _summary_maps(summary_payload=summary_payload, module_code=module_code)

    per_objective: dict[str, dict[str, object]] = {}
    for row in aggregated:
        objective_id = str(row.get("objective_id") or "").strip()
        activity_id = str(row.get("activity_id") or "").strip()
        if not objective_id or not activity_id:
            continue

        objective_label = _safe_label(
            row.get("objective_label") or objective_summary_label_map.get(objective_id),
            objective_id,
        )
        activity_label = _safe_label(
            row.get("activity_label") or activity_summary_label_map.get(activity_id),
            activity_id,
        )

        if metric == "exercise_balanced_success_rate":
            metric_value_raw = row.get("exercise_balanced_success_rate")
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
                if first_attempt_count_sum <= 0.0:
                    metric_value = 0.0
                else:
                    metric_value = (
                        float(row.get("weighted_first_attempt_success_sum") or 0.0)
                        / first_attempt_count_sum
                    )
            else:
                metric_value = float(row.get("weighted_repeat_sum") or 0.0) / attempts_sum

        if objective_id not in per_objective:
            per_objective[objective_id] = {
                "objective_label": objective_label,
                "activities": [],
            }

        per_objective[objective_id]["activities"].append(
            {
                "activity_id": activity_id,
                "activity_label": activity_label,
                "metric_value": metric_value,
            }
        )

    if not per_objective:
        return _empty_cells_df()

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
        objective_id: _safe_label(
            str(per_objective[objective_id]["objective_label"]),
            objective_id,
        )
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
                    "activity_label": _safe_label(
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
        return _empty_cells_df()

    return pl.DataFrame(records, schema=_CELLS_SCHEMA)


def build_ragged_matrix_payload(cells_df: pl.DataFrame | pl.LazyFrame) -> dict[str, object]:
    frame = _as_frame(cells_df)
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

    _assert_required_columns(
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
    text_values: list[list[str]] = [
        ["" for _ in range(max_activity_cols)] for _ in range(len(objective_ids))
    ]
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


def build_exercise_drilldown_frame(
    agg_exercise_daily: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    objective_id: str,
    activity_id: str,
    start_date: date,
    end_date: date,
    metric: str,
    fact_attempt_core: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.DataFrame:
    if metric not in VALID_MATRIX_METRICS:
        raise ValueError(
            f"Unsupported metric '{metric}'. Expected one of {list(VALID_MATRIX_METRICS)}"
        )

    if metric == "playlist_unique_exercises":
        if fact_attempt_core is None:
            raise ValueError(
                "Exercise drilldown for 'playlist_unique_exercises' requires fact_attempt_core."
            )
        fact_cols = _columns_of(fact_attempt_core)
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
            raise ValueError(
                "Drilldown source is missing required columns: "
                f"{missing_fact_cols}"
            )
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
        playlist_grouped = _collect_lazy(
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
            return _empty_drilldown_df()
        drilldown = (
            playlist_grouped.with_columns(
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8).alias("exercise_label"),
                pl.lit("unknown", dtype=pl.Utf8).alias("exercise_type"),
                pl.col("exercise_id").cast(pl.Utf8).str.slice(0, 8).alias("exercise_short_id"),
            )
            .with_columns(
                pl.col("exercise_short_id").alias("exercise_display_label")
            )
            .with_columns(
                pl.col("attempts").alias("metric_value")
            )
            .with_columns(
                pl.struct(["metric_value"])
                .map_elements(
                    lambda row: format_cell_value(metric="attempts", value=row["metric_value"]),
                    return_dtype=pl.Utf8,
                )
                .alias("metric_text")
            )
            .sort(["metric_value", "attempts"], descending=[True, True])
        )
        return drilldown.select(list(_DRILLDOWN_SCHEMA.keys()))

    frame = _as_frame(agg_exercise_daily)
    _assert_required_columns(
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
        _assert_required_columns(frame, ["first_attempt_success_rate", "first_attempt_count"])
    else:
        if "first_attempt_success_rate" not in frame.columns:
            frame = frame.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("first_attempt_success_rate")
            )
        if "first_attempt_count" not in frame.columns:
            frame = frame.with_columns(
                pl.lit(0, dtype=pl.Float64).alias("first_attempt_count")
            )
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
        return _empty_drilldown_df()

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
        .with_columns(
            pl.col("exercise_short_id").alias("exercise_display_label")
        )
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
            pl.struct(["metric_value"])
            .map_elements(
                lambda row: format_cell_value(metric=metric, value=row["metric_value"]),
                return_dtype=pl.Utf8,
            )
            .alias("metric_text")
        )
        .sort(["metric_value", "attempts"], descending=[True, True])
    )

    return drilldown.select(list(_DRILLDOWN_SCHEMA.keys()))

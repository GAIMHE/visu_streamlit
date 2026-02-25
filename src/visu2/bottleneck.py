from __future__ import annotations

from datetime import date

import pandas as pd
import polars as pl

from .contracts import ACTIVE_CANONICAL_MODULE_CODES

BOTTLENECK_LEVEL_CONFIG: dict[str, dict[str, str]] = {
    "Module": {
        "id_col": "module_id",
        "label_col": "module_label",
        "context_label_col": "module_code",
        "context_id_col": "module_code",
    },
    "Objective": {
        "id_col": "objective_id",
        "label_col": "objective_label",
        "context_label_col": "module_label",
        "context_id_col": "module_code",
    },
    "Activity": {
        "id_col": "activity_id",
        "label_col": "activity_label",
        "context_label_col": "objective_label",
        "context_id_col": "objective_id",
    },
}


def make_unique_plot_label(
    base_label: str | None,
    context: str | None,
    entity_id: str | None,
    collision_rank: int,
) -> str:
    base = str(base_label or "").strip() or "(unlabeled)"
    context_value = str(context or "").strip() or "unknown"
    entity = str(entity_id or "").strip()
    entity_short = entity[:8] if entity else "unknown"

    if collision_rank <= 0:
        return base
    if collision_rank == 1:
        return f"{base} ({context_value})"
    return f"{base} ({context_value} #{entity_short})"


def apply_bottleneck_filters(
    frame: pl.DataFrame,
    start_date: date,
    end_date: date,
    module_code: str | None,
    objective_id: str | None,
    activity_id: str | None,
    level: str,
    canonical_modules: tuple[str, ...] = ACTIVE_CANONICAL_MODULE_CODES,
) -> pl.DataFrame:
    if level not in BOTTLENECK_LEVEL_CONFIG:
        raise ValueError(f"Unsupported bottleneck level: {level}")

    filtered = frame.filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_code:
        filtered = filtered.filter(pl.col("module_code") == module_code)
    if level in {"Objective", "Activity"} and objective_id:
        filtered = filtered.filter(pl.col("objective_id") == objective_id)
    if level == "Activity" and activity_id:
        filtered = filtered.filter(pl.col("activity_id") == activity_id)

    filtered = filtered.filter(pl.col("module_code").is_in(list(canonical_modules)))
    return filtered


def build_bottleneck_frame(
    filtered_activity: pl.DataFrame,
    level: str,
    min_attempts: int,
    top_n: int,
) -> pd.DataFrame:
    if level not in BOTTLENECK_LEVEL_CONFIG:
        raise ValueError(f"Unsupported bottleneck level: {level}")
    if filtered_activity.height == 0:
        return pd.DataFrame()

    cfg = BOTTLENECK_LEVEL_CONFIG[level]
    id_col = cfg["id_col"]
    label_col = cfg["label_col"]
    context_label_col = cfg["context_label_col"]
    context_id_col = cfg["context_id_col"]

    group_cols = [id_col, label_col, context_label_col, context_id_col]
    group_cols = list(dict.fromkeys(group_cols))

    agg = (
        filtered_activity.with_columns(
            (1 - pl.col("success_rate")).alias("failure_rate"),
            ((1 - pl.col("success_rate")) * 0.7 + pl.col("repeat_attempt_rate") * 0.3).alias(
                "bottleneck_score"
            ),
        )
        .filter(pl.col(id_col).is_not_null())
        .group_by(group_cols)
        .agg(
            ((pl.col("bottleneck_score") * pl.col("attempts")).sum() / pl.col("attempts").sum()).alias(
                "bottleneck_score"
            ),
            ((pl.col("failure_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum()).alias(
                "failure_rate"
            ),
            (
                (pl.col("repeat_attempt_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum()
            ).alias("repeat_attempt_rate"),
            pl.sum("attempts").alias("attempts"),
        )
        .filter(pl.col("attempts") >= min_attempts)
        .with_columns(
            pl.col(id_col).cast(pl.Utf8).alias("entity_id"),
            pl.coalesce(
                [
                    pl.col(label_col).cast(pl.Utf8),
                    pl.col(id_col).cast(pl.Utf8),
                ]
            ).alias("entity_label_raw"),
            pl.coalesce(
                [
                    pl.col(context_label_col).cast(pl.Utf8),
                    pl.col(context_id_col).cast(pl.Utf8),
                    pl.col(id_col).cast(pl.Utf8),
                ]
            ).alias("entity_context_raw"),
        )
        .select(
            [
                "entity_id",
                "entity_label_raw",
                "entity_context_raw",
                "attempts",
                "failure_rate",
                "repeat_attempt_rate",
                "bottleneck_score",
            ]
        )
        .sort("bottleneck_score", descending=True)
        .head(top_n)
        .to_pandas()
    )
    if agg.empty:
        return agg

    agg["entity_label_raw"] = agg["entity_label_raw"].fillna("").map(lambda v: str(v).strip() or "(unlabeled)")
    agg["entity_context_raw"] = agg["entity_context_raw"].fillna("").map(lambda v: str(v).strip() or "unknown")
    agg["entity_id"] = agg["entity_id"].fillna("").map(str)

    agg["label_collision_count"] = (
        agg.groupby("entity_label_raw")["entity_id"].transform("size").astype(int)
    )
    agg["context_collision_count"] = (
        agg.groupby(["entity_label_raw", "entity_context_raw"])["entity_id"].transform("size").astype(int)
    )

    def _label_row(row: pd.Series) -> str:
        collision_rank = 0
        if int(row["label_collision_count"]) > 1:
            collision_rank = 1
        if int(row["context_collision_count"]) > 1:
            collision_rank = 2
        return make_unique_plot_label(
            base_label=str(row["entity_label_raw"]),
            context=str(row["entity_context_raw"]),
            entity_id=str(row["entity_id"]),
            collision_rank=collision_rank,
        )

    agg["entity_plot_label"] = agg.apply(_label_row, axis=1)
    agg["level"] = level
    return agg

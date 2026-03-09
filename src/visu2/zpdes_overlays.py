"""Overlay metrics and focus filters for ZPDES dependency graphs."""

from __future__ import annotations

from datetime import date

import polars as pl


def attach_overlay_metrics_to_nodes(
    nodes: pl.DataFrame,
    agg_activity_daily: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    """Attach weighted activity/objective overlay metrics to dependency nodes."""
    frame = agg_activity_daily.collect() if isinstance(agg_activity_daily, pl.LazyFrame) else agg_activity_daily
    required = {
        "date_utc",
        "module_code",
        "objective_id",
        "activity_id",
        "attempts",
        "success_rate",
        "repeat_attempt_rate",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"agg_activity_daily is missing required columns: {missing}")

    filtered = frame.filter(
        (pl.col("module_code") == module_code)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if filtered.height == 0:
        return nodes.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("overlay_attempts"),
            pl.lit(None, dtype=pl.Float64).alias("overlay_success_rate"),
            pl.lit(None, dtype=pl.Float64).alias("overlay_repeat_attempt_rate"),
        )

    activity_metrics = (
        filtered.group_by("activity_id")
        .agg(
            pl.sum("attempts").cast(pl.Float64).alias("activity_attempts"),
            ((pl.col("success_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("activity_success_rate"),
            ((pl.col("repeat_attempt_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("activity_repeat_attempt_rate"),
        )
        .rename({"activity_id": "node_id"})
    )
    objective_metrics = (
        filtered.group_by("objective_id")
        .agg(
            pl.sum("attempts").cast(pl.Float64).alias("objective_attempts"),
            ((pl.col("success_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("objective_success_rate"),
            ((pl.col("repeat_attempt_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("objective_repeat_attempt_rate"),
        )
        .rename({"objective_id": "node_id"})
    )

    return (
        nodes.join(activity_metrics, on="node_id", how="left")
        .join(objective_metrics, on="node_id", how="left")
        .with_columns(
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("activity_attempts"))
            .when(pl.col("node_type") == "objective")
            .then(pl.col("objective_attempts"))
            .otherwise(None)
            .alias("overlay_attempts"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("activity_success_rate"))
            .when(pl.col("node_type") == "objective")
            .then(pl.col("objective_success_rate"))
            .otherwise(None)
            .alias("overlay_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("activity_repeat_attempt_rate"))
            .when(pl.col("node_type") == "objective")
            .then(pl.col("objective_repeat_attempt_rate"))
            .otherwise(None)
            .alias("overlay_repeat_attempt_rate"),
        )
        .drop(
            [
                "activity_attempts",
                "activity_success_rate",
                "activity_repeat_attempt_rate",
                "objective_attempts",
                "objective_success_rate",
                "objective_repeat_attempt_rate",
            ]
        )
    )


def filter_dependency_graph_by_objectives(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    objective_codes: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Restrict the dependency graph to selected objective lanes and internal edges."""
    selected = {str(code).strip() for code in objective_codes if str(code).strip()}
    if not selected:
        return nodes.head(0), edges.head(0)

    filtered_nodes = nodes.filter(pl.col("objective_code").is_in(list(selected)))
    valid_node_codes = set(filtered_nodes.select("node_code")["node_code"].cast(pl.Utf8).to_list())
    filtered_edges = edges.filter(
        pl.col("from_node_code").is_in(list(valid_node_codes))
        & pl.col("to_node_code").is_in(list(valid_node_codes))
    )
    return filtered_nodes, filtered_edges

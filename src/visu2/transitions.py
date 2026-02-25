from __future__ import annotations

import polars as pl


def build_transition_edges_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    lf = fact.lazy() if isinstance(fact, pl.DataFrame) else fact
    sequenced = (
        lf.sort(["user_id", "created_at"])
        .with_columns(
            pl.col("activity_id").shift(-1).over("user_id").alias("to_activity_id"),
            pl.col("activity_label").shift(-1).over("user_id").alias("to_activity_label"),
            pl.col("module_id").shift(-1).over("user_id").alias("to_module_id"),
            pl.col("module_label").shift(-1).over("user_id").alias("to_module_label"),
            pl.col("objective_id").shift(-1).over("user_id").alias("to_objective_id"),
            pl.col("data_correct").shift(-1).over("user_id").alias("to_data_correct"),
        )
        .rename(
            {
                "activity_id": "from_activity_id",
                "activity_label": "from_activity_label",
                "module_id": "from_module_id",
                "module_code": "from_module_code",
                "module_label": "from_module_label",
            }
        )
        .filter(pl.col("to_activity_id").is_not_null())
    )

    return (
        sequenced.group_by(
            [
                "date_utc",
                "from_activity_id",
                "from_activity_label",
                "to_activity_id",
                "to_activity_label",
                "from_module_id",
                "from_module_code",
                "from_module_label",
            ]
        )
        .agg(
            pl.len().alias("transition_count"),
            pl.col("to_data_correct").cast(pl.Int64).sum().alias("success_conditioned_count"),
            ((pl.col("objective_id") == pl.col("to_objective_id")).cast(pl.Float64))
            .mean()
            .alias("same_objective_rate"),
        )
        .sort("transition_count", descending=True)
        .collect()
    )

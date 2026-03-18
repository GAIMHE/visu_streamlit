"""Helpers for overview attempt-concentration summaries and chart rendering."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import polars as pl

from visu2.loaders import catalog_to_summary_frames, load_learning_catalog

CONCENTRATION_LEVEL_OPTIONS = {
    "Exercise": "exercise",
    "Activity": "activity",
    "Objective": "objective",
    "Module": "module",
}

CONCENTRATION_BASIS_OPTIONS = {
    "Content concentration": "content",
    "Student concentration": "student",
}

STUDENT_CONCENTRATION_SCOPE_OPTIONS = {
    "All attempts": "all_attempts",
    "Exercise": "exercise",
    "Activity": "activity",
    "Objective": "objective",
    "Module": "module",
}

DECILE_LABELS = (
    "Top 10%",
    "10-20%",
    "20-30%",
    "30-40%",
    "40-50%",
    "50-60%",
    "60-70%",
    "70-80%",
    "80-90%",
    "90-100%",
)

BUCKET_LABEL_BY_ORDER = {idx: label for idx, label in enumerate(DECILE_LABELS, start=1)}


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def _is_blank_expr(column: str) -> pl.Expr:
    return pl.col(column).is_null() | (pl.col(column).cast(pl.Utf8).str.strip_chars() == "")


def _clean_text_expr(column: str) -> pl.Expr:
    return pl.when(_is_blank_expr(column)).then(None).otherwise(pl.col(column).cast(pl.Utf8))


def _label_expr(label_col: str, id_col: str) -> pl.Expr:
    return pl.coalesce([_clean_text_expr(label_col), pl.col(id_col).cast(pl.Utf8)])


def _unmapped_initial_test_label_expr(level: str) -> pl.Expr:
    noun = {
        "activity": "activity",
        "objective": "objective",
        "module": "module",
        "exercise": "exercise",
    }.get(level, "content")
    module_ref = pl.coalesce(
        [
            _clean_text_expr("module_code_ref"),
            _clean_text_expr("module_label_ref"),
            pl.lit("unknown module"),
        ]
    )
    return pl.concat_str(
        [
            pl.lit(f"Unmapped initial-test {noun} ("),
            module_ref,
            pl.lit(")"),
        ]
    )


def _resolved_label_expr(level: str) -> pl.Expr:
    base_label = pl.coalesce(
        [
            _clean_text_expr("label"),
            _clean_text_expr("label_catalog"),
        ]
    )
    initial_test_only = (
        (pl.col("work_modes_seen").list.len() == 1)
        & (pl.col("work_modes_seen").list.get(0).cast(pl.Utf8) == pl.lit("initial-test"))
    )
    return (
        pl.when(base_label.is_null() & initial_test_only)
        .then(_unmapped_initial_test_label_expr(level))
        .otherwise(pl.coalesce([base_label, pl.col("id").cast(pl.Utf8)]))
    )


def _clean_user_id_expr() -> pl.Expr:
    return _clean_text_expr("user_id")


def _non_blank_user_filter_expr() -> pl.Expr:
    return pl.col("user_id").is_not_null() & (_clean_user_id_expr() != "")


def _bucket_order_expr(rank_col: str, total_col: str) -> pl.Expr:
    return (
        (((pl.col(rank_col).cast(pl.Float64) - 1.0) * 10.0) / pl.col(total_col).cast(pl.Float64))
        .floor()
        + 1
    ).clip(1, 10).cast(pl.Int64)


def _bucket_label_expr(order_col: str = "bucket_order") -> pl.Expr:
    return (
        pl.col(order_col)
        .replace_strict(BUCKET_LABEL_BY_ORDER, default=None)
        .cast(pl.Utf8)
    )


def load_catalog_contained_exercise_counts(path: Path) -> dict[str, pl.DataFrame]:
    """Load catalog-based contained-exercise counts by entity level."""
    catalog = load_learning_catalog(path)
    frames = catalog_to_summary_frames(catalog)
    hierarchy = frames.exercise_hierarchy

    exercise = (
        hierarchy.select(pl.col("exercise_id").cast(pl.Utf8).alias("id"))
        .unique()
        .with_columns(
            pl.col("id").alias("label"),
            pl.lit(1, dtype=pl.Int64).alias("contained_exercises"),
        )
    )
    activity = (
        hierarchy.group_by(["activity_id", "activity_label"])
        .agg(pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("contained_exercises"))
        .rename({"activity_id": "id", "activity_label": "label"})
        .with_columns(_label_expr("label", "id").alias("label"))
    )
    objective = (
        hierarchy.group_by(["objective_id", "objective_label"])
        .agg(pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("contained_exercises"))
        .rename({"objective_id": "id", "objective_label": "label"})
        .with_columns(_label_expr("label", "id").alias("label"))
    )
    module = (
        hierarchy.group_by(["module_code", "module_label"])
        .agg(pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("contained_exercises"))
        .rename({"module_code": "id", "module_label": "label"})
        .with_columns(_label_expr("label", "id").alias("label"))
    )
    return {
        "exercise": exercise.select(["id", "label", "contained_exercises"]),
        "activity": activity.select(["id", "label", "contained_exercises"]),
        "objective": objective.select(["id", "label", "contained_exercises"]),
        "module": module.select(["id", "label", "contained_exercises"]),
    }


def _rank_bucket_label(rank: int, total_entities: int) -> str:
    if total_entities <= 0:
        return DECILE_LABELS[-1]
    for idx, label in enumerate(DECILE_LABELS, start=1):
        cutoff = max(1, int(math.ceil(total_entities * idx / 10.0)))
        if rank <= cutoff:
            return label
    return DECILE_LABELS[-1]


def assign_rank_buckets(entity_summary: pl.DataFrame) -> pl.DataFrame:
    """Assign rank-based decile buckets to a sorted entity summary."""
    if entity_summary.height == 0:
        return entity_summary.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("bucket_key"),
            pl.lit(None, dtype=pl.Utf8).alias("bucket_label"),
            pl.lit(None, dtype=pl.Int64).alias("bucket_order"),
        )
    total = entity_summary.height
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(entity_summary.to_dicts(), start=1):
        bucket_label = _rank_bucket_label(rank, total)
        bucket_order = DECILE_LABELS.index(bucket_label) + 1
        row["bucket_key"] = f"decile_{bucket_order}"
        row["bucket_label"] = bucket_label
        row["bucket_order"] = bucket_order
        rows.append(row)
    return pl.DataFrame(rows).with_columns(
        pl.col("bucket_key").cast(pl.Utf8),
        pl.col("bucket_label").cast(pl.Utf8),
        pl.col("bucket_order").cast(pl.Int64),
    )


def build_entity_attempt_summary(
    fact: pl.DataFrame | pl.LazyFrame,
    *,
    level: str,
    work_modes: tuple[str, ...],
    contained_exercise_counts: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    """Build entity-level attempt summaries for the concentration chart."""
    if level not in {"exercise", "activity", "objective", "module"}:
        raise ValueError(f"Unsupported concentration level: {level}")
    lf = _as_lazy(fact)
    if work_modes:
        lf = lf.filter(pl.col("work_mode").is_in(list(work_modes)))
    if level == "exercise":
        grouped = (
            lf.filter(pl.col("exercise_id").is_not_null())
            .group_by("exercise_id")
            .agg(
                pl.len().cast(pl.Int64).alias("attempts"),
                pl.lit(1, dtype=pl.Int64).alias("fact_contained_exercises"),
                pl.col("module_code").drop_nulls().first().alias("module_code_ref"),
                pl.col("module_label").drop_nulls().first().alias("module_label_ref"),
                pl.col("work_mode").drop_nulls().unique().sort().alias("work_modes_seen"),
            )
            .rename({"exercise_id": "id"})
            .with_columns(pl.col("id").cast(pl.Utf8).alias("label"))
        )
    elif level == "activity":
        grouped = (
            lf.filter(pl.col("activity_id").is_not_null())
            .group_by(["activity_id", "activity_label"])
            .agg(
                pl.len().cast(pl.Int64).alias("attempts"),
                pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("fact_contained_exercises"),
                pl.col("module_code").drop_nulls().first().alias("module_code_ref"),
                pl.col("module_label").drop_nulls().first().alias("module_label_ref"),
                pl.col("work_mode").drop_nulls().unique().sort().alias("work_modes_seen"),
            )
            .rename({"activity_id": "id", "activity_label": "label"})
        )
    elif level == "objective":
        grouped = (
            lf.filter(pl.col("objective_id").is_not_null())
            .group_by(["objective_id", "objective_label"])
            .agg(
                pl.len().cast(pl.Int64).alias("attempts"),
                pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("fact_contained_exercises"),
                pl.col("module_code").drop_nulls().first().alias("module_code_ref"),
                pl.col("module_label").drop_nulls().first().alias("module_label_ref"),
                pl.col("work_mode").drop_nulls().unique().sort().alias("work_modes_seen"),
            )
            .rename({"objective_id": "id", "objective_label": "label"})
        )
    else:
        grouped = (
            lf.filter(pl.col("module_code").is_not_null())
            .group_by(["module_code", "module_label"])
            .agg(
                pl.len().cast(pl.Int64).alias("attempts"),
                pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("fact_contained_exercises"),
                pl.col("module_code").drop_nulls().first().alias("module_code_ref"),
                pl.col("module_label").drop_nulls().first().alias("module_label_ref"),
                pl.col("work_mode").drop_nulls().unique().sort().alias("work_modes_seen"),
            )
            .rename({"module_code": "id", "module_label": "label"})
        )

    summary = (
        grouped.collect()
        .join(contained_exercise_counts[level], on="id", how="left", suffix="_catalog")
        .with_columns(
            _resolved_label_expr(level).alias("label"),
            pl.coalesce(
                [
                    pl.col("contained_exercises"),
                    pl.col("fact_contained_exercises"),
                    pl.lit(1, dtype=pl.Int64),
                ]
            )
            .cast(pl.Int64)
            .alias("contained_exercises"),
        )
        .select(["label", "id", "attempts", "contained_exercises"])
    )
    if summary.height == 0:
        return summary.with_columns(pl.lit(None, dtype=pl.Float64).alias("attempt_share"))
    total_attempts = int(summary["attempts"].sum())
    sorted_summary = (
        summary.with_columns(
            (pl.col("attempts") / total_attempts).cast(pl.Float64).alias("attempt_share"),
        )
        .sort(["attempts", "label", "id"], descending=[True, False, False])
    )
    if level == "module":
        return sorted_summary.with_row_index("bucket_order", offset=1).with_columns(
            pl.col("id").alias("bucket_key"),
            pl.col("label").alias("bucket_label"),
        )
    return assign_rank_buckets(sorted_summary)


def build_global_student_attempt_summary(
    fact: pl.DataFrame | pl.LazyFrame,
    *,
    work_modes: tuple[str, ...],
) -> pl.DataFrame:
    """Build a student-level attempt summary for global student concentration."""
    lf = _as_lazy(fact).filter(_non_blank_user_filter_expr()).with_columns(
        _clean_user_id_expr().alias("user_id")
    )
    if work_modes:
        lf = lf.filter(pl.col("work_mode").is_in(list(work_modes)))

    summary = (
        lf.group_by("user_id")
        .agg(
            pl.len().cast(pl.Int64).alias("attempts"),
            pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_exercises"),
            pl.col("activity_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_activities"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_objectives"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("unique_modules"),
        )
        .collect()
    )
    if summary.height == 0:
        return summary.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("label"),
            pl.lit(None, dtype=pl.Utf8).alias("id"),
            pl.lit(None, dtype=pl.Float64).alias("attempt_share"),
            pl.lit(None, dtype=pl.Utf8).alias("bucket_key"),
            pl.lit(None, dtype=pl.Utf8).alias("bucket_label"),
            pl.lit(None, dtype=pl.Int64).alias("bucket_order"),
        )

    total_attempts = int(summary["attempts"].sum())
    sorted_summary = (
        summary.with_columns(
            pl.col("user_id").alias("label"),
            pl.col("user_id").alias("id"),
            (pl.col("attempts") / total_attempts).cast(pl.Float64).alias("attempt_share"),
        )
        .sort(["attempts", "user_id"], descending=[True, False])
    )
    return assign_rank_buckets(sorted_summary)


def build_within_entity_student_concentration(
    fact: pl.DataFrame | pl.LazyFrame,
    *,
    level: str,
    work_modes: tuple[str, ...],
    contained_exercise_counts: dict[str, pl.DataFrame],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build chart and drilldown frames for within-entity student concentration."""
    if level not in {"exercise", "activity", "objective", "module"}:
        raise ValueError(f"Unsupported within-entity concentration level: {level}")

    entity_summary = build_entity_attempt_summary(
        fact,
        level=level,
        work_modes=work_modes,
        contained_exercise_counts=contained_exercise_counts,
    ).select(["label", "id", "attempts", "contained_exercises"])
    if entity_summary.height == 0:
        empty_bucket_summary = pl.DataFrame(
            schema={
                "bucket_key": pl.Utf8,
                "bucket_label": pl.Utf8,
                "bucket_order": pl.Int64,
                "attempts": pl.Int64,
                "attempt_share": pl.Float64,
                "entity_count": pl.Int64,
            }
        )
        empty_drilldown = pl.DataFrame(
            schema={
                "label": pl.Utf8,
                "id": pl.Utf8,
                "attempts": pl.Int64,
                "selected_bucket_attempt_share": pl.Float64,
                "top_10_students_share": pl.Float64,
                "unique_students": pl.Int64,
                "contained_exercises": pl.Int64,
                "bucket_key": pl.Utf8,
                "bucket_label": pl.Utf8,
                "bucket_order": pl.Int64,
            }
        )
        return empty_bucket_summary, empty_drilldown

    lf = _as_lazy(fact).filter(_non_blank_user_filter_expr()).with_columns(
        _clean_user_id_expr().alias("user_id")
    )
    if work_modes:
        lf = lf.filter(pl.col("work_mode").is_in(list(work_modes)))

    if level == "exercise":
        student_entity = (
            lf.filter(pl.col("exercise_id").is_not_null())
            .group_by(["exercise_id", "user_id"])
            .agg(pl.len().cast(pl.Int64).alias("attempts"))
            .rename({"exercise_id": "id"})
            .collect()
        )
    elif level == "activity":
        student_entity = (
            lf.filter(pl.col("activity_id").is_not_null())
            .group_by(["activity_id", "user_id"])
            .agg(pl.len().cast(pl.Int64).alias("attempts"))
            .rename({"activity_id": "id"})
            .collect()
        )
    elif level == "objective":
        student_entity = (
            lf.filter(pl.col("objective_id").is_not_null())
            .group_by(["objective_id", "user_id"])
            .agg(pl.len().cast(pl.Int64).alias("attempts"))
            .rename({"objective_id": "id"})
            .collect()
        )
    else:
        student_entity = (
            lf.filter(pl.col("module_code").is_not_null())
            .group_by(["module_code", "user_id"])
            .agg(pl.len().cast(pl.Int64).alias("attempts"))
            .rename({"module_code": "id"})
            .collect()
        )

    if student_entity.height == 0:
        empty_bucket_summary = pl.DataFrame(
            schema={
                "bucket_key": pl.Utf8,
                "bucket_label": pl.Utf8,
                "bucket_order": pl.Int64,
                "attempts": pl.Int64,
                "attempt_share": pl.Float64,
                "entity_count": pl.Int64,
            }
        )
        empty_drilldown = pl.DataFrame(
            schema={
                "label": pl.Utf8,
                "id": pl.Utf8,
                "attempts": pl.Int64,
                "selected_bucket_attempt_share": pl.Float64,
                "top_10_students_share": pl.Float64,
                "unique_students": pl.Int64,
                "contained_exercises": pl.Int64,
                "bucket_key": pl.Utf8,
                "bucket_label": pl.Utf8,
                "bucket_order": pl.Int64,
            }
        )
        return empty_bucket_summary, empty_drilldown

    ranked = (
        student_entity.sort(["id", "attempts", "user_id"], descending=[False, True, False])
        .with_columns(
            pl.col("user_id").cum_count().over("id").cast(pl.Int64).alias("student_rank"),
            pl.col("user_id").count().over("id").cast(pl.Int64).alias("entity_student_count"),
        )
        .with_columns(_bucket_order_expr("student_rank", "entity_student_count").alias("bucket_order"))
        .with_columns(
            pl.concat_str([pl.lit("decile_"), pl.col("bucket_order").cast(pl.Utf8)]).alias("bucket_key"),
            _bucket_label_expr("bucket_order").alias("bucket_label"),
        )
    )

    entity_meta = entity_summary.select(["label", "id", "attempts", "contained_exercises"])
    bucket_by_entity = (
        ranked.group_by(["id", "bucket_key", "bucket_label", "bucket_order"])
        .agg(pl.col("attempts").sum().cast(pl.Int64).alias("bucket_attempts"))
        .join(entity_meta, on="id", how="left")
        .join(
            ranked.group_by("id")
            .agg(pl.col("user_id").n_unique().cast(pl.Int64).alias("unique_students")),
            on="id",
            how="left",
        )
        .with_columns(
            (pl.col("bucket_attempts") / pl.col("attempts")).cast(pl.Float64).alias(
                "selected_bucket_attempt_share"
            )
        )
    )
    top_10_share = (
        bucket_by_entity.filter(pl.col("bucket_order") == 1)
        .select(
            "id",
            pl.col("selected_bucket_attempt_share")
            .cast(pl.Float64)
            .alias("top_10_students_share"),
        )
    )
    drilldown = (
        bucket_by_entity.join(top_10_share, on="id", how="left")
        .with_columns(pl.col("top_10_students_share").fill_null(0.0).cast(pl.Float64))
        .select(
            [
                "label",
                "id",
                "attempts",
                "selected_bucket_attempt_share",
                "top_10_students_share",
                "unique_students",
                "contained_exercises",
                "bucket_key",
                "bucket_label",
                "bucket_order",
            ]
        )
    )
    total_attempts = int(bucket_by_entity["bucket_attempts"].sum()) if bucket_by_entity.height else 0
    bucket_summary = (
        bucket_by_entity.group_by(["bucket_key", "bucket_label", "bucket_order"])
        .agg(
            pl.col("bucket_attempts").sum().cast(pl.Int64).alias("attempts"),
            pl.col("id").n_unique().cast(pl.Int64).alias("entity_count"),
        )
        .with_columns(
            pl.when(pl.lit(total_attempts) > 0)
            .then(pl.col("attempts") / pl.lit(total_attempts))
            .otherwise(pl.lit(0.0))
            .cast(pl.Float64)
            .alias("attempt_share")
        )
        .sort("bucket_order")
    )
    full_buckets = pl.DataFrame(
        {
            "bucket_key": [f"decile_{idx}" for idx in range(1, 11)],
            "bucket_label": list(DECILE_LABELS),
            "bucket_order": list(range(1, 11)),
        }
    )
    bucket_summary = (
        full_buckets.join(bucket_summary, on=["bucket_key", "bucket_label", "bucket_order"], how="left")
        .with_columns(
            pl.col("attempts").fill_null(0).cast(pl.Int64),
            pl.col("attempt_share").fill_null(0.0).cast(pl.Float64),
            pl.col("entity_count").fill_null(0).cast(pl.Int64),
        )
        .sort("bucket_order")
    )
    return bucket_summary, drilldown


def build_bucket_summary(entity_summary: pl.DataFrame, *, level: str) -> pl.DataFrame:
    """Aggregate entity summaries into chart bars."""
    if entity_summary.height == 0:
        return pl.DataFrame(
            schema={
                "bucket_key": pl.Utf8,
                "bucket_label": pl.Utf8,
                "bucket_order": pl.Int64,
                "attempts": pl.Int64,
                "attempt_share": pl.Float64,
                "entity_count": pl.Int64,
            }
        )
    if level == "module":
        return (
            entity_summary.select(
                [
                    "bucket_key",
                    "bucket_label",
                    "bucket_order",
                    "attempts",
                    "attempt_share",
                ]
            )
            .with_columns(pl.lit(1, dtype=pl.Int64).alias("entity_count"))
            .sort("bucket_order")
        )
    full_buckets = pl.DataFrame(
        {
            "bucket_key": [f"decile_{idx}" for idx in range(1, 11)],
            "bucket_label": list(DECILE_LABELS),
            "bucket_order": list(range(1, 11)),
        }
    )
    grouped = (
        entity_summary.group_by(["bucket_key", "bucket_label", "bucket_order"])
        .agg(
            pl.col("attempts").sum().cast(pl.Int64).alias("attempts"),
            pl.col("attempt_share").sum().cast(pl.Float64).alias("attempt_share"),
            pl.len().cast(pl.Int64).alias("entity_count"),
        )
    )
    return (
        full_buckets.join(grouped, on=["bucket_key", "bucket_label", "bucket_order"], how="left")
        .with_columns(
            pl.col("attempts").fill_null(0).cast(pl.Int64),
            pl.col("attempt_share").fill_null(0.0).cast(pl.Float64),
            pl.col("entity_count").fill_null(0).cast(pl.Int64),
        )
        .sort("bucket_order")
    )


def build_concentration_figure(
    bucket_summary: pl.DataFrame,
    *,
    level: str,
    xaxis_title: str | None = None,
    count_label: str = "Entities",
) -> go.Figure:
    """Build the attempt-concentration bar chart."""
    rows = bucket_summary.to_dicts()
    x_values = [str(row.get("bucket_label") or "") for row in rows]
    y_values = [float(row.get("attempt_share") or 0.0) * 100.0 for row in rows]
    customdata = [
        [
            str(row.get("bucket_key") or ""),
            str(row.get("bucket_label") or ""),
            int(row.get("attempts") or 0),
            int(row.get("entity_count") or 0),
        ]
        for row in rows
    ]
    figure = go.Figure(
        go.Bar(
            x=x_values,
            y=y_values,
            customdata=customdata,
            text=[f"{value:.1f}%" if value > 0 else "" for value in y_values],
            textposition="outside",
            marker={"color": "#2f5d50", "line": {"color": "#1c342c", "width": 1}},
            hovertemplate=(
                "<b>%{customdata[1]}</b><br>"
                "Attempt share: %{y:.1f}%<br>"
                "Attempts: %{customdata[2]:,}<br>"
                f"{count_label}: " + "%{customdata[3]:,}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        height=420,
        margin={"l": 48, "r": 24, "t": 36, "b": 64},
        clickmode="event+select",
        dragmode=False,
        yaxis_title="Attempt share (%)",
        xaxis_title=xaxis_title or ("Ranked entity bucket" if level != "module" else "Modules"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=False, categoryorder="array", categoryarray=x_values)
    figure.update_yaxes(showgrid=True, rangemode="tozero")
    return figure


def extract_selected_bucket(event: object) -> dict[str, str] | None:
    """Extract the selected bucket from a Plotly selection event."""
    if not isinstance(event, dict):
        return None
    selection = event.get("selection")
    if not isinstance(selection, dict):
        return None
    points = selection.get("points")
    if not isinstance(points, list) or not points:
        return None
    point = points[0]
    if not isinstance(point, dict):
        return None
    customdata = point.get("customdata")
    if isinstance(customdata, (list, tuple)) and len(customdata) >= 2:
        bucket_key = str(customdata[0] or "").strip()
        bucket_label = str(customdata[1] or "").strip()
        if bucket_key:
            return {"bucket_key": bucket_key, "bucket_label": bucket_label}
    return None

"""Student objective-level radar helpers built from fact attempts and catalog metadata."""

from __future__ import annotations

from math import ceil
from pathlib import Path
from random import Random

import plotly.graph_objects as go
import polars as pl

from visu2.loaders import load_learning_catalog


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def _clean_text_expr(column_name: str) -> pl.Expr:
    return (
        pl.col(column_name)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .replace("", None)
    )


def load_objective_catalog(path: Path) -> pl.DataFrame:
    """Load canonical objective metadata with exercise totals and stable module order."""
    catalog = load_learning_catalog(path)
    rows: list[dict[str, object]] = []
    for module in catalog.get("modules") or []:
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("id") or "").strip()
        module_code = str(module.get("code") or "").strip()
        if not module_code:
            continue
        module_title = module.get("title") or {}
        module_label = str(module_title.get("short") or module_code).strip() or module_code
        for objective_order, objective in enumerate(module.get("objectives") or [], start=1):
            if not isinstance(objective, dict):
                continue
            objective_id = str(objective.get("id") or "").strip()
            objective_code = str(objective.get("code") or objective_id).strip() or objective_id
            if not objective_id:
                continue
            objective_title = objective.get("title") or {}
            objective_label = str(objective_title.get("short") or objective_code).strip() or objective_code
            objective_exercise_ids: set[str] = set()
            for activity in objective.get("activities") or []:
                if not isinstance(activity, dict):
                    continue
                for exercise_id in activity.get("exercise_ids") or []:
                    if isinstance(exercise_id, str) and exercise_id.strip():
                        objective_exercise_ids.add(exercise_id.strip())
            rows.append(
                {
                    "module_id": module_id or None,
                    "module_code": module_code,
                    "module_label": module_label,
                    "objective_id": objective_id,
                    "objective_code": objective_code or objective_id,
                    "objective_label": objective_label,
                    "objective_order": objective_order,
                    "objective_exercise_total": len(objective_exercise_ids),
                }
            )
    return (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            schema={
                "module_id": pl.Utf8,
                "module_code": pl.Utf8,
                "module_label": pl.Utf8,
                "objective_id": pl.Utf8,
                "objective_code": pl.Utf8,
                "objective_label": pl.Utf8,
                "objective_order": pl.Int64,
                "objective_exercise_total": pl.Int64,
            }
        )
    )


def build_student_selection_profiles(
    fact: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    """Aggregate lightweight per-student profiles for page selection."""
    return (
        _as_lazy(fact)
        .with_columns(
            _clean_text_expr("user_id").alias("user_id"),
            _clean_text_expr("module_code").alias("module_code"),
        )
        .filter(pl.col("user_id").is_not_null())
        .group_by("user_id")
        .agg(
            pl.len().cast(pl.Int64).alias("total_attempts"),
            pl.col("created_at").min().alias("first_attempt_at"),
            pl.col("created_at").max().alias("last_attempt_at"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("unique_modules"),
            pl.col("module_code").drop_nulls().unique().alias("attempted_modules"),
        )
        .with_columns(
            ((pl.col("total_attempts") > 0) & (pl.col("unique_modules") > 0)).alias(
                "eligible_for_selection"
            )
        )
        .sort(["total_attempts", "user_id"], descending=[True, False])
        .collect()
    )


def select_students_near_attempt_target(
    profiles: pl.DataFrame,
    target_attempts: int,
    tolerance_ratio: float = 0.10,
    max_students: int = 1,
    seed: int | None = None,
) -> list[str]:
    """Sample up to ``max_students`` eligible students near the requested attempt count."""
    if profiles.height == 0:
        return []

    target = max(1, int(target_attempts))
    tolerance = max(0.0, float(tolerance_ratio))
    limit = max(1, int(max_students))
    lower = int(target * (1.0 - tolerance))
    upper = int(ceil(target * (1.0 + tolerance)))

    candidates = (
        profiles.filter(
            pl.col("eligible_for_selection")
            & (pl.col("total_attempts") >= lower)
            & (pl.col("total_attempts") <= upper)
        )
        .select("user_id")
        .to_series()
        .to_list()
    )
    normalized = sorted({str(user_id).strip() for user_id in candidates if str(user_id).strip()})
    if len(normalized) <= limit:
        return normalized

    rng = Random(seed)
    return sorted(rng.sample(normalized, k=limit))


def select_student_by_id(
    profiles: pl.DataFrame,
    user_id: str,
) -> str | None:
    """Return one eligible student matching an explicit identifier."""
    normalized = str(user_id or "").strip()
    if not normalized or profiles.height == 0:
        return None
    matches = profiles.filter(
        pl.col("eligible_for_selection")
        & (pl.col("user_id").cast(pl.Utf8) == normalized)
    )
    if matches.height == 0:
        return None
    return normalized


def build_student_module_options(
    fact: pl.DataFrame | pl.LazyFrame,
    objective_catalog: pl.DataFrame,
    user_id: str,
) -> pl.DataFrame:
    """Return attempted modules for one student, restricted to catalog-backed modules."""
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id or objective_catalog.height == 0:
        return pl.DataFrame(
            schema={
                "module_code": pl.Utf8,
                "module_label": pl.Utf8,
                "attempts": pl.Int64,
                "objectives_attempted": pl.Int64,
            }
        )

    catalog_modules = objective_catalog.select(["module_code", "module_label"]).unique(
        subset=["module_code"], keep="first"
    )

    return (
        _as_lazy(fact)
        .with_columns(
            _clean_text_expr("user_id").alias("user_id"),
            _clean_text_expr("module_code").alias("module_code"),
            _clean_text_expr("module_label").alias("module_label"),
            _clean_text_expr("objective_id").alias("objective_id"),
        )
        .filter((pl.col("user_id") == normalized_user_id) & pl.col("module_code").is_not_null())
        .group_by("module_code")
        .agg(
            pl.col("module_label").drop_nulls().first().alias("module_label"),
            pl.len().cast(pl.Int64).alias("attempts"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("objectives_attempted"),
        )
        .join(catalog_modules.lazy(), on="module_code", how="inner", suffix="_catalog")
        .with_columns(
            pl.coalesce([pl.col("module_label_catalog"), pl.col("module_label"), pl.col("module_code")]).alias(
                "module_label"
            )
        )
        .select(["module_code", "module_label", "attempts", "objectives_attempted"])
        .sort(["attempts", "module_code"], descending=[True, False])
        .collect()
    )


def build_student_objective_summary(
    fact: pl.DataFrame | pl.LazyFrame,
    objective_catalog: pl.DataFrame,
    *,
    user_id: str,
    module_code: str,
) -> pl.DataFrame:
    """Build one row per catalog objective for the selected student and module."""
    normalized_user_id = str(user_id or "").strip()
    normalized_module_code = str(module_code or "").strip()
    if not normalized_user_id or not normalized_module_code or objective_catalog.height == 0:
        return pl.DataFrame()

    catalog_slice = (
        objective_catalog.lazy()
        .filter(pl.col("module_code") == normalized_module_code)
        .sort(["objective_order", "objective_code", "objective_id"])
        .collect()
    )
    if catalog_slice.height == 0:
        return pl.DataFrame()

    observed = (
        _as_lazy(fact)
        .with_columns(
            _clean_text_expr("user_id").alias("user_id"),
            _clean_text_expr("module_code").alias("module_code"),
            _clean_text_expr("objective_id").alias("objective_id"),
            _clean_text_expr("exercise_id").alias("exercise_id"),
        )
        .filter(
            (pl.col("user_id") == normalized_user_id)
            & (pl.col("module_code") == normalized_module_code)
            & pl.col("objective_id").is_not_null()
        )
        .group_by("objective_id")
        .agg(
            pl.len().cast(pl.Int64).alias("attempts"),
            pl.col("exercise_id").drop_nulls().n_unique().cast(pl.Int64).alias("distinct_exercises_attempted"),
            (pl.col("attempt_number") == 1).sum().cast(pl.Int64).alias("first_attempt_count"),
            pl.col("data_correct").cast(pl.Float64, strict=False).mean().alias("success_rate_all_attempts"),
        )
        .collect()
    )

    return (
        catalog_slice.join(observed, on="objective_id", how="left")
        .with_columns(
            pl.col("attempts").fill_null(0).cast(pl.Int64),
            pl.col("distinct_exercises_attempted").fill_null(0).cast(pl.Int64),
            pl.col("first_attempt_count").fill_null(0).cast(pl.Int64),
        )
        .with_columns(
            pl.when(pl.col("objective_exercise_total") > 0)
            .then(
                pl.col("distinct_exercises_attempted").cast(pl.Float64)
                / pl.col("objective_exercise_total").cast(pl.Float64)
            )
            .otherwise(0.0)
            .alias("coverage_rate"),
            (pl.col("attempts") > 0).alias("has_attempts"),
        )
        .select(
            [
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_code",
                "objective_label",
                "objective_order",
                "objective_exercise_total",
                "distinct_exercises_attempted",
                "coverage_rate",
                "success_rate_all_attempts",
                "attempts",
                "first_attempt_count",
                "has_attempts",
            ]
        )
        .sort(["objective_order", "objective_code", "objective_id"])
    )


def summarize_student_module_profile(summary: pl.DataFrame) -> dict[str, float | int]:
    """Compute small summary metrics for the selected student/module slice."""
    if summary.height == 0:
        return {
            "objectives_total": 0,
            "objectives_attempted": 0,
            "module_attempts": 0,
            "module_distinct_exercises_attempted": 0,
            "module_exercise_total": 0,
            "module_coverage_rate": 0.0,
            "mean_success_rate": 0.0,
        }

    attempted = summary.filter(pl.col("has_attempts"))
    objectives_total = int(summary.height)
    objectives_attempted = int(attempted.height)
    module_attempts = int(summary["attempts"].sum())
    distinct_attempted = int(summary["distinct_exercises_attempted"].sum())
    module_exercise_total = int(summary["objective_exercise_total"].sum())
    module_coverage_rate = (
        float(distinct_attempted) / float(module_exercise_total) if module_exercise_total > 0 else 0.0
    )
    mean_success_rate = (
        float(attempted["success_rate_all_attempts"].mean())
        if attempted.height > 0 and attempted["success_rate_all_attempts"].drop_nulls().len() > 0
        else 0.0
    )
    return {
        "objectives_total": objectives_total,
        "objectives_attempted": objectives_attempted,
        "module_attempts": module_attempts,
        "module_distinct_exercises_attempted": distinct_attempted,
        "module_exercise_total": module_exercise_total,
        "module_coverage_rate": module_coverage_rate,
        "mean_success_rate": mean_success_rate,
    }


def build_student_objective_spider_figure(
    summary: pl.DataFrame,
    *,
    student_id: str,
    module_code: str,
    module_label: str | None = None,
) -> go.Figure:
    """Render the student objective radar chart."""
    module_display = str(module_label or module_code or "").strip() or str(module_code or "").strip()
    if summary.height == 0:
        figure = go.Figure()
        figure.add_annotation(
            text="No objective data available for the selected student/module.",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font={"family": "IBM Plex Sans, Arial, sans-serif", "size": 15, "color": "#4D4A46"},
        )
        figure.update_layout(
            paper_bgcolor="#FBF8F2",
            plot_bgcolor="#FBF8F2",
            font={"family": "IBM Plex Sans, Arial, sans-serif", "size": 14, "color": "#1E1B18"},
            margin={"l": 40, "r": 40, "t": 80, "b": 40},
        )
        return figure

    ordered = summary.sort(["objective_order", "objective_code", "objective_id"])
    theta = [
        str(row.get("objective_code") or row.get("objective_id") or "")
        for row in ordered.to_dicts()
    ]
    coverage_r = [max(0.0, min(100.0, float(value or 0.0) * 100.0)) for value in ordered["coverage_rate"].to_list()]
    success_r = [
        None if value is None else max(0.0, min(100.0, float(value) * 100.0))
        for value in ordered["success_rate_all_attempts"].to_list()
    ]
    customdata = [
        [
            str(row.get("objective_label") or row.get("objective_code") or row.get("objective_id") or ""),
            int(row.get("distinct_exercises_attempted") or 0),
            int(row.get("objective_exercise_total") or 0),
            int(row.get("attempts") or 0),
            "n/a"
            if row.get("success_rate_all_attempts") is None
            else f"{float(row.get('success_rate_all_attempts')) * 100:.1f}%",
            f"{float(row.get('coverage_rate') or 0.0) * 100:.1f}%",
        ]
        for row in ordered.to_dicts()
    ]

    figure = go.Figure()
    figure.add_trace(
        go.Scatterpolar(
            r=coverage_r,
            theta=theta,
            mode="lines+markers",
            name="Coverage %",
            line={"color": "#C97346", "width": 2.8},
            marker={"color": "#C97346", "size": 8},
            fill="toself",
            fillcolor="rgba(201, 115, 70, 0.18)",
            customdata=customdata,
            hovertemplate=(
                "<b>%{theta}</b><br>"
                "%{customdata[0]}<br>"
                "Coverage: %{customdata[5]}<br>"
                "Distinct exercises attempted: %{customdata[1]} / %{customdata[2]}<br>"
                "Total attempts: %{customdata[3]}<extra>Coverage %</extra>"
            ),
        )
    )
    figure.add_trace(
        go.Scatterpolar(
            r=success_r,
            theta=theta,
            mode="lines+markers",
            name="Success rate",
            line={"color": "#2D7F6E", "width": 3.0},
            marker={"color": "#2D7F6E", "size": 8},
            fill="toself",
            fillcolor="rgba(45, 127, 110, 0.16)",
            connectgaps=False,
            customdata=customdata,
            hovertemplate=(
                "<b>%{theta}</b><br>"
                "%{customdata[0]}<br>"
                "Success rate: %{customdata[4]}<br>"
                "Coverage: %{customdata[5]}<br>"
                "Distinct exercises attempted: %{customdata[1]} / %{customdata[2]}<br>"
                "Total attempts: %{customdata[3]}<extra>Success rate</extra>"
            ),
        )
    )
    figure.update_layout(
        title={
            "text": f"{student_id} · {module_display}",
            "x": 0.02,
            "xanchor": "left",
            "font": {"family": "Fraunces, Georgia, serif", "size": 22, "color": "#1E1B18"},
        },
        paper_bgcolor="#FBF8F2",
        plot_bgcolor="#FBF8F2",
        font={"family": "IBM Plex Sans, Arial, sans-serif", "size": 14, "color": "#1E1B18"},
        margin={"l": 48, "r": 48, "t": 88, "b": 36},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.08,
            "xanchor": "left",
            "x": 0.02,
            "bgcolor": "rgba(251, 248, 242, 0.85)",
            "bordercolor": "rgba(53, 45, 34, 0.08)",
            "borderwidth": 1,
        },
        polar={
            "bgcolor": "#FBF8F2",
            "radialaxis": {
                "visible": True,
                "range": [0, 100],
                "tickvals": [0, 25, 50, 75, 100],
                "ticktext": ["0%", "25%", "50%", "75%", "100%"],
                "gridcolor": "#D8CFBF",
                "linecolor": "#BDAE97",
                "tickfont": {"size": 11, "color": "#6B635A"},
                "angle": 90,
            },
            "angularaxis": {
                "direction": "clockwise",
                "rotation": 90,
                "gridcolor": "#E5DCCD",
                "linecolor": "#E5DCCD",
                "tickfont": {"size": 12, "color": "#3E3A34"},
            },
        },
    )
    return figure

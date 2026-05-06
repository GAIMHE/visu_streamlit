"""Student history-length distribution helpers."""

from __future__ import annotations

import math
from typing import Any

import plotly.graph_objects as go
import polars as pl


def build_student_interaction_counts(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Return one row per student with total interaction count."""
    lf = fact.lazy() if isinstance(fact, pl.DataFrame) else fact
    return (
        lf.filter(
            pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
        )
        .group_by("user_id")
        .agg(pl.len().alias("interactions"))
        .sort(["interactions", "user_id"], descending=[True, False])
        .collect()
    )


def filter_student_interaction_counts(
    counts: pl.DataFrame,
    *,
    max_interactions: int,
) -> pl.DataFrame:
    """Filter out students above the selected interaction-count threshold."""
    threshold = max(1, int(max_interactions))
    return counts.filter(pl.col("interactions") <= threshold)


def summarize_student_interaction_counts(
    counts: pl.DataFrame,
    *,
    max_interactions: int,
) -> dict[str, Any]:
    """Summarize retained and excluded students for a thresholded distribution."""
    filtered = filter_student_interaction_counts(counts, max_interactions=max_interactions)
    total_students = counts.height
    retained_students = filtered.height
    excluded_students = total_students - retained_students
    if retained_students == 0:
        return {
            "total_students": total_students,
            "retained_students": 0,
            "excluded_students": excluded_students,
            "excluded_share": excluded_students / total_students if total_students else 0.0,
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "q25": None,
            "q75": None,
        }
    return {
        "total_students": total_students,
        "retained_students": retained_students,
        "excluded_students": excluded_students,
        "excluded_share": excluded_students / total_students if total_students else 0.0,
        "mean": float(filtered["interactions"].mean() or 0.0),
        "median": float(filtered["interactions"].median() or 0.0),
        "min": int(filtered["interactions"].min() or 0),
        "max": int(filtered["interactions"].max() or 0),
        "q25": float(filtered["interactions"].quantile(0.25) or 0.0),
        "q75": float(filtered["interactions"].quantile(0.75) or 0.0),
    }


def _axis_upper_bound(max_observed: int, max_interactions: int) -> int:
    """Return a compact upper x-axis bound for the thresholded histogram."""
    upper = max(1, min(int(max_observed), int(max_interactions)))
    if upper <= 20:
        return upper
    magnitude = 10 ** int(math.log10(upper))
    step = max(10, magnitude // 2)
    return int(math.ceil(upper / step) * step)


def build_student_interaction_histogram(
    counts: pl.DataFrame,
    *,
    max_interactions: int,
    bin_count: int = 60,
) -> go.Figure:
    """Build a histogram with mean and median reference lines."""
    filtered = filter_student_interaction_counts(counts, max_interactions=max_interactions)
    summary = summarize_student_interaction_counts(counts, max_interactions=max_interactions)
    values = filtered["interactions"].to_list() if filtered.height else []

    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=values,
            nbinsx=max(5, int(bin_count)),
            marker=dict(color="#74a9cf", line=dict(color="#17221b", width=0.8)),
            opacity=0.86,
            name="Students",
            hovertemplate="Interactions: %{x}<br>Students: %{y}<extra></extra>",
        )
    )

    line_specs = (
        ("Mean", summary["mean"], "#D55E00", "dash"),
        ("Median", summary["median"], "#1e7a52", "dot"),
    )
    for label, value, color, dash in line_specs:
        if value is None:
            continue
        fig.add_vline(
            x=float(value),
            line_color=color,
            line_dash=dash,
            line_width=2,
        )
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="lines",
                line=dict(color=color, dash=dash, width=2),
                name=f"{label}: {float(value):.1f}",
                showlegend=True,
            )
        )

    x_upper = _axis_upper_bound(int(summary["max"] or 1), max_interactions)
    fig.update_layout(
        title="Distribution of Student Interaction Lengths",
        xaxis_title="Number of interactions",
        yaxis_title="Number of students",
        bargap=0.04,
        legend=dict(orientation="v", yanchor="top", y=0.98, xanchor="right", x=0.98),
        margin=dict(l=20, r=20, t=64, b=50),
    )
    fig.update_xaxes(range=[0, x_upper], automargin=True)
    fig.update_yaxes(automargin=True)
    return fig

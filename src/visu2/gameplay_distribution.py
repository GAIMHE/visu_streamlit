"""Gameplay distribution summaries and figures."""

from __future__ import annotations

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

GAMEPLAY_TYPE_COLUMN = "exercise_type"
REQUIRED_COLUMNS: frozenset[str] = frozenset({GAMEPLAY_TYPE_COLUMN, "exercise_id", "attempts"})
VALID_TOP_METRICS: frozenset[str] = frozenset({"unique_exercises", "attempts"})
GAMEPLAY_Y_AXIS_TITLE_STANDOFF = 36
GAMEPLAY_SUBPLOT_HORIZONTAL_SPACING = 0.30
GAMEPLAY_VALUE_LABEL_OFFSET_RATIO = 0.025
GAMEPLAY_VALUE_LABEL_RANGE_RATIO = 1.22


def _empty_summary() -> pl.DataFrame:
    """Return an empty summary with the expected schema."""
    return pl.DataFrame(
        schema={
            "gameplay_type": pl.Utf8,
            "unique_exercises": pl.Int64,
            "attempts": pl.Int64,
            "exercise_share": pl.Float64,
            "attempt_share": pl.Float64,
        }
    )


def build_gameplay_distribution(
    exercise_daily: pl.DataFrame | pl.LazyFrame,
    *,
    include_unknown: bool = True,
) -> pl.DataFrame:
    """Summarize unique exercises and attempts by gameplay type."""
    missing = REQUIRED_COLUMNS - frozenset(exercise_daily.collect_schema().names())
    if missing:
        raise ValueError(
            "Gameplay distribution requires columns: " + ", ".join(sorted(REQUIRED_COLUMNS))
        )

    lf = exercise_daily.lazy() if isinstance(exercise_daily, pl.DataFrame) else exercise_daily
    lf = lf.with_columns(
        pl.when(
            pl.col(GAMEPLAY_TYPE_COLUMN).is_null()
            | (pl.col(GAMEPLAY_TYPE_COLUMN).cast(pl.Utf8).str.strip_chars() == "")
        )
        .then(pl.lit("unknown"))
        .otherwise(pl.col(GAMEPLAY_TYPE_COLUMN).cast(pl.Utf8).str.strip_chars())
        .alias("gameplay_type")
    )
    if not include_unknown:
        lf = lf.filter(pl.col("gameplay_type") != "unknown")

    summary = (
        lf.group_by("gameplay_type")
        .agg(
            pl.col("exercise_id").n_unique().alias("unique_exercises"),
            pl.col("attempts").cast(pl.Int64).sum().alias("attempts"),
        )
        .collect()
    )
    if summary.height == 0:
        return _empty_summary()

    total_exercises = int(summary["unique_exercises"].sum() or 0)
    total_attempts = int(summary["attempts"].sum() or 0)
    return summary.with_columns(
        (
            pl.col("unique_exercises") / pl.lit(total_exercises)
            if total_exercises
            else pl.lit(0.0)
        ).alias("exercise_share"),
        (
            pl.col("attempts") / pl.lit(total_attempts)
            if total_attempts
            else pl.lit(0.0)
        ).alias("attempt_share"),
    ).sort(["attempts", "gameplay_type"], descending=[True, False])


def top_gameplays(summary: pl.DataFrame, *, metric: str, top_n: int) -> pl.DataFrame:
    """Return the top gameplay rows by one supported metric."""
    if metric not in VALID_TOP_METRICS:
        raise ValueError(f"Unsupported gameplay ranking metric: {metric}")
    if summary.height == 0:
        return summary
    return summary.sort([metric, "gameplay_type"], descending=[True, False]).head(
        max(1, int(top_n))
    )


def _format_count(values: list[int]) -> list[str]:
    return [f"{value:,}" for value in values]


def _value_label_positions(values: list[int]) -> list[float]:
    max_value = max(values, default=0)
    offset = max(1.0, max_value * GAMEPLAY_VALUE_LABEL_OFFSET_RATIO)
    return [float(value) + offset for value in values]


def _value_axis_range(values: list[int]) -> list[float]:
    max_value = max(values, default=0)
    return [0.0, max(1.0, max_value * GAMEPLAY_VALUE_LABEL_RANGE_RATIO)]


def _add_value_label_trace(
    fig: go.Figure,
    *,
    values: list[int],
    categories: list[str],
    row: int,
    col: int,
) -> None:
    fig.add_trace(
        go.Scatter(
            x=_value_label_positions(values),
            y=categories,
            mode="text",
            text=_format_count(values),
            textposition="middle right",
            cliponaxis=False,
            hoverinfo="skip",
            showlegend=False,
        ),
        row=row,
        col=col,
    )


def build_gameplay_distribution_figure(
    summary: pl.DataFrame,
    *,
    top_n: int = 10,
) -> go.Figure:
    """Build a side-by-side gameplay distribution figure."""
    top_by_exercises = top_gameplays(summary, metric="unique_exercises", top_n=top_n).sort(
        "unique_exercises"
    )
    top_by_attempts = top_gameplays(summary, metric="attempts", top_n=top_n).sort("attempts")

    fig = make_subplots(
        rows=1,
        cols=2,
        horizontal_spacing=GAMEPLAY_SUBPLOT_HORIZONTAL_SPACING,
        subplot_titles=(
            "Gameplay Distribution (Unique Exercises)",
            "Gameplay Distribution (Attempts)",
        ),
    )

    exercise_values = top_by_exercises["unique_exercises"].to_list()
    attempt_values = top_by_attempts["attempts"].to_list()
    exercise_categories = top_by_exercises["gameplay_type"].to_list()
    attempt_categories = top_by_attempts["gameplay_type"].to_list()
    fig.add_trace(
        go.Bar(
            x=exercise_values,
            y=exercise_categories,
            orientation="h",
            marker=dict(color="#5B84B1", line=dict(color="#17221b", width=0.8)),
            customdata=top_by_exercises["exercise_share"].to_list(),
            hovertemplate=(
                "Gameplay: %{y}<br>"
                "Unique exercises: %{x:,}<br>"
                "Exercise share: %{customdata:.2%}<extra></extra>"
            ),
            name="Unique exercises",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=attempt_values,
            y=attempt_categories,
            orientation="h",
            marker=dict(color="#F28E2B", line=dict(color="#17221b", width=0.8)),
            customdata=top_by_attempts["attempt_share"].to_list(),
            hovertemplate=(
                "Gameplay: %{y}<br>"
                "Attempts: %{x:,}<br>"
                "Attempt share: %{customdata:.2%}<extra></extra>"
            ),
            name="Attempts",
        ),
        row=1,
        col=2,
    )
    _add_value_label_trace(
        fig,
        values=exercise_values,
        categories=exercise_categories,
        row=1,
        col=1,
    )
    _add_value_label_trace(
        fig,
        values=attempt_values,
        categories=attempt_categories,
        row=1,
        col=2,
    )

    fig.update_layout(
        showlegend=False,
        height=max(440, 160 + max(top_by_exercises.height, top_by_attempts.height) * 34),
        margin=dict(l=96, r=70, t=70, b=52),
    )
    fig.update_xaxes(
        title_text="Unique exercises",
        range=_value_axis_range(exercise_values),
        automargin=True,
        row=1,
        col=1,
    )
    fig.update_xaxes(
        title_text="Attempts",
        range=_value_axis_range(attempt_values),
        automargin=True,
        row=1,
        col=2,
    )
    fig.update_yaxes(
        title_text="Gameplay type",
        title_standoff=GAMEPLAY_Y_AXIS_TITLE_STANDOFF,
        automargin=True,
        row=1,
        col=1,
    )
    fig.update_yaxes(title_text="", automargin=True, row=1, col=2)
    return fig

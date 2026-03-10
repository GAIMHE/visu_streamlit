"""Helpers for the static ZPDES transition-efficiency page."""

from __future__ import annotations

import math
from datetime import date

import plotly.graph_objects as go
import polars as pl

TRANSITION_WORK_MODE_OPTIONS = {
    "ZPDES mode": "zpdes",
    "Playlist mode": "playlist",
}

NODE_METRIC_OPTIONS = {
    "First-attempt success": "first_attempt_success_rate",
    "Activity mean exercise Elo": "activity_mean_exercise_elo",
}

NODE_METRIC_LABELS = {
    "first_attempt_success_rate": "First-attempt success",
    "activity_mean_exercise_elo": "Activity mean exercise Elo",
}

ARRIVAL_BUCKET_BEFORE = "before"
ARRIVAL_BUCKET_AFTER_CANDIDATE = "after_candidate"

ARRIVAL_EVENT_COLUMNS = [
    "created_at",
    "date_utc",
    "user_id",
    "module_id",
    "module_code",
    "module_label",
    "objective_id",
    "objective_label",
    "activity_id",
    "activity_label",
    "work_mode",
    "destination_rank",
    "first_arrival_outcome",
    "prior_attempt_count",
    "prior_before_attempt_count",
    "prior_later_attempt_count",
    "arrival_bucket_base",
]


def objective_sort_key(code: str | None) -> tuple[int, int, str]:
    """Return a stable sort key for objective codes like ``M1O2``."""
    text = str(code or "")
    if text.startswith("M") and "O" in text:
        try:
            module_num = int(text.split("O")[0][1:])
            objective_num = int(text.split("O")[1].split("A")[0])
            return (0, module_num * 1000 + objective_num, text)
        except ValueError:
            return (1, 10**9, text)
    return (1, 10**9, text)


def truncate_text(text: object, max_chars: int = 44) -> str:
    """Return a truncated label suitable for graph text rendering."""
    out = str(text or "").strip()
    if len(out) <= max_chars:
        return out
    return f"{out[: max_chars - 1].rstrip()}..."


def _is_missing(value: object) -> bool:
    """Return whether a value should be treated as missing for display."""
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return False


def format_rate(value: float | None) -> str:
    """Format a nullable rate for display."""
    if _is_missing(value):
        return "n/a"
    return f"{float(value):.1%}"


def format_int(value: int | float | None) -> str:
    """Format a nullable integer-like value for display."""
    if _is_missing(value):
        return "n/a"
    return f"{int(round(float(value))):,}"


def format_metric_value(metric: str, value: float | None) -> str:
    """Format a node metric value for hover text and captions."""
    if _is_missing(value):
        return "n/a"
    if metric == "activity_mean_exercise_elo":
        return f"{float(value):.0f}"
    return format_rate(value)


def _quadratic_curve_points(
    start: tuple[float, float],
    control: tuple[float, float],
    end: tuple[float, float],
    steps: int,
) -> list[tuple[float, float]]:
    """Sample a quadratic Bezier curve as polyline points."""
    n = max(8, int(steps))
    out: list[tuple[float, float]] = []
    x0, y0 = start
    cx, cy = control
    x1, y1 = end
    for idx in range(n + 1):
        t = idx / n
        mt = 1.0 - t
        x = (mt * mt * x0) + (2.0 * mt * t * cx) + (t * t * x1)
        y = (mt * mt * y0) + (2.0 * mt * t * cy) + (t * t * y1)
        out.append((x, y))
    return out


def attach_transition_metric_to_nodes(
    nodes: pl.DataFrame,
    agg_activity_daily: pl.DataFrame | pl.LazyFrame,
    agg_activity_elo: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date,
    end_date: date,
    metric: str,
) -> pl.DataFrame:
    """Attach the selected activity metric to ZPDES activity nodes."""
    nodes_frame = nodes.clone()
    if metric == "activity_mean_exercise_elo":
        elo_frame = agg_activity_elo.collect() if isinstance(agg_activity_elo, pl.LazyFrame) else agg_activity_elo
        metric_frame = (
            elo_frame.filter(pl.col("module_code") == module_code)
            .select(["activity_id", "activity_mean_exercise_elo"])
            .rename(
                {
                    "activity_id": "node_id",
                    "activity_mean_exercise_elo": "transition_metric_value",
                }
            )
        )
    elif metric == "first_attempt_success_rate":
        activity_frame = (
            agg_activity_daily.collect() if isinstance(agg_activity_daily, pl.LazyFrame) else agg_activity_daily
        )
        metric_frame = (
            activity_frame.filter(
                (pl.col("module_code") == module_code)
                & (pl.col("date_utc") >= pl.lit(start_date))
                & (pl.col("date_utc") <= pl.lit(end_date))
            )
            .group_by("activity_id")
            .agg(
                pl.sum("first_attempt_count").cast(pl.Float64).alias("_weight"),
                (
                    pl.col("first_attempt_success_rate").fill_null(0.0)
                    * pl.col("first_attempt_count").cast(pl.Float64)
                )
                .sum()
                .cast(pl.Float64)
                .alias("_weighted_success"),
            )
            .with_columns(
                pl.when(pl.col("_weight") > 0)
                .then(pl.col("_weighted_success") / pl.col("_weight"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("transition_metric_value")
            )
            .rename({"activity_id": "node_id"})
            .select(["node_id", "transition_metric_value"])
        )
    else:
        raise ValueError(f"Unsupported transition node metric: {metric}")

    return (
        nodes_frame.join(metric_frame, on="node_id", how="left")
        .with_columns(
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("transition_metric_value"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("transition_metric_value")
        )
    )


def attach_arrival_cohort_metrics_to_nodes(
    nodes: pl.DataFrame,
    arrival_events: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date,
    end_date: date,
    work_mode: str,
    later_attempt_threshold: int,
) -> pl.DataFrame:
    """Attach before/after first-arrival cohort metrics to activity nodes."""
    events_frame = arrival_events.collect() if isinstance(arrival_events, pl.LazyFrame) else arrival_events
    scoped = events_frame.filter(
        (pl.col("module_code") == module_code)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
        & (pl.col("work_mode") == work_mode)
    )
    if scoped.height == 0:
        return nodes.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("before_students"),
            pl.lit(None, dtype=pl.Float64).alias("before_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("before_previous_attempts"),
            pl.lit(None, dtype=pl.Int64).alias("after_students"),
            pl.lit(None, dtype=pl.Float64).alias("after_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("after_previous_attempts"),
        )

    before_metrics = (
        scoped.filter(pl.col("arrival_bucket_base") == ARRIVAL_BUCKET_BEFORE)
        .group_by("activity_id")
        .agg(
            pl.len().cast(pl.Int64).alias("before_students"),
            pl.col("first_arrival_outcome").cast(pl.Float64).mean().alias("before_success_rate"),
            pl.sum("prior_attempt_count").cast(pl.Int64).alias("before_previous_attempts"),
        )
        .rename({"activity_id": "node_id"})
    )
    threshold = max(1, int(later_attempt_threshold))
    after_metrics = (
        scoped.filter(
            (pl.col("arrival_bucket_base") == ARRIVAL_BUCKET_AFTER_CANDIDATE)
            & (pl.col("prior_later_attempt_count") >= threshold)
        )
        .group_by("activity_id")
        .agg(
            pl.len().cast(pl.Int64).alias("after_students"),
            pl.col("first_arrival_outcome").cast(pl.Float64).mean().alias("after_success_rate"),
            pl.sum("prior_attempt_count").cast(pl.Int64).alias("after_previous_attempts"),
        )
        .rename({"activity_id": "node_id"})
    )

    return (
        nodes.join(before_metrics, on="node_id", how="left")
        .join(after_metrics, on="node_id", how="left")
        .with_columns(
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_students"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("before_students"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("before_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_previous_attempts"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("before_previous_attempts"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_students"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("after_students"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("after_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_previous_attempts"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("after_previous_attempts"),
        )
    )


def _structural_edge_style(edge_type: str, same_lane: bool) -> dict[str, object]:
    """Return line styling for structural edges."""
    if same_lane:
        return {
            "color": "#1f7a4f",
            "width": 2.1,
            "dash": "dash" if edge_type == "deactivation" else "solid",
        }
    if edge_type == "deactivation":
        return {"color": "#b44545", "width": 2.0, "dash": "dash"}
    return {"color": "#3f5aa8", "width": 2.2}


def build_transition_efficiency_figure(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    metric: str,
    metric_label: str,
    later_attempt_threshold: int,
    show_ids: bool,
    curve_intra_objective_edges: bool,
) -> go.Figure:
    """Build the static ZPDES transition-efficiency graph."""
    if nodes.height == 0:
        return go.Figure()

    node_rows = nodes.to_dicts()
    edge_rows = edges.to_dicts()
    objective_codes = sorted(
        {str(row.get("objective_code") or "") for row in node_rows if str(row.get("objective_code") or "").strip()},
        key=objective_sort_key,
    )
    if not objective_codes:
        objective_codes = ["(no objective lane)"]
    lane_pos = {code: idx for idx, code in enumerate(objective_codes)}
    max_activity_idx = max(
        [int(row["activity_index"]) for row in node_rows if row.get("activity_index") is not None] + [1]
    )

    node_positions: dict[str, tuple[float, float]] = {}
    for row in node_rows:
        node_code = str(row.get("node_code") or "")
        objective_code = str(row.get("objective_code") or "") or objective_codes[0]
        y_pos = float(-lane_pos.get(objective_code, len(objective_codes)))
        if str(row.get("node_type")) == "objective":
            x_pos = 0.0
        else:
            idx = row.get("activity_index")
            x_pos = float(idx if isinstance(idx, int) and idx > 0 else 1)
        node_positions[node_code] = (x_pos, y_pos)

    fig = go.Figure()
    same_lane_edge_rank: dict[float, int] = {}
    structural_legend_added = {
        "activation": False,
        "deactivation": False,
        "intra": False,
    }

    for edge in edge_rows:
        from_code = str(edge.get("from_node_code") or "")
        to_code = str(edge.get("to_node_code") or "")
        if from_code not in node_positions or to_code not in node_positions:
            continue
        x0, y0 = node_positions[from_code]
        x1, y1 = node_positions[to_code]
        edge_type = str(edge.get("edge_type") or "activation")
        same_lane = abs(y0 - y1) < 1e-9
        use_curve = curve_intra_objective_edges and same_lane and abs(x1 - x0) > 0.45
        if use_curve:
            rank_key = round(y0, 2)
            rank = same_lane_edge_rank.get(rank_key, 0)
            same_lane_edge_rank[rank_key] = rank + 1
            tier = rank // 2
            curve_height = 0.28 + 0.06 * max(0, abs(x1 - x0) - 1) + 0.10 * tier
            curve_sign = 1.0 if rank % 2 == 0 else -1.0
            mx = (x0 + x1) / 2.0
            my = (y0 + y1) / 2.0 + curve_sign * curve_height
            points = _quadratic_curve_points(
                (x0, y0), (mx, my), (x1, y1), steps=int(max(12, abs(x1 - x0) * 14))
            )
            trace_key = "intra"
        else:
            points = [(x0, y0), (x1, y1)]
            trace_key = edge_type
        line_cfg = _structural_edge_style(edge_type, same_lane)
        show_legend = not structural_legend_added[trace_key]
        structural_legend_added[trace_key] = True
        fig.add_trace(
            go.Scatter(
                x=[point[0] for point in points],
                y=[point[1] for point in points],
                mode="lines",
                line=line_cfg,
                hoverinfo="skip",
                name="Structural dependency",
                showlegend=show_legend,
            )
        )

    objective_rows = [row for row in node_rows if str(row.get("node_type")) == "objective"]
    activity_rows = [row for row in node_rows if str(row.get("node_type")) == "activity"]

    objective_hover = (
        "<b>%{customdata[2]}</b><br>"
        + "Type: %{customdata[1]}<br>"
        + ("Code: %{customdata[0]}<br>" if show_ids else "")
        + "<extra></extra>"
    )
    fig.add_trace(
        go.Scatter(
            x=[node_positions[str(row.get("node_code"))][0] for row in objective_rows],
            y=[node_positions[str(row.get("node_code"))][1] for row in objective_rows],
            mode="markers+text",
            text=[truncate_text(row.get("label"), 36) for row in objective_rows],
            textposition="top center",
            textfont={"size": 11, "color": "rgba(35,40,50,0.95)"},
            customdata=[
                [str(row.get("node_code") or ""), str(row.get("node_type") or ""), str(row.get("label") or "")]
                for row in objective_rows
            ],
            hovertemplate=objective_hover,
            showlegend=False,
            marker={
                "size": 18,
                "symbol": "square",
                "color": "#5b6f8e",
                "line": {"width": 1.4, "color": "#1b1d22"},
            },
        )
    )

    metric_values = [
        float(row.get("transition_metric_value"))
        for row in activity_rows
        if row.get("transition_metric_value") is not None and not _is_missing(row.get("transition_metric_value"))
    ]
    cmin = None
    cmax = None
    colorscale = [[0.0, "#d4483b"], [0.5, "#f1c45b"], [1.0, "#1f7a4f"]]
    if metric == "first_attempt_success_rate":
        cmin = 0.0
        cmax = 1.0
    elif metric_values:
        cmin = min(metric_values)
        cmax = max(metric_values)
        colorscale = [[0.0, "#1f7a4f"], [0.5, "#f1c45b"], [1.0, "#d4483b"]]

    activity_hover = (
        "<b>%{customdata[3]}</b><br>"
        + ("ID: %{customdata[4]}<br>" if show_ids else "")
        + "Code: %{customdata[0]}<br>"
        + "Objective lane: %{customdata[5]}<br>"
        + f"{metric_label}: %{{customdata[6]}}<br>"
        + "Before success: %{customdata[7]}<br>"
        + "Before students: %{customdata[8]}<br>"
        + "Before previous attempts: %{customdata[9]}<br>"
        + f"After success (>={later_attempt_threshold} later attempts): %{{customdata[10]}}<br>"
        + f"After students (>={later_attempt_threshold} later attempts): %{{customdata[11]}}<br>"
        + f"After previous attempts (>={later_attempt_threshold} later attempts): %{{customdata[12]}}"
        + "<extra></extra>"
    )
    activity_marker: dict[str, object] = {
        "size": 14,
        "symbol": ["diamond-open" if bool(row.get("is_ghost")) else "circle" for row in activity_rows],
        "line": {"width": 1.5, "color": "#1b1d22"},
        "color": [row.get("transition_metric_value") for row in activity_rows],
        "colorscale": colorscale,
        "showscale": True,
        "colorbar": {"title": metric_label},
    }
    if cmin is not None and cmax is not None:
        activity_marker["cmin"] = cmin
        activity_marker["cmax"] = cmax

    fig.add_trace(
        go.Scatter(
            x=[node_positions[str(row.get("node_code"))][0] for row in activity_rows],
            y=[node_positions[str(row.get("node_code"))][1] for row in activity_rows],
            mode="markers",
            customdata=[
                [
                    str(row.get("node_code") or ""),
                    str(row.get("node_type") or ""),
                    str(row.get("objective_code") or ""),
                    str(row.get("label") or ""),
                    str(row.get("node_id") or ""),
                    str(row.get("objective_code") or ""),
                    format_metric_value(metric, row.get("transition_metric_value")),
                    format_rate(row.get("before_success_rate")),
                    format_int(row.get("before_students")),
                    format_int(row.get("before_previous_attempts")),
                    format_rate(row.get("after_success_rate")),
                    format_int(row.get("after_students")),
                    format_int(row.get("after_previous_attempts")),
                ]
                for row in activity_rows
            ],
            hovertemplate=activity_hover,
            showlegend=False,
            marker=activity_marker,
        )
    )

    fig.update_layout(
        height=max(520, 80 * len(objective_codes) + 140),
        margin={"l": 130, "r": 50, "t": 30, "b": 60},
        xaxis_title="Activity position within objective lane",
        yaxis_title="Objective lanes",
        dragmode=False,
        plot_bgcolor="rgba(255,255,255,0.65)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(40,40,40,0.10)",
        zeroline=False,
        tickmode="linear",
        dtick=1,
        range=[-0.6, float(max_activity_idx) + 0.8],
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(40,40,40,0.10)",
        zeroline=False,
        tickvals=[-lane_pos[code] for code in objective_codes],
        ticktext=objective_codes,
    )
    return fig

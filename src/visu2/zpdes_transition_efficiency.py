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

WORK_MODE_HOVER_LABELS = {
    "adaptive-test": "Adaptive-test mode",
    "initial-test": "Initial-test mode",
    "playlist": "Playlist mode",
    "zpdes": "ZPDES mode",
}

STRUCTURAL_ARROW_HEAD_OFFSET = 0.08
STRUCTURAL_ARROW_LENGTH = 0.12

STRUCTURAL_EDGE_LEGEND_LABELS = {
    "intra_unlock": "Intra-objective unlocking",
    "inter_unlock": "Inter-objective unlocking",
    "deactivation": "Deactivation",
}

PROGRESSION_EVENT_COLUMNS = [
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
    "exercise_id",
    "work_mode",
    "destination_rank",
    "exercise_first_attempt_outcome",
    "prior_attempt_count",
    "prior_before_activity_attempt_count",
    "prior_same_activity_attempt_count",
    "prior_later_activity_attempt_count",
]


def _collect_frame(frame: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """Collect a lazy frame with streaming fallback or clone an eager frame."""
    if isinstance(frame, pl.DataFrame):
        return frame.clone()
    try:
        return frame.collect(engine="streaming")
    except TypeError:
        return frame.collect()


def _as_lazyframe(frame: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
    """Return a lazy view of a frame without forcing materialization."""
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def _frame_columns(frame: pl.LazyFrame | pl.DataFrame) -> set[str]:
    """Return available column names for eager or lazy Polars frames."""
    if isinstance(frame, pl.DataFrame):
        return set(frame.columns)
    return set(frame.collect_schema().names())


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


def _empty_work_mode_first_attempt_frame() -> pl.DataFrame:
    """Return an empty all-work-mode first-attempt summary with the full expected schema."""
    schema: dict[str, pl.DataType] = {"node_id": pl.Utf8}
    for work_mode in WORK_MODE_HOVER_LABELS:
        prefix = work_mode.replace("-", "_")
        schema[f"{prefix}_first_attempt_success_rate"] = pl.Float64
        schema[f"{prefix}_first_attempt_event_count"] = pl.Int64
    return pl.DataFrame(schema=schema)


def _all_work_mode_first_attempt_frame(
    progression_events: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """Aggregate activity first-attempt success and event counts for each work mode."""
    required_columns = {
        "module_code",
        "date_utc",
        "activity_id",
        "work_mode",
        "exercise_first_attempt_outcome",
    }
    if not required_columns.issubset(_frame_columns(progression_events)):
        return _empty_work_mode_first_attempt_frame()
    scoped = _as_lazyframe(progression_events).filter(pl.col("module_code") == module_code)
    if start_date is not None and end_date is not None:
        scoped = scoped.filter(
            (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
        )

    grouped = _collect_frame(
        scoped.group_by(["activity_id", "work_mode"]).agg(
            pl.len().cast(pl.Int64).alias("first_attempt_event_count"),
            pl.col("exercise_first_attempt_outcome")
            .cast(pl.Float64)
            .mean()
            .alias("first_attempt_success_rate"),
        )
    )
    if grouped.height == 0:
        return _empty_work_mode_first_attempt_frame()

    work_mode_frames: list[pl.DataFrame] = []
    for work_mode in WORK_MODE_HOVER_LABELS:
        prefix = work_mode.replace("-", "_")
        mode_frame = (
            grouped.filter(pl.col("work_mode") == work_mode)
            .select(
                [
                    "activity_id",
                    pl.col("first_attempt_event_count").alias(f"{prefix}_first_attempt_event_count"),
                    pl.col("first_attempt_success_rate").alias(f"{prefix}_first_attempt_success_rate"),
                ]
            )
            .rename({"activity_id": "node_id"})
        )
        work_mode_frames.append(mode_frame)

    if not work_mode_frames:
        return _empty_work_mode_first_attempt_frame()

    summary = work_mode_frames[0]
    for frame in work_mode_frames[1:]:
        summary = summary.join(frame, on="node_id", how="full", coalesce=True)
    return summary


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


def _point_on_polyline_from_end(
    points: list[tuple[float, float]],
    distance: float,
) -> tuple[float, float] | None:
    """Return a point located ``distance`` units back from the polyline end."""
    if len(points) < 2:
        return None
    remaining = max(0.0, float(distance))
    for idx in range(len(points) - 1, 0, -1):
        x1, y1 = points[idx]
        x0, y0 = points[idx - 1]
        dx = x0 - x1
        dy = y0 - y1
        segment_length = math.hypot(dx, dy)
        if segment_length <= 1e-9:
            continue
        if remaining <= segment_length:
            ratio = remaining / segment_length
            return (x1 + dx * ratio, y1 + dy * ratio)
        remaining -= segment_length
    return points[0]


def _structural_arrow_points(
    points: list[tuple[float, float]],
    *,
    head_offset: float = STRUCTURAL_ARROW_HEAD_OFFSET,
    arrow_length: float = STRUCTURAL_ARROW_LENGTH,
) -> tuple[float, float, float, float] | None:
    """Return tail/head coordinates for an arrow aligned to a polyline end."""
    head = _point_on_polyline_from_end(points, head_offset)
    tail = _point_on_polyline_from_end(points, head_offset + arrow_length)
    if head is None or tail is None:
        return None
    if math.hypot(head[0] - tail[0], head[1] - tail[1]) <= 1e-6:
        return None
    return (tail[0], tail[1], head[0], head[1])


def _polyline_prefix_until_distance_from_end(
    points: list[tuple[float, float]],
    distance: float,
) -> list[tuple[float, float]]:
    """Return the polyline prefix that stops ``distance`` units before the end."""
    if len(points) < 2:
        return list(points)
    remaining = max(0.0, float(distance))
    for idx in range(len(points) - 1, 0, -1):
        x1, y1 = points[idx]
        x0, y0 = points[idx - 1]
        dx = x0 - x1
        dy = y0 - y1
        segment_length = math.hypot(dx, dy)
        if segment_length <= 1e-9:
            continue
        if remaining <= segment_length:
            ratio = remaining / segment_length
            cut_point = (x1 + dx * ratio, y1 + dy * ratio)
            return [*points[:idx], cut_point]
        remaining -= segment_length
    return [points[0]]


def _structural_trace_key(edge_type: str, same_lane: bool) -> str:
    """Return the semantic legend group for a structural edge."""
    if edge_type == "deactivation":
        return "deactivation"
    return "intra_unlock" if same_lane else "inter_unlock"


def _related_node_codes_for_focus(
    edges: pl.DataFrame,
    focused_node_code: str | None,
    *,
    node_rows: list[dict[str, object]] | None = None,
    ancestor_root_codes: set[str] | None = None,
) -> set[str] | None:
    """Return the focused node, all ancestors, and only its direct children."""
    focus = str(focused_node_code or "").strip()
    if not focus:
        return None
    outgoing: dict[str, set[str]] = {}
    incoming: dict[str, set[str]] = {}
    for row in edges.select(["from_node_code", "to_node_code"]).to_dicts():
        src = str(row.get("from_node_code") or "").strip()
        dst = str(row.get("to_node_code") or "").strip()
        if not src or not dst:
            continue
        outgoing.setdefault(src, set()).add(dst)
        incoming.setdefault(dst, set()).add(src)
    row_by_code = (
        {str(row.get("node_code") or "").strip(): row for row in node_rows}
        if node_rows is not None
        else {}
    )

    ancestor_roots = {focus}
    if ancestor_root_codes is not None:
        ancestor_roots |= {str(code).strip() for code in ancestor_root_codes if str(code).strip()}

    ancestors = set(ancestor_roots)
    stack = list(ancestor_roots)
    while stack:
        current = stack.pop()
        row = row_by_code.get(current)
        if row is not None and str(row.get("node_type") or "") == "activity":
            objective_code = str(row.get("objective_code") or "").strip()
            if objective_code and objective_code not in ancestors:
                ancestors.add(objective_code)
                stack.append(objective_code)
        for parent in incoming.get(current, set()):
            if parent not in ancestors:
                ancestors.add(parent)
                stack.append(parent)

    direct_children = set(outgoing.get(focus, set()))
    return ancestors | direct_children


def _highlighted_edge_pairs_for_focus(
    edges: pl.DataFrame,
    focused_node_code: str | None,
    *,
    node_rows: list[dict[str, object]] | None = None,
    ancestor_root_codes: set[str] | None = None,
) -> set[tuple[str, str]] | None:
    """Return highlighted edges for focus: recursive prerequisites + direct outgoing edges."""
    focus = str(focused_node_code or "").strip()
    if not focus:
        return None
    outgoing: dict[str, set[str]] = {}
    incoming: dict[str, set[str]] = {}
    for row in edges.select(["from_node_code", "to_node_code"]).to_dicts():
        src = str(row.get("from_node_code") or "").strip()
        dst = str(row.get("to_node_code") or "").strip()
        if not src or not dst:
            continue
        outgoing.setdefault(src, set()).add(dst)
        incoming.setdefault(dst, set()).add(src)
    row_by_code = (
        {str(row.get("node_code") or "").strip(): row for row in node_rows}
        if node_rows is not None
        else {}
    )

    ancestor_roots = {focus}
    if ancestor_root_codes is not None:
        ancestor_roots |= {str(code).strip() for code in ancestor_root_codes if str(code).strip()}

    highlighted_edges: set[tuple[str, str]] = set()
    visited = set(ancestor_roots)
    stack = list(ancestor_roots)
    while stack:
        current = stack.pop()
        row = row_by_code.get(current)
        if row is not None and str(row.get("node_type") or "") == "activity":
            objective_code = str(row.get("objective_code") or "").strip()
            if objective_code and objective_code not in visited:
                visited.add(objective_code)
                stack.append(objective_code)
        for parent in incoming.get(current, set()):
            highlighted_edges.add((parent, current))
            if parent not in visited:
                visited.add(parent)
                stack.append(parent)

    for child in outgoing.get(focus, set()):
        highlighted_edges.add((focus, child))

    return highlighted_edges


def _focus_ancestor_root_codes(
    node_rows: list[dict[str, object]],
    focused_node_code: str | None,
) -> set[str] | None:
    """Return ancestor-walk roots for a focused node, including its objective when needed."""
    focus = str(focused_node_code or "").strip()
    if not focus:
        return None
    row_by_code = {str(row.get("node_code") or "").strip(): row for row in node_rows}
    focus_row = row_by_code.get(focus)
    if focus_row is None:
        return {focus}
    roots = {focus}
    if str(focus_row.get("node_type") or "") == "activity":
        objective_code = str(focus_row.get("objective_code") or "").strip()
        if objective_code:
            roots.add(objective_code)
    return roots


def _dimmed_structural_edge_style(base_style: dict[str, object]) -> dict[str, object]:
    """Return a dimmed version of a structural edge style for de-emphasis."""
    style = dict(base_style)
    style["color"] = "rgba(165, 171, 184, 0.35)"
    style["width"] = max(1.0, float(base_style.get("width", 2.0)) - 0.6)
    return style


def build_transition_layout(
    nodes: pl.DataFrame,
) -> dict[str, object]:
    """Return reusable node-position metadata for ZPDES topology figures."""
    if nodes.height == 0:
        return {
            "node_rows": [],
            "objective_rows": [],
            "activity_rows": [],
            "objective_codes": [],
            "lane_pos": {},
            "max_activity_idx": 1,
            "node_positions": {},
        }

    node_rows = nodes.to_dicts()
    objective_codes = sorted(
        {
            str(row.get("objective_code") or "")
            for row in node_rows
            if str(row.get("objective_code") or "").strip()
        },
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

    return {
        "node_rows": node_rows,
        "objective_rows": [row for row in node_rows if str(row.get("node_type")) == "objective"],
        "activity_rows": [row for row in node_rows if str(row.get("node_type")) == "activity"],
        "objective_codes": objective_codes,
        "lane_pos": lane_pos,
        "max_activity_idx": max_activity_idx,
        "node_positions": node_positions,
    }


def add_structural_dependency_traces(
    fig: go.Figure,
    edges: pl.DataFrame,
    node_positions: dict[str, tuple[float, float]],
    *,
    curve_intra_objective_edges: bool,
    show_direction_arrows: bool = False,
    highlighted_edge_pairs: set[tuple[str, str]] | None = None,
) -> None:
    """Add the shared structural dependency background to an existing figure."""
    edge_rows = edges.to_dicts()
    same_lane_edge_rank: dict[tuple[float, bool], int] = {}
    structural_legend_added = {key: False for key in STRUCTURAL_EDGE_LEGEND_LABELS}

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
        trace_key = _structural_trace_key(edge_type, same_lane)
        is_related = highlighted_edge_pairs is None or (from_code, to_code) in highlighted_edge_pairs
        if use_curve:
            rank_key = (round(y0, 2), is_related)
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
        else:
            points = [(x0, y0), (x1, y1)]
        line_cfg = _structural_edge_style(edge_type, same_lane)
        if not is_related:
            line_cfg = _dimmed_structural_edge_style(line_cfg)
        arrow_points = _structural_arrow_points(points) if show_direction_arrows else None
        line_points = (
            _polyline_prefix_until_distance_from_end(
                points, distance=STRUCTURAL_ARROW_HEAD_OFFSET + STRUCTURAL_ARROW_LENGTH
            )
            if arrow_points is not None
            else points
        )
        show_legend = not structural_legend_added[trace_key]
        structural_legend_added[trace_key] = True
        fig.add_trace(
            go.Scatter(
                x=[point[0] for point in line_points],
                y=[point[1] for point in line_points],
                mode="lines",
                line=line_cfg,
                hoverinfo="skip",
                name=STRUCTURAL_EDGE_LEGEND_LABELS[trace_key],
                showlegend=show_legend,
            )
        )
        if arrow_points is None:
            continue
        tail_x, tail_y, head_x, head_y = arrow_points
        fig.add_annotation(
            x=head_x,
            y=head_y,
            ax=tail_x,
            ay=tail_y,
            xref="x",
            yref="y",
            axref="x",
            ayref="y",
            text="",
            showarrow=True,
            arrowhead=3,
            arrowsize=0.95,
            arrowwidth=max(1.8, float(line_cfg.get("width", 2.0))),
            arrowcolor=str(line_cfg.get("color", "#3f5aa8")),
        )


def attach_transition_metric_to_nodes(
    nodes: pl.DataFrame,
    agg_activity_elo: pl.DataFrame | pl.LazyFrame,
    progression_events: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date | None,
    end_date: date | None,
    metric: str,
    work_mode: str,
) -> pl.DataFrame:
    """Attach the selected activity metric to ZPDES activity nodes."""
    nodes_frame = nodes.clone()
    work_mode_summary_frame = _all_work_mode_first_attempt_frame(
        progression_events=progression_events,
        module_code=module_code,
        start_date=start_date,
        end_date=end_date,
    )
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
        mode_metric_frame = work_mode_summary_frame.select(
            [
                "node_id",
                pl.lit(None, dtype=pl.Float64).alias("selected_mode_first_attempt_success_rate"),
                pl.lit(None, dtype=pl.Int64).alias("selected_mode_first_attempt_event_count"),
            ]
        )
    elif metric == "first_attempt_success_rate":
        mode_metric_frame = (
            work_mode_summary_frame.select(
                [
                    "node_id",
                    pl.col(f"{work_mode.replace('-', '_')}_first_attempt_success_rate").alias(
                        "selected_mode_first_attempt_success_rate"
                    ),
                    pl.col(f"{work_mode.replace('-', '_')}_first_attempt_event_count").alias(
                        "selected_mode_first_attempt_event_count"
                    ),
                ]
            )
        )
        metric_frame = mode_metric_frame.select(
            [
                "node_id",
                pl.col("selected_mode_first_attempt_success_rate").alias("transition_metric_value"),
            ]
        )
    else:
        raise ValueError(f"Unsupported transition node metric: {metric}")

    return (
        nodes_frame.join(metric_frame, on="node_id", how="left")
        .join(mode_metric_frame, on="node_id", how="left")
        .join(work_mode_summary_frame, on="node_id", how="left")
        .with_columns(
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("transition_metric_value"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("transition_metric_value"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("selected_mode_first_attempt_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("selected_mode_first_attempt_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("selected_mode_first_attempt_event_count"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("selected_mode_first_attempt_event_count"),
            *[
                pl.when(pl.col("node_type") == "activity")
                .then(pl.col(f"{work_mode.replace('-', '_')}_first_attempt_success_rate"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias(f"{work_mode.replace('-', '_')}_first_attempt_success_rate")
                for work_mode in WORK_MODE_HOVER_LABELS
            ],
            *[
                pl.when(pl.col("node_type") == "activity")
                .then(pl.col(f"{work_mode.replace('-', '_')}_first_attempt_event_count"))
                .otherwise(pl.lit(None, dtype=pl.Int64))
                .alias(f"{work_mode.replace('-', '_')}_first_attempt_event_count")
                for work_mode in WORK_MODE_HOVER_LABELS
            ],
        )
    )


def _cohort_metrics(
    scoped_events: pl.DataFrame,
    condition: pl.Expr,
    prefix: str,
) -> pl.DataFrame:
    """Aggregate event-based cohort metrics for one destination activity."""
    return (
        scoped_events.filter(condition)
        .group_by("activity_id")
        .agg(
            pl.len().cast(pl.Int64).alias(f"{prefix}_event_count"),
            pl.col("user_id").drop_nulls().n_unique().cast(pl.Int64).alias(f"{prefix}_unique_students"),
            pl.col("exercise_first_attempt_outcome")
            .cast(pl.Float64)
            .mean()
            .alias(f"{prefix}_success_rate"),
            pl.sum("prior_attempt_count").cast(pl.Int64).alias(f"{prefix}_previous_attempts"),
        )
        .rename({"activity_id": "node_id"})
    )


def attach_progression_cohort_metrics_to_nodes(
    nodes: pl.DataFrame,
    progression_events: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date | None,
    end_date: date | None,
    work_mode: str,
    later_attempt_threshold: int,
) -> pl.DataFrame:
    """Attach event-based before/after/in-activity cohort metrics to activity nodes."""
    required_columns = {
        "module_code",
        "date_utc",
        "activity_id",
        "work_mode",
        "user_id",
        "exercise_first_attempt_outcome",
        "prior_attempt_count",
        "prior_before_activity_attempt_count",
        "prior_same_activity_attempt_count",
        "prior_later_activity_attempt_count",
    }
    if not required_columns.issubset(_frame_columns(progression_events)):
        return nodes.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("before_event_count"),
            pl.lit(None, dtype=pl.Int64).alias("before_unique_students"),
            pl.lit(None, dtype=pl.Float64).alias("before_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("before_previous_attempts"),
            pl.lit(None, dtype=pl.Int64).alias("after_event_count"),
            pl.lit(None, dtype=pl.Int64).alias("after_unique_students"),
            pl.lit(None, dtype=pl.Float64).alias("after_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("after_previous_attempts"),
            pl.lit(None, dtype=pl.Int64).alias("in_activity_event_count"),
            pl.lit(None, dtype=pl.Int64).alias("in_activity_unique_students"),
            pl.lit(None, dtype=pl.Float64).alias("in_activity_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("in_activity_previous_attempts"),
        )
    scoped = _as_lazyframe(progression_events).filter(
        (pl.col("module_code") == module_code) & (pl.col("work_mode") == work_mode)
    )
    if start_date is not None and end_date is not None:
        scoped = scoped.filter(
            (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
        )

    threshold = max(1, int(later_attempt_threshold))
    before_condition = (
        (pl.col("prior_same_activity_attempt_count") == 0)
        & (pl.col("prior_before_activity_attempt_count") > 0)
        & (pl.col("prior_later_activity_attempt_count") == 0)
    )
    after_condition = pl.col("prior_later_activity_attempt_count") >= threshold
    in_activity_condition = (
        (pl.col("prior_same_activity_attempt_count") > 0)
        & (pl.col("prior_later_activity_attempt_count") < threshold)
    )

    scoped_summary = _collect_frame(
        scoped.group_by("activity_id").agg(
            pl.col("exercise_first_attempt_outcome")
            .filter(before_condition)
            .count()
            .cast(pl.Int64)
            .alias("before_event_count"),
            pl.col("user_id")
            .filter(before_condition)
            .drop_nulls()
            .n_unique()
            .cast(pl.Int64)
            .alias("before_unique_students"),
            pl.col("exercise_first_attempt_outcome")
            .filter(before_condition)
            .cast(pl.Float64)
            .mean()
            .alias("before_success_rate"),
            pl.col("prior_attempt_count")
            .filter(before_condition)
            .sum()
            .cast(pl.Int64)
            .alias("before_previous_attempts"),
            pl.col("exercise_first_attempt_outcome")
            .filter(after_condition)
            .count()
            .cast(pl.Int64)
            .alias("after_event_count"),
            pl.col("user_id")
            .filter(after_condition)
            .drop_nulls()
            .n_unique()
            .cast(pl.Int64)
            .alias("after_unique_students"),
            pl.col("exercise_first_attempt_outcome")
            .filter(after_condition)
            .cast(pl.Float64)
            .mean()
            .alias("after_success_rate"),
            pl.col("prior_attempt_count")
            .filter(after_condition)
            .sum()
            .cast(pl.Int64)
            .alias("after_previous_attempts"),
            pl.col("exercise_first_attempt_outcome")
            .filter(in_activity_condition)
            .count()
            .cast(pl.Int64)
            .alias("in_activity_event_count"),
            pl.col("user_id")
            .filter(in_activity_condition)
            .drop_nulls()
            .n_unique()
            .cast(pl.Int64)
            .alias("in_activity_unique_students"),
            pl.col("exercise_first_attempt_outcome")
            .filter(in_activity_condition)
            .cast(pl.Float64)
            .mean()
            .alias("in_activity_success_rate"),
            pl.col("prior_attempt_count")
            .filter(in_activity_condition)
            .sum()
            .cast(pl.Int64)
            .alias("in_activity_previous_attempts"),
        )
    ).rename({"activity_id": "node_id"})
    if scoped_summary.height == 0:
        return nodes.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("before_event_count"),
            pl.lit(None, dtype=pl.Int64).alias("before_unique_students"),
            pl.lit(None, dtype=pl.Float64).alias("before_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("before_previous_attempts"),
            pl.lit(None, dtype=pl.Int64).alias("after_event_count"),
            pl.lit(None, dtype=pl.Int64).alias("after_unique_students"),
            pl.lit(None, dtype=pl.Float64).alias("after_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("after_previous_attempts"),
            pl.lit(None, dtype=pl.Int64).alias("in_activity_event_count"),
            pl.lit(None, dtype=pl.Int64).alias("in_activity_unique_students"),
            pl.lit(None, dtype=pl.Float64).alias("in_activity_success_rate"),
            pl.lit(None, dtype=pl.Int64).alias("in_activity_previous_attempts"),
        )

    return (
        nodes.join(scoped_summary, on="node_id", how="left")
        .with_columns(
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_event_count"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("before_event_count"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_unique_students"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("before_unique_students"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("before_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("before_previous_attempts"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("before_previous_attempts"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_event_count"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("after_event_count"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_unique_students"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("after_unique_students"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("after_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("after_previous_attempts"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("after_previous_attempts"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("in_activity_event_count"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("in_activity_event_count"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("in_activity_unique_students"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("in_activity_unique_students"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("in_activity_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("in_activity_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("in_activity_previous_attempts"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("in_activity_previous_attempts"),
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
    focused_node_code: str | None = None,
) -> go.Figure:
    """Build the static ZPDES transition-efficiency graph."""
    if nodes.height == 0:
        return go.Figure()

    layout = build_transition_layout(nodes)
    objective_rows = layout["objective_rows"]
    activity_rows = layout["activity_rows"]
    objective_codes = layout["objective_codes"]
    lane_pos = layout["lane_pos"]
    max_activity_idx = layout["max_activity_idx"]
    node_positions = layout["node_positions"]
    ancestor_root_codes = _focus_ancestor_root_codes(layout["node_rows"], focused_node_code)
    related_node_codes = _related_node_codes_for_focus(
        edges,
        focused_node_code,
        node_rows=layout["node_rows"],
        ancestor_root_codes=ancestor_root_codes,
    )
    highlighted_edge_pairs = _highlighted_edge_pairs_for_focus(
        edges,
        focused_node_code,
        node_rows=layout["node_rows"],
        ancestor_root_codes=ancestor_root_codes,
    )
    if related_node_codes is not None and focused_node_code not in node_positions:
        related_node_codes = None
        highlighted_edge_pairs = None
    related_objective_codes = (
        {
            str(row.get("objective_code") or "")
            for row in layout["node_rows"]
            if str(row.get("node_code") or "") in related_node_codes
        }
        if related_node_codes is not None
        else None
    )

    fig = go.Figure()
    add_structural_dependency_traces(
        fig,
        edges,
        node_positions,
        curve_intra_objective_edges=curve_intra_objective_edges,
        show_direction_arrows=True,
        highlighted_edge_pairs=highlighted_edge_pairs,
    )

    def _is_related(row: dict[str, object]) -> bool:
        if related_node_codes is None:
            return True
        node_code = str(row.get("node_code") or "")
        if node_code in related_node_codes:
            return True
        if str(row.get("node_type") or "") == "objective":
            return str(row.get("objective_code") or "") in (related_objective_codes or set())
        return False

    focused_code = str(focused_node_code or "").strip()
    focused_objective_rows = [
        row for row in objective_rows if str(row.get("node_code") or "") == focused_code
    ]
    active_objective_rows = [
        row
        for row in objective_rows
        if _is_related(row) and str(row.get("node_code") or "") != focused_code
    ]
    inactive_objective_rows = [row for row in objective_rows if not _is_related(row)]

    objective_hover = (
        "<b>%{customdata[2]}</b><br>"
        + "Type: %{customdata[1]}<br>"
        + ("Code: %{customdata[0]}<br>" if show_ids else "")
        + "<extra></extra>"
    )
    for rows, marker_color, text_color, size, line_width in [
        (inactive_objective_rows, "rgba(185, 190, 198, 0.55)", "rgba(124,130,142,0.82)", 18, 1.0),
        (active_objective_rows, "#5b6f8e", "rgba(35,40,50,0.95)", 18, 1.4),
        (focused_objective_rows, "#384c71", "rgba(24,28,35,0.98)", 21, 2.1),
    ]:
        if not rows:
            continue
        fig.add_trace(
            go.Scatter(
                x=[node_positions[str(row.get("node_code"))][0] for row in rows],
                y=[node_positions[str(row.get("node_code"))][1] for row in rows],
                mode="markers+text",
                text=[truncate_text(row.get("label"), 36) for row in rows],
                textposition="top center",
                textfont={"size": 11, "color": text_color},
                customdata=[
                    [str(row.get("node_code") or ""), str(row.get("node_type") or ""), str(row.get("label") or "")]
                    for row in rows
                ],
                hovertemplate=objective_hover,
                showlegend=False,
                marker={
                    "size": size,
                    "symbol": "square",
                    "color": marker_color,
                    "line": {"width": line_width, "color": "#1b1d22"},
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
        + "Adaptive-test mode first-attempt success / events: %{customdata[7]} / %{customdata[8]}<br>"
        + "Initial-test mode first-attempt success / events: %{customdata[9]} / %{customdata[10]}<br>"
        + "Playlist mode first-attempt success / events: %{customdata[11]} / %{customdata[12]}<br>"
        + "ZPDES mode first-attempt success / events: %{customdata[13]} / %{customdata[14]}<br>"
        + "Before success: %{customdata[15]}<br>"
        + "Before eligible events: %{customdata[16]}<br>"
        + "Before unique students: %{customdata[17]}<br>"
        + "Before previous attempts: %{customdata[18]}<br>"
        + f'After success (>={later_attempt_threshold} later attempts): %{{customdata[19]}}<br>'
        + f'After eligible events (>={later_attempt_threshold} later attempts): %{{customdata[20]}}<br>'
        + f'After unique students (>={later_attempt_threshold} later attempts): %{{customdata[21]}}<br>'
        + f'After previous attempts (>={later_attempt_threshold} later attempts): %{{customdata[22]}}<br>'
        + "In-activity success: %{customdata[23]}<br>"
        + "In-activity eligible events: %{customdata[24]}<br>"
        + "In-activity unique students: %{customdata[25]}<br>"
        + "In-activity previous attempts: %{customdata[26]}"
        + "<extra></extra>"
    )
    activity_marker: dict[str, object] = {
        "size": 14,
        "symbol": ["diamond-open" if bool(row.get("is_ghost")) else "circle" for row in activity_rows],
        "line": {"width": 1.5, "color": "#1b1d22"},
        "color": [
            float(row.get("transition_metric_value"))
            if row.get("transition_metric_value") is not None
            and not _is_missing(row.get("transition_metric_value"))
            else float("nan")
            for row in activity_rows
        ],
        "colorscale": colorscale,
        "showscale": True,
        "colorbar": {"title": metric_label, "x": 1.04, "y": 0.5, "len": 0.78, "thickness": 16},
    }
    if cmin is not None and cmax is not None:
        activity_marker["cmin"] = cmin
        activity_marker["cmax"] = cmax

    def _activity_customdata(row: dict[str, object]) -> list[str]:
        return [
            str(row.get("node_code") or ""),
            str(row.get("node_type") or ""),
            str(row.get("objective_code") or ""),
            str(row.get("label") or ""),
            str(row.get("node_id") or ""),
            str(row.get("objective_code") or ""),
            format_metric_value(metric, row.get("transition_metric_value")),
            format_rate(row.get("adaptive_test_first_attempt_success_rate")),
            format_int(row.get("adaptive_test_first_attempt_event_count")),
            format_rate(row.get("initial_test_first_attempt_success_rate")),
            format_int(row.get("initial_test_first_attempt_event_count")),
            format_rate(row.get("playlist_first_attempt_success_rate")),
            format_int(row.get("playlist_first_attempt_event_count")),
            format_rate(row.get("zpdes_first_attempt_success_rate")),
            format_int(row.get("zpdes_first_attempt_event_count")),
            format_rate(row.get("before_success_rate")),
            format_int(row.get("before_event_count")),
            format_int(row.get("before_unique_students")),
            format_int(row.get("before_previous_attempts")),
            format_rate(row.get("after_success_rate")),
            format_int(row.get("after_event_count")),
            format_int(row.get("after_unique_students")),
            format_int(row.get("after_previous_attempts")),
            format_rate(row.get("in_activity_success_rate")),
            format_int(row.get("in_activity_event_count")),
            format_int(row.get("in_activity_unique_students")),
            format_int(row.get("in_activity_previous_attempts")),
        ]

    focused_activity_rows = [row for row in activity_rows if str(row.get("node_code") or "") == focused_code]
    active_activity_rows = [
        row for row in activity_rows if _is_related(row) and str(row.get("node_code") or "") != focused_code
    ]
    inactive_activity_rows = [row for row in activity_rows if not _is_related(row)]

    if inactive_activity_rows:
        fig.add_trace(
            go.Scatter(
                x=[node_positions[str(row.get("node_code"))][0] for row in inactive_activity_rows],
                y=[node_positions[str(row.get("node_code"))][1] for row in inactive_activity_rows],
                mode="markers",
                customdata=[_activity_customdata(row) for row in inactive_activity_rows],
                hovertemplate=activity_hover,
                showlegend=False,
                marker={
                    "size": 14,
                    "symbol": ["diamond-open" if bool(row.get("is_ghost")) else "circle" for row in inactive_activity_rows],
                    "line": {"width": 1.1, "color": "rgba(95, 101, 112, 0.70)"},
                    "color": "rgba(194, 199, 208, 0.58)",
                },
            )
        )

    if active_activity_rows:
        active_marker = dict(activity_marker)
        active_marker["showscale"] = not focused_activity_rows
        active_marker["symbol"] = [
            "diamond-open" if bool(row.get("is_ghost")) else "circle" for row in active_activity_rows
        ]
        active_marker["color"] = [
            float(row.get("transition_metric_value"))
            if row.get("transition_metric_value") is not None and not _is_missing(row.get("transition_metric_value"))
            else float("nan")
            for row in active_activity_rows
        ]
        fig.add_trace(
            go.Scatter(
                x=[node_positions[str(row.get("node_code"))][0] for row in active_activity_rows],
                y=[node_positions[str(row.get("node_code"))][1] for row in active_activity_rows],
                mode="markers",
                customdata=[_activity_customdata(row) for row in active_activity_rows],
                hovertemplate=activity_hover,
                showlegend=False,
                marker=active_marker,
            )
        )

    if focused_activity_rows:
        focused_marker = dict(activity_marker)
        focused_marker["showscale"] = True
        focused_marker["size"] = 18
        focused_marker["line"] = {"width": 2.5, "color": "#11151d"}
        focused_marker["symbol"] = [
            "diamond-open" if bool(row.get("is_ghost")) else "circle" for row in focused_activity_rows
        ]
        focused_marker["color"] = [
            float(row.get("transition_metric_value"))
            if row.get("transition_metric_value") is not None and not _is_missing(row.get("transition_metric_value"))
            else float("nan")
            for row in focused_activity_rows
        ]
        fig.add_trace(
            go.Scatter(
                x=[node_positions[str(row.get("node_code"))][0] for row in focused_activity_rows],
                y=[node_positions[str(row.get("node_code"))][1] for row in focused_activity_rows],
                mode="markers",
                customdata=[_activity_customdata(row) for row in focused_activity_rows],
                hovertemplate=activity_hover,
                showlegend=False,
                marker=focused_marker,
            )
        )

    fig.update_layout(
        height=max(520, 80 * len(objective_codes) + 140),
        margin={"l": 130, "r": 115, "t": 88, "b": 60},
        xaxis_title="Activity position within objective lane",
        yaxis_title="Objective lanes",
        clickmode="event+select",
        dragmode=False,
        plot_bgcolor="rgba(255,255,255,0.65)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend={
            "orientation": "h",
            "x": 0.0,
            "y": 1.08,
            "xanchor": "left",
            "yanchor": "bottom",
            "bgcolor": "rgba(255,255,255,0.88)",
            "bordercolor": "rgba(42,48,58,0.16)",
            "borderwidth": 1,
        },
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

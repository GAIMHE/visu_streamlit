"""Helpers for the Module 1 individual-path replay page."""

from __future__ import annotations

from bisect import bisect_right
from math import sqrt
from random import Random
from typing import Any

import plotly.graph_objects as go
import polars as pl

from .zpdes_transition_efficiency import (
    add_structural_dependency_traces,
    build_transition_layout,
    format_int,
    format_rate,
    truncate_text,
)


def load_m1_student_profiles(path) -> pl.DataFrame:
    """Load module-local student profiles for Module 1 only."""
    return (
        pl.read_parquet(path)
        .filter(pl.col("module_code") == "M1")
        .sort(["total_attempts", "user_id"], descending=[True, False])
    )


def select_m1_students_near_attempt_target(
    profiles: pl.DataFrame,
    *,
    target_attempts: int,
    tolerance_ratio: float = 0.10,
    max_students: int = 1,
    seed: int | None = None,
) -> list[str]:
    """Select up to ``max_students`` students near a requested M1 attempt count."""
    if profiles.height == 0:
        return []
    limit = max(1, int(max_students))
    target = max(1, int(target_attempts))
    tolerance = max(1, int(round(target * max(0.0, float(tolerance_ratio)))))
    lower = max(1, target - tolerance)
    upper = target + tolerance
    candidates = (
        profiles.filter(
            (pl.col("total_attempts") >= lower) & (pl.col("total_attempts") <= upper)
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


def select_m1_student_by_id(profiles: pl.DataFrame, user_id: str) -> str | None:
    """Return one exact M1 student match when present."""
    normalized = str(user_id or "").strip()
    if not normalized or profiles.height == 0:
        return None
    matches = profiles.filter(pl.col("user_id").cast(pl.Utf8) == normalized)
    if matches.height == 0:
        return None
    return normalized


def _empty_payload(user_ids: list[str], step_size: int, topology: dict[str, object]) -> dict[str, Any]:
    """Return an empty replay payload with preserved topology metadata."""
    return {
        "student_ids": user_ids,
        "frame_cutoffs": [0],
        "step_size": max(1, int(step_size)),
        "series": {},
        "max_attempts": 0,
        "module_code": "M1",
        "module_label": "M1",
        "topology": topology,
    }


def _collect_frame(events: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    if isinstance(events, pl.DataFrame):
        return events.clone()
    try:
        return events.collect(engine="streaming")
    except TypeError:
        return events.collect()


def _normalize_outcome(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return 1.0 if float(value) > 0 else 0.0
    text = str(value).strip().lower()
    return 1.0 if text in {"1", "true", "t", "yes"} else 0.0


def build_m1_individual_path_payload(
    events: pl.DataFrame | pl.LazyFrame,
    user_ids: list[str],
    step_size: int,
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
) -> dict[str, Any]:
    """Build the replay payload for the Module 1 individual-path page."""
    normalized_user_ids = [str(user_id).strip() for user_id in user_ids if str(user_id).strip()]
    topology_layout = build_transition_layout(nodes)
    topology = {
        "objective_rows": topology_layout.get("objective_rows") or [],
        "activity_rows": topology_layout.get("activity_rows") or [],
        "objective_codes": topology_layout.get("objective_codes") or [],
        "lane_pos": topology_layout.get("lane_pos") or {},
        "max_activity_idx": int(topology_layout.get("max_activity_idx") or 1),
        "node_positions": topology_layout.get("node_positions") or {},
        "edge_rows": edges.to_dicts(),
    }
    if not normalized_user_ids:
        return _empty_payload([], step_size, topology)

    frame = _collect_frame(events)
    if frame.height == 0:
        return _empty_payload(normalized_user_ids, step_size, topology)

    frame = frame.sort(
        ["created_at", "user_id", "activity_id", "exercise_id", "attempt_number"],
        descending=[False, False, False, False, False],
    )
    activity_rows = topology["activity_rows"]
    mapped_activity_ids = {
        str(row.get("node_id") or "").strip()
        for row in activity_rows
        if str(row.get("node_id") or "").strip()
    }

    series: dict[str, dict[str, Any]] = {}
    max_attempts = 0
    module_label = next(
        (
            str(value).strip()
            for value in frame.get_column("module_label").to_list()
            if str(value or "").strip()
        ),
        "M1",
    )
    for row in frame.to_dicts():
        user_id = str(row.get("user_id") or "").strip()
        if not user_id or user_id not in normalized_user_ids:
            continue
        bucket = series.setdefault(
            user_id,
            {
                "attempt_ordinal": [],
                "created_at": [],
                "date_utc": [],
                "work_mode": [],
                "objective_id": [],
                "objective_label": [],
                "activity_id": [],
                "activity_label": [],
                "exercise_id": [],
                "outcome": [],
                "is_mapped_activity": [],
                "mapped_attempt_total": 0,
                "unmapped_attempt_total": 0,
                "max_activity_attempt_count": 0,
            },
        )
        activity_id = str(row.get("activity_id") or "").strip()
        is_mapped = activity_id in mapped_activity_ids
        ordinal = len(bucket["attempt_ordinal"]) + 1
        outcome = _normalize_outcome(row.get("data_correct"))

        bucket["attempt_ordinal"].append(ordinal)
        bucket["created_at"].append(str(row.get("created_at") or ""))
        bucket["date_utc"].append(str(row.get("date_utc") or ""))
        bucket["work_mode"].append(str(row.get("work_mode") or ""))
        bucket["objective_id"].append(str(row.get("objective_id") or ""))
        bucket["objective_label"].append(str(row.get("objective_label") or "") or str(row.get("objective_id") or ""))
        bucket["activity_id"].append(activity_id)
        bucket["activity_label"].append(str(row.get("activity_label") or "") or activity_id)
        bucket["exercise_id"].append(str(row.get("exercise_id") or ""))
        bucket["outcome"].append(outcome)
        bucket["is_mapped_activity"].append(is_mapped)
        if is_mapped:
            bucket["mapped_attempt_total"] += 1
        else:
            bucket["unmapped_attempt_total"] += 1
        max_attempts = max(max_attempts, ordinal)

    if not series:
        return _empty_payload(normalized_user_ids, step_size, topology)

    for bucket in series.values():
        counts: dict[str, int] = {}
        for activity_id, is_mapped in zip(bucket["activity_id"], bucket["is_mapped_activity"], strict=False):
            if not is_mapped:
                continue
            counts[activity_id] = counts.get(activity_id, 0) + 1
        bucket["max_activity_attempt_count"] = max(counts.values()) if counts else 0

    step = max(1, int(step_size))
    frame_cutoffs = [0]
    cursor = step
    while cursor < max_attempts:
        frame_cutoffs.append(cursor)
        cursor += step
    if frame_cutoffs[-1] != max_attempts:
        frame_cutoffs.append(max_attempts)

    return {
        "student_ids": [user_id for user_id in normalized_user_ids if user_id in series],
        "frame_cutoffs": frame_cutoffs,
        "step_size": step,
        "series": series,
        "max_attempts": max_attempts,
        "module_code": "M1",
        "module_label": module_label,
        "topology": topology,
    }


def _activity_state_for_prefix(
    user_series: dict[str, Any],
    visible_count: int,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    states: dict[str, dict[str, Any]] = {}
    mapped_sequence: list[str] = []
    for idx in range(visible_count):
        activity_id = str((user_series.get("activity_id") or [""])[idx] or "")
        if not activity_id:
            continue
        if not bool((user_series.get("is_mapped_activity") or [False])[idx]):
            continue
        state = states.setdefault(
            activity_id,
            {
                "attempts": 0,
                "successes": 0.0,
                "success_rate": None,
                "last_created_at": None,
                "last_work_mode": None,
            },
        )
        state["attempts"] += 1
        state["successes"] += float((user_series.get("outcome") or [0.0])[idx] or 0.0)
        state["success_rate"] = state["successes"] / state["attempts"] if state["attempts"] > 0 else None
        state["last_created_at"] = (user_series.get("created_at") or [""])[idx]
        state["last_work_mode"] = (user_series.get("work_mode") or [""])[idx]
        mapped_sequence.append(activity_id)
    return states, mapped_sequence


def _last_distinct_transitions(mapped_sequence: list[str], max_transitions: int = 3) -> list[tuple[str, str]]:
    if not mapped_sequence:
        return []
    compressed: list[str] = []
    for activity_id in mapped_sequence:
        if not compressed or compressed[-1] != activity_id:
            compressed.append(activity_id)
    transitions = list(zip(compressed[:-1], compressed[1:], strict=False))
    return transitions[-max(1, int(max_transitions)) :]


def _transition_arrow_points(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    offset: float = 0.18,
) -> tuple[float, float, float, float]:
    x0, y0 = start
    x1, y1 = end
    dx = x1 - x0
    dy = y1 - y0
    distance = max((dx * dx + dy * dy) ** 0.5, 1e-9)
    shrink = min(offset, distance / 3.0)
    sx = x0 + (dx / distance) * shrink
    sy = y0 + (dy / distance) * shrink
    ex = x1 - (dx / distance) * shrink
    ey = y1 - (dy / distance) * shrink
    return sx, sy, ex, ey


def build_m1_individual_path_figure(payload: dict[str, Any], frame_idx: int) -> go.Figure:
    """Build the frame-aware Module 1 replay figure."""
    student_ids = [str(user_id) for user_id in payload.get("student_ids") or [] if str(user_id).strip()]
    frame_cutoffs = payload.get("frame_cutoffs") or [0]
    current_frame_idx = min(max(0, int(frame_idx)), len(frame_cutoffs) - 1)
    cutoff = int(frame_cutoffs[current_frame_idx])
    topology = payload.get("topology") or {}
    node_positions = {
        str(key): tuple(value)
        for key, value in (topology.get("node_positions") or {}).items()
    }
    objective_rows = list(topology.get("objective_rows") or [])
    activity_rows = list(topology.get("activity_rows") or [])
    objective_codes = list(topology.get("objective_codes") or [])
    lane_pos = dict(topology.get("lane_pos") or {})
    max_activity_idx = int(topology.get("max_activity_idx") or 1)
    edge_rows = pl.DataFrame(topology.get("edge_rows") or [])

    fig = go.Figure()
    add_structural_dependency_traces(
        fig,
        edge_rows,
        node_positions,
        curve_intra_objective_edges=True,
    )

    objective_hover = (
        "<b>%{customdata[2]}</b><br>"
        + "Type: %{customdata[1]}<br>"
        + "Code: %{customdata[0]}<br>"
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

    user_series = ((payload.get("series") or {}).get(student_ids[0])) if student_ids else {}
    ordinals = [int(value) for value in (user_series or {}).get("attempt_ordinal") or []]
    visible_count = bisect_right(ordinals, cutoff)
    activity_state, mapped_sequence = _activity_state_for_prefix(user_series or {}, visible_count)
    recent_transitions = _last_distinct_transitions(mapped_sequence, max_transitions=3)
    activity_positions = {
        str(row.get("node_id") or ""): node_positions[str(row.get("node_code") or "")]
        for row in activity_rows
        if str(row.get("node_id") or "").strip() and str(row.get("node_code") or "") in node_positions
    }

    activity_hover = (
        "<b>%{customdata[1]}</b><br>"
        + "Code: %{customdata[0]}<br>"
        + "Objective lane: %{customdata[2]}<br>"
        + "Visible attempts: %{customdata[3]}<br>"
        + "Visible successes: %{customdata[4]}<br>"
        + "Visible success rate: %{customdata[5]}<br>"
        + "Last visible timestamp: %{customdata[6]}<br>"
        + "Last visible work mode: %{customdata[7]}"
        + "<extra></extra>"
    )
    base_customdata = []
    for row in activity_rows:
        activity_id = str(row.get("node_id") or "")
        state = activity_state.get(activity_id) or {}
        base_customdata.append(
            [
                str(row.get("node_code") or ""),
                str(row.get("label") or ""),
                str(row.get("objective_code") or ""),
                format_int(state.get("attempts")),
                format_int(state.get("successes")),
                format_rate(state.get("success_rate")),
                str(state.get("last_created_at") or "n/a"),
                str(state.get("last_work_mode") or "n/a"),
            ]
        )

    fig.add_trace(
        go.Scatter(
            x=[node_positions[str(row.get("node_code"))][0] for row in activity_rows],
            y=[node_positions[str(row.get("node_code"))][1] for row in activity_rows],
            mode="markers",
            showlegend=False,
            hovertemplate=activity_hover,
            customdata=base_customdata,
            marker={
                "size": 14,
                "symbol": ["diamond-open" if bool(row.get("is_ghost")) else "circle" for row in activity_rows],
                "color": "rgba(122, 127, 137, 0.35)",
                "line": {"width": 1.5, "color": "#1b1d22"},
            },
        )
    )

    visited_rows = [row for row in activity_rows if str(row.get("node_id") or "") in activity_state]
    if visited_rows:
        full_max_attempts = max(
            1,
            int((user_series or {}).get("max_activity_attempt_count") or 0),
        )
        overlay_customdata = []
        overlay_sizes = []
        overlay_colors = []
        for row in visited_rows:
            activity_id = str(row.get("node_id") or "")
            state = activity_state.get(activity_id) or {}
            attempts = int(state.get("attempts") or 0)
            overlay_customdata.append(
                [
                    str(row.get("node_code") or ""),
                    str(row.get("label") or ""),
                    str(row.get("objective_code") or ""),
                    format_int(attempts),
                    format_int(state.get("successes")),
                    format_rate(state.get("success_rate")),
                    str(state.get("last_created_at") or "n/a"),
                    str(state.get("last_work_mode") or "n/a"),
                ]
            )
            overlay_sizes.append(14.0 + (30.0 - 14.0) * sqrt(attempts / full_max_attempts))
            overlay_colors.append(float(state.get("success_rate") or 0.0))

        fig.add_trace(
            go.Scatter(
                x=[activity_positions[str(row.get("node_id") or "")][0] for row in visited_rows],
                y=[activity_positions[str(row.get("node_id") or "")][1] for row in visited_rows],
                mode="markers",
                showlegend=False,
                hovertemplate=activity_hover,
                customdata=overlay_customdata,
                marker={
                    "size": overlay_sizes,
                    "symbol": ["diamond-open" if bool(row.get("is_ghost")) else "circle" for row in visited_rows],
                    "color": overlay_colors,
                    "cmin": 0.0,
                    "cmax": 1.0,
                    "colorscale": [[0.0, "#d4483b"], [0.5, "#f1c45b"], [1.0, "#1f7a4f"]],
                    "showscale": True,
                    "colorbar": {"title": "Cumulative success"},
                    "line": {"width": 1.5, "color": "#1b1d22"},
                },
            )
        )

    for idx, (from_activity_id, to_activity_id) in enumerate(recent_transitions):
        if from_activity_id not in activity_positions or to_activity_id not in activity_positions:
            continue
        alpha = [0.35, 0.60, 0.90][max(0, 3 - len(recent_transitions)) + idx]
        start = activity_positions[from_activity_id]
        end = activity_positions[to_activity_id]
        sx, sy, ex, ey = _transition_arrow_points(start, end)
        fig.add_annotation(
            x=ex,
            y=ey,
            ax=sx,
            ay=sy,
            xref="x",
            yref="y",
            axref="x",
            ayref="y",
            text="",
            showarrow=True,
            arrowhead=3,
            arrowsize=1.1,
            arrowwidth=2.6,
            arrowcolor=f"rgba(33, 72, 164, {alpha:.2f})",
        )

    fig.update_layout(
        height=max(520, 80 * max(1, len(objective_codes)) + 140),
        margin={"l": 130, "r": 70, "t": 30, "b": 60},
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


__all__ = [
    "build_m1_individual_path_figure",
    "build_m1_individual_path_payload",
    "load_m1_student_profiles",
    "select_m1_student_by_id",
    "select_m1_students_near_attempt_target",
]

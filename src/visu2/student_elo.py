from __future__ import annotations

from bisect import bisect_right
from math import ceil
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import polars as pl


def load_student_elo_profiles(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def load_student_elo_events(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(path)


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def select_default_students(
    profiles: pl.DataFrame,
    min_attempts: int,
    max_students: int = 2,
) -> list[str]:
    if profiles.height == 0:
        return []
    limit = max(1, int(max_students))
    filtered = profiles.filter(
        pl.col("eligible_for_replay")
        & (pl.col("total_attempts") >= max(1, int(min_attempts)))
    ).sort(
        ["total_attempts", "final_student_elo", "user_id"],
        descending=[False, False, False],
    )
    if filtered.height == 0:
        return []

    # Use representative defaults rather than the heaviest users only:
    # upper-mid percentile first, then median, then lower quartile.
    percentile_targets = [0.75, 0.50, 0.25]
    selected_indices: list[int] = []
    max_index = filtered.height - 1

    def _nearest_available(target_idx: int) -> int:
        if target_idx not in selected_indices:
            return target_idx
        for offset in range(1, filtered.height):
            lower = target_idx - offset
            if lower >= 0 and lower not in selected_indices:
                return lower
            upper = target_idx + offset
            if upper <= max_index and upper not in selected_indices:
                return upper
        return target_idx

    for percentile in percentile_targets:
        if len(selected_indices) >= limit:
            break
        raw_idx = int(ceil(filtered.height * percentile) - 1)
        target_idx = min(max(0, raw_idx), max_index)
        resolved_idx = _nearest_available(target_idx)
        if resolved_idx not in selected_indices:
            selected_indices.append(resolved_idx)

    if len(selected_indices) < limit:
        for idx in range(max_index, -1, -1):
            if idx not in selected_indices:
                selected_indices.append(idx)
            if len(selected_indices) >= limit:
                break

    rows = filtered.select("user_id").to_series().to_list()
    return [str(rows[idx]) for idx in selected_indices[:limit]]


def _empty_payload(user_ids: list[str], step_size: int) -> dict[str, Any]:
    return {
        "student_ids": user_ids,
        "frame_cutoffs": [0],
        "step_size": max(1, int(step_size)),
        "series": {},
        "max_attempts": 0,
    }


def build_student_elo_payload(
    events: pl.DataFrame | pl.LazyFrame,
    user_ids: list[str],
    step_size: int,
) -> dict[str, Any]:
    normalized_user_ids = []
    for user_id in user_ids:
        text = str(user_id or "").strip()
        if text and text not in normalized_user_ids:
            normalized_user_ids.append(text)
    normalized_user_ids = normalized_user_ids[:2]
    if not normalized_user_ids:
        return _empty_payload([], step_size)

    step = max(1, int(step_size))
    frame = (
        _as_lazy(events)
        .filter(pl.col("user_id").cast(pl.Utf8).is_in(normalized_user_ids))
        .select(
            [
                "user_id",
                "attempt_ordinal",
                "created_at",
                "date_utc",
                "work_mode",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "outcome",
                "expected_success",
                "exercise_elo",
                "student_elo_pre",
                "student_elo_post",
            ]
        )
        .sort(["user_id", "attempt_ordinal"])
        .collect()
    )
    if frame.height == 0:
        return _empty_payload(normalized_user_ids, step)

    series: dict[str, dict[str, list[Any]]] = {}
    max_attempts = 0
    for row in frame.to_dicts():
        user_id = str(row.get("user_id") or "").strip()
        if not user_id:
            continue
        bucket = series.setdefault(
            user_id,
            {
                "attempt_ordinal": [],
                "student_elo_post": [],
                "created_at": [],
                "exercise_id": [],
                "activity_id": [],
                "work_mode": [],
                "module_code": [],
                "outcome": [],
                "expected_success": [],
                "exercise_elo": [],
                "student_elo_pre": [],
                "student_elo_post_hover": [],
            },
        )
        ordinal = int(row.get("attempt_ordinal") or 0)
        if ordinal <= 0:
            continue
        bucket["attempt_ordinal"].append(ordinal)
        bucket["student_elo_post"].append(float(row.get("student_elo_post") or 0.0))
        bucket["created_at"].append(
            row.get("created_at").isoformat() if hasattr(row.get("created_at"), "isoformat") else None
        )
        bucket["exercise_id"].append(None if row.get("exercise_id") is None else str(row.get("exercise_id")))
        bucket["activity_id"].append(None if row.get("activity_id") is None else str(row.get("activity_id")))
        bucket["work_mode"].append(None if row.get("work_mode") is None else str(row.get("work_mode")))
        bucket["module_code"].append(None if row.get("module_code") is None else str(row.get("module_code")))
        bucket["outcome"].append(float(row.get("outcome") or 0.0))
        bucket["expected_success"].append(float(row.get("expected_success") or 0.0))
        bucket["exercise_elo"].append(float(row.get("exercise_elo") or 0.0))
        bucket["student_elo_pre"].append(float(row.get("student_elo_pre") or 0.0))
        bucket["student_elo_post_hover"].append(float(row.get("student_elo_post") or 0.0))
        max_attempts = max(max_attempts, ordinal)

    if not series:
        return _empty_payload(normalized_user_ids, step)

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
    }


def build_student_elo_figure(payload: dict[str, Any], frame_idx: int) -> go.Figure:
    student_ids = [str(user_id) for user_id in payload.get("student_ids") or []]
    frame_cutoffs = payload.get("frame_cutoffs") or [0]
    current_frame_idx = min(max(0, int(frame_idx)), len(frame_cutoffs) - 1)
    cutoff = int(frame_cutoffs[current_frame_idx])
    series = payload.get("series") or {}

    colors = ["#1e7a52", "#2148a4"]
    fig = go.Figure()

    for idx, user_id in enumerate(student_ids):
        user_series = series.get(user_id)
        if not isinstance(user_series, dict):
            continue
        ordinals = [int(value) for value in user_series.get("attempt_ordinal") or []]
        visible_count = bisect_right(ordinals, cutoff)
        if visible_count <= 0:
            continue
        customdata = list(
            zip(
                user_series.get("created_at", [])[:visible_count],
                user_series.get("exercise_id", [])[:visible_count],
                user_series.get("activity_id", [])[:visible_count],
                user_series.get("work_mode", [])[:visible_count],
                user_series.get("module_code", [])[:visible_count],
                user_series.get("outcome", [])[:visible_count],
                user_series.get("expected_success", [])[:visible_count],
                user_series.get("exercise_elo", [])[:visible_count],
                user_series.get("student_elo_pre", [])[:visible_count],
                user_series.get("student_elo_post_hover", [])[:visible_count],
            )
        )
        fig.add_trace(
            go.Scatter(
                x=ordinals[:visible_count],
                y=(user_series.get("student_elo_post") or [])[:visible_count],
                mode="lines+markers",
                name=user_id,
                line={"width": 3, "color": colors[idx % len(colors)]},
                marker={"size": 7, "color": colors[idx % len(colors)]},
                customdata=customdata,
                hovertemplate=(
                    "<b>User</b>: %{fullData.name}<br>"
                    "<b>Attempt</b>: %{x}<br>"
                    "<b>Elo</b>: %{y:.1f}<br>"
                    "<b>Timestamp</b>: %{customdata[0]}<br>"
                    "<b>Exercise</b>: %{customdata[1]}<br>"
                    "<b>Activity</b>: %{customdata[2]}<br>"
                    "<b>Work mode</b>: %{customdata[3]}<br>"
                    "<b>Module</b>: %{customdata[4]}<br>"
                    "<b>Outcome</b>: %{customdata[5]:.0f}<br>"
                    "<b>Expected success</b>: %{customdata[6]:.3f}<br>"
                    "<b>Exercise Elo</b>: %{customdata[7]:.1f}<br>"
                    "<b>Student Elo (pre)</b>: %{customdata[8]:.1f}<br>"
                    "<b>Student Elo (post)</b>: %{customdata[9]:.1f}"
                    "<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title=f"Student Elo replay up to local attempt {cutoff}",
        xaxis_title="Student-local attempt ordinal",
        yaxis_title="Student Elo",
        height=540,
        margin={"l": 56, "r": 24, "t": 72, "b": 56},
        hovermode="x unified",
        font={"size": 13},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(23,34,27,0.08)")
    return fig

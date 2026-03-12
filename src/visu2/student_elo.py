"""
student_elo.py

Prepare student Elo replay payloads and Plotly traces for page-level rendering.

Dependencies
------------
- bisect
- math
- pathlib
- plotly
- polars
- typing

Classes
-------
- None.

Functions
---------
- load_student_elo_profiles: Load student elo profiles.
- load_student_elo_events: Load student elo events.
- _as_lazy: Utility for as lazy.
- select_default_students: Select default students.
- _empty_payload: Utility for empty payload.
- build_student_elo_payload: Build student elo payload.
- build_student_elo_figure: Build student elo figure.
"""
from __future__ import annotations

from bisect import bisect_right
from math import ceil
from pathlib import Path
from random import Random
from typing import Any

import plotly.graph_objects as go
import polars as pl

from visu2.loaders import catalog_to_summary_frames, load_learning_catalog


def load_student_elo_profiles(path: Path) -> pl.DataFrame:
    """Load student elo profiles.

Parameters
----------
path : Path
        Input parameter used by this routine.

Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.read_parquet(path)


def load_student_elo_events(path: Path) -> pl.LazyFrame:
    """Load student elo events.

Parameters
----------
path : Path
        Input parameter used by this routine.

Returns
-------
pl.LazyFrame
        Result produced by this routine.

"""
    return pl.scan_parquet(path)


def load_student_elo_label_lookup(path: Path) -> pl.DataFrame:
    """Load a readable label lookup for student Elo hover content.

    Parameters
    ----------
    path : Path
        Path to `learning_catalog.json`.

    Returns
    -------
    pl.DataFrame
        Activity-level label lookup keyed by activity, objective, and module.
    """
    catalog = load_learning_catalog(path)
    frames = catalog_to_summary_frames(catalog)
    return frames.activity_hierarchy.select(
        [
            "activity_id",
            "module_code",
            "module_label",
            "objective_id",
            "objective_label",
            "activity_label",
        ]
    ).unique()


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """As lazy.

Parameters
----------
frame : pl.DataFrame | pl.LazyFrame
        Input parameter used by this routine.

Returns
-------
pl.LazyFrame
        Result produced by this routine.

"""
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def select_default_students(
    profiles: pl.DataFrame,
    min_attempts: int,
    max_students: int = 2,
) -> list[str]:
    """Select default students.

Parameters
----------
profiles : pl.DataFrame
        Input parameter used by this routine.
min_attempts : int
        Input parameter used by this routine.
max_students : int
        Input parameter used by this routine.

Returns
-------
list[str]
        Result produced by this routine.

"""
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
        """Nearest available.

Parameters
----------
target_idx : int
            Input parameter used by this routine.

Returns
-------
int
            Result produced by this routine.

Notes
-----
        Behavior is intentionally documented for maintainability and traceability.
"""
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


def select_students_near_attempt_target(
    profiles: pl.DataFrame,
    target_attempts: int,
    tolerance_ratio: float = 0.10,
    max_students: int = 2,
    seed: int | None = None,
) -> list[str]:
    """Select up to ``max_students`` eligible students near a target attempt count.

    Parameters
    ----------
    profiles : pl.DataFrame
        Student Elo profile table.
    target_attempts : int
        Desired number of total attempts around which students are sampled.
    tolerance_ratio : float, optional
        Relative half-width of the acceptance band. A value of ``0.10`` means
        ``target_attempts +/- 10%``.
    max_students : int, optional
        Maximum number of students to return.
    seed : int | None, optional
        Optional random seed used to make sampling deterministic in tests.

    Returns
    -------
    list[str]
        Randomly sampled student identifiers from the eligible band.
    """
    if profiles.height == 0:
        return []

    target = max(1, int(target_attempts))
    tolerance = max(0.0, float(tolerance_ratio))
    limit = max(1, int(max_students))
    lower = int(target * (1.0 - tolerance))
    upper = int(ceil(target * (1.0 + tolerance)))

    candidates = (
        profiles.filter(
            pl.col("eligible_for_replay")
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


def _empty_payload(user_ids: list[str], step_size: int) -> dict[str, Any]:
    """Empty payload.

Parameters
----------
user_ids : list[str]
        Input parameter used by this routine.
step_size : int
        Input parameter used by this routine.

Returns
-------
dict[str, Any]
        Result produced by this routine.

"""
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
    label_lookup: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """Build student elo payload.

Parameters
----------
events : pl.DataFrame | pl.LazyFrame
        Input parameter used by this routine.
user_ids : list[str]
        Input parameter used by this routine.
step_size : int
        Input parameter used by this routine.

Returns
-------
dict[str, Any]
        Result produced by this routine.

"""
    normalized_user_ids = []
    for user_id in user_ids:
        text = str(user_id or "").strip()
        if text and text not in normalized_user_ids:
            normalized_user_ids.append(text)
    normalized_user_ids = normalized_user_ids[:2]
    if not normalized_user_ids:
        return _empty_payload([], step_size)

    step = max(1, int(step_size))
    events_lazy = _as_lazy(events).filter(pl.col("user_id").cast(pl.Utf8).is_in(normalized_user_ids))
    if label_lookup is not None and label_lookup.height > 0:
        events_lazy = events_lazy.join(
            label_lookup.lazy(),
            on=["activity_id", "objective_id", "module_code"],
            how="left",
        )
    else:
        events_lazy = events_lazy.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("module_label"),
                pl.lit(None, dtype=pl.Utf8).alias("objective_label"),
                pl.lit(None, dtype=pl.Utf8).alias("activity_label"),
            ]
        )

    frame = (
        events_lazy.select(
            [
                "user_id",
                "attempt_ordinal",
                "created_at",
                "date_utc",
                "work_mode",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
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
                "activity_label": [],
                "work_mode": [],
                "module_code": [],
                "module_label": [],
                "objective_label": [],
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
        bucket["activity_label"].append(
            None if row.get("activity_label") is None else str(row.get("activity_label"))
        )
        bucket["work_mode"].append(None if row.get("work_mode") is None else str(row.get("work_mode")))
        bucket["module_code"].append(None if row.get("module_code") is None else str(row.get("module_code")))
        bucket["module_label"].append(
            None if row.get("module_label") is None else str(row.get("module_label"))
        )
        bucket["objective_label"].append(
            None if row.get("objective_label") is None else str(row.get("objective_label"))
        )
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
    """Build student elo figure.

Parameters
----------
payload : dict[str, Any]
        Input parameter used by this routine.
frame_idx : int
        Input parameter used by this routine.

Returns
-------
go.Figure
        Result produced by this routine.

"""
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
                user_series.get("activity_label", [])[:visible_count],
                user_series.get("objective_label", [])[:visible_count],
                user_series.get("work_mode", [])[:visible_count],
                user_series.get("module_label", [])[:visible_count],
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
                    "<b>Objective</b>: %{customdata[3]}<br>"
                    "<b>Work mode</b>: %{customdata[4]}<br>"
                    "<b>Module</b>: %{customdata[5]}<br>"
                    "<b>Outcome</b>: %{customdata[6]:.0f}<br>"
                    "<b>Expected success</b>: %{customdata[7]:.3f}<br>"
                    "<b>Exercise Elo</b>: %{customdata[8]:.1f}<br>"
                    "<b>Student Elo (pre)</b>: %{customdata[9]:.1f}<br>"
                    "<b>Student Elo (post)</b>: %{customdata[10]:.1f}"
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

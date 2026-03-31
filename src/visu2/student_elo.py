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
from datetime import datetime
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


def load_student_elo_label_lookup(
    path: Path,
    exercise_elo_path: Path | None = None,
) -> pl.DataFrame:
    """Load a readable label lookup for student Elo hover content.

    Parameters
    ----------
    path : Path
        Path to `learning_catalog.json`.
    exercise_elo_path : Path | None, optional
        Optional path to `agg_exercise_elo.parquet` so orphan exercises can
        reuse the fallback activity/objective labels created during calibration.

    Returns
    -------
    pl.DataFrame
        Activity-level label lookup keyed by activity, objective, and module.
    """
    catalog = load_learning_catalog(path)
    frames = catalog_to_summary_frames(catalog)
    catalog_lookup = frames.activity_hierarchy.select(
        [
            "activity_id",
            "module_code",
            "module_label",
            "objective_id",
            "objective_label",
            "activity_label",
        ]
    ).unique()
    if exercise_elo_path is None or not exercise_elo_path.exists():
        return catalog_lookup
    orphan_lookup = (
        pl.read_parquet(exercise_elo_path)
        .select(
            [
                "activity_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_label",
            ]
        )
        .filter(
            pl.col("module_code").is_not_null()
            & pl.col("objective_id").is_not_null()
            & pl.col("activity_id").is_not_null()
        )
        .unique(subset=["activity_id", "module_code", "objective_id"], keep="first")
    )
    return pl.concat([catalog_lookup, orphan_lookup], how="diagonal_relaxed").unique(
        subset=["activity_id", "module_code", "objective_id"],
        keep="first",
    )


def merge_student_elo_label_lookups(*lookups: pl.DataFrame | None) -> pl.DataFrame:
    """Merge label lookup frames while preserving first-frame precedence."""
    normalized_frames: list[pl.DataFrame] = []
    for lookup in lookups:
        if lookup is None or lookup.height == 0:
            continue
        normalized_frames.append(
            lookup.select(
                [
                    "activity_id",
                    "module_code",
                    "module_label",
                    "objective_id",
                    "objective_label",
                    "activity_label",
                ]
            )
        )
    if not normalized_frames:
        return pl.DataFrame(
            {
                "activity_id": [],
                "module_code": [],
                "module_label": [],
                "objective_id": [],
                "objective_label": [],
                "activity_label": [],
            },
            schema={
                "activity_id": pl.Utf8,
                "module_code": pl.Utf8,
                "module_label": pl.Utf8,
                "objective_id": pl.Utf8,
                "objective_label": pl.Utf8,
                "activity_label": pl.Utf8,
            },
        )
    return pl.concat(normalized_frames, how="diagonal_relaxed").unique(
        subset=["activity_id", "module_code", "objective_id"],
        keep="first",
    )


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


def summarize_student_module_profiles(profiles: pl.DataFrame) -> pl.DataFrame:
    """Aggregate module-local Sequential Replay Elo profiles into one student-level selector summary."""
    if profiles.height == 0:
        return pl.DataFrame(
            {
                "user_id": [],
                "total_attempts": [],
                "first_attempt_at": [],
                "last_attempt_at": [],
                "unique_modules": [],
                "final_student_elo": [],
                "eligible_for_replay": [],
            },
            schema={
                "user_id": pl.Utf8,
                "total_attempts": pl.Int64,
                "first_attempt_at": pl.Datetime,
                "last_attempt_at": pl.Datetime,
                "unique_modules": pl.Int64,
                "final_student_elo": pl.Float64,
                "eligible_for_replay": pl.Boolean,
            },
        )
    return (
        profiles.lazy()
        .group_by("user_id")
        .agg(
            pl.col("total_attempts").sum().cast(pl.Int64).alias("total_attempts"),
            pl.col("first_attempt_at").min().alias("first_attempt_at"),
            pl.col("last_attempt_at").max().alias("last_attempt_at"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("unique_modules"),
            pl.col("final_student_elo")
            .sort_by(["total_attempts", "module_code"], descending=[True, False])
            .first()
            .alias("final_student_elo"),
            pl.col("eligible_for_replay").fill_null(False).any().alias("eligible_for_replay"),
        )
        .sort(["total_attempts", "user_id"], descending=[True, False])
        .collect()
    )


def modules_for_student(
    profiles: pl.DataFrame,
    user_id: str,
) -> pl.DataFrame:
    """Return the available module-local profile rows for one student."""
    normalized = str(user_id or "").strip()
    if not normalized or profiles.height == 0:
        return profiles.head(0)
    return (
        profiles.filter(pl.col("user_id").cast(pl.Utf8) == normalized)
        .sort(["total_attempts", "module_code"], descending=[True, False])
    )


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


def select_student_by_id(
    profiles: pl.DataFrame,
    user_id: str,
) -> str | None:
    """Return one replay-eligible student matching an explicit identifier."""
    normalized = str(user_id or "").strip()
    if not normalized or profiles.height == 0:
        return None
    matches = profiles.filter(
        pl.col("eligible_for_replay")
        & (pl.col("user_id").cast(pl.Utf8) == normalized)
    )
    if matches.height == 0:
        return None
    return normalized


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


def build_student_elo_comparison_payload(
    left_events: pl.DataFrame | pl.LazyFrame,
    right_events: pl.DataFrame | pl.LazyFrame,
    user_ids: list[str],
    step_size: int,
    label_lookup: pl.DataFrame | None = None,
    system_labels: tuple[str, str] = ("Sequential Replay Elo", "Iterative Elo"),
) -> dict[str, Any]:
    """Build a synchronized comparison payload for two Elo systems."""
    left_label, right_label = system_labels
    left_payload = build_student_elo_payload(
        left_events,
        user_ids,
        step_size,
        label_lookup=label_lookup,
    )
    right_payload = build_student_elo_payload(
        right_events,
        user_ids,
        step_size,
        label_lookup=label_lookup,
    )
    left_ids = left_payload.get("student_ids") or []
    right_ids = right_payload.get("student_ids") or []
    if left_ids != right_ids:
        raise ValueError("Compared Elo payloads do not expose the same selected students.")
    if (left_payload.get("frame_cutoffs") or [0]) != (right_payload.get("frame_cutoffs") or [0]):
        raise ValueError("Compared Elo payloads do not share the same replay frames.")

    for user_id in left_ids:
        left_series = (left_payload.get("series") or {}).get(user_id) or {}
        right_series = (right_payload.get("series") or {}).get(user_id) or {}
        left_ordinals = [int(value) for value in left_series.get("attempt_ordinal") or []]
        right_ordinals = [int(value) for value in right_series.get("attempt_ordinal") or []]
        if left_ordinals != right_ordinals:
            raise ValueError(
                f"Compared Elo attempt ordinals do not align for student {user_id}."
            )

    return {
        "student_ids": list(left_ids),
        "frame_cutoffs": list(left_payload.get("frame_cutoffs") or [0]),
        "step_size": int(left_payload.get("step_size") or step_size),
        "max_attempts": int(left_payload.get("max_attempts") or 0),
        "systems": (left_label, right_label),
        "series": {
            left_label: left_payload.get("series") or {},
            right_label: right_payload.get("series") or {},
        },
    }


def build_student_elo_figure(
    payload: dict[str, Any],
    frame_idx: int,
    gap_days_threshold: float | None = None,
    system_label: str | None = None,
) -> go.Figure:
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

    if gap_days_threshold is not None and gap_days_threshold > 0 and student_ids:
        first_series = series.get(student_ids[0]) or {}
        timestamps = list(first_series.get("created_at") or [])
        ordinals = [int(value) for value in first_series.get("attempt_ordinal") or []]
        visible_count = bisect_right(ordinals, cutoff)
        threshold_seconds = float(gap_days_threshold) * 86400.0
        for idx in range(1, visible_count):
            previous_raw = timestamps[idx - 1]
            current_raw = timestamps[idx]
            if not previous_raw or not current_raw:
                continue
            previous_dt = datetime.fromisoformat(str(previous_raw))
            current_dt = datetime.fromisoformat(str(current_raw))
            delta_seconds = (current_dt - previous_dt).total_seconds()
            if delta_seconds < threshold_seconds:
                continue
            fig.add_vline(
                x=ordinals[idx],
                line_width=1.5,
                line_dash="dot",
                line_color="rgba(95, 104, 118, 0.65)",
                annotation_text=_format_gap_label(delta_seconds),
                annotation_position="top",
                annotation_font={"size": 11, "color": "rgba(23,34,27,0.80)"},
            )

    fig.update_layout(
        title=f"{system_label or 'Student Elo'} module-local replay up to local attempt {cutoff}",
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


def _format_gap_label(delta_seconds: float) -> str:
    if delta_seconds >= 86400:
        return f"{delta_seconds / 86400:.0f}d"
    if delta_seconds >= 3600:
        return f"{delta_seconds / 3600:.0f}h"
    return f"{delta_seconds / 60:.0f}m"


def _work_mode_to_symbol(work_mode: str | None) -> str:
    normalized = str(work_mode or "").strip().lower()
    if normalized == "zpdes":
        return "triangle-up"
    if normalized == "adaptive-test":
        return "circle"
    if normalized == "playlist":
        return "diamond"
    if normalized == "initial-test":
        return "square"
    return "hexagon"


def build_student_elo_comparison_figure(
    payload: dict[str, Any],
    frame_idx: int,
    gap_days_threshold: float | None = None,
    visible_systems: tuple[str, ...] | None = None,
) -> go.Figure:
    """Build a comparison figure overlaying multiple Elo systems."""
    student_ids = [str(user_id) for user_id in payload.get("student_ids") or []]
    frame_cutoffs = payload.get("frame_cutoffs") or [0]
    current_frame_idx = min(max(0, int(frame_idx)), len(frame_cutoffs) - 1)
    cutoff = int(frame_cutoffs[current_frame_idx])
    systems = list(payload.get("systems") or ["Sequential Replay Elo", "Iterative Elo"])
    if visible_systems:
        allowed = {str(system) for system in visible_systems if str(system).strip()}
        systems = [system for system in systems if system in allowed]
    series = payload.get("series") or {}

    system_colors = {
        "Sequential Replay Elo": "#1e7a52",
        "Batch Replay Elo": "#2148a4",
        "Iterative Elo": "#2148a4",
    }
    single_student = len(student_ids) == 1
    fig = go.Figure()

    for user_id in student_ids:
        for system in systems:
            user_series = ((series.get(system) or {}).get(user_id)) or {}
            ordinals = [int(value) for value in user_series.get("attempt_ordinal") or []]
            visible_count = bisect_right(ordinals, cutoff)
            if visible_count <= 0:
                continue
            color = system_colors.get(system, "#6B7280")
            marker_symbols = [
                _work_mode_to_symbol(work_mode)
                for work_mode in (user_series.get("work_mode") or [])[:visible_count]
            ]
            customdata = list(
                zip(
                    [system] * visible_count,
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
                    name=system if single_student else f"{user_id} | {system}",
                    legendgroup=user_id,
                    line={"width": 3, "color": color, "dash": "solid"},
                    marker={
                        "size": 7,
                        "color": color,
                        "symbol": marker_symbols,
                        "line": {"width": 0.6, "color": "rgba(23,34,27,0.45)"},
                    },
                    customdata=customdata,
                    hovertemplate=(
                        "<b>User</b>: %{fullData.legendgroup}<br>"
                        "<b>System</b>: %{customdata[0]}<br>"
                        "<b>Attempt</b>: %{x}<br>"
                        "<b>Elo</b>: %{y:.1f}<br>"
                        "<b>Timestamp</b>: %{customdata[1]}<br>"
                        "<b>Exercise</b>: %{customdata[2]}<br>"
                        "<b>Activity</b>: %{customdata[3]}<br>"
                        "<b>Objective</b>: %{customdata[4]}<br>"
                        "<b>Work mode</b>: %{customdata[5]}<br>"
                        "<b>Module</b>: %{customdata[6]}<br>"
                        "<b>Outcome</b>: %{customdata[7]:.0f}<br>"
                        "<b>Expected success</b>: %{customdata[8]:.3f}<br>"
                        "<b>Exercise difficulty</b>: %{customdata[9]:.1f}<br>"
                        "<b>Student Elo (pre)</b>: %{customdata[10]:.1f}<br>"
                        "<b>Student Elo (post)</b>: %{customdata[11]:.1f}"
                        "<extra></extra>"
                    ),
                )
            )

    if gap_days_threshold is not None and gap_days_threshold > 0 and student_ids:
        current_series = ((series.get("Sequential Replay Elo") or {}).get(student_ids[0])) or {}
        timestamps = list(current_series.get("created_at") or [])
        ordinals = [int(value) for value in current_series.get("attempt_ordinal") or []]
        visible_count = bisect_right(ordinals, cutoff)
        threshold_seconds = float(gap_days_threshold) * 86400.0
        for idx in range(1, visible_count):
            previous_raw = timestamps[idx - 1]
            current_raw = timestamps[idx]
            if not previous_raw or not current_raw:
                continue
            previous_dt = datetime.fromisoformat(str(previous_raw))
            current_dt = datetime.fromisoformat(str(current_raw))
            delta_seconds = (current_dt - previous_dt).total_seconds()
            if delta_seconds < threshold_seconds:
                continue
            fig.add_vline(
                x=ordinals[idx],
                line_width=1.5,
                line_dash="dot",
                line_color="rgba(95, 104, 118, 0.65)",
                annotation_text=_format_gap_label(delta_seconds),
                annotation_position="top",
                annotation_font={"size": 11, "color": "rgba(23,34,27,0.80)"},
            )

    fig.update_layout(
        title=f"Student Elo comparison up to local attempt {cutoff}",
        xaxis_title="Student-local attempt ordinal",
        yaxis_title="Student Elo",
        height=560,
        margin={"l": 56, "r": 24, "t": 72, "b": 56},
        hovermode="x unified",
        font={"size": 13},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(23,34,27,0.08)")
    return fig

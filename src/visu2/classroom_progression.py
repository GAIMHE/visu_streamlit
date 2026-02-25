from __future__ import annotations

import math
from datetime import date
from typing import Any

import plotly.graph_objects as go
import polars as pl

VALID_MODE_SCOPES = ("zpdes", "playlist", "all")

_PROFILE_SCHEMA: dict[str, pl.DataType] = {
    "mode_scope": pl.Utf8,
    "classroom_id": pl.Utf8,
    "students": pl.Int64,
    "activities": pl.Int64,
    "objectives": pl.Int64,
    "modules": pl.Int64,
    "attempts": pl.Int64,
    "first_attempt_at": pl.Datetime(time_zone="UTC"),
    "last_attempt_at": pl.Datetime(time_zone="UTC"),
}

_REPLAY_REQUIRED_COLUMNS = [
    "created_at",
    "date_utc",
    "user_id",
    "activity_id",
    "activity_label",
    "data_correct",
    "work_mode",
    "classroom_id",
    "objective_id",
    "module_code",
    "exercise_id",
    "attempt_number",
]


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def _valid_classroom_filter() -> pl.Expr:
    return pl.col("classroom_id").is_not_null() & (pl.col("classroom_id").cast(pl.Utf8) != "None")


def _assert_required_columns(frame: pl.DataFrame | pl.LazyFrame, required: list[str]) -> None:
    columns = frame.collect_schema().names() if isinstance(frame, pl.LazyFrame) else frame.columns
    missing = [column for column in required if column not in columns]
    if missing:
        raise ValueError(f"Replay source is missing required columns: {missing}")


def _empty_profiles() -> pl.DataFrame:
    return pl.DataFrame(
        {name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in _PROFILE_SCHEMA.items()}
    )


def _empty_payload(classroom_id: str, mode_scope: str, start_date: date, end_date: date) -> dict[str, Any]:
    return {
        "classroom_id": classroom_id,
        "mode_scope": mode_scope,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "student_ids": [],
        "student_axis_labels": [],
        "activity_ids": [],
        "activity_axis_labels": [],
        "activity_full_labels": [],
        "frame_step_counts": [0],
        "frame_event_counts": [0],
        "frame_timestamps": [None],
        "rate_frames": [[]],
        "attempt_frames": [[]],
        "total_events_raw": 0,
        "total_events_valid_timestamp": 0,
        "total_sync_steps": 0,
        "dropped_invalid_timestamps": 0,
        "requested_step_size": 1,
        "effective_step": 1,
        "max_frames": 0,
        "events_capped": False,
    }


def _build_frame_step_counts(total_steps: int, effective_step: int) -> list[int]:
    if total_steps <= 0:
        return [0]
    counts = [0]
    cursor = max(1, effective_step)
    while cursor < total_steps:
        counts.append(cursor)
        cursor += max(1, effective_step)
    if counts[-1] != total_steps:
        counts.append(total_steps)
    return counts


def _serialize_timestamp(value: object) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _matrix_rate_snapshot(
    success_matrix: list[list[int]],
    attempt_matrix: list[list[int]],
) -> list[list[float | None]]:
    out: list[list[float | None]] = []
    for activity_idx in range(len(attempt_matrix)):
        row: list[float | None] = []
        for student_idx in range(len(attempt_matrix[activity_idx])):
            attempts = attempt_matrix[activity_idx][student_idx]
            if attempts <= 0:
                row.append(None)
            else:
                rate = success_matrix[activity_idx][student_idx] / attempts
                row.append(min(1.0, max(0.0, float(rate))))
        out.append(row)
    return out


def _matrix_attempt_snapshot(attempt_matrix: list[list[int]]) -> list[list[int]]:
    return [row[:] for row in attempt_matrix]


def _clip_threshold(threshold: float) -> float:
    return min(1.0, max(0.0, float(threshold)))


def build_classroom_mode_profiles(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Return per-(mode, classroom) and all-mode classroom profiles."""
    _assert_required_columns(fact, _REPLAY_REQUIRED_COLUMNS)
    lf = (
        _as_lazy(fact)
        .filter(_valid_classroom_filter())
        .with_columns(
            pl.col("classroom_id").cast(pl.Utf8),
            pl.col("work_mode").cast(pl.Utf8),
        )
    )

    by_mode = (
        lf.group_by(["work_mode", "classroom_id"])
        .agg(
            pl.col("user_id").n_unique().cast(pl.Int64).alias("students"),
            pl.col("activity_id").drop_nulls().n_unique().cast(pl.Int64).alias("activities"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("objectives"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("modules"),
            pl.len().cast(pl.Int64).alias("attempts"),
            pl.col("created_at").min().alias("first_attempt_at"),
            pl.col("created_at").max().alias("last_attempt_at"),
        )
        .rename({"work_mode": "mode_scope"})
    )

    all_mode = (
        lf.group_by("classroom_id")
        .agg(
            pl.col("user_id").n_unique().cast(pl.Int64).alias("students"),
            pl.col("activity_id").drop_nulls().n_unique().cast(pl.Int64).alias("activities"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("objectives"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("modules"),
            pl.len().cast(pl.Int64).alias("attempts"),
            pl.col("created_at").min().alias("first_attempt_at"),
            pl.col("created_at").max().alias("last_attempt_at"),
        )
        .with_columns(pl.lit("all").alias("mode_scope"))
    )

    by_mode_df = by_mode.collect().select(list(_PROFILE_SCHEMA.keys()))
    all_mode_df = all_mode.collect().select(list(_PROFILE_SCHEMA.keys()))
    profiles = pl.concat([by_mode_df, all_mode_df], how="vertical_relaxed")
    if profiles.height == 0:
        return _empty_profiles()
    return profiles.select(list(_PROFILE_SCHEMA.keys())).sort(
        ["mode_scope", "students", "attempts", "classroom_id"],
        descending=[False, True, True, False],
    )


def select_default_classroom(profiles: pl.DataFrame, mode_scope: str) -> str | None:
    """Select default classroom according to locked ranking rules."""
    if mode_scope not in VALID_MODE_SCOPES:
        raise ValueError(f"Unsupported mode_scope '{mode_scope}'. Expected one of {list(VALID_MODE_SCOPES)}")
    if profiles.height == 0:
        return None

    scoped = profiles.filter(pl.col("mode_scope") == mode_scope)
    if scoped.height == 0:
        return None

    if mode_scope == "zpdes":
        eligible = scoped.filter(
            (pl.col("students") >= 15)
            & (pl.col("students") <= 20)
            & (pl.col("activities") >= 10)
            & (pl.col("activities") < 50)
        )
        if eligible.height > 0:
            scoped = eligible

    winner = scoped.sort(
        ["students", "attempts", "activities", "classroom_id"],
        descending=[True, True, True, False],
    ).head(1)
    if winner.height == 0:
        return None
    return str(winner["classroom_id"][0])


def build_replay_payload(
    fact: pl.DataFrame | pl.LazyFrame,
    classroom_id: str,
    mode_scope: str,
    start_date: date,
    end_date: date,
    max_frames: int,
    step_size: int,
) -> dict[str, Any]:
    """Build replay payload with sampled cumulative matrices."""
    if mode_scope not in VALID_MODE_SCOPES:
        raise ValueError(f"Unsupported mode_scope '{mode_scope}'. Expected one of {list(VALID_MODE_SCOPES)}")

    _assert_required_columns(fact, _REPLAY_REQUIRED_COLUMNS)
    lf = _as_lazy(fact).with_columns(
        pl.col("classroom_id").cast(pl.Utf8),
        pl.col("work_mode").cast(pl.Utf8),
        pl.col("user_id").cast(pl.Utf8),
        pl.col("activity_id").cast(pl.Utf8),
    )

    classroom_txt = str(classroom_id or "").strip()
    if not classroom_txt:
        return _empty_payload(classroom_id="", mode_scope=mode_scope, start_date=start_date, end_date=end_date)

    scoped = lf.filter(
        _valid_classroom_filter()
        & (pl.col("classroom_id") == classroom_txt)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if mode_scope in {"zpdes", "playlist"}:
        scoped = scoped.filter(pl.col("work_mode") == mode_scope)

    total_events_raw = int(scoped.select(pl.len().alias("rows")).collect().item())
    valid_events = scoped.filter(pl.col("created_at").is_not_null())
    total_events_valid = int(valid_events.select(pl.len().alias("rows")).collect().item())
    dropped_invalid_timestamps = total_events_raw - total_events_valid

    if total_events_valid <= 0:
        payload = _empty_payload(
            classroom_id=classroom_txt,
            mode_scope=mode_scope,
            start_date=start_date,
            end_date=end_date,
        )
        payload["total_events_raw"] = total_events_raw
        payload["total_events_valid_timestamp"] = total_events_valid
        payload["dropped_invalid_timestamps"] = dropped_invalid_timestamps
        payload["requested_step_size"] = max(1, int(step_size))
        payload["effective_step"] = max(1, int(step_size))
        payload["max_frames"] = max(1, int(max_frames))
        return payload

    events = (
        valid_events.select(
            [
                "created_at",
                "user_id",
                "activity_id",
                "activity_label",
                "data_correct",
                "work_mode",
                "classroom_id",
                "objective_id",
                "module_code",
                "exercise_id",
                "attempt_number",
            ]
        )
        .sort(["created_at", "user_id", "activity_id", "exercise_id", "attempt_number"])
        .collect()
    )

    student_order = (
        events.group_by("user_id")
        .agg(pl.col("created_at").min().alias("first_seen"))
        .sort(["first_seen", "user_id"])
    )
    student_ids = [str(value) for value in student_order["user_id"].to_list()]
    student_axis_labels = [f"Student {idx + 1}" for idx in range(len(student_ids))]

    activity_order = (
        events.group_by("activity_id")
        .agg(
            pl.col("created_at").min().alias("first_seen"),
            pl.col("activity_label")
            .drop_nulls()
            .first()
            .cast(pl.Utf8)
            .alias("activity_label"),
        )
        .sort(["first_seen", "activity_id"])
    )
    activity_ids = [str(value) for value in activity_order["activity_id"].to_list()]
    activity_full_labels = [
        str(label).strip() if str(label).strip() else str(activity_id)
        for label, activity_id in zip(
            activity_order["activity_label"].fill_null("").to_list(),
            activity_ids,
            strict=False,
        )
    ]
    activity_axis_labels = [label if len(label) <= 48 else f"{label[:47].rstrip()}..." for label in activity_full_labels]

    student_index = {student_id: idx for idx, student_id in enumerate(student_ids)}
    activity_index = {activity_id: idx for idx, activity_id in enumerate(activity_ids)}

    n_students = len(student_ids)
    n_activities = len(activity_ids)
    success_matrix = [[0 for _ in range(n_students)] for _ in range(n_activities)]
    attempt_matrix = [[0 for _ in range(n_students)] for _ in range(n_activities)]

    rows = events.to_dicts()
    student_sequences: list[list[dict[str, Any]]] = [[] for _ in range(n_students)]
    for row in rows:
        student_id = str(row.get("user_id") or "")
        idx = student_index.get(student_id)
        if idx is not None:
            student_sequences[idx].append(row)

    total_sync_steps = max((len(sequence) for sequence in student_sequences), default=0)
    requested_step = max(1, int(step_size))
    cap = max(1, int(max_frames))
    cap_step = max(1, int(math.ceil(total_sync_steps / cap))) if total_sync_steps > 0 else 1
    effective_step = max(requested_step, cap_step)
    frame_step_counts = _build_frame_step_counts(total_sync_steps, effective_step)
    frame_event_counts = [0]

    rate_frames: list[list[list[float | None]]] = []
    attempt_frames: list[list[list[int]]] = []
    frame_timestamps: list[str | None] = []

    # frame 0: empty matrix
    rate_frames.append([[None for _ in range(n_students)] for _ in range(n_activities)])
    attempt_frames.append([[0 for _ in range(n_students)] for _ in range(n_activities)])
    frame_timestamps.append(None)

    previous_sync_step = 0
    events_processed = 0
    for sync_step in frame_step_counts[1:]:
        frame_last_timestamp: str | None = None

        for s_idx, sequence in enumerate(student_sequences):
            lower_bound = min(previous_sync_step, len(sequence))
            upper_bound = min(sync_step, len(sequence))
            for attempt_idx in range(lower_bound, upper_bound):
                row = sequence[attempt_idx]
                events_processed += 1

                timestamp_txt = _serialize_timestamp(row.get("created_at"))
                if timestamp_txt and (frame_last_timestamp is None or timestamp_txt > frame_last_timestamp):
                    frame_last_timestamp = timestamp_txt

                activity_id = str(row.get("activity_id") or "")
                a_idx = activity_index.get(activity_id)
                if a_idx is None:
                    continue

                raw_score = row.get("data_correct")
                score = float(raw_score) if isinstance(raw_score, (int, float)) else 0.0
                attempt_matrix[a_idx][s_idx] += 1
                success_matrix[a_idx][s_idx] += int(score >= 1.0)

        rate_frames.append(_matrix_rate_snapshot(success_matrix, attempt_matrix))
        attempt_frames.append(_matrix_attempt_snapshot(attempt_matrix))
        frame_timestamps.append(frame_last_timestamp)
        frame_event_counts.append(events_processed)
        previous_sync_step = sync_step

    return {
        "classroom_id": classroom_txt,
        "mode_scope": mode_scope,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "student_ids": student_ids,
        "student_axis_labels": student_axis_labels,
        "activity_ids": activity_ids,
        "activity_axis_labels": activity_axis_labels,
        "activity_full_labels": activity_full_labels,
        "frame_step_counts": frame_step_counts,
        "frame_event_counts": frame_event_counts,
        "frame_timestamps": frame_timestamps,
        "rate_frames": rate_frames,
        "attempt_frames": attempt_frames,
        "total_events_raw": total_events_raw,
        "total_events_valid_timestamp": total_events_valid,
        "total_sync_steps": total_sync_steps,
        "dropped_invalid_timestamps": dropped_invalid_timestamps,
        "requested_step_size": requested_step,
        "effective_step": effective_step,
        "max_frames": cap,
        "events_capped": effective_step > requested_step,
    }


def build_heatmap_figure(
    payload: dict[str, Any],
    frame_idx: int,
    threshold: float,
    show_values: bool,
) -> go.Figure:
    rate_frames = payload.get("rate_frames") or []
    attempt_frames = payload.get("attempt_frames") or []
    student_ids = [str(value) for value in payload.get("student_ids") or []]
    student_axis_labels = [str(value) for value in payload.get("student_axis_labels") or []]
    activity_ids = [str(value) for value in payload.get("activity_ids") or []]
    activity_axis_labels = [str(value) for value in payload.get("activity_axis_labels") or []]
    activity_full_labels = [str(value) for value in payload.get("activity_full_labels") or []]
    timestamps = payload.get("frame_timestamps") or []

    if not rate_frames or not student_axis_labels or not activity_axis_labels:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_white",
            height=420,
            margin={"l": 16, "r": 16, "t": 40, "b": 16},
            annotations=[
                {
                    "text": "No replay data in the selected scope.",
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "showarrow": False,
                    "font": {"size": 15},
                }
            ],
        )
        return fig

    index = max(0, min(int(frame_idx), len(rate_frames) - 1))
    z = rate_frames[index]
    attempts = attempt_frames[index]
    frame_time = timestamps[index] if index < len(timestamps) else None

    threshold_clipped = _clip_threshold(threshold)
    p1 = max(0.0, threshold_clipped * 0.5)
    p2 = threshold_clipped
    p3 = min(1.0, threshold_clipped + (1.0 - threshold_clipped) * 0.35)
    colorscale = [
        [0.0, "#5a189a"],
        [p1, "#7b2cbf"],
        [p2, "#b8d8ba"],
        [p3, "#5dbb63"],
        [1.0, "#2f9e44"],
    ]

    customdata: list[list[list[Any]]] = []
    text_matrix: list[list[str]] = []
    for row_idx, activity_id in enumerate(activity_ids):
        custom_row: list[list[Any]] = []
        text_row: list[str] = []
        for col_idx, student_id in enumerate(student_ids):
            rate_value = z[row_idx][col_idx]
            attempts_value = attempts[row_idx][col_idx]
            custom_row.append(
                [
                    student_id,
                    activity_id,
                    activity_full_labels[row_idx] if row_idx < len(activity_full_labels) else activity_id,
                    int(attempts_value),
                    None if rate_value is None else float(rate_value),
                    frame_time,
                ]
            )
            if show_values and rate_value is not None:
                text_row.append(f"{float(rate_value) * 100:.0f}%")
            else:
                text_row.append("")
        customdata.append(custom_row)
        text_matrix.append(text_row)

    fig = go.Figure(
        data=[
            go.Heatmap(
                z=z,
                x=student_axis_labels,
                y=activity_axis_labels,
                zmin=0.0,
                zmax=1.0,
                colorscale=colorscale,
                colorbar={"title": "Cumulative success rate"},
                text=text_matrix if show_values else None,
                texttemplate="%{text}" if show_values else None,
                textfont={"size": 10},
                customdata=customdata,
                hoverongaps=False,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + "Student: %{x}<br>"
                    + "User ID: %{customdata[0]}<br>"
                    + "Activity ID: %{customdata[1]}<br>"
                    + "Attempts in cell: %{customdata[3]}<br>"
                    + "Cumulative success rate: %{z:.1%}<br>"
                    + "Last event timestamp: %{customdata[5]}"
                    + "<extra></extra>"
                ),
                xgap=1,
                ygap=1,
            )
        ]
    )
    fig.update_layout(
        template="plotly_white",
        margin={"l": 220, "r": 24, "t": 44, "b": 24},
        height=max(460, 22 * len(activity_axis_labels) + 220),
        xaxis_title="Students (anonymized)",
        yaxis_title="Activities",
    )
    return fig

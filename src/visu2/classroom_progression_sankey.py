"""Build stage-based Sankey payloads for classroom activity progression."""

from __future__ import annotations

import hashlib
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import polars as pl

from visu2.classroom_progression import (
    MISSING_ACTIVITY_LABEL,
    SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID,
    VALID_MODE_SCOPES,
    _make_unique_axis_labels,
    _normalize_activity_label,
    _with_effective_classroom_ids,
)
from visu2.loaders import catalog_to_summary_frames, load_learning_catalog

_REQUIRED_COLUMNS = (
    "classroom_id",
    "user_id",
    "work_mode",
    "created_at",
    "date_utc",
    "activity_id",
    "activity_label",
    "exercise_id",
    "attempt_number",
)

_EMPTY_EDGE_SCHEMA: dict[str, pl.DataType] = {
    "source_key": pl.Utf8,
    "source_label": pl.Utf8,
    "source_full_label": pl.Utf8,
    "source_stage": pl.Int64,
    "target_key": pl.Utf8,
    "target_label": pl.Utf8,
    "target_full_label": pl.Utf8,
    "target_stage": pl.Int64,
    "student_count": pl.Int64,
    "classroom_share": pl.Float64,
    "source_share": pl.Float64,
}

_TERMINAL_COLOR = "#C8CDD6"


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()


def _normalized_text_expr(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strip_chars()


def _assert_required_columns(frame: pl.DataFrame | pl.LazyFrame) -> None:
    columns = frame.collect_schema().names() if isinstance(frame, pl.LazyFrame) else frame.columns
    missing = [column for column in _REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"Classroom Sankey source is missing required columns: {missing}")


def _empty_paths_payload(
    classroom_id: str,
    mode_scope: str,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    return {
        "classroom_id": classroom_id,
        "mode_scope": mode_scope,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "student_ids": [],
        "student_count": 0,
        "activity_ids": [],
        "activity_full_labels": [],
        "activity_display_labels": [],
        "student_paths": [],
        "total_events_raw": 0,
        "total_events_valid_timestamp": 0,
        "dropped_invalid_timestamps": 0,
    }


def max_classroom_activity_path_length(payload: dict[str, Any] | None) -> int:
    """Return the maximum first-time activity path length in one classroom payload."""
    if not isinstance(payload, dict):
        return 1
    student_paths = payload.get("student_paths") or []
    lengths = [
        int(row.get("path_length") or 0)
        for row in student_paths
        if isinstance(row, dict)
    ]
    return max(1, max(lengths, default=1))


def _terminal_label(activity_count: int) -> str:
    suffix = "activity" if activity_count == 1 else "activities"
    return f"Stopped after {activity_count} {suffix}"


def _color_for_activity_id(activity_id: str) -> str:
    digest = hashlib.md5(activity_id.encode("utf-8")).hexdigest()
    hue = int(digest[:8], 16) % 360
    saturation = 38
    lightness = 68
    chroma = (1 - abs((2 * lightness / 100) - 1)) * (saturation / 100)
    hue_prime = hue / 60
    x_value = chroma * (1 - abs((hue_prime % 2) - 1))
    red = green = blue = 0.0
    if 0 <= hue_prime < 1:
        red, green = chroma, x_value
    elif 1 <= hue_prime < 2:
        red, green = x_value, chroma
    elif 2 <= hue_prime < 3:
        green, blue = chroma, x_value
    elif 3 <= hue_prime < 4:
        green, blue = x_value, chroma
    elif 4 <= hue_prime < 5:
        red, blue = x_value, chroma
    else:
        red, blue = chroma, x_value
    match = lightness / 100 - chroma / 2
    rgb = [int(round((channel + match) * 255)) for channel in (red, green, blue)]
    return "#{:02X}{:02X}{:02X}".format(*rgb)


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    normalized = hex_color.lstrip("#")
    if len(normalized) != 6:
        return (107, 114, 128)
    return tuple(int(normalized[idx : idx + 2], 16) for idx in (0, 2, 4))


def _rgba_from_hex(hex_color: str, alpha: float = 0.45) -> str:
    rgb = _hex_to_rgb(hex_color)
    return f"rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, {alpha:.2f})"


def load_activity_code_lookup(learning_catalog_path: Path) -> dict[str, str]:
    """Load canonical activity codes from the learning catalog."""
    catalog = load_learning_catalog(learning_catalog_path)
    frames = catalog_to_summary_frames(catalog)
    activity_rows = (
        frames.activity_hierarchy.select(["activity_id", "activity_code"])
        .drop_nulls(subset=["activity_id", "activity_code"])
        .unique(subset=["activity_id"], keep="first")
        .to_dicts()
    )
    return {
        str(row["activity_id"]): str(row["activity_code"]).strip()
        for row in activity_rows
        if str(row.get("activity_id") or "").strip() and str(row.get("activity_code") or "").strip()
    }


def build_classroom_activity_paths(
    fact: pl.DataFrame | pl.LazyFrame,
    *,
    classroom_id: str,
    mode_scope: str,
    start_date: date,
    end_date: date,
    activity_code_lookup: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build ordered first-time activity sequences for one classroom."""
    if mode_scope not in VALID_MODE_SCOPES:
        raise ValueError(f"Unsupported mode scope: {mode_scope}")

    _assert_required_columns(fact)
    classroom_key = str(classroom_id).strip()
    if not classroom_key:
        raise ValueError("classroom_id must be a non-empty string.")

    lf = _with_effective_classroom_ids(fact).with_columns(
        _normalized_text_expr("user_id").alias("user_id_normalized"),
        _normalized_text_expr("activity_id").alias("activity_id_normalized"),
        pl.col("exercise_id").cast(pl.Utf8).alias("exercise_id"),
        pl.col("attempt_number").cast(pl.Int64).fill_null(0).alias("attempt_number"),
        pl.col("activity_label")
        .map_elements(_normalize_activity_label, return_dtype=pl.Utf8)
        .alias("activity_label_normalized"),
    )
    scoped = lf.filter(pl.col("date_utc").is_between(start_date, end_date))
    if classroom_key != SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID:
        scoped = scoped.filter(pl.col("classroom_id") == classroom_key)
    if mode_scope != "all":
        scoped = scoped.filter(pl.col("work_mode") == mode_scope)

    raw_rows = scoped.collect()
    total_events_raw = raw_rows.height
    valid_rows = (
        raw_rows.filter(pl.col("created_at").is_not_null())
        .filter(pl.col("user_id_normalized").is_not_null() & (pl.col("user_id_normalized") != ""))
        .filter(pl.col("activity_id_normalized").is_not_null() & (pl.col("activity_id_normalized") != ""))
        .sort(
            [
                "user_id_normalized",
                "created_at",
                "activity_id_normalized",
                "exercise_id",
                "attempt_number",
            ]
        )
    )
    total_events_valid = valid_rows.height
    dropped_invalid = max(0, total_events_raw - total_events_valid)
    if total_events_valid == 0:
        return _empty_paths_payload(classroom_key, mode_scope, start_date, end_date)

    ordered_activities = (
        valid_rows.group_by("activity_id_normalized")
        .agg(
            pl.col("created_at").min().alias("first_seen_at"),
            pl.col("activity_label_normalized")
            .drop_nulls()
            .first()
            .fill_null(MISSING_ACTIVITY_LABEL)
            .alias("activity_full_label"),
        )
        .sort(["first_seen_at", "activity_id_normalized"])
    )
    activity_ids = [str(value) for value in ordered_activities["activity_id_normalized"].to_list()]
    activity_full_labels = [str(value) for value in ordered_activities["activity_full_label"].to_list()]
    code_lookup = {
        str(key): str(value).strip()
        for key, value in (activity_code_lookup or {}).items()
        if str(key).strip() and str(value).strip()
    }
    display_candidates = []
    for activity_id, label in zip(activity_ids, activity_full_labels, strict=False):
        code = code_lookup.get(activity_id)
        if code:
            display_candidates.append(code)
        elif label == MISSING_ACTIVITY_LABEL:
            display_candidates.append(MISSING_ACTIVITY_LABEL)
        else:
            display_candidates.append(label if len(label) <= 48 else f"{label[:47].rstrip()}...")
    activity_display_labels = _make_unique_axis_labels(display_candidates)
    display_by_activity = dict(zip(activity_ids, activity_display_labels, strict=False))
    full_by_activity = dict(zip(activity_ids, activity_full_labels, strict=False))

    student_paths: list[dict[str, Any]] = []
    student_ids: list[str] = []
    grouped = (
        valid_rows.group_by("user_id_normalized")
        .agg(pl.col("activity_id_normalized").alias("activity_ids_ordered"))
        .sort("user_id_normalized")
        .to_dicts()
    )
    for row in grouped:
        user_id = str(row["user_id_normalized"])
        seen: set[str] = set()
        path_ids: list[str] = []
        for activity_id in row.get("activity_ids_ordered") or []:
            activity_key = str(activity_id)
            if activity_key in seen:
                continue
            seen.add(activity_key)
            path_ids.append(activity_key)
        if not path_ids:
            continue
        student_ids.append(user_id)
        student_paths.append(
            {
                "user_id": user_id,
                "activity_ids": path_ids,
                "activity_full_labels": [full_by_activity[activity_id] for activity_id in path_ids],
                "activity_display_labels": [display_by_activity[activity_id] for activity_id in path_ids],
                "path_length": len(path_ids),
            }
        )

    return {
        "classroom_id": classroom_key,
        "classroom_label": (
            "All students"
            if classroom_key == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
            else classroom_key
        ),
        "mode_scope": mode_scope,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "student_ids": student_ids,
        "student_count": len(student_ids),
        "activity_ids": activity_ids,
        "activity_full_labels": activity_full_labels,
        "activity_display_labels": activity_display_labels,
        "student_paths": student_paths,
        "total_events_raw": total_events_raw,
        "total_events_valid_timestamp": total_events_valid,
        "dropped_invalid_timestamps": dropped_invalid,
    }


def build_classroom_activity_sankey_edges(
    payload: dict[str, Any],
    *,
    visible_steps: int,
    start_step: int = 1,
) -> pl.DataFrame:
    """Aggregate classroom activity paths into stage-based Sankey edges."""
    student_paths = payload.get("student_paths") or []
    student_count = int(payload.get("student_count") or len(student_paths) or 0)
    if student_count <= 0 or not student_paths:
        return pl.DataFrame(schema=_EMPTY_EDGE_SCHEMA)

    depth = max(1, int(visible_steps))
    start_index = max(0, int(start_step) - 1)
    absolute_window_end = start_index + depth
    counter: Counter[tuple[str, str, str, int, str, str, str, int]] = Counter()
    source_totals: Counter[str] = Counter()

    def add_edge(
        source_key: str,
        source_label: str,
        source_full_label: str,
        source_stage: int,
        target_key: str,
        target_label: str,
        target_full_label: str,
        target_stage: int,
    ) -> None:
        key = (
            source_key,
            source_label,
            source_full_label,
            source_stage,
            target_key,
            target_label,
            target_full_label,
            target_stage,
        )
        counter[key] += 1
        source_totals[source_key] += 1

    for row in student_paths:
        activity_ids = [str(value) for value in row.get("activity_ids") or []]
        display_labels = [str(value) for value in row.get("activity_display_labels") or []]
        full_labels = [str(value) for value in row.get("activity_full_labels") or []]
        if not activity_ids or not display_labels or not full_labels:
            continue
        if len(activity_ids) <= start_index:
            continue
        activity_ids = activity_ids[start_index:absolute_window_end]
        display_labels = display_labels[start_index:absolute_window_end]
        full_labels = full_labels[start_index:absolute_window_end]
        visible_count = min(len(activity_ids), depth)
        for stage in range(max(0, visible_count - 1)):
            add_edge(
                f"stage{stage}::activity::{activity_ids[stage]}",
                display_labels[stage],
                full_labels[stage],
                stage,
                f"stage{stage + 1}::activity::{activity_ids[stage + 1]}",
                display_labels[stage + 1],
                full_labels[stage + 1],
                stage + 1,
            )
        last_visible_stage = visible_count - 1
        last_visible_id = activity_ids[last_visible_stage]
        last_visible_label = display_labels[last_visible_stage]
        last_visible_full = full_labels[last_visible_stage]
        terminal_stage = last_visible_stage + 1
        original_length = len(row.get("activity_ids") or [])
        if original_length > absolute_window_end:
            terminal_label = f"More than {absolute_window_end} activities"
            terminal_key = f"stage{terminal_stage}::terminal::more_than_{absolute_window_end}"
        else:
            terminal_label = _terminal_label(original_length)
            terminal_key = f"stage{terminal_stage}::terminal::stopped_after_{original_length}"
        add_edge(
            f"stage{last_visible_stage}::activity::{last_visible_id}",
            last_visible_label,
            last_visible_full,
            last_visible_stage,
            terminal_key,
            terminal_label,
            terminal_label,
            terminal_stage,
        )

    rows = []
    for (
        source_key,
        source_label,
        source_full_label,
        source_stage,
        target_key,
        target_label,
        target_full_label,
        target_stage,
    ), count in counter.items():
        source_total = source_totals[source_key]
        rows.append(
            {
                "source_key": source_key,
                "source_label": source_label,
                "source_full_label": source_full_label,
                "source_stage": source_stage,
                "target_key": target_key,
                "target_label": target_label,
                "target_full_label": target_full_label,
                "target_stage": target_stage,
                "student_count": count,
                "classroom_share": count / student_count,
                "source_share": count / source_total if source_total > 0 else 0.0,
            }
        )
    return pl.DataFrame(rows, schema=_EMPTY_EDGE_SCHEMA).sort(
        ["source_stage", "target_stage", "student_count", "source_label", "target_label"],
        descending=[False, False, True, False, False],
    )


def build_classroom_activity_sankey_figure(
    payload: dict[str, Any],
    *,
    visible_steps: int,
    start_step: int = 1,
) -> go.Figure:
    """Build a static classroom activity Sankey figure."""
    edges = build_classroom_activity_sankey_edges(
        payload,
        visible_steps=visible_steps,
        start_step=start_step,
    )
    if edges.height == 0:
        return go.Figure()

    activity_order = {
        str(activity_id): idx
        for idx, activity_id in enumerate(payload.get("activity_ids") or [])
    }
    node_meta: dict[str, dict[str, Any]] = {}
    node_weights: Counter[str] = Counter()
    for row in edges.to_dicts():
        source_key = str(row["source_key"])
        target_key = str(row["target_key"])
        node_meta[source_key] = {
            "label": str(row["source_label"]),
            "full_label": str(row["source_full_label"]),
            "stage": int(row["source_stage"]),
        }
        node_meta[target_key] = {
            "label": str(row["target_label"]),
            "full_label": str(row["target_full_label"]),
            "stage": int(row["target_stage"]),
        }
        node_weights[source_key] += int(row["student_count"])
        node_weights[target_key] += int(row["student_count"])

    def node_sort_key(key: str) -> tuple[int, int, int, str]:
        meta = node_meta[key]
        stage = int(meta["stage"])
        if "::terminal::" in key:
            text = str(meta["label"])
            if text.startswith("Stopped after "):
                count_text = text.replace("Stopped after ", "").split(" ", maxsplit=1)[0]
                return (stage, 1, int(count_text), text)
            if text.startswith("More than "):
                count_text = text.replace("More than ", "").split(" ", maxsplit=1)[0]
                return (stage, 1, int(count_text) + 1000, text)
            return (stage, 1, 9999, text)
        activity_id = key.split("::activity::", maxsplit=1)[-1]
        return (stage, 0, activity_order.get(activity_id, 9999), str(meta["label"]))

    ordered_nodes = sorted(
        node_meta,
        key=lambda key: (node_sort_key(key), -node_weights[key]),
    )
    index_by_key = {key: idx for idx, key in enumerate(ordered_nodes)}
    max_stage = max(int(node_meta[key]["stage"]) for key in ordered_nodes)
    stage_denominator = max(1, max_stage)
    node_labels = [str(node_meta[key]["label"]) for key in ordered_nodes]
    node_hover_labels = [str(node_meta[key]["full_label"]) for key in ordered_nodes]
    node_x = [0.02 + (0.94 * int(node_meta[key]["stage"]) / stage_denominator) for key in ordered_nodes]
    node_colors = []
    for key in ordered_nodes:
        if "::terminal::" in key:
            node_colors.append(_TERMINAL_COLOR)
            continue
        activity_id = key.split("::activity::", maxsplit=1)[-1]
        node_colors.append(_color_for_activity_id(activity_id))

    link_source: list[int] = []
    link_target: list[int] = []
    link_value: list[int] = []
    link_color: list[str] = []
    link_customdata: list[list[object]] = []
    for row in edges.to_dicts():
        source_key = str(row["source_key"])
        target_key = str(row["target_key"])
        target_color = _TERMINAL_COLOR
        if "::activity::" in target_key:
            target_color = _color_for_activity_id(target_key.split("::activity::", maxsplit=1)[-1])
        link_source.append(index_by_key[source_key])
        link_target.append(index_by_key[target_key])
        link_value.append(int(row["student_count"]))
        link_color.append(_rgba_from_hex(target_color))
        link_customdata.append(
            [
                str(row["source_full_label"]),
                str(row["target_full_label"]),
                int(row["student_count"]),
                float(row["classroom_share"]),
                float(row["source_share"]),
            ]
        )

    figure = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                textfont=dict(
                    family="IBM Plex Sans, Arial, sans-serif",
                    size=16,
                    color="#000000",
                ),
                node=dict(
                    pad=20,
                    thickness=18,
                    line=dict(color="rgba(23, 34, 27, 0.18)", width=0.6),
                    label=node_labels,
                    customdata=node_hover_labels,
                    color=node_colors,
                    x=node_x,
                    hovertemplate="%{customdata}<extra></extra>",
                ),
                link=dict(
                    source=link_source,
                    target=link_target,
                    value=link_value,
                    color=link_color,
                    customdata=link_customdata,
                    hovertemplate=(
                        "%{customdata[0]} -> %{customdata[1]}<br>"
                        "Students: %{customdata[2]:,}<br>"
                        "Share of classroom: %{customdata[3]:.1%}<br>"
                        "Share of source node: %{customdata[4]:.1%}<extra></extra>"
                    ),
                ),
            )
        ]
    )
    figure.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        height=max(560, min(1200, 28 * len(ordered_nodes) + 260)),
        font=dict(
            family="IBM Plex Sans, Arial, sans-serif",
            size=16,
            color="#000000",
        ),
    )
    return figure

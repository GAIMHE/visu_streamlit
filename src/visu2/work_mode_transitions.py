"""Build work-mode transition paths and Sankey figures for the overview page."""

from __future__ import annotations

from collections import Counter

import plotly.graph_objects as go
import polars as pl

WORK_MODE_LABELS: dict[str, str] = {
    "adaptive-test": "Adaptive-test",
    "initial-test": "Initial-test",
    "playlist": "Playlist",
    "zpdes": "ZPDES",
}

WORK_MODE_ORDER: dict[str, int] = {
    "initial-test": 0,
    "adaptive-test": 1,
    "playlist": 2,
    "zpdes": 3,
}

TERMINAL_LABELS: dict[str, str] = {
    "no_transition": "No transition",
    "stopped_after_1": "Stopped after 1",
    "stopped_after_2": "Stopped after 2",
    "stopped_after_3": "Stopped after 3",
    "more_than_3": "More than 3 transitions",
}

NODE_X_POSITIONS: dict[int, float] = {
    0: 0.02,
    1: 0.24,
    2: 0.48,
    3: 0.72,
    4: 0.96,
}

NODE_COLORS: dict[str, str] = {
    "adaptive-test": "#DAB89A",
    "initial-test": "#90A8D8",
    "playlist": "#98C7B7",
    "zpdes": "#8BB9A0",
    "terminal": "#C8CDD6",
}

MIN_VISIBLE_TRANSITION_STUDENTS = 1


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def _normalized_text_expr(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8).str.strip_chars()


def _mode_label(mode: str | None) -> str:
    text = str(mode or "").strip()
    return WORK_MODE_LABELS.get(text, text or "Unknown")


def _node_color_for_label(label: str, *, terminal: bool) -> str:
    if terminal:
        return NODE_COLORS["terminal"]
    reverse_map = {display: code for code, display in WORK_MODE_LABELS.items()}
    return NODE_COLORS.get(reverse_map.get(label, ""), "#6B7280")


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    normalized = hex_color.lstrip("#")
    if len(normalized) != 6:
        return (107, 114, 128)
    return tuple(int(normalized[idx : idx + 2], 16) for idx in (0, 2, 4))


def _rgba_from_hex(hex_color: str, alpha: float = 0.45) -> str:
    rgb = _hex_to_rgb(hex_color)
    return f"rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, {alpha:.2f})"


def _mode_sort_value(mode_label: str) -> tuple[int, str]:
    reverse_map = {label: code for code, label in WORK_MODE_LABELS.items()}
    code = reverse_map.get(mode_label)
    if code is None:
        return (len(WORK_MODE_ORDER), mode_label)
    return (WORK_MODE_ORDER.get(code, len(WORK_MODE_ORDER)), mode_label)


def build_work_mode_transition_paths(raw_attempts: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Build one transition-path row per student from the raw attempt history."""
    lf = (
        _as_lazy(raw_attempts)
        .select(
            [
                "user_id",
                "student_attempt_index",
                "created_at",
                "work_mode",
            ]
        )
        .filter(
            pl.col("user_id").is_not_null()
            & (_normalized_text_expr("user_id") != "")
            & pl.col("work_mode").is_not_null()
            & (_normalized_text_expr("work_mode") != "")
        )
        .with_columns(
            _normalized_text_expr("user_id").alias("user_id"),
            _normalized_text_expr("work_mode").alias("work_mode"),
        )
        .sort(["user_id", "student_attempt_index", "created_at", "work_mode"])
        .with_columns(pl.col("work_mode").shift(1).over("user_id").alias("prev_work_mode"))
        .with_columns(
            pl.when(pl.col("prev_work_mode").is_null())
            .then(pl.lit(0, dtype=pl.Int64))
            .when(pl.col("work_mode") != pl.col("prev_work_mode"))
            .then(pl.lit(1, dtype=pl.Int64))
            .otherwise(pl.lit(0, dtype=pl.Int64))
            .alias("transition_flag")
        )
        .with_columns(pl.col("transition_flag").cum_sum().over("user_id").alias("transition_index"))
        .filter(pl.col("prev_work_mode").is_null() | (pl.col("work_mode") != pl.col("prev_work_mode")))
    )

    return (
        lf.group_by("user_id")
        .agg(
            pl.col("work_mode").filter(pl.col("transition_index") == 0).first().alias("first_work_mode"),
            pl.col("work_mode").filter(pl.col("transition_index") == 1).first().alias("transition_1_mode"),
            pl.col("work_mode").filter(pl.col("transition_index") == 2).first().alias("transition_2_mode"),
            pl.col("work_mode").filter(pl.col("transition_index") == 3).first().alias("transition_3_mode"),
            pl.col("transition_index").max().cast(pl.Int64).alias("transition_count_total"),
        )
        .with_columns((pl.col("transition_count_total") > 3).alias("continues_after_transition_3"))
        .sort("user_id")
        .collect()
    )


def build_work_mode_transition_edge_frame(
    paths: pl.DataFrame,
    *,
    min_student_count: int | None = None,
) -> pl.DataFrame:
    """Build aggregated Sankey edges from student transition paths."""
    if paths.height == 0:
        return pl.DataFrame(
            {
                "source_key": [],
                "source_label": [],
                "source_stage": [],
                "target_key": [],
                "target_label": [],
                "target_stage": [],
                "student_count": [],
                "student_share": [],
            },
            schema={
                "source_key": pl.Utf8,
                "source_label": pl.Utf8,
                "source_stage": pl.Int64,
                "target_key": pl.Utf8,
                "target_label": pl.Utf8,
                "target_stage": pl.Int64,
                "student_count": pl.Int64,
                "student_share": pl.Float64,
            },
        )

    counter: Counter[tuple[str, str, int, str, str, int]] = Counter()
    total_students = paths.height

    def add_edge(
        source_key: str,
        source_label: str,
        source_stage: int,
        target_key: str,
        target_label: str,
        target_stage: int,
    ) -> None:
        counter[(source_key, source_label, source_stage, target_key, target_label, target_stage)] += 1

    for row in paths.to_dicts():
        first_mode = str(row.get("first_work_mode") or "").strip()
        transition_count = int(row.get("transition_count_total") or 0)
        transition_1 = str(row.get("transition_1_mode") or "").strip()
        transition_2 = str(row.get("transition_2_mode") or "").strip()
        transition_3 = str(row.get("transition_3_mode") or "").strip()

        if not first_mode:
            continue

        first_label = _mode_label(first_mode)
        first_key = f"stage0::{first_mode}"

        if transition_count <= 0 or not transition_1:
            add_edge(
                first_key,
                first_label,
                0,
                "stage1::terminal::no_transition",
                TERMINAL_LABELS["no_transition"],
                1,
            )
            continue

        first_transition_key = f"stage1::{transition_1}"
        first_transition_label = _mode_label(transition_1)
        add_edge(first_key, first_label, 0, first_transition_key, first_transition_label, 1)

        if transition_count == 1 or not transition_2:
            add_edge(
                first_transition_key,
                first_transition_label,
                1,
                "stage2::terminal::stopped_after_1",
                TERMINAL_LABELS["stopped_after_1"],
                2,
            )
            continue

        second_transition_key = f"stage2::{transition_2}"
        second_transition_label = _mode_label(transition_2)
        add_edge(
            first_transition_key,
            first_transition_label,
            1,
            second_transition_key,
            second_transition_label,
            2,
        )

        if transition_count == 2 or not transition_3:
            add_edge(
                second_transition_key,
                second_transition_label,
                2,
                "stage3::terminal::stopped_after_2",
                TERMINAL_LABELS["stopped_after_2"],
                3,
            )
            continue

        third_transition_key = f"stage3::{transition_3}"
        third_transition_label = _mode_label(transition_3)
        add_edge(
            second_transition_key,
            second_transition_label,
            2,
            third_transition_key,
            third_transition_label,
            3,
        )

        final_target = (
            ("stage4::terminal::more_than_3", TERMINAL_LABELS["more_than_3"])
            if bool(row.get("continues_after_transition_3"))
            else ("stage4::terminal::stopped_after_3", TERMINAL_LABELS["stopped_after_3"])
        )
        add_edge(
            third_transition_key,
            third_transition_label,
            3,
            final_target[0],
            final_target[1],
            4,
        )

    rows = [
        {
            "source_key": source_key,
            "source_label": source_label,
            "source_stage": source_stage,
            "target_key": target_key,
            "target_label": target_label,
            "target_stage": target_stage,
            "student_count": student_count,
            "student_share": student_count / total_students,
        }
        for (
            source_key,
            source_label,
            source_stage,
            target_key,
            target_label,
            target_stage,
        ), student_count in counter.items()
    ]
    edge_frame = pl.DataFrame(rows).sort(
        [
            "source_stage",
            "target_stage",
            "student_count",
            "source_label",
            "target_label",
        ],
        descending=[False, False, True, False, False],
    )
    if min_student_count is not None:
        edge_frame = edge_frame.filter(pl.col("student_count") >= int(min_student_count))
    return edge_frame


def build_work_mode_transition_sankey(
    paths: pl.DataFrame,
    *,
    min_student_count: int = MIN_VISIBLE_TRANSITION_STUDENTS,
) -> go.Figure:
    """Build the global work-mode transition Sankey figure."""
    edges = build_work_mode_transition_edge_frame(paths, min_student_count=min_student_count)
    if edges.height == 0:
        return go.Figure()

    node_meta: dict[str, dict[str, object]] = {}
    node_weights: Counter[str] = Counter()

    for row in edges.to_dicts():
        source_key = str(row["source_key"])
        target_key = str(row["target_key"])
        source_label = str(row["source_label"])
        target_label = str(row["target_label"])
        source_stage = int(row["source_stage"])
        target_stage = int(row["target_stage"])
        student_count = int(row["student_count"])

        node_meta[source_key] = {"label": source_label, "stage": source_stage}
        node_meta[target_key] = {"label": target_label, "stage": target_stage}
        node_weights[source_key] += student_count
        node_weights[target_key] += student_count

    def node_sort_key(key: str) -> tuple[int, int, str, str]:
        meta = node_meta[key]
        stage = int(meta["stage"])
        label = str(meta["label"])
        is_terminal = 1 if "::terminal::" in key else 0
        if is_terminal:
            terminal_order = {
                TERMINAL_LABELS["no_transition"]: 0,
                TERMINAL_LABELS["stopped_after_1"]: 1,
                TERMINAL_LABELS["stopped_after_2"]: 2,
                TERMINAL_LABELS["stopped_after_3"]: 3,
                TERMINAL_LABELS["more_than_3"]: 4,
            }.get(label, 99)
            return (stage, is_terminal, terminal_order, label)
        mode_order, label_text = _mode_sort_value(label)
        return (stage, is_terminal, mode_order, label_text)

    ordered_nodes = sorted(node_meta, key=lambda key: (node_sort_key(key), -node_weights[key]))
    index_by_key = {key: idx for idx, key in enumerate(ordered_nodes)}

    node_labels = [str(node_meta[key]["label"]) for key in ordered_nodes]
    node_x = [NODE_X_POSITIONS[int(node_meta[key]["stage"])] for key in ordered_nodes]
    node_colors = [
        _node_color_for_label(str(node_meta[key]["label"]), terminal="::terminal::" in key)
        for key in ordered_nodes
    ]

    link_source: list[int] = []
    link_target: list[int] = []
    link_value: list[int] = []
    link_color: list[str] = []
    customdata: list[list[object]] = []

    for row in edges.to_dicts():
        source_key = str(row["source_key"])
        target_key = str(row["target_key"])
        source_label = str(row["source_label"])
        target_label = str(row["target_label"])
        target_terminal = "::terminal::" in target_key
        link_source.append(index_by_key[source_key])
        link_target.append(index_by_key[target_key])
        link_value.append(int(row["student_count"]))
        link_color.append(
            _rgba_from_hex(
                _node_color_for_label(target_label, terminal=target_terminal),
            )
        )
        customdata.append(
            [
                source_label,
                target_label,
                int(row["student_count"]),
                float(row["student_share"]),
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
                    pad=18,
                    thickness=18,
                    line=dict(color="rgba(23, 34, 27, 0.20)", width=0.6),
                    label=node_labels,
                    color=node_colors,
                    x=node_x,
                    hovertemplate="%{label}<extra></extra>",
                ),
                link=dict(
                    source=link_source,
                    target=link_target,
                    value=link_value,
                    color=link_color,
                    customdata=customdata,
                    hovertemplate=(
                        "%{customdata[0]} -> %{customdata[1]}<br>"
                        "Students: %{customdata[2]:,}<br>"
                        "Share of all students: %{customdata[3]:.1%}<extra></extra>"
                    ),
                ),
            )
        ]
    )
    figure.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        height=640,
        font=dict(
            family="IBM Plex Sans, Arial, sans-serif",
            size=16,
            color="#000000",
        ),
    )
    return figure

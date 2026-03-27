"""Streamlit page for bottleneck ranking and path-transition analysis."""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from figure_analysis import render_figure_analysis
from figure_info import render_figure_info
from overview_shared import (
    collect_core_compatibility,
    compose_hover_label,
    ensure_label_columns,
    format_axis_label,
    format_missing_table_columns,
    label_or_id,
    load_top_transition_edges,
    parquet_columns,
    render_curriculum_filters,
    render_dashboard_style,
)
from plotly_config import build_plotly_chart_config
from source_state import get_active_source_id

from visu2.bottleneck import apply_bottleneck_filters, build_bottleneck_frame
from visu2.config import get_settings
from visu2.figure_analysis import analyze_bottleneck_chart, analyze_transition_chart

BOTTLENECKS_RUNTIME_TABLES: tuple[str, ...] = ("agg_activity_daily", "agg_transition_edges")


@st.cache_data(show_spinner=False)
def load_activity_aggregate(path: Path) -> pl.DataFrame:
    """Load the activity-daily aggregate table used by the page."""
    return pl.read_parquet(path)


def _source_module_scope(activity: pl.DataFrame) -> tuple[str, ...]:
    """Derive the active source's module scope from the loaded activity aggregate."""
    module_series = (
        activity.select(pl.col("module_code").drop_nulls().unique().sort())
        .to_series()
        .cast(pl.Utf8)
    )
    return tuple(code for code in module_series.to_list() if str(code).strip())


def main() -> None:
    """Render the bottleneck and transition analysis page."""
    render_dashboard_style()
    settings = get_settings(get_active_source_id())
    derived_dir = settings.artifacts_derived_dir
    activity_path = derived_dir / "agg_activity_daily.parquet"
    transition_path = derived_dir / "agg_transition_edges.parquet"

    required = [activity_path, transition_path]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing derived artifacts. Run `python scripts/build_derived.py` first.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    compatibility = collect_core_compatibility(
        table_columns={
            "agg_activity_daily": parquet_columns(activity_path),
            "agg_transition_edges": parquet_columns(transition_path),
        },
        required_tables=BOTTLENECKS_RUNTIME_TABLES,
    )
    if compatibility["status"] == "incompatible":
        st.error(
            "Artifact status: INCOMPATIBLE. One or more core columns are missing. "
            "Rebuild artifacts with `uv run python scripts/build_derived.py --strict-checks`."
        )
        st.markdown("**Missing core columns:**")
        st.markdown(format_missing_table_columns(compatibility["missing_core_by_table"]))
        st.stop()

    activity_raw = load_activity_aggregate(activity_path)
    activity, _ = ensure_label_columns(
        activity_raw,
        {
            "module_label": "module_code",
            "objective_label": "objective_id",
            "activity_label": "activity_id",
        },
    )
    source_module_scope = _source_module_scope(activity)
    filters = render_curriculum_filters(activity)

    st.sidebar.subheader("Chart Controls")
    top_n_bottlenecks = int(
        st.sidebar.slider("Top bottleneck entities", min_value=5, max_value=50, value=15, step=1)
    )
    top_n_transitions = int(
        st.sidebar.slider("Top transitions", min_value=5, max_value=50, value=15, step=1)
    )
    min_attempts_for_bottleneck = int(
        st.sidebar.number_input(
            "Min attempts for bottleneck",
            min_value=1,
            max_value=10_000,
            value=30,
            step=1,
        )
    )
    show_ids = bool(st.sidebar.checkbox("Show IDs in hover", value=False))

    transition_has_same_objective_rate = "same_objective_rate" in parquet_columns(transition_path)

    st.title("Bottlenecks and Transitions")

    st.subheader("Bottleneck Candidates")
    render_figure_info("bottlenecks_transitions_bottleneck_chart")
    bottleneck_level = st.radio(
        "Bottleneck level",
        options=["Module", "Objective", "Activity"],
        horizontal=True,
        index=2,
    )
    bottleneck_source = apply_bottleneck_filters(
        frame=activity,
        start_date=filters.start_date,
        end_date=filters.end_date,
        module_code=filters.module_code,
        objective_id=filters.objective_id,
        activity_id=filters.activity_id,
        level=bottleneck_level,
        canonical_modules=source_module_scope,
    )
    bottleneck_df = build_bottleneck_frame(
        filtered_activity=bottleneck_source,
        level=bottleneck_level,
        min_attempts=min_attempts_for_bottleneck,
        top_n=top_n_bottlenecks,
    )
    if bottleneck_df.empty:
        canonical_scope = ", ".join(source_module_scope) or "current source modules"
        st.info(
            f"No bottleneck rows after filters in source module scope ({canonical_scope})."
        )
    else:
        bottleneck_df["entity_axis_label"] = bottleneck_df["entity_plot_label"].map(
            lambda value: format_axis_label(str(value), max_chars=72)
        )
        axis_collision_count = (
            bottleneck_df.groupby("entity_axis_label")["entity_id"].transform("size").astype(int)
        )
        bottleneck_df["entity_axis_label"] = [
            label if int(collision_count) <= 1 else f"{label} #{str(entity_id)[:8]}"
            for label, collision_count, entity_id in zip(
                bottleneck_df["entity_axis_label"],
                axis_collision_count,
                bottleneck_df["entity_id"],
                strict=False,
            )
        ]
        bottleneck_df["entity_hover"] = [
            compose_hover_label(label, entity_id, show_ids)
            for label, entity_id in zip(
                bottleneck_df["entity_label_raw"],
                bottleneck_df["entity_id"],
                strict=False,
            )
        ]
        bottleneck_df["failure_text"] = bottleneck_df["failure_rate"].map(lambda value: f"{value:.0%}")
        chart_rows = len(bottleneck_df.index)
        chart_height = max(420, 30 * chart_rows)
        fig_bottleneck = px.bar(
            bottleneck_df.sort_values("failure_rate", ascending=True),
            x="failure_rate",
            y="entity_axis_label",
            orientation="h",
            color="repeat_attempt_rate",
            color_continuous_scale="YlOrRd",
            text="failure_text",
            custom_data=[
                "entity_hover",
                "level",
                "attempts",
                "failure_rate",
                "repeat_attempt_rate",
                "bottleneck_score",
            ],
            title=f"Top {bottleneck_level.lower()} bottleneck candidates: failure rate with repeat-attempt intensity",
            labels={
                "failure_rate": "Failure rate",
                "entity_axis_label": f"{bottleneck_level} entity",
                "repeat_attempt_rate": "Repeat attempt rate",
            },
        )
        fig_bottleneck.update_traces(
            textposition="outside",
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Level: %{customdata[1]}<br>"
                "Failure rate: %{x:.2%}<br>"
                "Attempts: %{customdata[2]:,}<br>"
                "Repeat attempt rate: %{customdata[4]:.2%}<br>"
                "Combined bottleneck score: %{customdata[5]:.3f}<extra></extra>"
            ),
        )
        fig_bottleneck.update_layout(
            height=chart_height,
            margin={"l": 340, "r": 20, "t": 56, "b": 36},
            font={"size": 13},
            coloraxis_colorbar={"title": "Repeat attempt rate"},
        )
        fig_bottleneck.update_xaxes(
            showgrid=True,
            gridcolor="rgba(23,34,27,0.14)",
            tickformat=".0%",
        )
        fig_bottleneck.update_yaxes(showgrid=False)
        st.plotly_chart(
            fig_bottleneck,
            width="stretch",
            config=build_plotly_chart_config(),
        )
    render_figure_analysis(analyze_bottleneck_chart(bottleneck_df))

    st.subheader("Path Transitions")
    render_figure_info("bottlenecks_transitions_path_chart")
    transition_edges = load_top_transition_edges(
        transition_path=transition_path,
        start_date=filters.start_date,
        end_date=filters.end_date,
        module_code=filters.module_code,
        activity_id=filters.activity_id,
        top_n=top_n_transitions,
        has_same_objective_rate=transition_has_same_objective_rate,
    ).to_pandas()
    if transition_edges.empty:
        st.info("No cross-objective transition rows after filters.")
        render_figure_analysis(analyze_transition_chart(None))
        return

    transition_edges["from_display_raw"] = [
        label_or_id(src_label, src_id)
        for src_label, src_id in zip(
            transition_edges.get("from_activity_label", transition_edges["from_activity_id"]),
            transition_edges["from_activity_id"],
            strict=False,
        )
    ]
    transition_edges["to_display_raw"] = [
        label_or_id(dst_label, dst_id)
        for dst_label, dst_id in zip(
            transition_edges.get("to_activity_label", transition_edges["to_activity_id"]),
            transition_edges["to_activity_id"],
            strict=False,
        )
    ]
    transition_edges["edge_base"] = [
        f"{format_axis_label(label_or_id(src_label, src_id), max_chars=36)} -> "
        f"{format_axis_label(label_or_id(dst_label, dst_id), max_chars=36)}"
        for src_label, src_id, dst_label, dst_id in zip(
            transition_edges.get("from_activity_label", transition_edges["from_activity_id"]),
            transition_edges["from_activity_id"],
            transition_edges.get("to_activity_label", transition_edges["to_activity_id"]),
            transition_edges["to_activity_id"],
            strict=False,
        )
    ]
    edge_collision_count = (
        transition_edges.groupby("edge_base")["from_activity_id"].transform("size").astype(int)
    )
    transition_edges["edge"] = [
        edge_base
        if int(collision_count) <= 1
        else f"{edge_base} #{str(from_id)[:8]}->{str(to_id)[:8]}"
        for edge_base, collision_count, from_id, to_id in zip(
            transition_edges["edge_base"],
            edge_collision_count,
            transition_edges["from_activity_id"],
            transition_edges["to_activity_id"],
            strict=False,
        )
    ]
    transition_edges["from_hover"] = [
        compose_hover_label(label_or_id(label, edge_id), edge_id, show_ids)
        for label, edge_id in zip(
            transition_edges["from_activity_label"],
            transition_edges["from_activity_id"],
            strict=False,
        )
    ]
    transition_edges["to_hover"] = [
        compose_hover_label(label_or_id(label, edge_id), edge_id, show_ids)
        for label, edge_id in zip(
            transition_edges["to_activity_label"],
            transition_edges["to_activity_id"],
            strict=False,
        )
    ]
    transition_edges["count_text"] = transition_edges["transition_count"].map(
        lambda value: f"{int(value):,}"
    )
    edge_rows = len(transition_edges.index)
    edge_height = max(420, 30 * edge_rows)
    fig_edges = px.bar(
        transition_edges.sort_values("transition_count", ascending=True),
        x="transition_count",
        y="edge",
        orientation="h",
        color="success_conditioned_count",
        color_continuous_scale="Viridis",
        text="count_text",
        custom_data=[
            "from_hover",
            "to_hover",
            "transition_count",
            "success_conditioned_count",
        ],
        title="Top cross-objective activity transitions by count",
        labels={
            "transition_count": "Transition count",
            "edge": "Activity path",
            "success_conditioned_count": "Successful destination attempts (count)",
        },
    )
    fig_edges.update_traces(
        textposition="outside",
        hovertemplate=(
            "<b>From</b>: %{customdata[0]}<br>"
            "<b>To</b>: %{customdata[1]}<br>"
            "Transitions: %{customdata[2]:,}<br>"
            "Successful destination attempts: %{customdata[3]:,}<extra></extra>"
        ),
    )
    fig_edges.update_layout(
        height=edge_height,
        margin={"l": 340, "r": 20, "t": 56, "b": 36},
        font={"size": 13},
        coloraxis_colorbar={"title": "Successful destination attempts (count)"},
    )
    fig_edges.update_xaxes(showgrid=True, gridcolor="rgba(23,34,27,0.14)")
    fig_edges.update_yaxes(showgrid=False)
    st.plotly_chart(
        fig_edges,
        width="stretch",
        config=build_plotly_chart_config(),
    )
    render_figure_analysis(analyze_transition_chart(transition_edges))


if __name__ == "__main__":
    main()

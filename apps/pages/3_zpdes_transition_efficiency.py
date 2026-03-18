"""Render a static before/after cohort view on top of the ZPDES layout."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
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
from plotly_config import build_plotly_chart_config
from runtime_bootstrap import bootstrap_runtime_assets
from runtime_paths import ZPDES_TRANSITION_EFFICIENCY_RUNTIME_RELATIVE_PATHS

from visu2.config import get_settings
from visu2.figure_analysis import analyze_zpdes_transition_population
from visu2.zpdes_dependencies import (
    build_dependency_tables_from_metadata,
    list_supported_module_codes_from_metadata,
)
from visu2.zpdes_transition_efficiency import (
    NODE_METRIC_LABELS,
    NODE_METRIC_OPTIONS,
    PROGRESSION_EVENT_COLUMNS,
    attach_progression_cohort_metrics_to_nodes,
    attach_transition_metric_to_nodes,
    build_transition_efficiency_figure,
    objective_sort_key,
)

st.set_page_config(
    page_title="ZPDES Transition Efficiency",
    page_icon=":bar_chart:",
    layout="wide",
)

st.markdown(
    """
<style>
h1, h2, h3 {
  font-family: "Fraunces", Georgia, serif !important;
}
div, p, label {
  font-family: "IBM Plex Sans", sans-serif !important;
}
[data-testid="stMetric"] {
  border: 1px solid rgba(23, 34, 27, 0.10);
  border-radius: 14px;
  padding: 0.85rem;
}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def _parquet_columns(path: Path) -> list[str]:
    """Return parquet column names without loading the full file."""
    return list(pq.ParquetFile(path).schema_arrow.names)


@st.cache_data(show_spinner=False)
def _load_activity_daily(path: Path) -> pl.DataFrame:
    """Load activity-daily aggregates."""
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def _load_activity_elo(path: Path) -> pl.DataFrame:
    """Load activity-level Elo aggregates."""
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def _load_dependency_tables(
    module_code: str,
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    """Load normalized dependency nodes and edges for one module."""
    return build_dependency_tables_from_metadata(
        module_code=module_code,
        learning_catalog_path=learning_catalog_path,
        zpdes_rules_path=zpdes_rules_path,
    )


@st.cache_data(show_spinner=False)
def _load_zpdes_population_summary(path: Path, work_mode: str, later_attempt_threshold: int) -> pl.DataFrame:
    """Aggregate global ZPDES cohort metrics across all supported modules."""
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
    return (
        pl.scan_parquet(path)
        .select(
            [
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "work_mode",
                "user_id",
                "exercise_first_attempt_outcome",
                "prior_before_activity_attempt_count",
                "prior_same_activity_attempt_count",
                "prior_later_activity_attempt_count",
            ]
        )
        .filter(pl.col("work_mode") == work_mode)
        .group_by(
            [
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
            ]
        )
        .agg(
            pl.len().cast(pl.Int64).alias("zpdes_first_attempt_event_count"),
            pl.col("exercise_first_attempt_outcome")
            .cast(pl.Int64)
            .sum()
            .cast(pl.Int64)
            .alias("zpdes_first_attempt_success_count"),
            pl.col("exercise_first_attempt_outcome")
            .cast(pl.Float64)
            .mean()
            .alias("zpdes_first_attempt_success_rate"),
            pl.col("exercise_first_attempt_outcome")
            .filter(before_condition)
            .count()
            .cast(pl.Int64)
            .alias("before_event_count"),
            pl.col("exercise_first_attempt_outcome")
            .filter(before_condition)
            .cast(pl.Int64)
            .sum()
            .cast(pl.Int64)
            .alias("before_success_count"),
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
            pl.col("exercise_first_attempt_outcome")
            .filter(after_condition)
            .count()
            .cast(pl.Int64)
            .alias("after_event_count"),
            pl.col("exercise_first_attempt_outcome")
            .filter(after_condition)
            .cast(pl.Int64)
            .sum()
            .cast(pl.Int64)
            .alias("after_success_count"),
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
            pl.col("exercise_first_attempt_outcome")
            .filter(in_activity_condition)
            .count()
            .cast(pl.Int64)
            .alias("in_activity_event_count"),
            pl.col("exercise_first_attempt_outcome")
            .filter(in_activity_condition)
            .cast(pl.Int64)
            .sum()
            .cast(pl.Int64)
            .alias("in_activity_success_count"),
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
        )
        .collect()
    )


def main() -> None:
    """Render the transition-efficiency page."""
    bootstrap_runtime_assets(ZPDES_TRANSITION_EFFICIENCY_RUNTIME_RELATIVE_PATHS)
    settings = get_settings()
    activity_path = settings.artifacts_derived_dir / "agg_activity_daily.parquet"
    activity_elo_path = settings.artifacts_derived_dir / "agg_activity_elo.parquet"
    arrival_path = settings.artifacts_derived_dir / "zpdes_exercise_progression_events.parquet"

    required_paths = [activity_path, activity_elo_path, arrival_path]
    missing = [path for path in required_paths if not path.exists()]
    if missing:
        st.error("Missing runtime artifacts required by the transition-efficiency page.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    activity_columns = set(_parquet_columns(activity_path))
    activity_elo_columns = set(_parquet_columns(activity_elo_path))
    arrival_columns = set(_parquet_columns(arrival_path))

    missing_activity = sorted(
        {
            "date_utc",
            "module_code",
            "activity_id",
            "first_attempt_success_rate",
            "first_attempt_count",
        }
        - activity_columns
    )
    missing_activity_elo = sorted(
        {"module_code", "activity_id", "activity_mean_exercise_elo"} - activity_elo_columns
    )
    missing_arrivals = sorted(set(PROGRESSION_EVENT_COLUMNS) - arrival_columns)
    if missing_activity or missing_activity_elo or missing_arrivals:
        st.error("Transition-efficiency page artifacts are incompatible with the current runtime contract.")
        if missing_activity:
            st.markdown("- `agg_activity_daily`: " + ", ".join(missing_activity))
        if missing_activity_elo:
            st.markdown("- `agg_activity_elo`: " + ", ".join(missing_activity_elo))
        if missing_arrivals:
            st.markdown("- `zpdes_exercise_progression_events`: " + ", ".join(missing_arrivals))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    activity = _load_activity_daily(activity_path)
    activity_elo = _load_activity_elo(activity_elo_path)

    observed_modules = {
        str(code)
        for code in activity.select(pl.col("module_code").drop_nulls().unique())["module_code"].to_list()
        if str(code).strip()
    }
    module_codes = list_supported_module_codes_from_metadata(
        settings.learning_catalog_path,
        settings.zpdes_rules_path,
        observed_module_codes=observed_modules,
    )
    if not module_codes:
        st.error("No modules available for ZPDES transition analysis.")
        st.stop()

    st.title("ZPDES Transition Efficiency")
    st.write(
        "The primary mode is an adaptive system based on a Zone of Proximal Development with "
        "Estimated Success (ZPDES) approach. In this setting, students initially encounter "
        "activities with minimal prerequisites, and the system continuously updates estimates "
        "of mastery based on observed success rates. As mastery is inferred, new activities may "
        "unlock dynamically, sometimes across different objectives. As a result, learning "
        "trajectories are not strictly linear but evolve in a personalized manner over time."
    )
    render_figure_info("zpdes_transition_efficiency_graph")

    st.sidebar.header("Graph Controls")
    selected_module = st.sidebar.selectbox("Module", module_codes, index=0)

    metric_label = st.sidebar.selectbox("Activity coloring", list(NODE_METRIC_OPTIONS.keys()), index=0)
    metric = NODE_METRIC_OPTIONS[metric_label]
    selected_work_mode = "zpdes"
    later_attempt_threshold = int(
        st.sidebar.number_input(
            'Minimum prior later attempts for "after" cohort',
            min_value=1,
            value=1,
            step=1,
        )
    )
    show_ids = bool(st.sidebar.checkbox("Show IDs in hover", value=False))

    all_nodes, all_edges, warnings = _load_dependency_tables(
        selected_module,
        settings.learning_catalog_path,
        settings.zpdes_rules_path,
    )
    if all_nodes.height == 0:
        st.warning("No dependency nodes found for the selected module.")
        if warnings:
            st.info("\n".join(f"- {warning}" for warning in warnings))
        st.stop()

    progression_events = pl.scan_parquet(arrival_path).select(
        [
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
        ]
    )

    nodes_with_metric = attach_transition_metric_to_nodes(
        nodes=all_nodes,
        agg_activity_elo=activity_elo,
        progression_events=progression_events,
        module_code=selected_module,
        start_date=None,
        end_date=None,
        metric=metric,
        work_mode=selected_work_mode,
    )
    nodes_with_cohorts = attach_progression_cohort_metrics_to_nodes(
        nodes=nodes_with_metric,
        progression_events=progression_events,
        module_code=selected_module,
        start_date=None,
        end_date=None,
        work_mode=selected_work_mode,
        later_attempt_threshold=later_attempt_threshold,
    )
    objective_options = sorted(
        {
            str(code)
            for code in nodes_with_cohorts.select(pl.col("objective_code").drop_nulls().unique())["objective_code"].to_list()
            if str(code).strip()
        },
        key=objective_sort_key,
    )
    selected_objectives = st.sidebar.multiselect(
        "Objectives in module",
        options=objective_options,
        default=objective_options,
    )
    if not selected_objectives:
        st.info("Select at least one objective to render the graph.")
        st.stop()

    filtered_nodes = nodes_with_cohorts.filter(pl.col("objective_code").is_in(selected_objectives))
    valid_codes = {
        str(code)
        for code in filtered_nodes.select(pl.col("node_code").drop_nulls().unique())["node_code"].to_list()
    }
    filtered_edges = all_edges.filter(
        pl.col("from_node_code").is_in(list(valid_codes)) & pl.col("to_node_code").is_in(list(valid_codes))
    )
    if filtered_nodes.height == 0:
        st.info("No nodes remain after the current objective filter.")
        st.stop()

    figure = build_transition_efficiency_figure(
        nodes=filtered_nodes,
        edges=filtered_edges,
        metric=metric,
        metric_label=NODE_METRIC_LABELS[metric],
        later_attempt_threshold=later_attempt_threshold,
        show_ids=show_ids,
        curve_intra_objective_edges=True,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        key="zpdes_transition_efficiency_graph",
        config=build_plotly_chart_config(
            modebar_buttons_to_remove=["select2d", "lasso2d"]
        ),
    )
    population_summary = _load_zpdes_population_summary(
        arrival_path,
        selected_work_mode,
        later_attempt_threshold,
    )
    render_figure_analysis(
        analyze_zpdes_transition_population(
            population_summary,
            later_attempt_threshold=later_attempt_threshold,
        )
    )

    if metric == "activity_mean_exercise_elo":
        st.info(
            "Activity mean exercise Elo is globally calibrated and does not change across the selected ZPDES history."
        )

if __name__ == "__main__":
    main()

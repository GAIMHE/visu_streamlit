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

from figure_info import render_figure_info
from runtime_bootstrap import bootstrap_runtime_assets

from visu2.config import get_settings
from visu2.zpdes_dependencies import (
    build_dependency_tables_from_metadata,
    list_supported_module_codes_from_metadata,
)
from visu2.zpdes_transition_efficiency import (
    NODE_METRIC_LABELS,
    NODE_METRIC_OPTIONS,
    PROGRESSION_EVENT_COLUMNS,
    TRANSITION_WORK_MODE_OPTIONS,
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
def _load_progression_events(path: Path) -> pl.DataFrame:
    """Load exercise-level progression events."""
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


def main() -> None:
    """Render the transition-efficiency page."""
    bootstrap_runtime_assets()
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
    progression_events = _load_progression_events(arrival_path)

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

    min_date = progression_events["date_utc"].min()
    max_date = progression_events["date_utc"].max()
    if min_date is None or max_date is None:
        st.info("No exercise progression cohort data is available.")
        st.stop()

    st.title("ZPDES Transition Efficiency")
    render_figure_info("zpdes_transition_efficiency_graph")

    st.sidebar.header("Cohort Controls")
    selected_module = st.sidebar.selectbox("Module", module_codes, index=0)
    start_date, end_date = st.sidebar.date_input(
        "Date range (UTC)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("Please select a valid date range.")
        st.stop()

    metric_label = st.sidebar.selectbox("Activity coloring", list(NODE_METRIC_OPTIONS.keys()), index=0)
    metric = NODE_METRIC_OPTIONS[metric_label]
    transition_population_label = st.sidebar.selectbox(
        "Cohort population",
        options=list(TRANSITION_WORK_MODE_OPTIONS.keys()),
        index=0,
    )
    selected_work_mode = TRANSITION_WORK_MODE_OPTIONS[transition_population_label]
    later_attempt_threshold = int(
        st.sidebar.number_input(
            'Minimum prior later attempts for "after" cohort',
            min_value=1,
            value=1,
            step=1,
        )
    )
    curve_intra_objective_edges = bool(
        st.sidebar.checkbox("Curve intra-objective structural edges", value=True)
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

    nodes_with_metric = attach_transition_metric_to_nodes(
        nodes=all_nodes,
        agg_activity_daily=activity,
        agg_activity_elo=activity_elo,
        progression_events=progression_events,
        module_code=selected_module,
        start_date=start_date,
        end_date=end_date,
        metric=metric,
        work_mode=selected_work_mode,
    )
    nodes_with_cohorts = attach_progression_cohort_metrics_to_nodes(
        nodes=nodes_with_metric,
        progression_events=progression_events,
        module_code=selected_module,
        start_date=start_date,
        end_date=end_date,
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
        curve_intra_objective_edges=curve_intra_objective_edges,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        key="zpdes_transition_efficiency_graph",
        config={"modeBarButtonsToRemove": ["select2d", "lasso2d"]},
    )

    if metric == "activity_mean_exercise_elo":
        st.info(
            "Activity mean exercise Elo is globally calibrated and does not change with the date filter. "
            "The date filter still applies to the exercise progression cohort metrics shown in node hover."
        )

    if warnings:
        with st.expander("Metadata warnings", expanded=False):
            st.markdown("\n".join(f"- {warning}" for warning in warnings))


if __name__ == "__main__":
    main()

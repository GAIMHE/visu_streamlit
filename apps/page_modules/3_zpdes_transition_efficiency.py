"""Render a static before/after cohort view on top of the ZPDES layout."""

from __future__ import annotations

import sys
from datetime import date
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
from overview_shared import render_population_filters
from plotly_config import build_plotly_chart_config
from source_state import get_active_source_id

from visu2.config import get_settings
from visu2.figure_analysis import analyze_zpdes_transition_population
from visu2.remote_query import query_fact_attempts
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


@st.cache_data(show_spinner=False)
def _parquet_columns(path: Path) -> list[str]:
    """Return parquet column names without loading the full file."""
    return list(pq.ParquetFile(path).schema_arrow.names)


@st.cache_data(show_spinner=False)
def _load_activity_daily_summary(path: Path) -> dict[str, object]:
    """Load only the summary needed from activity-daily aggregates."""
    summary_row = (
        pl.scan_parquet(path)
        .select(
            [
                pl.col("date_utc").min().alias("min_date"),
                pl.col("date_utc").max().alias("max_date"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )
    module_codes = (
        pl.scan_parquet(path)
        .select(pl.col("module_code").drop_nulls().unique().sort())
        .collect()["module_code"]
        .to_list()
    )
    return {
        "min_date": summary_row.get("min_date"),
        "max_date": summary_row.get("max_date"),
        "module_codes": [str(code) for code in module_codes if str(code or "").strip()],
    }


@st.cache_data(show_spinner=False)
def _load_activity_elo_for_module(path: Path, module_code: str) -> pl.DataFrame:
    """Load only the selected module slice from activity-level Elo aggregates."""
    return (
        pl.scan_parquet(path)
        .filter(pl.col("module_code") == module_code)
        .select(["module_code", "activity_id", "activity_mean_exercise_elo"])
        .collect()
    )


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
def _load_zpdes_population_summary(
    path: Path,
    work_mode: str,
    later_attempt_threshold: int,
    *,
    module_code: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    user_ids: tuple[str, ...] | None = None,
) -> pl.DataFrame:
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
    query = (
        pl.scan_parquet(path)
        .select(
            [
                "date_utc",
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
    )
    if module_code:
        query = query.filter(pl.col("module_code") == module_code)
    if start_date is not None and end_date is not None:
        query = query.filter(
            (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
        )
    if user_ids:
        query = query.filter(pl.col("user_id").is_in(list(user_ids)))
    return (
        query.group_by(
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


def _extract_selected_node_code(event: object) -> str | None:
    """Extract a selected node code from a Plotly point-selection event."""
    if not isinstance(event, dict):
        return None
    selection = event.get("selection")
    if not isinstance(selection, dict):
        return None
    points = selection.get("points")
    if not isinstance(points, list) or not points:
        return None
    first_point = points[0]
    if not isinstance(first_point, dict):
        return None
    customdata = first_point.get("customdata")
    if isinstance(customdata, (list, tuple)) and customdata:
        node_code = str(customdata[0] or "").strip()
        return node_code or None
    return None


def main() -> None:
    """Render the transition-efficiency page."""
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
    settings = get_settings(get_active_source_id())
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

    activity_summary = _load_activity_daily_summary(activity_path)

    observed_modules = {
        str(code)
        for code in activity_summary["module_codes"]
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

    min_date = activity_summary["min_date"]
    max_date = activity_summary["max_date"]
    population_filters = render_population_filters(
        source_id=settings.source_id,
        min_date=min_date,
        max_date=max_date,
        sidebar_header="Graph Controls",
    )
    start_date = population_filters.start_date
    end_date = population_filters.end_date
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
    activity_elo = _load_activity_elo_for_module(activity_elo_path, selected_module)

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
    eligible_user_ids: tuple[str, ...] | None = None
    if population_filters.min_student_attempts > 1:
        eligible_users = query_fact_attempts(
            settings,
            start_date=start_date,
            end_date=end_date,
            columns=("user_id",),
            module_code=selected_module,
            work_mode=selected_work_mode,
            min_student_attempts=population_filters.min_student_attempts,
        )
        if eligible_users.height == 0:
            st.info("No ZPDES students remain after the current minimum-attempt threshold.")
            st.stop()
        eligible_user_ids = tuple(
            str(user_id)
            for user_id in eligible_users["user_id"].to_list()
            if str(user_id or "").strip()
        )
        progression_events = progression_events.filter(pl.col("user_id").is_in(list(eligible_user_ids)))

    nodes_with_metric = attach_transition_metric_to_nodes(
        nodes=all_nodes,
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

    context_key = "zpdes_transition_efficiency_selection_context"
    selected_node_key = "zpdes_transition_efficiency_selected_node"
    current_context = {
        "module_code": selected_module,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "metric": metric,
        "later_attempt_threshold": later_attempt_threshold,
        "selected_objectives": tuple(selected_objectives),
        "min_student_attempts": population_filters.min_student_attempts,
    }
    if st.session_state.get(context_key) != current_context:
        st.session_state[context_key] = current_context
        st.session_state.pop(selected_node_key, None)
    focused_node_code = st.session_state.get(selected_node_key)
    if isinstance(focused_node_code, str) and focused_node_code not in valid_codes:
        st.session_state.pop(selected_node_key, None)
        focused_node_code = None

    figure = build_transition_efficiency_figure(
        nodes=filtered_nodes,
        edges=filtered_edges,
        metric=metric,
        metric_label=NODE_METRIC_LABELS[metric],
        later_attempt_threshold=later_attempt_threshold,
        show_ids=show_ids,
        curve_intra_objective_edges=True,
        focused_node_code=focused_node_code if isinstance(focused_node_code, str) else None,
    )
    event = st.plotly_chart(
        figure,
        width="stretch",
        key="zpdes_transition_efficiency_graph",
        on_select="rerun",
        selection_mode=("points",),
        config=build_plotly_chart_config(
            modebar_buttons_to_remove=["select2d", "lasso2d"]
        ),
    )
    selected_from_event = _extract_selected_node_code(event)
    if selected_from_event is not None and selected_from_event != st.session_state.get(selected_node_key):
        st.session_state[selected_node_key] = selected_from_event
        st.rerun()
    if selected_from_event is not None:
        focused_node_code = selected_from_event

    if isinstance(focused_node_code, str) and focused_node_code.strip():
        info_col, button_col = st.columns([4, 1])
        info_col.caption(f"Focused node: `{focused_node_code}`")
        if button_col.button("Clear focus", key="clear_zpdes_transition_focus"):
            st.session_state.pop(selected_node_key, None)
            st.rerun()
    population_summary = _load_zpdes_population_summary(
        arrival_path,
        selected_work_mode,
        later_attempt_threshold,
        module_code=selected_module,
        start_date=start_date,
        end_date=end_date,
        user_ids=eligible_user_ids,
    )
    render_figure_analysis(
        analyze_zpdes_transition_population(
            population_summary,
            later_attempt_threshold=later_attempt_threshold,
        )
    )

    if metric == "activity_mean_exercise_elo":
        st.info(
            "Activity mean exercise Elo is globally calibrated and does not change across the selected ZPDES history or the minimum-attempt threshold."
        )

if __name__ == "__main__":
    main()

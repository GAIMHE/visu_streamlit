from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from overview_shared import (
    collect_core_compatibility,
    collect_lazy,
    format_missing_table_columns,
    load_fact_dimensions,
    normalize_date_input_range,
    parquet_columns,
    render_dashboard_style,
)
from plotly_config import build_plotly_chart_config
from source_state import (
    clear_filter_state,
    get_active_source_id,
    get_filter_date_range,
    set_filter_date_range,
)

from visu2.cohort_filter_viewer import (
    HISTORY_BASIS_OPTIONS,
    RETRY_FILTER_MODE_OPTIONS,
    CohortFilterResult,
    build_final_module_summary,
    build_schema_summary_vs_baseline,
    filter_cohort_view,
)
from visu2.config import get_settings

PAGE_RUNTIME_TABLES: tuple[str, ...] = ("fact_attempt_core",)
MODULE_SELECTION_KEY = "cohort_filter_viewer_modules"
TRANSITION_SELECTION_KEY = "cohort_filter_viewer_transition_counts"
SCHEMA_SELECTION_KEY = "cohort_filter_viewer_schemas"
HISTORY_THRESHOLD_KEY = "cohort_filter_viewer_min_history"
HISTORY_BASIS_KEY = "cohort_filter_viewer_history_basis"
PLACEMENT_THRESHOLD_KEY = "cohort_filter_viewer_min_placement_attempts"
RETRY_FILTER_ENABLED_KEY = "cohort_filter_viewer_retry_filter_enabled"
RETRY_FILTER_MAX_KEY = "cohort_filter_viewer_max_retries"
RETRY_FILTER_MODE_KEY = "cohort_filter_viewer_retry_filter_mode"
REPEAT_MODULE_FILTER_KEY = "cohort_filter_viewer_repeat_module_filter"
SCHEMA_MIN_STUDENTS_KEY = "cohort_filter_viewer_schema_min_students"


@st.cache_data(show_spinner=False)
def _load_cohort_attempt_rows(
    fact_path: Path,
    *,
    start_date_iso: str,
    end_date_iso: str,
) -> pl.DataFrame:
    start_date = date.fromisoformat(start_date_iso)
    end_date = date.fromisoformat(end_date_iso)
    query = (
        pl.scan_parquet(fact_path)
        .filter((pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date)))
        .select(["user_id", "created_at", "work_mode", "module_code", "exercise_id", "attempt_number"])
    )
    return collect_lazy(query)


@st.cache_data(show_spinner=False)
def _compute_cohort_result(
    attempt_rows: pl.DataFrame,
    *,
    selected_modules: tuple[str, ...],
    max_retries: int,
    retry_filter_mode: str,
    min_placement_attempts: int,
    reject_same_placement_module_repeat: bool,
    min_history: int,
    history_basis: str,
    selected_transition_counts: tuple[int, ...],
    min_students_per_schema: int,
    selected_schemas: tuple[str, ...],
) -> CohortFilterResult:
    return filter_cohort_view(
        attempt_rows,
        selected_modules=selected_modules,
        max_retries=max_retries,
        retry_filter_mode=retry_filter_mode,
        min_placement_attempts=min_placement_attempts,
        reject_same_placement_module_repeat=reject_same_placement_module_repeat,
        min_history=min_history,
        history_basis=history_basis,
        selected_transition_counts=selected_transition_counts,
        min_students_per_schema=min_students_per_schema,
        selected_schemas=selected_schemas,
    )


def _clamp_date_range(
    start_date: date | None,
    end_date: date | None,
    *,
    min_date: date,
    max_date: date,
) -> tuple[date, date]:
    start = start_date or min_date
    end = end_date or max_date
    if start < min_date:
        start = min_date
    if start > max_date:
        start = max_date
    if end < min_date:
        end = min_date
    if end > max_date:
        end = max_date
    if start > end:
        start, end = min_date, max_date
    return start, end


def _reset_page_local_state() -> None:
    for key in (
        MODULE_SELECTION_KEY,
        TRANSITION_SELECTION_KEY,
        SCHEMA_SELECTION_KEY,
        HISTORY_THRESHOLD_KEY,
        HISTORY_BASIS_KEY,
        PLACEMENT_THRESHOLD_KEY,
        RETRY_FILTER_ENABLED_KEY,
        RETRY_FILTER_MAX_KEY,
        RETRY_FILTER_MODE_KEY,
        REPEAT_MODULE_FILTER_KEY,
        SCHEMA_MIN_STUDENTS_KEY,
    ):
        st.session_state.pop(key, None)


def _get_source_scoped_date_defaults(*, source_id: str, min_date: date, max_date: date) -> tuple[date, date]:
    stored_start, stored_end = get_filter_date_range(source_id)
    return _clamp_date_range(
        stored_start,
        stored_end,
        min_date=min_date,
        max_date=max_date,
    )


def _sync_multiselect_state(key: str, options: list[object], *, default_all: bool) -> None:
    current = st.session_state.get(key)
    if current is None:
        st.session_state[key] = list(options) if default_all else []
        return
    normalized = [value for value in current if value in options]
    st.session_state[key] = normalized


def _build_stage_bar_figure(frame: pl.DataFrame, *, value_col: str, title: str, yaxis_title: str):
    if frame.height == 0:
        return px.bar(title=title)
    plot_df = frame.to_pandas()
    figure = px.bar(
        plot_df,
        x="stage_label",
        y=value_col,
        text=value_col,
        title=title,
        color_discrete_sequence=["#1e7a52"],
    )
    figure.update_traces(textposition="outside", cliponaxis=False)
    figure.update_layout(
        font={"family": "IBM Plex Sans, Arial, sans-serif", "size": 14, "color": "#17221b"},
        title={"font": {"family": "Fraunces, Georgia, serif", "size": 22}},
        xaxis_title=None,
        yaxis_title=yaxis_title,
        showlegend=False,
        plot_bgcolor="rgba(255,255,255,0)",
        paper_bgcolor="rgba(255,255,255,0)",
    )
    return figure


def _build_stage_module_figure(frame: pl.DataFrame):
    if frame.height == 0:
        return px.bar(title="Attempts by module across stages")
    plot_df = frame.to_pandas()
    figure = px.bar(
        plot_df,
        x="stage_label",
        y="attempts",
        color="module_code",
        title="Attempts by module across stages",
        barmode="stack",
    )
    figure.update_layout(
        font={"family": "IBM Plex Sans, Arial, sans-serif", "size": 14, "color": "#17221b"},
        title={"font": {"family": "Fraunces, Georgia, serif", "size": 22}},
        xaxis_title=None,
        yaxis_title="Attempts",
        legend_title_text="Module",
        plot_bgcolor="rgba(255,255,255,0)",
        paper_bgcolor="rgba(255,255,255,0)",
    )
    return figure


def _build_final_module_figure(frame: pl.DataFrame):
    if frame.height == 0:
        return px.bar(title="Attempts by module in the final cohort")
    plot_df = frame.to_pandas()
    figure = px.bar(
        plot_df,
        x="module_code",
        y="attempts",
        text="attempts",
        title="Attempts by module in the final cohort",
        color_discrete_sequence=["#2148a4"],
    )
    figure.update_traces(textposition="outside", cliponaxis=False)
    figure.update_layout(
        font={"family": "IBM Plex Sans, Arial, sans-serif", "size": 14, "color": "#17221b"},
        title={"font": {"family": "Fraunces, Georgia, serif", "size": 22}},
        xaxis_title="Module",
        yaxis_title="Attempts",
        showlegend=False,
        plot_bgcolor="rgba(255,255,255,0)",
        paper_bgcolor="rgba(255,255,255,0)",
    )
    return figure


def _format_stage_table(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return frame
    return (
        frame.select(
            [
                pl.col("stage_label").alias("Stage"),
                pl.col("students").alias("Students"),
                pl.col("attempts").alias("Attempts"),
                pl.col("mean_attempts_per_student").round(1).alias("Mean attempts / student"),
                pl.col("represented_modules").alias("Represented modules"),
                (pl.col("student_share_vs_baseline") * 100).round(1)
                .cast(pl.Utf8)
                .add(pl.lit("%"))
                .alias("Student share vs baseline"),
                (pl.col("attempt_share_vs_baseline") * 100).round(1)
                .cast(pl.Utf8)
                .add(pl.lit("%"))
                .alias("Attempt share vs baseline"),
                pl.col("module_codes").alias("Module codes"),
            ]
        )
    )


def _format_schema_table(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return frame
    return (
        frame.with_columns(
            (pl.col("student_share") * 100)
            .round(1)
            .cast(pl.Utf8)
            .add(pl.lit("%"))
            .alias("Student share"),
            (pl.col("attempt_share") * 100)
            .round(1)
            .cast(pl.Utf8)
            .add(pl.lit("%"))
            .alias("Attempt share"),
        )
        .rename(
            {
                "cleaned_schema": "Schema",
                "students": "Students",
                "attempts": "Attempts",
            }
        )
        .select(["Schema", "Students", "Student share", "Attempts", "Attempt share"])
    )


def main() -> None:
    render_dashboard_style()
    source_id = get_active_source_id()
    settings = get_settings(source_id)
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    required = [fact_path, settings.learning_catalog_path]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing derived artifacts. Run `python scripts/build_derived.py` first.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    compatibility = collect_core_compatibility(
        table_columns={"fact_attempt_core": parquet_columns(fact_path)},
        required_tables=PAGE_RUNTIME_TABLES,
    )
    if compatibility["status"] == "incompatible":
        st.error(
            "Artifact status: INCOMPATIBLE. One or more core columns are missing. "
            "Rebuild artifacts with `uv run python scripts/build_derived.py --strict-checks`."
        )
        st.markdown("**Missing core columns:**")
        st.markdown(format_missing_table_columns(compatibility["missing_core_by_table"]))
        st.stop()

    dimension_domain = load_fact_dimensions(fact_path, settings.learning_catalog_path)
    default_start_date, default_end_date = _get_source_scoped_date_defaults(
        source_id=source_id,
        min_date=dimension_domain.min_date,
        max_date=dimension_domain.max_date,
    )
    st.sidebar.header("Cohort Filters")
    if st.sidebar.button("Reset cohort filters", key=f"{source_id}_reset_cohort_filters"):
        clear_filter_state(source_id)
        _reset_page_local_state()
        st.session_state[f"{source_id}_cohort_filter_date_range"] = (
            dimension_domain.min_date,
            dimension_domain.max_date,
        )
        set_filter_date_range(
            source_id,
            start_date=dimension_domain.min_date,
            end_date=dimension_domain.max_date,
        )
        st.rerun()

    module_options = (
        dimension_domain.curriculum_frame.select("module_code")
        .drop_nulls()
        .unique()
        .sort("module_code")
        .get_column("module_code")
        .to_list()
    )
    if HISTORY_THRESHOLD_KEY not in st.session_state:
        st.session_state[HISTORY_THRESHOLD_KEY] = 1
    if HISTORY_BASIS_KEY not in st.session_state:
        st.session_state[HISTORY_BASIS_KEY] = "Raw attempts"
    if PLACEMENT_THRESHOLD_KEY not in st.session_state:
        st.session_state[PLACEMENT_THRESHOLD_KEY] = 1
    if RETRY_FILTER_ENABLED_KEY not in st.session_state:
        st.session_state[RETRY_FILTER_ENABLED_KEY] = False
    if RETRY_FILTER_MAX_KEY not in st.session_state:
        st.session_state[RETRY_FILTER_MAX_KEY] = 0
    if RETRY_FILTER_MODE_KEY not in st.session_state:
        st.session_state[RETRY_FILTER_MODE_KEY] = "Remove offending exercises only"
    if REPEAT_MODULE_FILTER_KEY not in st.session_state:
        st.session_state[REPEAT_MODULE_FILTER_KEY] = False
    if SCHEMA_MIN_STUDENTS_KEY not in st.session_state:
        st.session_state[SCHEMA_MIN_STUDENTS_KEY] = 1

    with st.sidebar.form(key=f"{source_id}_cohort_filter_form", clear_on_submit=False):
        date_value = st.date_input(
            "Date range (UTC)",
            value=(default_start_date, default_end_date),
            min_value=dimension_domain.min_date,
            max_value=dimension_domain.max_date,
            key=f"{source_id}_cohort_filter_date_range",
            format="YYYY-MM-DD",
        )
        normalized_date_range = normalize_date_input_range(date_value)
        if normalized_date_range is None:
            st.error("Please provide a valid start and end date.")
            st.stop()
        start_date, end_date = normalized_date_range

        _sync_multiselect_state(MODULE_SELECTION_KEY, module_options, default_all=True)
        selected_modules = tuple(
            st.multiselect(
                "Modules to keep",
                options=module_options,
                key=MODULE_SELECTION_KEY,
                help="Keep only attempts from these modules before path analysis.",
            )
        )

        min_history = int(
            st.number_input(
                "Minimum student history",
                min_value=1,
                max_value=1_000_000,
                step=1,
                key=HISTORY_THRESHOLD_KEY,
                help="Applied after module scoping and placement cleanup.",
            )
        )
        history_basis_label = st.selectbox(
            "History basis",
            options=list(HISTORY_BASIS_OPTIONS.keys()),
            key=HISTORY_BASIS_KEY,
        )
        min_placement_attempts = int(
            st.number_input(
                "Minimum placement attempts",
                min_value=1,
                max_value=1_000_000,
                step=1,
                key=PLACEMENT_THRESHOLD_KEY,
                help=(
                    "Placement segments below this threshold are removed together with the immediately following segment."
                ),
            )
        )
        retry_filter_enabled = bool(
            st.checkbox(
                "Enable max retries filter",
                key=RETRY_FILTER_ENABLED_KEY,
                help=(
                    "Cap retries per student and exercise inside the selected date range and module slice."
                ),
            )
        )
        max_retries = int(
            st.number_input(
                "Max retries per exercise",
                min_value=0,
                max_value=1_000_000,
                step=1,
                key=RETRY_FILTER_MAX_KEY,
                disabled=not retry_filter_enabled,
            )
        )
        retry_filter_mode_label = st.selectbox(
            "Retry overflow handling",
            options=list(RETRY_FILTER_MODE_OPTIONS.keys()),
            key=RETRY_FILTER_MODE_KEY,
            disabled=not retry_filter_enabled,
            help=(
                "Either drop the whole student when one exercise exceeds the cap, or remove only the offending student-exercise rows."
            ),
        )
        effective_max_retries = max_retries if retry_filter_enabled else -1
        reject_same_placement_module_repeat = bool(
            st.checkbox(
                "Reject same placement -> same module loops",
                key=REPEAT_MODULE_FILTER_KEY,
                help=(
                    "Drop students who return to the same module after the same placement mode, such as "
                    "`initial-test -> zpdes(M1) -> initial-test -> zpdes(M1)`."
                ),
            )
        )
        min_students_per_schema = int(
            st.number_input(
                "Minimum students per schema",
                min_value=1,
                max_value=1_000_000,
                step=1,
                key=SCHEMA_MIN_STUDENTS_KEY,
                help="Remove cleaned schemas represented by fewer than this many students before exact schema selection.",
            )
        )

        attempt_rows = _load_cohort_attempt_rows(
            fact_path,
            start_date_iso=start_date.isoformat(),
            end_date_iso=end_date.isoformat(),
        )

        current_transition_counts = tuple(st.session_state.get(TRANSITION_SELECTION_KEY, []))
        current_schemas = tuple(st.session_state.get(SCHEMA_SELECTION_KEY, []))

        result = _compute_cohort_result(
            attempt_rows,
            selected_modules=selected_modules,
            max_retries=effective_max_retries,
            retry_filter_mode=RETRY_FILTER_MODE_OPTIONS[retry_filter_mode_label],
            min_placement_attempts=min_placement_attempts,
            reject_same_placement_module_repeat=reject_same_placement_module_repeat,
            min_history=min_history,
            history_basis=HISTORY_BASIS_OPTIONS[history_basis_label],
            selected_transition_counts=tuple(int(value) for value in current_transition_counts),
            min_students_per_schema=min_students_per_schema,
            selected_schemas=tuple(str(value) for value in current_schemas),
        )

        transition_options = (
            result.transition_options.get_column("transition_count").to_list()
            if result.transition_options.height
            else []
        )
        _sync_multiselect_state(TRANSITION_SELECTION_KEY, transition_options, default_all=False)
        selected_transition_counts = tuple(
            int(value)
            for value in st.multiselect(
                "Transition counts",
                options=transition_options,
                key=TRANSITION_SELECTION_KEY,
                help=(
                    "Leave empty to keep all transition counts after cleanup and history filtering. "
                    "Options refresh when you click Process cohort."
                ),
            )
        )

        result = _compute_cohort_result(
            attempt_rows,
            selected_modules=selected_modules,
            max_retries=effective_max_retries,
            retry_filter_mode=RETRY_FILTER_MODE_OPTIONS[retry_filter_mode_label],
            min_placement_attempts=min_placement_attempts,
            reject_same_placement_module_repeat=reject_same_placement_module_repeat,
            min_history=min_history,
            history_basis=HISTORY_BASIS_OPTIONS[history_basis_label],
            selected_transition_counts=selected_transition_counts,
            min_students_per_schema=min_students_per_schema,
            selected_schemas=(),
        )

        schema_options = (
            result.schema_options.get_column("cleaned_schema").to_list()
            if result.schema_options.height
            else []
        )
        _sync_multiselect_state(SCHEMA_SELECTION_KEY, schema_options, default_all=False)
        selected_schemas = tuple(
            str(value)
            for value in st.multiselect(
                "Exact schemas",
                options=schema_options,
                key=SCHEMA_SELECTION_KEY,
                help=(
                    "Leave empty to keep all schemas after the transition-count and schema-size filters. "
                    "Options refresh when you click Process cohort."
                ),
            )
        )

        process_filters = st.form_submit_button("Process cohort", type="primary")

    if attempt_rows.height == 0:
        st.info("No attempts are available for the selected date range.")
        return

    if process_filters:
        set_filter_date_range(source_id, start_date=start_date, end_date=end_date)

    final_result = _compute_cohort_result(
        attempt_rows,
        selected_modules=selected_modules,
        max_retries=effective_max_retries,
        retry_filter_mode=RETRY_FILTER_MODE_OPTIONS[retry_filter_mode_label],
        min_placement_attempts=min_placement_attempts,
        reject_same_placement_module_repeat=reject_same_placement_module_repeat,
        min_history=min_history,
        history_basis=HISTORY_BASIS_OPTIONS[history_basis_label],
        selected_transition_counts=selected_transition_counts,
        min_students_per_schema=min_students_per_schema,
        selected_schemas=selected_schemas,
    )

    st.title("Cohort Filter Viewer")
    st.caption(
        "Inspect how module, path, and history constraints change the retained student population and attempt volume."
    )

    stage_summary = final_result.stage_summary
    stage_module_attempts = final_result.stage_module_attempts

    st.subheader("Funnel")
    st.dataframe(_format_stage_table(stage_summary), use_container_width=True, hide_index=True)

    chart_config = build_plotly_chart_config(modebar_buttons_to_remove=["lasso2d", "select2d"])
    students_chart, attempts_chart = st.columns(2)
    with students_chart:
        st.plotly_chart(
            _build_stage_bar_figure(
                stage_summary,
                value_col="students",
                title="Students retained across stages",
                yaxis_title="Students",
            ),
            use_container_width=True,
            config=chart_config,
        )
    with attempts_chart:
        st.plotly_chart(
            _build_stage_bar_figure(
                stage_summary,
                value_col="attempts",
                title="Attempts retained across stages",
                yaxis_title="Attempts",
            ),
            use_container_width=True,
            config=chart_config,
        )

    st.plotly_chart(
        _build_stage_module_figure(stage_module_attempts),
        use_container_width=True,
        config=chart_config,
    )

    st.subheader("Final Slice")
    final_rows = final_result.final_rows
    final_user_paths = final_result.final_user_paths

    if final_rows.height == 0 or final_user_paths.height == 0:
        st.info("No students remain after the current cohort filters. Try relaxing the module, placement, history, transition, or schema constraints.")
        return

    final_module_summary = build_final_module_summary(final_rows)
    final_schema_summary = build_schema_summary_vs_baseline(
        final_user_paths,
        baseline_students=final_result.baseline_students,
        baseline_attempts=final_result.baseline_attempts,
    )
    final_students = int(final_user_paths.height)
    final_attempts = int(final_rows.height)
    final_mean_attempts = final_attempts / final_students if final_students else 0.0
    final_median_attempts = float(final_user_paths.get_column("retained_attempts").median()) if final_students else 0.0
    represented_modules = int(final_module_summary.height)
    final_student_share = (
        final_students / final_result.baseline_students if final_result.baseline_students else 0.0
    )
    final_attempt_share = (
        final_attempts / final_result.baseline_attempts if final_result.baseline_attempts else 0.0
    )

    metric_cols = st.columns(5)
    metric_cols[0].metric("Students", f"{final_students:,}", delta=f"{final_student_share:.1%} of full slice")
    metric_cols[1].metric("Attempts", f"{final_attempts:,}", delta=f"{final_attempt_share:.1%} of full slice")
    metric_cols[2].metric("Mean attempts / student", f"{final_mean_attempts:,.1f}")
    metric_cols[3].metric("Median attempts / student", f"{final_median_attempts:,.1f}")
    metric_cols[4].metric("Represented modules", f"{represented_modules:,}")

    st.plotly_chart(
        _build_final_module_figure(final_module_summary),
        use_container_width=True,
        config=chart_config,
    )
    st.dataframe(_format_schema_table(final_schema_summary), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

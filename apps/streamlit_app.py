"""Main Streamlit overview page for top-level learning analytics summaries."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from figure_analysis import render_figure_analysis
from figure_info import render_figure_info
from overview_shared import (
    build_fact_query,
    collect_core_compatibility,
    collect_lazy,
    format_missing_table_columns,
    load_fact_dimensions,
    parquet_columns,
    render_curriculum_filters,
    render_dashboard_style,
)
from plotly_config import build_plotly_chart_config
from runtime_bootstrap import bootstrap_runtime_assets
from runtime_paths import OVERVIEW_RUNTIME_RELATIVE_PATHS

from visu2.config import get_settings
from visu2.figure_analysis import (
    analyze_overview_concentration,
    analyze_overview_kpis,
    analyze_work_mode_summary,
    analyze_work_mode_transitions,
)
from visu2.overview_concentration import (
    CONCENTRATION_BASIS_OPTIONS,
    CONCENTRATION_LEVEL_OPTIONS,
    build_bucket_summary,
    build_concentration_figure,
    build_entity_attempt_summary,
    build_global_student_attempt_summary,
    extract_selected_bucket,
    load_catalog_contained_exercise_counts,
)
from visu2.work_mode_transitions import build_work_mode_transition_sankey

st.set_page_config(
    page_title="Learning Analytics Overview",
    page_icon=":bar_chart:",
    layout="wide",
)

render_dashboard_style()

OVERVIEW_RUNTIME_TABLES: tuple[str, ...] = ("fact_attempt_core", "work_mode_transition_paths")


@st.cache_data(show_spinner=False)
def _load_concentration_catalog_counts(path: Path) -> dict[str, pl.DataFrame]:
    """Load catalog-based contained-exercise counts for overview concentration views."""
    return load_catalog_contained_exercise_counts(path)


@st.cache_data(show_spinner=False)
def _load_work_mode_transition_paths(path: Path) -> pl.DataFrame:
    """Load global student work-mode transition paths for the overview Sankey."""
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def _load_content_concentration_frames(
    fact_path: Path,
    learning_catalog_path: Path,
    *,
    start_date_iso: str,
    end_date_iso: str,
    module_code: str | None,
    objective_id: str | None,
    activity_id: str | None,
    work_modes: tuple[str, ...],
    level: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load entity and bucket summaries for the overview concentration chart."""
    fact_query = build_fact_query(
        fact_path=fact_path,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        module_code=module_code,
        objective_id=objective_id,
        activity_id=activity_id,
    )
    catalog_counts = _load_concentration_catalog_counts(learning_catalog_path)
    entity_summary = build_entity_attempt_summary(
        fact_query,
        level=level,
        work_modes=work_modes,
        contained_exercise_counts=catalog_counts,
    )
    bucket_summary = build_bucket_summary(entity_summary, level=level)
    return entity_summary, bucket_summary


@st.cache_data(show_spinner=False)
def _load_global_student_concentration_frames(
    fact_path: Path,
    *,
    start_date_iso: str,
    end_date_iso: str,
    module_code: str | None,
    objective_id: str | None,
    activity_id: str | None,
    work_modes: tuple[str, ...],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load student-level global concentration summaries for the overview chart."""
    fact_query = build_fact_query(
        fact_path=fact_path,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        module_code=module_code,
        objective_id=objective_id,
        activity_id=activity_id,
    )
    entity_summary = build_global_student_attempt_summary(
        fact_query,
        work_modes=work_modes,
    )
    bucket_summary = build_bucket_summary(entity_summary, level="student")
    return entity_summary, bucket_summary


def main() -> None:
    """Render the simplified overview page."""
    bootstrap_runtime_assets(OVERVIEW_RUNTIME_RELATIVE_PATHS)
    settings = get_settings()
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    work_mode_transition_path = settings.artifacts_derived_dir / "work_mode_transition_paths.parquet"

    required = [fact_path, work_mode_transition_path]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing derived artifacts. Run `python scripts/build_derived.py` first.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    compatibility = collect_core_compatibility(
        table_columns={
            "fact_attempt_core": parquet_columns(fact_path),
            "work_mode_transition_paths": parquet_columns(work_mode_transition_path),
        },
        required_tables=OVERVIEW_RUNTIME_TABLES,
    )
    if compatibility["status"] == "incompatible":
        st.error(
            "Artifact status: INCOMPATIBLE. One or more core columns are missing. "
            "Rebuild artifacts with `uv run python scripts/build_derived.py --strict-checks`."
        )
        st.markdown("**Missing core columns:**")
        st.markdown(format_missing_table_columns(compatibility["missing_core_by_table"]))
        st.stop()

    dimension_domain = load_fact_dimensions(fact_path)
    filters = render_curriculum_filters(dimension_domain)

    fact_query = build_fact_query(
        fact_path=fact_path,
        start_date=filters.start_date,
        end_date=filters.end_date,
        module_code=filters.module_code,
        objective_id=filters.objective_id,
        activity_id=filters.activity_id,
    )

    kpi = fact_query.select(
        pl.len().alias("attempts"),
        pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
        pl.col("exercise_id").drop_nulls().n_unique().alias("unique_exercises"),
    )
    kpi = collect_lazy(kpi).to_dicts()[0]

    st.title("Learning Analytics Overview")

    c1, c2, c3 = st.columns(3)
    c1.metric("Attempts", f"{int(kpi['attempts']):,}")
    c2.metric("Unique Students", f"{int(kpi['unique_students']):,}")
    c3.metric("Unique Exercises", f"{int(kpi['unique_exercises']):,}")

    st.markdown(
        "The Adaptiv'Math dataset contains interaction traces from a large-scale adaptive digital "
        "math learning environment used in real classrooms.\n\n"
        "It includes learning trajectories from more than 29,000 students, capturing how learners "
        "navigate structured math content over time.\n\n"
        "The traces combine algorithm-driven progression and teacher-defined sequencing."
    )
    render_figure_analysis(
        analyze_overview_kpis(
            attempts=int(kpi["attempts"]),
            unique_students=int(kpi["unique_students"]),
            unique_exercises=int(kpi["unique_exercises"]),
        )
    )

    st.subheader("Work Mode Summary")
    render_figure_info("overview_work_mode_summary_table")
    work_mode_summary = (
        fact_query.filter(pl.col("work_mode").is_not_null())
        .group_by("work_mode")
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
            pl.col("module_code").drop_nulls().n_unique().alias("unique_modules_explored"),
            pl.col("objective_id").drop_nulls().n_unique().alias("unique_objectives_explored"),
            pl.col("activity_id").drop_nulls().n_unique().alias("unique_activities_explored"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
        )
        .join(
            fact_query.filter(pl.col("work_mode").is_not_null() & pl.col("exercise_id").is_not_null())
            .group_by(["work_mode", "exercise_id"])
            .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("exercise_success_rate"))
            .group_by("work_mode")
            .agg(
                pl.col("exercise_success_rate")
                .mean()
                .alias("exercise_balanced_success_rate")
            ),
            on="work_mode",
            how="left",
        )
        .join(
            fact_query.filter(pl.col("work_mode").is_not_null() & pl.col("activity_id").is_not_null())
            .group_by(["work_mode", "activity_id"])
            .agg(pl.len().alias("activity_attempts"))
            .group_by("work_mode")
            .agg(pl.col("activity_attempts").median().alias("median_attempts_per_activity")),
            on="work_mode",
            how="left",
        )
        .sort("attempts", descending=True)
    )
    work_mode_summary = collect_lazy(work_mode_summary)

    if work_mode_summary.height == 0:
        st.info("No work mode rows available after filters.")
        render_figure_analysis(analyze_work_mode_summary(None))
        return

    available_work_modes = work_mode_summary["work_mode"].to_list()
    selected_work_modes = st.multiselect(
        "Work modes shown",
        options=available_work_modes,
        default=available_work_modes,
    )
    if not selected_work_modes:
        st.info("Select at least one work mode to render the summary table.")
        render_figure_analysis(analyze_work_mode_summary(None))
        return

    selected_work_mode_summary = work_mode_summary.filter(pl.col("work_mode").is_in(selected_work_modes))
    if selected_work_mode_summary.height == 0:
        st.info("No rows available for the selected work modes.")
        render_figure_analysis(analyze_work_mode_summary(None))
        return

    summary_table = selected_work_mode_summary.select(
        [
            "work_mode",
            "attempts",
            "success_rate",
            "exercise_balanced_success_rate",
            "unique_students",
            "unique_modules_explored",
            "unique_objectives_explored",
            "unique_activities_explored",
            "median_attempts_per_activity",
            "repeat_attempt_rate",
        ]
    ).sort("attempts", descending=True).to_pandas()
    summary_display = summary_table.copy()
    summary_display["success_rate"] = summary_display["success_rate"].map(
        lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
    )
    summary_display["exercise_balanced_success_rate"] = summary_display[
        "exercise_balanced_success_rate"
    ].map(lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%")
    summary_display["repeat_attempt_rate"] = summary_display["repeat_attempt_rate"].map(
        lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
    )
    st.dataframe(
        summary_display,
        width="stretch",
        hide_index=True,
        column_config={
            "work_mode": "Work mode",
            "attempts": st.column_config.NumberColumn("Attempts", format="%d"),
            "success_rate": "Success rate (attempt-weighted)",
            "exercise_balanced_success_rate": "Success rate (exercise-balanced)",
            "unique_students": st.column_config.NumberColumn("Unique students", format="%d"),
            "unique_modules_explored": st.column_config.NumberColumn(
                "Unique modules explored", format="%d"
            ),
            "unique_objectives_explored": st.column_config.NumberColumn(
                "Unique objectives explored", format="%d"
            ),
            "unique_activities_explored": st.column_config.NumberColumn(
                "Unique activities explored", format="%d"
            ),
            "median_attempts_per_activity": st.column_config.NumberColumn(
                "Median attempts per activity",
                format="%.2f",
            ),
            "repeat_attempt_rate": "Repeat attempt rate",
        },
    )
    render_figure_analysis(analyze_work_mode_summary(selected_work_mode_summary))

    st.subheader("Attempt Concentration")
    render_figure_info("overview_attempt_concentration_chart")

    basis_col, scope_col, mode_col = st.columns([1.2, 1.4, 2.4])
    selected_basis_label = basis_col.radio(
        "Basis",
        options=list(CONCENTRATION_BASIS_OPTIONS.keys()),
        horizontal=True,
        key="overview_attempt_concentration_basis",
    )
    selected_basis = CONCENTRATION_BASIS_OPTIONS[selected_basis_label]
    if selected_basis == "content":
        selected_level_label = scope_col.radio(
            "Level",
            options=list(CONCENTRATION_LEVEL_OPTIONS.keys()),
            horizontal=True,
            key="overview_attempt_concentration_scope",
        )
    else:
        selected_level_label = "All attempts"
        scope_col.markdown("**Student scope**")
        scope_col.caption("Global ranking across all visible attempts.")
    selected_chart_work_modes = mode_col.multiselect(
        "Work modes in concentration chart",
        options=available_work_modes,
        default=available_work_modes,
        key="overview_attempt_concentration_work_modes",
    )
    if not selected_chart_work_modes:
        st.info("Select at least one work mode to render the concentration chart.")
        render_figure_analysis(
            analyze_overview_concentration(
                None,
                None,
                level_label=selected_level_label,
                basis_label=selected_basis_label,
                student_scope_label=selected_level_label if selected_basis == "student" else None,
            )
        )
        return

    selected_level = (
        CONCENTRATION_LEVEL_OPTIONS[selected_level_label]
        if selected_basis == "content"
        else "all_attempts"
    )
    if selected_basis == "content":
        entity_summary, bucket_summary = _load_content_concentration_frames(
            fact_path,
            settings.learning_catalog_path,
            start_date_iso=filters.start_date.isoformat(),
            end_date_iso=filters.end_date.isoformat(),
            module_code=filters.module_code,
            objective_id=filters.objective_id,
            activity_id=filters.activity_id,
            work_modes=tuple(selected_chart_work_modes),
            level=selected_level,
        )
    else:
        entity_summary, bucket_summary = _load_global_student_concentration_frames(
            fact_path,
            start_date_iso=filters.start_date.isoformat(),
            end_date_iso=filters.end_date.isoformat(),
            module_code=filters.module_code,
            objective_id=filters.objective_id,
            activity_id=filters.activity_id,
            work_modes=tuple(selected_chart_work_modes),
        )

    if entity_summary.height == 0 or bucket_summary.height == 0:
        st.info("No concentration rows are available after the current filters.")
        render_figure_analysis(
            analyze_overview_concentration(
                entity_summary,
                bucket_summary,
                level_label=selected_level_label,
                basis_label=selected_basis_label,
                student_scope_label="All attempts" if selected_basis == "student" else None,
            )
        )
        return

    concentration_context_key = "overview_attempt_concentration_context"
    selected_bucket_key = "overview_attempt_concentration_selected_bucket"
    current_context = {
        "basis": selected_basis,
        "level": selected_level,
        "work_modes": tuple(selected_chart_work_modes),
        "start_date": filters.start_date.isoformat(),
        "end_date": filters.end_date.isoformat(),
        "module_code": filters.module_code,
        "objective_id": filters.objective_id,
        "activity_id": filters.activity_id,
    }
    if st.session_state.get(concentration_context_key) != current_context:
        st.session_state[concentration_context_key] = current_context
        st.session_state.pop(selected_bucket_key, None)

    if selected_basis == "content":
        xaxis_title = "Ranked entity bucket" if selected_level != "module" else "Modules"
        count_label = "Entities"
    else:
        xaxis_title = "Ranked student bucket"
        count_label = "Students"

    concentration_figure = build_concentration_figure(
        bucket_summary,
        level=selected_level,
        xaxis_title=xaxis_title,
        count_label=count_label,
    )
    concentration_event = st.plotly_chart(
        concentration_figure,
        key="overview_attempt_concentration_chart",
        width="stretch",
        on_select="rerun",
        selection_mode=("points",),
        config=build_plotly_chart_config(
            modebar_buttons_to_remove=["select2d", "lasso2d"]
        ),
    )
    selected_bucket = extract_selected_bucket(concentration_event)
    if selected_bucket is not None:
        st.session_state[selected_bucket_key] = selected_bucket
    render_figure_analysis(
        analyze_overview_concentration(
            entity_summary,
            bucket_summary,
            level_label=selected_level_label,
            basis_label=selected_basis_label,
            student_scope_label="All attempts" if selected_basis == "student" else None,
        )
    )

    selected_bucket = st.session_state.get(selected_bucket_key)
    if not isinstance(selected_bucket, dict):
        st.info("Click a bar to view the rows inside that bucket.")
    else:
        table_col, clear_col = st.columns([4, 1])
        bucket_label = selected_bucket.get("bucket_label") or selected_bucket.get("bucket_key")
        table_col.caption(f"Rows inside **{bucket_label}**.")
        if clear_col.button("Clear selection", key="clear_overview_attempt_concentration_selection"):
            st.session_state.pop(selected_bucket_key, None)
            st.rerun()

        if selected_basis == "content":
            drilldown = (
                entity_summary.filter(pl.col("bucket_key") == str(selected_bucket.get("bucket_key") or ""))
                .select(["label", "id", "attempts", "attempt_share", "contained_exercises"])
                .sort(["attempts", "label", "id"], descending=[True, False, False])
            )
            if drilldown.height == 0:
                st.info("No entities were found inside the selected bucket.")
            else:
                drilldown_display = drilldown.to_pandas()
                drilldown_display["attempt_share"] = drilldown_display["attempt_share"].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                st.dataframe(
                    drilldown_display,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "label": "Label",
                        "id": "ID",
                        "attempts": st.column_config.NumberColumn("Attempts", format="%d"),
                        "attempt_share": "Attempt share",
                        "contained_exercises": st.column_config.NumberColumn("Contained exercises", format="%d"),
                    },
                )
        else:
            drilldown = (
                entity_summary.filter(pl.col("bucket_key") == str(selected_bucket.get("bucket_key") or ""))
                .select(
                    [
                        pl.col("user_id").alias("id"),
                        "attempts",
                        "attempt_share",
                        "unique_exercises",
                        "unique_activities",
                        "unique_objectives",
                        "unique_modules",
                    ]
                )
                .sort(["attempts", "id"], descending=[True, False])
            )
            if drilldown.height == 0:
                st.info("No students were found inside the selected bucket.")
            else:
                drilldown_display = drilldown.to_pandas()
                drilldown_display["attempt_share"] = drilldown_display["attempt_share"].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                st.dataframe(
                    drilldown_display,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "id": "User ID",
                        "attempts": st.column_config.NumberColumn("Attempts", format="%d"),
                        "attempt_share": "Attempt share",
                        "unique_exercises": st.column_config.NumberColumn("Unique exercises", format="%d"),
                        "unique_activities": st.column_config.NumberColumn("Unique activities", format="%d"),
                        "unique_objectives": st.column_config.NumberColumn("Unique objectives", format="%d"),
                        "unique_modules": st.column_config.NumberColumn("Unique modules", format="%d"),
                    },
                )

    st.subheader("Work Mode Transitions")
    render_figure_info("overview_work_mode_transitions_sankey")
    st.caption("Global full-history view across all students. This chart ignores the page filters above.")
    transition_paths = _load_work_mode_transition_paths(work_mode_transition_path)
    if transition_paths.height == 0:
        st.info("No work-mode transition histories are available.")
        render_figure_analysis(analyze_work_mode_transitions(None))
        return

    sankey_figure = build_work_mode_transition_sankey(transition_paths)
    st.plotly_chart(
        sankey_figure,
        use_container_width=True,
        config=build_plotly_chart_config(),
    )
    render_figure_analysis(analyze_work_mode_transitions(transition_paths))


if __name__ == "__main__":
    main()

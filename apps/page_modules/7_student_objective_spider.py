"""Render a student-by-module objective spider/radar chart."""

from __future__ import annotations

import sys
from pathlib import Path
from random import SystemRandom

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
from source_state import get_active_source_id

from visu2.config import get_settings
from visu2.figure_analysis import analyze_student_objective_spider
from visu2.student_objective_spider import (
    build_student_module_options,
    build_student_objective_spider_figure,
    build_student_objective_summary,
    build_student_selection_profiles,
    load_objective_catalog,
    select_student_by_id,
    select_students_near_attempt_target,
    summarize_student_module_profile,
)


@st.cache_data(show_spinner=False)
def _load_profiles(fact_path: Path) -> pl.DataFrame:
    return build_student_selection_profiles(pl.scan_parquet(fact_path))


@st.cache_data(show_spinner=False)
def _load_objective_catalog(learning_catalog_path: Path) -> pl.DataFrame:
    return load_objective_catalog(learning_catalog_path)


@st.cache_data(show_spinner=False)
def _load_module_options(
    fact_path: Path,
    learning_catalog_path: Path,
    user_id: str,
) -> pl.DataFrame:
    return build_student_module_options(
        pl.scan_parquet(fact_path),
        _load_objective_catalog(learning_catalog_path),
        user_id,
    )


@st.cache_data(show_spinner=False)
def _load_objective_summary(
    fact_path: Path,
    learning_catalog_path: Path,
    user_id: str,
    module_code: str,
) -> pl.DataFrame:
    return build_student_objective_summary(
        pl.scan_parquet(fact_path),
        _load_objective_catalog(learning_catalog_path),
        user_id=user_id,
        module_code=module_code,
    )


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _format_module_option(row: dict[str, object]) -> str:
    module_code = str(row.get("module_code") or "").strip()
    module_label = str(row.get("module_label") or module_code).strip() or module_code
    attempts = int(row.get("attempts") or 0)
    objectives_attempted = int(row.get("objectives_attempted") or 0)
    return f"{module_code} - {module_label} ({attempts} attempts, {objectives_attempted} objectives)"


def main() -> None:
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
  padding: 0.75rem;
}
</style>
""",
        unsafe_allow_html=True,
    )
    settings = get_settings(get_active_source_id())
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    learning_catalog_path = settings.learning_catalog_path

    required = [fact_path, learning_catalog_path]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing runtime assets for the student objective spider page.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    required_columns = {
        "created_at",
        "user_id",
        "module_code",
        "module_label",
        "objective_id",
        "exercise_id",
        "attempt_number",
        "data_correct",
    }
    actual_columns = set(_parquet_columns(fact_path))
    missing_columns = sorted(required_columns - actual_columns)
    if missing_columns:
        st.error("Student Objective Spider cannot run: fact_attempt_core is missing required columns.")
        st.markdown("- " + "\n- ".join(f"`{name}`" for name in missing_columns))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    profiles = _load_profiles(fact_path)
    eligible_profiles = profiles.filter(pl.col("eligible_for_selection"))
    if eligible_profiles.height == 0:
        st.info("No students are available for the spider page.")
        st.stop()

    st.title("Student Objective Spider")
    render_figure_info("student_objective_spider")
    st.caption(
        "This page profiles one student inside one module. The spider overlays all-attempt success rate with catalog-relative exercise coverage across every objective in that module."
    )

    min_attempt_count = int(eligible_profiles["total_attempts"].min() or 0)
    max_attempt_count = int(eligible_profiles["total_attempts"].max() or 0)
    median_attempt_count = int(eligible_profiles["total_attempts"].median() or min_attempt_count or 1)

    st.caption(
        f"Eligible students range from **{min_attempt_count}** to **{max_attempt_count}** attempts."
    )
    target_attempts = int(
        st.number_input(
            "Target attempt count",
            min_value=max(1, min_attempt_count),
            max_value=max(1, max_attempt_count),
            value=min(max(1, median_attempt_count), max(1, max_attempt_count)),
            step=10,
        )
    )
    manual_student_id = st.text_input(
        "Student ID override (optional)",
        value="",
        help="If you enter an eligible student ID here, the page will use that student directly instead of the sampled attempt-range selection.",
    ).strip()

    if manual_student_id:
        selected_student = select_student_by_id(eligible_profiles, manual_student_id)
        if selected_student is None:
            st.info("No eligible student matches that ID. Please check the ID or clear the field.")
            st.stop()
        normalized_student = selected_student
        st.caption("Using the typed student ID override.")
    else:
        selection_signature = ("student_objective_spider_attempt_target", target_attempts)
        selection_state_key = "student_objective_spider_selected_students"
        selection_signature_key = "student_objective_spider_attempt_target_signature"
        if st.session_state.get(selection_signature_key) != selection_signature:
            sampled = select_students_near_attempt_target(
                eligible_profiles,
                target_attempts=target_attempts,
                tolerance_ratio=0.10,
                max_students=1,
                seed=SystemRandom().randrange(0, 2**31 - 1),
            )
            st.session_state[selection_signature_key] = selection_signature
            st.session_state[selection_state_key] = sampled

        selected_students = [
            str(user_id)
            for user_id in st.session_state.get(selection_state_key, [])
            if str(user_id).strip()
        ][:1]
        if not selected_students:
            lower = int(target_attempts * 0.9)
            upper = int(target_attempts * 1.1)
            st.info(
                f"No students found in the {lower}-{upper} attempt range. Please try another range."
            )
            st.stop()
        normalized_student = selected_students[0]

    module_options = _load_module_options(fact_path, learning_catalog_path, normalized_student)
    if module_options.height == 0:
        st.info("The selected student has no catalog-backed module attempts to display.")
        st.stop()

    module_rows = module_options.to_dicts()
    option_map = {_format_module_option(row): str(row.get("module_code")) for row in module_rows}
    selected_module_option = st.selectbox("Module", list(option_map.keys()), index=0)
    selected_module_code = option_map[selected_module_option]
    selected_module_row = next(
        (row for row in module_rows if str(row.get("module_code")) == selected_module_code),
        module_rows[0],
    )
    selected_module_label = (
        str(selected_module_row.get("module_label") or selected_module_code).strip() or selected_module_code
    )

    summary = _load_objective_summary(
        fact_path,
        learning_catalog_path,
        normalized_student,
        selected_module_code,
    )
    if summary.height == 0:
        st.info("No objective rows are available for this student/module selection.")
        st.stop()

    selected_profile_rows = eligible_profiles.filter(pl.col("user_id") == normalized_student).to_dicts()
    selected_profile = selected_profile_rows[0] if selected_profile_rows else {}
    module_summary = summarize_student_module_profile(summary)

    metric_cols = st.columns(6)
    metric_cols[0].metric("Student", normalized_student)
    metric_cols[1].metric("Total attempts", f"{int(selected_profile.get('total_attempts') or 0):,}")
    metric_cols[2].metric("Module", selected_module_code)
    metric_cols[3].metric(
        "Objectives touched",
        f"{int(module_summary['objectives_attempted'])}/{int(module_summary['objectives_total'])}",
    )
    metric_cols[4].metric(
        "Exercises covered",
        f"{int(module_summary['module_distinct_exercises_attempted'])}/{int(module_summary['module_exercise_total'])}",
        delta=f"{module_summary['module_coverage_rate'] * 100:.1f}%",
    )
    metric_cols[5].metric(
        "Mean success",
        f"{module_summary['mean_success_rate'] * 100:.1f}%",
    )

    st.caption(
        "All catalog objectives for the selected module stay visible. Coverage uses distinct exercises only, while success rate uses all attempts inside each objective."
    )

    figure = build_student_objective_spider_figure(
        summary,
        student_id=normalized_student,
        module_code=selected_module_code,
        module_label=selected_module_label,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        config=build_plotly_chart_config(),
    )

    render_figure_analysis(
        analyze_student_objective_spider(
            summary,
            student_id=normalized_student,
            module_code=selected_module_code,
            module_label=selected_module_label,
            total_attempts=int(selected_profile.get("total_attempts") or 0),
        )
    )


if __name__ == "__main__":
    main()

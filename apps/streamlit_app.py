"""Main Streamlit overview page for top-level learning analytics summaries."""

from __future__ import annotations

import sys
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

from figure_info import render_figure_info
from overview_shared import (
    build_fact_query,
    collect_core_compatibility,
    collect_lazy,
    ensure_label_columns,
    format_missing_table_columns,
    load_fact_dimensions,
    parquet_columns,
    render_curriculum_filters,
    render_dashboard_style,
)
from runtime_bootstrap import bootstrap_runtime_assets

from visu2.config import get_settings

st.set_page_config(
    page_title="Learning Analytics Overview",
    page_icon=":bar_chart:",
    layout="wide",
)

render_dashboard_style()

OVERVIEW_RUNTIME_TABLES: tuple[str, ...] = ("fact_attempt_core",)


def main() -> None:
    """Render the simplified overview page."""
    bootstrap_runtime_assets()
    settings = get_settings()
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"

    required = [fact_path]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing derived artifacts. Run `python scripts/build_derived.py` first.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    compatibility = collect_core_compatibility(
        table_columns={"fact_attempt_core": parquet_columns(fact_path)},
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

    dimension_frame_raw = load_fact_dimensions(fact_path)
    dimension_frame, _ = ensure_label_columns(
        dimension_frame_raw,
        {
            "module_label": "module_code",
            "objective_label": "objective_id",
            "activity_label": "activity_id",
        },
    )
    filters = render_curriculum_filters(dimension_frame)

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
        return

    available_work_modes = work_mode_summary["work_mode"].to_list()
    selected_work_modes = st.multiselect(
        "Work modes shown",
        options=available_work_modes,
        default=available_work_modes,
    )
    if not selected_work_modes:
        st.info("Select at least one work mode to render the summary table.")
        return

    selected_work_mode_summary = work_mode_summary.filter(pl.col("work_mode").is_in(selected_work_modes))
    if selected_work_mode_summary.height == 0:
        st.info("No rows available for the selected work modes.")
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


if __name__ == "__main__":
    main()

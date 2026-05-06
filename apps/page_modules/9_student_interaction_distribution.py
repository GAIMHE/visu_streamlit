"""Streamlit page for student interaction-count distributions."""

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

from overview_shared import render_dashboard_style
from plotly_config import build_plotly_chart_config
from source_state import get_active_source_id

from visu2.config import get_settings
from visu2.student_interaction_distribution import (
    build_student_interaction_counts,
    build_student_interaction_histogram,
    filter_student_interaction_counts,
    summarize_student_interaction_counts,
)


@st.cache_data(show_spinner=False)
def _load_student_counts(fact_path: Path) -> pl.DataFrame:
    """Load student-level interaction counts from the runtime fact table."""
    return build_student_interaction_counts(pl.scan_parquet(fact_path))


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _format_optional_number(value: object, *, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):,.{digits}f}"


def main() -> None:
    """Render the student interaction-count distribution page."""
    render_dashboard_style()
    settings = get_settings(get_active_source_id())
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"

    if not fact_path.exists():
        st.error("Missing runtime asset for the student interaction distribution page.")
        st.code(str(fact_path))
        st.stop()

    required_columns = {"user_id"}
    actual_columns = set(_parquet_columns(fact_path))
    missing_columns = sorted(required_columns - actual_columns)
    if missing_columns:
        st.error(
            "Student Interaction Distribution cannot run: fact_attempt_core is missing required columns."
        )
        st.markdown("- " + "\n- ".join(f"`{name}`" for name in missing_columns))
        st.stop()

    st.title("Student Interaction Distribution")
    st.caption(
        "This page shows how many exercise attempts each student contributes to the released interaction table. Use the cutoff to remove high-volume outliers and let the histogram rescale to the retained population."
    )

    counts = _load_student_counts(fact_path)
    if counts.height == 0:
        st.info("No student interaction counts are available.")
        st.stop()

    min_attempts = int(counts["interactions"].min() or 1)
    max_attempts = int(counts["interactions"].max() or 1)
    default_cutoff = min(5_000, max_attempts)

    st.sidebar.header("Distribution controls")
    cutoff = int(
        st.sidebar.slider(
            "Maximum interactions per student",
            min_value=max(1, min_attempts),
            max_value=max(1, max_attempts),
            value=max(1, default_cutoff),
            step=50 if max_attempts > 500 else 10,
            help="Students above this threshold are excluded from the histogram and summary metrics.",
        )
    )
    bin_count = int(
        st.sidebar.slider(
            "Histogram bins",
            min_value=20,
            max_value=120,
            value=60,
            step=5,
            help="Controls histogram granularity without changing the underlying student counts.",
        )
    )

    summary = summarize_student_interaction_counts(counts, max_interactions=cutoff)
    retained = filter_student_interaction_counts(counts, max_interactions=cutoff)
    if retained.height == 0:
        st.warning("The current cutoff excludes every student. Increase the threshold.")
        st.stop()

    metric_cols = st.columns(6)
    metric_cols[0].metric("Retained students", f"{int(summary['retained_students']):,}")
    metric_cols[1].metric(
        "Excluded outliers",
        f"{int(summary['excluded_students']):,}",
        delta=f"{float(summary['excluded_share']) * 100:.2f}%",
        delta_color="inverse",
    )
    metric_cols[2].metric("Mean", _format_optional_number(summary["mean"]))
    metric_cols[3].metric("Median", _format_optional_number(summary["median"]))
    metric_cols[4].metric("Q25-Q75", f"{summary['q25']:.0f}-{summary['q75']:.0f}")
    metric_cols[5].metric("Retained max", f"{int(summary['max'] or 0):,}")

    figure = build_student_interaction_histogram(
        counts,
        max_interactions=cutoff,
        bin_count=bin_count,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        config=build_plotly_chart_config(),
    )

    st.caption(
        f"Full population before cutoff: **{counts.height:,}** students. "
        f"Observed interaction range: **{min_attempts:,}** to **{max_attempts:,}** attempts per student."
    )


if __name__ == "__main__":
    main()

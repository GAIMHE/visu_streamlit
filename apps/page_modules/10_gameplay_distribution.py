"""Streamlit page for gameplay-type distribution summaries."""

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
from visu2.gameplay_distribution import (
    REQUIRED_COLUMNS,
    build_gameplay_distribution,
    build_gameplay_distribution_figure,
    top_gameplays,
)


@st.cache_data(show_spinner=False)
def _load_gameplay_summary(path: Path, *, include_unknown: bool) -> pl.DataFrame:
    """Load the runtime exercise aggregate and summarize by gameplay type."""
    return build_gameplay_distribution(pl.scan_parquet(path), include_unknown=include_unknown)


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _format_share(value: float) -> str:
    return f"{float(value) * 100:.2f}%"


def main() -> None:
    """Render the gameplay distribution page."""
    render_dashboard_style()
    settings = get_settings(get_active_source_id())
    exercise_daily_path = settings.artifacts_derived_dir / "agg_exercise_daily.parquet"

    if not exercise_daily_path.exists():
        st.error("Missing runtime asset for the gameplay distribution page.")
        st.code(str(exercise_daily_path))
        st.stop()

    actual_columns = set(_parquet_columns(exercise_daily_path))
    missing_columns = sorted(REQUIRED_COLUMNS - actual_columns)
    if missing_columns:
        st.error("Gameplay Distribution cannot run: agg_exercise_daily is missing columns.")
        st.markdown("- " + "\n- ".join(f"`{name}`" for name in missing_columns))
        st.stop()

    st.title("Gameplay Distribution")
    st.caption(
        "This page compares how gameplay types are represented in the released exercise set and how often students encounter them in the interaction table."
    )

    st.sidebar.header("Distribution controls")
    remove_unknown = st.sidebar.checkbox(
        "Remove unknown gameplay type",
        value=False,
        help="Exclude rows with missing or blank exercise_type values from the chart and table.",
    )

    summary = _load_gameplay_summary(exercise_daily_path, include_unknown=not remove_unknown)
    if summary.height == 0:
        st.info("No gameplay distribution rows are available for the selected settings.")
        st.stop()

    max_top_n = min(25, summary.height)
    if max_top_n < 3:
        top_n = max_top_n
    else:
        top_n = int(
            st.sidebar.slider(
                "Top gameplay types",
                min_value=3,
                max_value=max_top_n,
                value=min(10, max_top_n),
                step=1,
                help="Ranks gameplay types separately by unique exercise count and attempt count.",
            )
        )
    total_attempts = int(summary["attempts"].sum() or 0)
    total_exercises = int(summary["unique_exercises"].sum() or 0)
    top_exercises = top_gameplays(summary, metric="unique_exercises", top_n=1).row(0, named=True)
    top_attempts = top_gameplays(summary, metric="attempts", top_n=1).row(0, named=True)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Gameplay types", f"{summary.height:,}")
    metric_cols[1].metric("Unique exercises", f"{total_exercises:,}")
    metric_cols[2].metric("Attempts", f"{total_attempts:,}")
    metric_cols[3].metric(
        "Top attempt share",
        _format_share(top_attempts["attempt_share"]),
    )

    figure = build_gameplay_distribution_figure(summary, top_n=top_n)
    st.plotly_chart(
        figure,
        width="stretch",
        config=build_plotly_chart_config(),
    )

    st.caption(
        f"Most represented gameplay by exercise count: **{top_exercises['gameplay_type']}** "
        f"({_format_share(top_exercises['exercise_share'])} of unique exercises). "
        f"Most encountered gameplay by attempts: **{top_attempts['gameplay_type']}** "
        f"({_format_share(top_attempts['attempt_share'])} of attempts)."
    )

    with st.expander("Gameplay summary table"):
        display = summary.select(
            [
                "gameplay_type",
                "unique_exercises",
                "attempts",
            ]
        ).with_columns(
            (pl.col("unique_exercises") / pl.lit(total_exercises) * 100).alias(
                "exercise_share_percent"
            ),
            (pl.col("attempts") / pl.lit(total_attempts) * 100).alias(
                "attempt_share_percent"
            ),
        ).sort(["attempts", "gameplay_type"], descending=[True, False])
        st.dataframe(
            display.to_pandas(),
            width="stretch",
            hide_index=True,
            column_config={
                "gameplay_type": "Gameplay type",
                "unique_exercises": st.column_config.NumberColumn(
                    "Unique exercises",
                    format="%d",
                ),
                "exercise_share_percent": st.column_config.NumberColumn(
                    "Exercise share (%)",
                    format="%.2f",
                ),
                "attempts": st.column_config.NumberColumn("Attempts", format="%d"),
                "attempt_share_percent": st.column_config.NumberColumn(
                    "Attempt share (%)",
                    format="%.2f",
                ),
            },
        )


if __name__ == "__main__":
    main()

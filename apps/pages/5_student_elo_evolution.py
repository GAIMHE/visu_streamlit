"""
5_student_elo_evolution.py

Render student Elo trajectory comparison, replay controls, and chart output.

Dependencies
------------
- pathlib
- polars
- pyarrow
- runtime_bootstrap
- streamlit
- sys
- time
- visu2
"""
from __future__ import annotations

import sys
import time
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
from runtime_bootstrap import bootstrap_runtime_assets
from runtime_paths import STUDENT_ELO_RUNTIME_RELATIVE_PATHS

from visu2.config import get_settings
from visu2.contracts import RUNTIME_CORE_COLUMNS
from visu2.figure_analysis import analyze_student_elo_comparison
from visu2.student_elo import (
    build_student_elo_comparison_figure,
    build_student_elo_comparison_payload,
    load_student_elo_events,
    load_student_elo_label_lookup,
    load_student_elo_profiles,
    select_student_by_id,
    select_students_near_attempt_target,
)

st.set_page_config(
    page_title="Student Elo Evolution",
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
def _load_profiles(path: Path) -> pl.DataFrame:
    return load_student_elo_profiles(path)


@st.cache_data(show_spinner=False)
def _load_label_lookup(path: Path, exercise_elo_path: Path) -> pl.DataFrame:
    return load_student_elo_label_lookup(path, exercise_elo_path)


@st.cache_data(show_spinner=False)
def _load_comparison_profiles(
    current_profiles_path: Path,
    iterative_profiles_path: Path,
) -> pl.DataFrame:
    current = _load_profiles(current_profiles_path).rename(
        {
            "total_attempts": "current_total_attempts",
            "first_attempt_at": "current_first_attempt_at",
            "last_attempt_at": "current_last_attempt_at",
            "unique_modules": "current_unique_modules",
            "unique_objectives": "current_unique_objectives",
            "unique_activities": "current_unique_activities",
            "final_student_elo": "current_final_student_elo",
            "eligible_for_replay": "current_eligible_for_replay",
        }
    )
    iterative = _load_profiles(iterative_profiles_path).rename(
        {
            "total_attempts": "iterative_total_attempts",
            "first_attempt_at": "iterative_first_attempt_at",
            "last_attempt_at": "iterative_last_attempt_at",
            "unique_modules": "iterative_unique_modules",
            "unique_objectives": "iterative_unique_objectives",
            "unique_activities": "iterative_unique_activities",
            "final_student_elo": "iterative_final_student_elo",
            "eligible_for_replay": "iterative_eligible_for_replay",
        }
    )
    return (
        current.join(iterative, on="user_id", how="inner")
        .with_columns(
            (
                pl.col("current_eligible_for_replay").fill_null(False)
                & pl.col("iterative_eligible_for_replay").fill_null(False)
            ).alias("eligible_for_replay"),
            pl.col("current_total_attempts").alias("total_attempts"),
            pl.col("current_first_attempt_at").alias("first_attempt_at"),
            pl.col("current_last_attempt_at").alias("last_attempt_at"),
            pl.col("current_unique_modules").alias("unique_modules"),
            pl.col("current_unique_objectives").alias("unique_objectives"),
            pl.col("current_unique_activities").alias("unique_activities"),
            pl.col("current_final_student_elo").alias("final_student_elo"),
        )
        .sort(["total_attempts", "user_id"], descending=[True, False])
    )


@st.cache_data(show_spinner=False)
def _load_comparison_payload(
    current_events_path: Path,
    iterative_events_path: Path,
    label_path: Path,
    exercise_elo_path: Path,
    user_ids: tuple[str, ...],
    step_size: int,
) -> dict[str, object]:
    return build_student_elo_comparison_payload(
        load_student_elo_events(current_events_path),
        load_student_elo_events(iterative_events_path),
        list(user_ids),
        step_size,
        label_lookup=_load_label_lookup(label_path, exercise_elo_path),
    )


@st.cache_data(show_spinner=False)
def _load_exercise_comparison(
    current_exercise_elo_path: Path,
    iterative_exercise_elo_path: Path,
) -> pl.DataFrame:
    current = (
        pl.read_parquet(current_exercise_elo_path)
        .select(
            [
                "exercise_id",
                "exercise_label",
                "module_code",
                "calibration_attempts",
                "exercise_elo",
                "calibrated",
            ]
        )
        .rename(
            {
                "exercise_elo": "current_exercise_elo",
                "calibrated": "current_calibrated",
            }
        )
    )
    iterative = (
        pl.read_parquet(iterative_exercise_elo_path)
        .select(["exercise_id", "exercise_elo", "calibrated"])
        .rename(
            {
                "exercise_elo": "iterative_exercise_elo",
                "calibrated": "iterative_calibrated",
            }
        )
    )
    return (
        current.join(iterative, on="exercise_id", how="inner")
        .with_columns(
            (
                pl.col("current_calibrated").fill_null(False)
                & pl.col("iterative_calibrated").fill_null(False)
            ).alias("calibrated"),
            (pl.col("iterative_exercise_elo") - pl.col("current_exercise_elo")).alias("elo_diff"),
            (pl.col("iterative_exercise_elo") - pl.col("current_exercise_elo"))
            .abs()
            .alias("abs_elo_diff"),
        )
        .sort(["abs_elo_diff", "calibration_attempts", "exercise_id"], descending=[True, True, False])
    )


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def main() -> None:
    bootstrap_runtime_assets(STUDENT_ELO_RUNTIME_RELATIVE_PATHS)
    settings = get_settings()
    current_profiles_path = settings.artifacts_derived_dir / "student_elo_profiles.parquet"
    current_events_path = settings.artifacts_derived_dir / "student_elo_events.parquet"
    current_exercise_elo_path = settings.artifacts_derived_dir / "agg_exercise_elo.parquet"
    iterative_profiles_path = settings.artifacts_derived_dir / "student_elo_profiles_iterative.parquet"
    iterative_events_path = settings.artifacts_derived_dir / "student_elo_events_iterative.parquet"
    iterative_exercise_elo_path = settings.artifacts_derived_dir / "agg_exercise_elo_iterative.parquet"

    required = [
        current_profiles_path,
        current_events_path,
        current_exercise_elo_path,
        iterative_profiles_path,
        iterative_events_path,
        iterative_exercise_elo_path,
    ]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing Elo comparison artifacts. Rebuild derived data.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    compatibility_checks = {
        "student_elo_profiles": (current_profiles_path, RUNTIME_CORE_COLUMNS["student_elo_profiles"]),
        "student_elo_events": (current_events_path, RUNTIME_CORE_COLUMNS["student_elo_events"]),
        "agg_exercise_elo": (current_exercise_elo_path, RUNTIME_CORE_COLUMNS["agg_exercise_elo"]),
        "student_elo_profiles_iterative": (
            iterative_profiles_path,
            RUNTIME_CORE_COLUMNS["student_elo_profiles_iterative"],
        ),
        "student_elo_events_iterative": (
            iterative_events_path,
            RUNTIME_CORE_COLUMNS["student_elo_events_iterative"],
        ),
        "agg_exercise_elo_iterative": (
            iterative_exercise_elo_path,
            RUNTIME_CORE_COLUMNS["agg_exercise_elo_iterative"],
        ),
    }
    missing_contracts: list[str] = []
    for label, (path, required_columns) in compatibility_checks.items():
        missing_columns = [col for col in required_columns if col not in _parquet_columns(path)]
        if missing_columns:
            missing_contracts.append(f"- `{label}`: {', '.join(missing_columns)}")
    if missing_contracts:
        st.error("Student Elo comparison artifacts are incompatible with the current runtime contract.")
        st.markdown("\n".join(missing_contracts))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    comparison_profiles = _load_comparison_profiles(current_profiles_path, iterative_profiles_path)
    if comparison_profiles.height == 0:
        st.info("No students are available in both Elo systems.")
        st.stop()

    eligible_profiles = comparison_profiles.filter(pl.col("eligible_for_replay"))
    if eligible_profiles.height == 0:
        st.info("No student Elo trajectories are jointly eligible for comparison.")
        st.stop()

    st.title("Student Elo Evolution")
    render_figure_info("student_elo_page")
    st.caption(
        "This page compares the current retrospective item-Elo system against an iterative offline calibration that refits fixed exercise difficulty without using the human graph structure."
    )

    min_attempt_count = int(eligible_profiles["total_attempts"].min() or 0)
    max_attempt_count = int(eligible_profiles["total_attempts"].max() or 0)
    median_attempt_count = int(eligible_profiles["total_attempts"].median() or min_attempt_count or 1)

    st.caption(
        f"Jointly eligible students range from **{min_attempt_count}** to **{max_attempt_count}** attempts."
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
        help="If you enter a replay-eligible student ID here, the page will use that student directly instead of the sampled attempt-range selection.",
    ).strip()

    if manual_student_id:
        selected_student = select_student_by_id(eligible_profiles, manual_student_id)
        if selected_student is None:
            st.info(
                "No jointly replay-eligible student matches that ID. Please check the ID or clear the field."
            )
            st.stop()
        normalized_students = [selected_student]
        st.caption("Using the typed student ID override.")
    else:
        selection_signature = ("student_elo_comparison_attempt_target", target_attempts)
        selection_state_key = "student_elo_comparison_selected_students"
        selection_signature_key = "student_elo_comparison_attempt_target_signature"
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

        normalized_students = [
            str(user_id)
            for user_id in st.session_state.get(selection_state_key, [])
            if str(user_id).strip()
        ][:1]
        if not normalized_students:
            lower = int(target_attempts * 0.9)
            upper = int(target_attempts * 1.1)
            st.info(
                f"No students found in the {lower}-{upper} attempt range. Please try another range."
            )
            st.stop()

    st.sidebar.header("Replay")
    step_size = int(
        st.sidebar.number_input(
            "Step size (attempts/frame)",
            min_value=1,
            max_value=500,
            value=10,
            step=1,
        )
    )
    speed_ms = int(
        st.sidebar.slider(
            "Autoplay speed (ms/frame)",
            min_value=100,
            max_value=1500,
            value=400,
            step=50,
        )
    )
    display_choice = st.sidebar.radio(
        "Displayed Elo system",
        options=("Current Elo", "Iterative Elo", "Both"),
        index=0,
        help="Choose whether to display only the current Elo system, only the iterative Elo system, or both together.",
    )
    gap_days_threshold = float(
        st.sidebar.number_input(
            "Highlight timestamp gaps >= days",
            min_value=0.0,
            max_value=365.0,
            value=7.0,
            step=0.5,
            help="Set to 0 to disable gap markers. When enabled, dotted vertical lines mark long inactivity gaps between consecutive attempts.",
        )
    )

    try:
        payload = _load_comparison_payload(
            current_events_path,
            iterative_events_path,
            settings.learning_catalog_path,
            current_exercise_elo_path,
            tuple(normalized_students),
            step_size,
        )
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    student_ids = payload.get("student_ids") or []
    if not student_ids:
        st.info("No Elo event rows are available for the selected students.")
        st.stop()

    frame_cutoffs = payload.get("frame_cutoffs") or [0]
    max_frame_idx = max(0, len(frame_cutoffs) - 1)
    state_signature = (
        tuple(student_ids),
        int(step_size),
        manual_student_id or int(target_attempts),
    )
    signature_key = "student_elo_comparison_signature"
    frame_key = "student_elo_comparison_frame_idx"
    playing_key = "student_elo_comparison_playing"
    if st.session_state.get(signature_key) != state_signature:
        st.session_state[signature_key] = state_signature
        st.session_state[frame_key] = 0
        st.session_state[playing_key] = False
    if frame_key not in st.session_state:
        st.session_state[frame_key] = 0
    if playing_key not in st.session_state:
        st.session_state[playing_key] = False

    selected_profiles = eligible_profiles.filter(pl.col("user_id").is_in(student_ids))
    summary_cols = st.columns(max(1, len(student_ids)))
    for idx, user_id in enumerate(student_ids):
        row = selected_profiles.filter(pl.col("user_id") == user_id).to_dicts()
        if not row:
            continue
        entry = row[0]
        with summary_cols[idx]:
            st.metric("Student", user_id)
            st.caption(
                f"Attempts: {entry.get('current_total_attempts')} | "
                f"Current Elo: {float(entry.get('current_final_student_elo') or 0.0):.1f} | "
                f"Iterative Elo: {float(entry.get('iterative_final_student_elo') or 0.0):.1f}"
            )
            st.caption(
                f"{entry.get('current_first_attempt_at')} -> {entry.get('current_last_attempt_at')}"
            )

    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        if st.button("Play" if not st.session_state[playing_key] else "Pause", width="stretch"):
            st.session_state[playing_key] = not st.session_state[playing_key]
    with c2:
        if st.button("Reset", width="stretch"):
            st.session_state[playing_key] = False
            st.session_state[frame_key] = 0
    with c3:
        if st.button("Step +1", width="stretch"):
            st.session_state[playing_key] = False
            st.session_state[frame_key] = min(max_frame_idx, int(st.session_state[frame_key]) + 1)
    with c4:
        st.caption(
            f"Student: **{student_ids[0]}** | "
            f"Step size: **{step_size}** | "
            f"Max local attempt: **{payload.get('max_attempts') or 0}**"
        )

    slider_value = st.slider(
        "Replay frame",
        min_value=0,
        max_value=max_frame_idx,
        value=min(max_frame_idx, int(st.session_state[frame_key])),
    )
    st.session_state[frame_key] = int(slider_value)
    frame_idx = int(st.session_state[frame_key])
    cutoff = int(frame_cutoffs[frame_idx]) if frame_idx < len(frame_cutoffs) else 0
    st.caption(f"Frame {frame_idx}/{max_frame_idx} | Local attempt cutoff: {cutoff}")
    visible_systems = (
        ("Current Elo", "Iterative Elo")
        if display_choice == "Both"
        else (display_choice,)
    )

    figure = build_student_elo_comparison_figure(
        payload,
        frame_idx,
        gap_days_threshold=gap_days_threshold,
        visible_systems=visible_systems,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        config=build_plotly_chart_config(),
    )
    if gap_days_threshold > 0:
        st.caption(
            "Dotted vertical markers show gaps between consecutive attempts that exceed the selected day threshold."
        )

    exercise_comparison = _load_exercise_comparison(
        current_exercise_elo_path,
        iterative_exercise_elo_path,
    )
    render_figure_analysis(
        analyze_student_elo_comparison(
            payload,
            exercise_comparison,
            eligible_profiles,
        )
    )

    if st.session_state[playing_key]:
        if frame_idx >= max_frame_idx:
            st.session_state[playing_key] = False
        else:
            time.sleep(max(0.1, float(speed_ms) / 1000.0))
            st.session_state[frame_key] = min(max_frame_idx, frame_idx + 1)
            st.rerun()


if __name__ == "__main__":
    main()

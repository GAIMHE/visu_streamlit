from __future__ import annotations

import sys
import time
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

from runtime_bootstrap import bootstrap_runtime_assets
from visu2.config import get_settings
from visu2.contracts import RUNTIME_CORE_COLUMNS
from visu2.student_elo import (
    build_student_elo_figure,
    build_student_elo_payload,
    load_student_elo_events,
    load_student_elo_profiles,
    select_default_students,
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
def _load_profiles(path: Path):
    return load_student_elo_profiles(path)


@st.cache_data(show_spinner=False)
def _load_payload(path: Path, user_ids: tuple[str, ...], step_size: int):
    return build_student_elo_payload(load_student_elo_events(path), list(user_ids), step_size)


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def main() -> None:
    bootstrap_runtime_assets()
    settings = get_settings()
    profiles_path = settings.artifacts_derived_dir / "student_elo_profiles.parquet"
    events_path = settings.artifacts_derived_dir / "student_elo_events.parquet"

    required = [profiles_path, events_path]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing Elo artifacts. Rebuild derived data.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    profiles_columns = _parquet_columns(profiles_path)
    events_columns = _parquet_columns(events_path)
    missing_profiles = [
        col for col in RUNTIME_CORE_COLUMNS["student_elo_profiles"] if col not in profiles_columns
    ]
    missing_events = [
        col for col in RUNTIME_CORE_COLUMNS["student_elo_events"] if col not in events_columns
    ]
    if missing_profiles or missing_events:
        st.error("Student Elo artifacts are incompatible with the current runtime contract.")
        if missing_profiles:
            st.markdown(
                "- `student_elo_profiles`: " + ", ".join(missing_profiles)
            )
        if missing_events:
            st.markdown(
                "- `student_elo_events`: " + ", ".join(missing_events)
            )
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    profiles = _load_profiles(profiles_path)
    if profiles.height == 0:
        st.info("No student Elo trajectories are available.")
        st.stop()

    st.title("Student Elo Evolution")
    st.caption(
        "Replay one or two student trajectories against fixed exercise difficulty. "
        "Exercise Elo is frozen; only the student rating changes over time."
    )

    st.sidebar.header("Selection")
    min_attempts = int(
        st.sidebar.number_input(
            "Minimum attempts",
            min_value=1,
            max_value=5000,
            value=100,
            step=10,
        )
    )

    eligible_profiles = profiles.filter(pl.col("total_attempts") >= min_attempts)
    if eligible_profiles.height == 0:
        st.info("No students match the current minimum-attempt threshold.")
        st.stop()

    default_students = select_default_students(profiles, min_attempts=min_attempts, max_students=2)
    options = eligible_profiles["user_id"].to_list()
    selected_students = st.sidebar.multiselect(
        "Students to display",
        options=options,
        default=default_students,
    )
    normalized_students = [str(user_id) for user_id in selected_students if str(user_id).strip()]
    if len(normalized_students) > 2:
        normalized_students = normalized_students[:2]
        st.warning("Only the first two selected students are displayed.")
    if not normalized_students:
        normalized_students = default_students[:2]
    if not normalized_students:
        st.info("Select at least one student to render a trajectory.")
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

    payload = _load_payload(events_path, tuple(normalized_students), step_size)
    student_ids = payload.get("student_ids") or []
    if not student_ids:
        st.info("No Elo event rows are available for the selected students.")
        st.stop()

    frame_cutoffs = payload.get("frame_cutoffs") or [0]
    max_frame_idx = max(0, len(frame_cutoffs) - 1)
    state_signature = (tuple(student_ids), int(step_size), int(min_attempts))
    signature_key = "student_elo_signature"
    frame_key = "student_elo_frame_idx"
    playing_key = "student_elo_playing"
    if st.session_state.get(signature_key) != state_signature:
        st.session_state[signature_key] = state_signature
        st.session_state[frame_key] = 0
        st.session_state[playing_key] = False
    if frame_key not in st.session_state:
        st.session_state[frame_key] = 0
    if playing_key not in st.session_state:
        st.session_state[playing_key] = False

    selected_profiles = profiles.filter(pl.col("user_id").is_in(student_ids))
    summary_cols = st.columns(max(1, len(student_ids)))
    for idx, user_id in enumerate(student_ids):
        row = selected_profiles.filter(pl.col("user_id") == user_id).to_dicts()
        if not row:
            continue
        entry = row[0]
        with summary_cols[idx]:
            st.metric(f"Student {idx + 1}", user_id)
            st.caption(
                f"Attempts: {entry.get('total_attempts')} | "
                f"Final Elo: {float(entry.get('final_student_elo') or 0.0):.1f}"
            )
            st.caption(
                f"{entry.get('first_attempt_at')} -> {entry.get('last_attempt_at')}"
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
            f"Students: **{len(student_ids)}** | "
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

    figure = build_student_elo_figure(payload, frame_idx)
    st.plotly_chart(figure, width="stretch")

    if st.session_state[playing_key]:
        if frame_idx >= max_frame_idx:
            st.session_state[playing_key] = False
        else:
            time.sleep(max(0.1, float(speed_ms) / 1000.0))
            st.session_state[frame_key] = min(max_frame_idx, frame_idx + 1)
            st.rerun()


if __name__ == "__main__":
    main()

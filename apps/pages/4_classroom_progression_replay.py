from __future__ import annotations

import sys
import time
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

from visu2.classroom_progression import (
    VALID_MODE_SCOPES,
    build_classroom_mode_profiles,
    build_heatmap_figure,
    build_replay_payload,
    select_default_classroom,
)
from visu2.config import get_settings
from runtime_bootstrap import bootstrap_runtime_assets


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


MODE_OPTIONS = {
    "ZPDES": "zpdes",
    "Playlist": "playlist",
    "All modes": "all",
}


@st.cache_data(show_spinner=False)
def _load_profiles(fact_path: Path) -> pl.DataFrame:
    return build_classroom_mode_profiles(pl.scan_parquet(fact_path))


@st.cache_data(show_spinner=False)
def _load_replay_payload(
    fact_path: Path,
    classroom_id: str,
    mode_scope: str,
    start_date: date,
    end_date: date,
    max_frames: int,
    step_size: int,
) -> dict:
    return build_replay_payload(
        fact=pl.scan_parquet(fact_path),
        classroom_id=classroom_id,
        mode_scope=mode_scope,
        start_date=start_date,
        end_date=end_date,
        max_frames=max_frames,
        step_size=step_size,
    )


def _format_classroom_option(row: dict[str, object]) -> str:
    return (
        f"{row.get('classroom_id')}  "
        f"({row.get('students')} students, {row.get('activities')} activities, {row.get('attempts')} attempts)"
    )


def _mode_label(mode_scope: str) -> str:
    for label, value in MODE_OPTIONS.items():
        if value == mode_scope:
            return label
    return mode_scope


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _default_date_range(row: dict[str, object]) -> tuple[date, date]:
    first_ts = row.get("first_attempt_at")
    last_ts = row.get("last_attempt_at")
    if hasattr(first_ts, "date") and hasattr(last_ts, "date"):
        return first_ts.date(), last_ts.date()
    today = date.today()
    return today, today


def main() -> None:
    bootstrap_runtime_assets()
    settings = get_settings()
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    if not fact_path.exists():
        st.error("Missing artifact: fact_attempt_core.parquet.")
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    required_columns = {
        "created_at",
        "date_utc",
        "user_id",
        "activity_id",
        "activity_label",
        "data_correct",
        "work_mode",
        "classroom_id",
        "objective_id",
        "module_code",
        "exercise_id",
        "attempt_number",
    }
    actual_columns = set(_parquet_columns(fact_path))
    missing = sorted(required_columns - actual_columns)
    if missing:
        st.error("Replay page cannot run: fact_attempt_core is missing required columns.")
        st.markdown("- " + "\n- ".join(f"`{name}`" for name in missing))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    profiles = _load_profiles(fact_path)
    if profiles.height == 0:
        st.info("No valid classroom rows found (excluding null and 'None').")
        st.stop()

    st.title("Classroom Progression Replay")
    st.caption(
        "Replay cumulative student success by activity with synchronized steps. "
        "At each step, every student advances by the same number of local attempts while preserving their own chronology."
    )

    st.sidebar.header("Scope")
    mode_label = st.sidebar.selectbox("Work mode scope", list(MODE_OPTIONS.keys()), index=0)
    mode_scope = MODE_OPTIONS[mode_label]
    if mode_scope not in VALID_MODE_SCOPES:
        st.error(f"Unsupported mode scope: {mode_scope}")
        st.stop()

    scoped_profiles = (
        profiles.filter(pl.col("mode_scope") == mode_scope)
        .sort(["students", "attempts", "classroom_id"], descending=[True, True, False])
    )
    if scoped_profiles.height == 0:
        st.info(f"No classrooms available for scope '{_mode_label(mode_scope)}'.")
        st.stop()

    default_classroom_id = select_default_classroom(profiles, mode_scope)
    rows = scoped_profiles.to_dicts()
    option_map = {_format_classroom_option(row): str(row.get("classroom_id")) for row in rows}
    option_keys = list(option_map.keys())

    default_index = 0
    if default_classroom_id is not None:
        for idx, row in enumerate(rows):
            if str(row.get("classroom_id")) == default_classroom_id:
                default_index = idx
                break

    selected_option = st.sidebar.selectbox("Classroom", option_keys, index=default_index)
    selected_classroom_id = option_map[selected_option]
    selected_row = next(
        (row for row in rows if str(row.get("classroom_id")) == selected_classroom_id),
        rows[0],
    )

    min_date, max_date = _default_date_range(selected_row)
    start_date, end_date = st.sidebar.date_input(
        "Date range (UTC)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("Please select a valid date range.")
        st.stop()
    if start_date > end_date:
        st.error("Start date must be <= end date.")
        st.stop()

    st.sidebar.header("Replay")
    speed_ms = st.sidebar.slider("Autoplay speed (ms/frame)", min_value=100, max_value=1500, value=450, step=50)
    step_size = st.sidebar.number_input(
        "Step size (attempts per student/frame)",
        min_value=1,
        max_value=250,
        value=1,
        step=1,
    )
    max_frames = st.sidebar.number_input("Max frames", min_value=50, max_value=5000, value=2000, step=50)

    st.sidebar.header("Color")
    threshold = st.sidebar.slider("Mastery threshold", min_value=0.50, max_value=0.95, value=0.75, step=0.01)
    show_values = st.sidebar.checkbox("Show cell values", value=False)

    payload = _load_replay_payload(
        fact_path=fact_path,
        classroom_id=selected_classroom_id,
        mode_scope=mode_scope,
        start_date=start_date,
        end_date=end_date,
        max_frames=int(max_frames),
        step_size=int(step_size),
    )

    total_events = int(payload.get("total_events_valid_timestamp") or 0)
    if total_events == 0:
        st.info("No attempts found for this classroom/mode/date selection.")
        st.stop()

    if len(payload.get("activity_ids") or []) == 1:
        st.info("This classroom currently contains only one activity in the selected scope.")

    dropped = int(payload.get("dropped_invalid_timestamps") or 0)
    if dropped > 0:
        st.warning(f"{dropped} row(s) were dropped because `created_at` was missing.")

    if bool(payload.get("events_capped")):
        st.info(
            "Replay frame cap is active. Effective step was increased "
            f"from {payload.get('requested_step_size')} to {payload.get('effective_step')} attempts/student/frame."
        )

    frame_step_counts = payload.get("frame_step_counts") or [0]
    frame_event_counts = payload.get("frame_event_counts") or [0]
    num_frames = len(frame_step_counts)
    max_frame_idx = max(0, num_frames - 1)

    state_signature = (
        mode_scope,
        selected_classroom_id,
        str(start_date),
        str(end_date),
        int(step_size),
        int(max_frames),
    )
    signature_key = "classroom_replay_signature"
    frame_key = "classroom_replay_frame_idx"
    playing_key = "classroom_replay_playing"
    if st.session_state.get(signature_key) != state_signature:
        st.session_state[signature_key] = state_signature
        st.session_state[frame_key] = 0
        st.session_state[playing_key] = False
    if frame_key not in st.session_state:
        st.session_state[frame_key] = 0
    if playing_key not in st.session_state:
        st.session_state[playing_key] = False

    col_a, col_b, col_c, col_d = st.columns([1, 1, 1, 2])
    with col_a:
        if st.button("Play" if not st.session_state[playing_key] else "Pause", width='stretch'):
            st.session_state[playing_key] = not st.session_state[playing_key]
    with col_b:
        if st.button("Reset", width='stretch'):
            st.session_state[playing_key] = False
            st.session_state[frame_key] = 0
    with col_c:
        if st.button("Step +1", width='stretch'):
            st.session_state[playing_key] = False
            st.session_state[frame_key] = min(max_frame_idx, int(st.session_state[frame_key]) + 1)
    with col_d:
        st.caption(
            f"Scope: **{_mode_label(mode_scope)}**  |  Classroom: **{selected_classroom_id}**  "
            f"|  Students: **{len(payload.get('student_ids') or [])}**  "
            f"|  Activities: **{len(payload.get('activity_ids') or [])}**"
        )

    slider_value = st.slider(
        "Replay frame",
        min_value=0,
        max_value=max_frame_idx,
        value=min(max_frame_idx, int(st.session_state[frame_key])),
    )
    st.session_state[frame_key] = int(slider_value)
    frame_idx = int(st.session_state[frame_key])

    frame_ts = (payload.get("frame_timestamps") or [None])[frame_idx]
    frame_events = frame_event_counts[frame_idx] if frame_idx < len(frame_event_counts) else 0
    frame_step = frame_step_counts[frame_idx] if frame_idx < len(frame_step_counts) else 0
    total_sync_steps = int(payload.get("total_sync_steps") or 0)
    st.caption(
        f"Frame {frame_idx}/{max_frame_idx} | Synchronized step: {frame_step}/{total_sync_steps} "
        f"| Integrated attempts: {frame_events}/{total_events}"
        + (f" | Last event at: {frame_ts}" if frame_ts else " | Start state")
    )

    figure = build_heatmap_figure(
        payload=payload,
        frame_idx=frame_idx,
        threshold=float(threshold),
        show_values=bool(show_values),
    )
    st.plotly_chart(figure, width='stretch')

    if st.session_state[playing_key]:
        if frame_idx >= max_frame_idx:
            st.session_state[playing_key] = False
        else:
            time.sleep(max(0.1, float(speed_ms) / 1000.0))
            st.session_state[frame_key] = min(max_frame_idx, frame_idx + 1)
            st.rerun()


if __name__ == "__main__":
    main()

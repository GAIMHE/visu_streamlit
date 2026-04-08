"""
4_classroom_progression_replay.py

Render classroom progression replay controls and animated matrix visualization.

Dependencies
------------
- datetime
- pathlib
- polars
- pyarrow
- runtime_bootstrap
- streamlit
- sys
- time
- visu2

Classes
-------
- None.

Functions
---------
- _load_profiles: Utility for load profiles.
- _load_replay_payload: Utility for load replay payload.
- _format_classroom_option: Utility for format classroom option.
- _mode_label: Utility for mode label.
- _parquet_columns: Utility for parquet columns.
- main: Utility for main.
"""
from __future__ import annotations

import math
import sys
import time
from datetime import date
from pathlib import Path

import polars as pl
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
from source_state import get_active_source_id, get_query_value, set_query_value

from visu2.classroom_picker_state import (
    initialize_classroom_picker_state,
    preferred_classroom_option_index,
    preferred_target_students,
)
from visu2.classroom_profile_loader import load_or_build_classroom_mode_profiles
from visu2.classroom_progression import (
    SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID,
    VALID_MODE_SCOPES,
    build_classroom_activity_summary_by_mode,
    build_classroom_mode_profiles,
    build_heatmap_figure,
    build_replay_payload,
    select_classroom_by_id,
    select_classrooms_near_student_target,
)
from visu2.config import get_settings
from visu2.figure_analysis import analyze_classroom_progression_population
from visu2.remote_query import query_fact_attempts_for_classroom
from visu2.runtime_sources import get_runtime_source

MODE_OPTIONS = {
    "ZPDES": "zpdes",
    "Playlist": "playlist",
    "All modes": "all",
}
CLASSROOM_ID_QUERY_KEY = "classroom_id"
MODE_SCOPE_QUERY_KEY = "mode_scope"
HEATMAP_MASTERY_THRESHOLD = 0.75


FACT_QUERY_COLUMNS: tuple[str, ...] = (
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
)


@st.cache_data(show_spinner=False)
def _load_profiles(source_id: str) -> tuple[pl.DataFrame, str]:
    settings = get_settings(source_id)
    return load_or_build_classroom_mode_profiles(settings)


@st.cache_data(show_spinner=False)
def _load_replay_payload(
    source_id: str,
    classroom_id: str,
    mode_scope: str,
    start_date_iso: str,
    end_date_iso: str,
    min_student_attempts: int,
    max_frames: int,
    step_size: int,
) -> dict:
    """Load replay payload.

Parameters
----------
fact_path : Path
        Input parameter used by this routine.
classroom_id : str
        Input parameter used by this routine.
mode_scope : str
        Input parameter used by this routine.
start_date_iso : str
        Input parameter used by this routine.
end_date_iso : str
        Input parameter used by this routine.
max_frames : int
        Input parameter used by this routine.
step_size : int
        Input parameter used by this routine.

Returns
-------
dict
        Result produced by this routine.

"""
    settings = get_settings(source_id)
    fact_slice = query_fact_attempts_for_classroom(
        settings,
        classroom_id=classroom_id,
        mode_scope=mode_scope,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        min_student_attempts=min_student_attempts,
        columns=FACT_QUERY_COLUMNS,
    )
    return build_replay_payload(
        fact=fact_slice,
        classroom_id=classroom_id,
        mode_scope=mode_scope,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        max_frames=max_frames,
        step_size=step_size,
    )


@st.cache_data(show_spinner=False)
def _load_selected_population_summary(
    source_id: str,
    classroom_id: str,
    mode_scope: str,
    start_date_iso: str,
    end_date_iso: str,
    min_student_attempts: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build a summary from the exact filtered classroom slice."""
    settings = get_settings(source_id)
    fact_slice = query_fact_attempts_for_classroom(
        settings,
        classroom_id=classroom_id,
        mode_scope=mode_scope,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        min_student_attempts=min_student_attempts,
        columns=FACT_QUERY_COLUMNS,
    )
    if fact_slice.height == 0:
        return pl.DataFrame(), pl.DataFrame()
    profiles = build_classroom_mode_profiles(fact_slice)
    scoped_profiles = profiles.filter(pl.col("mode_scope") == mode_scope)
    activity_summary = build_classroom_activity_summary_by_mode(fact_slice)
    scoped_activity_summary = activity_summary.filter(pl.col("mode_scope") == mode_scope)
    return scoped_profiles, scoped_activity_summary


def _format_classroom_option(row: dict[str, object]) -> str:
    """Format classroom option.

Parameters
----------
row : dict[str, object]
        Input parameter used by this routine.

Returns
-------
str
        Result produced by this routine.

"""
    classroom_id = str(row.get("classroom_id") or "")
    classroom_label = (
        "All students"
        if classroom_id == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
        else classroom_id
    )
    return (
        f"{classroom_label}  "
        f"({row.get('students')} students, {row.get('activities')} activities, {row.get('attempts')} attempts)"
    )


def _mode_label(mode_scope: str) -> str:
    """Mode label.

Parameters
----------
mode_scope : str
        Input parameter used by this routine.

Returns
-------
str
        Result produced by this routine.

"""
    for label, value in MODE_OPTIONS.items():
        if value == mode_scope:
            return label
    return mode_scope


def _initial_mode_label(query_mode_scope: str | None) -> str:
    for label, value in MODE_OPTIONS.items():
        if value == query_mode_scope:
            return label
    return "ZPDES"


def _clear_picker_keys(*keys: str) -> None:
    for key in keys:
        st.session_state.pop(key, None)


def main() -> None:
    """Main.


Returns
-------
None
        Result produced by this routine.

"""
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
    try:
        profiles, profiles_source = _load_profiles(settings.source_id)
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()
    if profiles_source == "fact_fallback":
        st.info(
            "Selector profiles were rebuilt from `fact_attempt_core` because "
            "`classroom_mode_profiles.parquet` was unavailable in the runtime source."
        )
    if profiles.height == 0:
        st.info("No valid classroom rows found (excluding null and 'None').")
        st.stop()

    st.title("Classroom Progression Replay")
    render_figure_info("classroom_progression_replay_heatmap")

    st.sidebar.header("Scope")
    query_mode_scope = get_query_value(MODE_SCOPE_QUERY_KEY)
    mode_options = list(MODE_OPTIONS.keys())
    mode_label = st.sidebar.selectbox(
        "Work mode scope",
        mode_options,
        index=mode_options.index(_initial_mode_label(query_mode_scope)),
    )
    mode_scope = MODE_OPTIONS[mode_label]
    set_query_value(MODE_SCOPE_QUERY_KEY, mode_scope)
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

    min_students = int(scoped_profiles["students"].min() or 0)
    max_students = int(scoped_profiles["students"].max() or 0)
    allow_all_data = "has_classroom_all_data_option" in get_runtime_source(
        settings.source_id
    ).capability_flags
    synthetic_only_scope = (
        scoped_profiles.height == 1
        and str(scoped_profiles["classroom_id"][0]) == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
    )
    if synthetic_only_scope:
        selected_classroom_id = SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
        st.caption(
            "This source does not have a classroom dimension. The replay is using one synthetic classroom that groups all students."
        )
    else:
        st.caption(
            f"Classrooms in this scope range from **{min_students}** to **{max_students}** students."
        )
        query_classroom_id = get_query_value(CLASSROOM_ID_QUERY_KEY)
        population_scope = "Specific classroom"
        if allow_all_data:
            population_scope = st.radio(
                "Population scope",
                ["Specific classroom", "All data"],
                horizontal=True,
            )
        if allow_all_data and population_scope == "All data":
            selected_classroom_id = SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
            st.caption(
                f"Using all available classroom data in **{_mode_label(mode_scope)}** scope "
                f"across **{scoped_profiles.height}** classrooms."
            )
        else:
            default_target = int(scoped_profiles["students"].median() or min_students or 1)
            picker_context_key = "classroom_replay_picker_context"
            target_students_key = "classroom_replay_target_students"
            manual_classroom_key = "classroom_replay_manual_classroom_id"
            matching_classrooms_key = "classroom_replay_matching_classroom"
            preferred_classroom_key = "classroom_replay_preferred_classroom_id"
            initialize_classroom_picker_state(
                st.session_state,
                context_key=picker_context_key,
                current_context=(settings.source_id, mode_scope),
                target_key=target_students_key,
                manual_key=manual_classroom_key,
                selectbox_key=matching_classrooms_key,
                preferred_key=preferred_classroom_key,
                default_target=preferred_target_students(
                    scoped_profiles,
                    query_classroom_id,
                    default_target,
                ),
                preferred_classroom_id=query_classroom_id,
                min_students=min_students,
                max_students=max_students,
            )
            target_students = int(
                st.number_input(
                    "Target classroom size (students)",
                    min_value=max(1, min_students),
                    max_value=max(1, max_students),
                    key=target_students_key,
                    step=1,
                    on_change=_clear_picker_keys,
                    args=(
                        manual_classroom_key,
                        matching_classrooms_key,
                        preferred_classroom_key,
                    ),
                )
            )
            manual_classroom_id = st.text_input(
                "Classroom ID override (optional)",
                key=manual_classroom_key,
                help=(
                    "If you enter a classroom ID here, the page will use that classroom directly "
                    "inside the selected work-mode scope instead of the currently selected matching classroom."
                ),
            ).strip()

            lower = max(1, int(math.floor(target_students * 0.9)))
            upper = max(lower, int(math.ceil(target_students * 1.1)))
            matching_profiles = select_classrooms_near_student_target(
                profiles,
                mode_scope=mode_scope,
                target_students=target_students,
                tolerance_ratio=0.10,
            )
            if matching_profiles.height == 0:
                st.info("No classrooms found in that range, please try another range.")
                st.stop()

            st.caption(
                f"Showing classrooms with **{lower}** to **{upper}** students in **{_mode_label(mode_scope)}** scope."
            )
            rows = matching_profiles.to_dicts()
            option_map = {_format_classroom_option(row): str(row.get("classroom_id")) for row in rows}
            option_keys = list(option_map.keys())
            selected_option = st.selectbox(
                "Matching classrooms",
                option_keys,
                index=preferred_classroom_option_index(
                    option_map,
                    str(st.session_state.get(preferred_classroom_key) or ""),
                ),
                key=matching_classrooms_key,
                on_change=_clear_picker_keys,
                args=(manual_classroom_key,),
            )
            selected_classroom_id = option_map[selected_option]
            if manual_classroom_id:
                override_classroom_id = select_classroom_by_id(profiles, mode_scope, manual_classroom_id)
                if override_classroom_id is None:
                    st.info(
                        "No classroom in the selected work-mode scope matches that ID. Please check the ID or clear the field."
                    )
                    st.stop()
                override_rows = scoped_profiles.filter(pl.col("classroom_id") == override_classroom_id).to_dicts()
                if not override_rows:
                    st.info("The typed classroom ID is not available in the selected work-mode scope.")
                    st.stop()
                selected_classroom_id = override_classroom_id
                st.caption("Using the typed classroom ID override.")
    set_query_value(
        CLASSROOM_ID_QUERY_KEY,
        None
        if selected_classroom_id == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
        else selected_classroom_id,
    )
    source_first_ts = profiles.select(pl.col("first_attempt_at").min()).item()
    source_last_ts = profiles.select(pl.col("last_attempt_at").max()).item()
    if not (hasattr(source_first_ts, "date") and hasattr(source_last_ts, "date")):
        st.info("This source does not have a valid replay time span.")
        st.stop()
    population_filters = render_population_filters(
        source_id=settings.source_id,
        min_date=source_first_ts.date(),
        max_date=source_last_ts.date(),
        sidebar_header="Global Filters",
    )
    start_date = population_filters.start_date
    end_date = population_filters.end_date

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

    st.sidebar.header("Display")
    show_values = st.sidebar.checkbox("Show cell values", value=False)

    try:
        payload = _load_replay_payload(
            source_id=settings.source_id,
            classroom_id=selected_classroom_id,
            mode_scope=mode_scope,
            start_date_iso=start_date.isoformat(),
            end_date_iso=end_date.isoformat(),
            min_student_attempts=population_filters.min_student_attempts,
            max_frames=int(max_frames),
            step_size=int(step_size),
        )
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    total_events = int(payload.get("total_events_valid_timestamp") or 0)
    if total_events == 0:
        st.info("No attempts found for this classroom and work-mode scope.")
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
        start_date.isoformat(),
        end_date.isoformat(),
        population_filters.min_student_attempts,
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
            f"Scope: **{_mode_label(mode_scope)}**  |  Classroom: **{'All students' if selected_classroom_id == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID else selected_classroom_id}**  "
            f"|  Date range: **{start_date.isoformat()} -> {end_date.isoformat()}**  "
            f"|  Min attempts: **{population_filters.min_student_attempts}**  "
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
        threshold=HEATMAP_MASTERY_THRESHOLD,
        show_values=bool(show_values),
    )
    st.plotly_chart(
        figure,
        width='stretch',
        config=build_plotly_chart_config(),
    )
    scoped_profiles, activity_summary = _load_selected_population_summary(
        settings.source_id,
        selected_classroom_id,
        mode_scope,
        start_date.isoformat(),
        end_date.isoformat(),
        population_filters.min_student_attempts,
    )
    render_figure_analysis(
        analyze_classroom_progression_population(
            scoped_profiles,
            activity_summary,
            mode_scope_label=_mode_label(mode_scope),
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

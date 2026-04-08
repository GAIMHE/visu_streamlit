"""Render a static classroom activity Sankey from final classroom progression paths."""

from __future__ import annotations

import math
import sys
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
    select_classroom_by_id,
    select_classrooms_near_student_target,
)
from visu2.classroom_progression_sankey import (
    build_classroom_activity_paths,
    build_classroom_activity_sankey_figure,
    load_activity_code_lookup,
    max_classroom_activity_path_length,
)
from visu2.config import get_settings
from visu2.figure_analysis import analyze_classroom_progression_sankey
from visu2.remote_query import query_fact_attempts_for_classroom
from visu2.runtime_sources import get_runtime_source

MODE_OPTIONS = {
    "ZPDES": "zpdes",
    "Playlist": "playlist",
    "All modes": "all",
}
CLASSROOM_ID_QUERY_KEY = "classroom_id"
MODE_SCOPE_QUERY_KEY = "mode_scope"


@st.cache_data(show_spinner=False)
def _load_profiles(source_id: str) -> tuple[pl.DataFrame, str]:
    settings = get_settings(source_id)
    return load_or_build_classroom_mode_profiles(settings)


@st.cache_data(show_spinner=False)
def _load_sankey_payload(
    source_id: str,
    learning_catalog_path: Path,
    classroom_id: str,
    mode_scope: str,
    start_date_iso: str,
    end_date_iso: str,
    min_student_attempts: int,
) -> dict:
    settings = get_settings(source_id)
    fact_slice = query_fact_attempts_for_classroom(
        settings,
        classroom_id=classroom_id,
        mode_scope=mode_scope,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        min_student_attempts=min_student_attempts,
        columns=(
            "classroom_id",
            "user_id",
            "work_mode",
            "created_at",
            "date_utc",
            "activity_id",
            "activity_label",
            "exercise_id",
            "attempt_number",
        ),
    )
    return build_classroom_activity_paths(
        fact=fact_slice,
        classroom_id=classroom_id,
        mode_scope=mode_scope,
        start_date=date.fromisoformat(start_date_iso),
        end_date=date.fromisoformat(end_date_iso),
        activity_code_lookup=load_activity_code_lookup(learning_catalog_path),
    )


def _format_classroom_option(row: dict[str, object]) -> str:
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
    learning_catalog_path = settings.learning_catalog_path
    if not learning_catalog_path.exists():
        st.error("Missing runtime metadata: learning_catalog.json.")
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()
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

    st.title("Classroom Progression Sankey")
    render_figure_info("classroom_progression_sankey")

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
            "This source does not have a classroom dimension. The Sankey is using one synthetic classroom that groups all students."
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
            picker_context_key = "classroom_sankey_picker_context"
            target_students_key = "classroom_sankey_target_students"
            manual_classroom_key = "classroom_sankey_manual_classroom_id"
            matching_classrooms_key = "classroom_sankey_matching_classroom"
            preferred_classroom_key = "classroom_sankey_preferred_classroom_id"
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
            selected_option = st.selectbox(
                "Matching classrooms",
                list(option_map.keys()),
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
        st.info("This source does not have a valid time span.")
        st.stop()
    population_filters = render_population_filters(
        source_id=settings.source_id,
        min_date=source_first_ts.date(),
        max_date=source_last_ts.date(),
        sidebar_header="Global Filters",
    )
    start_date = population_filters.start_date
    end_date = population_filters.end_date

    try:
        payload = _load_sankey_payload(
            source_id=settings.source_id,
            learning_catalog_path=learning_catalog_path,
            classroom_id=selected_classroom_id,
            mode_scope=mode_scope,
            start_date_iso=start_date.isoformat(),
            end_date_iso=end_date.isoformat(),
            min_student_attempts=population_filters.min_student_attempts,
        )
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()
    total_events = int(payload.get("total_events_valid_timestamp") or 0)
    if total_events == 0:
        st.info("No attempts found for this classroom and work-mode scope.")
        st.stop()

    max_visible_steps = max_classroom_activity_path_length(payload)
    start_slider_key = "classroom_sankey_start_step"
    start_slider_signature_key = "classroom_sankey_start_step_signature"
    visible_slider_key = "classroom_sankey_visible_steps"
    visible_slider_signature_key = "classroom_sankey_visible_steps_signature"
    slider_signature = (
        mode_scope,
        selected_classroom_id,
        start_date.isoformat(),
        end_date.isoformat(),
        population_filters.min_student_attempts,
        max_visible_steps,
    )
    if st.session_state.get(start_slider_signature_key) != slider_signature:
        current_start = st.session_state.get(start_slider_key)
        if isinstance(current_start, int):
            st.session_state[start_slider_key] = max(1, min(current_start, max_visible_steps))
        else:
            st.session_state[start_slider_key] = 1
        st.session_state[start_slider_signature_key] = slider_signature
    start_step = int(
        st.sidebar.slider(
            "Starting activity step",
            min_value=1,
            max_value=max_visible_steps,
            step=1,
            key=start_slider_key,
        )
    )
    max_window_steps = max(1, max_visible_steps - start_step + 1)
    visible_slider_signature = (*slider_signature, start_step)
    if st.session_state.get(visible_slider_signature_key) != visible_slider_signature:
        current_value = st.session_state.get(visible_slider_key)
        if isinstance(current_value, int):
            st.session_state[visible_slider_key] = max(1, min(current_value, max_window_steps))
        else:
            st.session_state[visible_slider_key] = min(6, max_window_steps)
        st.session_state[visible_slider_signature_key] = visible_slider_signature
    visible_steps = int(
        st.sidebar.slider(
            "Visible activity steps",
            min_value=1,
            max_value=max_window_steps,
            step=1,
            key=visible_slider_key,
        )
    )
    window_end_step = start_step + visible_steps - 1
    visible_student_count = sum(
        1
        for row in (payload.get("student_paths") or [])
        if len(row.get("activity_ids") or []) >= start_step
    )

    dropped = int(payload.get("dropped_invalid_timestamps") or 0)
    if dropped > 0:
        st.warning(f"{dropped} row(s) were dropped because `created_at`, `user_id`, or `activity_id` was missing.")

    distinct_activities = len(payload.get("activity_ids") or [])
    if distinct_activities <= 1:
        st.info("This classroom reaches only one activity in the selected scope, so the Sankey mainly shows entry and stop.")

    st.caption(
        "This Sankey uses the selected-classroom history inside the chosen date range and work-mode scope. "
        "Each student path keeps only the first time the student reaches a new activity; revisits are not displayed as new Sankey steps."
    )
    st.caption(
        f"Scope: **{_mode_label(mode_scope)}**  |  Classroom: **{payload.get('classroom_label') or selected_classroom_id}**  "
        f"|  Date range: **{start_date.isoformat()} -> {end_date.isoformat()}**  "
        f"|  Min attempts: **{population_filters.min_student_attempts}**  "
        f"|  Students in classroom: **{len(payload.get('student_ids') or [])}**  "
        f"|  Students reaching step {start_step}: **{visible_student_count}**  "
        f"|  Activities: **{distinct_activities}**  |  Window: **steps {start_step}-{window_end_step}**"
    )

    figure = build_classroom_activity_sankey_figure(
        payload,
        visible_steps=visible_steps,
        start_step=start_step,
    )
    if not figure.data:
        st.info(
            f"No students reach step {start_step} in the selected classroom, date range, and work-mode scope."
        )
    else:
        st.plotly_chart(
            figure,
            width="stretch",
            config=build_plotly_chart_config(),
        )

    render_figure_analysis(
        analyze_classroom_progression_sankey(
            payload,
            visible_steps=visible_steps,
            start_step=start_step,
        )
    )


if __name__ == "__main__":
    main()

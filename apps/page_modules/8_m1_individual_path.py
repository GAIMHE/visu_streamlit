"""Replay one student's Module 1 path on the M1 dependency layout."""

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
from source_state import get_active_source_id

from visu2.config import get_settings
from visu2.contracts import RUNTIME_CORE_COLUMNS
from visu2.figure_analysis import analyze_m1_individual_path
from visu2.m1_individual_path import (
    build_m1_individual_path_figure,
    build_m1_individual_path_payload,
    load_m1_student_profiles,
    select_m1_student_by_id,
    select_m1_students_near_attempt_target,
)
from visu2.remote_query import query_student_module_attempts
from visu2.zpdes_dependencies import build_dependency_tables_from_metadata

MODULE_CODE = "M1"
FACT_COLUMNS: tuple[str, ...] = (
    "created_at",
    "date_utc",
    "user_id",
    "objective_id",
    "objective_label",
    "activity_id",
    "activity_label",
    "exercise_id",
    "data_correct",
    "work_mode",
    "attempt_number",
    "module_code",
    "module_label",
)


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


@st.cache_data(show_spinner=False)
def _load_m1_profiles(path: Path) -> pl.DataFrame:
    return load_m1_student_profiles(path)


@st.cache_data(show_spinner=False)
def _load_m1_topology(
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    return build_dependency_tables_from_metadata(
        module_code=MODULE_CODE,
        learning_catalog_path=learning_catalog_path,
        zpdes_rules_path=zpdes_rules_path,
    )


@st.cache_data(show_spinner=False)
def _load_payload(
    source_id: str,
    user_ids: tuple[str, ...],
    step_size: int,
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
) -> dict[str, object]:
    settings = get_settings(source_id)
    nodes, edges, _warnings = _load_m1_topology(learning_catalog_path, zpdes_rules_path)
    events = query_student_module_attempts(
        settings,
        user_ids=list(user_ids),
        module_code=MODULE_CODE,
        columns=FACT_COLUMNS,
    )
    return build_m1_individual_path_payload(events, list(user_ids), step_size, nodes, edges)


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
  padding: 0.85rem;
}
</style>
""",
        unsafe_allow_html=True,
    )
    settings = get_settings(get_active_source_id())
    if settings.source_id != "main":
        st.info("Module 1 Individual Path is only available for the main source.")
        st.stop()

    profiles_path = settings.artifacts_derived_dir / "student_elo_profiles.parquet"
    required = [
        settings.learning_catalog_path,
        settings.zpdes_rules_path,
        profiles_path,
    ]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing runtime artifacts required by the Module 1 path page.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    profile_missing = [
        col for col in RUNTIME_CORE_COLUMNS["student_elo_profiles"] if col not in _parquet_columns(profiles_path)
    ]
    if profile_missing:
        st.error("Module 1 path profiles are incompatible with the current runtime contract.")
        st.markdown(f"- `student_elo_profiles`: {', '.join(profile_missing)}")
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    m1_profiles = _load_m1_profiles(profiles_path)
    if m1_profiles.height == 0:
        st.info("No Module 1 student profiles are available.")
        st.stop()

    nodes, _edges, warnings = _load_m1_topology(settings.learning_catalog_path, settings.zpdes_rules_path)
    if nodes.height == 0:
        st.warning("No Module 1 dependency nodes were found in the current metadata.")
        if warnings:
            st.info("\n".join(f"- {warning}" for warning in warnings))
        st.stop()

    st.title("Module 1 Individual Path")
    render_figure_info("m1_individual_path_page")
    if warnings:
        st.caption(" | ".join(warnings))
    st.caption(
        "This page replays all Module 1 attempts across work modes on top of the fixed M1 dependency layout."
    )

    min_attempt_count = int(m1_profiles["total_attempts"].min() or 1)
    max_attempt_count = int(m1_profiles["total_attempts"].max() or 1)
    median_attempt_count = int(m1_profiles["total_attempts"].median() or min_attempt_count)
    st.caption(
        f"Module 1 students range from **{min_attempt_count}** to **{max_attempt_count}** attempts."
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
        help="If you enter a student ID here, the page will use that student directly instead of the sampled attempt-range selection.",
    ).strip()

    if manual_student_id:
        selected_student = select_m1_student_by_id(m1_profiles, manual_student_id)
        if selected_student is None:
            st.info("No Module 1 student matches that ID. Please check the ID or clear the field.")
            st.stop()
        normalized_students = [selected_student]
        st.caption("Using the typed student ID override.")
    else:
        selection_signature = ("m1_individual_path_attempt_target", target_attempts)
        selection_state_key = "m1_individual_path_selected_students"
        selection_signature_key = "m1_individual_path_attempt_target_signature"
        if st.session_state.get(selection_signature_key) != selection_signature:
            sampled = select_m1_students_near_attempt_target(
                m1_profiles,
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
                f"No Module 1 students found in the {lower}-{upper} attempt range. Please try another range."
            )
            st.stop()

    selected_student_id = normalized_students[0]
    selected_profile_rows = m1_profiles.filter(pl.col("user_id") == selected_student_id).to_dicts()
    if not selected_profile_rows:
        st.info("The selected Module 1 student is no longer available in the current profiles.")
        st.stop()
    selected_profile = selected_profile_rows[0]

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

    try:
        payload = _load_payload(
            settings.source_id,
            tuple(normalized_students),
            step_size,
            settings.learning_catalog_path,
            settings.zpdes_rules_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    if not payload.get("student_ids"):
        st.info("No Module 1 attempts are available for the selected student.")
        st.stop()

    state_signature = (
        tuple(payload.get("student_ids") or []),
        int(step_size),
        manual_student_id or int(target_attempts),
    )
    signature_key = "m1_individual_path_signature"
    frame_key = "m1_individual_path_frame_idx"
    playing_key = "m1_individual_path_playing"
    if st.session_state.get(signature_key) != state_signature:
        st.session_state[signature_key] = state_signature
        st.session_state[frame_key] = 0
        st.session_state[playing_key] = False
    if frame_key not in st.session_state:
        st.session_state[frame_key] = 0
    if playing_key not in st.session_state:
        st.session_state[playing_key] = False

    user_series = ((payload.get("series") or {}).get(selected_student_id)) or {}
    total_attempts = int(payload.get("max_attempts") or 0)
    total_mapped_attempts = int(user_series.get("mapped_attempt_total") or 0)
    total_unmapped_attempts = int(user_series.get("unmapped_attempt_total") or 0)
    total_activity_nodes = len((payload.get("topology") or {}).get("activity_rows") or [])
    current_frame_cutoffs = [int(value) for value in payload.get("frame_cutoffs") or [0]]
    max_frame_idx = max(0, len(current_frame_cutoffs) - 1)

    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Student", selected_student_id)
        st.caption("Selected from Module 1 profiles")
    with summary_cols[1]:
        st.metric("Module", selected_profile.get("module_label") or MODULE_CODE)
        st.caption(MODULE_CODE)
    with summary_cols[2]:
        st.metric("Total attempts", total_attempts)
        st.caption(f"Mapped: {total_mapped_attempts} | Unmapped: {total_unmapped_attempts}")
    with summary_cols[3]:
        st.metric("Mapped activities", total_activity_nodes)
        st.caption(
            f"{selected_profile.get('first_attempt_at')} -> {selected_profile.get('last_attempt_at')}"
        )
    st.caption(
        f"All visible work modes inside M1 | Profile attempts: **{int(selected_profile.get('total_attempts') or 0)}**"
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
            f"Student: **{selected_student_id}** | Module: **{MODULE_CODE}** | "
            f"Step size: **{step_size}** | Max local attempt: **{total_attempts}**"
        )

    slider_value = st.slider(
        "Replay frame",
        min_value=0,
        max_value=max_frame_idx,
        value=min(max_frame_idx, int(st.session_state[frame_key])),
    )
    st.session_state[frame_key] = int(slider_value)
    frame_idx = int(st.session_state[frame_key])
    cutoff = int(current_frame_cutoffs[frame_idx]) if frame_idx < len(current_frame_cutoffs) else 0
    st.caption(f"Frame {frame_idx}/{max_frame_idx} | Local attempt cutoff: {cutoff}")

    figure = build_m1_individual_path_figure(payload, frame_idx)
    st.plotly_chart(
        figure,
        width="stretch",
        key="m1_individual_path_graph",
        config=build_plotly_chart_config(
            extra={
                "toImageButtonOptions": {
                    "filename": f"m1_individual_path_{selected_student_id}",
                }
            },
        ),
    )
    render_figure_analysis(analyze_m1_individual_path(payload, frame_idx=frame_idx))

    if st.session_state[playing_key]:
        if frame_idx >= max_frame_idx:
            st.session_state[playing_key] = False
        else:
            time.sleep(max(0.1, float(speed_ms) / 1000.0))
            st.session_state[frame_key] = min(max_frame_idx, frame_idx + 1)
            st.rerun()

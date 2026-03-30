"""
5_student_elo_evolution.py

Render student Elo trajectories, system selector, replay controls, and chart output.

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
from runtime_bootstrap import bootstrap_optional_runtime_assets
from source_state import get_active_source_id

from visu2.config import get_settings
from visu2.contracts import RUNTIME_CORE_COLUMNS
from visu2.figure_analysis import analyze_student_elo_page
from visu2.remote_query import query_student_elo_events, query_student_fact_label_lookup
from visu2.student_elo import (
    build_student_elo_figure,
    build_student_elo_payload,
    load_student_elo_label_lookup,
    load_student_elo_profiles,
    merge_student_elo_label_lookups,
    modules_for_student,
    select_student_by_id,
    select_students_near_attempt_target,
    summarize_student_module_profiles,
)

CURRENT_EVENTS_RELATIVE_PATH = "artifacts/derived/student_elo_events.parquet"
BATCH_REPLAY_EVENTS_RELATIVE_PATH = "artifacts/derived/student_elo_events_batch_replay.parquet"
CURRENT_PROFILES_FILENAME = "student_elo_profiles.parquet"
BATCH_REPLAY_PROFILES_FILENAME = "student_elo_profiles_batch_replay.parquet"
BATCH_REPLAY_PROFILES_RELATIVE_PATH = (
    f"artifacts/derived/{BATCH_REPLAY_PROFILES_FILENAME}"
)
ELO_SYSTEM_CONFIGS: dict[str, dict[str, str]] = {
    "Current Elo": {
        "profiles_filename": CURRENT_PROFILES_FILENAME,
        "profiles_contract_key": "student_elo_profiles",
        "events_relative_path": CURRENT_EVENTS_RELATIVE_PATH,
        "caption": (
            "Current Elo uses the fixed module-local exercise difficulties calibrated from first "
            "attempts only, then replays the student sequentially inside the selected module "
            "with the student reset to 1500 at the start of that module."
        ),
    },
    "Batch Replay Elo": {
        "profiles_filename": BATCH_REPLAY_PROFILES_FILENAME,
        "profiles_contract_key": "student_elo_profiles_batch_replay",
        "events_relative_path": BATCH_REPLAY_EVENTS_RELATIVE_PATH,
        "caption": (
            "Batch Replay Elo reuses the same fixed module-local exercise difficulties, but at "
            "each visible attempt the student's level is refit from the whole module-local "
            "history seen so far, starting from 1500 on the first attempt of that module."
        ),
    },
}
ELO_EVENT_QUERY_COLUMNS: tuple[str, ...] = (
    "user_id",
    "attempt_ordinal",
    "created_at",
    "date_utc",
    "work_mode",
    "module_code",
    "objective_id",
    "activity_id",
    "exercise_id",
    "outcome",
    "expected_success",
    "exercise_elo",
    "student_elo_pre",
    "student_elo_post",
)


@st.cache_data(show_spinner=False)
def _load_profiles(path: Path) -> pl.DataFrame:
    return load_student_elo_profiles(path)


@st.cache_data(show_spinner=False)
def _load_label_lookup(path: Path, exercise_elo_path: Path) -> pl.DataFrame:
    return load_student_elo_label_lookup(path, exercise_elo_path)


@st.cache_data(show_spinner=False)
def _load_payload(
    source_id: str,
    label_path: Path,
    exercise_elo_path: Path,
    events_relative_path: str,
    user_ids: tuple[str, ...],
    module_code: str,
    step_size: int,
) -> dict[str, object]:
    settings = get_settings(source_id)
    users = list(user_ids)
    label_lookup = merge_student_elo_label_lookups(
        query_student_fact_label_lookup(settings, user_ids=users, module_code=module_code),
        _load_label_lookup(label_path, exercise_elo_path),
    )
    return build_student_elo_payload(
        query_student_elo_events(
            settings,
            relative_path=events_relative_path,
            user_ids=users,
            columns=ELO_EVENT_QUERY_COLUMNS,
            module_code=module_code,
        ),
        users,
        step_size,
        label_lookup=label_lookup,
    )


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _available_elo_system_configs(settings) -> dict[str, dict[str, str]]:
    """Return only the Elo systems whose local profile artifacts are present."""
    available: dict[str, dict[str, str]] = {}
    for system_name, config in ELO_SYSTEM_CONFIGS.items():
        profiles_path = settings.artifacts_derived_dir / config["profiles_filename"]
        if profiles_path.exists():
            available[system_name] = config
    return available


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
    bootstrap_optional_runtime_assets(
        settings.source_id,
        required_paths=(BATCH_REPLAY_PROFILES_RELATIVE_PATH,),
    )
    current_exercise_elo_path = settings.artifacts_derived_dir / "agg_exercise_elo.parquet"
    all_system_paths = {
        system_name: settings.artifacts_derived_dir / config["profiles_filename"]
        for system_name, config in ELO_SYSTEM_CONFIGS.items()
    }

    required = [
        current_exercise_elo_path,
        all_system_paths["Current Elo"],
    ]
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Missing Student Elo artifacts. Rebuild derived data.")
        st.code("\n".join(str(path) for path in missing))
        st.stop()

    available_system_configs = _available_elo_system_configs(settings)
    if "Current Elo" not in available_system_configs:
        st.error("Current Elo artifacts are missing or incomplete.")
        st.code(
            "\n".join(
                (
                    str(all_system_paths["Current Elo"]),
                )
            )
        )
        st.stop()

    compatibility_checks = {
        "agg_exercise_elo": (current_exercise_elo_path, RUNTIME_CORE_COLUMNS["agg_exercise_elo"]),
        **{
            config["profiles_contract_key"]: (
                all_system_paths[system_name],
                RUNTIME_CORE_COLUMNS[config["profiles_contract_key"]],
            )
            for system_name, config in available_system_configs.items()
        },
    }
    missing_contracts: list[str] = []
    for label, (path, required_columns) in compatibility_checks.items():
        missing_columns = [col for col in required_columns if col not in _parquet_columns(path)]
        if missing_columns:
            missing_contracts.append(f"- `{label}`: {', '.join(missing_columns)}")
    if missing_contracts:
        st.error("Student Elo artifacts are incompatible with the current runtime contract.")
        st.markdown("\n".join(missing_contracts))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    st.title("Student Elo Evolution")
    render_figure_info("student_elo_page")
    system_names = tuple(available_system_configs.keys())
    selected_system = "Current Elo"
    if len(system_names) > 1:
        selected_system = st.radio(
            "Elo system",
            options=system_names,
            index=system_names.index("Current Elo"),
            horizontal=True,
            help=(
                "Current Elo replays one incremental update per attempt. Batch Replay Elo refits "
                "the student level from the whole module-local prefix at each attempt."
            ),
        )
    else:
        st.caption(
            "Batch Replay Elo is not available yet for this source. It will appear once its "
            "artifacts are built locally."
        )
    selected_system_config = available_system_configs[selected_system]
    module_profiles = _load_profiles(all_system_paths[selected_system])
    if module_profiles.height == 0:
        st.info(f"No {selected_system} profiles are available.")
        st.stop()

    student_summary = summarize_student_module_profiles(module_profiles)
    eligible_students = student_summary.filter(pl.col("eligible_for_replay"))
    if eligible_students.height == 0:
        st.info(f"No {selected_system} trajectories are replay-eligible.")
        st.stop()
    st.caption(selected_system_config["caption"])

    min_attempt_count = int(eligible_students["total_attempts"].min() or 0)
    max_attempt_count = int(eligible_students["total_attempts"].max() or 0)
    median_attempt_count = int(eligible_students["total_attempts"].median() or min_attempt_count or 1)

    st.caption(
        f"Replay-eligible students range from **{min_attempt_count}** to **{max_attempt_count}** total attempts across modules for **{selected_system}**."
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
        selected_student = select_student_by_id(eligible_students, manual_student_id)
        if selected_student is None:
            st.info(
                "No replay-eligible student matches that ID. Please check the ID or clear the field."
            )
            st.stop()
        normalized_students = [selected_student]
        st.caption("Using the typed student ID override.")
    else:
        selection_signature = ("student_elo_attempt_target", selected_system, target_attempts)
        selection_state_key = "student_elo_selected_students"
        selection_signature_key = "student_elo_attempt_target_signature"
        if st.session_state.get(selection_signature_key) != selection_signature:
            sampled = select_students_near_attempt_target(
                eligible_students,
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

    selected_student_id = normalized_students[0]
    selected_student_summary_rows = (
        eligible_students.filter(pl.col("user_id") == selected_student_id).to_dicts() or []
    )
    if not selected_student_summary_rows:
        st.info("The selected student is no longer replay-eligible under the current profiles.")
        st.stop()
    selected_student_summary = selected_student_summary_rows[0]
    student_modules = modules_for_student(
        module_profiles.filter(pl.col("eligible_for_replay")),
        selected_student_id,
    )
    if student_modules.height == 0:
        st.info("The selected student has no replay-eligible module trajectory.")
        st.stop()

    module_options = student_modules.to_dicts()
    module_codes = [str(row.get("module_code") or "") for row in module_options]
    selected_module_code = st.selectbox(
        "Module",
        options=module_codes,
        index=0,
        format_func=lambda code: next(
            (
                f"{row.get('module_label') or code} ({code}) | "
                f"{int(row.get('total_attempts') or 0)} attempts | "
                f"final Elo {float(row.get('final_student_elo') or 0.0):.1f}"
                for row in module_options
                if str(row.get("module_code") or "") == code
            ),
            code,
        ),
        help="Choose one of the modules available for the selected student. The replay and Elo values are module-local.",
    )
    selected_module_profile = (
        student_modules.filter(pl.col("module_code") == selected_module_code).to_dicts() or []
    )
    if not selected_module_profile:
        st.info("No replay profile is available for the selected module.")
        st.stop()
    selected_module_entry = selected_module_profile[0]

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
        payload = _load_payload(
            settings.source_id,
            settings.learning_catalog_path,
            current_exercise_elo_path,
            selected_system_config["events_relative_path"],
            tuple(normalized_students),
            selected_module_code,
            step_size,
        )
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    student_ids = payload.get("student_ids") or []
    if not student_ids:
        st.info("No Elo event rows are available for the selected students.")
        st.stop()

    frame_cutoffs = payload.get("frame_cutoffs") or [0]
    max_frame_idx = max(0, len(frame_cutoffs) - 1)
    state_signature = (
        selected_system,
        tuple(student_ids),
        selected_module_code,
        int(step_size),
        manual_student_id or int(target_attempts),
    )
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

    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Student", selected_student_id)
    with summary_cols[1]:
        st.metric("Elo system", selected_system)
        st.caption("One system at a time")
    with summary_cols[2]:
        st.metric("Module", selected_module_entry.get("module_label") or selected_module_code)
        st.caption(selected_module_code)
    with summary_cols[3]:
        st.metric("Final Elo", f"{float(selected_module_entry.get('final_student_elo') or 0.0):.1f}")
        st.caption(
            f"{selected_module_entry.get('first_attempt_at')} -> {selected_module_entry.get('last_attempt_at')}"
        )
    st.caption(
        f"Module attempts: **{int(selected_module_entry.get('total_attempts') or 0)}** | "
        f"Student total across modules in {selected_system}: **{int(selected_student_summary.get('total_attempts') or 0)}**"
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
            f"System: **{selected_system}** | "
            f"Student: **{student_ids[0]}** | "
            f"Module: **{selected_module_code}** | "
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
    figure = build_student_elo_figure(
        payload,
        frame_idx,
        gap_days_threshold=gap_days_threshold,
        system_label=selected_system,
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

    render_figure_analysis(analyze_student_elo_page(payload))

    if st.session_state[playing_key]:
        if frame_idx >= max_frame_idx:
            st.session_state[playing_key] = False
        else:
            time.sleep(max(0.1, float(speed_ms) / 1000.0))
            st.session_state[frame_key] = min(max_frame_idx, frame_idx + 1)
            st.rerun()


if __name__ == "__main__":
    main()

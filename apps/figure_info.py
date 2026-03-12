"""Shared collapsed info panels for analytical Streamlit blocks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import streamlit as st

SectionContent = Mapping[str, Sequence[str]]


FIGURE_INFO: dict[str, dict[str, tuple[str, ...]]] = {
    "overview_kpi_cards": {
        "What it shows": (
            "A compact summary of the currently filtered attempt history.",
            "The cards report total attempts, unique students, unique exercises, attempt-weighted success, and exercise-balanced success.",
        ),
        "Why it matters": (
            "This is the quickest way to understand the volume and overall level of the selected slice before reading the detailed charts.",
        ),
        "Metrics": (
            "Attempt-weighted success = mean of `data_correct` over all filtered attempts.",
            "Exercise-balanced success = mean of per-exercise success rates, so each exercise counts equally.",
        ),
        "Controls that affect it": (
            "Sidebar date, module, objective, and activity filters all change these cards.",
        ),
        "How to read / interact": (
            "Use the cards as the top-level baseline for the rest of the page.",
            "A large gap between the two success rates usually means performance varies a lot across exercises.",
        ),
    },
    "overview_work_mode_summary_table": {
        "Work modes": (
            "`zpdes` is the adaptive progression algorithm.",
            "`playlist` is a learning path chosen by the teacher.",
            "`adaptive-test` is a test used to estimate the student's level before regular progression.",
            "`initial-test` is an older version of that level-estimation test.",
        ),
        "Success metrics": (
            "Success rate (attempt-weighted) is the share of attempts answered correctly. Exercises that are attempted many times have more weight in this value.",
            "Success rate (exercise-balanced) is computed by first calculating a success rate for each exercise, then averaging those exercise-level rates so every exercise counts equally.",
        ),
    },
    "bottlenecks_transitions_bottleneck_chart": {
        "What it shows": (
            "A ranked horizontal bar chart of modules, objectives, or activities where learners appear to struggle the most in the selected slice.",
            "The chart can switch between module level, objective level, and activity level.",
        ),
        "Metrics": (
            "Failure rate is the share of attempts that were not successful.",
            "Repeat attempt rate is the share of attempts that were repeats rather than first tries.",
            "Bottleneck score combines those two signals, with more weight given to low success than to repetition. Higher values indicate stronger candidate bottlenecks.",
        ),
    },
    "bottlenecks_transitions_path_chart": {
        "What it shows": (
            "A horizontal bar chart of the most frequent activity-to-activity transitions in the selected slice.",
            "It focuses on cross-objective transitions, so it highlights movement between different parts of the module rather than short local loops inside the same objective.",
        ),
        "Metrics": (
            "Transition count is the number of times students were observed moving from one activity to the next.",
            "Successful destination attempts counts how many of those transitions were followed by a correct attempt on the destination activity.",
        ),
    },
    "matrix_objective_activity_heatmap": {
        "What it shows": (
            "A module-level matrix where rows are objectives and columns are activity positions.",
            "Each populated cell summarizes one objective/activity pair with the selected metric.",
            "Click a populated cell to open the exercise drilldown for that activity.",
        ),
        "Metrics": (
            "`Attempts` counts all recorded attempts on exercises inside the activity.",
            "`Success rate` is the share of those attempts answered correctly.",
            "`Exercise-balanced success` first computes a success rate for each exercise, then averages those exercise-level rates so heavily used exercises do not dominate the result.",
            "`Repeat-attempt rate` is the share of attempts that are not the first try on that exercise.",
            "`First-attempt success` is the share of first tries that were correct.",
            "`Playlist unique exercises` counts how many distinct exercises from that activity were used in playlist mode.",
            "`Activity mean exercise Elo` is the average calibrated difficulty of the exercises in that activity; higher values indicate harder content.",
            "The Elo metric is global difficulty calibration and does not change with the date filter.",
        ),
    },
    "matrix_exercise_drilldown_table": {
        "What it shows": (
            "An exercise-level table for the currently selected matrix cell.",
        ),
        "Metrics": (
            "Standard metrics include attempts, success rate, first-attempt success, repeat-attempt rate, and median duration.",
            "For the Elo metric, the table switches to exercise Elo, calibration attempts, and calibration success rate.",
        ),
    },
    "matrix_exercise_instruction_panel": {
        "What it shows": (
            "Instruction text and a placeholder screenshot for the currently selected exercise row.",
        ),
        "Why it matters": (
            "It gives immediate exercise context without leaving the analytics page.",
        ),
        "Metrics": (
            "This block does not compute new metrics.",
        ),
        "Controls that affect it": (
            "It updates only when one row is selected in the exercise drilldown table.",
        ),
        "How to read / interact": (
            "If no row is selected, this panel stays empty.",
            "The instruction text is currently sourced from the exercise label shown in the drilldown.",
        ),
    },
    "classroom_progression_replay_heatmap": {
        "What it shows": (
            "An animated student-by-activity heatmap for one classroom.",
            "Rows are activities, columns are students, and each frame shows the classroom state after a synchronized progression step.",
            "Student labels are bold when that student is still active.",
        ),
        "Metrics": (
            "One replay step means each student advances by the same number of their own local attempts in that frame.",
            "With step size `1`, each frame adds the next attempt of each student if one exists; larger step sizes add that many next local attempts per student.",
            "Each cell stores cumulative attempts and cumulative successes for one student on one activity.",
            "Cell color is `cumulative successes / cumulative attempts` for that student-activity pair.",
            "The hover also reports cumulative successful attempts and the number of unique exercises already seen in that student-activity cell.",
            "Unseen cells stay blank until the student reaches that activity.",
            "The color scale uses a fixed reference threshold of `75%` to separate weaker and stronger cumulative performance.",
            "`Show cell values` writes the current cumulative success rate percentage only on populated cells.",
        ),
    },
    "student_elo_summary_cards": {
        "What it shows": (
            "A compact summary for the one or two currently selected student trajectories.",
        ),
        "Why it matters": (
            "It frames the comparison before you interpret the replay chart.",
        ),
        "Metrics": (
            "Each card shows total attempts, final student Elo, and the first/last attempt timestamps from the profile artifact.",
        ),
        "Controls that affect it": (
            "The target attempt-count field determines which randomly sampled student trajectories appear.",
        ),
        "How to read / interact": (
            "Use these cards to compare trajectory length and end-state Elo before reading the line chart.",
        ),
    },
    "student_elo_replay_chart": {
        "What it shows": (
            "A replayable Elo trajectory chart for one or two students over their own attempt sequences.",
        ),
        "Why it matters": (
            "It helps compare pace, stability, and recovery patterns under a fixed exercise-difficulty scale.",
        ),
        "Metrics": (
            "The plotted value is post-attempt student Elo.",
            "Exercise difficulty is fixed from calibrated exercise Elo; only the student rating moves during replay.",
        ),
        "Controls that affect it": (
            "The target attempt-count field, step size, autoplay speed, replay buttons, and frame slider affect this chart.",
        ),
        "How to read / interact": (
            "Use `Play`, `Pause`, `Reset`, and `Step +1` to step through the local attempt timeline.",
            "The frame caption shows the current local attempt cutoff shared across displayed students.",
        ),
    },
    "zpdes_transition_efficiency_graph": {
        "What it shows": (
            "A structural view of ZPDES dependencies inside one module.",
            "Squares represent objectives and circles represent activities inside the module.",
            "Activity circles are colored either by first-attempt success in ZPDES mode or by activity mean exercise Elo.",
        ),
        "Metrics": (
            "Hover always shows ZPDES first-attempt success and event counts for the activity.",
            "The `before` cohort groups first attempts on new exercises made by students whose prior ZPDES history in the module stays on earlier activities only.",
            "The `after` cohort groups first attempts on new exercises made by students who already have at least the configured number of prior attempts on later activities in the module.",
            "The `in-activity` cohort groups first attempts on new exercises made by students who already worked on another exercise from the same activity but do not meet the `after` condition.",
            "Each cohort reports four values: success rate, eligible event count, unique-student count, and total previous attempts.",
        ),
    },
}


def render_figure_info(figure_key: str, label: str = "Info") -> None:
    """Render a collapsed informational panel for one analytical block.

    Parameters
    ----------
    figure_key : str
        Key in ``FIGURE_INFO`` selecting the content to display.
    label : str, optional
        Expander header label, by default ``"Info"``.
    """
    sections = FIGURE_INFO[figure_key]
    with st.expander(label, expanded=False):
        for section_title, bullets in sections.items():
            st.markdown(f"**{section_title}**")
            st.markdown("\n".join(f"- {bullet}" for bullet in bullets))

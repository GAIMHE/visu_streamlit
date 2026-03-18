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
    "overview_attempt_concentration_chart": {
        "What it shows": (
            "A ranked concentration chart of attempt volume after the current overview filters are applied.",
            "The chart has two bases: `Content concentration` and `Student concentration`.",
            "In content concentration, you can switch between exercise, activity, objective, and module level.",
            "In student concentration, students are ranked globally across all visible attempts.",
        ),
        "Metrics": (
            "The y-axis is always the share of visible attempts covered by the selected bar.",
            "For content concentration, exercise, activity, and objective rows are ranked by attempts and grouped into rank buckets such as `Top 10%`; module view shows one bar per module.",
            "For global student concentration, students are ranked by attempts in the current slice, then grouped into the same rank buckets.",
            "The drilldown table changes with the selected basis: it shows either entities or students.",
            "Contained exercises means `1` for exercises, and the number of catalog exercises inside the activity, objective, or module at the higher levels.",
        ),
        "How to read / interact": (
            "Use the basis selector to switch between content and student concentration, then choose the relevant content level when content concentration is active.",
            "The work-mode multiselect restricts the chart to one or several modes inside the current overview filter slice.",
            "Click a bar to open the matching entity or student rows below the chart.",
        ),
    },
    "overview_work_mode_transitions_sankey": {
        "What it shows": (
            "A global Sankey diagram of student work-mode histories, independent of the page filters above.",
            "Each student enters through their first observed work mode, then follows up to the first three mode changes seen in the raw attempt history.",
            "Students who never change mode end in `No transition`, and students who keep changing after the third visible change end in `More than 3 transitions`.",
        ),
        "Metrics": (
            "A transition is counted only when the work mode differs from the student's previous attempt; repeated attempts in the same mode do not create extra transitions.",
            "Link width is the number of students following that part of the path.",
            "To keep the chart readable, links involving fewer than 10 students are hidden from the display.",
            "Hover reports the source and target stages, the student count, and the percentage of all students covered by that link.",
            "Each link uses the destination-mode color so the landing stage is easier to follow visually.",
            "The figure is based on the raw attempt parquet ordered by `student_attempt_index`, so it reflects full student histories rather than the current overview filter slice.",
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
            "Bar length now shows failure rate directly, while bar color shows repeat attempt rate.",
            "The combined bottleneck score is still kept in hover and analysis as a summary signal, with more weight given to low success than to repetition.",
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
    "classroom_progression_sankey": {
        "What it shows": (
            "A static Sankey diagram of one classroom's activity progression in the selected work-mode scope.",
            "Each student starts at the first activity they reach, then follows the ordered sequence of newly reached activities.",
            "Revisits to already-seen activities are intentionally ignored so the figure shows progression breadth rather than back-and-forth navigation.",
        ),
        "Metrics": (
            "Link width counts unique students following that part of the classroom path.",
            "`Visible activity steps` controls how many newly reached activity stages remain visible before students are sent to a terminal node. Its maximum automatically follows the selected classroom's longest first-time path.",
            "When the activity exists in the canonical catalog, the visible node label uses its code, such as `M1O1A1`; the real activity name stays in hover.",
            "Students who stop before the visible depth go to labels such as `Stopped after 2 activities`, while students who continue beyond the visible depth go to `More than N activities`.",
            "Activity codes stay visible on the nodes, while hover keeps the real activity names; terminal nodes keep their visible labels.",
            "Hover reports the source and target activities, the student count, the share of the selected classroom, and the share of students arriving at the source node.",
            "The figure uses the full time span of the selected classroom in the chosen mode scope; page replay frames are not used here.",
        ),
        "How to use": (
            "Use `Target classroom size (students)` and `Matching classrooms` to keep the current size-based matching workflow.",
            "If you already know a classroom ID, type it in `Classroom ID override (optional)` to load that classroom directly inside the selected work-mode scope.",
            "The typed classroom override supersedes the currently selected matching classroom, but the matching list stays available for browsing nearby classes.",
        ),
    },
    "student_elo_page": {
        "What it shows": (
            "A replayable comparison chart for one student over that student's own attempt sequence.",
            "The selected student is shown twice: once with the current retrospective item-Elo system and once with the new iterative offline calibration.",
            "The summary card above the chart shows the student's total attempts, both final Elo values, and first/last timestamps.",
        ),
        "Metrics": (
            "The plotted value is post-attempt student Elo after each visible update; line style identifies the Elo system, point color identifies the module of the attempt, and point shape identifies the work mode.",
            "The hover reports the system name, timestamp, module, objective, activity, exercise, work mode, outcome, expected success, fixed exercise difficulty, and the student's Elo before and after that attempt.",
            "Optional dotted vertical markers show large timestamp gaps between consecutive attempts while keeping the x-axis on local attempt ordinal rather than calendar time.",
            "Expected success is computed from the gap between the student's current Elo and the fixed difficulty of the exercise.",
            "After each attempt, both systems update student Elo with the same correction proportional to `(outcome - expected success)`; the comparison isolates the effect of changing only the fixed exercise calibration.",
            "The current system keeps the existing sequential retrospective item-Elo calibration on first attempts.",
            "The iterative system starts from smoothed first-attempt success rate, then alternates between student replay and item refitting until the fixed exercise difficulties stabilize.",
            "The human graph structure is not used to define difficulty in either system; it remains an interpretation tool only.",
            "If an attempted exercise is outside the mapped catalog, it can still be calibrated and replayed; the page then uses fallback labels such as `Unmapped initial-test activity (M1)` to make that missing context explicit.",
        ),
        "How to use": (
            "Choose a target attempt count to sample one student whose total attempts fall within about +/- 10% of that value in both systems.",
            "If you already know a student ID, type it in the override field to load that jointly replay-eligible student directly.",
            "Use `Displayed Elo system` to switch between the current system only, the iterative system only, or both together; the default view shows only the current Elo.",
            "Use the replay controls to step through the local attempt timeline with a chosen step size and autoplay speed while both systems stay synchronized when both are visible.",
            "Use `Highlight timestamp gaps >= days` to mark long inactivity periods without switching the chart away from attempt-based progression.",
            "If no students are found in the requested attempt range, try another target count, or clear the ID override if it does not match an eligible student.",
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

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
            "A Sankey diagram of student work-mode histories inside the current overview filter slice.",
            "Each student enters through their first observed work mode, then follows up to the first three mode changes seen in the raw attempt history.",
            "Students who never change mode end in `No transition`, and students who keep changing after the third visible change end in `More than 3 transitions`.",
        ),
        "Metrics": (
            "A transition is counted only when the work mode differs from the student's previous attempt; repeated attempts in the same mode do not create extra transitions.",
            "Link width is the number of students following that part of the path.",
            "To keep the chart readable, links involving fewer than 10 students are hidden from the display.",
            "Hover reports the source and target stages, the student count, and the percentage of all students covered by that link.",
            "Each link uses the destination-mode color so the landing stage is easier to follow visually.",
            "The figure is rebuilt from the filtered attempt history ordered by `student_attempt_index`, so changing the date or curriculum slice changes the visible paths.",
        ),
    },
    "bottlenecks_transitions_bottleneck_chart": {
        "What it shows": (
            "A ranked horizontal bar chart of modules, objectives, or activities where learners appear to struggle the most in the selected slice.",
            "The chart can switch between module level, objective level, and activity level.",
        ),
        "Metrics": (
            "Failure rate is the share of attempts that were not successful.",
            "Retries before first success is the share of attempts that repeat an exercise while the student has not yet succeeded on that exercise.",
            "Bar length shows failure rate directly, while bar color shows retries before first success.",
            "All repeat attempts are still shown in hover as context, but they are not the retry signal used in the bottleneck score.",
            "The combined bottleneck score gives more weight to failure than to retries before first success: 70% failure rate and 30% retry-before-success rate.",
            "The volume-weighted impact score used in the automatic analysis combines the bottleneck score with the number of attempts.",
        ),
        "Controls that affect it": (
            "The sidebar `Min attempts for bottleneck` filter removes low-volume candidates before the score ranking and volume-weighted impact analysis are computed.",
        ),
    },
    "bottlenecks_transitions_path_chart": {
        "What it shows": (
            "A horizontal bar chart of the most frequent activity-to-activity transitions in the selected slice.",
            "It focuses on cross-objective transitions, so it highlights movement between different parts of the module rather than short local loops inside the same objective.",
        ),
        "Metrics": (
            "Rows are selected from the common cross-objective transitions, using transition count.",
            "Bar length shows `transition_count / attempts in the source objective`, so each row has its own denominator.",
            "Because the denominator changes by row, the displayed percentages should not be expected to sum to 100%.",
            "Transition count is still available in hover as the raw number of observed moves from one activity to the next.",
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
            "`Starting activity step` lets you shift the visible window later in the progression, so the first displayed node can be step 2, step 6, or any later first-time activity rank reached by students.",
            "`Visible activity steps` controls how many newly reached activity stages remain visible after the chosen starting step. Its maximum automatically follows the selected classroom's longest first-time path.",
            "When the activity exists in the canonical catalog, the visible node label uses its code, such as `M1O1A1`; the real activity name stays in hover.",
            "Students who stop inside the visible window go to labels such as `Stopped after 7 activities`, while students who continue beyond the window go to `More than N activities` using the absolute activity count reached so far.",
            "Activity codes stay visible on the nodes, while hover keeps the real activity names; terminal nodes keep their visible labels.",
            "Hover reports the source and target activities, the student count, the share of the selected classroom, and the share of students arriving at the source node.",
            "The figure uses the full time span of the selected classroom in the chosen mode scope; page replay frames are not used here.",
        ),
        "How to use": (
            "Use `Target classroom size (students)` and `Matching classrooms` to keep the current size-based matching workflow.",
            "If you already know a classroom ID, type it in `Classroom ID override (optional)` to load that classroom directly inside the selected work-mode scope.",
            "The typed classroom override supersedes the currently selected matching classroom, but the matching list stays available for browsing nearby classes.",
            "For sources without classroom identifiers, the page collapses to one synthetic classroom containing all students in the selected scope.",
        ),
    },
    "student_elo_page": {
        "What it shows": (
            "A replayable module-local Elo chart for one student inside one selected module, with a selector for the Elo system.",
            "The page first selects an Elo system, then a student, then one of that student's available modules.",
            "The summary row above the chart shows the selected student, module, module-local attempts, final module-local Elo, and first/last timestamps for that module slice.",
        ),
        "Metrics": (
            "The plotted value is post-attempt student Elo after each visible update inside the selected module only.",
            "The hover reports timestamp, module, objective, activity, exercise, work mode, outcome, expected success, fixed exercise difficulty, and the student's Elo before and after that attempt.",
            "Optional dotted vertical markers show large timestamp gaps between consecutive attempts while keeping the x-axis on local attempt ordinal rather than calendar time.",
            "Expected success is computed from the gap between the student's current Elo and the fixed difficulty of the exercise.",
            "Exercise difficulty is fixed offline per module from first attempts only, then both systems reset the student's Elo to 1500 at the first visible attempt of the selected module.",
            "`Sequential Replay Elo` applies one sequential update per attempt, while `Batch Replay Elo` refits the student's level from the full module-local prefix seen so far at each attempt.",
            "Exercises are calibrated by raw module/objective/activity/exercise context rather than bare exercise ID, so reused exercises can carry different difficulty in different module contexts.",
            "If an attempted exercise is outside the mapped catalog, it can still be calibrated and replayed; the page then uses fallback labels such as `Unmapped initial-test activity (M1)` to make that missing context explicit.",
        ),
        "How to use": (
            "Choose the Elo system first, then choose a target attempt count to sample one replay-eligible student using that student's total attempts across modules, or type a student ID directly.",
            "Once the student is fixed, choose one of the modules available for that student; the default is the module with the most attempts.",
            "Use the replay controls to step through the local attempt timeline with a chosen step size and autoplay speed.",
            "Use `Highlight timestamp gaps >= days` to mark long inactivity periods without switching the chart away from attempt-based progression.",
            "If no students are found in the requested attempt range, try another target count, or clear the ID override if it does not match an eligible student.",
        ),
    },
    "m1_individual_path_page": {
        "What it shows": (
            "A replayable Module 1 path for one selected student drawn on top of the fixed M1 dependency layout.",
            "Objective squares stay static, while activity circles start grey and then update as the student's M1 attempts become visible.",
            "The page is fixed to Module 1 and replays all work modes observed inside that module.",
        ),
        "Metrics": (
            "Activity color is the cumulative success rate for that student on that activity up to the current replay frame.",
            "Activity size grows with the cumulative number of attempts on that activity using a bounded square-root scale, so repeated activity exposure is visible without overwhelming the layout.",
            "Only the last three distinct mapped activity-to-activity changes are shown as arrows; repeated attempts inside the same activity do not create extra arrows.",
            "Attempts whose activity is outside the M1 topology are kept in the replay timeline but are excluded from node color/size updates and from arrow drawing.",
        ),
        "How to use": (
            "Choose a target Module 1 attempt count to sample one nearby student, or type a student ID directly.",
            "Use the replay controls to step through the student's Module 1 timeline frame by frame or autoplay it.",
            "Read the graph as a path replay rather than a benchmark chart: grey means unseen so far, warmer colors mean weaker cumulative success, greener colors mean stronger cumulative success.",
        ),
    },
    "student_objective_spider": {
        "What it shows": (
            "A radar chart for one selected student inside one selected module.",
            "Every catalog objective in the selected module remains visible as a spoke, even if the student never reached it.",
            "The chart overlays all-attempt success rate with catalog-relative objective coverage.",
        ),
        "Metrics": (
            "`Success rate` is the share of correct answers across all attempts the student made inside that objective.",
            "`Coverage %` is the share of catalog exercises in the objective that the student attempted at least once; retries do not increase coverage.",
            "Raw counts such as `distinct exercises attempted / total objective exercises` stay visible in hover and summary cards.",
            "Untouched objectives keep `0%` coverage while the success-rate trace leaves a gap instead of inventing a zero score.",
        ),
        "How to use": (
            "Use `Target attempt count` to sample one eligible student near the desired trajectory length.",
            "If you already know the student ID, type it in `Student ID override (optional)` to load that student directly.",
            "After a student is selected, choose one of the modules that student actually attempted; the default module is the student's most-attempted one.",
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

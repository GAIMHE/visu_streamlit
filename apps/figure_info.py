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
    "overview_work_mode_success_table": {
        "What it shows": (
            "A work-mode comparison table for the selected period and curriculum slice.",
            "Each row summarizes one work mode with attempts and two success definitions.",
        ),
        "Why it matters": (
            "It helps compare whether a mode looks strong because of many easy attempts or because students succeed more consistently across exercises.",
        ),
        "Metrics": (
            "Attempts = number of filtered attempts in that work mode.",
            "Success rate (attempt-weighted) = mean of `data_correct` over attempts.",
            "Success rate (exercise-balanced) = mean of per-exercise success rates within the mode.",
        ),
        "Controls that affect it": (
            "Sidebar date, module, objective, and activity filters affect the source rows.",
            "The `Work modes shown` multiselect keeps or removes rows from this table only.",
        ),
        "How to read / interact": (
            "Compare the two success columns together rather than reading only one.",
            "If exercise-balanced success is much lower than attempt-weighted success, success is concentrated on a narrower subset of exercises.",
        ),
    },
    "overview_work_mode_footprint_depth_chart": {
        "What it shows": (
            "A grouped bar chart of curriculum footprint by work mode.",
            "It counts how many modules, objectives, and activities were explored in the current slice.",
        ),
        "Why it matters": (
            "It separates broad exploration from narrow repeated practice.",
        ),
        "Metrics": (
            "Modules explored, objectives explored, and activities explored are distinct counts within each work mode.",
            "Depth is not drawn as bars here; it is summarized in the companion table through median attempts per activity and repeat-attempt rate.",
        ),
        "Controls that affect it": (
            "Sidebar date, module, objective, and activity filters affect the data.",
            "The `Work modes shown` multiselect limits which modes appear.",
        ),
        "How to read / interact": (
            "Higher bars mean broader curriculum coverage, not better performance by themselves.",
            "Read this chart together with the summary table to separate width from repetition.",
        ),
    },
    "overview_work_mode_summary_table": {
        "What it shows": (
            "A wider comparison table that combines usage volume, exploration width, depth, and success by work mode.",
        ),
        "Why it matters": (
            "It gives the most complete descriptive view of how each work mode is used.",
        ),
        "Metrics": (
            "Median attempts per activity describes local practice depth.",
            "Repeat attempt rate = share of attempts with `attempt_number > 1`.",
            "Success columns keep the same attempt-weighted vs exercise-balanced meaning as the success table.",
        ),
        "Controls that affect it": (
            "The same filters and `Work modes shown` multiselect as the other work-mode blocks apply here.",
        ),
        "How to read / interact": (
            "Look for combinations such as high breadth with low depth, or low breadth with high repetition.",
            "Percent values are formatted after aggregation; the underlying computation is unchanged.",
        ),
    },
    "overview_bottleneck_candidates_chart": {
        "What it shows": (
            "A ranked horizontal bar chart of modules, objectives, or activities where learners struggle most.",
        ),
        "Why it matters": (
            "It helps prioritize where to inspect content or pedagogy first.",
        ),
        "Metrics": (
            "Failure rate = `1 - success_rate` from the filtered aggregate rows.",
            "Bottleneck score combines failure rate and repeat-attempt rate, with more weight on failure.",
            "The selected level determines whether rows are rolled up by module, objective, or activity.",
        ),
        "Controls that affect it": (
            "Sidebar date, module, objective, and activity filters affect the chart according to the selected bottleneck level.",
            "The `Bottleneck level`, `Top bottleneck entities`, and `Min attempts for bottleneck` controls directly change this chart.",
        ),
        "How to read / interact": (
            "Longer bars indicate stronger candidate bottlenecks under the current score definition.",
            "Use the hover to compare score, attempts, failure rate, and repeat-attempt rate together.",
        ),
    },
    "overview_path_transitions_chart": {
        "What it shows": (
            "The most frequent cross-objective activity-to-activity transitions in the current slice.",
        ),
        "Why it matters": (
            "It highlights common learning paths and where students tend to go next.",
        ),
        "Metrics": (
            "Transition count = number of observed source-to-destination transitions.",
            "Successful destination attempts = count of destination attempts marked correct after that transition.",
        ),
        "Controls that affect it": (
            "Sidebar date, module, and activity filters affect the transition set.",
            "The `Top transitions` control changes how many edges are kept.",
        ),
        "How to read / interact": (
            "This chart emphasizes common paths, not causal impact.",
            "Use the hover labels to inspect the full source and destination activities when axis labels are shortened.",
        ),
    },
    "overview_data_quality_panel": {
        "What it shows": (
            "The latest consistency status and contract checks produced by the derived-data pipeline.",
        ),
        "Why it matters": (
            "It helps confirm whether the dashboard is reading artifacts that match the expected runtime contract.",
        ),
        "Metrics": (
            "PASS/FAIL comes from the consistency report generated during the build.",
            "When checks fail, the table lists the check name together with expected and observed values.",
        ),
        "Controls that affect it": (
            "No analytical filters affect this panel.",
        ),
        "How to read / interact": (
            "Treat this panel as runtime health information, not as a learning metric.",
            "If it shows failures, rebuild artifacts before trusting downstream figures.",
        ),
    },
    "matrix_objective_activity_heatmap": {
        "What it shows": (
            "A module-level matrix where rows are objectives and columns are local activity positions (`A1..An`).",
            "Each populated cell summarizes one objective/activity pair with the selected metric.",
        ),
        "Why it matters": (
            "It helps compare where strong or weak areas are concentrated inside a module.",
        ),
        "Metrics": (
            "Available metrics include attempts, success rate, exercise-balanced success, repeat-attempt rate, first-attempt success, playlist unique exercises, and activity mean exercise Elo when artifacts support them.",
            "The Elo metric is global difficulty calibration and does not change with the date filter.",
        ),
        "Controls that affect it": (
            "Sidebar module, date range, metric, `Show cell values`, and `Show IDs in hover` affect this heatmap.",
        ),
        "How to read / interact": (
            "Click a populated cell to open the exercise drilldown for that activity.",
            "Blank positions mean there is no activity at that local position for the objective.",
        ),
    },
    "matrix_exercise_drilldown_table": {
        "What it shows": (
            "An exercise-level table for the currently selected matrix cell.",
        ),
        "Why it matters": (
            "It breaks an activity down into its exercises so you can see whether the matrix signal is broad or driven by a few exercises.",
        ),
        "Metrics": (
            "Standard metrics include attempts, success rate, first-attempt success, repeat-attempt rate, and median duration.",
            "For the Elo metric, the table switches to exercise Elo, calibration attempts, and calibration success rate.",
        ),
        "Controls that affect it": (
            "The selected matrix cell determines which activity appears here.",
            "Module, date range, and selected metric also affect the drilldown rows.",
        ),
        "How to read / interact": (
            "Click one row in this table to open the instruction panel below.",
            "If the metric is Elo, the date filter affects the selected activity context but not the Elo values themselves.",
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
    "zpdes_dependency_graph": {
        "What it shows": (
            "A structural dependency graph with objective lanes, activity nodes, and activation/deactivation edges.",
        ),
        "Why it matters": (
            "It shows how content is supposed to unlock and which nodes may play a structural prerequisite role.",
        ),
        "Metrics": (
            "Optional overlays color activity nodes by attempts, success rate, or repeat-attempt rate.",
            "Objective squares stay structural.",
        ),
        "Controls that affect it": (
            "Module, date range, objective selection, overlay choice, curve toggle, and `Show IDs in hover` affect the graph.",
        ),
        "How to read / interact": (
            "Click a node to focus its neighborhood and populate the rule-detail panel.",
            "Solid blue edges are activation rules; dashed red edges are deactivation rules; curved green edges are intra-objective dependencies.",
        ),
    },
    "zpdes_rule_detail_panel": {
        "What it shows": (
            "A node-focused detail view for the current graph selection.",
        ),
        "Why it matters": (
            "It separates the selected node's incoming prerequisite chain from its direct outgoing unlock rules.",
        ),
        "Metrics": (
            "The panel can display overlay attempts, success rate, and repeat-attempt rate for the selected node when available.",
        ),
        "Controls that affect it": (
            "It depends on the current node selected in the dependency graph and on the same module/date/objective scope as the graph.",
        ),
        "How to read / interact": (
            "Use `Clear focus` to return the graph to its unfocused state.",
            "Incoming rows show what must already be unlocked; outgoing rows show what the selected node can unlock directly.",
        ),
    },
    "zpdes_dependency_audit_table": {
        "What it shows": (
            "A full table of the dependency edges currently visible under the graph filters.",
        ),
        "Why it matters": (
            "It gives an audit-friendly view of the graph with enriched edge metadata that is harder to read from the visualization alone.",
        ),
        "Metrics": (
            "This block is structural; it lists edge type, source/target labels and codes, thresholds, and provenance fields.",
        ),
        "Controls that affect it": (
            "The same module, objective, and graph-scope filters as the dependency graph apply here.",
        ),
        "How to read / interact": (
            "Use this table when you need exact node codes or provenance fields rather than a visual overview.",
        ),
    },
    "classroom_progression_replay_heatmap": {
        "What it shows": (
            "An animated student-by-activity matrix for one classroom.",
            "Each cell reflects cumulative success on that activity at the current replay step.",
        ),
        "Why it matters": (
            "It reveals whether a class progresses together or diverges across activities.",
        ),
        "Metrics": (
            "Cell color is cumulative success rate for that student/activity pair at the current frame.",
            "The mastery threshold only changes the color interpretation, not the underlying replay data.",
        ),
        "Controls that affect it": (
            "Classroom, work-mode scope, date range, replay buttons, frame slider, step size, max frames, mastery threshold, and `Show cell values` affect this block.",
        ),
        "How to read / interact": (
            "Use `Play`, `Pause`, `Reset`, and `Step +1` to move through synchronized classroom steps.",
            "The frame caption above the chart tells you how many attempts have already been integrated.",
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
            "Minimum attempts and the student multiselect determine which cards appear.",
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
            "Student selection, minimum attempts, step size, autoplay speed, replay buttons, and frame slider affect this chart.",
        ),
        "How to read / interact": (
            "Use `Play`, `Pause`, `Reset`, and `Step +1` to step through the local attempt timeline.",
            "The frame caption shows the current local attempt cutoff shared across displayed students.",
        ),
    },
    "zpdes_transition_efficiency_graph": {
        "What it shows": (
            "A structural ZPDES layout with hover-based progression cohorts for each activity.",
        ),
        "Why it matters": (
            "It compares how new-exercise first attempts behave for students coming from earlier content, already-later content, or prior work inside the same activity.",
        ),
        "Metrics": (
            "Node color can show selected-work-mode first-attempt success or activity mean exercise Elo.",
            "Hover always shows first-attempt success and event counts for all work modes, plus before/after/in-activity cohort summaries for the selected population.",
            "Each cohort reports success rate, eligible event count, unique-student count, and total previous attempts.",
        ),
        "Controls that affect it": (
            "Module, date range, objective selection, activity-color metric, cohort population, later-attempt threshold, curve toggle, and `Show IDs in hover` affect the graph.",
        ),
        "How to read / interact": (
            "This graph is hover-driven; there is no click selection or arrow overlay in the current design.",
            "The `after` cohort requires at least the configured number of prior later-activity attempts.",
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

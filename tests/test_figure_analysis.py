from __future__ import annotations

import polars as pl

from visu2.figure_analysis import (
    INSUFFICIENT_EVIDENCE_MESSAGE,
    FigureAnalysis,
    analyze_bottleneck_chart,
    analyze_classroom_progression_population,
    analyze_classroom_progression_sankey,
    analyze_m1_individual_path,
    analyze_matrix_drilldown_table,
    analyze_overview_concentration,
    analyze_overview_kpis,
    analyze_student_elo_comparison,
    analyze_student_elo_population,
    analyze_transition_chart,
    analyze_work_mode_summary,
    analyze_work_mode_transitions,
    analyze_zpdes_transition_efficiency,
    analyze_zpdes_transition_population,
    build_discussion_paragraph,
)


def test_analyze_work_mode_summary_requires_attempt_threshold() -> None:
    frame = pl.DataFrame(
        {
            "work_mode": ["zpdes", "playlist"],
            "attempts": [12, 18],
            "success_rate": [0.8, 0.7],
            "exercise_balanced_success_rate": [0.78, 0.68],
        }
    )
    analysis = analyze_work_mode_summary(frame)
    assert analysis.findings == ()
    assert analysis.interpretation is None
    assert INSUFFICIENT_EVIDENCE_MESSAGE in analysis.caveats


def test_analyze_bottleneck_chart_uses_stable_sorting_for_ties() -> None:
    frame = pl.DataFrame(
        {
            "entity_label_raw": ["B", "A"],
            "attempts": [30, 30],
            "failure_rate": [0.5, 0.5],
            "repeat_attempt_rate": [0.2, 0.2],
            "bottleneck_score": [0.44, 0.44],
        }
    )
    analysis = analyze_bottleneck_chart(frame)
    assert analysis.findings
    assert analysis.findings[0].startswith("The strongest visible bottleneck candidate is A")


def test_analyze_transition_chart_requires_transition_threshold() -> None:
    frame = pl.DataFrame(
        {
            "from_activity_label": ["A1"],
            "to_activity_label": ["A2"],
            "transition_count": [19],
            "success_conditioned_count": [10],
        }
    )
    analysis = analyze_transition_chart(frame)
    assert analysis.findings == ()
    assert analysis.interpretation is None
    assert INSUFFICIENT_EVIDENCE_MESSAGE in analysis.caveats


def test_analyze_matrix_drilldown_table_reports_supported_spread() -> None:
    frame = pl.DataFrame(
        {
            "exercise_short_id": ["e1", "e2", "e3"],
            "attempts": [25.0, 20.0, 8.0],
            "success_rate": [0.9, 0.4, 0.2],
            "first_attempt_success_rate": [0.9, 0.4, 0.2],
            "repeat_attempt_rate": [0.1, 0.3, 0.6],
        }
    )
    analysis = analyze_matrix_drilldown_table(
        frame,
        metric="success_rate",
        activity_label="Activity X",
    )
    assert analysis.findings
    assert "e1" in analysis.findings[0]
    assert "e2" in analysis.findings[1]
    assert analysis.interpretation is not None


def test_analyze_zpdes_transition_efficiency_requires_thresholded_cohorts() -> None:
    frame = pl.DataFrame(
        {
            "node_type": ["activity"],
            "label": ["A1"],
            "zpdes_first_attempt_event_count": [10],
            "zpdes_first_attempt_success_rate": [0.4],
            "before_unique_students": [4],
            "before_success_rate": [0.8],
            "after_unique_students": [4],
            "after_event_count": [10],
            "after_success_rate": [0.5],
            "in_activity_unique_students": [4],
            "in_activity_event_count": [10],
            "in_activity_success_rate": [0.6],
        }
    )
    analysis = analyze_zpdes_transition_efficiency(frame, later_attempt_threshold=1)
    assert analysis.findings == ()
    assert analysis.interpretation is None
    assert INSUFFICIENT_EVIDENCE_MESSAGE in analysis.caveats


def test_analyze_zpdes_transition_population_reports_raw_and_normalized_rankings() -> None:
    frame = pl.DataFrame(
        {
            "module_code": ["M1", "M31"],
            "module_label": ["Module 1", "Module 31"],
            "objective_label": ["Objective 1", "Objective 3"],
            "activity_label": ["High-volume bridge", "Sharp failure"],
            "before_event_count": [200, 25],
            "before_unique_students": [30, 8],
            "before_success_rate": [0.60, 0.20],
            "after_event_count": [40, 25],
            "after_unique_students": [10, 8],
            "after_success_rate": [0.62, 0.10],
            "in_activity_event_count": [50, 30],
            "in_activity_unique_students": [12, 8],
            "in_activity_success_rate": [0.70, 0.65],
        }
    )
    analysis = analyze_zpdes_transition_population(frame, later_attempt_threshold=2)
    assert analysis.findings
    assert "Module 1" in analysis.findings[0]
    assert "Module 31" in analysis.findings[1]
    assert any("structural-insufficiency candidate" in finding for finding in analysis.findings)
    assert any("after students with at least 2 later attempts" in finding for finding in analysis.findings)


def test_analyze_classroom_progression_population_uses_mode_scope_frames() -> None:
    profiles = pl.DataFrame(
        {
            "classroom_id": ["C1", "C2", "C3", "C4"],
            "students": [18, 20, 22, 24],
            "activities": [8, 12, 11, 10],
            "attempts": [80, 120, 110, 60],
        }
    )
    activity_summary = pl.DataFrame(
        {
            "activity_label": ["Fractions", "Addition"],
            "classrooms_observed": [6, 6],
            "attempts_total": [140, 90],
            "success_rate": [0.42, 0.78],
            "mean_classroom_success_rate": [0.45, 0.74],
            "median_classroom_success_rate": [0.44, 0.75],
            "weak_classroom_count": [5, 1],
            "weak_classroom_share": [5 / 6, 1 / 6],
        }
    )
    analysis = analyze_classroom_progression_population(
        profiles,
        activity_summary,
        mode_scope_label="Playlist",
    )
    assert analysis.findings
    assert analysis.findings[0].startswith("In Playlist")
    assert any("Fractions" in finding for finding in analysis.findings)
    assert analysis.interpretation is not None


def test_analyze_m1_individual_path_reports_visible_progress_and_unmapped_attempts() -> None:
    payload = {
        "student_ids": ["u1"],
        "frame_cutoffs": [0, 2, 4],
        "series": {
            "u1": {
                "attempt_ordinal": [1, 2, 3, 4],
                "activity_id": ["a1", "a1", "missing", "a2"],
                "activity_label": ["A1", "A1", "Missing", "A2"],
                "outcome": [1.0, 0.0, 1.0, 1.0],
                "is_mapped_activity": [True, True, False, True],
            }
        },
    }

    analysis = analyze_m1_individual_path(payload, frame_idx=2)

    assert analysis.findings
    assert any("u1" in finding for finding in analysis.findings)
    assert any("Mapped activity success" in finding for finding in analysis.findings)
    assert any("outside the M1 topology" in caveat for caveat in analysis.caveats)


def test_analyze_student_elo_population_reports_raw_vs_normalized_severity() -> None:
    activity_summary = pl.DataFrame(
        {
            "module_code": ["M1", "M31"],
            "module_label": ["Module 1", "Module 31"],
            "objective_label": ["Objective 1", "Objective 3"],
            "activity_label": ["Busy activity", "Sharp drop"],
            "event_count": [200, 25],
            "unique_students": [40, 8],
            "mean_delta": [-0.4, -2.0],
            "mean_negative_delta": [-1.2, -2.5],
            "worst_delta": [-8.0, -12.0],
            "total_negative_loss": [140.0, 30.0],
            "mean_abs_delta": [2.2, 2.8],
        }
    )
    region_summary = pl.DataFrame(
        {
            "module_code": ["M1", "M31"],
            "module_label": ["Module 1", "Module 31"],
            "objective_label": ["Objective 1", "Objective 3"],
            "event_count": [200, 25],
            "student_mentions": [40, 8],
            "total_negative_loss": [140.0, 30.0],
            "mean_delta": [-0.4, -2.0],
            "mean_abs_delta": [2.2, 2.8],
            "worst_delta": [-8.0, -12.0],
        }
    )
    eligible_profiles = pl.DataFrame(
        {
            "user_id": [f"u{i}" for i in range(8)],
            "total_attempts": [120, 115, 110, 130, 140, 150, 90, 95],
        }
    )
    analysis = analyze_student_elo_population(activity_summary, region_summary, eligible_profiles)
    assert analysis.findings
    assert "Module 1" in analysis.findings[1]
    assert "Module 31" in analysis.findings[2]
    assert any("Objective 1" in finding or "Objective 3" in finding for finding in analysis.findings)


def test_analyze_student_elo_comparison_reports_selected_and_population_differences() -> None:
    comparison_payload = {
        "student_ids": ["u1"],
        "systems": ("Sequential Replay Elo", "Iterative Elo"),
        "series": {
            "Sequential Replay Elo": {
                "u1": {
                    "attempt_ordinal": [1, 2, 3],
                    "student_elo_post": [1505.0, 1510.0, 1512.0],
                }
            },
            "Iterative Elo": {
                "u1": {
                    "attempt_ordinal": [1, 2, 3],
                    "student_elo_post": [1498.0, 1504.0, 1506.0],
                }
            },
        },
    }
    exercise_comparison = pl.DataFrame(
        {
            "exercise_id": ["e1", "e2", "e3"],
            "exercise_label": ["Exercise 1", "Exercise 2", "Exercise 3"],
            "module_code": ["M1", "M1", "M31"],
            "calibration_attempts": [10, 40, 120],
            "current_exercise_elo": [1490.0, 1510.0, 1540.0],
            "iterative_exercise_elo": [1460.0, 1500.0, 1560.0],
            "calibrated": [True, True, True],
            "elo_diff": [-30.0, -10.0, 20.0],
            "abs_elo_diff": [30.0, 10.0, 20.0],
        }
    )
    eligible_profiles = pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "eligible_for_replay": [True, True, True],
            "total_attempts": [100, 120, 140],
        }
    )
    analysis = analyze_student_elo_comparison(
        comparison_payload,
        exercise_comparison,
        eligible_profiles,
    )
    assert analysis.findings
    assert "final gap" in analysis.findings[1]
    assert any("correlation" in finding for finding in analysis.findings)
    assert any("Largest exercise-level shifts" in finding for finding in analysis.findings)


def test_analyze_overview_kpis_can_include_source_retry_summary() -> None:
    analysis = analyze_overview_kpis(
        attempts=1000,
        unique_students=100,
        unique_exercises=50,
        retry_attempt_rate=0.131,
        retry_after_success_share=0.67,
        retry_after_failure_share=0.33,
    )
    assert any("13.1%" in finding for finding in analysis.findings)
    assert any("67.0%" in finding and "33.0%" in finding for finding in analysis.findings)


def test_build_discussion_paragraph_uses_interpretation_and_findings() -> None:
    analysis = FigureAnalysis(
        findings=(
            "Mode A has the highest success rate.",
            "Mode B has the largest weighted-balanced gap.",
        ),
        interpretation="The visible modes look uneven in both performance level and exercise mix.",
    )
    paragraph = build_discussion_paragraph(analysis)
    assert paragraph is not None
    assert paragraph.startswith("Taken together, these results suggest")
    assert "Mode A has the highest success rate".lower() in paragraph.lower()
    assert "Mode B has the largest weighted-balanced gap".lower() in paragraph.lower()


def test_build_discussion_paragraph_prefers_explicit_discussion() -> None:
    analysis = FigureAnalysis(
        findings=("A finding.",),
        interpretation="An interpretation.",
        discussion="This is the final article-style paragraph.",
    )
    paragraph = build_discussion_paragraph(analysis)
    assert paragraph == "This is the final article-style paragraph."


def test_build_discussion_paragraph_can_be_suppressed_explicitly() -> None:
    analysis = FigureAnalysis(
        findings=("A finding.",),
        interpretation="An interpretation.",
        discussion="",
    )
    paragraph = build_discussion_paragraph(analysis)
    assert paragraph is None


def test_analyze_overview_concentration_summarizes_top_buckets() -> None:
    entity_summary = pl.DataFrame(
        {
            "label": ["A", "B", "C", "D"],
            "id": ["A", "B", "C", "D"],
            "attempts": [50, 30, 15, 5],
            "attempt_share": [0.50, 0.30, 0.15, 0.05],
            "contained_exercises": [1, 1, 1, 1],
            "bucket_key": ["decile_1", "decile_2", "decile_5", "decile_10"],
            "bucket_label": ["Top 10%", "10-20%", "40-50%", "90-100%"],
            "bucket_order": [1, 2, 5, 10],
        }
    )
    bucket_summary = pl.DataFrame(
        {
            "bucket_key": [f"decile_{idx}" for idx in range(1, 11)],
            "bucket_label": ["Top 10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"],
            "bucket_order": list(range(1, 11)),
            "attempts": [50, 30, 0, 0, 15, 0, 0, 0, 0, 5],
            "attempt_share": [0.50, 0.30, 0.0, 0.0, 0.15, 0.0, 0.0, 0.0, 0.0, 0.05],
            "entity_count": [1, 1, 0, 0, 1, 0, 0, 0, 0, 1],
        }
    )
    analysis = analyze_overview_concentration(entity_summary, bucket_summary, level_label="Exercise")
    assert analysis.findings
    assert "top 10% bucket covers 50.0%" in analysis.findings[0].lower()
    assert any("Bucket concentration ranking" in finding for finding in analysis.findings)
    assert analysis.discussion == ""


def test_analyze_overview_concentration_supports_global_student_view() -> None:
    student_summary = pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3", "u4"],
            "attempts": [50, 30, 15, 5],
            "attempt_share": [0.50, 0.30, 0.15, 0.05],
            "bucket_key": ["decile_1", "decile_2", "decile_5", "decile_10"],
            "bucket_label": ["Top 10%", "10-20%", "40-50%", "90-100%"],
            "bucket_order": [1, 2, 5, 10],
        }
    )
    bucket_summary = pl.DataFrame(
        {
            "bucket_key": [f"decile_{idx}" for idx in range(1, 11)],
            "bucket_label": ["Top 10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"],
            "bucket_order": list(range(1, 11)),
            "attempts": [50, 30, 0, 0, 15, 0, 0, 0, 0, 5],
            "attempt_share": [0.50, 0.30, 0.0, 0.0, 0.15, 0.0, 0.0, 0.0, 0.0, 0.05],
            "entity_count": [1, 1, 0, 0, 1, 0, 0, 0, 0, 1],
        }
    )
    analysis = analyze_overview_concentration(
        student_summary,
        bucket_summary,
        level_label="All attempts",
        basis_label="Student concentration",
        student_scope_label="All attempts",
    )
    assert analysis.findings
    assert analysis.findings[0].startswith("The top 10% of students contribute 50.0%")
    assert any("Most active students" in finding for finding in analysis.findings)


def test_analyze_overview_concentration_supports_within_entity_student_view() -> None:
    entity_summary = pl.DataFrame(
        {
            "label": ["Activity A", "Activity B"],
            "id": ["A", "B"],
            "attempts": [60, 40],
            "selected_bucket_attempt_share": [0.70, 0.45],
            "top_10_students_share": [0.70, 0.45],
            "unique_students": [8, 6],
            "contained_exercises": [3, 2],
            "bucket_key": ["decile_1", "decile_1"],
            "bucket_label": ["Top 10%", "Top 10%"],
            "bucket_order": [1, 1],
        }
    )
    bucket_summary = pl.DataFrame(
        {
            "bucket_key": [f"decile_{idx}" for idx in range(1, 11)],
            "bucket_label": ["Top 10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"],
            "bucket_order": list(range(1, 11)),
            "attempts": [60, 20, 10, 5, 5, 0, 0, 0, 0, 0],
            "attempt_share": [0.60, 0.20, 0.10, 0.05, 0.05, 0.0, 0.0, 0.0, 0.0, 0.0],
            "entity_count": [2, 2, 2, 1, 1, 0, 0, 0, 0, 0],
        }
    )
    analysis = analyze_overview_concentration(
        entity_summary,
        bucket_summary,
        level_label="Activity",
        basis_label="Student concentration",
        student_scope_label="Activity",
    )
    assert analysis.findings
    assert any("median activity gets 57.5%" in finding.lower() for finding in analysis.findings)
    assert any("Most student-concentrated activities" in finding for finding in analysis.findings)


def test_analyze_work_mode_transitions_summarizes_global_paths() -> None:
    paths = pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3", "u4", "u5", "u6"] + [f"ux{idx}" for idx in range(30)],
            "first_work_mode": ["zpdes", "zpdes", "playlist", "initial-test", "initial-test", "playlist"]
            + ["initial-test"] * 30,
            "transition_count_total": [0, 1, 2, 4, 1, 0] + [1] * 30,
            "transition_1_mode": [None, "playlist", "adaptive-test", "zpdes", "zpdes", None] + ["zpdes"] * 30,
            "transition_2_mode": [None, None, "playlist", "playlist", None, None] + [None] * 30,
            "transition_3_mode": [None, None, None, "adaptive-test", None, None] + [None] * 30,
            "continues_after_transition_3": [False, False, False, True, False, False] + [False] * 30,
        }
    )
    analysis = analyze_work_mode_transitions(paths)
    assert analysis.findings
    assert analysis.findings[0].startswith("Students who never change work mode represent")
    assert any("Initial mode distribution" in finding for finding in analysis.findings)
    assert any("Most common first transitions" in finding for finding in analysis.findings)
    assert any("Most common displayed paths" in finding for finding in analysis.findings)
    assert analysis.interpretation is not None


def test_analyze_classroom_progression_sankey_summarizes_selected_classroom() -> None:
    payload = {
        "student_count": 6,
        "activity_ids": ["a1", "a2", "a3", "a4"],
        "activity_full_labels": ["Intro", "Practice", "Bridge", "Wrap"],
        "student_paths": [
            {
                "user_id": "u1",
                "activity_ids": ["a1", "a2", "a3"],
                "activity_full_labels": ["Intro", "Practice", "Bridge"],
                "activity_display_labels": ["Intro", "Practice", "Bridge"],
                "path_length": 3,
            },
            {
                "user_id": "u2",
                "activity_ids": ["a1", "a2", "a3"],
                "activity_full_labels": ["Intro", "Practice", "Bridge"],
                "activity_display_labels": ["Intro", "Practice", "Bridge"],
                "path_length": 3,
            },
            {
                "user_id": "u3",
                "activity_ids": ["a1", "a2", "a4"],
                "activity_full_labels": ["Intro", "Practice", "Wrap"],
                "activity_display_labels": ["Intro", "Practice", "Wrap"],
                "path_length": 3,
            },
            {
                "user_id": "u4",
                "activity_ids": ["a1"],
                "activity_full_labels": ["Intro"],
                "activity_display_labels": ["Intro"],
                "path_length": 1,
            },
            {
                "user_id": "u5",
                "activity_ids": ["a2", "a3", "a4", "a1"],
                "activity_full_labels": ["Practice", "Bridge", "Wrap", "Intro"],
                "activity_display_labels": ["Practice", "Bridge", "Wrap", "Intro"],
                "path_length": 4,
            },
            {
                "user_id": "u6",
                "activity_ids": ["a1", "a2", "a3", "a4"],
                "activity_full_labels": ["Intro", "Practice", "Bridge", "Wrap"],
                "activity_display_labels": ["Intro", "Practice", "Bridge", "Wrap"],
                "path_length": 4,
            },
        ],
    }
    analysis = analyze_classroom_progression_sankey(payload, visible_steps=3)
    assert analysis.findings
    assert analysis.findings[0].startswith("The selected classroom includes 6 students overall; 6 reach step 1")
    assert any("The most common visible entry activity is Intro" in finding for finding in analysis.findings)
    assert any("Most common first transitions" in finding for finding in analysis.findings)
    assert any("Most common visible paths" in finding for finding in analysis.findings)
    assert analysis.interpretation is not None


def test_analyze_classroom_progression_sankey_can_focus_on_later_window() -> None:
    payload = {
        "student_count": 4,
        "activity_ids": ["a1", "a2", "a3", "a4"],
        "activity_full_labels": ["Intro", "Practice", "Bridge", "Wrap"],
        "student_paths": [
            {
                "user_id": "u1",
                "activity_ids": ["a1", "a2", "a3"],
                "activity_full_labels": ["Intro", "Practice", "Bridge"],
                "activity_display_labels": ["Intro", "Practice", "Bridge"],
                "path_length": 3,
            },
            {
                "user_id": "u2",
                "activity_ids": ["a1", "a2", "a4"],
                "activity_full_labels": ["Intro", "Practice", "Wrap"],
                "activity_display_labels": ["Intro", "Practice", "Wrap"],
                "path_length": 3,
            },
            {
                "user_id": "u3",
                "activity_ids": ["a1"],
                "activity_full_labels": ["Intro"],
                "activity_display_labels": ["Intro"],
                "path_length": 1,
            },
            {
                "user_id": "u4",
                "activity_ids": ["a1", "a3", "a4", "a2"],
                "activity_full_labels": ["Intro", "Bridge", "Wrap", "Practice"],
                "activity_display_labels": ["Intro", "Bridge", "Wrap", "Practice"],
                "path_length": 4,
            },
        ],
    }
    analysis = analyze_classroom_progression_sankey(payload, visible_steps=2, start_step=2)
    assert analysis.findings
    assert analysis.findings[0].startswith("The selected classroom includes 4 students overall; 3 reach step 2")
    assert any("step 2" in finding for finding in analysis.findings)
    assert any("More than 3 activities" in finding for finding in analysis.findings)

"""
test_student_elo_page_logic.py

Validate student Elo page selection defaults and replay payload stepping.

Dependencies
------------
- datetime
- polars
- visu2

Classes
-------
- None.

Functions
---------
- _profiles: Utility for profiles.
- _events: Utility for events.
- test_select_default_students_uses_percentile_defaults: Test scenario for select default students uses percentile defaults.
- test_select_default_students_keeps_single_eligible_student: Test scenario for select default students keeps single eligible student.
- test_build_student_elo_payload_respects_step_size_and_final_point: Test scenario for build student elo payload respects step size and final point.
- test_build_student_elo_payload_keeps_single_student_valid: Test scenario for build student elo payload keeps single student valid.
- test_build_student_elo_figure_uses_synchronized_cutoff: Test scenario for build student elo figure uses synchronized cutoff.
"""
from __future__ import annotations

from datetime import datetime

import polars as pl

from visu2.student_elo import (
    build_student_elo_figure,
    build_student_elo_payload,
    select_default_students,
    select_students_near_attempt_target,
)


def _profiles() -> pl.DataFrame:
    """Profiles.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "user_id": ["u2", "u1", "u3"],
            "total_attempts": [120, 240, 40],
            "first_attempt_at": [
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 8, 0, 0),
            ],
            "last_attempt_at": [
                datetime(2025, 1, 2, 10, 0, 0),
                datetime(2025, 1, 3, 9, 0, 0),
                datetime(2025, 1, 1, 8, 30, 0),
            ],
            "unique_modules": [2, 3, 1],
            "unique_objectives": [3, 5, 1],
            "unique_activities": [10, 15, 2],
            "final_student_elo": [1510.0, 1580.0, 1490.0],
            "eligible_for_replay": [True, True, True],
        }
    )


def _events() -> pl.DataFrame:
    """Events.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u2", "u2"],
            "attempt_ordinal": [1, 2, 3, 1, 2],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 9, 5, 0),
                datetime(2025, 1, 1, 9, 10, 0),
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 5, 0),
            ],
            "date_utc": [datetime(2025, 1, 1).date()] * 5,
            "work_mode": ["zpdes", "zpdes", "zpdes", "playlist", "playlist"],
            "module_code": ["M1", "M1", "M1", "M1", "M1"],
            "objective_id": ["o1", "o1", "o1", "o2", "o2"],
            "activity_id": ["a1", "a1", "a2", "a3", "a3"],
            "exercise_id": ["e1", "e2", "e3", "e4", "e5"],
            "outcome": [1.0, 0.0, 1.0, 1.0, 0.0],
            "expected_success": [0.5, 0.4, 0.6, 0.5, 0.5],
            "exercise_elo": [1500.0, 1520.0, 1490.0, 1510.0, 1510.0],
            "student_elo_pre": [1500.0, 1512.0, 1502.4, 1500.0, 1512.0],
            "student_elo_post": [1512.0, 1502.4, 1512.0, 1512.0, 1500.0],
        }
    )


def test_select_default_students_uses_percentile_defaults() -> None:
    """Test select default students uses percentile defaults.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    selected = select_default_students(_profiles(), min_attempts=100, max_students=2)
    assert selected == ["u1", "u2"]


def test_select_default_students_keeps_single_eligible_student() -> None:
    """Test select default students keeps single eligible student.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    selected = select_default_students(_profiles(), min_attempts=200, max_students=2)
    assert selected == ["u1"]


def test_select_students_near_attempt_target_samples_within_band() -> None:
    """Test attempt-target sampling returns only students inside the requested band."""
    selected = select_students_near_attempt_target(
        _profiles(),
        target_attempts=110,
        tolerance_ratio=0.10,
        max_students=2,
        seed=7,
    )
    assert selected == ["u2"]


def _label_lookup() -> pl.DataFrame:
    """Return a minimal readable label lookup for hover enrichment."""
    return pl.DataFrame(
        {
            "activity_id": ["a1", "a2", "a3"],
            "module_code": ["M1", "M1", "M1"],
            "module_label": ["Numbers", "Numbers", "Numbers"],
            "objective_id": ["o1", "o1", "o2"],
            "objective_label": ["Counting", "Counting", "Comparisons"],
            "activity_label": ["Activity One", "Activity Two", "Activity Three"],
        }
    )


def test_select_students_near_attempt_target_can_return_two_random_students() -> None:
    """Test attempt-target sampling returns two students when the band is wide enough."""
    selected = select_students_near_attempt_target(
        _profiles(),
        target_attempts=120,
        tolerance_ratio=1.0,
        max_students=2,
        seed=3,
    )
    assert len(selected) == 2
    assert set(selected).issubset({"u1", "u2", "u3"})


def test_build_student_elo_payload_respects_step_size_and_final_point() -> None:
    """Test build student elo payload respects step size and final point.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    payload = build_student_elo_payload(_events(), ["u1", "u2"], step_size=2)
    assert payload["frame_cutoffs"] == [0, 2, 3]
    assert payload["max_attempts"] == 3


def test_build_student_elo_payload_keeps_single_student_valid() -> None:
    """Test build student elo payload keeps single student valid.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    payload = build_student_elo_payload(_events(), ["u2"], step_size=10)
    assert payload["student_ids"] == ["u2"]
    assert payload["frame_cutoffs"] == [0, 2]


def test_build_student_elo_payload_backfills_readable_labels() -> None:
    """Test payload enrichment uses catalog-backed readable labels."""
    payload = build_student_elo_payload(
        _events(),
        ["u1"],
        step_size=10,
        label_lookup=_label_lookup(),
    )
    series = payload["series"]["u1"]
    assert series["activity_label"][:2] == ["Activity One", "Activity One"]
    assert series["objective_label"][:2] == ["Counting", "Counting"]
    assert series["module_label"][:2] == ["Numbers", "Numbers"]


def test_build_student_elo_figure_uses_synchronized_cutoff() -> None:
    """Test build student elo figure uses synchronized cutoff.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    payload = build_student_elo_payload(_events(), ["u1", "u2"], step_size=2)
    figure = build_student_elo_figure(payload, frame_idx=1)

    assert len(figure.data) == 2
    trace_lengths = [len(trace.x) for trace in figure.data]
    assert trace_lengths == [2, 2]


def test_build_student_elo_figure_hovertemplate_mentions_objective_and_module() -> None:
    """Test hover template exposes readable activity context fields."""
    payload = build_student_elo_payload(
        _events(),
        ["u1"],
        step_size=2,
        label_lookup=_label_lookup(),
    )
    figure = build_student_elo_figure(payload, frame_idx=1)
    hovertemplate = figure.data[0].hovertemplate
    assert "<b>Objective</b>" in hovertemplate
    assert "<b>Module</b>" in hovertemplate

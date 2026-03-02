from __future__ import annotations

from datetime import datetime

import polars as pl

from visu2.student_elo import (
    build_student_elo_figure,
    build_student_elo_payload,
    select_default_students,
)


def _profiles() -> pl.DataFrame:
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
    selected = select_default_students(_profiles(), min_attempts=100, max_students=2)
    assert selected == ["u1", "u2"]


def test_select_default_students_keeps_single_eligible_student() -> None:
    selected = select_default_students(_profiles(), min_attempts=200, max_students=2)
    assert selected == ["u1"]


def test_build_student_elo_payload_respects_step_size_and_final_point() -> None:
    payload = build_student_elo_payload(_events(), ["u1", "u2"], step_size=2)
    assert payload["frame_cutoffs"] == [0, 2, 3]
    assert payload["max_attempts"] == 3


def test_build_student_elo_payload_keeps_single_student_valid() -> None:
    payload = build_student_elo_payload(_events(), ["u2"], step_size=10)
    assert payload["student_ids"] == ["u2"]
    assert payload["frame_cutoffs"] == [0, 2]


def test_build_student_elo_figure_uses_synchronized_cutoff() -> None:
    payload = build_student_elo_payload(_events(), ["u1", "u2"], step_size=2)
    figure = build_student_elo_figure(payload, frame_idx=1)

    assert len(figure.data) == 2
    trace_lengths = [len(trace.x) for trace in figure.data]
    assert trace_lengths == [2, 2]

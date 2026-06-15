from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from visu2.adaptive_test_elo_export import build_student_module_adaptive_test_elo


def _dt(hour: int) -> datetime:
    return datetime(2026, 1, 1, hour, tzinfo=UTC)


def _write_fact(path: Path) -> None:
    rows = [
        {
            "user_id": "u1",
            "module_id": "module-1",
            "module_code": "M1",
            "module_label": "Module 1",
            "objective_id": "O1",
            "activity_id": "A1",
            "exercise_id": "e1",
            "created_at": _dt(8),
            "work_mode": "adaptive-test",
            "data_correct": True,
        },
        {
            "user_id": "u1",
            "module_id": "module-1",
            "module_code": "M1",
            "module_label": "Module 1",
            "objective_id": "O1",
            "activity_id": "A1",
            "exercise_id": "e2",
            "created_at": _dt(9),
            "work_mode": "adaptive-test",
            "data_correct": False,
        },
        {
            "user_id": "u1",
            "module_id": "module-1",
            "module_code": "M1",
            "module_label": "Module 1",
            "objective_id": "O1",
            "activity_id": "A1",
            "exercise_id": "e1",
            "created_at": _dt(10),
            "work_mode": "zpdes",
            "data_correct": True,
        },
        {
            "user_id": "u1",
            "module_id": "module-1",
            "module_code": "M1",
            "module_label": "Module 1",
            "objective_id": "O1",
            "activity_id": "A1",
            "exercise_id": "e2",
            "created_at": _dt(11),
            "work_mode": "adaptive-test",
            "data_correct": True,
        },
        {
            "user_id": "u2",
            "module_id": "module-1",
            "module_code": "M1",
            "module_label": "Module 1",
            "objective_id": "O1",
            "activity_id": "A1",
            "exercise_id": "e1",
            "created_at": _dt(8),
            "work_mode": "adaptive-test",
            "data_correct": True,
        },
        {
            "user_id": "u2",
            "module_id": "module-1",
            "module_code": "M1",
            "module_label": "Module 1",
            "objective_id": "O1",
            "activity_id": "A1",
            "exercise_id": "e2",
            "created_at": _dt(9),
            "work_mode": "adaptive-test",
            "data_correct": True,
        },
    ]
    pl.DataFrame(rows).write_parquet(path)


def _write_exercise_elo(path: Path) -> None:
    pl.DataFrame(
        {
            "module_code": ["M1", "M1"],
            "objective_id": ["O1", "O1"],
            "activity_id": ["A1", "A1"],
            "exercise_id": ["e1", "e2"],
            "exercise_elo": [1500.0, 1600.0],
            "calibrated": [True, True],
        }
    ).write_parquet(path)


def test_build_student_module_adaptive_test_elo_uses_all_adaptive_rows(tmp_path: Path) -> None:
    fact_path = tmp_path / "fact_attempt_core.parquet"
    exercise_elo_path = tmp_path / "agg_exercise_elo.parquet"
    _write_fact(fact_path)
    _write_exercise_elo(exercise_elo_path)

    frame = build_student_module_adaptive_test_elo(fact_path, exercise_elo_path)
    rows = {row["user_id"]: row for row in frame.to_dicts()}

    assert frame.height == 2
    assert rows["u1"]["adaptive_test_attempts"] == 3
    assert rows["u1"]["adaptive_test_elo_attempts"] == 3
    assert rows["u1"]["has_same_module_practice"] is True
    assert rows["u1"]["all_adaptive_test_attempts_before_first_practice"] is False
    assert rows["u1"]["any_adaptive_test_attempt_after_first_practice"] is True
    assert rows["u1"]["has_adaptive_test_elo"] is True

    assert rows["u2"]["adaptive_test_attempts"] == 2
    assert rows["u2"]["has_same_module_practice"] is False
    assert rows["u2"]["has_adaptive_test_elo"] is True
    assert float(rows["u2"]["adaptive_test_elo"]) > 1500.0


def test_build_student_module_adaptive_test_elo_can_keep_only_before_practice(
    tmp_path: Path,
) -> None:
    fact_path = tmp_path / "fact_attempt_core.parquet"
    exercise_elo_path = tmp_path / "agg_exercise_elo.parquet"
    _write_fact(fact_path)
    _write_exercise_elo(exercise_elo_path)

    frame = build_student_module_adaptive_test_elo(
        fact_path,
        exercise_elo_path,
        only_before_practice=True,
    )
    rows = {row["user_id"]: row for row in frame.to_dicts()}

    assert rows["u1"]["adaptive_test_attempts"] == 2
    assert rows["u1"]["all_adaptive_test_attempts_before_first_practice"] is True
    assert rows["u1"]["any_adaptive_test_attempt_after_first_practice"] is False
    assert rows["u2"]["adaptive_test_attempts"] == 2

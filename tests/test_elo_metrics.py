"""
test_elo_metrics.py

Validate Elo calibration, aggregation, and replay formulas.

Dependencies
------------
- datetime
- json
- pathlib
- polars
- visu2

Classes
-------
- None.

Functions
---------
- _build_settings: Utility for build settings.
- _fact: Utility for fact.
- test_stage_a_single_correct_answer_updates_exercise_elo_symmetrically: Test scenario for stage a single correct answer updates exercise elo symmetrically.
- test_activity_elo_excludes_uncalibrated_exercises: Test scenario for activity elo excludes uncalibrated exercises.
- test_stage_b_student_replay_updates_ordinal_and_rating: Test scenario for stage b student replay updates ordinal and rating.
- test_student_profiles_aggregate_from_events: Test scenario for student profiles aggregate from events.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl

from visu2.config import Settings
from visu2.derive import (
    build_agg_activity_elo_from_exercise_elo,
    build_agg_exercise_elo_from_fact,
    build_agg_exercise_elo_iterative_from_fact,
    build_student_elo_events_from_fact,
    build_student_elo_events_iterative_from_fact,
    build_student_elo_profiles_from_events,
)


def _build_settings(tmp_path: Path) -> Settings:
    """Build settings.

Parameters
----------
tmp_path : Path
        Input parameter used by this routine.

Returns
-------
Settings
        Result produced by this routine.

"""
    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"
    resources_dir = tmp_path / "ressources"
    for path in (data_dir, derived_dir, reports_dir, resources_dir):
        path.mkdir(parents=True, exist_ok=True)

    catalog = {
        "meta": {},
        "id_label_index": {},
        "conflicts": {},
        "orphans": [],
        "exercise_to_hierarchy": {
            "e1": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"},
            "e2": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"},
        },
        "modules": [
            {
                "id": "m1",
                "code": "M1",
                "title": {"short": "Module 1", "long": "Module 1"},
                "objectives": [
                    {
                        "id": "o1",
                        "code": "M1O1",
                        "title": {"short": "Objective 1", "long": "Objective 1"},
                        "activities": [
                            {
                                "id": "a1",
                                "code": "M1O1A1",
                                "title": {"short": "Activity 1", "long": "Activity 1"},
                                "exercise_ids": ["e1", "e2"],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    exercises = {
        "exercises": [
            {"id": "e1", "instruction": "Exercise 1", "type": "MCQ"},
            {"id": "e2", "instruction": "Exercise 2", "type": "MCQ"},
        ]
    }

    (data_dir / "learning_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    (data_dir / "exercises.json").write_text(json.dumps(exercises), encoding="utf-8")
    (data_dir / "zpdes_rules.json").write_text(json.dumps({}), encoding="utf-8")

    return Settings(
        root_dir=tmp_path,
        data_dir=data_dir,
        resources_dir=resources_dir,
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def _fact(rows: list[dict[str, object]]) -> pl.DataFrame:
    """Fact.

Parameters
----------
rows : list[dict[str, object]]
        Input parameter used by this routine.

Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(rows)


def test_stage_a_single_correct_answer_updates_exercise_elo_symmetrically(tmp_path: Path) -> None:
    """Test stage a single correct answer updates exercise elo symmetrically.

Parameters
----------
tmp_path : Path
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            }
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    row = exercise_elo.filter(pl.col("exercise_id") == "e1").to_dicts()[0]

    assert row["calibrated"] is True
    assert row["calibration_attempts"] == 1
    assert abs(float(row["exercise_elo"]) - 1500.0) < 1e-9


def test_activity_elo_excludes_uncalibrated_exercises(tmp_path: Path) -> None:
    """Test activity elo excludes uncalibrated exercises.

Parameters
----------
tmp_path : Path
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            }
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    activity_elo = build_agg_activity_elo_from_exercise_elo(exercise_elo, settings=settings)
    row = activity_elo.to_dicts()[0]

    assert row["catalog_exercise_count"] == 2
    assert row["calibrated_exercise_count"] == 1
    assert abs(float(row["activity_mean_exercise_elo"]) - 1500.0) < 1e-9
    assert abs(float(row["calibration_coverage_ratio"]) - 0.5) < 1e-9


def test_stage_b_student_replay_updates_ordinal_and_rating(tmp_path: Path) -> None:
    """Test stage b student replay updates ordinal and rating.

Parameters
----------
tmp_path : Path
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "seed",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 2,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    events = build_student_elo_events_from_fact(fact, exercise_elo)
    u1_events = events.filter(pl.col("user_id") == "u1").sort("attempt_ordinal")

    assert u1_events.height == 2
    assert u1_events["attempt_ordinal"].to_list() == [1, 2]

    rows = u1_events.to_dicts()
    for row in rows:
        expected_post = float(row["student_elo_pre"]) + 24.0 * (
            float(row["outcome"]) - float(row["expected_success"])
        )
        assert abs(float(row["student_elo_post"]) - expected_post) < 1e-9


def test_student_profiles_aggregate_from_events(tmp_path: Path) -> None:
    """Test student profiles aggregate from events.

Parameters
----------
tmp_path : Path
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 2,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    events = build_student_elo_events_from_fact(fact, exercise_elo)
    profiles = build_student_elo_profiles_from_events(events)
    row = profiles.to_dicts()[0]

    assert row["user_id"] == "u1"
    assert row["module_code"] == "M1"
    assert row["module_label"] == "Module 1"
    assert row["total_attempts"] == 2
    assert row["unique_modules"] == 1
    assert row["unique_objectives"] == 1
    assert row["unique_activities"] == 1
    assert row["eligible_for_replay"] is True


def test_current_exercise_elo_keeps_reused_exercise_separate_across_modules(tmp_path: Path) -> None:
    """Test Current Elo calibrates the same exercise ID independently per module context."""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "shared",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u2",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "shared",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 10, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "anchor_m1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 15, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u2",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "anchor_m1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u3",
                "classroom_id": "c1",
                "playlist_or_module_id": "p2",
                "objective_id": "o2",
                "objective_label": "Objective 2",
                "activity_id": "a2",
                "activity_label": "Activity 2",
                "exercise_id": "shared",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m2",
                "module_code": "M2",
                "module_label": "Module 2",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u4",
                "classroom_id": "c1",
                "playlist_or_module_id": "p2",
                "objective_id": "o2",
                "objective_label": "Objective 2",
                "activity_id": "a2",
                "activity_label": "Activity 2",
                "exercise_id": "shared",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m2",
                "module_code": "M2",
                "module_label": "Module 2",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 10, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u3",
                "classroom_id": "c1",
                "playlist_or_module_id": "p2",
                "objective_id": "o2",
                "objective_label": "Objective 2",
                "activity_id": "a2",
                "activity_label": "Activity 2",
                "exercise_id": "anchor_m2",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m2",
                "module_code": "M2",
                "module_label": "Module 2",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 15, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u4",
                "classroom_id": "c1",
                "playlist_or_module_id": "p2",
                "objective_id": "o2",
                "objective_label": "Objective 2",
                "activity_id": "a2",
                "activity_label": "Activity 2",
                "exercise_id": "anchor_m2",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m2",
                "module_code": "M2",
                "module_label": "Module 2",
            },
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    shared_rows = exercise_elo.filter(pl.col("exercise_id") == "shared").sort("module_code")

    assert shared_rows.height == 2
    assert shared_rows["module_code"].to_list() == ["M1", "M2"]
    assert shared_rows["exercise_elo"].to_list()[0] != shared_rows["exercise_elo"].to_list()[1]


def test_current_student_elo_resets_when_module_changes(tmp_path: Path) -> None:
    """Test Current Elo replay restarts attempt ordinal and Elo at each module boundary."""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 2,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p2",
                "objective_id": "o2",
                "objective_label": "Objective 2",
                "activity_id": "a2",
                "activity_label": "Activity 2",
                "exercise_id": "e3",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "adaptive-test",
                "attempt_number": 1,
                "module_id": "m2",
                "module_code": "M2",
                "module_label": "Module 2",
            },
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    events = build_student_elo_events_from_fact(fact, exercise_elo)
    grouped = events.group_by("module_code").agg(
        pl.col("attempt_ordinal").alias("ordinals"),
        pl.col("student_elo_pre").alias("pre_elo"),
    )
    rows = {str(row["module_code"]): row for row in grouped.to_dicts()}

    assert rows["M1"]["ordinals"] == [1, 2]
    assert rows["M2"]["ordinals"] == [1]
    assert abs(float(rows["M2"]["pre_elo"][0]) - 1500.0) < 1e-9


def test_orphan_exercises_are_calibrated_and_replayed_with_fallback_context(
    tmp_path: Path,
) -> None:
    """Test orphan exercises still receive Elo and remain visible in replay."""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "seed",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u_orphan",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "orphan_o",
                "objective_label": None,
                "activity_id": "orphan_a",
                "activity_label": None,
                "exercise_id": "e_orphan",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "initial-test",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 10, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u_orphan",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "orphan_o",
                "objective_label": None,
                "activity_id": "orphan_a",
                "activity_label": None,
                "exercise_id": "e_orphan",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "initial-test",
                "attempt_number": 2,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
        ]
    )

    exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    orphan_row = exercise_elo.filter(pl.col("exercise_id") == "e_orphan").to_dicts()[0]

    assert orphan_row["calibrated"] is True
    assert orphan_row["calibration_attempts"] == 1
    assert orphan_row["module_code"] == "M1"
    assert orphan_row["objective_id"] == "orphan_o"
    assert orphan_row["activity_id"] == "orphan_a"
    assert orphan_row["objective_label"] == "Unmapped initial-test objective (M1)"
    assert orphan_row["activity_label"] == "Unmapped initial-test activity (M1)"

    events = build_student_elo_events_from_fact(fact, exercise_elo)
    orphan_events = events.filter(pl.col("exercise_id") == "e_orphan").sort("attempt_ordinal")

    assert orphan_events.height == 2
    assert orphan_events["user_id"].to_list() == ["u_orphan", "u_orphan"]
    assert orphan_events["activity_id"].to_list() == ["orphan_a", "orphan_a"]
    assert orphan_events["objective_id"].to_list() == ["orphan_o", "orphan_o"]
    assert orphan_events["work_mode"].to_list() == ["initial-test", "initial-test"]


def test_iterative_exercise_elo_shrinks_sparse_items_toward_global_mean(tmp_path: Path) -> None:
    """Test iterative initialization keeps sparse items closer to the global center."""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u2",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e2",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 10, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u3",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e2",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
        ]
    )

    iterative = build_agg_exercise_elo_iterative_from_fact(fact, settings=settings)
    e1 = iterative.filter(pl.col("exercise_id") == "e1").to_dicts()[0]
    e2 = iterative.filter(pl.col("exercise_id") == "e2").to_dicts()[0]

    assert abs(float(e1["smoothed_calibration_success_rate"]) - 0.3650793651) < 1e-6
    assert abs(float(e2["smoothed_calibration_success_rate"]) - 0.3030303030) < 1e-6
    assert float(e1["exercise_elo"]) < float(e2["exercise_elo"])


def test_iterative_exercise_elo_recenters_mean_to_base_rating(tmp_path: Path) -> None:
    """Test iterative exercise Elo keeps the calibrated mean at the Elo anchor."""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u2",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e2",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
        ]
    )

    iterative = build_agg_exercise_elo_iterative_from_fact(fact, settings=settings)
    mean_rating = (
        iterative.filter(pl.col("calibrated"))
        .select(pl.col("exercise_elo").mean().alias("mean_rating"))
        .item()
    )
    assert abs(float(mean_rating) - 1500.0) < 1e-6


def test_iterative_student_replay_keeps_attempt_ordinals_aligned(tmp_path: Path) -> None:
    """Test iterative student replay preserves the same local attempt count as current replay."""
    settings = _build_settings(tmp_path)
    fact = _fact(
        [
            {
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e1",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 5, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e2",
                "data_correct": False,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
            {
                "created_at": datetime(2025, 1, 1, 9, 10, 0),
                "date_utc": datetime(2025, 1, 1).date(),
                "user_id": "u1",
                "classroom_id": "c1",
                "playlist_or_module_id": "p1",
                "objective_id": "o1",
                "objective_label": "Objective 1",
                "activity_id": "a1",
                "activity_label": "Activity 1",
                "exercise_id": "e2",
                "data_correct": True,
                "data_duration": 10.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 2,
                "module_id": "m1",
                "module_code": "M1",
                "module_label": "Module 1",
            },
        ]
    )

    current_exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    iterative_exercise_elo = build_agg_exercise_elo_iterative_from_fact(fact, settings=settings)
    current_events = build_student_elo_events_from_fact(fact, current_exercise_elo)
    iterative_events = build_student_elo_events_iterative_from_fact(fact, iterative_exercise_elo)

    assert current_events["attempt_ordinal"].to_list() == iterative_events["attempt_ordinal"].to_list()
    assert current_events["exercise_id"].to_list() == iterative_events["exercise_id"].to_list()

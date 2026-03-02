from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl

from visu2.config import Settings
from visu2.derive import (
    build_agg_activity_elo_from_exercise_elo,
    build_agg_exercise_elo_from_fact,
    build_student_elo_events_from_fact,
    build_student_elo_profiles_from_events,
)


def _build_settings(tmp_path: Path) -> Settings:
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
        parquet_path=data_dir / "adaptiv_math_history.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def _fact(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def test_stage_a_single_correct_answer_updates_exercise_elo_symmetrically(tmp_path: Path) -> None:
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
    assert abs(float(row["exercise_elo"]) - 1488.0) < 1e-9


def test_activity_elo_excludes_uncalibrated_exercises(tmp_path: Path) -> None:
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
    assert abs(float(row["activity_mean_exercise_elo"]) - 1488.0) < 1e-9
    assert abs(float(row["calibration_coverage_ratio"]) - 0.5) < 1e-9


def test_stage_b_student_replay_updates_ordinal_and_rating(tmp_path: Path) -> None:
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
    assert row["total_attempts"] == 2
    assert row["unique_modules"] == 1
    assert row["unique_objectives"] == 1
    assert row["unique_activities"] == 1
    assert row["eligible_for_replay"] is True

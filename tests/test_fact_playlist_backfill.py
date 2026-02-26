from __future__ import annotations

import json
from datetime import datetime

import polars as pl

from visu2.config import Settings
from visu2.derive import build_fact_attempt_core


def _build_settings(tmp_path) -> Settings:
    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"
    resources_dir = tmp_path / "ressources"

    data_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    resources_dir.mkdir(parents=True, exist_ok=True)

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


def test_playlist_placeholder_ids_are_backfilled_from_exercise_summary(tmp_path) -> None:
    settings = _build_settings(tmp_path)

    raw = pl.DataFrame(
        {
            "classroom_id": ["c1"],
            "teacher_id": ["t1"],
            "user_id": ["u1"],
            "playlist_or_module_id": ["playlist-1"],
            "objective_id": ["None"],
            "activity_id": ["None"],
            "exercise_id": ["ex-1"],
            "module_long_title": [None],
            "created_at": [datetime(2025, 1, 1, 10, 0, 0)],
            "login_time": [datetime(2025, 1, 1, 9, 59, 0)],
            "data_correct": [True],
            "work_mode": ["playlist"],
            "data_answer": [None],
            "data_duration": [12.0],
            "session_duration": [12.0],
            "student_attempt_index": [1],
            "attempt_number": [1],
            "first_attempt_success_rate": [1.0],
        }
    )
    raw.write_parquet(settings.parquet_path)

    learning_catalog_payload = {
        "meta": {},
        "id_label_index": {
            "m1": {
                "type": "module",
                "code": "M1",
                "short_title": "Module 1",
                "long_title": "Module 1",
                "sources": ["summary_1.json"],
            },
            "o1": {
                "type": "objective",
                "code": "M1O1",
                "short_title": "Objective 1",
                "long_title": "Objective 1",
                "sources": ["summary_1.json"],
            },
            "a1": {
                "type": "activity",
                "code": "M1O1A1",
                "short_title": "Activity 1",
                "long_title": "Activity 1",
                "sources": ["summary_1.json"],
            },
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
                                "exercise_ids": ["ex-1"],
                            }
                        ],
                    }
                ],
            }
        ],
        "exercise_to_hierarchy": {
            "ex-1": {
                "activity_id": "a1",
                "objective_id": "o1",
                "module_id": "m1",
            }
        },
        "conflicts": {"coverage": {}},
        "orphans": [],
    }
    zpdes_rules_payload = {
        "meta": {},
        "module_rules": [],
        "map_id_code": {
            "code_to_id": {"M1": "m1", "M1O1": "o1", "M1O1A1": "a1"},
            "id_to_codes": {"m1": ["M1"], "o1": ["M1O1"], "a1": ["M1O1A1"]},
        },
        "links_to_catalog": {},
        "unresolved_links": {},
    }
    settings.learning_catalog_path.write_text(
        json.dumps(learning_catalog_payload), encoding="utf-8"
    )
    settings.zpdes_rules_path.write_text(json.dumps(zpdes_rules_payload), encoding="utf-8")
    settings.exercises_json_path.write_text(json.dumps({"exercises": []}), encoding="utf-8")

    fact = build_fact_attempt_core(settings)
    row = fact.to_dicts()[0]

    assert row["exercise_id"] == "ex-1"
    assert row["activity_id"] == "a1"
    assert row["objective_id"] == "o1"
    assert row["module_id"] == "m1"
    assert row["module_code"] == "M1"

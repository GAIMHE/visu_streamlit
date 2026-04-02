"""
test_fact_playlist_backfill.py

Validate playlist/module backfill behavior on fact table construction.

Dependencies
------------
- datetime
- json
- polars
- visu2

Classes
-------
- None.

Functions
---------
- _build_settings: Utility for build settings.
- test_playlist_placeholder_ids_are_backfilled_from_exercise_summary: Test scenario for playlist placeholder ids are backfilled from exercise summary.
"""
from __future__ import annotations

import json
from datetime import datetime

import polars as pl

from visu2.config import Settings
from visu2.derive import build_fact_attempt_core
from visu2.derive_fact import build_hierarchy_context_lookup, build_hierarchy_resolution_report


def _build_settings(tmp_path) -> Settings:
    """Build settings.

Parameters
----------
tmp_path : Any
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
        parquet_path=data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def test_playlist_placeholder_ids_are_backfilled_from_exercise_summary(tmp_path) -> None:
    """Test playlist placeholder ids are backfilled from exercise summary.

Parameters
----------
tmp_path : Any
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


def test_raw_activity_and_objective_ids_win_over_exercise_summary_context(tmp_path) -> None:
    """Test raw hierarchy ids keep their own labels even when the exercise maps elsewhere."""
    settings = _build_settings(tmp_path)

    raw = pl.DataFrame(
        {
            "classroom_id": ["c1"],
            "teacher_id": ["t1"],
            "user_id": ["u1"],
            "playlist_or_module_id": ["m_ctx"],
            "objective_id": ["o_ctx"],
            "activity_id": ["a_ctx"],
            "exercise_id": ["ex-shared"],
            "module_long_title": [None],
            "created_at": [datetime(2025, 1, 2, 10, 0, 0)],
            "login_time": [datetime(2025, 1, 2, 9, 59, 0)],
            "data_correct": [True],
            "work_mode": ["zpdes"],
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
            "m_ctx": {
                "type": "module",
                "code": "M31",
                "short_title": "Context module",
                "long_title": "Context module",
                "sources": ["catalog"],
            },
            "o_ctx": {
                "type": "objective",
                "code": "M31O4",
                "short_title": "Context objective",
                "long_title": "Context objective",
                "sources": ["catalog"],
            },
            "a_ctx": {
                "type": "activity",
                "code": "M31O4A1",
                "short_title": "Context activity",
                "long_title": "Context activity",
                "sources": ["catalog"],
            },
            "m_fallback": {
                "type": "module",
                "code": "M32",
                "short_title": "Fallback module",
                "long_title": "Fallback module",
                "sources": ["catalog"],
            },
            "o_fallback": {
                "type": "objective",
                "code": "M32O4",
                "short_title": "Fallback objective",
                "long_title": "Fallback objective",
                "sources": ["catalog"],
            },
            "a_fallback": {
                "type": "activity",
                "code": "M32O4A1",
                "short_title": "Fallback activity",
                "long_title": "Fallback activity",
                "sources": ["catalog"],
            },
        },
        "modules": [
            {
                "id": "m_ctx",
                "code": "M31",
                "title": {"short": "Context module", "long": "Context module"},
                "objectives": [],
            },
            {
                "id": "m_fallback",
                "code": "M32",
                "title": {"short": "Fallback module", "long": "Fallback module"},
                "objectives": [],
            },
        ],
        "exercise_to_hierarchy": {
            "ex-shared": {
                "activity_id": "a_fallback",
                "objective_id": "o_fallback",
                "module_id": "m_fallback",
            }
        },
        "conflicts": {"coverage": {}},
        "orphans": [],
    }
    zpdes_rules_payload = {
        "meta": {},
        "module_rules": [],
        "map_id_code": {
            "code_to_id": {
                "M31": "m_ctx",
                "M31O4": "o_ctx",
                "M31O4A1": "a_ctx",
                "M32": "m_fallback",
                "M32O4": "o_fallback",
                "M32O4A1": "a_fallback",
            },
            "id_to_codes": {
                "m_ctx": ["M31"],
                "o_ctx": ["M31O4"],
                "a_ctx": ["M31O4A1"],
                "m_fallback": ["M32"],
                "o_fallback": ["M32O4"],
                "a_fallback": ["M32O4A1"],
            },
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

    assert row["module_code"] == "M31"
    assert row["module_label"] == "Context module"
    assert row["objective_id"] == "o_ctx"
    assert row["objective_label"] == "Context objective"
    assert row["activity_id"] == "a_ctx"
    assert row["activity_label"] == "Context activity"


def test_initial_test_unknown_ids_keep_module_and_leave_labels_null(tmp_path) -> None:
    """Test raw unknown lower-level ids do not borrow labels from exercise fallback."""
    settings = _build_settings(tmp_path)

    raw = pl.DataFrame(
        {
            "classroom_id": ["c1"],
            "teacher_id": ["t1"],
            "user_id": ["u1"],
            "playlist_or_module_id": ["m41"],
            "objective_id": ["o_unknown"],
            "activity_id": ["a_unknown"],
            "exercise_id": ["ex-orphan"],
            "module_long_title": [None],
            "created_at": [datetime(2025, 1, 3, 10, 0, 0)],
            "login_time": [datetime(2025, 1, 3, 9, 59, 0)],
            "data_correct": [True],
            "work_mode": ["initial-test"],
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
            "m41": {
                "type": "module",
                "code": "M41",
                "short_title": "Résolution de problèmes, grandeurs et mesures - Niveau 1",
                "long_title": "Résolution de problèmes, grandeurs et mesures",
                "sources": ["catalog"],
            }
        },
        "modules": [
            {
                "id": "m41",
                "code": "M41",
                "title": {
                    "short": "Résolution de problèmes, grandeurs et mesures - Niveau 1",
                    "long": "Résolution de problèmes, grandeurs et mesures",
                },
                "objectives": [],
            }
        ],
        "exercise_to_hierarchy": {},
        "conflicts": {"coverage": {}},
        "orphans": [{"exercise_id": "ex-orphan", "history_row_count": 1, "reason": "not_in_primary"}],
    }
    zpdes_rules_payload = {
        "meta": {},
        "module_rules": [],
        "map_id_code": {
            "code_to_id": {"M41": "m41"},
            "id_to_codes": {"m41": ["M41"]},
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

    assert row["module_id"] == "m41"
    assert row["module_code"] == "M41"
    assert row["module_label"] == "Résolution de problèmes, grandeurs et mesures - Niveau 1"
    assert row["objective_id"] == "o_unknown"
    assert row["objective_label"] is None
    assert row["activity_id"] == "a_unknown"
    assert row["activity_label"] is None


def test_context_lookup_keeps_multi_context_exercise_distinct(tmp_path) -> None:
    """Test the context lookup preserves distinct raw contexts for reused exercises."""
    settings = _build_settings(tmp_path)

    raw = pl.DataFrame(
        {
            "classroom_id": ["c1", "c1"],
            "teacher_id": ["t1", "t1"],
            "user_id": ["u1", "u2"],
            "playlist_or_module_id": ["m31", "m32"],
            "objective_id": ["o31", "o32"],
            "activity_id": ["a_shared", "a_shared"],
            "exercise_id": ["ex-shared", "ex-shared"],
            "module_long_title": [None, None],
            "created_at": [datetime(2025, 1, 4, 10, 0, 0), datetime(2025, 1, 4, 10, 1, 0)],
            "login_time": [datetime(2025, 1, 4, 9, 59, 0), datetime(2025, 1, 4, 10, 0, 0)],
            "data_correct": [True, False],
            "work_mode": ["zpdes", "zpdes"],
            "data_answer": [None, None],
            "data_duration": [12.0, 12.0],
            "session_duration": [12.0, 12.0],
            "student_attempt_index": [1, 2],
            "attempt_number": [1, 1],
            "first_attempt_success_rate": [1.0, 0.0],
        }
    )
    raw.write_parquet(settings.parquet_path)

    learning_catalog_payload = {
        "meta": {},
        "id_label_index": {
            "m31": {"type": "module", "code": "M31", "short_title": "M31", "long_title": "M31", "sources": ["catalog"]},
            "o31": {"type": "objective", "code": "M31O4", "short_title": "O31", "long_title": "O31", "sources": ["catalog"]},
            "m32": {"type": "module", "code": "M32", "short_title": "M32", "long_title": "M32", "sources": ["catalog"]},
            "o32": {"type": "objective", "code": "M32O4", "short_title": "O32", "long_title": "O32", "sources": ["catalog"]},
            "a_shared": {
                "type": "activity",
                "code": "M32O4A1",
                "short_title": "Shared activity",
                "long_title": "Shared activity",
                "sources": ["catalog"],
            },
        },
        "modules": [
            {"id": "m31", "code": "M31", "title": {"short": "M31", "long": "M31"}, "objectives": []},
            {"id": "m32", "code": "M32", "title": {"short": "M32", "long": "M32"}, "objectives": []},
        ],
        "exercise_to_hierarchy": {
            "ex-shared": {"activity_id": "a_shared", "objective_id": "o32", "module_id": "m32"}
        },
        "conflicts": {"coverage": {}},
        "orphans": [],
    }
    zpdes_rules_payload = {
        "meta": {},
        "module_rules": [],
        "map_id_code": {
            "code_to_id": {
                "M31": "m31",
                "M31O4": "o31",
                "M32": "m32",
                "M32O4": "o32",
                "M32O4A1": "a_shared",
            },
            "id_to_codes": {
                "m31": ["M31"],
                "o31": ["M31O4"],
                "m32": ["M32"],
                "o32": ["M32O4"],
                "a_shared": ["M32O4A1", "M31O4A1"],
            },
        },
        "links_to_catalog": {},
        "unresolved_links": {},
    }
    settings.learning_catalog_path.write_text(
        json.dumps(learning_catalog_payload), encoding="utf-8"
    )
    settings.zpdes_rules_path.write_text(json.dumps(zpdes_rules_payload), encoding="utf-8")
    settings.exercises_json_path.write_text(json.dumps({"exercises": []}), encoding="utf-8")

    context_lookup = build_hierarchy_context_lookup(settings)
    matching = context_lookup.filter(pl.col("exercise_id") == "ex-shared")
    report = build_hierarchy_resolution_report(context_lookup)

    assert matching.height == 2
    assert set(matching["playlist_or_module_id"].to_list()) == {"m31", "m32"}
    assert report["exercise_ids_with_multiple_raw_contexts"] == 1

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from visu2.overview_concentration import (
    build_bucket_summary,
    build_entity_attempt_summary,
    build_global_student_attempt_summary,
    build_within_entity_student_concentration,
    load_catalog_contained_exercise_counts,
)


def _sample_fact() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date_utc": [date(2026, 3, 1)] * 9,
            "module_code": ["M1", "M1", "M1", "M1", "M1", "M2", "M2", "M2", "M2"],
            "module_label": ["Module 1"] * 5 + ["Module 2"] * 4,
            "objective_id": ["O1", "O1", "O2", "O2", "O2", "O3", "O3", "O4", "O4"],
            "objective_label": ["Objective 1", "Objective 1", "Objective 2", "Objective 2", "Objective 2", "Objective 3", "Objective 3", "Objective 4", "Objective 4"],
            "activity_id": ["A1", "A1", "A2", "A2", "A3", "A4", "A4", "A5", "A5"],
            "activity_label": ["Activity 1", "Activity 1", "Activity 2", "Activity 2", "Activity 3", "Activity 4", "Activity 4", "Activity 5", "Activity 5"],
            "exercise_id": ["E1", "E1", "E2", "E2", "E3", "E4", "E4", "E5", "E6"],
            "user_id": ["u1", "u1", "u1", "u2", "u3", "u2", "u2", "u4", "u5"],
            "work_mode": ["zpdes", "zpdes", "playlist", "playlist", "zpdes", "playlist", "playlist", "zpdes", "zpdes"],
        }
    )


def _sample_unmapped_initial_test_fact() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date_utc": [date(2026, 3, 1)] * 4,
            "module_code": ["M1"] * 4,
            "module_label": ["Module 1"] * 4,
            "objective_id": ["unknown-objective"] * 4,
            "objective_label": [None] * 4,
            "activity_id": ["unknown-activity"] * 4,
            "activity_label": [None] * 4,
            "exercise_id": ["E100", "E100", "E101", "E101"],
            "user_id": ["u1", "u1", "u2", "u2"],
            "work_mode": ["initial-test"] * 4,
        }
    )


def _write_catalog(tmp_path: Path) -> Path:
    payload = {
        "meta": {},
        "id_label_index": {},
        "conflicts": {},
        "orphans": [],
        "exercise_to_hierarchy": {
            "E1": {"activity_id": "A1", "objective_id": "O1", "module_id": "m1"},
            "E2": {"activity_id": "A2", "objective_id": "O2", "module_id": "m1"},
            "E3": {"activity_id": "A3", "objective_id": "O2", "module_id": "m1"},
            "E4": {"activity_id": "A4", "objective_id": "O3", "module_id": "m2"},
            "E5": {"activity_id": "A5", "objective_id": "O4", "module_id": "m2"},
            "E6": {"activity_id": "A5", "objective_id": "O4", "module_id": "m2"},
        },
        "modules": [
            {
                "id": "m1",
                "code": "M1",
                "title": {"short": "Module 1", "long": "Module 1"},
                "objectives": [
                    {
                        "id": "O1",
                        "code": "M1O1",
                        "title": {"short": "Objective 1", "long": "Objective 1"},
                        "activities": [
                            {
                                "id": "A1",
                                "code": "M1O1A1",
                                "title": {"short": "Activity 1", "long": "Activity 1"},
                                "exercise_ids": ["E1"],
                            },
                        ],
                    },
                    {
                        "id": "O2",
                        "code": "M1O2",
                        "title": {"short": "Objective 2", "long": "Objective 2"},
                        "activities": [
                            {
                                "id": "A2",
                                "code": "M1O2A1",
                                "title": {"short": "Activity 2", "long": "Activity 2"},
                                "exercise_ids": ["E2"],
                            },
                            {
                                "id": "A3",
                                "code": "M1O2A2",
                                "title": {"short": "Activity 3", "long": "Activity 3"},
                                "exercise_ids": ["E3"],
                            },
                        ],
                    },
                ],
            },
            {
                "id": "m2",
                "code": "M2",
                "title": {"short": "Module 2", "long": "Module 2"},
                "objectives": [
                    {
                        "id": "O3",
                        "code": "M2O1",
                        "title": {"short": "Objective 3", "long": "Objective 3"},
                        "activities": [
                            {
                                "id": "A4",
                                "code": "M2O1A1",
                                "title": {"short": "Activity 4", "long": "Activity 4"},
                                "exercise_ids": ["E4"],
                            },
                        ],
                    },
                    {
                        "id": "O4",
                        "code": "M2O2",
                        "title": {"short": "Objective 4", "long": "Objective 4"},
                        "activities": [
                            {
                                "id": "A5",
                                "code": "M2O2A1",
                                "title": {"short": "Activity 5", "long": "Activity 5"},
                                "exercise_ids": ["E5", "E6"],
                            },
                        ],
                    },
                ],
            },
        ]
    }
    path = tmp_path / "learning_catalog.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_build_entity_attempt_summary_activity_uses_catalog_counts(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    counts = load_catalog_contained_exercise_counts(catalog_path)
    summary = build_entity_attempt_summary(
        _sample_fact(),
        level="activity",
        work_modes=("zpdes", "playlist"),
        contained_exercise_counts=counts,
    )
    activity_five = summary.filter(pl.col("id") == "A5").row(0, named=True)
    assert activity_five["contained_exercises"] == 2
    assert abs(float(summary["attempt_share"].sum()) - 1.0) < 1e-9


def test_build_bucket_summary_module_bypasses_deciles(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    counts = load_catalog_contained_exercise_counts(catalog_path)
    summary = build_entity_attempt_summary(
        _sample_fact(),
        level="module",
        work_modes=("zpdes", "playlist"),
        contained_exercise_counts=counts,
    )
    bucket_summary = build_bucket_summary(summary, level="module")
    assert bucket_summary.height == 2
    assert bucket_summary["bucket_key"].to_list() == ["M1", "M2"]


def test_build_bucket_summary_deciles_use_top_bucket_for_highest_rank(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    counts = load_catalog_contained_exercise_counts(catalog_path)
    summary = build_entity_attempt_summary(
        _sample_fact(),
        level="exercise",
        work_modes=("zpdes", "playlist"),
        contained_exercise_counts=counts,
    )
    top_row = summary.row(0, named=True)
    assert top_row["bucket_label"] == "Top 10%"
    bucket_summary = build_bucket_summary(summary, level="exercise")
    assert abs(float(bucket_summary["attempt_share"].sum()) - 1.0) < 1e-9


def test_unmapped_initial_test_activity_uses_fact_count_and_fallback_label(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    counts = load_catalog_contained_exercise_counts(catalog_path)
    summary = build_entity_attempt_summary(
        _sample_unmapped_initial_test_fact(),
        level="activity",
        work_modes=("initial-test",),
        contained_exercise_counts=counts,
    )
    row = summary.row(0, named=True)
    assert row["label"] == "Unmapped initial-test activity (M1)"
    assert row["contained_exercises"] == 2


def test_unmapped_initial_test_objective_uses_fact_count_and_fallback_label(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    counts = load_catalog_contained_exercise_counts(catalog_path)
    summary = build_entity_attempt_summary(
        _sample_unmapped_initial_test_fact(),
        level="objective",
        work_modes=("initial-test",),
        contained_exercise_counts=counts,
    )
    row = summary.row(0, named=True)
    assert row["label"] == "Unmapped initial-test objective (M1)"
    assert row["contained_exercises"] == 2


def test_build_global_student_attempt_summary_assigns_rank_buckets(tmp_path: Path) -> None:
    summary = build_global_student_attempt_summary(
        _sample_fact(),
        work_modes=("zpdes", "playlist"),
    )
    assert abs(float(summary["attempt_share"].sum()) - 1.0) < 1e-9
    top_row = summary.row(0, named=True)
    assert top_row["user_id"] == "u1"
    assert top_row["bucket_label"] == "Top 10%"


def test_build_within_entity_student_concentration_keeps_within_entity_student_shares(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    counts = load_catalog_contained_exercise_counts(catalog_path)
    bucket_summary, drilldown = build_within_entity_student_concentration(
        _sample_fact(),
        level="activity",
        work_modes=("zpdes", "playlist"),
        contained_exercise_counts=counts,
    )
    assert abs(float(bucket_summary["attempt_share"].sum()) - 1.0) < 1e-9
    activity_one = drilldown.filter((pl.col("id") == "A1") & (pl.col("bucket_order") == 1)).row(0, named=True)
    assert activity_one["selected_bucket_attempt_share"] == 1.0
    assert activity_one["top_10_students_share"] == 1.0
    assert activity_one["unique_students"] == 1

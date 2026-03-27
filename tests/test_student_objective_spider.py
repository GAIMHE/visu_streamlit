from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from visu2.figure_analysis import analyze_student_objective_spider
from visu2.student_objective_spider import (
    build_student_module_options,
    build_student_objective_spider_figure,
    build_student_objective_summary,
    build_student_selection_profiles,
    load_objective_catalog,
    select_student_by_id,
    select_students_near_attempt_target,
    summarize_student_module_profile,
)


def _write_catalog(tmp_path: Path) -> Path:
    payload = {
        "meta": {},
        "id_label_index": {},
        "exercise_to_hierarchy": {},
        "modules": [
            {
                "id": "m1",
                "code": "M1",
                "title": {"short": "Numbers", "long": "Numbers"},
                "objectives": [
                    {
                        "id": "o1",
                        "code": "M1O1",
                        "title": {"short": "Counting", "long": "Counting"},
                        "activities": [
                            {
                                "id": "a1",
                                "code": "M1O1A1",
                                "title": {"short": "Count to 5", "long": "Count to 5"},
                                "exercise_ids": ["e1", "e2"],
                            },
                            {
                                "id": "a2",
                                "code": "M1O1A2",
                                "title": {"short": "Count to 10", "long": "Count to 10"},
                                "exercise_ids": ["e3"],
                            },
                        ],
                    },
                    {
                        "id": "o2",
                        "code": "M1O2",
                        "title": {"short": "Compare", "long": "Compare"},
                        "activities": [
                            {
                                "id": "a3",
                                "code": "M1O2A1",
                                "title": {"short": "Compare sets", "long": "Compare sets"},
                                "exercise_ids": ["e4", "e5"],
                            }
                        ],
                    },
                    {
                        "id": "o3",
                        "code": "M1O3",
                        "title": {"short": "Order", "long": "Order"},
                        "activities": [
                            {
                                "id": "a4",
                                "code": "M1O3A1",
                                "title": {"short": "Order", "long": "Order"},
                                "exercise_ids": ["e6"],
                            }
                        ],
                    },
                ],
            },
            {
                "id": "m2",
                "code": "M2",
                "title": {"short": "Geometry", "long": "Geometry"},
                "objectives": [
                    {
                        "id": "o4",
                        "code": "M2O1",
                        "title": {"short": "Shapes", "long": "Shapes"},
                        "activities": [
                            {
                                "id": "a5",
                                "code": "M2O1A1",
                                "title": {"short": "Triangles", "long": "Triangles"},
                                "exercise_ids": ["e7"],
                            }
                        ],
                    }
                ],
            },
        ],
    }
    path = tmp_path / "learning_catalog.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _fact_fixture() -> pl.DataFrame:
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)

    def row(
        minutes: int,
        *,
        user_id: str,
        module_code: str,
        module_label: str,
        objective_id: str,
        exercise_id: str,
        attempt_number: int,
        data_correct: float,
    ) -> dict[str, object]:
        created_at = ts0 + timedelta(minutes=minutes)
        return {
            "created_at": created_at,
            "user_id": user_id,
            "module_code": module_code,
            "module_label": module_label,
            "objective_id": objective_id,
            "exercise_id": exercise_id,
            "attempt_number": attempt_number,
            "data_correct": data_correct,
        }

    rows = [
        row(1, user_id="u1", module_code="M1", module_label="Numbers", objective_id="o1", exercise_id="e1", attempt_number=1, data_correct=1.0),
        row(2, user_id="u1", module_code="M1", module_label="Numbers", objective_id="o1", exercise_id="e1", attempt_number=2, data_correct=0.0),
        row(3, user_id="u1", module_code="M1", module_label="Numbers", objective_id="o1", exercise_id="e2", attempt_number=1, data_correct=1.0),
        row(4, user_id="u1", module_code="M1", module_label="Numbers", objective_id="o2", exercise_id="e4", attempt_number=1, data_correct=0.0),
        row(5, user_id="u1", module_code="M2", module_label="Geometry", objective_id="o4", exercise_id="e7", attempt_number=1, data_correct=1.0),
        row(6, user_id="u1", module_code="M9", module_label="Legacy", objective_id="o9", exercise_id="e99", attempt_number=1, data_correct=1.0),
    ]
    rows.extend(
        row(
            10 + idx,
            user_id="u2",
            module_code="M1",
            module_label="Numbers",
            objective_id="o1" if idx < 5 else "o2",
            exercise_id=f"u2e{idx}",
            attempt_number=1,
            data_correct=1.0 if idx % 2 == 0 else 0.0,
        )
        for idx in range(12)
    )
    return pl.DataFrame(rows)


def test_build_student_selection_profiles_returns_attempt_counts_and_eligibility() -> None:
    profiles = build_student_selection_profiles(_fact_fixture())

    u1 = profiles.filter(pl.col("user_id") == "u1").row(0, named=True)
    assert u1["total_attempts"] == 6
    assert u1["eligible_for_selection"] is True
    assert u1["unique_modules"] == 3


def test_select_students_near_attempt_target_matches_band() -> None:
    profiles = pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "total_attempts": [95, 108, 220],
            "eligible_for_selection": [True, True, True],
        }
    )
    selected = select_students_near_attempt_target(
        profiles,
        target_attempts=100,
        tolerance_ratio=0.10,
        max_students=2,
        seed=7,
    )
    assert selected == ["u1", "u2"]


def test_select_student_by_id_accepts_eligible_and_rejects_unknown() -> None:
    profiles = pl.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "total_attempts": [10, 20],
            "eligible_for_selection": [True, False],
        }
    )
    assert select_student_by_id(profiles, "u1") == "u1"
    assert select_student_by_id(profiles, "u2") is None
    assert select_student_by_id(profiles, "missing") is None


def test_build_student_module_options_limits_modules_to_catalog_backed_attempts(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    objective_catalog = load_objective_catalog(catalog_path)

    options = build_student_module_options(_fact_fixture(), objective_catalog, "u1")

    assert options["module_code"].to_list() == ["M1", "M2"]
    assert options["attempts"].to_list() == [4, 1]


def test_build_student_objective_summary_includes_all_objectives_and_ignores_retries_for_coverage(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    objective_catalog = load_objective_catalog(catalog_path)

    summary = build_student_objective_summary(
        _fact_fixture(),
        objective_catalog,
        user_id="u1",
        module_code="M1",
    )

    assert summary["objective_code"].to_list() == ["M1O1", "M1O2", "M1O3"]
    row_o1 = summary.filter(pl.col("objective_code") == "M1O1").row(0, named=True)
    row_o2 = summary.filter(pl.col("objective_code") == "M1O2").row(0, named=True)
    row_o3 = summary.filter(pl.col("objective_code") == "M1O3").row(0, named=True)

    assert row_o1["distinct_exercises_attempted"] == 2
    assert abs(row_o1["coverage_rate"] - (2 / 3)) < 1e-9
    assert abs(row_o1["success_rate_all_attempts"] - (2 / 3)) < 1e-9
    assert row_o1["attempts"] == 3

    assert row_o2["distinct_exercises_attempted"] == 1
    assert abs(row_o2["coverage_rate"] - 0.5) < 1e-9
    assert row_o2["success_rate_all_attempts"] == 0.0

    assert row_o3["distinct_exercises_attempted"] == 0
    assert row_o3["coverage_rate"] == 0.0
    assert row_o3["attempts"] == 0
    assert row_o3["success_rate_all_attempts"] is None


def test_summarize_student_module_profile_returns_counts_and_rates(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    objective_catalog = load_objective_catalog(catalog_path)
    summary = build_student_objective_summary(
        _fact_fixture(),
        objective_catalog,
        user_id="u1",
        module_code="M1",
    )

    profile = summarize_student_module_profile(summary)

    assert profile["objectives_total"] == 3
    assert profile["objectives_attempted"] == 2
    assert profile["module_attempts"] == 4
    assert profile["module_distinct_exercises_attempted"] == 3
    assert profile["module_exercise_total"] == 6
    assert abs(profile["module_coverage_rate"] - 0.5) < 1e-9


def test_build_student_objective_spider_figure_has_two_traces_and_percent_axis(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    objective_catalog = load_objective_catalog(catalog_path)
    summary = build_student_objective_summary(
        _fact_fixture(),
        objective_catalog,
        user_id="u1",
        module_code="M1",
    )

    figure = build_student_objective_spider_figure(
        summary,
        student_id="u1",
        module_code="M1",
        module_label="Numbers",
    )

    assert len(figure.data) == 2
    assert figure.data[0].name == "Coverage %"
    assert figure.data[1].name == "Success rate"
    assert list(figure.data[0].theta) == ["M1O1", "M1O2", "M1O3"]
    assert list(figure.layout.polar.radialaxis.range) == [0, 100]
    assert figure.data[0].customdata[0][1] == 2
    assert figure.data[0].customdata[0][2] == 3
    assert figure.data[1].r[2] is None


def test_analyze_student_objective_spider_surfaces_touch_and_gap_findings(tmp_path: Path) -> None:
    catalog_path = _write_catalog(tmp_path)
    objective_catalog = load_objective_catalog(catalog_path)
    summary = build_student_objective_summary(
        _fact_fixture(),
        objective_catalog,
        user_id="u1",
        module_code="M1",
    )

    analysis = analyze_student_objective_spider(
        summary,
        student_id="u1",
        module_code="M1",
        module_label="Numbers",
        total_attempts=6,
    )

    assert len(analysis.findings) >= 4
    assert "u1 has 6 total attempts overall" in analysis.findings[0]
    assert analysis.interpretation is not None

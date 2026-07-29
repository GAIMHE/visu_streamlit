from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.model_zpdes_elo_quartile_equity import (
    build_student_module_analysis,
    fit_quartile_equity_model,
)


def _synthetic_inputs(
    *,
    n_modules: int = 8,
    students_per_module: int = 40,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(14)
    activity_rows = []
    elo_rows = []
    lookup_rows = []

    for module_index in range(n_modules):
        module_code = f"M{module_index}"
        module_title = f"Module {module_index}"
        lookup_rows.append(
            {"module_code": module_code, "module_title": module_title}
        )
        for student_index in range(students_per_module):
            student_id = f"student-{student_index}"
            quartile_index = student_index // (students_per_module // 4)
            elo_rows.append(
                {
                    "student_id": student_id,
                    "module_code": module_code,
                    "adaptive_test_attempts": 10,
                    "adaptive_test_elo": (
                        1200.0 + quartile_index * 100.0 + rng.normal(0.0, 3.0)
                    ),
                }
            )
            for sequence_index in range(2):
                activity_rows.append(
                    {
                        "student_id": student_id,
                        "classroom_id": f"classroom-{student_index % 5}",
                        "module": module_title,
                        "activity_id": f"activity-{sequence_index}",
                        "work_mode": "zpdes",
                        "mean_progress": (
                            10.0 + quartile_index + rng.normal(0.0, 0.3)
                        ),
                    }
                )

    return (
        pd.DataFrame(activity_rows),
        pd.DataFrame(elo_rows),
        pd.DataFrame(lookup_rows),
    )


def test_builds_one_row_per_student_module_and_module_specific_quartiles() -> None:
    activity, elo, lookup = _synthetic_inputs()

    analysis, audit, coverage = build_student_module_analysis(
        activity,
        elo,
        lookup,
        min_pairs_per_quartile=5,
    )

    assert len(analysis) == 8 * 40
    assert not analysis.duplicated(["student_id", "module_code"]).any()
    assert set(analysis["elo_quartile"].astype(str)) == {"Q1", "Q2", "Q3", "Q4"}
    assert audit.loc[0, "modules_retained"] == 8
    assert len(coverage) == 8
    assert coverage["eligible"].all()
    assert (coverage[["Q1", "Q2", "Q3", "Q4"]] == 10).all().all()


def test_rejects_students_mapped_to_multiple_classrooms() -> None:
    activity, elo, lookup = _synthetic_inputs()
    duplicate = activity.iloc[[0]].copy()
    duplicate["classroom_id"] = "another-classroom"
    activity = pd.concat([activity, duplicate], ignore_index=True)

    with pytest.raises(ValueError, match="multiple classrooms"):
        build_student_module_analysis(
            activity,
            elo,
            lookup,
            min_pairs_per_quartile=5,
        )


def test_crossed_model_recovers_increasing_average_quartile_effects() -> None:
    activity, elo, lookup = _synthetic_inputs()
    analysis, _, _ = build_student_module_analysis(
        activity,
        elo,
        lookup,
        min_pairs_per_quartile=5,
    )

    result = fit_quartile_equity_model(analysis, maxiter=80)
    effects = result.fixed_effects.set_index("term")["estimate"]

    assert result.diagnostics.loc[0, "converged"]
    assert effects["Q2"] > 0.5
    assert effects["Q3"] > effects["Q2"]
    assert effects["Q4"] > effects["Q3"]
    assert set(result.module_heterogeneity["contrast"]) == {
        "Q2 - Q1",
        "Q3 - Q1",
        "Q4 - Q1",
    }

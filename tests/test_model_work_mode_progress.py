from __future__ import annotations

from argparse import Namespace

import numpy as np
import pandas as pd

from scripts.model_work_mode_progress import (
    _keep_only_single_module_playlists,
    _student_classroom_error,
    build_activity_level,
    fit_mixed_model,
    fit_population_interaction_model,
    split_populations,
)


def test_multi_module_playlists_are_included_by_default() -> None:
    assert _keep_only_single_module_playlists(Namespace()) is False
    assert (
        _keep_only_single_module_playlists(
            Namespace(keep_only_single_module_playlists=True)
        )
        is True
    )


def _synthetic_activity_data() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows = []
    for classroom_index in range(12):
        classroom_effect = rng.normal(0, 2)
        for student_index in range(8):
            population = "exclusive_modes" if student_index < 4 else "both_modes"
            mode_effect = 10 if population == "exclusive_modes" else 8
            student_effect = rng.normal(0, 3)
            for module_index, module_effect in enumerate((-4.0, 0.0, 5.0)):
                for work_mode in ("playlist", "zpdes"):
                    for repetition in range(2):
                        rows.append(
                            {
                                "student_id": f"student_{classroom_index}_{student_index}",
                                "classroom_id": f"classroom_{classroom_index}",
                                "module": f"module_{module_index}",
                                "work_mode": work_mode,
                                "activity_id": f"activity_{module_index}_{repetition}",
                                "source_population": population,
                                "mean_progress": (
                                    -2
                                    + mode_effect * (work_mode == "zpdes")
                                    + module_effect
                                    + classroom_effect
                                    + student_effect
                                    + rng.normal(0, 5)
                                ),
                            }
                        )
    return pd.DataFrame(rows)


def test_primary_model_uses_classroom_and_nested_student_intercepts() -> None:
    activity = _synthetic_activity_data()

    summary = fit_mixed_model(activity, population="synthetic", maxiter=200)

    assert summary.status == "ok"
    assert summary.converged is True
    assert summary.random_student_var is not None
    assert summary.random_classroom_var is not None
    assert "activity" not in summary.model_specification
    assert "activity_id" not in (summary.variance_components or "")
    assert 8.0 < summary.estimate_zpdes_vs_playlist < 10.0
    assert np.isclose(
        summary.zpdes_adjusted_mean - summary.playlist_adjusted_mean,
        summary.estimate_zpdes_vs_playlist,
    )


def test_population_interaction_recovers_smaller_both_mode_effect() -> None:
    activity = _synthetic_activity_data()

    summary = fit_population_interaction_model(activity, maxiter=200)

    assert summary.status == "ok"
    assert summary.converged is True
    assert summary.interaction_both_minus_exclusive < 0
    assert summary.exclusive_zpdes_vs_playlist > summary.both_zpdes_vs_playlist


def test_primary_model_rejects_students_in_multiple_classrooms() -> None:
    activity = _synthetic_activity_data()
    activity.loc[activity.index[0], "classroom_id"] = "another_classroom"

    error = _student_classroom_error(activity)

    assert error is not None
    assert "1 students span multiple classrooms" in error


def test_activity_progress_uses_first_attempts_and_module_qualified_activities() -> None:
    rows = []
    outcomes = {
        "module_1": [0, 0, 1, 1],
        "module_2": [1, 1, 0, 0],
    }
    minute = 0
    for module, successes in outcomes.items():
        for exercise_index, success in enumerate(successes):
            minute += 1
            rows.append(
                {
                    "student_id": "student_1",
                    "classroom_id": "classroom_1",
                    "module": module,
                    "activity_id": "activity_1",
                    "work_mode": "playlist",
                    "exercise_id": f"{module}_exercise_{exercise_index}",
                    "created_at": pd.Timestamp("2026-01-01", tz="UTC")
                    + pd.Timedelta(minutes=minute),
                    "success": success,
                }
            )
    rows.append(
        {
            **rows[0],
            "created_at": pd.Timestamp("2026-01-02", tz="UTC"),
            "success": 1,
        }
    )

    activity = build_activity_level(pd.DataFrame(rows), min_activity_exercises=4)

    assert len(activity) == 2
    assert set(activity["module"]) == {"module_1", "module_2"}
    assert activity.set_index("module")["n_first_attempts"].to_dict() == {
        "module_1": 4,
        "module_2": 4,
    }
    assert activity.set_index("module")["mean_progress"].to_dict() == {
        "module_1": 100.0,
        "module_2": -100.0,
    }


def test_population_split_has_no_global_exercise_threshold_by_default() -> None:
    attempts = pd.DataFrame(
        {
            "student_id": ["playlist_student", "zpdes_student"],
            "work_mode": ["playlist", "zpdes"],
            "exercise_id": ["exercise_1", "exercise_2"],
        }
    )

    populations = split_populations(attempts)

    assert set(populations["exclusive_modes"]["student_id"]) == {
        "playlist_student",
        "zpdes_student",
    }

from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.model_work_mode_first_attempt_trajectory import (
    GROUP_COLUMNS,
    build_empirical_trajectory_summary,
    build_first_attempt_trajectory,
    build_fixed_effect_trajectory,
    fit_gpboost_trajectory,
    prepare_gpboost_inputs,
)


def _attempt(
    student: str,
    classroom: str,
    module: str,
    mode: str,
    exercise: str,
    minute: int,
    success: int,
    activity: str | None = None,
) -> dict[str, object]:
    return {
        "student_id": student,
        "classroom_id": classroom,
        "module": module,
        "work_mode": mode,
        "exercise_id": exercise,
        "activity_id": activity or f"activity_{module}",
        "created_at": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(minutes=minute),
        "success": success,
    }


def test_first_attempts_are_unique_and_positions_restart_by_segment() -> None:
    attempts = pd.DataFrame(
        [
            _attempt("s1", "c1", "m101", "zpdes", "e1", 1, 0),
            _attempt("s1", "c1", "m101", "zpdes", "e1", 2, 1),
            _attempt("s1", "c1", "m101", "zpdes", "e2", 3, 1),
            _attempt("s1", "c1", "m101", "playlist", "e1", 4, 1),
            _attempt("s1", "c1", "m102", "playlist", "e3", 5, 0),
            _attempt("s1", "c1", "m102", "playlist", "e4", 6, 1),
        ]
    )

    trajectory = build_first_attempt_trajectory(attempts, min_activity_exercises=2)

    assert trajectory["exercise_id"].tolist() == ["e1", "e2", "e3", "e4"]
    assert trajectory["attempt_position"].tolist() == [0, 1, 0, 1]
    assert trajectory.groupby(["student_id", "exercise_id"]).size().max() == 1
    assert trajectory.loc[trajectory["exercise_id"].eq("e1"), "success"].item() == 0
    assert trajectory["student_in_classroom"].nunique() == 1


def test_short_segments_are_removed_after_first_attempt_selection() -> None:
    attempts = pd.DataFrame(
        [
            _attempt("s1", "c1", "m101", "playlist", "e1", 1, 0),
            _attempt("s1", "c1", "m101", "playlist", "e1", 2, 1),
            _attempt("s1", "c1", "m102", "zpdes", "e2", 3, 1),
            _attempt("s1", "c1", "m102", "zpdes", "e3", 4, 1),
        ]
    )

    trajectory = build_first_attempt_trajectory(attempts, min_activity_exercises=2)

    assert set(trajectory["module"]) == {"m102"}
    assert trajectory["attempt_position"].tolist() == [0, 1]


def test_positions_restart_for_activities_within_the_same_module() -> None:
    attempts = pd.DataFrame(
        [
            _attempt(
                "s1",
                "c1",
                "m101",
                "playlist",
                f"e{exercise}",
                exercise,
                exercise % 2,
                activity="a1" if exercise <= 4 else "a2",
            )
            for exercise in range(1, 9)
        ]
    )

    trajectory = build_first_attempt_trajectory(attempts)

    assert trajectory.groupby("activity_id", observed=True).size().to_dict() == {"a1": 4, "a2": 4}
    assert trajectory.groupby("activity_id", observed=True)["attempt_position"].apply(
        list
    ).to_dict() == {"a1": [0, 1, 2, 3], "a2": [0, 1, 2, 3]}


def test_module_sequences_continue_across_activities() -> None:
    attempts = pd.DataFrame(
        [
            _attempt(
                "s1",
                "c1",
                "m101",
                "zpdes",
                f"e{exercise}",
                exercise,
                exercise % 2,
                activity="a1" if exercise <= 2 else "a2",
            )
            for exercise in range(1, 5)
        ]
    )

    trajectory = build_first_attempt_trajectory(
        attempts,
        min_activity_exercises=4,
        sequence_scope="module",
    )

    assert trajectory["module_sequence_id"].nunique() == 1
    assert trajectory["attempt_position"].tolist() == [0, 1, 2, 3]
    assert trajectory["activity_id"].tolist() == ["a1", "a1", "a2", "a2"]


def test_module_change_starts_a_new_sequence_when_returning_to_a_module() -> None:
    modules = ["m101", "m101", "m102", "m102", "m101", "m101"]
    attempts = pd.DataFrame(
        [
            _attempt(
                "s1",
                "c1",
                module,
                "zpdes",
                f"e{index}",
                index,
                index % 2,
            )
            for index, module in enumerate(modules, start=1)
        ]
    )

    trajectory = build_first_attempt_trajectory(
        attempts,
        min_activity_exercises=2,
        sequence_scope="module",
    )

    assert trajectory["module_sequence_id"].nunique() == 3
    assert trajectory.groupby("module_sequence_id")["attempt_position"].apply(
        list
    ).to_dict() == {1: [0, 1], 2: [0, 1], 3: [0, 1]}


def test_module_sequences_split_on_mode_change_and_require_more_than_30_exercises() -> None:
    modes = ["zpdes"] * 30 + ["playlist"] * 31
    attempts = pd.DataFrame(
        [
            _attempt(
                "s1",
                "c1",
                "m101",
                mode,
                f"e{index}",
                index,
                index % 2,
                activity=f"a{index % 3}",
            )
            for index, mode in enumerate(modes, start=1)
        ]
    )

    trajectory = build_first_attempt_trajectory(
        attempts,
        min_activity_exercises=31,
        sequence_scope="module",
    )

    assert set(trajectory["work_mode"]) == {"playlist"}
    assert len(trajectory) == 31
    assert trajectory["attempt_position"].tolist() == list(range(31))


def test_interrupted_work_mode_runs_are_not_combined() -> None:
    modes = ["zpdes", "zpdes", "playlist", "playlist", "zpdes", "zpdes"]
    attempts = pd.DataFrame(
        [
            _attempt(
                "s1",
                "c1",
                "m101",
                mode,
                f"e{index}",
                index,
                index % 2,
                activity="a1",
            )
            for index, mode in enumerate(modes, start=1)
        ]
    )

    trajectory = build_first_attempt_trajectory(attempts)

    assert trajectory.empty


def test_gpboost_inputs_encode_requested_fixed_and_random_effects() -> None:
    attempts = pd.DataFrame(
        [
            _attempt("s1", "c1", "m1", "playlist", "e1", 1, 0),
            _attempt("s1", "c1", "m1", "playlist", "e2", 2, 1),
            _attempt("s2", "c2", "m1", "zpdes", "e1", 1, 1),
            _attempt("s2", "c2", "m1", "zpdes", "e2", 2, 1),
        ]
    )
    trajectory = build_first_attempt_trajectory(attempts, min_activity_exercises=2)

    response, fixed_effects, group_data = prepare_gpboost_inputs(trajectory)

    assert response.tolist() == [0.0, 1.0, 1.0, 1.0]
    assert fixed_effects.columns.tolist() == [
        "Intercept",
        "zpdes",
        "attempt_position",
        "zpdes_x_attempt_position",
    ]
    assert GROUP_COLUMNS == ["classroom_id", "student_in_classroom"]
    assert group_data.columns.tolist() == GROUP_COLUMNS
    assert fixed_effects.loc[trajectory["work_mode"].eq("playlist"), "zpdes"].eq(0).all()
    assert fixed_effects.loc[trajectory["work_mode"].eq("zpdes"), "zpdes"].eq(1).all()


def test_empirical_summary_averages_success_and_exercise_elo_by_position() -> None:
    trajectory = pd.DataFrame(
        {
            "student_id": ["s1", "s2", "s1", "s2"],
            "module": ["m1"] * 4,
            "module_sequence_id": [1, 1, 1, 1],
            "work_mode": ["playlist", "playlist", "zpdes", "zpdes"],
            "attempt_position": [0, 0, 0, 0],
            "success": [0, 1, 1, 1],
            "exercise_id": ["e1", "e2", "e3", "e4"],
            "activity_id": ["catalog_1", "a1", "a2", "a2"],
        }
    )
    exercise_elo = pd.DataFrame(
        {
            "exercise_id": ["e1", "e1", "e2", "e3", "e4"],
            "activity_id": ["a1", "a9", "a1", "a2", "a2"],
            "exercise_elo": [1000.0, 1200.0, 1300.0, 1400.0, 1600.0],
            "calibrated": [True, True, True, True, False],
        }
    )

    summary = build_empirical_trajectory_summary(trajectory, exercise_elo).set_index(
        "work_mode"
    )

    assert summary.loc["playlist", "success_rate"] == 0.5
    assert summary.loc["playlist", "mean_exercise_elo"] == 1200.0
    assert summary.loc["playlist", "elo_coverage"] == 1.0
    assert summary.loc["playlist", "exact_context_elo_attempt_rows"] == 1
    assert summary.loc["playlist", "exercise_id_fallback_elo_attempt_rows"] == 1
    assert summary.loc["zpdes", "success_rate"] == 1.0
    assert summary.loc["zpdes", "mean_exercise_elo"] == 1400.0
    assert summary.loc["zpdes", "elo_coverage"] == 0.5


def test_fixed_effect_curve_uses_playlist_and_zpdes_slopes() -> None:
    fixed_effects = pd.DataFrame(
        {
            "term": [
                "Intercept",
                "zpdes",
                "attempt_position",
                "zpdes_x_attempt_position",
            ],
            "estimate": [-0.85, 0.44, 0.10, 0.05],
        }
    )

    curve = build_fixed_effect_trajectory(fixed_effects, positions=[0, 9])
    first = curve[curve["attempt_position"].eq(0)].set_index("work_mode")
    tenth = curve[curve["attempt_position"].eq(9)].set_index("work_mode")

    assert np.isclose(first.loc["playlist", "predicted_probability"], 0.2994, atol=1e-3)
    assert np.isclose(first.loc["zpdes", "predicted_probability"], 0.3989, atol=1e-3)
    assert tenth.loc["zpdes", "predicted_probability"] > tenth.loc[
        "playlist", "predicted_probability"
    ]


def _synthetic_trajectory() -> pd.DataFrame:
    rng = np.random.default_rng(20260630)
    exercise_effects = {
        (module, exercise): rng.normal(0, 0.15)
        for module in range(2)
        for exercise in range(20)
    }
    rows = []
    for classroom in range(6):
        classroom_effect = rng.normal(0, 0.15)
        for student in range(10):
            student_id = f"s_{classroom}_{student}"
            student_effect = rng.normal(0, 0.25)
            for module in range(2):
                positions = {"playlist": 0, "zpdes": 0}
                for exercise in range(20):
                    mode = "zpdes" if (exercise + student) % 2 else "playlist"
                    position = positions[mode]
                    positions[mode] += 1
                    zpdes = mode == "zpdes"
                    linear_predictor = (
                        -1.0
                        + 0.2 * zpdes
                        + 0.04 * position
                        + 0.12 * zpdes * position
                        + classroom_effect
                        + student_effect
                        + exercise_effects[(module, exercise)]
                    )
                    probability = 1.0 / (1.0 + np.exp(-linear_predictor))
                    rows.append(
                        {
                            "success": rng.binomial(1, probability),
                            "work_mode": mode,
                            "attempt_position": position,
                            "classroom_id": f"c_{classroom}",
                            "student_id": student_id,
                            "student_in_classroom": f"c_{classroom}\x1f{student_id}",
                            "module": f"m_{module}",
                            "activity_id": f"a_{module}",
                            "activity_sequence_id": 0,
                            "exercise_id": f"m_{module}_e_{exercise}",
                        }
                    )
    return pd.DataFrame(rows)


def test_gpboost_model_recovers_positive_slope_difference() -> None:
    result = fit_gpboost_trajectory(
        _synthetic_trajectory(),
        population="synthetic",
        maxiter=200,
    )

    assert result.summary["status"] == "ok"
    assert result.summary["converged"] is True
    assert result.summary["slope_difference"] > 0
    assert set(result.variance_components["group"]) == set(GROUP_COLUMNS)

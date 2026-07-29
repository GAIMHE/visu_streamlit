from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.model_work_mode_sequence_slope import (
    GROUP_COLUMNS,
    _standard_error_diagnostics,
    build_adjusted_probability_summary,
    build_sequence_slope_trajectory,
    fit_sequence_slope_model,
    prepare_sequence_slope_inputs,
)


def _attempts_and_elo() -> tuple[pd.DataFrame, pd.DataFrame]:
    attempts = []
    elo_rows = []
    minute = 0
    for student_index in range(2):
        for work_mode in ("playlist", "zpdes"):
            for exercise_index in range(4):
                minute += 1
                exercise_id = f"s{student_index}_{work_mode}_e{exercise_index}"
                attempts.append(
                    {
                        "student_id": f"student_{student_index}",
                        "classroom_id": "classroom_1",
                        "module": "module_1",
                        "activity_id": "activity_1",
                        "playlist_id": "playlist_1" if work_mode == "playlist" else pd.NA,
                        "work_mode": work_mode,
                        "exercise_id": exercise_id,
                        "created_at": pd.Timestamp("2026-01-01", tz="UTC")
                        + pd.Timedelta(minutes=minute),
                        "success": float(exercise_index >= 2),
                    }
                )
                elo_rows.append(
                    {
                        "exercise_id": exercise_id,
                        "activity_id": "activity_1",
                        "exercise_elo": 1400.0 + exercise_index * 10.0,
                        "calibrated": True,
                    }
                )
    return pd.DataFrame(attempts), pd.DataFrame(elo_rows)


def test_build_sequence_slope_trajectory_normalizes_each_sequence() -> None:
    attempts, exercise_elo = _attempts_and_elo()

    trajectory = build_sequence_slope_trajectory(
        attempts,
        exercise_elo,
        min_sequence_exercises=4,
    )

    assert trajectory["sequence_id"].nunique() == 4
    assert trajectory.groupby("sequence_id")["normalized_position"].min().eq(0.0).all()
    assert trajectory.groupby("sequence_id")["normalized_position"].max().eq(1.0).all()
    assert trajectory["exercise_elo"].notna().all()


def test_prepare_sequence_slope_inputs_builds_sequence_random_slope() -> None:
    attempts, exercise_elo = _attempts_and_elo()
    trajectory = build_sequence_slope_trajectory(attempts, exercise_elo, 4)

    prepared = prepare_sequence_slope_inputs(trajectory)

    assert prepared.fixed_effects.columns.tolist() == [
        "Intercept",
        "zpdes",
        "centered_position",
        "zpdes_x_centered_position",
        "exercise_elo_centered_100",
    ]
    assert prepared.group_data.columns.tolist() == GROUP_COLUMNS
    assert prepared.sequence_random_slope.columns.tolist() == ["centered_position"]
    assert prepared.sequence_random_slope["centered_position"].min() == -0.5
    assert prepared.sequence_random_slope["centered_position"].max() == 0.5
    assert abs(prepared.fixed_effects["zpdes"].mean()) < 1e-12
    assert prepared.zpdes_reference == 0.5
    assert abs(prepared.fixed_effects["exercise_elo_centered_100"].mean()) < 1e-12


def test_adjusted_probability_summary_returns_start_to_end_change() -> None:
    fixed_effects = pd.DataFrame(
        {
            "term": [
                "Intercept",
                "zpdes",
                "centered_position",
                "zpdes_x_centered_position",
                "exercise_elo_centered_100",
            ],
            "estimate": [
                0.75 * np.log(3.0),
                0.5 * np.log(3.0),
                1.5 * np.log(3.0),
                np.log(3.0),
                -0.1,
            ],
        }
    )

    summary = build_adjusted_probability_summary(
        fixed_effects,
        "both_modes",
        1500.0,
        zpdes_reference=0.5,
    )
    summary = summary.set_index("work_mode")

    assert summary.loc["playlist", "adjusted_start_probability"] == 0.5
    assert np.isclose(summary.loc["playlist", "adjusted_end_probability"], 0.75)
    assert np.isclose(summary.loc["playlist", "adjusted_change_points"], 25.0)
    assert np.isclose(summary.loc["zpdes", "adjusted_end_probability"], 0.9)
    assert np.isclose(summary.loc["zpdes", "adjusted_change_points"], 40.0)


def test_reportability_keeps_valid_slope_inference_when_baseline_se_is_invalid() -> None:
    coefficient_table = pd.DataFrame(
        {
            "term": [
                "Intercept",
                "zpdes",
                "centered_position",
                "zpdes_x_centered_position",
                "exercise_elo_centered_100",
            ],
            "std_error": [np.nan, 0.0, 0.02, 0.03, 0.01],
        }
    )

    diagnostics = _standard_error_diagnostics(coefficient_table, converged=True)

    assert diagnostics["slope_comparison_reportable"] is True
    assert diagnostics["probability_levels_reportable"] is False
    assert diagnostics["invalid_standard_error_terms"] == ["Intercept", "zpdes"]


def test_fit_sequence_slope_model_exposes_principal_interaction() -> None:
    rng = np.random.default_rng(12)
    rows = []
    for sequence_id in range(80):
        zpdes = float(sequence_id % 2)
        student_id = sequence_id // 2
        classroom_id = student_id // 5
        sequence_slope = rng.normal(0.0, 0.25)
        for position_index in range(8):
            position = position_index / 7.0
            exercise_elo = rng.normal(1500.0, 80.0)
            linear_predictor = (
                -1.0
                + 0.2 * zpdes
                + 0.7 * position
                + 0.5 * zpdes * position
                - 0.002 * (exercise_elo - 1500.0)
                + sequence_slope * position
            )
            probability = 1.0 / (1.0 + np.exp(-linear_predictor))
            rows.append(
                {
                    "student_id": f"student_{student_id}",
                    "classroom_id": f"classroom_{classroom_id}",
                    "module": "module_1",
                    "work_mode": "zpdes" if zpdes else "playlist",
                    "sequence_id": sequence_id,
                    "normalized_position": position,
                    "exercise_elo": exercise_elo,
                    "success": float(rng.binomial(1, probability)),
                }
            )

    result = fit_sequence_slope_model(
        pd.DataFrame(rows),
        population="both_modes",
        maxiter=100,
    )

    assert result.summary["status"] in {"ok", "not_converged"}
    assert "slope_difference_log_odds" in result.summary
    assert "zpdes_x_centered_position" in set(result.fixed_effects["term"])
    assert set(GROUP_COLUMNS).issubset(set(result.variance_components["group"]))
    assert "sequence_id_rand_coef_centered_position" in set(result.variance_components["group"])

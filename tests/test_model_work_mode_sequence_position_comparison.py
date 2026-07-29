from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.model_work_mode_sequence_position_comparison import (
    BINARY_HALF,
    CONTINUOUS,
    add_binary_half_indicator,
    build_adjusted_change_summary,
    build_adjusted_probability_curve,
    build_half_assignment_audit,
    fit_position_model,
    prepare_position_inputs,
)


def _trajectory(sequence_length: int = 5) -> pd.DataFrame:
    rows = []
    for sequence_id, work_mode in enumerate(("playlist", "zpdes")):
        for index in range(sequence_length):
            rows.append(
                {
                    "student_id": f"student_{sequence_id}",
                    "classroom_id": "classroom_1",
                    "module": "module_1",
                    "work_mode": work_mode,
                    "sequence_id": sequence_id,
                    "normalized_position": index / (sequence_length - 1),
                    "success": float(index >= sequence_length // 2),
                }
            )
    return pd.DataFrame(rows)


def test_binary_half_retains_odd_sequence_midpoint_in_first_half() -> None:
    trajectory = add_binary_half_indicator(_trajectory(5))

    assert len(trajectory) == 10
    midpoint = trajectory[trajectory["normalized_position"].eq(0.5)]
    assert len(midpoint) == 2
    assert midpoint["is_exact_midpoint"].all()
    assert midpoint["later_half"].eq(0).all()

    audit = build_half_assignment_audit(trajectory)
    counts = audit.groupby("half")["attempt_rows"].sum().to_dict()
    assert counts == {"first": 6, "later": 4}


def test_prepared_models_use_identical_rows_groups_and_response_without_elo() -> None:
    trajectory = add_binary_half_indicator(_trajectory(6))
    trajectory["exercise_elo"] = np.nan

    continuous = prepare_position_inputs(trajectory, CONTINUOUS)
    binary = prepare_position_inputs(trajectory, BINARY_HALF)

    assert len(continuous.frame) == len(trajectory)
    assert len(binary.frame) == len(trajectory)
    assert continuous.frame.index.tolist() == binary.frame.index.tolist()
    np.testing.assert_array_equal(continuous.response, binary.response)
    pd.testing.assert_frame_equal(continuous.group_data, binary.group_data)
    assert continuous.fixed_effects.columns.tolist() == [
        "Intercept",
        "zpdes",
        "position",
        "zpdes_x_position",
    ]
    assert binary.fixed_effects.columns.tolist() == continuous.fixed_effects.columns.tolist()
    np.testing.assert_allclose(
        continuous.fixed_effects["zpdes"],
        binary.fixed_effects["zpdes"],
    )
    assert not np.array_equal(
        continuous.fixed_effects["position"],
        binary.fixed_effects["position"],
    )


def test_adjusted_change_summary_uses_endpoints_or_halves() -> None:
    fixed_effects = pd.DataFrame(
        {
            "term": [
                "Intercept",
                "zpdes",
                "position",
                "zpdes_x_position",
            ],
            "estimate": [
                0.75 * np.log(3.0),
                0.5 * np.log(3.0),
                1.5 * np.log(3.0),
                np.log(3.0),
            ],
        }
    )

    continuous = build_adjusted_change_summary(
        fixed_effects,
        position_encoding=CONTINUOUS,
        population="combined",
        zpdes_reference=0.5,
    ).set_index("work_mode")
    binary = build_adjusted_change_summary(
        fixed_effects,
        position_encoding=BINARY_HALF,
        population="combined",
        zpdes_reference=0.5,
    ).set_index("work_mode")

    assert continuous.loc["playlist", "period_0_label"] == "start"
    assert binary.loc["playlist", "period_0_label"] == "first_half"
    assert np.isclose(continuous.loc["playlist", "adjusted_change_points"], 25.0)
    assert np.isclose(binary.loc["zpdes", "adjusted_change_points"], 40.0)

    curve = build_adjusted_probability_curve(
        fixed_effects,
        position_encoding=BINARY_HALF,
        population="combined",
        zpdes_reference=0.5,
        grid_points=5,
    )
    playlist_curve = curve[curve["work_mode"].eq("playlist")]
    assert (
        playlist_curve.loc[playlist_curve["normalized_position"].eq(0.5), "model_position"].item()
        == 0.0
    )
    assert (
        playlist_curve.loc[playlist_curve["normalized_position"].eq(0.75), "model_position"].item()
        == 1.0
    )


def test_binary_position_model_exposes_matched_interaction() -> None:
    rng = np.random.default_rng(23)
    rows = []
    for sequence_id in range(80):
        zpdes = float(sequence_id % 2)
        student_id = sequence_id // 2
        classroom_id = student_id // 5
        sequence_shift = rng.normal(0.0, 0.25)
        for position_index in range(8):
            position = position_index / 7.0
            later = float(position > 0.5)
            linear_predictor = (
                -1.0
                + 0.2 * zpdes
                + 0.7 * later
                + 0.5 * zpdes * later
                + sequence_shift * later
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
                    "success": float(rng.binomial(1, probability)),
                }
            )

    result = fit_position_model(
        add_binary_half_indicator(pd.DataFrame(rows)),
        position_encoding=BINARY_HALF,
        population="combined",
        maxiter=100,
    )

    assert result.summary["status"] in {"ok", "not_converged"}
    assert result.summary["n_rows"] == len(rows)
    assert "zpdes_x_position" in set(result.fixed_effects["term"])
    assert "sequence_id_rand_coef_position" in set(result.variance_components["group"])

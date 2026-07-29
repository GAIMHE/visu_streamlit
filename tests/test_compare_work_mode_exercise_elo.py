from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.compare_work_mode_exercise_elo import (
    build_calibration_coverage,
    build_matched_work_mode_elo,
    build_work_mode_agreement_metrics,
    lin_concordance_correlation,
)


def _elo_table(
    values: dict[tuple[str, str], tuple[float, int]],
) -> pd.DataFrame:
    rows = []
    for (module_code, exercise_id), (elo, attempts) in values.items():
        rows.append(
            {
                "module_code": module_code,
                "objective_id": "objective_1",
                "activity_id": "activity_1",
                "exercise_id": exercise_id,
                "exercise_label": exercise_id,
                "exercise_type": "test",
                "module_id": module_code.lower(),
                "module_label": module_code,
                "objective_label": "Objective 1",
                "activity_label": "Activity 1",
                "exercise_elo": elo,
                "calibration_attempts": attempts,
                "calibration_success_rate": 0.5,
                "calibrated": True,
            }
        )
    return pd.DataFrame(rows)


def test_matched_comparison_keeps_shared_contexts_and_aligns_module_offset() -> None:
    all_mode = _elo_table(
        {
            ("M1", "e1"): (1450.0, 80),
            ("M1", "e2"): (1550.0, 80),
            ("M1", "all_only"): (1500.0, 10),
        }
    )
    playlist = _elo_table(
        {
            ("M1", "e1"): (1400.0, 40),
            ("M1", "e2"): (1600.0, 60),
            ("M1", "playlist_only"): (1500.0, 20),
        }
    )
    zpdes = _elo_table(
        {
            ("M1", "e1"): (1500.0, 50),
            ("M1", "e2"): (1700.0, 70),
            ("M1", "zpdes_only"): (1300.0, 20),
        }
    )

    matched = build_matched_work_mode_elo(
        all_mode,
        {"playlist": playlist, "zpdes": zpdes},
    )

    assert matched["exercise_id"].tolist() == ["e1", "e2"]
    assert matched["elo_difference_raw"].tolist() == [100.0, 100.0]
    assert np.allclose(matched["elo_difference_aligned"], 0.0)
    assert matched["min_mode_attempts"].tolist() == [40, 60]


def test_agreement_metrics_apply_attempt_threshold_and_preserve_direction() -> None:
    all_mode = _elo_table(
        {
            ("M1", "e1"): (1400.0, 100),
            ("M1", "e2"): (1500.0, 100),
            ("M1", "e3"): (1600.0, 100),
        }
    )
    playlist = _elo_table(
        {
            ("M1", "e1"): (1400.0, 5),
            ("M1", "e2"): (1500.0, 30),
            ("M1", "e3"): (1600.0, 40),
        }
    )
    zpdes = _elo_table(
        {
            ("M1", "e1"): (1400.0, 50),
            ("M1", "e2"): (1550.0, 30),
            ("M1", "e3"): (1650.0, 40),
        }
    )
    matched = build_matched_work_mode_elo(
        all_mode,
        {"playlist": playlist, "zpdes": zpdes},
    )

    metrics = build_work_mode_agreement_metrics(matched, thresholds=[1, 25])
    raw_25 = metrics.loc[
        (metrics["scale"] == "raw") & (metrics["min_attempts_per_mode"] == 25)
    ].iloc[0]
    aligned_25 = metrics.loc[
        (metrics["scale"] == "shared_module_aligned") & (metrics["min_attempts_per_mode"] == 25)
    ].iloc[0]

    assert raw_25["contexts"] == 2
    assert raw_25["mean_difference"] == 50.0
    assert raw_25["mean_absolute_difference"] == 50.0
    assert raw_25["within_50_elo_percent"] == 100.0
    assert np.isclose(aligned_25["mean_absolute_difference"], 0.0)


def test_lin_concordance_penalizes_a_constant_offset() -> None:
    left = pd.Series([1400.0, 1500.0, 1600.0])

    assert lin_concordance_correlation(left, left) == 1.0
    assert lin_concordance_correlation(left, left + 100.0) < 1.0


def test_calibration_coverage_reports_only_calibrated_rows() -> None:
    all_mode = _elo_table({("M1", "e1"): (1500.0, 100)})
    playlist = _elo_table({("M1", "e1"): (1490.0, 40)})
    zpdes = _elo_table({("M1", "e1"): (1510.0, 60)})
    uncalibrated = playlist.iloc[[0]].copy()
    uncalibrated["exercise_id"] = "e2"
    uncalibrated["calibrated"] = False
    uncalibrated["calibration_attempts"] = 0
    uncalibrated["exercise_elo"] = np.nan
    playlist = pd.concat([playlist, uncalibrated], ignore_index=True)

    coverage = build_calibration_coverage(
        all_mode,
        {"playlist": playlist, "zpdes": zpdes},
    ).set_index("calibration")

    assert coverage.loc["playlist", "calibrated_contexts"] == 1
    assert coverage.loc["playlist", "first_attempts"] == 40

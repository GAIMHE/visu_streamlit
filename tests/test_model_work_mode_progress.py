from __future__ import annotations

import json
from argparse import Namespace

import numpy as np
import pandas as pd

from scripts.model_work_mode_progress import (
    _keep_only_single_module_playlists,
    _prepare_gpboost_half_success_inputs,
    _prepare_gpboost_progress_inputs,
    _read_mia_exercise_catalog,
    _student_classroom_error,
    build_activity_level,
    fit_half_success_model,
    fit_mixed_model,
    fit_population_interaction_model,
    load_attempts,
    split_populations,
    summarize_half_exercise_elo,
)


def test_multi_module_playlists_are_included_by_default() -> None:
    assert _keep_only_single_module_playlists(Namespace()) is False
    assert (
        _keep_only_single_module_playlists(
            Namespace(keep_only_single_module_playlists=True)
        )
        is True
    )


def test_catalog_uses_config_learning_items_and_uuid_activity(tmp_path) -> None:
    module_config = {
        "config": {
            "module": {
                "101": {
                    "code": "M101",
                    "title": {"short": "Module 101"},
                },
            },
            "activity": {
                "M101O2A3": {
                    "id": "activity-uuid",
                    "code": "M101O2A3",
                    "learning_items": ["exercise_1"],
                }
            },
        }
    }
    exercise_catalog = {
        "exercises": [
            {
                "id": "exercise_1",
                # These local ordinals are deliberately misleading and must
                # not override the configured UUID-backed hierarchy.
                "modules": ["1"],
                "objectives": ["1"],
                "activities": ["1"],
            }
        ]
    }
    module_path = tmp_path / "modules.json"
    exercise_path = tmp_path / "exercises.json"
    module_path.write_text(json.dumps(module_config), encoding="utf-8")
    exercise_path.write_text(json.dumps(exercise_catalog), encoding="utf-8")

    catalog = _read_mia_exercise_catalog(exercise_path, module_path)

    assert catalog.to_dict("records") == [
        {
            "exercise_id": "exercise_1",
            "catalog_module": "Module 101",
            "catalog_activity_id": "activity-uuid",
        }
    ]


def test_catalog_omits_exercises_with_ambiguous_activity_context(tmp_path) -> None:
    module_config = {
        "config": {
            "module": {
                "101": {"code": "M101", "title": {"short": "Module 101"}},
            },
            "activity": {
                "M101O1A1": {
                    "id": "activity-1",
                    "code": "M101O1A1",
                    "learning_items": ["shared_exercise", "unique_exercise"],
                },
                "M101O1A2": {
                    "id": "activity-2",
                    "code": "M101O1A2",
                    "learning_items": ["shared_exercise"],
                },
            },
        }
    }
    module_path = tmp_path / "modules.json"
    exercise_path = tmp_path / "exercises.json"
    module_path.write_text(json.dumps(module_config), encoding="utf-8")
    exercise_path.write_text(json.dumps({"exercises": []}), encoding="utf-8")

    catalog = _read_mia_exercise_catalog(exercise_path, module_path)

    assert catalog["exercise_id"].tolist() == ["unique_exercise"]


def test_load_attempts_accepts_mixed_valid_timestamp_formats(tmp_path) -> None:
    input_path = tmp_path / "attempts.parquet"
    pd.DataFrame(
        {
            "user_id": ["student", "student"],
            "classroom_id": ["classroom", "classroom"],
            "playlist_or_module_id": ["playlist", "playlist"],
            "exercise_id": ["exercise_1", "exercise_2"],
            "activity_id": [None, None],
            "module_short_title": [None, None],
            "created_at": [
                "2025-04-21 10:07:04.486000+00:00",
                "2025-04-21 10:41:45+00:00",
            ],
            "data_correct": [True, False],
            "work_mode": ["playlist", "playlist"],
        }
    ).to_parquet(input_path, index=False)
    module_config = {
        "config": {
            "module": {
                "101": {"code": "M101", "title": {"short": "Module 101"}},
            },
            "activity": {
                "M101O1A1": {
                    "id": "activity-uuid",
                    "code": "M101O1A1",
                    "learning_items": ["exercise_1", "exercise_2"],
                }
            },
        }
    }
    module_path = tmp_path / "config.json"
    exercise_path = tmp_path / "exercises.json"
    module_path.write_text(json.dumps(module_config), encoding="utf-8")
    exercise_path.write_text(json.dumps({"exercises": []}), encoding="utf-8")

    attempts = load_attempts(
        Namespace(
            input_file=input_path,
            input_csv=None,
            exercise_catalog_json=exercise_path,
            module_config_json=module_path,
            keep_only_single_module_playlists=False,
        )
    )

    assert attempts["exercise_id"].tolist() == ["exercise_1", "exercise_2"]
    assert attempts["created_at"].notna().all()
    assert attempts["activity_id"].eq("activity-uuid").all()
    assert attempts["module"].eq("Module 101").all()


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


def test_primary_model_uses_classroom_and_student_intercepts() -> None:
    activity = _synthetic_activity_data()

    summary = fit_mixed_model(activity, population="synthetic", maxiter=200)

    assert summary.status == "ok"
    assert summary.converged is True
    assert summary.random_student_var is not None
    assert summary.random_classroom_var is not None
    assert "student within classroom" not in summary.model_specification
    assert "activity" not in summary.model_specification
    assert "activity_id" not in (summary.variance_components or "")
    assert 8.0 < summary.estimate_zpdes_vs_playlist < 10.0
    assert np.isclose(
        summary.zpdes_adjusted_mean - summary.playlist_adjusted_mean,
        summary.estimate_zpdes_vs_playlist,
    )


def test_gpboost_groups_use_unique_student_ids() -> None:
    activity = _synthetic_activity_data()

    _, _, group_data = _prepare_gpboost_progress_inputs(activity)

    assert list(group_data.columns) == ["classroom_id", "student_id"]
    assert group_data["student_id"].nunique() == activity["student_id"].nunique()


def test_half_success_model_recovers_initial_difference_and_changes() -> None:
    activity = _synthetic_activity_data()
    rng = np.random.default_rng(7)
    is_zpdes = activity["work_mode"].eq("zpdes").astype(float)
    shared_noise = rng.normal(0, 0.025, len(activity))
    activity["success_rate_first"] = np.clip(
        0.72 - 0.10 * is_zpdes + shared_noise,
        0.01,
        0.99,
    )
    activity["success_rate_later"] = np.clip(
        activity["success_rate_first"]
        + 0.02 * (1 - is_zpdes)
        + 0.15 * is_zpdes
        + rng.normal(0, 0.015, len(activity)),
        0.01,
        0.99,
    )

    summary = fit_half_success_model(activity, population="synthetic", maxiter=200)

    assert summary.status == "ok"
    assert summary.converged is True
    assert -11.0 < summary.initial_zpdes_vs_playlist < -9.0
    assert 1.0 < summary.playlist_change < 3.0
    assert 14.0 < summary.zpdes_change < 16.0
    assert 12.0 < summary.difference_in_differences < 14.0
    assert summary.random_sequence_var is not None


def test_half_success_inputs_pair_sequence_random_intercepts() -> None:
    activity = _synthetic_activity_data().head(12).copy()
    activity["success_rate_first"] = 0.5
    activity["success_rate_later"] = 0.75

    response, fixed_effects, group_data = _prepare_gpboost_half_success_inputs(activity)

    assert len(response) == len(activity) * 2
    assert list(fixed_effects.columns) == [
        "Intercept",
        "zpdes",
        "later_half",
        "zpdes_x_later_half",
    ]
    assert list(group_data.columns) == ["classroom_id", "student_id", "sequence_id"]
    assert group_data.groupby("sequence_id").size().eq(2).all()


def test_activity_level_summarizes_first_and_later_half_exercise_elo() -> None:
    rows = []
    elo_rows = []
    mode_elos = {
        "playlist": [1400.0, 1410.0, 1500.0, 1510.0],
        "zpdes": [1600.0, 1610.0, 1700.0, 1710.0],
    }
    for mode, elo_values in mode_elos.items():
        for position, elo in enumerate(elo_values):
            exercise_id = f"{mode}_exercise_{position}"
            rows.append(
                {
                    "student_id": f"{mode}_student",
                    "classroom_id": "classroom",
                    "module": "module",
                    "activity_id": "activity",
                    "playlist_id": "playlist" if mode == "playlist" else pd.NA,
                    "work_mode": mode,
                    "exercise_id": exercise_id,
                    "created_at": pd.Timestamp("2025-01-01", tz="UTC")
                    + pd.Timedelta(minutes=position),
                    "success": float(position % 2),
                }
            )
            elo_rows.append(
                {
                    "exercise_id": exercise_id,
                    "activity_id": "activity",
                    "exercise_elo": elo,
                    "calibrated": True,
                }
            )

    activity = build_activity_level(
        pd.DataFrame(rows),
        min_activity_exercises=4,
        exercise_elo=pd.DataFrame(elo_rows),
    )
    summary = summarize_half_exercise_elo(activity).set_index(["work_mode", "half"])

    assert summary.loc[("playlist", "first"), "mean_sequence_elo"] == 1405.0
    assert summary.loc[("playlist", "later"), "mean_sequence_elo"] == 1505.0
    assert summary.loc[("zpdes", "first"), "median_sequence_elo"] == 1605.0
    assert summary.loc[("zpdes", "later"), "median_sequence_elo"] == 1705.0
    assert summary["elo_coverage"].eq(1.0).all()
    assert summary["fallback_rows"].eq(0).all()


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
                "playlist_id": "playlist_1",
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


def test_playlist_activity_progress_is_separate_between_playlists() -> None:
    rows = []
    minute = 0
    for playlist_id, outcomes in {
        "playlist_1": [0, 0, 1, 1],
        "playlist_2": [1, 1, 0, 0],
    }.items():
        for exercise_index, success in enumerate(outcomes):
            minute += 1
            rows.append(
                {
                    "student_id": "student_1",
                    "classroom_id": "classroom_1",
                    "module": "module_1",
                    "activity_id": "M1O1A1",
                    "playlist_id": playlist_id,
                    "work_mode": "playlist",
                    "exercise_id": f"{playlist_id}_exercise_{exercise_index}",
                    "created_at": pd.Timestamp("2026-01-01", tz="UTC")
                    + pd.Timedelta(minutes=minute),
                    "success": success,
                }
            )

    activity = build_activity_level(pd.DataFrame(rows), min_activity_exercises=4)

    assert len(activity) == 2
    assert set(activity["mean_progress"]) == {-100.0, 100.0}
    assert activity["activity_id"].nunique() == 2


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

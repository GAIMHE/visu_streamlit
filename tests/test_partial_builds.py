from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

from visu2.config import Settings
from visu2.derive import write_derived_tables


def _build_settings(tmp_path: Path, *, source_id: str = "main") -> Settings:
    runtime_root = tmp_path / "artifacts" / "sources" / source_id
    local_root = tmp_path / "artifacts" / "local" / source_id
    legacy_root = tmp_path / "artifacts" / "legacy" / source_id
    data_dir = runtime_root / "data"
    artifacts_dir = runtime_root / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"
    local_data_dir = local_root / "data"
    local_reports_dir = local_root / "artifacts" / "reports"
    resources_dir = tmp_path / "ressources"
    for path in (
        data_dir,
        derived_dir,
        reports_dir,
        local_data_dir,
        local_reports_dir,
        legacy_root / "artifacts" / "reports",
        resources_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return Settings(
        root_dir=tmp_path,
        data_dir=data_dir,
        resources_dir=resources_dir,
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=local_data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=local_data_dir / "exercises.json",
        consistency_report_path=local_reports_dir / "consistency_report.json",
        derived_manifest_path=local_reports_dir / "derived_manifest.json",
        runtime_root_dir=runtime_root,
        local_root_dir=local_root,
        legacy_root_dir=legacy_root,
        source_id=source_id,
        source_label=source_id,
    )


def test_partial_batch_replay_build_reuses_existing_runtime_dependencies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _build_settings(tmp_path, source_id="main")

    fact = pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 9, 5, 0),
            ],
            "date_utc": [datetime(2025, 1, 1).date(), datetime(2025, 1, 1).date()],
            "user_id": ["u1", "u1"],
            "classroom_id": ["c1", "c1"],
            "playlist_or_module_id": ["p1", "p1"],
            "objective_id": ["o1", "o1"],
            "objective_label": ["Objective 1", "Objective 1"],
            "activity_id": ["a1", "a1"],
            "activity_label": ["Activity 1", "Activity 1"],
            "exercise_id": ["e1", "e1"],
            "data_correct": [True, False],
            "data_duration": [10.0, 10.0],
            "session_duration": [10.0, 10.0],
            "work_mode": ["zpdes", "zpdes"],
            "attempt_number": [1, 2],
            "module_id": ["m1", "m1"],
            "module_code": ["M1", "M1"],
            "module_label": ["Module 1", "Module 1"],
        }
    )
    fact.write_parquet(settings.artifacts_derived_dir / "fact_attempt_core.parquet")

    exercise_elo = pl.DataFrame(
        {
            "exercise_id": ["e1"],
            "exercise_label": ["Exercise 1"],
            "exercise_type": ["unknown"],
            "module_id": ["m1"],
            "module_code": ["M1"],
            "module_label": ["Module 1"],
            "objective_id": ["o1"],
            "objective_label": ["Objective 1"],
            "activity_id": ["a1"],
            "activity_label": ["Activity 1"],
            "exercise_elo": [1510.0],
            "calibration_attempts": [1],
            "calibration_success_rate": [1.0],
            "calibrated": [True],
        }
    )
    exercise_elo.write_parquet(settings.artifacts_derived_dir / "agg_exercise_elo.parquet")

    def _unexpected_bundle(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("Partial build should reuse existing fact/elo dependencies.")

    monkeypatch.setattr("visu2.derive.build_hierarchy_resolution_bundle", _unexpected_bundle)

    outputs = write_derived_tables(
        settings,
        table_names=("student_elo_events_batch_replay", "student_elo_profiles_batch_replay"),
    )

    assert set(outputs) == {"student_elo_events_batch_replay", "student_elo_profiles_batch_replay"}
    assert outputs["student_elo_events_batch_replay"].exists()
    assert outputs["student_elo_profiles_batch_replay"].exists()

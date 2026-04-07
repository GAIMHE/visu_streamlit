from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from visu2.classroom_profile_loader import (
    CLASSROOM_PROFILE_FACT_COLUMNS,
    load_or_build_classroom_mode_profiles,
)


def _sample_profiles() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mode_scope": ["zpdes", "all"],
            "classroom_id": ["C1", "C1"],
            "students": [2, 2],
            "activities": [1, 1],
            "objectives": [1, 1],
            "modules": [1, 1],
            "attempts": [2, 2],
            "first_attempt_at": [
                datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            ],
            "last_attempt_at": [
                datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
                datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
            ],
        }
    ).with_columns(
        pl.col("first_attempt_at").cast(pl.Datetime(time_zone="UTC")),
        pl.col("last_attempt_at").cast(pl.Datetime(time_zone="UTC")),
    )


def _sample_fact() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "created_at": [
                datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
                datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
            ],
            "date_utc": [date(2024, 1, 1), date(2024, 1, 2)],
            "user_id": ["U1", "U2"],
            "activity_id": ["A1", "A1"],
            "activity_label": ["Activity 1", "Activity 1"],
            "data_correct": [1, 0],
            "work_mode": ["zpdes", "zpdes"],
            "classroom_id": ["C1", "C1"],
            "objective_id": ["O1", "O1"],
            "module_code": ["M1", "M1"],
            "exercise_id": ["E1", "E1"],
            "attempt_number": [1, 1],
        }
    ).select(CLASSROOM_PROFILE_FACT_COLUMNS)


def test_load_or_build_classroom_mode_profiles_prefers_valid_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    profiles_path = tmp_path / "artifacts" / "derived" / "classroom_mode_profiles.parquet"
    profiles_path.parent.mkdir(parents=True, exist_ok=True)
    expected = _sample_profiles()
    expected.write_parquet(profiles_path)
    settings = SimpleNamespace(artifacts_derived_dir=tmp_path / "artifacts" / "derived")

    def _unexpected_query(*args, **kwargs):  # pragma: no cover - safety
        raise AssertionError("query_runtime_parquet should not be called when the artifact is valid.")

    monkeypatch.setattr("visu2.classroom_profile_loader.query_runtime_parquet", _unexpected_query)

    profiles, source_kind = load_or_build_classroom_mode_profiles(settings)

    assert source_kind == "artifact"
    assert profiles.equals(expected)


def test_load_or_build_classroom_mode_profiles_falls_back_to_fact_when_missing(
    tmp_path: Path, monkeypatch
) -> None:
    settings = SimpleNamespace(artifacts_derived_dir=tmp_path / "artifacts" / "derived")
    fact = _sample_fact()

    monkeypatch.setattr(
        "visu2.classroom_profile_loader.query_runtime_parquet",
        lambda *args, **kwargs: fact,
    )

    profiles, source_kind = load_or_build_classroom_mode_profiles(settings)

    assert source_kind == "fact_fallback"
    assert profiles.height == 2
    assert sorted(profiles["mode_scope"].to_list()) == ["all", "zpdes"]
    assert (settings.artifacts_derived_dir / "classroom_mode_profiles.parquet").exists()

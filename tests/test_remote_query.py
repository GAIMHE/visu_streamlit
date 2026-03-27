from __future__ import annotations

import json
from datetime import UTC, date, datetime

import polars as pl

from visu2.classroom_progression import SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
from visu2.config import Settings
from visu2.remote_query import (
    _build_hf_runtime_url,
    query_fact_attempts,
    query_fact_attempts_for_classroom,
    query_runtime_parquet,
    query_student_fact_label_lookup,
    resolve_runtime_parquet_reference,
)


def _build_settings(tmp_path, *, source_id: str = "main") -> Settings:
    runtime_root = tmp_path / source_id
    data_dir = runtime_root / "data"
    artifacts_dir = runtime_root / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"
    resources_dir = tmp_path / "ressources"
    data_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    resources_dir.mkdir(parents=True, exist_ok=True)
    return Settings(
        root_dir=tmp_path,
        data_dir=data_dir,
        resources_dir=resources_dir,
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
        runtime_root_dir=runtime_root,
        source_id=source_id,
        source_label=source_id,
    )


def test_query_runtime_parquet_uses_local_fallback_and_projection(tmp_path) -> None:
    settings = _build_settings(tmp_path)
    table_path = settings.artifacts_derived_dir / "student_elo_events.parquet"
    pl.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "attempt_ordinal": [2, 1],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 2, 9, 0, tzinfo=UTC),
            ],
        }
    ).write_parquet(table_path)

    frame = query_runtime_parquet(
        settings,
        "artifacts/derived/student_elo_events.parquet",
        columns=("user_id", "attempt_ordinal"),
        filters=(("user_id", "=", "u1"),),
        order_by=(("attempt_ordinal", False),),
    )

    assert frame.columns == ["user_id", "attempt_ordinal"]
    assert frame.to_dicts() == [{"user_id": "u1", "attempt_ordinal": 2}]


def test_resolve_runtime_parquet_reference_uses_hf_source_when_local_missing(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path, source_id="maureen_m16fr")
    monkeypatch.setenv(
        "VISU2_HF_SOURCES_JSON",
        json.dumps(
            {
                "maureen_m16fr": {
                    "repo_id": "org/maureen",
                    "revision": "v1",
                }
            }
        ),
    )
    monkeypatch.setenv("HF_TOKEN", "token")

    reference, config = resolve_runtime_parquet_reference(
        settings,
        "artifacts/derived/student_elo_events.parquet",
    )

    assert reference == _build_hf_runtime_url(
        "org/maureen",
        "v1",
        "artifacts/derived/student_elo_events.parquet",
    )
    assert config is not None
    assert config.source_id == "maureen_m16fr"


def test_query_fact_attempts_for_classroom_omits_filter_for_synthetic_classroom(tmp_path) -> None:
    settings = _build_settings(tmp_path, source_id="maureen_m16fr")
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
            ],
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1)],
            "user_id": ["u1", "u2"],
            "activity_id": ["a1", "a2"],
            "activity_label": ["A1", "A2"],
            "data_correct": [1, 0],
            "work_mode": ["zpdes", "playlist"],
            "classroom_id": [None, None],
            "objective_id": ["o1", "o2"],
            "module_code": ["M1", "M1"],
            "exercise_id": ["e1", "e2"],
            "attempt_number": [1, 1],
        }
    ).write_parquet(fact_path)

    frame = query_fact_attempts_for_classroom(
        settings,
        classroom_id=SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID,
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        columns=(
            "created_at",
            "date_utc",
            "user_id",
            "activity_id",
            "activity_label",
            "data_correct",
            "work_mode",
            "classroom_id",
            "objective_id",
            "module_code",
            "exercise_id",
            "attempt_number",
        ),
    )

    assert frame.height == 1
    assert frame["user_id"].to_list() == ["u1"]


def test_query_fact_attempts_enforces_min_student_attempts(tmp_path) -> None:
    settings = _build_settings(tmp_path)
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 5, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 10, tzinfo=UTC),
            ],
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)],
            "user_id": ["u1", "u1", "u2"],
            "activity_id": ["a1", "a2", "a3"],
            "exercise_id": ["e1", "e2", "e3"],
            "attempt_number": [1, 1, 1],
            "module_code": ["M1", "M1", "M1"],
            "objective_id": ["o1", "o1", "o1"],
            "work_mode": ["zpdes", "zpdes", "zpdes"],
            "classroom_id": ["c1", "c1", "c1"],
        }
    ).write_parquet(fact_path)

    frame = query_fact_attempts(
        settings,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        columns=("user_id", "activity_id"),
        module_code="M1",
        min_student_attempts=2,
    )

    assert frame.to_dicts() == [
        {"user_id": "u1", "activity_id": "a1"},
        {"user_id": "u1", "activity_id": "a2"},
    ]


def test_query_student_fact_label_lookup_filters_selected_students(tmp_path) -> None:
    settings = _build_settings(tmp_path)
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 5, tzinfo=UTC),
            ],
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1)],
            "user_id": ["u1", "u2"],
            "activity_id": ["a_m51", "a_other"],
            "activity_label": ["Shared activity in M51", "Other activity"],
            "objective_id": ["o_m51", "o_other"],
            "objective_label": ["Shared objective", "Other objective"],
            "module_code": ["M51", "M1"],
            "module_label": ["Fractions level 1", "Numbers"],
            "exercise_id": ["e1", "e2"],
            "attempt_number": [1, 1],
            "classroom_id": ["c1", "c1"],
            "work_mode": ["adaptive-test", "zpdes"],
        }
    ).write_parquet(fact_path)

    lookup = query_student_fact_label_lookup(settings, user_ids=("u1",))

    assert lookup.to_dicts() == [
        {
            "activity_id": "a_m51",
            "module_code": "M51",
            "module_label": "Fractions level 1",
            "objective_id": "o_m51",
            "objective_label": "Shared objective",
            "activity_label": "Shared activity in M51",
        }
    ]

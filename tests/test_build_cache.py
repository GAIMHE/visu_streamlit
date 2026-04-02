from __future__ import annotations

import json
from pathlib import Path

from visu2.build_cache import (
    BUILD_CACHE_VERSION,
    build_source_input_snapshot,
    can_reuse_derived_build,
)
from visu2.config import Settings


def _build_settings(tmp_path: Path, *, source_id: str = "maureen_m16fr") -> Settings:
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
    data_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    local_data_dir.mkdir(parents=True, exist_ok=True)
    local_reports_dir.mkdir(parents=True, exist_ok=True)
    (legacy_root / "artifacts" / "reports").mkdir(parents=True, exist_ok=True)
    resources_dir.mkdir(parents=True, exist_ok=True)
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


def test_build_source_input_snapshot_uses_size_and_mtime(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    raw_inputs = {
        "attempts_csv": Path("data_maureen") / "attempts.csv",
        "module_config_csv": Path("data_maureen") / "config.csv",
    }
    for relative_path, content in [
        (raw_inputs["attempts_csv"], "a,b\n1,2\n"),
        (raw_inputs["module_config_csv"], "x;y\n3;4\n"),
    ]:
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    snapshot = build_source_input_snapshot(settings, raw_inputs)

    assert set(snapshot) == {"attempts_csv", "module_config_csv"}
    assert snapshot["attempts_csv"]["path"] == "data_maureen/attempts.csv"
    assert snapshot["attempts_csv"]["size_bytes"] > 0
    assert snapshot["attempts_csv"]["mtime_ns"] > 0


def test_can_reuse_derived_build_when_manifest_matches(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    expected_tables = ("fact_attempt_core", "agg_activity_daily")
    for runtime_path in [
        settings.parquet_path,
        settings.learning_catalog_path,
        settings.build_zpdes_rules_path,
        settings.exercises_json_path,
    ]:
        runtime_path.write_text("placeholder", encoding="utf-8")
    for table_name in expected_tables:
        (settings.artifacts_derived_dir / f"{table_name}.parquet").write_text(
            "placeholder",
            encoding="utf-8",
        )

    snapshot = {
        "attempts_csv": {
            "path": "data_maureen/attempts.csv",
            "size_bytes": 10,
            "mtime_ns": 20,
        }
    }
    manifest = {
        "manifest_version": 1,
        "generated_at_utc": "2026-03-26T00:00:00+00:00",
        "schema_version": 1,
        "cache_version": BUILD_CACHE_VERSION,
        "source_id": settings.source_id,
        "source_input_snapshot": snapshot,
        "build_context": {
            "sample_rows": None,
            "strict_checks": True,
            "checks_status": "pass",
        },
        "tables": {
            table_name: {"path": str(settings.artifacts_derived_dir / f"{table_name}.parquet")}
            for table_name in expected_tables
        },
    }
    settings.derived_manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    can_reuse, reason = can_reuse_derived_build(
        settings=settings,
        source_id=settings.source_id,
        expected_tables=expected_tables,
        sample_rows=None,
        source_input_snapshot=snapshot,
    )

    assert can_reuse is True
    assert "already exist" in reason


def test_can_reuse_derived_build_rejects_changed_inputs(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    manifest = {
        "manifest_version": 1,
        "generated_at_utc": "2026-03-26T00:00:00+00:00",
        "schema_version": 1,
        "cache_version": BUILD_CACHE_VERSION,
        "source_id": settings.source_id,
        "source_input_snapshot": {
            "attempts_csv": {
                "path": "data_maureen/attempts.csv",
                "size_bytes": 10,
                "mtime_ns": 20,
            }
        },
        "build_context": {
            "sample_rows": None,
            "strict_checks": False,
            "checks_status": "pass",
        },
        "tables": {
            "fact_attempt_core": {"path": "placeholder"},
        },
    }
    settings.derived_manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    can_reuse, reason = can_reuse_derived_build(
        settings=settings,
        source_id=settings.source_id,
        expected_tables=("fact_attempt_core",),
        sample_rows=None,
        source_input_snapshot={
            "attempts_csv": {
                "path": "data_maureen/attempts.csv",
                "size_bytes": 99,
                "mtime_ns": 20,
            }
        },
    )

    assert can_reuse is False
    assert "changed" in reason

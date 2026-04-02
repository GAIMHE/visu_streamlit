from __future__ import annotations

from pathlib import Path

from visu2.artifact_migration import migrate_source_artifacts
from visu2.config import Settings
from visu2.runtime_sources import get_runtime_source


def _build_settings(tmp_path: Path, *, source_id: str) -> Settings:
    runtime_root = tmp_path / "artifacts" / "sources" / source_id
    local_root = tmp_path / "artifacts" / "local" / source_id
    legacy_root = tmp_path / "artifacts" / "legacy" / source_id
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
        parquet_path=local_root / "data" / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=local_root / "data" / "exercises.json",
        consistency_report_path=local_root / "artifacts" / "reports" / "consistency_report.json",
        derived_manifest_path=local_root / "artifacts" / "reports" / "derived_manifest.json",
        runtime_root_dir=runtime_root,
        local_root_dir=local_root,
        legacy_root_dir=legacy_root,
        source_id=source_id,
        source_label=source_id,
    )


def test_migrate_source_artifacts_moves_local_and_legacy_files(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, source_id="main")
    source = get_runtime_source("main")
    runtime_root = settings.runtime_root

    local_runtime_path = runtime_root / "data" / "student_interaction.parquet"
    legacy_runtime_path = runtime_root / "artifacts" / "derived" / "work_mode_transition_paths.parquet"
    local_runtime_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_runtime_path.parent.mkdir(parents=True, exist_ok=True)
    local_runtime_path.write_text("local", encoding="utf-8")
    legacy_runtime_path.write_text("legacy", encoding="utf-8")

    summary = migrate_source_artifacts(settings, source=source)

    assert summary.count("moved") >= 2
    assert not local_runtime_path.exists()
    assert not legacy_runtime_path.exists()
    assert (settings.local_root / "data" / "student_interaction.parquet").read_text(encoding="utf-8") == "local"
    assert (
        settings.legacy_root / "artifacts" / "derived" / "work_mode_transition_paths.parquet"
    ).read_text(encoding="utf-8") == "legacy"


def test_migrate_source_artifacts_is_idempotent(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, source_id="maureen_m16fr")
    source = get_runtime_source("maureen_m16fr")
    runtime_root = settings.runtime_root

    runtime_path = runtime_root / "artifacts" / "reports" / "hierarchy_resolution_report.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text("{}", encoding="utf-8")

    first = migrate_source_artifacts(settings, source=source)
    second = migrate_source_artifacts(settings, source=source)

    assert first.count("moved") >= 1
    assert second.count("moved") == 0
    assert second.count("already_migrated") >= 1


def test_migrate_source_artifacts_removes_identical_runtime_duplicates(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, source_id="main")
    source = get_runtime_source("main")

    runtime_duplicate = settings.runtime_root / "data" / "student_interaction.parquet"
    local_target = settings.local_root / "data" / "student_interaction.parquet"
    runtime_duplicate.parent.mkdir(parents=True, exist_ok=True)
    local_target.parent.mkdir(parents=True, exist_ok=True)
    runtime_duplicate.write_text("same", encoding="utf-8")
    local_target.write_text("same", encoding="utf-8")

    summary = migrate_source_artifacts(settings, source=source)

    assert summary.count("removed_duplicate") >= 1
    assert not runtime_duplicate.exists()
    assert local_target.exists()


def test_migrate_source_artifacts_removes_runtime_copy_when_target_already_exists(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, source_id="maureen_m16fr")
    source = get_runtime_source("maureen_m16fr")

    runtime_copy = settings.runtime_root / "artifacts" / "reports" / "derived_manifest.json"
    local_target = settings.local_root / "artifacts" / "reports" / "derived_manifest.json"
    runtime_copy.parent.mkdir(parents=True, exist_ok=True)
    local_target.parent.mkdir(parents=True, exist_ok=True)
    runtime_copy.write_text("runtime", encoding="utf-8")
    local_target.write_text("local", encoding="utf-8")

    summary = migrate_source_artifacts(settings, source=source)

    assert summary.count("removed_runtime_copy") >= 1
    assert not runtime_copy.exists()
    assert local_target.read_text(encoding="utf-8") == "local"

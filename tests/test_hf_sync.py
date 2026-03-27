"""Validate source-aware HF runtime sync configuration parsing and synchronization behavior."""

from __future__ import annotations

import json

import pytest

from visu2.config import Settings
from visu2.hf_sync import (
    DEFAULT_RUNTIME_RELATIVE_PATHS,
    HFRepoConfig,
    ensure_runtime_assets_from_hf,
    load_hf_repo_config,
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


def _write_runtime_files(root_dir, relative_paths: tuple[str, ...]) -> None:
    for rel_path in relative_paths:
        path = root_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"placeholder")


def test_load_hf_repo_config_from_legacy_env() -> None:
    config = load_hf_repo_config(
        environ={
            "VISU2_HF_REPO_ID": "org/repo",
            "VISU2_HF_REVISION": "v1",
            "HF_TOKEN": "token",
        }
    )
    assert config is not None
    assert config.source_id == "main"
    assert config.repo_id == "org/repo"
    assert config.revision == "v1"
    assert config.repo_type == "dataset"
    assert config.token == "token"
    assert config.allow_patterns == DEFAULT_RUNTIME_RELATIVE_PATHS


def test_load_hf_repo_config_from_multisource_json() -> None:
    config = load_hf_repo_config(
        source_id="maureen_m16fr",
        environ={
            "VISU2_HF_SOURCES_JSON": json.dumps(
                {
                    "main": {"repo_id": "org/main", "revision": "main-v1"},
                    "maureen_m16fr": {
                        "repo_id": "org/maureen",
                        "revision": "m16-v1",
                        "allow_patterns": ["data/learning_catalog.json"],
                    },
                }
            ),
            "HF_TOKEN": "token",
        },
    )
    assert config is not None
    assert config.source_id == "maureen_m16fr"
    assert config.repo_id == "org/maureen"
    assert config.revision == "m16-v1"
    assert config.allow_patterns == ("data/learning_catalog.json",)


def test_load_hf_repo_config_returns_none_without_matching_source() -> None:
    config = load_hf_repo_config(
        source_id="maureen_m16fr",
        environ={
            "VISU2_HF_SOURCES_JSON": json.dumps(
                {"main": {"repo_id": "org/main", "revision": "main-v1"}}
            ),
            "HF_TOKEN": "token",
        },
    )
    assert config is None


def test_load_hf_repo_config_missing_required_values() -> None:
    with pytest.raises(ValueError):
        load_hf_repo_config(environ={"VISU2_HF_REPO_ID": "org/repo"})
    with pytest.raises(ValueError):
        load_hf_repo_config(
            environ={
                "VISU2_HF_SOURCES_JSON": json.dumps({"main": {"repo_id": "org/repo"}}),
                "HF_TOKEN": "token",
            }
        )


def test_load_hf_repo_config_returns_none_without_any_repo_config() -> None:
    assert load_hf_repo_config(environ={}) is None


def test_ensure_runtime_assets_from_hf_success(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    config = HFRepoConfig(
        source_id="main",
        repo_id="org/repo",
        revision="v1",
        repo_type="dataset",
        token="token",
        allow_patterns=DEFAULT_RUNTIME_RELATIVE_PATHS,
    )

    def fake_snapshot_download(**kwargs):
        assert kwargs["repo_id"] == "org/repo"
        assert kwargs["revision"] == "v1"
        assert kwargs["local_dir"] == str(settings.runtime_root)
        _write_runtime_files(settings.runtime_root, DEFAULT_RUNTIME_RELATIVE_PATHS)
        return str(settings.runtime_root)

    monkeypatch.setattr("visu2.hf_sync.snapshot_download", fake_snapshot_download)
    result = ensure_runtime_assets_from_hf(settings, config)
    assert result.mode == "synced"
    assert result.downloaded is True
    assert result.files_checked == len(DEFAULT_RUNTIME_RELATIVE_PATHS)
    assert result.missing_files == ()


def test_ensure_runtime_assets_from_hf_respects_required_paths_subset(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path, source_id="maureen_m16fr")
    config = HFRepoConfig(
        source_id="maureen_m16fr",
        repo_id="org/repo",
        revision="v1",
        repo_type="dataset",
        token="token",
        allow_patterns=DEFAULT_RUNTIME_RELATIVE_PATHS,
    )
    required_subset = ("data/learning_catalog.json", "artifacts/derived/fact_attempt_core.parquet")

    def fake_snapshot_download(**kwargs):
        assert tuple(kwargs["allow_patterns"]) == required_subset
        assert kwargs["local_dir"] == str(settings.runtime_root)
        _write_runtime_files(settings.runtime_root, required_subset)
        return str(settings.runtime_root)

    monkeypatch.setattr("visu2.hf_sync.snapshot_download", fake_snapshot_download)
    result = ensure_runtime_assets_from_hf(settings, config, required_paths=required_subset)
    assert result.mode == "synced"
    assert result.files_checked == len(required_subset)
    assert result.missing_files == ()


def test_ensure_runtime_assets_from_hf_raises_on_missing_files(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    config = HFRepoConfig(
        source_id="main",
        repo_id="org/repo",
        revision="v1",
        repo_type="dataset",
        token="token",
        allow_patterns=DEFAULT_RUNTIME_RELATIVE_PATHS,
    )

    def fake_snapshot_download(**kwargs):
        _write_runtime_files(settings.runtime_root, DEFAULT_RUNTIME_RELATIVE_PATHS[:2])
        return str(settings.runtime_root)

    monkeypatch.setattr("visu2.hf_sync.snapshot_download", fake_snapshot_download)
    with pytest.raises(FileNotFoundError):
        ensure_runtime_assets_from_hf(settings, config)

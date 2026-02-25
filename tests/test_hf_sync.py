from __future__ import annotations

import pytest

from visu2.config import Settings
from visu2.hf_sync import (
    DEFAULT_RUNTIME_RELATIVE_PATHS,
    HFRepoConfig,
    ensure_runtime_assets_from_hf,
    load_hf_repo_config,
)


def _build_settings(tmp_path) -> Settings:
    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
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
        parquet_path=data_dir / "adaptiv_math_history.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def _write_runtime_files(root_dir, relative_paths: tuple[str, ...]) -> None:
    for rel_path in relative_paths:
        path = root_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"placeholder")


def test_load_hf_repo_config_from_env() -> None:
    config = load_hf_repo_config(
        environ={
            "VISU2_HF_REPO_ID": "org/repo",
            "VISU2_HF_REVISION": "v1",
            "HF_TOKEN": "token",
        }
    )
    assert config is not None
    assert config.repo_id == "org/repo"
    assert config.revision == "v1"
    assert config.repo_type == "dataset"
    assert config.token == "token"
    assert config.allow_patterns == DEFAULT_RUNTIME_RELATIVE_PATHS


def test_load_hf_repo_config_secrets_override_env() -> None:
    config = load_hf_repo_config(
        secrets={
            "VISU2_HF_REPO_ID": "secret/repo",
            "VISU2_HF_REVISION": "v2",
            "HF_TOKEN": "secret-token",
        },
        environ={
            "VISU2_HF_REPO_ID": "env/repo",
            "VISU2_HF_REVISION": "env-v1",
            "HF_TOKEN": "env-token",
        },
    )
    assert config is not None
    assert config.repo_id == "secret/repo"
    assert config.revision == "v2"
    assert config.token == "secret-token"


def test_load_hf_repo_config_missing_required_values() -> None:
    with pytest.raises(ValueError):
        load_hf_repo_config(environ={"VISU2_HF_REPO_ID": "org/repo"})
    with pytest.raises(ValueError):
        load_hf_repo_config(
            environ={
                "VISU2_HF_REPO_ID": "org/repo",
                "VISU2_HF_REVISION": "v1",
            }
        )


def test_load_hf_repo_config_returns_none_without_repo_id() -> None:
    assert load_hf_repo_config(environ={}) is None


def test_ensure_runtime_assets_from_hf_success(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    config = HFRepoConfig(
        repo_id="org/repo",
        revision="v1",
        repo_type="dataset",
        token="token",
        allow_patterns=DEFAULT_RUNTIME_RELATIVE_PATHS,
    )

    def fake_snapshot_download(**kwargs):
        assert kwargs["repo_id"] == "org/repo"
        assert kwargs["revision"] == "v1"
        _write_runtime_files(settings.root_dir, DEFAULT_RUNTIME_RELATIVE_PATHS)
        return str(settings.root_dir)

    monkeypatch.setattr("visu2.hf_sync.snapshot_download", fake_snapshot_download)
    result = ensure_runtime_assets_from_hf(settings, config)
    assert result.mode == "synced"
    assert result.downloaded is True
    assert result.files_checked == len(DEFAULT_RUNTIME_RELATIVE_PATHS)
    assert result.missing_files == ()


def test_ensure_runtime_assets_from_hf_raises_on_missing_files(tmp_path, monkeypatch) -> None:
    settings = _build_settings(tmp_path)
    config = HFRepoConfig(
        repo_id="org/repo",
        revision="v1",
        repo_type="dataset",
        token="token",
        allow_patterns=DEFAULT_RUNTIME_RELATIVE_PATHS,
    )

    def fake_snapshot_download(**kwargs):
        _write_runtime_files(settings.root_dir, DEFAULT_RUNTIME_RELATIVE_PATHS[:2])
        return str(settings.root_dir)

    monkeypatch.setattr("visu2.hf_sync.snapshot_download", fake_snapshot_download)
    with pytest.raises(FileNotFoundError):
        ensure_runtime_assets_from_hf(settings, config)

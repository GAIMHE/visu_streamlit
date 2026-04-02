from __future__ import annotations

import sys
from pathlib import Path

from visu2.hf_sync import HFRepoConfig, SyncResult

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

import runtime_bootstrap


def test_bootstrap_optional_runtime_assets_returns_failed_result_without_stopping(
    monkeypatch,
) -> None:
    monkeypatch.setattr(runtime_bootstrap, "_secrets_mapping", lambda: None)
    monkeypatch.setattr(
        runtime_bootstrap,
        "_publish_runtime_secrets_to_env",
        lambda secrets: None,
    )
    monkeypatch.setattr(
        runtime_bootstrap,
        "load_hf_repo_config",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = runtime_bootstrap.bootstrap_optional_runtime_assets(
        "main",
        required_paths=("artifacts/derived/student_elo_profiles_batch_replay.parquet",),
    )

    assert result.mode == "optional_sync_failed"
    assert result.missing_files == (
        "artifacts/derived/student_elo_profiles_batch_replay.parquet",
    )
    assert result.message == "boom"


def test_bootstrap_optional_runtime_assets_reuses_cached_sync_for_subset(monkeypatch) -> None:
    monkeypatch.setattr(runtime_bootstrap, "_secrets_mapping", lambda: None)
    monkeypatch.setattr(
        runtime_bootstrap,
        "_publish_runtime_secrets_to_env",
        lambda secrets: None,
    )
    config = HFRepoConfig(
        source_id="main",
        repo_id="org/repo",
        revision="v1",
        repo_type="dataset",
        token="token",
        allow_patterns=("data/learning_catalog.json",),
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        runtime_bootstrap,
        "load_hf_repo_config",
        lambda **kwargs: config,
    )

    def fake_cached_sync(source_id, resolved_config, required_paths):
        captured["source_id"] = source_id
        captured["config"] = resolved_config
        captured["required_paths"] = required_paths
        return SyncResult(
            mode="synced",
            repo_id=resolved_config.repo_id,
            revision=resolved_config.revision,
            downloaded=True,
            files_checked=len(required_paths or ()),
            missing_files=(),
            message="ok",
        )

    monkeypatch.setattr(runtime_bootstrap, "_cached_runtime_sync", fake_cached_sync)

    result = runtime_bootstrap.bootstrap_optional_runtime_assets(
        "main",
        required_paths=("artifacts/derived/student_elo_profiles_batch_replay.parquet",),
    )

    assert result.mode == "synced"
    assert captured["source_id"] == "main"
    assert captured["config"] == config
    assert captured["required_paths"] == (
        "artifacts/derived/student_elo_profiles_batch_replay.parquet",
    )

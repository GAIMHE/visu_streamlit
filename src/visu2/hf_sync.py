from __future__ import annotations

import json
import os
from inspect import signature
from dataclasses import dataclass
from typing import Mapping, Sequence

from huggingface_hub import snapshot_download

from .config import Settings, ensure_artifact_directories


DEFAULT_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "data/learning_catalog.json",
    "data/zpdes_rules.json",
    "data/exercises.json",
    "artifacts/reports/consistency_report.json",
    "artifacts/reports/derived_manifest.json",
    "artifacts/derived/fact_attempt_core.parquet",
    "artifacts/derived/agg_activity_daily.parquet",
    "artifacts/derived/agg_objective_daily.parquet",
    "artifacts/derived/agg_student_module_progress.parquet",
    "artifacts/derived/agg_transition_edges.parquet",
    "artifacts/derived/agg_module_usage_daily.parquet",
    "artifacts/derived/agg_student_module_exposure.parquet",
    "artifacts/derived/agg_playlist_module_usage.parquet",
    "artifacts/derived/agg_module_activity_usage.parquet",
    "artifacts/derived/agg_exercise_daily.parquet",
)


@dataclass(frozen=True)
class HFRepoConfig:
    repo_id: str
    revision: str
    repo_type: str
    token: str
    allow_patterns: tuple[str, ...]


@dataclass(frozen=True)
class SyncResult:
    mode: str
    repo_id: str | None
    revision: str | None
    downloaded: bool
    files_checked: int
    missing_files: tuple[str, ...]
    message: str = ""


def _read_key(
    key: str,
    *,
    secrets: Mapping[str, object] | None,
    environ: Mapping[str, str],
) -> str | None:
    if secrets is not None and key in secrets:
        value = secrets.get(key)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    value = environ.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_allow_patterns(raw: object) -> tuple[str, ...]:
    if raw is None:
        return DEFAULT_RUNTIME_RELATIVE_PATHS

    if isinstance(raw, list):
        parsed = [str(item).strip() for item in raw if str(item).strip()]
    else:
        text = str(raw).strip()
        if not text:
            return DEFAULT_RUNTIME_RELATIVE_PATHS
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as err:
            raise ValueError("VISU2_HF_ALLOW_PATTERNS_JSON must be valid JSON.") from err
        if not isinstance(obj, list):
            raise ValueError("VISU2_HF_ALLOW_PATTERNS_JSON must decode to a JSON array.")
        parsed = [str(item).strip() for item in obj if str(item).strip()]

    if not parsed:
        raise ValueError("HF allow_patterns cannot be empty.")
    return tuple(parsed)


def load_hf_repo_config(
    *,
    secrets: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_patterns_override: Sequence[str] | None = None,
) -> HFRepoConfig | None:
    env = dict(os.environ) if environ is None else dict(environ)
    repo_id = _read_key("VISU2_HF_REPO_ID", secrets=secrets, environ=env)
    if not repo_id:
        return None

    revision = _read_key("VISU2_HF_REVISION", secrets=secrets, environ=env)
    if not revision:
        raise ValueError(
            "VISU2_HF_REVISION is required when VISU2_HF_REPO_ID is configured."
        )

    token = _read_key("HF_TOKEN", secrets=secrets, environ=env)
    if not token:
        raise ValueError(
            "HF_TOKEN is required for private Hugging Face dataset access."
        )

    repo_type = _read_key("VISU2_HF_REPO_TYPE", secrets=secrets, environ=env) or "dataset"
    if allow_patterns_override is not None:
        parsed_patterns = tuple(
            str(pattern).strip()
            for pattern in allow_patterns_override
            if str(pattern).strip()
        )
        if not parsed_patterns:
            raise ValueError("allow_patterns_override cannot be empty.")
    else:
        raw_patterns = _read_key(
            "VISU2_HF_ALLOW_PATTERNS_JSON",
            secrets=secrets,
            environ=env,
        )
        parsed_patterns = _parse_allow_patterns(raw_patterns)

    return HFRepoConfig(
        repo_id=repo_id,
        revision=revision,
        repo_type=repo_type,
        token=token,
        allow_patterns=parsed_patterns,
    )


def local_only_sync_result() -> SyncResult:
    return SyncResult(
        mode="local_only",
        repo_id=None,
        revision=None,
        downloaded=False,
        files_checked=0,
        missing_files=(),
        message="No VISU2_HF_REPO_ID configured; running with local files only.",
    )


def ensure_runtime_assets_from_hf(settings: Settings, config: HFRepoConfig) -> SyncResult:
    ensure_artifact_directories(settings)
    kwargs: dict[str, object] = {
        "repo_id": config.repo_id,
        "repo_type": config.repo_type,
        "revision": config.revision,
        "token": config.token,
        "local_dir": str(settings.root_dir),
        "allow_patterns": list(config.allow_patterns),
    }
    if "local_dir_use_symlinks" in signature(snapshot_download).parameters:
        kwargs["local_dir_use_symlinks"] = False
    snapshot_download(
        **kwargs
    )

    missing = tuple(
        relative_path
        for relative_path in DEFAULT_RUNTIME_RELATIVE_PATHS
        if not (settings.root_dir / relative_path).exists()
    )
    if missing:
        missing_text = ", ".join(missing)
        raise FileNotFoundError(
            "Missing required runtime files after HF sync: "
            f"{missing_text}."
        )

    return SyncResult(
        mode="synced",
        repo_id=config.repo_id,
        revision=config.revision,
        downloaded=True,
        files_checked=len(DEFAULT_RUNTIME_RELATIVE_PATHS),
        missing_files=(),
        message="Runtime files synchronized from Hugging Face.",
    )

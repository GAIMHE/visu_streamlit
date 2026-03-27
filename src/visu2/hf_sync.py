"""Synchronize runtime data assets from Hugging Face repositories into source-local paths."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from inspect import signature

from huggingface_hub import snapshot_download

from .config import Settings, ensure_artifact_directories
from .runtime_sources import DEFAULT_SOURCE_ID, runtime_relative_paths_for_source

LEGACY_DEFAULT_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = runtime_relative_paths_for_source(
    DEFAULT_SOURCE_ID
)
DEFAULT_RUNTIME_RELATIVE_PATHS = LEGACY_DEFAULT_RUNTIME_RELATIVE_PATHS


@dataclass(frozen=True)
class HFRepoConfig:
    """Resolved HF sync configuration for one runtime source."""

    source_id: str
    repo_id: str
    revision: str
    repo_type: str
    token: str
    allow_patterns: tuple[str, ...]


@dataclass(frozen=True)
class SyncResult:
    """Result of one runtime synchronization attempt."""

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
    """Read a configuration value from Streamlit secrets first, then env."""
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


def _parse_allow_patterns(raw: object, *, source_id: str) -> tuple[str, ...]:
    """Parse optional allow_patterns JSON, defaulting to the source runtime set."""
    if raw is None:
        return runtime_relative_paths_for_source(source_id)

    if isinstance(raw, list):
        parsed = [str(item).strip() for item in raw if str(item).strip()]
    else:
        text = str(raw).strip()
        if not text:
            return runtime_relative_paths_for_source(source_id)
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as err:
            raise ValueError("HF allow_patterns must be valid JSON.") from err
        if not isinstance(obj, list):
            raise ValueError("HF allow_patterns must decode to a JSON array.")
        parsed = [str(item).strip() for item in obj if str(item).strip()]

    if not parsed:
        raise ValueError("HF allow_patterns cannot be empty.")
    return tuple(parsed)


def _normalize_required_paths(required_paths: Sequence[str] | None, *, source_id: str) -> tuple[str, ...]:
    """Normalize a caller-provided runtime file subset."""
    if required_paths is None:
        return runtime_relative_paths_for_source(source_id)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_path in required_paths:
        path = str(raw_path).strip()
        if not path or path in seen:
            continue
        normalized.append(path)
        seen.add(path)
    if not normalized:
        raise ValueError("required_paths cannot be empty.")
    return tuple(normalized)


def _parse_hf_sources_json(raw: object) -> dict[str, dict[str, object]]:
    """Parse the multi-source HF JSON payload into a normalized mapping."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        payload = raw
    else:
        text = str(raw).strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as err:
            raise ValueError("VISU2_HF_SOURCES_JSON must be valid JSON.") from err
    if not isinstance(payload, dict):
        raise ValueError("VISU2_HF_SOURCES_JSON must decode to a JSON object.")

    normalized: dict[str, dict[str, object]] = {}
    for source_id, config in payload.items():
        normalized_source_id = str(source_id).strip()
        if not normalized_source_id:
            continue
        if not isinstance(config, dict):
            raise ValueError(
                "VISU2_HF_SOURCES_JSON entries must be objects keyed by source id."
            )
        normalized[normalized_source_id] = config
    return normalized


def load_hf_repo_config(
    *,
    source_id: str = DEFAULT_SOURCE_ID,
    secrets: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_patterns_override: Sequence[str] | None = None,
) -> HFRepoConfig | None:
    """Load either multi-source or legacy single-source HF repo config."""
    env = dict(os.environ) if environ is None else dict(environ)
    requested_source_id = str(source_id or DEFAULT_SOURCE_ID).strip() or DEFAULT_SOURCE_ID
    token = _read_key("HF_TOKEN", secrets=secrets, environ=env)

    raw_sources = _read_key("VISU2_HF_SOURCES_JSON", secrets=secrets, environ=env)
    parsed_sources = _parse_hf_sources_json(raw_sources)
    if parsed_sources:
        source_config = parsed_sources.get(requested_source_id)
        if source_config is None:
            return None
        repo_id = str(source_config.get("repo_id") or "").strip()
        if not repo_id:
            raise ValueError(
                f"VISU2_HF_SOURCES_JSON[{requested_source_id!r}].repo_id is required."
            )
        revision = str(source_config.get("revision") or "").strip()
        if not revision:
            raise ValueError(
                f"VISU2_HF_SOURCES_JSON[{requested_source_id!r}].revision is required."
            )
        repo_type = str(source_config.get("repo_type") or "dataset").strip() or "dataset"
        if allow_patterns_override is not None:
            parsed_patterns = tuple(
                str(pattern).strip()
                for pattern in allow_patterns_override
                if str(pattern).strip()
            )
            if not parsed_patterns:
                raise ValueError("allow_patterns_override cannot be empty.")
        else:
            parsed_patterns = _parse_allow_patterns(
                source_config.get("allow_patterns"),
                source_id=requested_source_id,
            )
    else:
        repo_id = _read_key("VISU2_HF_REPO_ID", secrets=secrets, environ=env)
        if not repo_id:
            return None
        revision = _read_key("VISU2_HF_REVISION", secrets=secrets, environ=env)
        if not revision:
            raise ValueError(
                "VISU2_HF_REVISION is required when VISU2_HF_REPO_ID is configured."
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
            parsed_patterns = _parse_allow_patterns(raw_patterns, source_id=requested_source_id)

    if not token:
        raise ValueError("HF_TOKEN is required for private Hugging Face dataset access.")

    return HFRepoConfig(
        source_id=requested_source_id,
        repo_id=repo_id,
        revision=revision,
        repo_type=repo_type,
        token=token,
        allow_patterns=parsed_patterns,
    )


def local_only_sync_result() -> SyncResult:
    """Return the sentinel result used when no HF repo is configured."""
    return SyncResult(
        mode="local_only",
        repo_id=None,
        revision=None,
        downloaded=False,
        files_checked=0,
        missing_files=(),
        message="No Hugging Face runtime source configured; running with local files only.",
    )


def ensure_runtime_assets_from_hf(
    settings: Settings,
    config: HFRepoConfig,
    required_paths: Sequence[str] | None = None,
) -> SyncResult:
    """Ensure one source-local runtime tree is synchronized from Hugging Face."""
    ensure_artifact_directories(settings)
    expected_paths = (
        tuple(config.allow_patterns)
        if required_paths is None
        else _normalize_required_paths(required_paths, source_id=config.source_id)
    )
    kwargs: dict[str, object] = {
        "repo_id": config.repo_id,
        "repo_type": config.repo_type,
        "revision": config.revision,
        "token": config.token,
        "local_dir": str(settings.runtime_root),
        "allow_patterns": list(expected_paths),
    }
    if "local_dir_use_symlinks" in signature(snapshot_download).parameters:
        kwargs["local_dir_use_symlinks"] = False
    snapshot_download(**kwargs)

    missing = tuple(
        relative_path
        for relative_path in expected_paths
        if not (settings.runtime_root / relative_path).exists()
    )
    if missing:
        missing_text = ", ".join(missing)
        raise FileNotFoundError(
            "Missing required runtime files after HF sync: " f"{missing_text}."
        )

    return SyncResult(
        mode="synced",
        repo_id=config.repo_id,
        revision=config.revision,
        downloaded=True,
        files_checked=len(expected_paths),
        missing_files=(),
        message="Runtime files synchronized from Hugging Face.",
    )

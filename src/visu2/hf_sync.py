"""
hf_sync.py

Synchronize runtime data assets from Hugging Face repositories into local paths.

Dependencies
------------
- collections
- config
- dataclasses
- huggingface_hub
- inspect
- json
- os

Classes
-------
- HFRepoConfig: Structured model for hfrepo config.
- SyncResult: Structured model for sync result.

Functions
---------
- _read_key: Utility for read key.
- _parse_allow_patterns: Utility for parse allow patterns.
- _normalize_required_paths: Utility for normalize required paths.
- load_hf_repo_config: Load hf repo config.
- local_only_sync_result: Utility for local only sync result.
- ensure_runtime_assets_from_hf: Utility for ensure runtime assets from hf.
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from inspect import signature

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
    "artifacts/derived/agg_playlist_module_usage.parquet",
    "artifacts/derived/agg_module_activity_usage.parquet",
    "artifacts/derived/agg_exercise_daily.parquet",
    "artifacts/derived/agg_exercise_elo.parquet",
    "artifacts/derived/agg_exercise_elo_iterative.parquet",
    "artifacts/derived/agg_activity_elo.parquet",
    "artifacts/derived/student_elo_events.parquet",
    "artifacts/derived/student_elo_profiles.parquet",
    "artifacts/derived/student_elo_events_iterative.parquet",
    "artifacts/derived/student_elo_profiles_iterative.parquet",
    "artifacts/derived/zpdes_exercise_progression_events.parquet",
    "artifacts/derived/work_mode_transition_paths.parquet",
)


@dataclass(frozen=True)
class HFRepoConfig:
    """Hfrepo config.

Notes
-----
This class is documented in NumPy style for consistency across the codebase.
"""
    repo_id: str
    revision: str
    repo_type: str
    token: str
    allow_patterns: tuple[str, ...]


@dataclass(frozen=True)
class SyncResult:
    """Sync result.

Notes
-----
This class is documented in NumPy style for consistency across the codebase.
"""
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
    """Read key.

Parameters
----------
key : str
        Input parameter used by this routine.
secrets : Mapping[str, object] | None
        Input parameter used by this routine.
environ : Mapping[str, str]
        Input parameter used by this routine.

Returns
-------
str | None
        Result produced by this routine.

"""
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
    """Parse allow patterns.

Parameters
----------
raw : object
        Input parameter used by this routine.

Returns
-------
tuple[str, ...]
        Result produced by this routine.

"""
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


def _normalize_required_paths(required_paths: Sequence[str] | None) -> tuple[str, ...]:
    """Normalize a caller-provided runtime file subset.

    Parameters
    ----------
    required_paths : Sequence[str] | None
        Requested runtime relative paths, or `None` to use the default full
        runtime set.

    Returns
    -------
    tuple[str, ...]
        Deduplicated runtime relative paths in caller order.
    """
    if required_paths is None:
        return DEFAULT_RUNTIME_RELATIVE_PATHS

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


def load_hf_repo_config(
    *,
    secrets: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_patterns_override: Sequence[str] | None = None,
) -> HFRepoConfig | None:
    """Load hf repo config.

Parameters
----------
secrets : Mapping[str, object] | None
        Input parameter used by this routine.
environ : Mapping[str, str] | None
        Input parameter used by this routine.
allow_patterns_override : Sequence[str] | None
        Input parameter used by this routine.

Returns
-------
HFRepoConfig | None
        Result produced by this routine.

"""
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
    """Local only sync result.


Returns
-------
SyncResult
        Result produced by this routine.

"""
    return SyncResult(
        mode="local_only",
        repo_id=None,
        revision=None,
        downloaded=False,
        files_checked=0,
        missing_files=(),
        message="No VISU2_HF_REPO_ID configured; running with local files only.",
    )


def ensure_runtime_assets_from_hf(
    settings: Settings,
    config: HFRepoConfig,
    required_paths: Sequence[str] | None = None,
) -> SyncResult:
    """Ensure runtime assets from hf.

Parameters
----------
settings : Settings
        Input parameter used by this routine.
config : HFRepoConfig
        Input parameter used by this routine.
required_paths : Sequence[str] | None
        Optional page-specific runtime subset. When omitted, the full default
        runtime set is synchronized.

Returns
-------
SyncResult
        Result produced by this routine.

"""
    ensure_artifact_directories(settings)
    expected_paths = (
        tuple(config.allow_patterns)
        if required_paths is None
        else _normalize_required_paths(required_paths)
    )
    kwargs: dict[str, object] = {
        "repo_id": config.repo_id,
        "repo_type": config.repo_type,
        "revision": config.revision,
        "token": config.token,
        "local_dir": str(settings.root_dir),
        "allow_patterns": list(expected_paths),
    }
    if "local_dir_use_symlinks" in signature(snapshot_download).parameters:
        kwargs["local_dir_use_symlinks"] = False
    snapshot_download(
        **kwargs
    )

    missing = tuple(
        relative_path
        for relative_path in expected_paths
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
        files_checked=len(expected_paths),
        missing_files=(),
        message="Runtime files synchronized from Hugging Face.",
    )

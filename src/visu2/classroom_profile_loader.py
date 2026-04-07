"""Load classroom mode profiles, preferring selector artifacts before fact fallback."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from .classroom_progression import build_classroom_mode_profiles
from .config import Settings
from .contracts import REQUIRED_AGG_COLUMNS, RUNTIME_CORE_COLUMNS
from .remote_query import query_runtime_parquet

CLASSROOM_PROFILE_FACT_COLUMNS: tuple[str, ...] = (
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
)
CLASSROOM_PROFILE_COLUMNS: tuple[str, ...] = tuple(REQUIRED_AGG_COLUMNS["classroom_mode_profiles"])


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _has_profile_contract(path: Path) -> bool:
    if not path.exists():
        return False
    required = RUNTIME_CORE_COLUMNS["classroom_mode_profiles"]
    return all(column in _parquet_columns(path) for column in required)


def _query_profiles_artifact(settings: Settings) -> pl.DataFrame | None:
    """Return selector profiles from the runtime artifact, local or remote."""
    try:
        profiles = query_runtime_parquet(
            settings,
            "artifacts/derived/classroom_mode_profiles.parquet",
            columns=CLASSROOM_PROFILE_COLUMNS,
        )
    except Exception:
        return None

    missing = [
        column
        for column in RUNTIME_CORE_COLUMNS["classroom_mode_profiles"]
        if column not in profiles.columns
    ]
    if missing:
        return None
    return profiles


def load_or_build_classroom_mode_profiles(settings: Settings) -> tuple[pl.DataFrame, str]:
    """Return classroom profiles from selector artifacts when available, else rebuild from fact data.

    Returns
    -------
    tuple[pl.DataFrame, str]
        A tuple of `(profiles, source_kind)` where `source_kind` is either
        `"artifact"` or `"fact_fallback"`.
    """

    profiles_path = settings.artifacts_derived_dir / "classroom_mode_profiles.parquet"
    if _has_profile_contract(profiles_path):
        return pl.read_parquet(profiles_path), "artifact"

    artifact_profiles = _query_profiles_artifact(settings)
    if artifact_profiles is not None:
        try:
            profiles_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_profiles.write_parquet(profiles_path)
        except OSError:
            pass
        return artifact_profiles, "artifact"

    fact = query_runtime_parquet(
        settings,
        "artifacts/derived/fact_attempt_core.parquet",
        columns=CLASSROOM_PROFILE_FACT_COLUMNS,
    )
    profiles = build_classroom_mode_profiles(fact)
    try:
        profiles_path.parent.mkdir(parents=True, exist_ok=True)
        profiles.write_parquet(profiles_path)
    except OSError:
        pass
    return profiles, "fact_fallback"


__all__ = [
    "CLASSROOM_PROFILE_FACT_COLUMNS",
    "load_or_build_classroom_mode_profiles",
]

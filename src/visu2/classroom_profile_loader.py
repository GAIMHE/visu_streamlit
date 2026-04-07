"""Load classroom mode profiles, with a fact-table fallback when the artifact is missing."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from .classroom_progression import build_classroom_mode_profiles
from .config import Settings
from .contracts import RUNTIME_CORE_COLUMNS
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


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _has_profile_contract(path: Path) -> bool:
    if not path.exists():
        return False
    required = RUNTIME_CORE_COLUMNS["classroom_mode_profiles"]
    return all(column in _parquet_columns(path) for column in required)


def load_or_build_classroom_mode_profiles(settings: Settings) -> tuple[pl.DataFrame, str]:
    """Return classroom profiles from the artifact when available, else rebuild from fact data.

    Returns
    -------
    tuple[pl.DataFrame, str]
        A tuple of `(profiles, source_kind)` where `source_kind` is either
        `"artifact"` or `"fact_fallback"`.
    """

    profiles_path = settings.artifacts_derived_dir / "classroom_mode_profiles.parquet"
    if _has_profile_contract(profiles_path):
        return pl.read_parquet(profiles_path), "artifact"

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

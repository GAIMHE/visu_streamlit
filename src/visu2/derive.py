"""Facade and orchestration layer for all derived runtime table builders."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .config import Settings, ensure_artifact_directories
from .contracts import REQUIRED_AGG_COLUMNS, REQUIRED_FACT_COLUMNS
from .derive_aggregates import (
    build_agg_activity_daily_from_fact,
    build_agg_exercise_daily_from_fact,
    build_agg_module_activity_usage_from_fact,
    build_agg_module_usage_daily_from_fact,
    build_agg_objective_daily_from_fact,
    build_agg_playlist_module_usage_from_fact,
    build_agg_student_module_progress_from_fact,
)
from .derive_elo import (
    build_agg_activity_elo_from_exercise_elo,
    build_agg_exercise_elo_from_fact,
    build_student_elo_events_from_fact,
    build_student_elo_profiles_from_events,
)
from .derive_fact import build_fact_attempt_core
from .transitions import build_transition_edges_from_fact


def _validate_required_columns(df: pl.DataFrame, required: list[str], label: str) -> None:
    """Raise a clear error when a builder no longer matches the declared contract."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def write_derived_tables(settings: Settings, sample_rows: int | None = None) -> dict[str, Path]:
    """Build and persist every runtime parquet artifact expected by the app."""
    ensure_artifact_directories(settings)
    outputs = {
        "fact_attempt_core": settings.artifacts_derived_dir / "fact_attempt_core.parquet",
        "agg_activity_daily": settings.artifacts_derived_dir / "agg_activity_daily.parquet",
        "agg_objective_daily": settings.artifacts_derived_dir / "agg_objective_daily.parquet",
        "agg_student_module_progress": settings.artifacts_derived_dir
        / "agg_student_module_progress.parquet",
        "agg_transition_edges": settings.artifacts_derived_dir / "agg_transition_edges.parquet",
        "agg_module_usage_daily": settings.artifacts_derived_dir / "agg_module_usage_daily.parquet",
        "agg_playlist_module_usage": settings.artifacts_derived_dir
        / "agg_playlist_module_usage.parquet",
        "agg_module_activity_usage": settings.artifacts_derived_dir
        / "agg_module_activity_usage.parquet",
        "agg_exercise_daily": settings.artifacts_derived_dir / "agg_exercise_daily.parquet",
        "agg_exercise_elo": settings.artifacts_derived_dir / "agg_exercise_elo.parquet",
        "agg_activity_elo": settings.artifacts_derived_dir / "agg_activity_elo.parquet",
        "student_elo_events": settings.artifacts_derived_dir / "student_elo_events.parquet",
        "student_elo_profiles": settings.artifacts_derived_dir / "student_elo_profiles.parquet",
    }

    fact = build_fact_attempt_core(settings, sample_rows=sample_rows)
    _validate_required_columns(fact, REQUIRED_FACT_COLUMNS, "fact_attempt_core")

    agg_activity = build_agg_activity_daily_from_fact(fact)
    agg_objective = build_agg_objective_daily_from_fact(fact)
    agg_student_module = build_agg_student_module_progress_from_fact(fact)
    agg_transition = build_transition_edges_from_fact(fact)
    agg_module_usage_daily = build_agg_module_usage_daily_from_fact(fact)
    agg_playlist_module_usage = build_agg_playlist_module_usage_from_fact(fact)
    agg_module_activity_usage = build_agg_module_activity_usage_from_fact(fact)
    agg_exercise_daily = build_agg_exercise_daily_from_fact(fact, settings=settings)
    agg_exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    agg_activity_elo = build_agg_activity_elo_from_exercise_elo(
        agg_exercise_elo,
        settings=settings,
    )
    student_elo_events = build_student_elo_events_from_fact(fact, agg_exercise_elo)
    student_elo_profiles = build_student_elo_profiles_from_events(student_elo_events)

    derived_frames = {
        "agg_activity_daily": agg_activity,
        "agg_objective_daily": agg_objective,
        "agg_student_module_progress": agg_student_module,
        "agg_transition_edges": agg_transition,
        "agg_module_usage_daily": agg_module_usage_daily,
        "agg_playlist_module_usage": agg_playlist_module_usage,
        "agg_module_activity_usage": agg_module_activity_usage,
        "agg_exercise_daily": agg_exercise_daily,
        "agg_exercise_elo": agg_exercise_elo,
        "agg_activity_elo": agg_activity_elo,
        "student_elo_events": student_elo_events,
        "student_elo_profiles": student_elo_profiles,
    }
    for label, frame in derived_frames.items():
        _validate_required_columns(frame, REQUIRED_AGG_COLUMNS[label], label)

    fact.write_parquet(outputs["fact_attempt_core"])
    for label, frame in derived_frames.items():
        frame.write_parquet(outputs[label])

    return outputs


__all__ = [
    "build_fact_attempt_core",
    "build_agg_activity_daily_from_fact",
    "build_agg_objective_daily_from_fact",
    "build_agg_student_module_progress_from_fact",
    "build_agg_module_usage_daily_from_fact",
    "build_agg_playlist_module_usage_from_fact",
    "build_agg_module_activity_usage_from_fact",
    "build_agg_exercise_daily_from_fact",
    "build_agg_exercise_elo_from_fact",
    "build_agg_activity_elo_from_exercise_elo",
    "build_student_elo_events_from_fact",
    "build_student_elo_profiles_from_events",
    "write_derived_tables",
]

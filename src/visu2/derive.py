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
    build_agg_exercise_elo_iterative_from_fact,
    build_student_elo_events_from_fact,
    build_student_elo_profiles_from_events,
)
from .derive_fact import build_fact_attempt_core
from .derive_zpdes import build_zpdes_exercise_progression_events_from_fact
from .transitions import build_transition_edges_from_fact
from .work_mode_transitions import build_work_mode_transition_paths


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
        "agg_exercise_elo_iterative": settings.artifacts_derived_dir / "agg_exercise_elo_iterative.parquet",
        "agg_activity_elo": settings.artifacts_derived_dir / "agg_activity_elo.parquet",
        "student_elo_events": settings.artifacts_derived_dir / "student_elo_events.parquet",
        "student_elo_profiles": settings.artifacts_derived_dir / "student_elo_profiles.parquet",
        "student_elo_events_iterative": settings.artifacts_derived_dir / "student_elo_events_iterative.parquet",
        "student_elo_profiles_iterative": settings.artifacts_derived_dir / "student_elo_profiles_iterative.parquet",
        "zpdes_exercise_progression_events": settings.artifacts_derived_dir
        / "zpdes_exercise_progression_events.parquet",
        "work_mode_transition_paths": settings.artifacts_derived_dir / "work_mode_transition_paths.parquet",
    }

    fact = build_fact_attempt_core(settings, sample_rows=sample_rows)
    _validate_required_columns(fact, REQUIRED_FACT_COLUMNS, "fact_attempt_core")
    fact.write_parquet(outputs["fact_attempt_core"])

    def write_frame(label: str, frame: pl.DataFrame) -> None:
        _validate_required_columns(frame, REQUIRED_AGG_COLUMNS[label], label)
        frame.write_parquet(outputs[label])

    write_frame("agg_activity_daily", build_agg_activity_daily_from_fact(fact))
    write_frame("agg_objective_daily", build_agg_objective_daily_from_fact(fact))
    write_frame("agg_student_module_progress", build_agg_student_module_progress_from_fact(fact))
    write_frame("agg_transition_edges", build_transition_edges_from_fact(fact))
    write_frame("agg_module_usage_daily", build_agg_module_usage_daily_from_fact(fact))
    write_frame("agg_playlist_module_usage", build_agg_playlist_module_usage_from_fact(fact))
    write_frame("agg_module_activity_usage", build_agg_module_activity_usage_from_fact(fact))
    write_frame("agg_exercise_daily", build_agg_exercise_daily_from_fact(fact, settings=settings))
    write_frame(
        "work_mode_transition_paths",
        build_work_mode_transition_paths(
            pl.scan_parquet(settings.parquet_path).limit(sample_rows)
            if sample_rows is not None
            else pl.scan_parquet(settings.parquet_path)
        ),
    )

    agg_exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    write_frame("agg_exercise_elo", agg_exercise_elo)
    agg_exercise_elo_iterative = build_agg_exercise_elo_iterative_from_fact(fact, settings=settings)
    write_frame("agg_exercise_elo_iterative", agg_exercise_elo_iterative)
    write_frame(
        "agg_activity_elo",
        build_agg_activity_elo_from_exercise_elo(agg_exercise_elo, settings=settings),
    )
    student_elo_events = build_student_elo_events_from_fact(fact, agg_exercise_elo)
    write_frame("student_elo_events", student_elo_events)
    write_frame("student_elo_profiles", build_student_elo_profiles_from_events(student_elo_events))
    student_elo_events_iterative = build_student_elo_events_from_fact(fact, agg_exercise_elo_iterative)
    write_frame("student_elo_events_iterative", student_elo_events_iterative)
    write_frame(
        "student_elo_profiles_iterative",
        build_student_elo_profiles_from_events(student_elo_events_iterative),
    )
    write_frame(
        "zpdes_exercise_progression_events",
        build_zpdes_exercise_progression_events_from_fact(fact, settings=settings),
    )

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
    "build_agg_exercise_elo_iterative_from_fact",
    "build_agg_activity_elo_from_exercise_elo",
    "build_student_elo_events_from_fact",
    "build_student_elo_profiles_from_events",
    "build_zpdes_exercise_progression_events_from_fact",
    "build_work_mode_transition_paths",
    "write_derived_tables",
]

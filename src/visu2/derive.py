"""Facade and orchestration layer for source-aware derived runtime table builders."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .classroom_progression import (
    build_classroom_activity_summary_by_mode,
    build_classroom_mode_profiles,
)
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
    build_student_elo_events_batch_replay_from_fact,
    build_student_elo_events_from_fact,
    build_student_elo_events_iterative_from_fact,
    build_student_elo_profiles_batch_replay_from_events,
    build_student_elo_profiles_from_events,
    build_student_elo_profiles_iterative_from_events,
)
from .derive_fact import (
    build_fact_attempt_core,
    build_hierarchy_context_lookup,
    build_hierarchy_resolution_bundle,
    build_hierarchy_resolution_report,
)
from .derive_zpdes import build_zpdes_exercise_progression_events_from_fact
from .reporting import write_json_report
from .runtime_sources import get_runtime_source
from .transitions import build_transition_edges_from_fact
from .work_mode_transitions import build_work_mode_transition_paths


def _validate_required_columns(df: pl.DataFrame, required: list[str], label: str) -> None:
    """Raise a clear error when a builder no longer matches the declared contract."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _output_path(settings: Settings, table_name: str) -> Path:
    source_spec = get_runtime_source(settings.source_id)
    if table_name in source_spec.runtime_derived_tables:
        return settings.artifacts_derived_dir / f"{table_name}.parquet"
    if table_name in source_spec.legacy_derived_tables:
        return settings.legacy_artifacts_derived_dir / f"{table_name}.parquet"
    raise ValueError(f"Output path classification is missing for derived table: {table_name}")


def _load_existing_runtime_table(settings: Settings, table_name: str) -> pl.DataFrame:
    path = settings.artifacts_derived_dir / f"{table_name}.parquet"
    if not path.exists():
        raise RuntimeError(
            f"Requested partial build needs existing runtime dependency '{table_name}', but {path} is missing."
        )
    return pl.read_parquet(path)


ALL_TABLE_BUILDERS: tuple[str, ...] = (
    "fact_attempt_core",
    "hierarchy_context_lookup",
    "classroom_mode_profiles",
    "classroom_activity_summary_by_mode",
    "agg_activity_daily",
    "agg_objective_daily",
    "agg_student_module_progress",
    "agg_transition_edges",
    "agg_module_usage_daily",
    "agg_playlist_module_usage",
    "agg_module_activity_usage",
    "agg_exercise_daily",
    "agg_exercise_elo",
    "agg_exercise_elo_iterative",
    "agg_activity_elo",
    "student_elo_events",
    "student_elo_profiles",
    "student_elo_events_batch_replay",
    "student_elo_profiles_batch_replay",
    "student_elo_events_iterative",
    "student_elo_profiles_iterative",
    "zpdes_exercise_progression_events",
    "work_mode_transition_paths",
)


def write_derived_tables(
    settings: Settings,
    sample_rows: int | None = None,
    table_names: tuple[str, ...] | None = None,
) -> dict[str, Path]:
    """Build and persist the requested runtime parquet artifacts for one source."""
    ensure_artifact_directories(settings)
    source_spec = get_runtime_source(settings.source_id)
    requested_tables = tuple(table_names or source_spec.runtime_derived_tables)
    unknown = sorted(set(requested_tables) - set(ALL_TABLE_BUILDERS))
    if unknown:
        raise ValueError(f"Unknown derived tables requested: {unknown}")

    outputs = {table_name: _output_path(settings, table_name) for table_name in requested_tables}
    requested_set = set(requested_tables)
    resolution_bundle = None
    fact: pl.DataFrame | None = None
    hierarchy_context_lookup: pl.DataFrame | None = None

    def get_resolution_bundle():
        nonlocal resolution_bundle
        if resolution_bundle is None:
            resolution_bundle = build_hierarchy_resolution_bundle(settings, sample_rows=sample_rows)
        return resolution_bundle

    def get_fact() -> pl.DataFrame:
        nonlocal fact
        if fact is not None:
            return fact
        if "fact_attempt_core" not in requested_set and sample_rows is None:
            try:
                fact = _load_existing_runtime_table(settings, "fact_attempt_core")
            except RuntimeError:
                pass
        if fact is None:
            fact = get_resolution_bundle().fact_attempt_core
        _validate_required_columns(fact, REQUIRED_FACT_COLUMNS, "fact_attempt_core")
        return fact

    def get_hierarchy_context_lookup() -> pl.DataFrame:
        nonlocal hierarchy_context_lookup
        if hierarchy_context_lookup is None:
            hierarchy_context_lookup = get_resolution_bundle().hierarchy_context_lookup
        return hierarchy_context_lookup

    if "fact_attempt_core" in requested_set:
        get_fact().write_parquet(outputs["fact_attempt_core"])
    if "hierarchy_context_lookup" in requested_set:
        hierarchy_context_lookup = get_hierarchy_context_lookup()
        _validate_required_columns(
            hierarchy_context_lookup,
            REQUIRED_AGG_COLUMNS["hierarchy_context_lookup"],
            "hierarchy_context_lookup",
        )
        hierarchy_context_lookup.write_parquet(outputs["hierarchy_context_lookup"])
        write_json_report(
            build_hierarchy_resolution_report(hierarchy_context_lookup),
            settings.hierarchy_resolution_report_path,
        )

    def write_frame(label: str, frame: pl.DataFrame) -> pl.DataFrame:
        _validate_required_columns(frame, REQUIRED_AGG_COLUMNS[label], label)
        if label in requested_set:
            frame.write_parquet(outputs[label])
        return frame

    if "classroom_mode_profiles" in requested_set:
        write_frame("classroom_mode_profiles", build_classroom_mode_profiles(get_fact()))
    if "classroom_activity_summary_by_mode" in requested_set:
        write_frame(
            "classroom_activity_summary_by_mode",
            build_classroom_activity_summary_by_mode(get_fact()),
        )
    if "agg_activity_daily" in requested_set:
        write_frame("agg_activity_daily", build_agg_activity_daily_from_fact(get_fact()))
    if "agg_objective_daily" in requested_set:
        write_frame("agg_objective_daily", build_agg_objective_daily_from_fact(get_fact()))
    if "agg_student_module_progress" in requested_set:
        write_frame("agg_student_module_progress", build_agg_student_module_progress_from_fact(get_fact()))
    if "agg_transition_edges" in requested_set:
        write_frame("agg_transition_edges", build_transition_edges_from_fact(get_fact()))
    if "agg_module_usage_daily" in requested_set:
        write_frame("agg_module_usage_daily", build_agg_module_usage_daily_from_fact(get_fact()))
    if "agg_playlist_module_usage" in requested_set:
        write_frame("agg_playlist_module_usage", build_agg_playlist_module_usage_from_fact(get_fact()))
    if "agg_module_activity_usage" in requested_set:
        write_frame("agg_module_activity_usage", build_agg_module_activity_usage_from_fact(get_fact()))
    if "agg_exercise_daily" in requested_set:
        write_frame("agg_exercise_daily", build_agg_exercise_daily_from_fact(get_fact(), settings=settings))
    if "work_mode_transition_paths" in requested_set:
        work_mode_source = (
            pl.scan_parquet(settings.parquet_path).limit(sample_rows)
            if sample_rows is not None
            else pl.scan_parquet(settings.parquet_path)
        )
        write_frame("work_mode_transition_paths", build_work_mode_transition_paths(work_mode_source))

    agg_exercise_elo: pl.DataFrame | None = None

    def get_current_exercise_elo() -> pl.DataFrame:
        nonlocal agg_exercise_elo
        if agg_exercise_elo is not None:
            return agg_exercise_elo
        if "agg_exercise_elo" in requested_set:
            agg_exercise_elo = write_frame(
                "agg_exercise_elo",
                build_agg_exercise_elo_from_fact(get_fact(), settings=settings),
            )
        else:
            agg_exercise_elo = _load_existing_runtime_table(settings, "agg_exercise_elo")
            _validate_required_columns(agg_exercise_elo, REQUIRED_AGG_COLUMNS["agg_exercise_elo"], "agg_exercise_elo")
        return agg_exercise_elo

    if "agg_exercise_elo" in requested_set:
        get_current_exercise_elo()
    if "agg_activity_elo" in requested_set:
        write_frame(
            "agg_activity_elo",
            build_agg_activity_elo_from_exercise_elo(get_current_exercise_elo(), settings=settings),
        )
    if "student_elo_events" in requested_set or "student_elo_profiles" in requested_set:
        student_elo_events = (
            write_frame(
                "student_elo_events",
                build_student_elo_events_from_fact(get_fact(), get_current_exercise_elo()),
            )
            if "student_elo_events" in requested_set
            else _load_existing_runtime_table(settings, "student_elo_events")
        )
        if "student_elo_profiles" in requested_set:
            write_frame("student_elo_profiles", build_student_elo_profiles_from_events(student_elo_events))
    if "student_elo_events_batch_replay" in requested_set or "student_elo_profiles_batch_replay" in requested_set:
        student_elo_events_batch_replay = (
            write_frame(
                "student_elo_events_batch_replay",
                build_student_elo_events_batch_replay_from_fact(get_fact(), get_current_exercise_elo()),
            )
            if "student_elo_events_batch_replay" in requested_set
            else _load_existing_runtime_table(settings, "student_elo_events_batch_replay")
        )
        if "student_elo_profiles_batch_replay" in requested_set:
            write_frame(
                "student_elo_profiles_batch_replay",
                build_student_elo_profiles_batch_replay_from_events(student_elo_events_batch_replay),
            )

    agg_exercise_elo_iterative: pl.DataFrame | None = None

    def get_iterative_exercise_elo() -> pl.DataFrame:
        nonlocal agg_exercise_elo_iterative
        if agg_exercise_elo_iterative is not None:
            return agg_exercise_elo_iterative
        if "agg_exercise_elo_iterative" in requested_set:
            agg_exercise_elo_iterative = write_frame(
                "agg_exercise_elo_iterative",
                build_agg_exercise_elo_iterative_from_fact(get_fact(), settings=settings),
            )
        else:
            agg_exercise_elo_iterative = _load_existing_runtime_table(settings, "agg_exercise_elo_iterative")
            _validate_required_columns(
                agg_exercise_elo_iterative,
                REQUIRED_AGG_COLUMNS["agg_exercise_elo_iterative"],
                "agg_exercise_elo_iterative",
            )
        return agg_exercise_elo_iterative

    if "agg_exercise_elo_iterative" in requested_set:
        get_iterative_exercise_elo()
    if "student_elo_events_iterative" in requested_set or "student_elo_profiles_iterative" in requested_set:
        student_elo_events_iterative = (
            write_frame(
                "student_elo_events_iterative",
                build_student_elo_events_iterative_from_fact(get_fact(), get_iterative_exercise_elo()),
            )
            if "student_elo_events_iterative" in requested_set
            else _load_existing_runtime_table(settings, "student_elo_events_iterative")
        )
        if "student_elo_profiles_iterative" in requested_set:
            write_frame(
                "student_elo_profiles_iterative",
                build_student_elo_profiles_iterative_from_events(student_elo_events_iterative),
            )

    if "zpdes_exercise_progression_events" in requested_set:
        write_frame(
            "zpdes_exercise_progression_events",
            build_zpdes_exercise_progression_events_from_fact(get_fact(), settings=settings),
        )

    return outputs


__all__ = [
    "build_fact_attempt_core",
    "build_hierarchy_context_lookup",
    "build_classroom_mode_profiles",
    "build_classroom_activity_summary_by_mode",
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
    "build_student_elo_events_batch_replay_from_fact",
    "build_student_elo_profiles_batch_replay_from_events",
    "build_student_elo_events_iterative_from_fact",
    "build_student_elo_profiles_iterative_from_events",
    "build_zpdes_exercise_progression_events_from_fact",
    "build_work_mode_transition_paths",
    "write_derived_tables",
]

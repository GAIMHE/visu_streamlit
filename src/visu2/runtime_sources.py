"""Source registry for source-aware runtime builds, sync, and page gating."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

COMMON_RUNTIME_DATA_RELATIVE_PATHS: tuple[str, ...] = ("data/learning_catalog.json",)

MAIN_RUNTIME_DATA_RELATIVE_PATHS: tuple[str, ...] = (
    *COMMON_RUNTIME_DATA_RELATIVE_PATHS,
    "data/zpdes_rules.json",
)

MAUREEN_RUNTIME_DATA_RELATIVE_PATHS: tuple[str, ...] = COMMON_RUNTIME_DATA_RELATIVE_PATHS

COMMON_LOCAL_BUILD_DATA_RELATIVE_PATHS: tuple[str, ...] = (
    "data/student_interaction.parquet",
    "data/exercises.json",
)

MAIN_LOCAL_BUILD_DATA_RELATIVE_PATHS: tuple[str, ...] = COMMON_LOCAL_BUILD_DATA_RELATIVE_PATHS

MAUREEN_LOCAL_BUILD_DATA_RELATIVE_PATHS: tuple[str, ...] = (
    *COMMON_LOCAL_BUILD_DATA_RELATIVE_PATHS,
    "data/zpdes_rules.json",
)

COMMON_LOCAL_BUILD_REPORT_RELATIVE_PATHS: tuple[str, ...] = (
    "artifacts/reports/consistency_report.json",
    "artifacts/reports/derived_manifest.json",
)

MAIN_RUNTIME_DERIVED_TABLES: tuple[str, ...] = (
    "fact_attempt_core",
    "classroom_mode_profiles",
    "agg_activity_daily",
    "agg_transition_edges",
    "agg_exercise_daily",
    "agg_exercise_elo",
    "agg_exercise_elo_iterative",
    "agg_activity_elo",
    "student_elo_events",
    "student_elo_profiles",
    "student_elo_events_iterative",
    "student_elo_profiles_iterative",
    "zpdes_exercise_progression_events",
)

MAUREEN_RUNTIME_DERIVED_TABLES: tuple[str, ...] = (
    "fact_attempt_core",
    "classroom_mode_profiles",
    "agg_activity_daily",
    "agg_transition_edges",
    "agg_exercise_daily",
    "agg_exercise_elo",
    "agg_exercise_elo_iterative",
    "agg_activity_elo",
    "student_elo_events",
    "student_elo_profiles",
    "student_elo_events_iterative",
    "student_elo_profiles_iterative",
)

COMMON_LEGACY_DERIVED_TABLES: tuple[str, ...] = (
    "hierarchy_context_lookup",
    "classroom_activity_summary_by_mode",
    "agg_objective_daily",
    "agg_module_usage_daily",
    "agg_playlist_module_usage",
    "agg_module_activity_usage",
    "work_mode_transition_paths",
)

MAIN_LEGACY_DERIVED_TABLES: tuple[str, ...] = (
    *COMMON_LEGACY_DERIVED_TABLES,
    "agg_student_module_progress",
)

MAUREEN_LEGACY_DERIVED_TABLES: tuple[str, ...] = COMMON_LEGACY_DERIVED_TABLES

COMMON_LEGACY_REPORT_RELATIVE_PATHS: tuple[str, ...] = ("artifacts/reports/hierarchy_resolution_report.json",)

COMMON_LEGACY_CLEANUP_RELATIVE_PATHS: tuple[str, ...] = (
    "data/adaptiv_math_history.parquet",
    "artifacts/derived/zpdes_first_arrival_events.parquet",
    "artifacts/derived/agg_student_module_exposure.parquet",
    "artifacts/reports/schema_snapshot.json",
    "artifacts/reports/classroom_id_descriptive_stats.json",
    "artifacts/reports/classroom_workmode_stats.json",
    "artifacts/reports/.duckdb_tmp",
)

MAIN_LEGACY_CLEANUP_RELATIVE_PATHS: tuple[str, ...] = COMMON_LEGACY_CLEANUP_RELATIVE_PATHS

MAUREEN_LEGACY_CLEANUP_RELATIVE_PATHS: tuple[str, ...] = COMMON_LEGACY_CLEANUP_RELATIVE_PATHS

ALL_PAGE_IDS: frozenset[str] = frozenset(
    {
        "overview",
        "bottlenecks",
        "matrix",
        "zpdes_transition_efficiency",
        "classroom_replay",
        "student_elo",
        "classroom_sankey",
        "student_objective_spider",
    }
)

CAPABILITY_HAS_CLASSROOMS = "has_classrooms"
CAPABILITY_HAS_PLAYLIST_DIMENSION = "has_playlist_dimension"
CAPABILITY_HAS_DURATION_FIELDS = "has_duration_fields"
CAPABILITY_HAS_ZPDES_TOPOLOGY = "has_zpdes_topology"
CAPABILITY_HAS_EXERCISE_METADATA = "has_exercise_metadata"
CAPABILITY_HAS_EXACT_MIN_STUDENT_ATTEMPT_FILTER = "has_exact_min_student_attempt_filter"
CAPABILITY_HAS_CLASSROOM_ALL_DATA_OPTION = "has_classroom_all_data_option"


def _derived_runtime_relative_paths(table_names: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(f"artifacts/derived/{table_name}.parquet" for table_name in table_names)


@dataclass(frozen=True, slots=True)
class RuntimeSourceSpec:
    """Source-specific runtime metadata and capabilities."""

    source_id: str
    label: str
    description: str
    runtime_root_relative: Path
    local_root_relative: Path
    legacy_root_relative: Path
    build_profile: str
    raw_inputs: dict[str, Path]
    supported_pages: tuple[str, ...]
    capability_flags: frozenset[str]
    remote_config_key: str
    runtime_relative_paths: tuple[str, ...]
    runtime_derived_tables: tuple[str, ...]
    local_build_relative_paths: tuple[str, ...]
    legacy_derived_tables: tuple[str, ...]
    legacy_relative_paths: tuple[str, ...]
    legacy_cleanup_relative_paths: tuple[str, ...]

    def runtime_root(self, root_dir: Path) -> Path:
        """Return the absolute runtime root for one source."""
        return root_dir / self.runtime_root_relative

    def local_root(self, root_dir: Path) -> Path:
        """Return the absolute local-build root for one source."""
        return root_dir / self.local_root_relative

    def legacy_root(self, root_dir: Path) -> Path:
        """Return the absolute legacy/debug root for one source."""
        return root_dir / self.legacy_root_relative


RUNTIME_SOURCES: dict[str, RuntimeSourceSpec] = {
    "main": RuntimeSourceSpec(
        source_id="main",
        label="Adaptiv'Math Main",
        description=(
            "Full classroom-scale Adaptiv'Math dataset with the lean runtime surface "
            "required by the active Streamlit application."
        ),
        runtime_root_relative=Path("artifacts") / "sources" / "main",
        local_root_relative=Path("artifacts") / "local" / "main",
        legacy_root_relative=Path("artifacts") / "legacy" / "main",
        build_profile="main",
        raw_inputs={
            "parquet": Path("data") / "adaptiv_math_history.parquet",
            "learning_catalog": Path("data") / "learning_catalog.json",
            "zpdes_rules": Path("data") / "zpdes_rules.json",
            "exercises": Path("data") / "exercises.json",
        },
        supported_pages=tuple(sorted(ALL_PAGE_IDS)),
        capability_flags=frozenset(
            {
                CAPABILITY_HAS_CLASSROOMS,
                CAPABILITY_HAS_PLAYLIST_DIMENSION,
                CAPABILITY_HAS_DURATION_FIELDS,
                CAPABILITY_HAS_ZPDES_TOPOLOGY,
                CAPABILITY_HAS_EXERCISE_METADATA,
            }
        ),
        remote_config_key="main",
        runtime_relative_paths=(
            MAIN_RUNTIME_DATA_RELATIVE_PATHS
            + _derived_runtime_relative_paths(MAIN_RUNTIME_DERIVED_TABLES)
        ),
        runtime_derived_tables=MAIN_RUNTIME_DERIVED_TABLES,
        local_build_relative_paths=(
            MAIN_LOCAL_BUILD_DATA_RELATIVE_PATHS + COMMON_LOCAL_BUILD_REPORT_RELATIVE_PATHS
        ),
        legacy_derived_tables=MAIN_LEGACY_DERIVED_TABLES,
        legacy_relative_paths=(
            COMMON_LEGACY_REPORT_RELATIVE_PATHS
            + _derived_runtime_relative_paths(MAIN_LEGACY_DERIVED_TABLES)
        ),
        legacy_cleanup_relative_paths=MAIN_LEGACY_CLEANUP_RELATIVE_PATHS,
    ),
    "maureen_m16fr": RuntimeSourceSpec(
        source_id="maureen_m16fr",
        label="Maureen M16 FR",
        description=(
            "Single-module remediation dataset adapted into the shared runtime contract, "
            "using the researcher export with real classroom identifiers."
        ),
        runtime_root_relative=Path("artifacts") / "sources" / "maureen_m16fr",
        local_root_relative=Path("artifacts") / "local" / "maureen_m16fr",
        legacy_root_relative=Path("artifacts") / "legacy" / "maureen_m16fr",
        build_profile="maureen",
        raw_inputs={
            "attempts_csv": Path("data_maureen")
            / "researcher_data_Comprendre les mots pour mieux les lire(in).csv",
            "module_config_csv": Path("data_maureen") / "M16FR_modules_config 1(M16-Fr).csv",
        },
        supported_pages=(
            "overview",
            "bottlenecks",
            "matrix",
            "student_elo",
            "classroom_replay",
            "classroom_sankey",
            "student_objective_spider",
        ),
        capability_flags=frozenset(
            {
                CAPABILITY_HAS_CLASSROOMS,
                CAPABILITY_HAS_EXERCISE_METADATA,
                CAPABILITY_HAS_EXACT_MIN_STUDENT_ATTEMPT_FILTER,
                CAPABILITY_HAS_CLASSROOM_ALL_DATA_OPTION,
            }
        ),
        remote_config_key="maureen_m16fr",
        runtime_relative_paths=(
            MAUREEN_RUNTIME_DATA_RELATIVE_PATHS
            + _derived_runtime_relative_paths(MAUREEN_RUNTIME_DERIVED_TABLES)
        ),
        runtime_derived_tables=MAUREEN_RUNTIME_DERIVED_TABLES,
        local_build_relative_paths=(
            MAUREEN_LOCAL_BUILD_DATA_RELATIVE_PATHS + COMMON_LOCAL_BUILD_REPORT_RELATIVE_PATHS
        ),
        legacy_derived_tables=MAUREEN_LEGACY_DERIVED_TABLES,
        legacy_relative_paths=(
            COMMON_LEGACY_REPORT_RELATIVE_PATHS
            + _derived_runtime_relative_paths(MAUREEN_LEGACY_DERIVED_TABLES)
        ),
        legacy_cleanup_relative_paths=MAUREEN_LEGACY_CLEANUP_RELATIVE_PATHS,
    ),
}

DEFAULT_SOURCE_ID = "main"


def get_runtime_source(source_id: str | None = None) -> RuntimeSourceSpec:
    """Return the configured runtime source spec, defaulting to `main`."""
    normalized = str(source_id or DEFAULT_SOURCE_ID).strip() or DEFAULT_SOURCE_ID
    try:
        return RUNTIME_SOURCES[normalized]
    except KeyError as err:
        available = ", ".join(sorted(RUNTIME_SOURCES))
        raise KeyError(f"Unknown runtime source '{normalized}'. Available: {available}") from err


def list_runtime_sources() -> tuple[RuntimeSourceSpec, ...]:
    """Return all runtime source specs in a stable order."""
    return tuple(RUNTIME_SOURCES[source_id] for source_id in sorted(RUNTIME_SOURCES))


def runtime_relative_paths_for_source(source_id: str | None = None) -> tuple[str, ...]:
    """Return the runtime file set expected by the deployed app for one source."""
    return get_runtime_source(source_id).runtime_relative_paths


def local_build_relative_paths_for_source(source_id: str | None = None) -> tuple[str, ...]:
    """Return the local-only build files maintained outside the runtime tree."""
    return get_runtime_source(source_id).local_build_relative_paths


def legacy_relative_paths_for_source(source_id: str | None = None) -> tuple[str, ...]:
    """Return the legacy/debug outputs that should live outside the runtime tree."""
    return get_runtime_source(source_id).legacy_relative_paths


def legacy_cleanup_relative_paths_for_source(source_id: str | None = None) -> tuple[str, ...]:
    """Return extra stale runtime files that should be relocated if present."""
    return get_runtime_source(source_id).legacy_cleanup_relative_paths


def source_supports_exact_min_student_attempt_filter(source_id: str | None = None) -> bool:
    """Return whether one source should expose the exact min-attempts population filter."""
    source = get_runtime_source(source_id)
    return CAPABILITY_HAS_EXACT_MIN_STUDENT_ATTEMPT_FILTER in source.capability_flags


def source_supports_classroom_all_data_option(source_id: str | None = None) -> bool:
    """Return whether one source exposes an explicit All data classroom selection."""
    source = get_runtime_source(source_id)
    return CAPABILITY_HAS_CLASSROOM_ALL_DATA_OPTION in source.capability_flags

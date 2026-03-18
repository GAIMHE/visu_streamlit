"""Page-specific Hugging Face runtime asset lists for Streamlit pages."""

from __future__ import annotations

OVERVIEW_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "data/learning_catalog.json",
    "artifacts/derived/fact_attempt_core.parquet",
    "artifacts/derived/work_mode_transition_paths.parquet",
)

BOTTLENECKS_TRANSITIONS_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "artifacts/derived/agg_activity_daily.parquet",
    "artifacts/derived/agg_transition_edges.parquet",
)

MATRIX_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "data/learning_catalog.json",
    "artifacts/derived/agg_activity_daily.parquet",
    "artifacts/derived/agg_exercise_daily.parquet",
    "artifacts/derived/agg_activity_elo.parquet",
    "artifacts/derived/agg_exercise_elo.parquet",
    "artifacts/derived/fact_attempt_core.parquet",
)

ZPDES_TRANSITION_EFFICIENCY_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "data/learning_catalog.json",
    "data/zpdes_rules.json",
    "artifacts/derived/agg_activity_daily.parquet",
    "artifacts/derived/agg_activity_elo.parquet",
    "artifacts/derived/zpdes_exercise_progression_events.parquet",
)

CLASSROOM_REPLAY_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "artifacts/derived/fact_attempt_core.parquet",
)

CLASSROOM_SANKEY_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "data/learning_catalog.json",
    "artifacts/derived/fact_attempt_core.parquet",
)

STUDENT_ELO_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "data/learning_catalog.json",
    "artifacts/derived/agg_exercise_elo.parquet",
    "artifacts/derived/agg_exercise_elo_iterative.parquet",
    "artifacts/derived/student_elo_profiles.parquet",
    "artifacts/derived/student_elo_events.parquet",
    "artifacts/derived/student_elo_profiles_iterative.parquet",
    "artifacts/derived/student_elo_events_iterative.parquet",
)

USAGE_PAGE_RUNTIME_RELATIVE_PATHS: tuple[str, ...] = (
    "artifacts/derived/fact_attempt_core.parquet",
    "artifacts/derived/agg_module_usage_daily.parquet",
    "artifacts/derived/agg_playlist_module_usage.parquet",
    "artifacts/derived/agg_module_activity_usage.parquet",
)

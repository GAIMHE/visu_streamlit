"""Source-aware page registry for the custom Streamlit navigation shell."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from types import ModuleType

from visu2.runtime_sources import RuntimeSourceSpec


@dataclass(frozen=True, slots=True)
class PageSpec:
    """Metadata needed to render one page in the custom navigation shell."""

    page_id: str
    label: str
    icon: str
    module_path: str
    bootstrap_runtime_paths: tuple[str, ...]
    remote_query_paths: tuple[str, ...] = ()
    required_capabilities: frozenset[str] = frozenset()

    def is_supported_by(self, source: RuntimeSourceSpec) -> bool:
        return self.page_id in source.supported_pages and self.required_capabilities.issubset(
            source.capability_flags
        )


PAGE_SPECS: tuple[PageSpec, ...] = (
    PageSpec(
        page_id="overview",
        label="Overview",
        icon=":bar_chart:",
        module_path="page_modules.overview",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "artifacts/derived/fact_attempt_core.parquet",
        ),
    ),
    PageSpec(
        page_id="cohort_filter_viewer",
        label="Cohort Filter Viewer",
        icon=":bar_chart:",
        module_path="page_modules.9_cohort_filter_viewer",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "artifacts/derived/fact_attempt_core.parquet",
        ),
    ),
    PageSpec(
        page_id="bottlenecks",
        label="Bottlenecks and Transitions",
        icon=":twisted_rightwards_arrows:",
        module_path="page_modules.1_bottlenecks_and_transitions",
        bootstrap_runtime_paths=(
            "artifacts/derived/agg_activity_daily.parquet",
            "artifacts/derived/agg_transition_edges.parquet",
        ),
        remote_query_paths=("artifacts/derived/fact_attempt_core.parquet",),
    ),
    PageSpec(
        page_id="matrix",
        label="Objective-Activity Matrix",
        icon=":bar_chart:",
        module_path="page_modules.2_objective_activity_matrix",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "artifacts/derived/agg_activity_daily.parquet",
            "artifacts/derived/agg_exercise_daily.parquet",
            "artifacts/derived/agg_activity_elo.parquet",
            "artifacts/derived/agg_exercise_elo.parquet",
            "artifacts/derived/fact_attempt_core.parquet",
        ),
    ),
    PageSpec(
        page_id="zpdes_transition_efficiency",
        label="ZPDES Transition Efficiency",
        icon=":bar_chart:",
        module_path="page_modules.3_zpdes_transition_efficiency",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "data/zpdes_rules.json",
            "artifacts/derived/agg_activity_daily.parquet",
            "artifacts/derived/agg_activity_elo.parquet",
            "artifacts/derived/zpdes_exercise_progression_events.parquet",
        ),
        remote_query_paths=("artifacts/derived/fact_attempt_core.parquet",),
    ),
    PageSpec(
        page_id="m1_individual_path",
        label="Module 1 Individual Path",
        icon=":bar_chart:",
        module_path="page_modules.8_m1_individual_path",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "data/zpdes_rules.json",
            "artifacts/derived/student_elo_profiles.parquet",
        ),
        remote_query_paths=("artifacts/derived/fact_attempt_core.parquet",),
    ),
    PageSpec(
        page_id="classroom_replay",
        label="Classroom Progression Replay",
        icon=":bar_chart:",
        module_path="page_modules.4_classroom_progression_replay",
        bootstrap_runtime_paths=(),
        remote_query_paths=("artifacts/derived/fact_attempt_core.parquet",),
    ),
    PageSpec(
        page_id="student_elo",
        label="Student Elo Evolution",
        icon=":bar_chart:",
        module_path="page_modules.5_student_elo_evolution",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "artifacts/derived/agg_exercise_elo.parquet",
            "artifacts/derived/student_elo_profiles.parquet",
        ),
        remote_query_paths=(
            "artifacts/derived/fact_attempt_core.parquet",
            "artifacts/derived/student_elo_events.parquet",
            "artifacts/derived/student_elo_events_batch_replay.parquet",
        ),
    ),
    PageSpec(
        page_id="classroom_sankey",
        label="Classroom Progression Sankey",
        icon=":bar_chart:",
        module_path="page_modules.6_classroom_progression_sankey",
        bootstrap_runtime_paths=("data/learning_catalog.json",),
        remote_query_paths=("artifacts/derived/fact_attempt_core.parquet",),
    ),
    PageSpec(
        page_id="student_objective_spider",
        label="Student Objective Spider",
        icon=":bar_chart:",
        module_path="page_modules.7_student_objective_spider",
        bootstrap_runtime_paths=(
            "data/learning_catalog.json",
            "artifacts/derived/fact_attempt_core.parquet",
        ),
    ),
)

PAGE_SPEC_BY_ID = {page.page_id: page for page in PAGE_SPECS}


def visible_pages_for_source(source: RuntimeSourceSpec) -> tuple[PageSpec, ...]:
    """Return the visible page set for the active source."""
    return tuple(page for page in PAGE_SPECS if page.is_supported_by(source))


def default_page_id_for_source(source: RuntimeSourceSpec) -> str:
    """Return the first supported page id for a source."""
    pages = visible_pages_for_source(source)
    if not pages:
        raise ValueError(f"Source '{source.source_id}' has no visible pages.")
    return pages[0].page_id


def import_page_module(page: PageSpec) -> ModuleType:
    """Import a page module on demand."""
    return importlib.import_module(page.module_path)

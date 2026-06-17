"""Source registry, settings, and page-visibility regression tests."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from page_registry import visible_pages_for_source

from visu2.config import get_settings
from visu2.runtime_sources import (
    get_runtime_source,
    legacy_relative_paths_for_source,
    list_runtime_sources,
    local_build_relative_paths_for_source,
    runtime_relative_paths_for_source,
    source_supports_classroom_all_data_option,
    source_supports_exact_min_student_attempt_filter,
)


def test_runtime_source_ids_are_unique() -> None:
    source_ids = [spec.source_id for spec in list_runtime_sources()]
    assert len(source_ids) == len(set(source_ids))


def test_get_settings_resolves_source_local_roots() -> None:
    am_settings = get_settings("am")
    maureen_settings = get_settings("maureen_m16fr")
    mia_settings = get_settings("mia")
    assert am_settings.source_id == "am"
    assert maureen_settings.source_id == "maureen_m16fr"
    assert mia_settings.source_id == "mia"
    assert am_settings.runtime_root != maureen_settings.runtime_root
    assert str(am_settings.runtime_root).endswith("artifacts\\sources\\am")
    assert str(maureen_settings.runtime_root).endswith("artifacts\\sources\\maureen_m16fr")
    assert str(mia_settings.runtime_root).endswith("artifacts\\sources\\mia")
    assert str(am_settings.local_root).endswith("artifacts\\local\\am")
    assert str(maureen_settings.local_root).endswith("artifacts\\local\\maureen_m16fr")
    assert str(mia_settings.local_root).endswith("artifacts\\local\\mia")
    assert str(am_settings.legacy_root).endswith("artifacts\\legacy\\am")
    assert str(maureen_settings.legacy_root).endswith("artifacts\\legacy\\maureen_m16fr")
    assert str(mia_settings.legacy_root).endswith("artifacts\\legacy\\mia")
    assert am_settings.data_dir.parent == am_settings.runtime_root
    assert maureen_settings.data_dir.parent == maureen_settings.runtime_root
    assert mia_settings.data_dir.parent == mia_settings.runtime_root


def test_runtime_relative_paths_are_source_scoped() -> None:
    am_paths = runtime_relative_paths_for_source("am")
    maureen_paths = runtime_relative_paths_for_source("maureen_m16fr")
    mia_paths = runtime_relative_paths_for_source("mia")
    assert "artifacts/derived/zpdes_exercise_progression_events.parquet" in am_paths
    assert "artifacts/derived/zpdes_exercise_progression_events.parquet" not in maureen_paths
    assert "artifacts/derived/zpdes_exercise_progression_events.parquet" in mia_paths
    assert "artifacts/derived/student_elo_events.parquet" in maureen_paths
    assert "artifacts/derived/student_elo_events.parquet" in mia_paths
    assert "artifacts/derived/student_elo_events_batch_replay.parquet" not in am_paths
    assert "artifacts/derived/student_elo_profiles_batch_replay.parquet" not in maureen_paths
    assert "artifacts/derived/student_elo_profiles_batch_replay.parquet" not in mia_paths
    assert "artifacts/derived/classroom_mode_profiles.parquet" in am_paths
    assert "data/exercises.json" not in am_paths
    assert "artifacts/reports/derived_manifest.json" not in am_paths
    assert "artifacts/derived/classroom_activity_summary_by_mode.parquet" not in maureen_paths
    assert "data/zpdes_rules.json" in mia_paths


def test_local_build_relative_paths_are_separate_from_runtime() -> None:
    am_paths = local_build_relative_paths_for_source("am")
    maureen_paths = local_build_relative_paths_for_source("maureen_m16fr")
    mia_paths = local_build_relative_paths_for_source("mia")
    assert "data/student_interaction.parquet" in am_paths
    assert "data/exercises.json" in am_paths
    assert "data/zpdes_rules.json" not in am_paths
    assert "data/zpdes_rules.json" in maureen_paths
    assert "data/zpdes_rules.json" not in mia_paths
    assert "data/exercises.json" in mia_paths
    assert "artifacts/reports/consistency_report.json" in maureen_paths
    assert "artifacts/reports/derived_manifest.json" in am_paths


def test_legacy_relative_paths_are_not_runtime_required() -> None:
    am_runtime = set(runtime_relative_paths_for_source("am"))
    am_legacy = set(legacy_relative_paths_for_source("am"))
    assert "artifacts/derived/hierarchy_context_lookup.parquet" in am_legacy
    assert "artifacts/reports/hierarchy_resolution_report.json" in am_legacy
    assert "artifacts/derived/work_mode_transition_paths.parquet" in am_legacy
    assert am_runtime.isdisjoint(am_legacy)


def test_visible_pages_hide_unsupported_maureen_views() -> None:
    am_pages = {page.page_id for page in visible_pages_for_source(get_runtime_source("am"))}
    maureen_pages = {page.page_id for page in visible_pages_for_source(get_runtime_source("maureen_m16fr"))}
    mia_pages = {page.page_id for page in visible_pages_for_source(get_runtime_source("mia"))}
    assert {
        "classroom_replay",
        "classroom_sankey",
        "zpdes_transition_efficiency",
        "m1_individual_path",
    }.issubset(am_pages)
    assert "student_objective_spider" in maureen_pages
    assert "student_elo" in maureen_pages
    assert "classroom_sankey" in maureen_pages
    assert "classroom_replay" in maureen_pages
    assert "zpdes_transition_efficiency" not in maureen_pages
    assert "m1_individual_path" not in maureen_pages
    assert "zpdes_transition_efficiency" in mia_pages
    assert "m1_individual_path" not in mia_pages
    assert mia_pages == (maureen_pages | {"zpdes_transition_efficiency"})


def test_exact_min_student_attempt_filter_is_enabled_for_partial_sources_only() -> None:
    assert source_supports_exact_min_student_attempt_filter("am") is False
    assert source_supports_exact_min_student_attempt_filter("maureen_m16fr") is True
    assert source_supports_exact_min_student_attempt_filter("mia") is True


def test_classroom_all_data_option_is_enabled_for_partial_sources_only() -> None:
    assert source_supports_classroom_all_data_option("am") is False
    assert source_supports_classroom_all_data_option("maureen_m16fr") is True
    assert source_supports_classroom_all_data_option("mia") is True

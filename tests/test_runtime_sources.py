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
    list_runtime_sources,
    runtime_relative_paths_for_source,
)


def test_runtime_source_ids_are_unique() -> None:
    source_ids = [spec.source_id for spec in list_runtime_sources()]
    assert len(source_ids) == len(set(source_ids))


def test_get_settings_resolves_source_local_roots() -> None:
    main_settings = get_settings("main")
    maureen_settings = get_settings("maureen_m16fr")
    assert main_settings.source_id == "main"
    assert maureen_settings.source_id == "maureen_m16fr"
    assert main_settings.runtime_root != maureen_settings.runtime_root
    assert str(main_settings.runtime_root).endswith("artifacts\\sources\\main")
    assert str(maureen_settings.runtime_root).endswith("artifacts\\sources\\maureen_m16fr")
    assert main_settings.data_dir.parent == main_settings.runtime_root
    assert maureen_settings.data_dir.parent == maureen_settings.runtime_root


def test_runtime_relative_paths_are_source_scoped() -> None:
    main_paths = runtime_relative_paths_for_source("main")
    maureen_paths = runtime_relative_paths_for_source("maureen_m16fr")
    assert "artifacts/derived/zpdes_exercise_progression_events.parquet" in main_paths
    assert "artifacts/derived/zpdes_exercise_progression_events.parquet" not in maureen_paths
    assert "artifacts/derived/student_elo_events.parquet" in maureen_paths
    assert "artifacts/derived/classroom_mode_profiles.parquet" in main_paths
    assert "artifacts/derived/classroom_activity_summary_by_mode.parquet" in maureen_paths


def test_visible_pages_hide_unsupported_maureen_views() -> None:
    main_pages = {page.page_id for page in visible_pages_for_source(get_runtime_source("main"))}
    maureen_pages = {page.page_id for page in visible_pages_for_source(get_runtime_source("maureen_m16fr"))}
    assert {"classroom_replay", "classroom_sankey", "zpdes_transition_efficiency"}.issubset(main_pages)
    assert "student_objective_spider" in maureen_pages
    assert "student_elo" in maureen_pages
    assert "classroom_sankey" in maureen_pages
    assert "classroom_replay" in maureen_pages
    assert "zpdes_transition_efficiency" not in maureen_pages

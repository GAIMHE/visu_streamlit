from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from page_registry import PAGE_SPEC_BY_ID, default_page_id_for_source

from visu2.runtime_sources import get_runtime_source


def test_student_elo_bootstrap_paths_exclude_heavy_event_tables() -> None:
    page = PAGE_SPEC_BY_ID["student_elo"]
    assert "artifacts/derived/student_elo_events.parquet" not in page.bootstrap_runtime_paths
    assert "artifacts/derived/student_elo_events_iterative.parquet" not in page.bootstrap_runtime_paths
    assert "artifacts/derived/fact_attempt_core.parquet" not in page.bootstrap_runtime_paths
    assert page.remote_query_paths == (
        "artifacts/derived/fact_attempt_core.parquet",
        "artifacts/derived/student_elo_events.parquet",
        "artifacts/derived/student_elo_events_iterative.parquet",
    )


def test_classroom_pages_bootstrap_only_selector_artifacts() -> None:
    replay = PAGE_SPEC_BY_ID["classroom_replay"]
    sankey = PAGE_SPEC_BY_ID["classroom_sankey"]

    assert replay.bootstrap_runtime_paths == ("artifacts/derived/classroom_mode_profiles.parquet",)
    assert replay.remote_query_paths == ("artifacts/derived/fact_attempt_core.parquet",)

    assert sankey.bootstrap_runtime_paths == (
        "data/learning_catalog.json",
        "artifacts/derived/classroom_mode_profiles.parquet",
    )
    assert sankey.remote_query_paths == ("artifacts/derived/fact_attempt_core.parquet",)


def test_default_page_id_for_source_uses_first_visible_page() -> None:
    assert default_page_id_for_source(get_runtime_source("main")) == "overview"
    assert default_page_id_for_source(get_runtime_source("maureen_m16fr")) == "overview"

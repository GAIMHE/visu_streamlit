from __future__ import annotations

import importlib
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
    assert "artifacts/derived/student_elo_events_batch_replay.parquet" not in page.bootstrap_runtime_paths
    assert "artifacts/derived/student_elo_events_iterative.parquet" not in page.bootstrap_runtime_paths
    assert "artifacts/derived/fact_attempt_core.parquet" not in page.bootstrap_runtime_paths
    assert "artifacts/derived/student_elo_profiles_batch_replay.parquet" not in page.bootstrap_runtime_paths
    assert page.remote_query_paths == (
        "artifacts/derived/fact_attempt_core.parquet",
        "artifacts/derived/student_elo_events.parquet",
        "artifacts/derived/student_elo_events_batch_replay.parquet",
    )


def test_student_elo_page_exposes_current_and_batch_replay_systems() -> None:
    module = importlib.import_module("page_modules.5_student_elo_evolution")

    assert tuple(module.ELO_SYSTEM_CONFIGS.keys()) == ("Sequential Replay Elo", "Batch Replay Elo")
    assert (
        module.ELO_SYSTEM_CONFIGS["Sequential Replay Elo"]["events_relative_path"]
        == "artifacts/derived/student_elo_events.parquet"
    )
    assert (
        module.ELO_SYSTEM_CONFIGS["Batch Replay Elo"]["events_relative_path"]
        == "artifacts/derived/student_elo_events_batch_replay.parquet"
    )


def test_m1_individual_path_page_bootstraps_light_assets_only() -> None:
    page = PAGE_SPEC_BY_ID["m1_individual_path"]

    assert page.bootstrap_runtime_paths == (
        "data/learning_catalog.json",
        "data/zpdes_rules.json",
        "artifacts/derived/student_elo_profiles.parquet",
    )
    assert page.remote_query_paths == ("artifacts/derived/fact_attempt_core.parquet",)


def test_classroom_pages_bootstrap_only_selector_artifacts() -> None:
    replay = PAGE_SPEC_BY_ID["classroom_replay"]
    sankey = PAGE_SPEC_BY_ID["classroom_sankey"]

    assert replay.bootstrap_runtime_paths == ()
    assert replay.remote_query_paths == ("artifacts/derived/fact_attempt_core.parquet",)

    assert sankey.bootstrap_runtime_paths == ("data/learning_catalog.json",)
    assert sankey.remote_query_paths == ("artifacts/derived/fact_attempt_core.parquet",)


def test_default_page_id_for_source_uses_first_visible_page() -> None:
    assert default_page_id_for_source(get_runtime_source("main")) == "overview"
    assert default_page_id_for_source(get_runtime_source("maureen_m16fr")) == "overview"
    assert default_page_id_for_source(get_runtime_source("mia_module1")) == "overview"

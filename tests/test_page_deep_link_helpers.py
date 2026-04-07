from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))


def test_classroom_pages_map_query_mode_scope_to_sidebar_label() -> None:
    replay = importlib.import_module("page_modules.4_classroom_progression_replay")
    sankey = importlib.import_module("page_modules.6_classroom_progression_sankey")

    assert replay._initial_mode_label("playlist") == "Playlist"
    assert sankey._initial_mode_label("all") == "All modes"
    assert replay._initial_mode_label("unknown") == "ZPDES"
    assert sankey._initial_mode_label(None) == "ZPDES"


def test_student_elo_prefers_query_module_when_available() -> None:
    module = importlib.import_module("page_modules.5_student_elo_evolution")

    assert module._preferred_option_index(["M1", "M31", "M41"], "M31") == 1
    assert module._preferred_option_index(["M1", "M31", "M41"], "missing") == 0
    assert module._preferred_option_index(["M1"], None) == 0

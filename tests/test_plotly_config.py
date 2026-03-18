"""Tests for shared Plotly chart config helpers."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from plotly_config import build_plotly_chart_config


def test_build_plotly_chart_config_sets_svg_export_by_default() -> None:
    """Shared config should use SVG export by default."""
    config = build_plotly_chart_config()

    assert config["displaylogo"] is False
    assert config["toImageButtonOptions"]["format"] == "svg"
    assert config["toImageButtonOptions"]["scale"] == 4


def test_build_plotly_chart_config_merges_extra_options() -> None:
    """Extra options should merge without dropping the export defaults."""
    config = build_plotly_chart_config(
        modebar_buttons_to_remove=["select2d", "lasso2d"],
        extra={"toImageButtonOptions": {"filename": "custom-export"}},
    )

    assert config["modeBarButtonsToRemove"] == ["select2d", "lasso2d"]
    assert config["toImageButtonOptions"]["format"] == "svg"
    assert config["toImageButtonOptions"]["scale"] == 4
    assert config["toImageButtonOptions"]["filename"] == "custom-export"

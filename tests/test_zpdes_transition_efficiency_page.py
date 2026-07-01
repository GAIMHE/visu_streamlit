"""Validate page-level helpers for the ZPDES transition-efficiency view."""

from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

page_module = importlib.import_module("page_modules.3_zpdes_transition_efficiency")


def test_activity_summary_supplies_full_module_selector_labels(tmp_path: Path) -> None:
    """Use full module names in selectors and fall back to codes for blank names."""
    path = tmp_path / "agg_activity_daily.parquet"
    pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)],
            "module_code": ["M5", "M5", "M6"],
            "module_label": [
                "Amélioration de la production écrite et de l'activité rédactionnelle",
                "Amélioration de la production écrite et de l'activité rédactionnelle",
                "  ",
            ],
        }
    ).write_parquet(path)

    summary = page_module._load_activity_daily_summary(path)

    assert summary["module_codes"] == ["M5", "M6"]
    assert page_module._format_module_option("M5", summary["module_labels"]) == (
        "Amélioration de la production écrite et de l'activité rédactionnelle (M5)"
    )
    assert page_module._format_module_option("M6", summary["module_labels"]) == "M6"

"""Validate shared overview helper behavior."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from overview_shared import normalize_date_input_range


def test_normalize_date_input_range_accepts_single_date() -> None:
    """Treat a single Streamlit date input as a one-day inclusive range."""
    assert normalize_date_input_range(date(2025, 1, 2)) == (
        date(2025, 1, 2),
        date(2025, 1, 2),
    )


def test_normalize_date_input_range_accepts_date_tuple() -> None:
    """Keep a two-date range unchanged."""
    assert normalize_date_input_range((date(2025, 1, 2), date(2025, 1, 4))) == (
        date(2025, 1, 2),
        date(2025, 1, 4),
    )


def test_normalize_date_input_range_rejects_empty_selection() -> None:
    """Reject empty or malformed date selections."""
    assert normalize_date_input_range(()) is None
    assert normalize_date_input_range(("2025-01-01",)) is None

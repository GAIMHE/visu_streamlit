"""Validate shared overview helper behavior."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

import polars as pl
from overview_shared import apply_min_student_attempts_filter, normalize_date_input_range


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


def test_apply_min_student_attempts_filter_keeps_only_eligible_students() -> None:
    """The shared population filter should keep only students above the visible-attempt threshold."""
    fact = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 2)],
            "user_id": ["u1", "u1", "u2", "u3"],
            "module_code": ["M1", "M1", "M1", "M1"],
            "objective_id": ["o1", "o1", "o1", "o1"],
            "activity_id": ["a1", "a1", "a1", "a1"],
        }
    )

    filtered = apply_min_student_attempts_filter(fact, min_student_attempts=2).collect()

    assert filtered["user_id"].to_list() == ["u1", "u1"]

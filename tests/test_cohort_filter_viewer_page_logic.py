from __future__ import annotations

import importlib
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

page_module = importlib.import_module("page_modules.9_cohort_filter_viewer")


def test_load_cohort_attempt_rows_filters_by_date_range(tmp_path: Path) -> None:
    fact_path = tmp_path / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "created_at": [datetime(2025, 1, 1, 8, 0, 0), datetime(2025, 1, 2, 8, 0, 0)],
            "date_utc": [datetime(2025, 1, 1).date(), datetime(2025, 1, 2).date()],
            "work_mode": ["adaptive-test", "zpdes"],
            "module_code": ["M1", "M1"],
            "exercise_id": ["e1", "e2"],
            "attempt_number": [1, 1],
        }
    ).write_parquet(fact_path)

    result = page_module._load_cohort_attempt_rows(
        fact_path,
        start_date_iso="2025-01-02",
        end_date_iso="2025-01-02",
    )

    assert result.height == 1
    assert result.item(0, "user_id") == "u2"


def test_sync_multiselect_state_discards_stale_values(monkeypatch) -> None:
    session_state: dict[str, object] = {"cohort_key": ["M1", "M99"]}
    monkeypatch.setattr(page_module.st, "session_state", session_state)

    page_module._sync_multiselect_state("cohort_key", ["M1", "M31"], default_all=False)

    assert session_state["cohort_key"] == ["M1"]


def test_clamp_date_range_resets_invalid_order() -> None:
    min_date = datetime(2025, 1, 1).date()
    max_date = datetime(2025, 1, 31).date()

    start_date, end_date = page_module._clamp_date_range(
        datetime(2025, 2, 1).date(),
        datetime(2024, 12, 31).date(),
        min_date=min_date,
        max_date=max_date,
    )

    assert (start_date, end_date) == (min_date, max_date)


def test_format_schema_table_includes_share_columns() -> None:
    frame = pl.DataFrame(
        {
            "cleaned_schema": ["adaptive-test -> zpdes"],
            "students": [4],
            "attempts": [20],
            "student_share": [0.4],
            "attempt_share": [0.25],
        }
    )

    formatted = page_module._format_schema_table(frame)

    assert formatted.columns == ["Schema", "Students", "Student share", "Attempts", "Attempt share"]
    assert formatted.item(0, "Student share") == "40.0%"
    assert formatted.item(0, "Attempt share") == "25.0%"

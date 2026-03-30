from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

import page_modules.overview as overview_module
from page_modules.overview import (
    _build_overview_kpi_analysis,
    _load_work_mode_transition_paths_from_fact,
)


def test_load_work_mode_transition_paths_derives_student_attempt_index(tmp_path: Path) -> None:
    fact_path = tmp_path / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 8, 0, 0),
                datetime(2025, 1, 1, 8, 5, 0),
                datetime(2025, 1, 1, 9, 0, 0),
            ],
            "date_utc": [
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
            ],
            "user_id": ["u1", "u1", "u2"],
            "module_code": ["M16", "M16", "M16"],
            "objective_id": ["o1", "o1", "o1"],
            "activity_id": ["a1", "a1", "a1"],
            "work_mode": ["adaptive-test", "zpdes", "adaptive-test"],
        }
    ).write_parquet(fact_path)

    paths = _load_work_mode_transition_paths_from_fact(
        fact_path,
        start_date_iso="2025-01-01",
        end_date_iso="2025-01-01",
        module_code=None,
        objective_id=None,
        activity_id=None,
        min_student_attempts=1,
    )

    rows = {row["user_id"]: row for row in paths.to_dicts()}
    assert rows["u1"]["first_work_mode"] == "adaptive-test"
    assert rows["u1"]["transition_1_mode"] == "zpdes"
    assert rows["u1"]["transition_count_total"] == 1
    assert rows["u2"]["first_work_mode"] == "adaptive-test"
    assert rows["u2"]["transition_count_total"] == 0


def test_build_overview_kpi_analysis_tolerates_legacy_signature(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def legacy_analyze_overview_kpis(*, attempts: int, unique_students: int, unique_exercises: int):
        captured["attempts"] = attempts
        captured["unique_students"] = unique_students
        captured["unique_exercises"] = unique_exercises
        return "ok"

    monkeypatch.setattr(overview_module, "analyze_overview_kpis", legacy_analyze_overview_kpis)

    result = _build_overview_kpi_analysis(
        source_id="main",
        attempts=10,
        unique_students=2,
        unique_exercises=3,
    )

    assert result == "ok"
    assert captured == {
        "attempts": 10,
        "unique_students": 2,
        "unique_exercises": 3,
    }

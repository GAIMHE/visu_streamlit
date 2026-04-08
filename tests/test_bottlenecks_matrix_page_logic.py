from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

bottlenecks_module = importlib.import_module("page_modules.1_bottlenecks_and_transitions")
matrix_module = importlib.import_module("page_modules.2_objective_activity_matrix")


def test_annotate_transition_edges_with_source_objective_share() -> None:
    activity_frame = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1)] * 4,
            "module_code": ["M1"] * 4,
            "objective_id": ["o1", "o1", "o2", "o2"],
            "objective_label": ["Objective 1", "Objective 1", "Objective 2", "Objective 2"],
            "activity_id": ["a1", "a2", "a3", "a4"],
            "attempts": [60.0, 40.0, 30.0, 20.0],
        }
    )
    transition_edges = pd.DataFrame(
        {
            "from_activity_id": ["a1", "a3"],
            "to_activity_id": ["b1", "b2"],
            "from_activity_label": ["A1", "A3"],
            "to_activity_label": ["B1", "B2"],
            "transition_count": [25, 10],
            "success_conditioned_count": [12, 7],
        }
    )

    result = bottlenecks_module._annotate_transition_edges_with_source_objective_share(
        transition_edges,
        activity_frame=activity_frame,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        module_code="M1",
        objective_id=None,
        canonical_modules=("M1",),
    )

    rows = {row["from_activity_id"]: row for row in result.to_dict("records")}
    assert rows["a1"]["source_objective_attempts"] == 100.0
    assert rows["a1"]["source_objective_attempt_share"] == 0.25
    assert rows["a3"]["source_objective_attempts"] == 50.0
    assert rows["a3"]["source_objective_attempt_share"] == 0.2


def test_rename_drilldown_display_columns_marks_median_duration_unit() -> None:
    frame = pl.DataFrame(
        {
            "exercise_short_id": ["e1"],
            "attempts": [10.0],
            "median_duration": [3703.0],
        }
    )

    renamed = matrix_module._rename_drilldown_display_columns(frame, metric="success_rate")
    assert "median_duration (ms)" in renamed.columns
    assert "median_duration" not in renamed.columns

    elo_unchanged = matrix_module._rename_drilldown_display_columns(
        frame, metric="activity_mean_exercise_elo"
    )
    assert "median_duration" in elo_unchanged.columns

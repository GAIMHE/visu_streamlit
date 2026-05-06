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


def test_matrix_excludes_lost_exercises_rows_by_label() -> None:
    frame = pl.DataFrame(
        {
            "module_label": ["Numbers", "Lost exercises", "Numbers", "Numbers"],
            "objective_label": ["Objective 1", "Objective 2", "Lost exercises", "Objective 4"],
            "activity_label": ["Activity 1", "Activity 2", "Activity 3", "Lost exercises"],
            "activity_id": ["a1", "a2", "a3", "a4"],
        }
    )

    filtered = matrix_module._exclude_lost_exercises(frame)

    assert filtered["activity_id"].to_list() == ["a1"]


def test_matrix_excludes_lost_exercises_rows_from_lazy_frame() -> None:
    frame = pl.DataFrame(
        {
            "objective_label": ["Objective 1", " LOST EXERCISES "],
            "activity_id": ["a1", "lost"],
        }
    )

    filtered = matrix_module._exclude_lost_exercises(frame.lazy()).collect()

    assert filtered["activity_id"].to_list() == ["a1"]


def test_matrix_text_color_grid_uses_light_text_on_dark_cells() -> None:
    colors = matrix_module._matrix_text_color_grid(
        [[0.2, 0.56, 0.9, None]],
        z_min=0.0,
        z_max=1.0,
    )

    assert colors == [[
        matrix_module.MATRIX_CELL_DARK_TEXT,
        matrix_module.MATRIX_CELL_LIGHT_TEXT,
        matrix_module.MATRIX_CELL_LIGHT_TEXT,
        matrix_module.MATRIX_CELL_DARK_TEXT,
    ]]


def test_matrix_text_layer_points_keep_per_cell_colors() -> None:
    points = matrix_module._matrix_text_layer_points(
        x_labels=["A1", "A2"],
        y_labels=["O1"],
        text_grid=[["10%", "90%"]],
        color_grid=[[
            matrix_module.MATRIX_CELL_DARK_TEXT,
            matrix_module.MATRIX_CELL_LIGHT_TEXT,
        ]],
    )

    assert points == {
        "x": ["A1", "A2"],
        "y": ["O1", "O1"],
        "text": ["10%", "90%"],
        "colors": [
            matrix_module.MATRIX_CELL_DARK_TEXT,
            matrix_module.MATRIX_CELL_LIGHT_TEXT,
        ],
    }

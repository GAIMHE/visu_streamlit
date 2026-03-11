"""
test_objective_activity_matrix.py

Validate matrix cell construction, formatting, and drilldown behavior.

Dependencies
------------
- datetime
- polars
- visu2

Classes
-------
- None.

Functions
---------
- _summary_payload: Utility for summary payload.
- _activity_daily_sample: Utility for activity daily sample.
- _exercise_daily_sample: Utility for exercise daily sample.
- _activity_elo_sample: Utility for activity elo sample.
- _exercise_elo_sample: Utility for exercise elo sample.
- test_weighted_metrics_and_attempt_sums_are_correct: Test scenario for weighted metrics and attempt sums are correct.
- test_exercise_balanced_success_rate_is_correct: Test scenario for exercise balanced success rate is correct.
- test_exercise_balanced_success_rate_requires_exercise_source: Test scenario for exercise balanced success rate requires exercise source.
- test_activity_mean_exercise_elo_uses_dedicated_source: Test scenario for activity mean exercise elo uses dedicated source.
- test_summary_first_order_with_deterministic_fallback_ordering: Test scenario for summary first order with deterministic fallback ordering.
- test_weighted_first_attempt_success_rate_is_correct: Test scenario for weighted first attempt success rate is correct.
- test_ragged_matrix_payload_is_unique_and_padded: Test scenario for ragged matrix payload is unique and padded.
- test_label_fallback_and_objective_row_disambiguation: Test scenario for label fallback and objective row disambiguation.
- test_exercise_drilldown_weighting_and_sorting: Test scenario for exercise drilldown weighting and sorting.
- test_exercise_drilldown_returns_empty_when_no_rows: Test scenario for exercise drilldown returns empty when no rows.
- test_exercise_drilldown_for_activity_elo_uses_elo_rows: Test scenario for exercise drilldown for activity elo uses elo rows.
"""
from __future__ import annotations

from datetime import date

import polars as pl

from visu2.objective_activity_matrix import (
    build_exercise_drilldown_frame,
    build_objective_activity_cells,
    build_ragged_matrix_payload,
    format_cell_value,
)


def _summary_payload() -> dict:
    """Summary payload.


Returns
-------
dict
        Result produced by this routine.

"""
    return {
        "modules": [
            {
                "id": "m1",
                "code": "M1",
                "title": {"short": "Module 1", "long": "Module 1"},
                "objectiveIds": ["o2", "o1"],
            }
        ],
        "objectives": [
            {
                "id": "o1",
                "title": {"short": "Objective One", "long": "Objective One"},
                "activityIds": ["a1"],
            },
            {
                "id": "o2",
                "title": {"short": "Objective Two", "long": "Objective Two"},
                "activityIds": ["a3", "a2"],
            },
        ],
        "activities": [
            {"id": "a1", "title": {"short": "Activity One", "long": "Activity One"}},
            {"id": "a2", "title": {"short": "Activity Two", "long": "Activity Two"}},
            {"id": "a3", "title": {"short": "Activity Three", "long": "Activity Three"}},
        ],
    }


def _activity_daily_sample() -> pl.DataFrame:
    """Activity daily sample.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "date_utc": [
                date(2025, 1, 1),
                date(2025, 1, 2),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
            ],
            "module_code": ["M1", "M1", "M1", "M1", "M1", "M1", "M2"],
            "objective_id": ["o1", "o1", "o2", "o2", "oX", "oX", "o9"],
            "objective_label": [
                "Objective One",
                "Objective One",
                "Objective Two",
                "Objective Two",
                "Fallback Objective",
                "Fallback Objective",
                "Other objective",
            ],
            "activity_id": ["a1", "a1", "a2", "a3", "ax2", "ax1", "b1"],
            "activity_label": [
                "Activity One",
                "Activity One",
                "Activity Two",
                "Activity Three",
                "Zeta Activity",
                "Beta Activity",
                "Other Activity",
            ],
            "attempts": [10, 30, 20, 10, 5, 8, 999],
            "success_rate": [0.5, 0.8, 0.7, 0.6, 0.9, 0.2, 0.9],
            "repeat_attempt_rate": [0.2, 0.1, 0.3, 0.4, 0.1, 0.8, 0.1],
            "first_attempt_success_rate": [0.6, 0.9, 0.75, 0.5, 1.0, 0.0, 1.0],
            "first_attempt_count": [5, 10, 8, 4, 2, 3, 1],
            "module_id": ["m1", "m1", "m1", "m1", "m1", "m1", "m2"],
            "module_label": ["Module 1", "Module 1", "Module 1", "Module 1", "Module 1", "Module 1", "Module 2"],
            "unique_students": [10, 10, 8, 6, 5, 5, 50],
            "median_duration": [12.0, 13.0, 11.0, 14.0, 15.0, 18.0, 9.0],
            "avg_attempt_number": [1.2, 1.3, 1.2, 1.4, 1.1, 1.6, 1.1],
        }
    )


def _exercise_daily_sample() -> pl.DataFrame:
    """Exercise daily sample.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "date_utc": [
                date(2025, 1, 1),
                date(2025, 1, 2),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
            ],
            "module_code": ["M1", "M1", "M1", "M1", "M2"],
            "objective_id": ["o1", "o1", "o1", "o2", "o9"],
            "objective_label": [
                "Objective One",
                "Objective One",
                "Objective One",
                "Objective Two",
                "Other objective",
            ],
            "activity_id": ["a1", "a1", "a1", "a2", "b1"],
            "activity_label": [
                "Activity One",
                "Activity One",
                "Activity One",
                "Activity Two",
                "Other Activity",
            ],
            "exercise_id": ["e1", "e1", "e2", "e3", "e9"],
            "attempts": [10.0, 30.0, 2.0, 5.0, 100.0],
            "success_rate": [0.5, 0.8, 1.0, 0.4, 0.9],
            "repeat_attempt_rate": [0.2, 0.1, 0.0, 0.2, 0.1],
            "median_duration": [12.0, 13.0, 9.0, 11.0, 9.0],
            "avg_attempt_number": [1.2, 1.3, 1.0, 1.1, 1.1],
        }
    )


def _activity_elo_sample() -> pl.DataFrame:
    """Activity elo sample.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "module_id": ["m1", "m1", "m2"],
            "module_code": ["M1", "M1", "M2"],
            "module_label": ["Module 1", "Module 1", "Module 2"],
            "objective_id": ["o1", "o2", "o9"],
            "objective_label": ["Objective One", "Objective Two", "Other objective"],
            "activity_id": ["a1", "a2", "b1"],
            "activity_label": ["Activity One", "Activity Two", "Other Activity"],
            "activity_mean_exercise_elo": [1525.0, 1490.0, 1510.0],
            "calibrated_exercise_count": [2, 1, 1],
            "catalog_exercise_count": [2, 1, 1],
            "calibration_coverage_ratio": [1.0, 1.0, 1.0],
        }
    )


def _exercise_elo_sample() -> pl.DataFrame:
    """Exercise elo sample.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "exercise_id": ["e1", "e2", "e3"],
            "exercise_label": ["Exercise One", "Exercise Two", "Exercise Three"],
            "exercise_type": ["MCQ", "MCQ", "MCQ"],
            "module_id": ["m1", "m1", "m1"],
            "module_code": ["M1", "M1", "M1"],
            "module_label": ["Module 1", "Module 1", "Module 1"],
            "objective_id": ["o1", "o1", "o2"],
            "objective_label": ["Objective One", "Objective One", "Objective Two"],
            "activity_id": ["a1", "a1", "a2"],
            "activity_label": ["Activity One", "Activity One", "Activity Two"],
            "exercise_elo": [1510.0, 1540.0, 1490.0],
            "calibration_attempts": [4, 3, 2],
            "calibration_success_rate": [0.75, 0.33, 0.5],
            "calibrated": [True, True, True],
        }
    )


def _fact_attempt_core_sample() -> pl.DataFrame:
    """Return a compact fact table with playlist and ZPDES rows for work-mode matrix tests."""
    return pl.DataFrame(
        {
            "date_utc": [
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 2),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 2),
            ],
            "module_code": ["M1", "M1", "M1", "M1", "M1", "M1"],
            "objective_id": ["o1", "o1", "o1", "o2", "o1", "o1"],
            "objective_label": [
                "Objective One",
                "Objective One",
                "Objective One",
                "Objective Two",
                "Objective One",
                "Objective One",
            ],
            "activity_id": ["a1", "a1", "a1", "a2", "a1", "a1"],
            "activity_label": [
                "Activity One",
                "Activity One",
                "Activity One",
                "Activity Two",
                "Activity One",
                "Activity One",
            ],
            "exercise_id": ["e1", "e2", "e1", "e3", "e1", "e4"],
            "work_mode": ["playlist", "playlist", "playlist", "playlist", "zpdes", "zpdes"],
            "data_correct": [1, 0, 1, 1, 0, 1],
            "attempt_number": [1, 1, 2, 1, 1, 1],
            "data_duration": [10.0, 20.0, 12.0, 15.0, 11.0, 9.0],
        }
    )


def test_weighted_metrics_and_attempt_sums_are_correct() -> None:
    """Test weighted metrics and attempt sums are correct.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = _activity_daily_sample()
    summary_payload = _summary_payload()

    attempts_cells = build_objective_activity_cells(
        agg_activity_daily=frame,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="attempts",
        summary_payload=summary_payload,
    )
    success_cells = build_objective_activity_cells(
        agg_activity_daily=frame,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="success_rate",
        summary_payload=summary_payload,
    )
    repeat_cells = build_objective_activity_cells(
        agg_activity_daily=frame,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="repeat_attempt_rate",
        summary_payload=summary_payload,
    )

    attempts_value = attempts_cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()
    success_value = success_cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()
    repeat_value = repeat_cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()

    assert float(attempts_value) == 40.0
    assert float(success_value) == 0.725
    assert float(repeat_value) == 0.125
    assert format_cell_value("success_rate", float(success_value)) == "72.5%"


def test_exercise_balanced_success_rate_is_correct() -> None:
    """Test exercise balanced success rate is correct.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = _activity_daily_sample()
    exercise_frame = _exercise_daily_sample()
    summary_payload = _summary_payload()

    cells = build_objective_activity_cells(
        agg_activity_daily=frame,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="exercise_balanced_success_rate",
        summary_payload=summary_payload,
        agg_exercise_daily=exercise_frame,
    )

    value = cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()
    # e1 weighted exercise success = (0.5*10 + 0.8*30) / 40 = 0.725
    # e2 weighted exercise success = 1.0
    # activity metric = mean([0.725, 1.0]) = 0.8625
    assert abs(float(value) - 0.8625) < 1e-9


def test_exercise_balanced_success_rate_requires_exercise_source() -> None:
    """Test exercise balanced success rate requires exercise source.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = _activity_daily_sample()
    try:
        build_objective_activity_cells(
            agg_activity_daily=frame,
            module_code="M1",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            metric="exercise_balanced_success_rate",
            summary_payload=_summary_payload(),
        )
    except ValueError as err:
        assert "requires agg_exercise_daily" in str(err)
        return
    raise AssertionError("Expected ValueError when agg_exercise_daily is missing.")


def test_activity_mean_exercise_elo_uses_dedicated_source() -> None:
    """Test activity mean exercise elo uses dedicated source.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    cells = build_objective_activity_cells(
        agg_activity_daily=_activity_daily_sample(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="activity_mean_exercise_elo",
        summary_payload=_summary_payload(),
        agg_activity_elo=_activity_elo_sample(),
    )

    value = cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()
    assert float(value) == 1525.0
    assert format_cell_value("activity_mean_exercise_elo", float(value)) == "1525"


def test_summary_first_order_with_deterministic_fallback_ordering() -> None:
    """Test summary first order with deterministic fallback ordering.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = _activity_daily_sample()
    cells = build_objective_activity_cells(
        agg_activity_daily=frame,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="attempts",
        summary_payload=_summary_payload(),
    )

    objective_order: list[str] = []
    for row in cells.to_dicts():
        objective_id = row["objective_id"]
        if objective_id not in objective_order:
            objective_order.append(objective_id)

    assert objective_order == ["o2", "o1", "oX"]

    o2_activities = (
        cells.filter(pl.col("objective_id") == "o2")
        .sort("activity_col_idx")["activity_id"]
        .to_list()
    )
    ox_activities = (
        cells.filter(pl.col("objective_id") == "oX")
        .sort("activity_col_idx")["activity_id"]
        .to_list()
    )

    assert o2_activities == ["a3", "a2"]
    assert ox_activities == ["ax1", "ax2"]


def test_weighted_first_attempt_success_rate_is_correct() -> None:
    """Test weighted first attempt success rate is correct.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    cells = build_objective_activity_cells(
        agg_activity_daily=_activity_daily_sample(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="first_attempt_success_rate",
        summary_payload=_summary_payload(),
    )
    value = cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()
    # (0.6*5 + 0.9*10) / (5+10) = 0.8
    assert float(value) == 0.8


def test_ragged_matrix_payload_is_unique_and_padded() -> None:
    """Test ragged matrix payload is unique and padded.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    cells = build_objective_activity_cells(
        agg_activity_daily=_activity_daily_sample(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="attempts",
        summary_payload=_summary_payload(),
    )

    assert (
        cells.group_by(["objective_id", "activity_col_idx"])
        .len()
        .filter(pl.col("len") > 1)
        .height
        == 0
    )

    payload = build_ragged_matrix_payload(cells)
    assert payload["x_labels"] == ["A1", "A2"]
    assert len(payload["y_labels"]) == 3

    counts_by_objective = {
        row["objective_id"]: int(row["len"])
        for row in cells.group_by("objective_id").len().to_dicts()
    }
    for objective_id, row_values in zip(payload["objective_ids"], payload["z_values"], strict=False):
        populated = sum(1 for value in row_values if value is not None)
        assert populated == counts_by_objective[objective_id]


def test_label_fallback_and_objective_row_disambiguation() -> None:
    """Test label fallback and objective row disambiguation.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)],
            "module_code": ["MZ", "MZ", "MZ"],
            "objective_id": ["oA", "oB", "oC"],
            "objective_label": ["Shared", "Shared", None],
            "activity_id": ["aA", "aB", "aC"],
            "activity_label": [None, "Named", None],
            "attempts": [5, 4, 3],
            "success_rate": [0.5, 0.5, 0.5],
            "repeat_attempt_rate": [0.2, 0.2, 0.2],
            "module_id": ["mz", "mz", "mz"],
            "module_label": ["Module Z", "Module Z", "Module Z"],
            "unique_students": [2, 2, 2],
            "median_duration": [10.0, 10.0, 10.0],
            "avg_attempt_number": [1.0, 1.0, 1.0],
        }
    )

    cells = build_objective_activity_cells(
        agg_activity_daily=frame,
        module_code="MZ",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        metric="attempts",
        summary_payload={"modules": [], "objectives": [], "activities": []},
    )

    objective_labels = cells.select("objective_row_label")["objective_row_label"].to_list()
    assert len(set(objective_labels)) == len(objective_labels)
    assert all(isinstance(label, str) and label.strip() for label in objective_labels)

    activity_fallback_label = cells.filter(pl.col("activity_id") == "aA")["activity_label"].item()
    assert activity_fallback_label == "aA"

    objective_fallback_label = cells.filter(pl.col("objective_id") == "oC")["objective_label"].item()
    assert objective_fallback_label == "oC"


def test_exercise_drilldown_weighting_and_sorting() -> None:
    """Test exercise drilldown weighting and sorting.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)],
            "module_code": ["M1", "M1", "M1"],
            "objective_id": ["o1", "o1", "o1"],
            "activity_id": ["a1", "a1", "a1"],
            "exercise_id": ["e1", "e1", "e2"],
            "exercise_label": ["Exercise One", "Exercise One", "Exercise Two"],
            "exercise_type": ["MULTIPLE_CHOICE", "MULTIPLE_CHOICE", "MULTIPLE_CHOICE"],
            "attempts": [10.0, 30.0, 20.0],
            "success_rate": [0.5, 0.8, 0.7],
            "first_attempt_success_rate": [0.6, 0.9, 0.75],
            "first_attempt_count": [5.0, 10.0, 8.0],
            "median_duration": [10.0, 20.0, 15.0],
            "repeat_attempt_rate": [0.2, 0.1, 0.3],
            "avg_attempt_number": [1.2, 1.3, 1.1],
        }
    )

    drilldown = build_exercise_drilldown_frame(
        agg_exercise_daily=frame,
        module_code="M1",
        objective_id="o1",
        activity_id="a1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="success_rate",
    )
    assert drilldown.height == 2
    # e1 weighted success: (0.5*10 + 0.8*30) / 40 = 0.725
    e1_success = drilldown.filter(pl.col("exercise_id") == "e1")["success_rate"].item()
    assert float(e1_success) == 0.725
    # Sorted by metric descending, tie-break attempts descending.
    assert drilldown[0, "exercise_id"] == "e1"

    first_attempt = build_exercise_drilldown_frame(
        agg_exercise_daily=frame,
        module_code="M1",
        objective_id="o1",
        activity_id="a1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="first_attempt_success_rate",
    )
    e1_first = first_attempt.filter(pl.col("exercise_id") == "e1")["first_attempt_success_rate"].item()
    assert float(e1_first) == 0.8


def test_exercise_drilldown_returns_empty_when_no_rows() -> None:
    """Test exercise drilldown returns empty when no rows.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    frame = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1)],
            "module_code": ["M1"],
            "objective_id": ["o1"],
            "activity_id": ["a1"],
            "exercise_id": ["e1"],
            "exercise_label": ["Exercise One"],
            "exercise_type": ["MULTIPLE_CHOICE"],
            "attempts": [10.0],
            "success_rate": [0.5],
            "first_attempt_success_rate": [0.6],
            "first_attempt_count": [5.0],
            "median_duration": [10.0],
            "repeat_attempt_rate": [0.2],
            "avg_attempt_number": [1.2],
        }
    )
    result = build_exercise_drilldown_frame(
        agg_exercise_daily=frame,
        module_code="M1",
        objective_id="o9",
        activity_id="a9",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="attempts",
    )
    assert result.height == 0


def test_exercise_drilldown_for_activity_elo_uses_elo_rows() -> None:
    """Test exercise drilldown for activity elo uses elo rows.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    drilldown = build_exercise_drilldown_frame(
        agg_exercise_daily=_exercise_daily_sample(),
        module_code="M1",
        objective_id="o1",
        activity_id="a1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="activity_mean_exercise_elo",
        agg_exercise_elo=_exercise_elo_sample(),
    )

    assert {"exercise_elo", "calibration_attempts", "calibration_success_rate"}.issubset(
        set(drilldown.columns)
    )
    first_row = drilldown.to_dicts()[0]
    assert float(first_row["metric_value"]) == 1540.0


def test_work_mode_filtered_cells_use_fact_attempt_core() -> None:
    """Test cohort-population matrix cells are recomputed from the fact table."""
    cells = build_objective_activity_cells(
        agg_activity_daily=_activity_daily_sample(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="success_rate",
        summary_payload=_summary_payload(),
        fact_attempt_core=_fact_attempt_core_sample(),
        work_mode="playlist",
    )

    playlist_a1 = cells.filter((pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1"))
    assert playlist_a1.height == 1
    assert float(playlist_a1["metric_value"].item()) == 2.0 / 3.0
    assert playlist_a1["metric_text"].item() == "66.7%"

    balanced_cells = build_objective_activity_cells(
        agg_activity_daily=_activity_daily_sample(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="exercise_balanced_success_rate",
        summary_payload=_summary_payload(),
        fact_attempt_core=_fact_attempt_core_sample(),
        work_mode="playlist",
    )
    balanced_value = balanced_cells.filter(
        (pl.col("objective_id") == "o1") & (pl.col("activity_id") == "a1")
    )["metric_value"].item()
    assert float(balanced_value) == 0.5


def test_work_mode_filtered_drilldown_uses_fact_attempt_core() -> None:
    """Test cohort-population drilldown rows are computed from first-order fact data."""
    drilldown = build_exercise_drilldown_frame(
        agg_exercise_daily=_exercise_daily_sample(),
        module_code="M1",
        objective_id="o1",
        activity_id="a1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        metric="repeat_attempt_rate",
        fact_attempt_core=_fact_attempt_core_sample(),
        work_mode="playlist",
    )

    e1_row = drilldown.filter(pl.col("exercise_id") == "e1")
    assert e1_row.height == 1
    assert float(e1_row["attempts"].item()) == 2.0
    assert float(e1_row["repeat_attempt_rate"].item()) == 0.5
    assert float(e1_row["first_attempt_success_rate"].item()) == 1.0


def test_playlist_unique_exercises_is_rejected_for_non_playlist_mode() -> None:
    """Test the playlist-only metric cannot be requested with another cohort population."""
    try:
        build_objective_activity_cells(
            agg_activity_daily=_activity_daily_sample(),
            module_code="M1",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            metric="playlist_unique_exercises",
            summary_payload=_summary_payload(),
            fact_attempt_core=_fact_attempt_core_sample(),
            work_mode="zpdes",
        )
    except ValueError as err:
        assert "playlist_unique_exercises" in str(err)
        return
    raise AssertionError("Expected playlist_unique_exercises to reject non-playlist work mode.")

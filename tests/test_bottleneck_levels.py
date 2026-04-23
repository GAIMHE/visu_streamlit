"""
test_bottleneck_levels.py

Validate bottleneck aggregation levels and ranking behavior.

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
- _sample_activity_daily: Utility for sample activity daily.
- test_build_bottleneck_frame_is_id_safe_and_plot_labels_are_unique: Test scenario for build bottleneck frame is id safe and plot labels are unique.
- test_apply_bottleneck_filters_context_aware_by_level: Test scenario for apply bottleneck filters context aware by level.
- test_apply_bottleneck_filters_enforces_canonical_module_scope: Test scenario for apply bottleneck filters enforces canonical module scope.
"""
from __future__ import annotations

from datetime import date

import polars as pl

from visu2.bottleneck import apply_bottleneck_filters, build_bottleneck_frame


def _sample_activity_daily() -> pl.DataFrame:
    """Sample activity daily.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "date_utc": [
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 2),
                date(2025, 1, 2),
                date(2025, 1, 2),
                date(2025, 1, 2),
            ],
            "module_id": ["m31", "m31", "m31", "m31", "m32", "m32", "m99", "m32"],
            "module_code": ["M31", "M31", "M31", "M31", "M32", "M32", "M99", "M32"],
            "module_label": [
                "Module 31",
                "Module 31",
                "Module 31",
                "Module 31",
                "Module 32",
                "Module 32",
                "Module 99",
                "Module 32",
            ],
            "objective_id": ["o1", "o1", "o2", "o2", "o3", "o4", "o9", "o4"],
            "objective_label": [
                "Shared Objective",
                "Shared Objective",
                "Shared Objective",
                "Shared Objective",
                "Objective 3",
                "Objective 4",
                "Objective 9",
                "Objective 4",
            ],
            "activity_id": ["a1", "a2", "a3", "a4", "a5", "a6", "a9", "a6"],
            "activity_label": [
                "Shared Activity",
                "Shared Activity",
                "Activity 3",
                "Activity 4",
                "Activity 5",
                "Activity 6",
                "Activity 9",
                "Activity 6",
            ],
            "attempts": [120, 70, 90, 30, 55, 40, 80, 20],
            "success_rate": [0.40, 0.55, 0.52, 0.70, 0.65, 0.50, 0.45, 0.60],
            "repeat_attempt_rate": [0.25, 0.10, 0.15, 0.05, 0.07, 0.18, 0.20, 0.16],
            "retry_before_success_rate": [0.18, 0.06, 0.12, 0.04, 0.05, 0.14, 0.16, 0.10],
            "unique_students": [50, 30, 40, 15, 25, 20, 35, 12],
            "median_duration": [20.0, 22.0, 18.0, 15.0, 14.0, 16.0, 17.0, 16.5],
            "avg_attempt_number": [1.2, 1.3, 1.15, 1.05, 1.1, 1.2, 1.25, 1.18],
        }
    )


def test_build_bottleneck_frame_is_id_safe_and_plot_labels_are_unique() -> None:
    """Test build bottleneck frame is id safe and plot labels are unique.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    source = _sample_activity_daily()
    for level in ("Module", "Objective", "Activity"):
        frame = build_bottleneck_frame(
            filtered_activity=source,
            level=level,
            min_attempts=1,
            top_n=50,
        )
        assert not frame.empty
        assert frame["entity_id"].nunique() == len(frame.index)
        assert frame["entity_plot_label"].nunique() == len(frame.index)


def test_apply_bottleneck_filters_context_aware_by_level() -> None:
    """Test apply bottleneck filters context aware by level.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    source = _sample_activity_daily()
    start = date(2025, 1, 1)
    end = date(2025, 1, 2)

    module_level = apply_bottleneck_filters(
        frame=source,
        start_date=start,
        end_date=end,
        module_code="M31",
        objective_id="o1",
        activity_id="a1",
        level="Module",
    )
    objective_level = apply_bottleneck_filters(
        frame=source,
        start_date=start,
        end_date=end,
        module_code="M31",
        objective_id="o1",
        activity_id="a1",
        level="Objective",
    )
    activity_level = apply_bottleneck_filters(
        frame=source,
        start_date=start,
        end_date=end,
        module_code="M31",
        objective_id="o1",
        activity_id="a1",
        level="Activity",
    )

    assert module_level.select(pl.col("objective_id").n_unique()).item() == 2
    assert objective_level.select(pl.col("objective_id").n_unique()).item() == 1
    assert objective_level.select(pl.col("activity_id").n_unique()).item() == 2
    assert activity_level.select(pl.col("activity_id").n_unique()).item() == 1


def test_apply_bottleneck_filters_enforces_canonical_module_scope() -> None:
    """Test apply bottleneck filters enforces canonical module scope.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    source = _sample_activity_daily()
    start = date(2025, 1, 1)
    end = date(2025, 1, 2)

    all_filtered = apply_bottleneck_filters(
        frame=source,
        start_date=start,
        end_date=end,
        module_code=None,
        objective_id=None,
        activity_id=None,
        level="Activity",
    )
    assert "M99" not in set(all_filtered["module_code"].to_list())

    only_non_canonical = apply_bottleneck_filters(
        frame=source,
        start_date=start,
        end_date=end,
        module_code="M99",
        objective_id=None,
        activity_id=None,
        level="Module",
    )
    assert only_non_canonical.height == 0


def test_apply_bottleneck_filters_accepts_source_specific_module_scope() -> None:
    """Test source-specific module scopes can override the main-dataset canonical modules."""
    source = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1)],
            "module_id": ["m16", "m99"],
            "module_code": ["M16", "M99"],
            "module_label": ["Module 16", "Module 99"],
            "objective_id": ["o1", "o9"],
            "objective_label": ["Objective 1", "Objective 9"],
            "activity_id": ["a1", "a9"],
            "activity_label": ["Activity 1", "Activity 9"],
            "attempts": [12, 8],
            "success_rate": [0.4, 0.5],
            "repeat_attempt_rate": [0.2, 0.1],
            "unique_students": [6, 4],
            "median_duration": [10.0, 11.0],
            "avg_attempt_number": [1.1, 1.0],
        }
    )

    filtered = apply_bottleneck_filters(
        frame=source,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        module_code=None,
        objective_id=None,
        activity_id=None,
        level="Activity",
        canonical_modules=("M16",),
    )

    assert filtered.height == 1
    assert filtered["module_code"].to_list() == ["M16"]


def test_build_bottleneck_frame_scores_retry_before_first_success() -> None:
    """The score should ignore repeats that happen after an earlier success."""
    source = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1)],
            "module_id": ["m1", "m1"],
            "module_code": ["M1", "M1"],
            "module_label": ["Module 1", "Module 1"],
            "objective_id": ["o1", "o1"],
            "objective_label": ["Objective 1", "Objective 1"],
            "activity_id": ["after_success_replays", "unresolved_retries"],
            "activity_label": ["After success replays", "Unresolved retries"],
            "attempts": [100, 100],
            "success_rate": [0.5, 0.5],
            "repeat_attempt_rate": [0.9, 0.1],
            "retry_before_success_rate": [0.0, 0.8],
            "unique_students": [50, 50],
            "median_duration": [20.0, 20.0],
            "avg_attempt_number": [2.0, 2.0],
        }
    )

    frame = build_bottleneck_frame(
        filtered_activity=source,
        level="Activity",
        min_attempts=1,
        top_n=2,
    )

    assert frame.iloc[0]["entity_id"] == "unresolved_retries"
    assert frame.iloc[0]["bottleneck_retry_rate"] == 0.8

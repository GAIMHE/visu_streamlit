from __future__ import annotations

from datetime import date

import polars as pl

from visu2.bottleneck import apply_bottleneck_filters, build_bottleneck_frame


def _sample_activity_daily() -> pl.DataFrame:
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
            "unique_students": [50, 30, 40, 15, 25, 20, 35, 12],
            "median_duration": [20.0, 22.0, 18.0, 15.0, 14.0, 16.0, 17.0, 16.5],
            "avg_attempt_number": [1.2, 1.3, 1.15, 1.05, 1.1, 1.2, 1.25, 1.18],
        }
    )


def test_build_bottleneck_frame_is_id_safe_and_plot_labels_are_unique() -> None:
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

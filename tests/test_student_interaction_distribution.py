from __future__ import annotations

import polars as pl

from visu2.student_interaction_distribution import (
    build_student_interaction_counts,
    build_student_interaction_histogram,
    filter_student_interaction_counts,
    summarize_student_interaction_counts,
)


def test_build_student_interaction_counts_ignores_blank_students() -> None:
    fact = pl.DataFrame(
        {
            "user_id": ["s1", "s1", "s2", "", None],
            "exercise_id": ["e1", "e2", "e3", "e4", "e5"],
        }
    )

    counts = build_student_interaction_counts(fact)

    assert counts.to_dicts() == [
        {"user_id": "s1", "interactions": 2},
        {"user_id": "s2", "interactions": 1},
    ]


def test_threshold_summary_reports_excluded_outliers() -> None:
    counts = pl.DataFrame(
        {
            "user_id": ["s1", "s2", "s3", "s4"],
            "interactions": [10, 20, 30, 5_100],
        }
    )

    summary = summarize_student_interaction_counts(counts, max_interactions=5_000)
    filtered = filter_student_interaction_counts(counts, max_interactions=5_000)

    assert filtered["user_id"].to_list() == ["s1", "s2", "s3"]
    assert summary["total_students"] == 4
    assert summary["retained_students"] == 3
    assert summary["excluded_students"] == 1
    assert summary["mean"] == 20.0
    assert summary["median"] == 20.0
    assert summary["max"] == 30


def test_histogram_uses_thresholded_axis_range_and_reference_lines() -> None:
    counts = pl.DataFrame(
        {
            "user_id": ["s1", "s2", "s3", "s4"],
            "interactions": [10, 20, 30, 5_100],
        }
    )

    figure = build_student_interaction_histogram(counts, max_interactions=5_000, bin_count=30)

    assert figure.layout.xaxis.range[0] == 0
    assert figure.layout.xaxis.range[1] < 5_100
    assert len(figure.layout.shapes) == 2
    assert {trace.name for trace in figure.data if trace.name} >= {"Students", "Mean: 20.0", "Median: 20.0"}

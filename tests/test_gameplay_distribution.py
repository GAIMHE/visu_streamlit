from __future__ import annotations

import polars as pl
import pytest

from visu2.gameplay_distribution import (
    GAMEPLAY_SUBPLOT_HORIZONTAL_SPACING,
    GAMEPLAY_VALUE_LABEL_RANGE_RATIO,
    GAMEPLAY_Y_AXIS_TITLE_STANDOFF,
    build_gameplay_distribution,
    build_gameplay_distribution_figure,
    top_gameplays,
)


def _exercise_daily_sample() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exercise_type": [
                "Multiple Choice",
                "Multiple Choice",
                "Number Input",
                "Number Input",
                "Drag Drop",
                "",
            ],
            "exercise_id": ["e1", "e1", "e2", "e3", "e4", "e5"],
            "attempts": [10, 7, 5, 20, 3, 11],
        }
    )


def test_build_gameplay_distribution_sums_attempts_and_counts_unique_exercises() -> None:
    summary = build_gameplay_distribution(_exercise_daily_sample(), include_unknown=False)

    rows = {row["gameplay_type"]: row for row in summary.iter_rows(named=True)}
    assert rows["Multiple Choice"]["attempts"] == 17
    assert rows["Multiple Choice"]["unique_exercises"] == 1
    assert rows["Number Input"]["attempts"] == 25
    assert rows["Number Input"]["unique_exercises"] == 2
    assert rows["Drag Drop"]["attempts"] == 3
    assert rows["Drag Drop"]["unique_exercises"] == 1
    assert "unknown" not in rows
    assert summary["attempt_share"].sum() == pytest.approx(1.0)
    assert summary["exercise_share"].sum() == pytest.approx(1.0)


def test_build_gameplay_distribution_can_include_unknown_bucket() -> None:
    summary = build_gameplay_distribution(_exercise_daily_sample(), include_unknown=True)

    rows = {row["gameplay_type"]: row for row in summary.iter_rows(named=True)}
    assert rows["unknown"]["attempts"] == 11
    assert rows["unknown"]["unique_exercises"] == 1


def test_top_gameplays_ranks_deterministically_by_metric_then_name() -> None:
    summary = build_gameplay_distribution(_exercise_daily_sample(), include_unknown=False)

    by_attempts = top_gameplays(summary, metric="attempts", top_n=2)
    by_exercises = top_gameplays(summary, metric="unique_exercises", top_n=2)

    assert by_attempts["gameplay_type"].to_list() == ["Number Input", "Multiple Choice"]
    assert by_exercises["gameplay_type"].to_list() == ["Number Input", "Drag Drop"]


def test_top_gameplays_rejects_unknown_metric() -> None:
    summary = build_gameplay_distribution(_exercise_daily_sample())

    with pytest.raises(ValueError, match="Unsupported gameplay ranking metric"):
        top_gameplays(summary, metric="success_rate", top_n=5)


def test_build_gameplay_distribution_figure_uses_two_panels() -> None:
    summary = build_gameplay_distribution(_exercise_daily_sample(), include_unknown=False)

    figure = build_gameplay_distribution_figure(summary, top_n=3)

    assert len(figure.data) == 4
    assert figure.data[0].orientation == "h"
    assert figure.data[1].orientation == "h"
    assert figure.data[0].name == "Unique exercises"
    assert figure.data[1].name == "Attempts"
    assert figure.data[2].mode == "text"
    assert figure.data[3].mode == "text"
    assert max(figure.data[3].x) > max(figure.data[1].x)
    assert figure.layout.xaxis2.range[1] == pytest.approx(
        max(figure.data[1].x) * GAMEPLAY_VALUE_LABEL_RANGE_RATIO
    )
    assert figure.layout.xaxis2.domain[0] - figure.layout.xaxis.domain[1] == pytest.approx(
        GAMEPLAY_SUBPLOT_HORIZONTAL_SPACING
    )
    assert figure.layout.margin.l == 96
    assert figure.layout.yaxis.title.text == "Gameplay type"
    assert figure.layout.yaxis.title.standoff == GAMEPLAY_Y_AXIS_TITLE_STANDOFF
    assert figure.layout.yaxis2.title.text == ""
    assert figure.layout.title.text is None

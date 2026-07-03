from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from visu2.derive_elo import _fit_batch_student_rating_weighted
from visu2.work_mode_segment_elo_export import build_work_mode_segment_elo_export


def _event(
    *,
    minute: float,
    work_mode: str,
    module: str,
    exercise: str,
    success: bool,
    attempt_number: int = 1,
) -> dict[str, object]:
    return {
        "user_id": "student",
        "classroom_id": "classroom",
        "playlist_or_module_id": f"path-{work_mode}",
        "module_id": f"module-{module}",
        "module_code": module,
        "module_label": f"Module {module}",
        "objective_id": f"objective-{module}",
        "activity_id": f"activity-{module}",
        "exercise_id": exercise,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=minute),
        "data_correct": success,
        "work_mode": work_mode,
        "attempt_number": attempt_number,
    }


def _write_inputs(fact_path: Path, exercise_elo_path: Path) -> None:
    rows = [
        _event(
            minute=-1,
            work_mode="adaptive-test",
            module="M1",
            exercise="first_0",
            success=False,
        )
    ]
    for index in range(30):
        rows.append(
            _event(
                minute=index,
                work_mode="playlist",
                module="M1",
                exercise=f"first_{index}",
                success=index < 15,
                attempt_number=2 if index == 0 else 1,
            )
        )
    rows.append(
        _event(
            minute=0.5,
            work_mode="playlist",
            module="M1",
            exercise="first_0",
            success=False,
            attempt_number=3,
        )
    )
    for index in range(10):
        rows.append(
            _event(
                minute=30 + index,
                work_mode="zpdes",
                module="M1",
                exercise=f"short_{index}",
                success=True,
            )
        )
    for index in range(30):
        rows.append(
            _event(
                minute=40 + index,
                work_mode="playlist",
                module="M1",
                exercise=f"second_{index}",
                success=index >= 15,
            )
        )
    pl.DataFrame(rows).write_parquet(fact_path)

    exercise_rows = []
    for prefix, count in (("first", 30), ("short", 10), ("second", 30)):
        for index in range(count):
            exercise_rows.append(
                {
                    "module_code": "M1",
                    "objective_id": "objective-M1",
                    "activity_id": "activity-M1",
                    "exercise_id": f"{prefix}_{index}",
                    "exercise_elo": 1500.0,
                    "calibrated": True,
                }
            )
    pl.DataFrame(exercise_rows).write_parquet(exercise_elo_path)


def test_segment_export_uses_first_15_of_contiguous_runs_with_at_least_30(
    tmp_path: Path,
) -> None:
    fact_path = tmp_path / "fact.parquet"
    exercise_elo_path = tmp_path / "exercise_elo.parquet"
    _write_inputs(fact_path, exercise_elo_path)

    frame = build_work_mode_segment_elo_export(fact_path, exercise_elo_path)

    assert frame.height == 30
    assert frame["segment_id"].n_unique() == 2
    assert frame["work_mode"].unique().to_list() == ["playlist"]
    assert frame["segment_total_attempts"].unique().to_list() == [30]
    assert frame.group_by("segment_id").len()["len"].to_list() == [15, 15]
    assert frame.group_by("segment_id").agg(
        pl.col("segment_attempt_position").min().alias("minimum"),
        pl.col("segment_attempt_position").max().alias("maximum"),
    ).select(["minimum", "maximum"]).unique().to_dicts() == [
        {"minimum": 1, "maximum": 15}
    ]
    assert frame.filter(pl.col("exercise_id") == "first_0").height == 1
    assert frame.filter(pl.col("exercise_id") == "first_0")["attempt_number"].item() == 2

    segments = (
        frame.group_by("segment_id")
        .agg(
            pl.col("segment_ordinal").first(),
            pl.col("segment_success_rate_first_window").first(),
            pl.col("student_elo_first_window").first(),
            pl.col("segment_elo_attempts").first(),
        )
        .sort("segment_ordinal")
    )
    assert segments["segment_ordinal"].to_list() == [1, 3]
    assert segments["segment_success_rate_first_window"].to_list() == [1.0, 0.0]
    assert segments["segment_elo_attempts"].to_list() == [15, 15]
    assert segments["student_elo_first_window"][0] > 1500.0
    assert segments["student_elo_first_window"][1] < 1500.0
    expected_success_elo = _fit_batch_student_rating_weighted(
        [(1500.0, 15, 15.0)],
        initial_rating=1500.0,
    )
    assert abs(segments["student_elo_first_window"][0] - expected_success_elo) < 1e-9


def test_segment_export_validates_window_and_minimum_lengths(tmp_path: Path) -> None:
    fact_path = tmp_path / "fact.parquet"
    exercise_elo_path = tmp_path / "exercise_elo.parquet"
    _write_inputs(fact_path, exercise_elo_path)

    try:
        build_work_mode_segment_elo_export(
            fact_path,
            exercise_elo_path,
            first_window_attempts=15,
            min_segment_attempts=14,
        )
    except ValueError as error:
        assert "greater than or equal" in str(error)
    else:
        raise AssertionError("Expected an invalid segment length configuration to fail")

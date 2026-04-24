"""
test_derive_shapes.py

Validate derived table shapes and required columns after build.

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
- _sample_fact: Utility for sample fact.
- test_activity_agg_shape: Test scenario for activity agg shape.
- test_activity_agg_first_attempt_success_rate_is_computed_correctly: Test scenario for activity agg first attempt success rate is computed correctly.
- test_objective_agg_shape: Test scenario for objective agg shape.
- test_student_module_agg_shape: Test scenario for student module agg shape.
- test_module_usage_daily_shape_and_keys: Test scenario for module usage daily shape and keys.
- test_playlist_module_usage_shape_and_keys: Test scenario for playlist module usage shape and keys.
- test_playlist_usage_drops_null_module_row_when_mapped_exists: Test scenario for playlist usage drops null module row when mapped exists.
- test_module_activity_usage_shape_and_keys: Test scenario for module activity usage shape and keys.
- test_exercise_daily_shape_and_keys: Test scenario for exercise daily shape and keys.
- test_exercise_elo_shape_and_keys: Test scenario for exercise elo shape and keys.
- test_activity_elo_shape_and_keys: Test scenario for activity elo shape and keys.
- test_student_elo_events_and_profiles_shape: Test scenario for student elo events and profiles shape.
"""
from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from visu2.derive import (
    build_agg_activity_daily_from_fact,
    build_agg_activity_elo_from_exercise_elo,
    build_agg_exercise_daily_from_fact,
    build_agg_exercise_elo_from_fact,
    build_agg_exercise_elo_iterative_from_fact,
    build_agg_module_activity_usage_from_fact,
    build_agg_module_usage_daily_from_fact,
    build_agg_objective_daily_from_fact,
    build_agg_playlist_module_usage_from_fact,
    build_agg_student_module_progress_from_fact,
    build_student_elo_events_batch_replay_from_fact,
    build_student_elo_events_from_fact,
    build_student_elo_profiles_batch_replay_from_events,
    build_student_elo_profiles_from_events,
)


def _sample_fact() -> pl.DataFrame:
    """Sample fact.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 5, 0),
                datetime(2025, 1, 2, 9, 0, 0),
            ],
            "date_utc": [
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 2).date(),
            ],
            "user_id": ["u1", "u1", "u2"],
            "classroom_id": ["c1", "c1", "c2"],
            "playlist_or_module_id": ["p1", "p1", "p2"],
            "activity_id": ["a1", "a2", "a1"],
            "activity_label": ["Activity 1", "Activity 2", "Activity 1"],
            "exercise_id": ["e1", "e2", "e1"],
            "objective_id": ["o1", "o1", "o1"],
            "objective_label": ["Objective 1", "Objective 1", "Objective 1"],
            "module_id": ["m1", "m1", "m1"],
            "module_code": ["M1", "M1", "M1"],
            "module_label": ["Module 1", "Module 1", "Module 1"],
            "work_mode": ["playlist", "playlist", "playlist"],
            "data_correct": [True, False, True],
            "data_duration": [10.0, 20.0, 15.0],
            "session_duration": [None, 30.0, None],
            "attempt_number": [1, 2, 1],
        }
    )


def test_activity_agg_shape() -> None:
    """Test activity agg shape.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    agg = build_agg_activity_daily_from_fact(_sample_fact())
    assert agg.height >= 2
    assert {
        "date_utc",
        "activity_id",
        "attempts",
        "success_rate",
        "first_attempt_success_rate",
        "first_attempt_count",
        "retry_before_success_rate",
    }.issubset(set(agg.columns))


def test_activity_agg_first_attempt_success_rate_is_computed_correctly() -> None:
    """Test activity agg first attempt success rate is computed correctly.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    fact = pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 1, 0),
                datetime(2025, 1, 1, 10, 2, 0),
            ],
            "date_utc": [datetime(2025, 1, 1).date()] * 3,
            "user_id": ["u1", "u1", "u2"],
            "classroom_id": ["c1", "c1", "c1"],
            "playlist_or_module_id": ["p1", "p1", "p1"],
            "activity_id": ["a1", "a1", "a1"],
            "activity_label": ["Activity 1", "Activity 1", "Activity 1"],
            "exercise_id": ["e1", "e1", "e2"],
            "objective_id": ["o1", "o1", "o1"],
            "objective_label": ["Objective 1", "Objective 1", "Objective 1"],
            "module_id": ["m1", "m1", "m1"],
            "module_code": ["M1", "M1", "M1"],
            "module_label": ["Module 1", "Module 1", "Module 1"],
            "work_mode": ["playlist", "playlist", "playlist"],
            "data_correct": [True, False, True],
            "data_duration": [10.0, 20.0, 15.0],
            "session_duration": [10.0, 20.0, 15.0],
            "attempt_number": [1, 2, 1],
        }
    )
    agg = build_agg_activity_daily_from_fact(fact)
    assert agg.height == 1
    row = agg.to_dicts()[0]
    assert int(row["first_attempt_count"]) == 2
    assert float(row["first_attempt_success_rate"]) == 1.0


def test_activity_agg_retry_before_success_rate_excludes_post_success_replays() -> None:
    """Repeated attempts after a prior success should not count as unresolved retries."""
    fact = pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 1, 0),
                datetime(2025, 1, 1, 10, 2, 0),
                datetime(2025, 1, 1, 10, 3, 0),
            ],
            "date_utc": [datetime(2025, 1, 1).date()] * 4,
            "user_id": ["u1"] * 4,
            "classroom_id": ["c1"] * 4,
            "playlist_or_module_id": ["p1"] * 4,
            "activity_id": ["a1"] * 4,
            "activity_label": ["Activity 1"] * 4,
            "exercise_id": ["e1"] * 4,
            "objective_id": ["o1"] * 4,
            "objective_label": ["Objective 1"] * 4,
            "module_id": ["m1"] * 4,
            "module_code": ["M1"] * 4,
            "module_label": ["Module 1"] * 4,
            "work_mode": ["zpdes"] * 4,
            "data_correct": [False, False, True, True],
            "data_duration": [10.0, 11.0, 12.0, 13.0],
            "session_duration": [10.0, 11.0, 12.0, 13.0],
            "attempt_number": [1, 2, 3, 4],
        }
    )

    agg = build_agg_activity_daily_from_fact(fact)
    row = agg.to_dicts()[0]

    assert row["repeat_attempt_rate"] == pytest.approx(0.75)
    assert row["retry_before_success_rate"] == pytest.approx(0.5)


def test_objective_agg_shape() -> None:
    """Test objective agg shape.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    agg = build_agg_objective_daily_from_fact(_sample_fact())
    assert agg.height >= 2
    assert {"date_utc", "objective_id", "attempts", "unique_students"}.issubset(set(agg.columns))


def test_student_module_agg_shape() -> None:
    """Test student module agg shape.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    agg = build_agg_student_module_progress_from_fact(_sample_fact())
    assert agg.height >= 2
    assert {"date_utc", "user_id", "module_id", "attempts", "last_attempt_at"}.issubset(
        set(agg.columns)
    )


def test_module_usage_daily_shape_and_keys() -> None:
    """Test module usage daily shape and keys.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    agg = build_agg_module_usage_daily_from_fact(_sample_fact())
    assert {"date_utc", "module_code", "attempts", "unique_students"}.issubset(set(agg.columns))
    assert agg.group_by(["date_utc", "module_code"]).len().filter(pl.col("len") > 1).height == 0


def test_playlist_module_usage_shape_and_keys() -> None:
    """Test playlist module usage shape and keys.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    agg = build_agg_playlist_module_usage_from_fact(_sample_fact())
    assert {"playlist_or_module_id", "module_code", "work_mode", "attempts", "unique_activities"}.issubset(
        set(agg.columns)
    )
    assert set(agg["work_mode"].to_list()) == {"playlist"}
    assert (
        agg.group_by(["playlist_or_module_id", "module_code"]).len().filter(pl.col("len") > 1).height
        == 0
    )


def test_playlist_usage_drops_null_module_row_when_mapped_exists() -> None:
    """Test playlist usage drops null module row when mapped exists.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    fact = pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 2, 0),
            ],
            "date_utc": [datetime(2025, 1, 1).date(), datetime(2025, 1, 1).date()],
            "user_id": ["u1", "u1"],
            "classroom_id": ["c1", "c1"],
            "playlist_or_module_id": ["p_same", "p_same"],
            "activity_id": ["a1", "a2"],
            "activity_label": ["Activity 1", "Activity 2"],
            "objective_id": ["o1", "o1"],
            "objective_label": ["Objective 1", "Objective 1"],
            "module_id": ["m1", None],
            "module_code": ["M1", None],
            "module_label": ["Module 1", None],
            "work_mode": ["module", "playlist"],
            "data_correct": [True, False],
            "data_duration": [10.0, 20.0],
            "session_duration": [10.0, 20.0],
            "attempt_number": [1, 2],
        }
    )
    agg = build_agg_playlist_module_usage_from_fact(fact)
    rows_for_playlist = agg.filter(pl.col("playlist_or_module_id") == "p_same")
    assert rows_for_playlist.height == 1
    assert rows_for_playlist["module_code"][0] == "M1"


def test_module_activity_usage_shape_and_keys() -> None:
    """Test module activity usage shape and keys.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    agg = build_agg_module_activity_usage_from_fact(_sample_fact())
    assert {"module_code", "activity_id", "attempts", "activity_share_within_module"}.issubset(
        set(agg.columns)
    )
    assert agg.group_by(["module_code", "activity_id"]).len().filter(pl.col("len") > 1).height == 0


def test_exercise_daily_shape_and_keys() -> None:
    """Test exercise daily shape and keys.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    from visu2.config import get_settings

    agg = build_agg_exercise_daily_from_fact(_sample_fact(), settings=get_settings("main"))
    assert {
        "date_utc",
        "module_code",
        "objective_id",
        "activity_id",
        "exercise_id",
        "exercise_label",
        "exercise_type",
        "attempts",
        "success_rate",
        "first_attempt_success_rate",
        "first_attempt_count",
        "repeat_attempt_rate",
        "retry_before_success_rate",
    }.issubset(set(agg.columns))
    assert (
        agg.group_by(["date_utc", "module_code", "objective_id", "activity_id", "exercise_id"])
        .len()
        .filter(pl.col("len") > 1)
        .height
        == 0
    )


def test_exercise_elo_shape_and_keys() -> None:
    """Test exercise elo shape and keys.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    from visu2.config import get_settings

    agg = build_agg_exercise_elo_from_fact(_sample_fact(), settings=get_settings("main"))
    assert {
        "exercise_id",
        "exercise_elo",
        "calibration_attempts",
        "calibration_success_rate",
        "calibrated",
        "activity_id",
    }.issubset(set(agg.columns))


def test_activity_elo_shape_and_keys() -> None:
    """Test activity elo shape and keys.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    from visu2.config import get_settings

    exercise_elo = build_agg_exercise_elo_from_fact(_sample_fact(), settings=get_settings("main"))
    agg = build_agg_activity_elo_from_exercise_elo(exercise_elo, settings=get_settings("main"))
    assert {
        "activity_id",
        "activity_mean_exercise_elo",
        "calibrated_exercise_count",
        "catalog_exercise_count",
        "calibration_coverage_ratio",
    }.issubset(set(agg.columns))


def test_iterative_exercise_elo_shape_and_keys() -> None:
    """Test iterative exercise Elo shape and keys."""
    from visu2.config import get_settings

    agg = build_agg_exercise_elo_iterative_from_fact(_sample_fact(), settings=get_settings("main"))
    assert {
        "exercise_id",
        "exercise_elo",
        "calibration_attempts",
        "calibration_success_rate",
        "calibrated",
        "smoothed_calibration_success_rate",
        "activity_id",
    }.issubset(set(agg.columns))


def test_student_elo_events_and_profiles_shape() -> None:
    """Test student elo events and profiles shape.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    from visu2.config import get_settings

    exercise_elo = build_agg_exercise_elo_from_fact(_sample_fact(), settings=get_settings("main"))
    events = build_student_elo_events_from_fact(_sample_fact(), exercise_elo)
    profiles = build_student_elo_profiles_from_events(events)

    assert {
        "user_id",
        "attempt_ordinal",
        "exercise_elo",
        "student_elo_pre",
        "student_elo_post",
    }.issubset(set(events.columns))
    assert {
        "user_id",
        "module_code",
        "module_label",
        "total_attempts",
        "final_student_elo",
        "eligible_for_replay",
    }.issubset(set(profiles.columns))


def test_batch_replay_student_elo_events_and_profiles_shape() -> None:
    """Test Batch Replay Elo events and profiles expose the same runtime shape."""
    from visu2.config import get_settings

    exercise_elo = build_agg_exercise_elo_from_fact(_sample_fact(), settings=get_settings("main"))
    events = build_student_elo_events_batch_replay_from_fact(_sample_fact(), exercise_elo)
    profiles = build_student_elo_profiles_batch_replay_from_events(events)

    assert {
        "user_id",
        "attempt_ordinal",
        "exercise_elo",
        "student_elo_pre",
        "student_elo_post",
    }.issubset(set(events.columns))
    assert {
        "user_id",
        "module_code",
        "module_label",
        "total_attempts",
        "final_student_elo",
        "eligible_for_replay",
    }.issubset(set(profiles.columns))

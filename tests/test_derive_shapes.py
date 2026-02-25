from __future__ import annotations

from datetime import datetime

import polars as pl

from visu2.derive import (
    build_agg_activity_daily_from_fact,
    build_agg_exercise_daily_from_fact,
    build_agg_module_activity_usage_from_fact,
    build_agg_module_usage_daily_from_fact,
    build_agg_objective_daily_from_fact,
    build_agg_playlist_module_usage_from_fact,
    build_agg_student_module_exposure_from_fact,
    build_agg_student_module_progress_from_fact,
)


def _sample_fact() -> pl.DataFrame:
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
    agg = build_agg_activity_daily_from_fact(_sample_fact())
    assert agg.height >= 2
    assert {
        "date_utc",
        "activity_id",
        "attempts",
        "success_rate",
        "first_attempt_success_rate",
        "first_attempt_count",
    }.issubset(set(agg.columns))


def test_activity_agg_first_attempt_success_rate_is_computed_correctly() -> None:
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


def test_objective_agg_shape() -> None:
    agg = build_agg_objective_daily_from_fact(_sample_fact())
    assert agg.height >= 2
    assert {"date_utc", "objective_id", "attempts", "unique_students"}.issubset(set(agg.columns))


def test_student_module_agg_shape() -> None:
    agg = build_agg_student_module_progress_from_fact(_sample_fact())
    assert agg.height >= 2
    assert {"date_utc", "user_id", "module_id", "attempts", "last_attempt_at"}.issubset(
        set(agg.columns)
    )


def test_module_usage_daily_shape_and_keys() -> None:
    agg = build_agg_module_usage_daily_from_fact(_sample_fact())
    assert {"date_utc", "module_code", "attempts", "unique_playlists"}.issubset(set(agg.columns))
    assert agg.group_by(["date_utc", "module_code"]).len().filter(pl.col("len") > 1).height == 0


def test_student_module_exposure_shape_and_bucket_boundaries() -> None:
    high_attempts = 52
    total_rows = 10 + 40 + high_attempts
    fact = pl.DataFrame(
        {
            "created_at": [datetime(2025, 1, 1, 10, 0, 0)] * total_rows,
            "date_utc": [datetime(2025, 1, 1).date()] * total_rows,
            "user_id": ["u_low"] * 10 + ["u_mid"] * 40 + ["u_high"] * high_attempts,
            "classroom_id": ["c1"] * total_rows,
            "playlist_or_module_id": ["p1"] * total_rows,
            "activity_id": ["a1"] * total_rows,
            "activity_label": ["Activity 1"] * total_rows,
            "objective_id": ["o1"] * total_rows,
            "objective_label": ["Objective 1"] * total_rows,
            "module_id": ["m1"] * total_rows,
            "module_code": ["M1"] * total_rows,
            "module_label": ["Module 1"] * total_rows,
            "work_mode": ["playlist"] * total_rows,
            "data_correct": [True] * total_rows,
            "data_duration": [10.0] * total_rows,
            "session_duration": [None] * total_rows,
            "attempt_number": [1] * total_rows,
        }
    )
    agg = build_agg_student_module_exposure_from_fact(fact)
    bucket_map = {
        row["user_id"]: row["exposure_bucket"] for row in agg.select(["user_id", "exposure_bucket"]).to_dicts()
    }
    assert bucket_map["u_low"] == "low<=10"
    assert bucket_map["u_mid"] == "mid11-50"
    assert bucket_map["u_high"] == "high>50"
    assert agg.group_by(["user_id", "module_code"]).len().filter(pl.col("len") > 1).height == 0


def test_playlist_module_usage_shape_and_keys() -> None:
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
    agg = build_agg_module_activity_usage_from_fact(_sample_fact())
    assert {"module_code", "activity_id", "attempts", "activity_share_within_module"}.issubset(
        set(agg.columns)
    )
    assert agg.group_by(["module_code", "activity_id"]).len().filter(pl.col("len") > 1).height == 0


def test_exercise_daily_shape_and_keys() -> None:
    from visu2.config import get_settings

    agg = build_agg_exercise_daily_from_fact(_sample_fact(), settings=get_settings())
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
    }.issubset(set(agg.columns))
    assert (
        agg.group_by(["date_utc", "module_code", "objective_id", "activity_id", "exercise_id"])
        .len()
        .filter(pl.col("len") > 1)
        .height
        == 0
    )

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl

from visu2.classroom_progression import (
    build_classroom_mode_profiles,
    build_replay_payload,
    select_default_classroom,
)


def _base_fact_fixture() -> pl.DataFrame:
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []

    def row(
        minutes: int,
        classroom_id: str | None,
        work_mode: str,
        user_id: str,
        activity_id: str,
        objective_id: str,
        module_code: str,
        exercise_id: str,
        data_correct: int,
        activity_label: str | None = None,
    ) -> dict[str, object]:
        created_at = ts0 + timedelta(minutes=minutes)
        return {
            "created_at": created_at,
            "date_utc": created_at.date(),
            "user_id": user_id,
            "teacher_id": "t1",
            "classroom_id": classroom_id,
            "playlist_or_module_id": "pm1",
            "objective_id": objective_id,
            "objective_label": objective_id,
            "activity_id": activity_id,
            "activity_label": activity_label or activity_id,
            "exercise_id": exercise_id,
            "module_long_title": "Module",
            "data_correct": data_correct,
            "data_duration": 5.0,
            "session_duration": 10.0,
            "work_mode": work_mode,
            "attempt_number": 1,
            "student_attempt_index": 1,
            "first_attempt_success_rate": 0.0,
            "module_id": "mid-1",
            "module_code": module_code,
            "module_label": module_code,
        }

    rows.extend(
        [
            row(1, "c1", "zpdes", "u1", "a1", "o1", "M1", "e1", 0, "A1"),
            row(2, "c1", "zpdes", "u2", "a2", "o1", "M1", "e2", 1, "A2"),
            row(3, "c1", "zpdes", "u1", "a1", "o1", "M1", "e3", 1, "A1"),
            row(4, "c1", "zpdes", "u2", "a1", "o1", "M1", "e4", 1, "A1"),
            row(5, "None", "zpdes", "u3", "a3", "o2", "M1", "e5", 1, "A3"),
            row(6, None, "playlist", "u4", "a4", "o3", "M31", "e6", 1, "A4"),
            row(7, "c2", "playlist", "u5", "a5", "o4", "M32", "e7", 1, "A5"),
        ]
    )
    return pl.DataFrame(rows)


def test_build_classroom_mode_profiles_excludes_invalid_classroom_ids() -> None:
    fact = _base_fact_fixture()
    profiles = build_classroom_mode_profiles(fact)
    assert profiles.height > 0
    assert "None" not in set(profiles["classroom_id"].to_list())
    assert profiles.filter(pl.col("classroom_id").is_null()).height == 0

    zpdes_c1 = profiles.filter((pl.col("mode_scope") == "zpdes") & (pl.col("classroom_id") == "c1"))
    assert zpdes_c1.height == 1
    assert int(zpdes_c1["students"][0]) == 2
    assert int(zpdes_c1["activities"][0]) == 2
    assert int(zpdes_c1["attempts"][0]) == 4


def test_select_default_classroom_uses_zpdes_eligibility_then_ranking() -> None:
    profiles = pl.DataFrame(
        {
            "mode_scope": ["zpdes", "zpdes", "zpdes", "all"],
            "classroom_id": ["cA", "cB", "cC", "cA"],
            "students": [16, 20, 21, 16],
            "activities": [12, 11, 40, 12],
            "objectives": [4, 5, 5, 4],
            "modules": [1, 1, 1, 1],
            "attempts": [1000, 900, 2000, 1000],
            "first_attempt_at": [None, None, None, None],
            "last_attempt_at": [None, None, None, None],
        }
    )
    # cC is not eligible (students=21). cB should win among eligible because students=20.
    assert select_default_classroom(profiles, "zpdes") == "cB"
    assert select_default_classroom(profiles, "all") == "cA"


def test_build_replay_payload_has_empty_initial_frame_and_cumulative_updates() -> None:
    fact = _base_fact_fixture()
    payload = build_replay_payload(
        fact=fact,
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        max_frames=2000,
        step_size=1,
    )

    assert payload["total_events_valid_timestamp"] == 4
    assert payload["frame_event_counts"][0] == 0
    assert payload["frame_step_counts"] == [0, 1, 2]

    # frame 0 must be empty
    frame0 = payload["rate_frames"][0]
    assert frame0[0][0] is None
    assert frame0[0][1] is None
    assert frame0[1][0] is None
    assert frame0[1][1] is None

    # frame 1 advances each student by one local attempt (synchronized stepping)
    frame1 = payload["rate_frames"][1]
    # Student 1 first local attempt on a1 is incorrect.
    assert abs(float(frame1[0][0]) - 0.0) < 1e-9
    # Student 2 first local attempt on a2 is correct.
    assert abs(float(frame1[1][1]) - 1.0) < 1e-9
    # Student 2 has not yet reached activity a1 at step 1.
    assert frame1[0][1] is None

    # final frame cumulative checks
    last = payload["rate_frames"][-1]
    attempts_last = payload["attempt_frames"][-1]

    student_axis = payload["student_axis_labels"]
    activity_ids = payload["activity_ids"]
    assert student_axis == ["Student 1", "Student 2"]
    assert activity_ids == ["a1", "a2"]

    # a1 x Student1 -> 0 then 1 => 0.5
    assert abs(float(last[0][0]) - 0.5) < 1e-9
    assert int(attempts_last[0][0]) == 2
    # a1 x Student2 -> 1/1
    assert abs(float(last[0][1]) - 1.0) < 1e-9
    assert int(attempts_last[0][1]) == 1
    # a2 x Student2 -> 1/1
    assert abs(float(last[1][1]) - 1.0) < 1e-9


def test_build_replay_payload_applies_frame_cap_with_effective_step() -> None:
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for idx in range(10):
        created_at = ts0 + timedelta(minutes=idx)
        rows.append(
            {
                "created_at": created_at,
                "date_utc": created_at.date(),
                "user_id": "u1",
                "teacher_id": "t1",
                "classroom_id": "cZ",
                "playlist_or_module_id": "pm1",
                "objective_id": "o1",
                "objective_label": "o1",
                "activity_id": "a1",
                "activity_label": "A1",
                "exercise_id": f"e{idx}",
                "module_long_title": "Module",
                "data_correct": 1,
                "data_duration": 4.0,
                "session_duration": 8.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "student_attempt_index": idx + 1,
                "first_attempt_success_rate": 1.0,
                "module_id": "mid-1",
                "module_code": "M1",
                "module_label": "M1",
            }
        )
    fact = pl.DataFrame(rows)

    payload = build_replay_payload(
        fact=fact,
        classroom_id="cZ",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        max_frames=3,
        step_size=1,
    )
    assert payload["effective_step"] == 4
    assert payload["events_capped"] is True
    assert payload["frame_step_counts"] == [0, 4, 8, 10]
    assert payload["frame_event_counts"] == [0, 4, 8, 10]


def test_build_replay_payload_sync_step_counts_for_uneven_students() -> None:
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)
    rows = [
        {
            "created_at": ts0 + timedelta(minutes=1),
            "date_utc": (ts0 + timedelta(minutes=1)).date(),
            "user_id": "u1",
            "teacher_id": "t1",
            "classroom_id": "cX",
            "playlist_or_module_id": "pm1",
            "objective_id": "o1",
            "objective_label": "o1",
            "activity_id": "a1",
            "activity_label": "A1",
            "exercise_id": "e1",
            "module_long_title": "Module",
            "data_correct": 1,
            "data_duration": 4.0,
            "session_duration": 8.0,
            "work_mode": "zpdes",
            "attempt_number": 1,
            "student_attempt_index": 1,
            "first_attempt_success_rate": 1.0,
            "module_id": "mid-1",
            "module_code": "M1",
            "module_label": "M1",
        },
        {
            "created_at": ts0 + timedelta(minutes=2),
            "date_utc": (ts0 + timedelta(minutes=2)).date(),
            "user_id": "u2",
            "teacher_id": "t1",
            "classroom_id": "cX",
            "playlist_or_module_id": "pm1",
            "objective_id": "o1",
            "objective_label": "o1",
            "activity_id": "a1",
            "activity_label": "A1",
            "exercise_id": "e2",
            "module_long_title": "Module",
            "data_correct": 0,
            "data_duration": 4.0,
            "session_duration": 8.0,
            "work_mode": "zpdes",
            "attempt_number": 1,
            "student_attempt_index": 1,
            "first_attempt_success_rate": 0.0,
            "module_id": "mid-1",
            "module_code": "M1",
            "module_label": "M1",
        },
        {
            "created_at": ts0 + timedelta(minutes=3),
            "date_utc": (ts0 + timedelta(minutes=3)).date(),
            "user_id": "u1",
            "teacher_id": "t1",
            "classroom_id": "cX",
            "playlist_or_module_id": "pm1",
            "objective_id": "o1",
            "objective_label": "o1",
            "activity_id": "a1",
            "activity_label": "A1",
            "exercise_id": "e3",
            "module_long_title": "Module",
            "data_correct": 1,
            "data_duration": 4.0,
            "session_duration": 8.0,
            "work_mode": "zpdes",
            "attempt_number": 2,
            "student_attempt_index": 2,
            "first_attempt_success_rate": 0.0,
            "module_id": "mid-1",
            "module_code": "M1",
            "module_label": "M1",
        },
        {
            "created_at": ts0 + timedelta(minutes=4),
            "date_utc": (ts0 + timedelta(minutes=4)).date(),
            "user_id": "u1",
            "teacher_id": "t1",
            "classroom_id": "cX",
            "playlist_or_module_id": "pm1",
            "objective_id": "o1",
            "objective_label": "o1",
            "activity_id": "a1",
            "activity_label": "A1",
            "exercise_id": "e4",
            "module_long_title": "Module",
            "data_correct": 0,
            "data_duration": 4.0,
            "session_duration": 8.0,
            "work_mode": "zpdes",
            "attempt_number": 3,
            "student_attempt_index": 3,
            "first_attempt_success_rate": 0.0,
            "module_id": "mid-1",
            "module_code": "M1",
            "module_label": "M1",
        },
    ]
    fact = pl.DataFrame(rows)

    payload = build_replay_payload(
        fact=fact,
        classroom_id="cX",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        max_frames=20,
        step_size=1,
    )

    assert payload["frame_step_counts"] == [0, 1, 2, 3]
    assert payload["frame_event_counts"] == [0, 2, 3, 4]

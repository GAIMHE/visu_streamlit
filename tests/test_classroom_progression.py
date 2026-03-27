"""
test_classroom_progression.py

Validate classroom replay profile selection, payload generation, and frame semantics.

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
- _base_fact_fixture: Utility for base fact fixture.
- test_build_classroom_mode_profiles_excludes_invalid_classroom_ids: Test scenario for build classroom mode profiles excludes invalid classroom ids.
- test_select_default_classroom_uses_zpdes_eligibility_then_ranking: Test scenario for select default classroom uses zpdes eligibility then ranking.
- test_build_replay_payload_has_empty_initial_frame_and_cumulative_updates: Test scenario for build replay payload has empty initial frame and cumulative updates.
- test_build_replay_payload_applies_frame_cap_with_effective_step: Test scenario for build replay payload applies frame cap with effective step.
- test_build_replay_payload_sync_step_counts_for_uneven_students: Test scenario for build replay payload sync step counts for uneven students.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl

from visu2.classroom_progression import (
    SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID,
    build_classroom_activity_summary_by_mode,
    build_classroom_mode_profiles,
    build_heatmap_figure,
    build_replay_payload,
    select_classroom_by_id,
    select_classrooms_near_student_target,
    select_default_classroom,
)


def _base_fact_fixture() -> pl.DataFrame:
    """Base fact fixture.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
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
        """Row.

Parameters
----------
minutes : int
            Input parameter used by this routine.
classroom_id : str | None
            Input parameter used by this routine.
work_mode : str
            Input parameter used by this routine.
user_id : str
            Input parameter used by this routine.
activity_id : str
            Input parameter used by this routine.
objective_id : str
            Input parameter used by this routine.
module_code : str
            Input parameter used by this routine.
exercise_id : str
            Input parameter used by this routine.
data_correct : int
            Input parameter used by this routine.
activity_label : str | None
            Input parameter used by this routine.

Returns
-------
dict[str, object]
            Result produced by this routine.

Notes
-----
        Behavior is intentionally documented for maintainability and traceability.
"""
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
    """Test build classroom mode profiles excludes invalid classroom ids.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
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


def test_build_classroom_mode_profiles_uses_synthetic_classroom_when_missing() -> None:
    fact = _base_fact_fixture().with_columns(pl.lit(None, dtype=pl.Utf8).alias("classroom_id"))

    profiles = build_classroom_mode_profiles(fact)

    assert profiles.height > 0
    assert set(profiles["classroom_id"].to_list()) == {SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID}
    zpdes_profile = profiles.filter(pl.col("mode_scope") == "zpdes")
    assert zpdes_profile.height == 1
    assert int(zpdes_profile["students"][0]) == 3


def test_build_classroom_activity_summary_by_mode_matches_expected_rates() -> None:
    summary = build_classroom_activity_summary_by_mode(_base_fact_fixture())

    zpdes_a1 = summary.filter(
        (pl.col("mode_scope") == "zpdes") & (pl.col("activity_label") == "A1")
    ).row(0, named=True)
    assert zpdes_a1["classrooms_observed"] == 1
    assert zpdes_a1["attempts_total"] == 3
    assert zpdes_a1["successes_total"] == 2
    assert zpdes_a1["success_rate"] == 2 / 3
    assert zpdes_a1["weak_classroom_share"] == 0.0

    playlist_a5 = summary.filter(
        (pl.col("mode_scope") == "playlist") & (pl.col("activity_label") == "A5")
    ).row(0, named=True)
    assert playlist_a5["classrooms_observed"] == 1
    assert playlist_a5["success_rate"] == 1.0


def test_build_classroom_activity_summary_by_mode_uses_synthetic_classroom_when_missing() -> None:
    fact = _base_fact_fixture().with_columns(pl.lit(None, dtype=pl.Utf8).alias("classroom_id"))

    summary = build_classroom_activity_summary_by_mode(fact)

    zpdes = summary.filter(pl.col("mode_scope") == "zpdes")
    assert zpdes.height > 0
    assert zpdes["classrooms_observed"].to_list() == [1, 1, 1]


def test_select_default_classroom_uses_zpdes_eligibility_then_ranking() -> None:
    """Test select default classroom uses zpdes eligibility then ranking.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
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


def test_select_classrooms_near_student_target_filters_and_sorts() -> None:
    """Test classroom target selection filters by band and sorts by locked rules."""
    profiles = pl.DataFrame(
        {
            "mode_scope": ["zpdes", "zpdes", "zpdes", "playlist"],
            "classroom_id": ["cA", "cB", "cC", "cD"],
            "students": [18, 21, 19, 20],
            "activities": [10, 9, 14, 20],
            "objectives": [4, 4, 4, 4],
            "modules": [1, 1, 1, 1],
            "attempts": [120, 300, 200, 100],
            "first_attempt_at": [None, None, None, None],
            "last_attempt_at": [None, None, None, None],
        }
    )
    selected = select_classrooms_near_student_target(profiles, "zpdes", target_students=20)
    assert selected["classroom_id"].to_list() == ["cC", "cA", "cB"]


def test_select_classrooms_near_student_target_returns_empty_when_no_match() -> None:
    """Test classroom target selection returns an empty frame when no classroom matches."""
    profiles = pl.DataFrame(
        {
            "mode_scope": ["zpdes"],
            "classroom_id": ["cA"],
            "students": [40],
            "activities": [10],
            "objectives": [4],
            "modules": [1],
            "attempts": [120],
            "first_attempt_at": [None],
            "last_attempt_at": [None],
        }
    )
    selected = select_classrooms_near_student_target(profiles, "zpdes", target_students=20)
    assert selected.height == 0


def test_select_classroom_by_id_returns_exact_match_in_scope() -> None:
    """Test classroom ID override resolves one exact classroom in the selected scope."""
    profiles = pl.DataFrame(
        {
            "mode_scope": ["zpdes", "playlist", "zpdes"],
            "classroom_id": ["cA", "cA", "cB"],
            "students": [18, 18, 21],
            "activities": [10, 10, 14],
            "objectives": [4, 4, 4],
            "modules": [1, 1, 1],
            "attempts": [120, 100, 200],
            "first_attempt_at": [None, None, None],
            "last_attempt_at": [None, None, None],
        }
    )

    assert select_classroom_by_id(profiles, "zpdes", "cA") == "cA"
    assert select_classroom_by_id(profiles, "playlist", "cA") == "cA"


def test_select_classroom_by_id_returns_none_for_unknown_or_wrong_scope() -> None:
    """Test classroom ID override rejects unknown IDs and classrooms outside the current scope."""
    profiles = pl.DataFrame(
        {
            "mode_scope": ["zpdes", "playlist"],
            "classroom_id": ["cA", "cB"],
            "students": [18, 20],
            "activities": [10, 12],
            "objectives": [4, 4],
            "modules": [1, 1],
            "attempts": [120, 100],
            "first_attempt_at": [None, None],
            "last_attempt_at": [None, None],
        }
    )

    assert select_classroom_by_id(profiles, "zpdes", "missing") is None
    assert select_classroom_by_id(profiles, "zpdes", "cB") is None


def test_build_heatmap_figure_overlays_values_only_on_populated_cells() -> None:
    """Test classroom heatmap values are rendered only for populated cells."""
    payload = build_replay_payload(
        fact=_base_fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        max_frames=2000,
        step_size=1,
    )

    figure = build_heatmap_figure(payload=payload, frame_idx=1, threshold=0.75, show_values=True)

    assert len(figure.data) == 2
    heatmap = figure.data[0]
    text_overlay = figure.data[1]
    assert heatmap.type == "heatmap"
    assert text_overlay.type == "scatter"
    assert list(text_overlay.text) == ["0%", "100%"]
    assert list(text_overlay.x) == ["Student 1", "Student 2"]
    assert list(text_overlay.y) == ["A1", "A2"]
    hover_customdata = heatmap.customdata
    assert hover_customdata[0][0][3] == 1
    assert hover_customdata[0][0][6] == 0
    assert hover_customdata[0][0][7] == 1
    assert hover_customdata[1][1][3] == 1
    assert hover_customdata[1][1][6] == 1
    assert hover_customdata[1][1][7] == 1


def test_build_replay_payload_makes_duplicate_activity_axis_labels_unique() -> None:
    """Test replay payload uniquifies duplicate activity labels for Plotly axes."""
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)
    fact = pl.DataFrame(
        [
            {
                "created_at": ts0,
                "date_utc": ts0.date(),
                "user_id": "u1",
                "teacher_id": "t1",
                "classroom_id": "c1",
                "playlist_or_module_id": "pm1",
                "objective_id": "o1",
                "objective_label": "o1",
                "activity_id": "a1",
                "activity_label": "Repeated label",
                "exercise_id": "e1",
                "module_long_title": "Module",
                "data_correct": 1,
                "data_duration": 5.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "student_attempt_index": 1,
                "first_attempt_success_rate": 1.0,
                "module_id": "mid-1",
                "module_code": "M1",
                "module_label": "M1",
            },
            {
                "created_at": ts0 + timedelta(minutes=1),
                "date_utc": ts0.date(),
                "user_id": "u1",
                "teacher_id": "t1",
                "classroom_id": "c1",
                "playlist_or_module_id": "pm1",
                "objective_id": "o1",
                "objective_label": "o1",
                "activity_id": "a2",
                "activity_label": "Repeated label",
                "exercise_id": "e2",
                "module_long_title": "Module",
                "data_correct": 0,
                "data_duration": 5.0,
                "session_duration": 10.0,
                "work_mode": "zpdes",
                "attempt_number": 1,
                "student_attempt_index": 2,
                "first_attempt_success_rate": 0.0,
                "module_id": "mid-1",
                "module_code": "M1",
                "module_label": "M1",
            },
        ]
    )

    payload = build_replay_payload(
        fact=fact,
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        max_frames=2000,
        step_size=1,
    )

    assert payload["activity_axis_labels"] == ["Repeated label", "Repeated label [2]"]

    figure = build_heatmap_figure(payload=payload, frame_idx=2, threshold=0.75, show_values=True)
    assert list(figure.data[1].y) == ["Repeated label", "Repeated label [2]"]


def test_build_replay_payload_keeps_missing_activity_metadata_visible() -> None:
    """Test replay payload keeps attempts with missing activity metadata in a placeholder row."""
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)
    fact = pl.DataFrame(
        [
            {
                "created_at": ts0,
                "date_utc": ts0.date(),
                "user_id": "u1",
                "teacher_id": "t1",
                "classroom_id": "c1",
                "playlist_or_module_id": "pm1",
                "objective_id": "o1",
                "objective_label": "o1",
                "activity_id": None,
                "activity_label": None,
                "exercise_id": "e1",
                "module_long_title": "Module",
                "data_correct": 1,
                "data_duration": 5.0,
                "session_duration": 10.0,
                "work_mode": "playlist",
                "attempt_number": 1,
                "student_attempt_index": 1,
                "first_attempt_success_rate": 1.0,
                "module_id": "mid-1",
                "module_code": "M1",
                "module_label": "M1",
            },
            {
                "created_at": ts0 + timedelta(minutes=1),
                "date_utc": ts0.date(),
                "user_id": "u2",
                "teacher_id": "t1",
                "classroom_id": "c1",
                "playlist_or_module_id": "pm1",
                "objective_id": "o1",
                "objective_label": "o1",
                "activity_id": "a1",
                "activity_label": "A1",
                "exercise_id": "e2",
                "module_long_title": "Module",
                "data_correct": 0,
                "data_duration": 5.0,
                "session_duration": 10.0,
                "work_mode": "playlist",
                "attempt_number": 1,
                "student_attempt_index": 1,
                "first_attempt_success_rate": 0.0,
                "module_id": "mid-1",
                "module_code": "M1",
                "module_label": "M1",
            },
        ]
    )

    payload = build_replay_payload(
        fact=fact,
        classroom_id="c1",
        mode_scope="playlist",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        max_frames=2000,
        step_size=1,
    )

    assert "(missing activity metadata)" in payload["activity_axis_labels"]
    final_attempts = payload["attempt_frames"][-1]
    nonzero_cells = sum(1 for row in final_attempts for value in row if value > 0)
    assert nonzero_cells == 2


def test_build_replay_payload_has_empty_initial_frame_and_cumulative_updates() -> None:
    """Test build replay payload has empty initial frame and cumulative updates.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
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
    successes_last = payload["success_frames"][-1]
    unique_exercises_last = payload["unique_exercise_frames"][-1]

    student_axis = payload["student_axis_labels"]
    activity_ids = payload["activity_ids"]
    assert student_axis == ["Student 1", "Student 2"]
    assert activity_ids == ["a1", "a2"]

    # a1 x Student1 -> 0 then 1 => 0.5
    assert abs(float(last[0][0]) - 0.5) < 1e-9
    assert int(attempts_last[0][0]) == 2
    assert int(successes_last[0][0]) == 1
    assert int(unique_exercises_last[0][0]) == 2
    # a1 x Student2 -> 1/1
    assert abs(float(last[0][1]) - 1.0) < 1e-9
    assert int(attempts_last[0][1]) == 1
    assert int(successes_last[0][1]) == 1
    assert int(unique_exercises_last[0][1]) == 1
    # a2 x Student2 -> 1/1
    assert abs(float(last[1][1]) - 1.0) < 1e-9
    assert int(successes_last[1][1]) == 1
    assert int(unique_exercises_last[1][1]) == 1


def test_build_replay_payload_applies_frame_cap_with_effective_step() -> None:
    """Test build replay payload applies frame cap with effective step.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
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
    """Test build replay payload sync step counts for uneven students.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
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
    assert payload["student_total_attempts"] == [3, 1]

    figure = build_heatmap_figure(payload=payload, frame_idx=2, threshold=0.75, show_values=False)
    ticktext = list(figure.layout.xaxis.ticktext)
    assert ticktext == ["<b>Student 1</b>", "Student 2"]

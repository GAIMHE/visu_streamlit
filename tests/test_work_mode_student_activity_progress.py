from __future__ import annotations

import pandas as pd

from scripts.work_mode_student_activity_progress import (
    build_cumulative_module_progress,
    build_student_activity_progress,
)


def _attempt(
    student: str,
    mode: str,
    module: str,
    activity: str,
    exercise: str,
    minute: int,
    success: int,
    playlist_id: str | None = None,
) -> dict[str, object]:
    return {
        "student_id": student,
        "work_mode": mode,
        "module": module,
        "activity_id": activity,
        "exercise_id": exercise,
        "created_at": pd.Timestamp("2026-01-01", tz="UTC")
        + pd.Timedelta(minutes=minute),
        "success": success,
        "playlist_id": (
            playlist_id
            if playlist_id is not None
            else f"playlist_{student}"
            if mode == "playlist"
            else None
        ),
    }


def test_interleaved_activities_are_eligible_within_student_module_mode() -> None:
    rows = [
        _attempt("student", "zpdes", "module", "a1", "a1e1", 1, 0),
        _attempt("student", "zpdes", "module", "a1", "a1e2", 2, 0),
        _attempt("student", "zpdes", "module", "a2", "a2e1", 3, 0),
        _attempt("student", "zpdes", "module", "a2", "a2e2", 4, 0),
        _attempt("student", "zpdes", "module", "a1", "a1e3", 5, 1),
        _attempt("student", "zpdes", "module", "a1", "a1e4", 6, 1),
        _attempt("student", "zpdes", "module", "a2", "a2e3", 7, 1),
        _attempt("student", "zpdes", "module", "a2", "a2e4", 8, 1),
    ]

    result = build_student_activity_progress(
        pd.DataFrame(rows),
        min_activity_exercises=4,
        min_student_exercises=8,
    )

    assert len(result.activity_progress) == 2
    assert set(result.activity_progress["activity_id"]) == {"a1", "a2"}
    assert set(result.activity_progress["mean_progress"]) == {100.0}
    assert result.student_summary.iloc[0]["activities"] == 2
    assert result.student_summary.iloc[0]["unique_exercises"] == 8


def test_thirty_exercise_threshold_is_applied_separately_by_work_mode() -> None:
    rows = []
    minute = 0
    for activity_index in range(8):
        for exercise_index, success in enumerate((0, 0, 1, 1)):
            minute += 1
            rows.append(
                _attempt(
                    "mixed_student",
                    "zpdes",
                    "module",
                    f"z{activity_index}",
                    f"ze{activity_index}_{exercise_index}",
                    minute,
                    success,
                )
            )
    for exercise_index, success in enumerate((0, 0, 1, 1)):
        minute += 1
        rows.append(
            _attempt(
                "mixed_student",
                "playlist",
                "module",
                "p1",
                f"pe{exercise_index}",
                minute,
                success,
            )
        )

    result = build_student_activity_progress(pd.DataFrame(rows))

    assert result.student_summary[["student_id", "work_mode"]].to_records(
        index=False
    ).tolist() == [("mixed_student", "zpdes")]


def test_student_progress_gives_each_activity_equal_weight() -> None:
    rows = []
    minute = 0
    outcomes = {
        "short": (0, 0, 1, 1),
        "long": (1, 1, 1, 0, 0, 0),
    }
    for activity, successes in outcomes.items():
        for exercise_index, success in enumerate(successes):
            minute += 1
            rows.append(
                _attempt(
                    "student",
                    "playlist",
                    "module",
                    activity,
                    f"{activity}_{exercise_index}",
                    minute,
                    success,
                )
            )

    result = build_student_activity_progress(
        pd.DataFrame(rows),
        min_activity_exercises=4,
        min_student_exercises=10,
    )

    progress = result.activity_progress.set_index("activity_id")["mean_progress"]
    assert progress.to_dict() == {"long": -100.0, "short": 100.0}
    assert result.student_summary.iloc[0]["mean_progress"] == 0.0


def test_thirty_exercises_are_checked_before_activity_eligibility() -> None:
    rows = []
    for exercise_index in range(30):
        activity = "eligible" if exercise_index < 4 else f"short_{exercise_index}"
        rows.append(
            _attempt(
                "student",
                "playlist",
                "module",
                activity,
                f"exercise_{exercise_index}",
                exercise_index,
                int(exercise_index >= 2),
            )
        )

    result = build_student_activity_progress(pd.DataFrame(rows))

    assert result.student_summary.iloc[0]["unique_exercises"] == 30
    assert result.student_summary.iloc[0]["activities"] == 1
    assert result.activity_progress.iloc[0]["activity_id"] == "eligible"


def test_same_activity_in_two_playlists_counts_as_two_sequence_activities() -> None:
    rows = []
    minute = 0
    for playlist_id in ("playlist_1", "playlist_2"):
        for exercise_index, success in enumerate((0, 0, 1, 1)):
            minute += 1
            rows.append(
                _attempt(
                    "student",
                    "playlist",
                    "module",
                    "activity",
                    f"{playlist_id}_{exercise_index}",
                    minute,
                    success,
                    playlist_id=playlist_id,
                )
            )

    result = build_student_activity_progress(
        pd.DataFrame(rows),
        min_student_exercises=8,
    )

    assert result.student_summary.iloc[0]["activities"] == 2
    assert result.activity_progress["sequence_id"].nunique() == 2


def test_cumulative_progress_averages_activities_then_sums_modules() -> None:
    activity_progress = pd.DataFrame(
        {
            "student_id": ["student"] * 4,
            "work_mode": ["zpdes"] * 4,
            "module": ["module_1", "module_1", "module_1", "module_2"],
            "activity_id": ["a1", "a2", "a3", "a4"],
            "mean_progress": [100.0, 100.0, -100.0, -20.0],
        }
    )

    result = build_cumulative_module_progress(activity_progress)

    module_values = result.module_progress.set_index("module")[
        "module_progress"
    ].to_dict()
    assert module_values == {
        "module_1": 100.0 / 3.0,
        "module_2": -20.0,
    }
    assert result.student_progress.iloc[0]["cumulative_progress"] == 100.0 / 3.0 - 20.0
    assert result.student_progress.iloc[0]["modules"] == 2

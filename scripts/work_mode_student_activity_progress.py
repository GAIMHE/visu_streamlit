"""Build student-level activity-count and mean-progress summaries.

The analysis retains the first attempt on each exercise globally. The
30-exercise student threshold is applied separately by work mode before
activity eligibility. Playlist sequences are distinct playlist identifiers;
ZPDES sequences are modules. Within a sequence, activities do not need to be
chronologically uninterrupted: attempts from another activity may occur in
between. An activity is eligible after at least four unique exercises.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from scripts.model_work_mode_progress import WORK_MODES

ACTIVITY_KEYS = [
    "student_id",
    "work_mode",
    "sequence_id",
    "module",
    "activity_id",
]
STUDENT_MODE_KEYS = ["student_id", "work_mode"]
REQUIRED_COLUMNS = {
    "student_id",
    "module",
    "work_mode",
    "activity_id",
    "exercise_id",
    "created_at",
    "success",
}


@dataclass(frozen=True)
class StudentActivityProgressData:
    activity_progress: pd.DataFrame
    student_summary: pd.DataFrame
    audit: pd.DataFrame


@dataclass(frozen=True)
class CumulativeModuleProgressData:
    module_progress: pd.DataFrame
    student_progress: pd.DataFrame


def build_student_activity_progress(
    attempts: pd.DataFrame,
    min_activity_exercises: int = 4,
    min_student_exercises: int = 30,
) -> StudentActivityProgressData:
    """Aggregate eligible activity progress to an equally weighted student mean."""

    if min_activity_exercises < 2:
        raise ValueError("min_activity_exercises must be at least 2")
    if min_student_exercises < 1:
        raise ValueError("min_student_exercises must be at least 1")
    missing = sorted(REQUIRED_COLUMNS.difference(attempts.columns))
    if missing:
        raise ValueError(f"Missing required attempt columns: {', '.join(missing)}")

    frame_columns = list(REQUIRED_COLUMNS)
    if "playlist_id" in attempts.columns:
        frame_columns.append("playlist_id")
    frame = attempts[frame_columns].copy()
    if "playlist_id" not in frame.columns:
        frame["playlist_id"] = pd.NA
    frame = frame[frame["work_mode"].isin(WORK_MODES)].copy()
    frame["created_at"] = pd.to_datetime(
        frame["created_at"],
        format="mixed",
        errors="coerce",
        utc=True,
    )
    frame["success"] = pd.to_numeric(frame["success"], errors="coerce")
    frame = frame.dropna(subset=list(REQUIRED_COLUMNS))
    frame["_row_order"] = np.arange(len(frame), dtype=np.int64)
    frame = frame.sort_values(
        ["student_id", "created_at", "_row_order"],
        kind="mergesort",
    )

    first_attempts = frame.drop_duplicates(
        ["student_id", "exercise_id"],
        keep="first",
    ).copy()
    all_first_attempts = first_attempts
    student_exercises = (
        first_attempts.groupby(STUDENT_MODE_KEYS, observed=True)["exercise_id"]
        .nunique()
        .rename("unique_exercises")
        .reset_index()
    )
    eligible_student_modes = student_exercises[
        student_exercises["unique_exercises"] >= min_student_exercises
    ].copy()
    first_attempts = first_attempts.merge(
        eligible_student_modes[STUDENT_MODE_KEYS],
        on=STUDENT_MODE_KEYS,
        how="inner",
        validate="many_to_one",
    )
    first_attempts["playlist_id"] = (
        first_attempts["playlist_id"].astype("string").str.strip()
    )
    first_attempts.loc[
        first_attempts["playlist_id"].fillna("").eq(""),
        "playlist_id",
    ] = pd.NA
    playlist_rows = first_attempts["work_mode"].eq("playlist")
    missing_playlist_id = playlist_rows & first_attempts["playlist_id"].isna()
    if missing_playlist_id.any():
        raise ValueError(
            "Playlist sequence construction requires playlist_id for every "
            f"retained playlist row; missing on {int(missing_playlist_id.sum())} rows"
        )
    first_attempts["sequence_id"] = "module::" + first_attempts["module"].astype(str)
    first_attempts.loc[playlist_rows, "sequence_id"] = (
        "playlist::"
        + first_attempts.loc[playlist_rows, "playlist_id"].astype(str)
    )
    activity_sizes = (
        first_attempts.groupby(ACTIVITY_KEYS, observed=True)["exercise_id"]
        .nunique()
        .rename("activity_exercises")
        .reset_index()
    )
    eligible_keys = activity_sizes[
        activity_sizes["activity_exercises"] >= min_activity_exercises
    ]
    eligible_attempts = first_attempts.merge(
        eligible_keys,
        on=ACTIVITY_KEYS,
        how="inner",
        validate="many_to_one",
    )
    eligible_attempts = eligible_attempts.sort_values(
        [*ACTIVITY_KEYS, "created_at", "_row_order"],
        kind="mergesort",
    )

    activity_groups = eligible_attempts.groupby(ACTIVITY_KEYS, observed=True)
    eligible_attempts["attempt_position"] = activity_groups.cumcount() + 1
    n_exercises = eligible_attempts["activity_exercises"]
    position = eligible_attempts["attempt_position"]
    first_half = position <= (n_exercises // 2)
    later_half = position > (n_exercises - n_exercises // 2)
    eligible_attempts["success_first"] = eligible_attempts["success"].where(first_half)
    eligible_attempts["success_later"] = eligible_attempts["success"].where(later_half)

    activity_progress = (
        eligible_attempts.groupby(ACTIVITY_KEYS, observed=True, as_index=False)
        .agg(
            success_rate_first=("success_first", "mean"),
            success_rate_later=("success_later", "mean"),
            success_rate_all=("success", "mean"),
            n_first_attempts=("success", "size"),
            unique_exercises=("exercise_id", "nunique"),
        )
        .dropna(subset=["success_rate_first", "success_rate_later"])
    )
    activity_progress["mean_progress"] = (
        activity_progress["success_rate_later"]
        - activity_progress["success_rate_first"]
    ) * 100.0

    student_summary_all = (
        activity_progress.groupby(STUDENT_MODE_KEYS, observed=True, as_index=False)
        .agg(
            activities=("activity_id", "size"),
            modules=("module", "nunique"),
            mean_progress=("mean_progress", "mean"),
            median_activity_progress=("mean_progress", "median"),
        )
        .merge(
            student_exercises,
            on=STUDENT_MODE_KEYS,
            how="left",
            validate="one_to_one",
        )
    )
    student_summary = student_summary_all[
        student_summary_all["unique_exercises"] >= min_student_exercises
    ].copy()
    student_summary["negative_progress"] = student_summary["mean_progress"] < 0
    student_summary = student_summary.sort_values(STUDENT_MODE_KEYS).reset_index(drop=True)

    retained_keys = student_summary[STUDENT_MODE_KEYS]
    retained_activities = activity_progress.merge(
        retained_keys,
        on=STUDENT_MODE_KEYS,
        how="inner",
        validate="many_to_one",
    )
    audit_rows = []
    for work_mode in WORK_MODES:
        first_mode = all_first_attempts[
            all_first_attempts["work_mode"].eq(work_mode)
        ]
        eligible_mode = eligible_attempts[eligible_attempts["work_mode"].eq(work_mode)]
        activities_mode = activity_progress[activity_progress["work_mode"].eq(work_mode)]
        students_with_activities = student_summary_all[
            student_summary_all["work_mode"].eq(work_mode)
        ]
        students_after_30 = eligible_student_modes[
            eligible_student_modes["work_mode"].eq(work_mode)
        ]
        students_plotted = student_summary[
            student_summary["work_mode"].eq(work_mode)
        ]
        retained_mode = retained_activities[
            retained_activities["work_mode"].eq(work_mode)
        ]
        audit_rows.append(
            {
                "work_mode": work_mode,
                "first_attempt_rows": len(first_mode),
                "eligible_attempt_rows": len(eligible_mode),
                "eligible_activities": len(activities_mode),
                "students_after_30_exercises": len(students_after_30),
                "students_with_eligible_activities": len(students_with_activities),
                "students_plotted": len(students_plotted),
                "retained_student_activities": len(retained_mode),
                "negative_progress_students": int(
                    students_plotted["negative_progress"].sum()
                ),
                "negative_progress_share": (
                    float(students_plotted["negative_progress"].mean())
                    if not students_plotted.empty
                    else np.nan
                ),
            }
        )

    return StudentActivityProgressData(
        activity_progress=activity_progress,
        student_summary=student_summary,
        audit=pd.DataFrame(audit_rows),
    )


def build_cumulative_module_progress(
    activity_progress: pd.DataFrame,
) -> CumulativeModuleProgressData:
    """Average activities within modules, then sum module means by student and mode."""

    required = {
        "student_id",
        "work_mode",
        "module",
        "activity_id",
        "mean_progress",
    }
    missing = sorted(required.difference(activity_progress.columns))
    if missing:
        raise ValueError(
            f"Missing required activity-progress columns: {', '.join(missing)}"
        )

    module_progress = (
        activity_progress.groupby(
            ["student_id", "work_mode", "module"],
            observed=True,
            as_index=False,
        )
        .agg(
            module_progress=("mean_progress", "mean"),
            eligible_activities=("activity_id", "size"),
        )
        .sort_values(["student_id", "work_mode", "module"])
        .reset_index(drop=True)
    )
    student_progress = (
        module_progress.groupby(STUDENT_MODE_KEYS, observed=True, as_index=False)
        .agg(
            cumulative_progress=("module_progress", "sum"),
            modules=("module", "nunique"),
            eligible_activities=("eligible_activities", "sum"),
        )
        .sort_values(STUDENT_MODE_KEYS)
        .reset_index(drop=True)
    )
    student_progress["negative_cumulative_progress"] = (
        student_progress["cumulative_progress"] < 0
    )
    return CumulativeModuleProgressData(
        module_progress=module_progress,
        student_progress=student_progress,
    )

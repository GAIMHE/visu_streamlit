from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from visu2.cohort_filter_viewer import (
    HISTORY_BASIS_DISTINCT_EXERCISES,
    HISTORY_BASIS_RAW_ATTEMPTS,
    RETRY_FILTER_MODE_REMOVE_EXERCISE,
    RETRY_FILTER_MODE_REMOVE_STUDENT,
    SCHEMA_FILTER_MODE_REMOVE,
    build_final_module_summary,
    build_final_schema_summary,
    build_schema_summary_vs_baseline,
    filter_cohort_view,
)


def _sample_attempts() -> pl.DataFrame:
    rows: list[dict[str, object]] = []

    def add_segment(
        user_id: str,
        start_idx: int,
        *,
        work_mode: str,
        attempts: int,
        module_code: str,
        exercise_ids: list[str],
        activity_id: str | None = None,
    ) -> int:
        idx = start_idx
        resolved_activity_id = activity_id or f"{user_id}_{work_mode}_{module_code}_{exercise_ids[0]}_activity"
        for offset in range(attempts):
            rows.append(
                {
                    "user_id": user_id,
                    "created_at": datetime(2025, 1, 1, 8, 0, 0) + timedelta(minutes=idx),
                    "work_mode": work_mode,
                    "module_code": module_code,
                    "activity_id": resolved_activity_id,
                    "exercise_id": exercise_ids[offset % len(exercise_ids)],
                    "attempt_number": offset + 1,
                }
            )
            idx += 1
        return idx

    idx = 0
    idx = add_segment("u_mod", idx, work_mode="initial-test", attempts=2, module_code="M1", exercise_ids=["m1_a"])
    idx = add_segment("u_mod", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["m1_z"])
    idx = add_segment("u_mod", idx, work_mode="adaptive-test", attempts=3, module_code="M31", exercise_ids=["m31_a"])
    idx = add_segment("u_mod", idx, work_mode="zpdes", attempts=3, module_code="M31", exercise_ids=["m31_z"])

    idx = add_segment("u_cleanup", idx, work_mode="adaptive-test", attempts=5, module_code="M1", exercise_ids=["c_a1", "c_a2"])
    idx = add_segment("u_cleanup", idx, work_mode="zpdes", attempts=4, module_code="M1", exercise_ids=["c_z1", "c_z2"])
    idx = add_segment("u_cleanup", idx, work_mode="adaptive-test", attempts=4, module_code="M1", exercise_ids=["c_bad"])
    idx = add_segment("u_cleanup", idx, work_mode="zpdes", attempts=3, module_code="M1", exercise_ids=["c_drop"])
    idx = add_segment("u_cleanup", idx, work_mode="adaptive-test", attempts=5, module_code="M1", exercise_ids=["c_a3", "c_a4"])
    idx = add_segment("u_cleanup", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["c_z3"])

    idx = add_segment("u_raw_only", idx, work_mode="zpdes", attempts=3, module_code="M1", exercise_ids=["same_ex"])
    idx = add_segment("u_distinct", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["d1", "d2"])
    idx = add_segment("u_schema", idx, work_mode="initial-test", attempts=5, module_code="M1", exercise_ids=["s_a"])
    idx = add_segment("u_schema", idx, work_mode="zpdes", attempts=5, module_code="M1", exercise_ids=["s_z"])
    idx = add_segment("u_retry", idx, work_mode="zpdes", attempts=4, module_code="M1", exercise_ids=["r_bad"])
    idx = add_segment("u_retry", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["r_keep1", "r_keep2"])
    idx = add_segment("u_same_module_bad", idx, work_mode="initial-test", attempts=5, module_code="M1", exercise_ids=["smb_i1"])
    idx = add_segment("u_same_module_bad", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["smb_z1"])
    idx = add_segment("u_same_module_bad", idx, work_mode="initial-test", attempts=5, module_code="M1", exercise_ids=["smb_i2"])
    idx = add_segment("u_same_module_bad", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["smb_z2"])
    idx = add_segment("u_same_module_ok", idx, work_mode="initial-test", attempts=5, module_code="M1", exercise_ids=["smo_i1"])
    idx = add_segment("u_same_module_ok", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["smo_z1"])
    idx = add_segment("u_same_module_ok", idx, work_mode="adaptive-test", attempts=5, module_code="M1", exercise_ids=["smo_a1"])
    idx = add_segment("u_same_module_ok", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["smo_z2"])
    idx = add_segment("u_empty", idx, work_mode="adaptive-test", attempts=2, module_code="M1", exercise_ids=["e_a"])
    add_segment("u_empty", idx, work_mode="zpdes", attempts=2, module_code="M1", exercise_ids=["e_z"])

    return pl.DataFrame(rows)


def test_module_filter_builds_paths_from_selected_modules_only() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M31",),
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )

    assert result.final_user_paths.height == 1
    row = result.final_user_paths.row(0, named=True)
    assert row["user_id"] == "u_mod"
    assert row["cleaned_schema"] == "adaptive-test -> zpdes"
    assert row["transition_count"] == 1


def test_placement_cleanup_removes_short_segment_and_following_segment() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=5,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )

    row = result.final_user_paths.filter(pl.col("user_id") == "u_cleanup").row(0, named=True)
    assert row["cleaned_schema"] == "adaptive-test -> zpdes -> adaptive-test -> zpdes"
    assert row["transition_count"] == 3
    assert row["retained_attempts"] == 16


def test_work_mode_removal_drops_attempts_without_dropping_student() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M31",),
        selected_removed_work_modes=("adaptive-test",),
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )

    row = result.final_user_paths.row(0, named=True)
    assert row["user_id"] == "u_mod"
    assert row["cleaned_schema"] == "zpdes"
    assert row["transition_count"] == 0
    assert row["retained_attempts"] == 3
    assert set(result.final_rows.get_column("work_mode").to_list()) == {"zpdes"}


def test_history_threshold_supports_raw_attempts_and_distinct_exercises() -> None:
    raw_result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=1,
        min_history=3,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )
    assert "u_raw_only" in set(raw_result.final_user_paths["user_id"].to_list())

    distinct_result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=1,
        min_history=2,
        history_basis=HISTORY_BASIS_DISTINCT_EXERCISES,
    )
    assert "u_raw_only" not in set(distinct_result.final_user_paths["user_id"].to_list())
    assert "u_distinct" in set(distinct_result.final_user_paths["user_id"].to_list())


def test_transition_count_and_schema_filters_apply_exact_matches() -> None:
    base_result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=5,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        selected_transition_counts=(3,),
    )
    assert set(base_result.final_user_paths["user_id"].to_list()) == {
        "u_cleanup",
        "u_same_module_bad",
        "u_same_module_ok",
    }

    schema_result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=5,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        selected_schemas=("initial-test -> zpdes",),
    )
    assert set(schema_result.final_user_paths["user_id"].to_list()) == {"u_schema"}

    schema_remove_result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=5,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        selected_schemas=("initial-test -> zpdes",),
        schema_filter_mode=SCHEMA_FILTER_MODE_REMOVE,
    )
    assert "u_schema" not in set(schema_remove_result.final_user_paths["user_id"].to_list())


def test_max_retry_filter_can_remove_only_offending_exercise_rows() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        max_retries=2,
        retry_filter_mode=RETRY_FILTER_MODE_REMOVE_EXERCISE,
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )

    retry_user = result.final_user_paths.filter(pl.col("user_id") == "u_retry").row(0, named=True)
    assert retry_user["retained_attempts"] == 2
    remaining_exercises = (
        result.final_rows.filter(pl.col("user_id") == "u_retry").get_column("exercise_id").to_list()
    )
    assert set(remaining_exercises) == {"r_keep1", "r_keep2"}


def test_max_retry_filter_can_ignore_small_activities_when_exemption_is_enabled() -> None:
    attempts = pl.DataFrame(
        {
            "user_id": ["u_exempt"] * 4 + ["u_blocked"] * 4,
            "created_at": [datetime(2025, 1, 1, 8, 0, 0) + timedelta(minutes=index) for index in range(8)],
            "work_mode": ["zpdes"] * 8,
            "module_code": ["M1"] * 8,
            "activity_id": ["a_single"] * 4 + ["a_multi"] * 4,
            "exercise_id": ["ex_single"] * 4 + ["ex_multi"] * 4,
            "attempt_number": [1, 2, 3, 4, 1, 2, 3, 4],
        }
    )
    activity_exercise_counts = pl.DataFrame(
        {
            "activity_id": ["a_single", "a_multi"],
            "activity_exercise_count": [1, 2],
        }
    )

    result = filter_cohort_view(
        attempts,
        selected_modules=("M1",),
        max_retries=1,
        retry_filter_mode=RETRY_FILTER_MODE_REMOVE_EXERCISE,
        retry_small_activity_exemption_enabled=True,
        retry_small_activity_max_exercises=1,
        activity_exercise_counts=activity_exercise_counts,
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )

    assert set(result.final_user_paths.get_column("user_id").to_list()) == {"u_exempt"}
    assert result.final_rows.filter(pl.col("user_id") == "u_exempt").height == 4
    assert result.final_rows.filter(pl.col("user_id") == "u_blocked").height == 0


def test_max_retry_filter_can_remove_full_student_history() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        max_retries=2,
        retry_filter_mode=RETRY_FILTER_MODE_REMOVE_STUDENT,
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
    )

    assert "u_retry" not in set(result.final_user_paths.get_column("user_id").to_list())


def test_same_placement_module_repeat_filter_removes_offending_students_only() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        reject_same_placement_module_repeat=True,
    )

    user_ids = set(result.final_user_paths.get_column("user_id").to_list())
    assert "u_same_module_bad" not in user_ids
    assert "u_same_module_ok" in user_ids


def test_schema_size_threshold_removes_small_schemas_before_exact_selection() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=1,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        min_students_per_schema=2,
    )

    assert "initial-test -> zpdes" in set(result.schema_options.get_column("cleaned_schema").to_list())
    assert "adaptive-test -> zpdes -> adaptive-test -> zpdes" not in set(
        result.schema_options.get_column("cleaned_schema").to_list()
    )


def test_stage_summaries_are_monotonic_and_final_module_totals_match() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=5,
        min_history=2,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        selected_transition_counts=(1, 3),
    )

    students = result.stage_summary["students"].to_list()
    attempts = result.stage_summary["attempts"].to_list()
    assert students == sorted(students, reverse=True)
    assert attempts == sorted(attempts, reverse=True)

    module_summary = build_final_module_summary(result.final_rows)
    assert module_summary["attempts"].sum() == result.final_rows.height

    schema_summary = build_final_schema_summary(result.final_user_paths)
    assert schema_summary["students"].sum() == result.final_user_paths.height
    assert schema_summary["attempts"].sum() == result.final_user_paths["retained_attempts"].sum()
    assert schema_summary["student_share"].sum() == pytest.approx(1.0)
    assert schema_summary["attempt_share"].sum() == pytest.approx(1.0)

    baseline_schema_summary = build_schema_summary_vs_baseline(
        result.final_user_paths,
        baseline_students=result.baseline_students,
        baseline_attempts=result.baseline_attempts,
    )
    assert baseline_schema_summary["student_share"].sum() <= 1.0
    assert baseline_schema_summary["attempt_share"].sum() <= 1.0


def test_empty_final_slice_is_supported() -> None:
    result = filter_cohort_view(
        _sample_attempts(),
        selected_modules=("M1",),
        min_placement_attempts=5,
        min_history=1,
        history_basis=HISTORY_BASIS_RAW_ATTEMPTS,
        selected_transition_counts=(99,),
    )

    assert result.final_rows.height == 0
    assert result.final_user_paths.height == 0
    assert result.stage_summary.filter(pl.col("stage_key") == "schemas").item(0, "students") == 0

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

PLACEMENT_WORK_MODES: frozenset[str] = frozenset({"adaptive-test", "initial-test"})
HISTORY_BASIS_RAW_ATTEMPTS = "raw_attempts"
HISTORY_BASIS_DISTINCT_EXERCISES = "distinct_exercises"
RETRY_FILTER_MODE_REMOVE_EXERCISE = "remove_exercise"
RETRY_FILTER_MODE_REMOVE_STUDENT = "remove_student"
HISTORY_BASIS_OPTIONS: dict[str, str] = {
    "Raw attempts": HISTORY_BASIS_RAW_ATTEMPTS,
    "Distinct exercises": HISTORY_BASIS_DISTINCT_EXERCISES,
}
RETRY_FILTER_MODE_OPTIONS: dict[str, str] = {
    "Remove offending exercises only": RETRY_FILTER_MODE_REMOVE_EXERCISE,
    "Remove full student history": RETRY_FILTER_MODE_REMOVE_STUDENT,
}
UNKNOWN_MODULE_LABEL = "Unknown"


@dataclass(frozen=True, slots=True)
class CohortFilterResult:
    """Structured output for the cohort filter viewer page."""

    stage_summary: pl.DataFrame
    stage_module_attempts: pl.DataFrame
    transition_options: pl.DataFrame
    schema_options: pl.DataFrame
    final_rows: pl.DataFrame
    final_user_paths: pl.DataFrame
    final_module_summary: pl.DataFrame
    final_attempts: int
    baseline_students: int
    baseline_attempts: int


def _collect_lazy(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect()


def _empty_user_paths_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "user_id": [],
            "cleaned_schema": [],
            "transition_count": [],
            "retained_attempts": [],
            "retained_distinct_exercises": [],
        },
        schema={
            "user_id": pl.Utf8,
            "cleaned_schema": pl.Utf8,
            "transition_count": pl.Int64,
            "retained_attempts": pl.Int64,
            "retained_distinct_exercises": pl.Int64,
        },
    )


def _normalize_optional_string(column: str) -> pl.Expr:
    stripped = (
        pl.when(pl.col(column).is_not_null())
        .then(pl.col(column).cast(pl.Utf8).str.strip_chars())
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )
    return (
        pl.when(stripped.is_null() | (stripped == ""))
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(stripped)
        .alias(column)
    )


def _empty_stage_module_attempts_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "stage_key": [],
            "stage_label": [],
            "module_code": [],
            "attempts": [],
        },
        schema={
            "stage_key": pl.Utf8,
            "stage_label": pl.Utf8,
            "module_code": pl.Utf8,
            "attempts": pl.Int64,
        },
    )


def normalize_attempt_rows(frame: pl.DataFrame) -> pl.DataFrame:
    """Return a sorted attempt slice with the minimum fields required by the viewer."""
    required_columns = {
        "user_id",
        "created_at",
        "work_mode",
        "module_code",
        "exercise_id",
        "attempt_number",
    }
    missing = required_columns - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"Attempt frame is missing required columns: {missing_text}")

    normalized = (
        frame.select(
            [
                pl.col("user_id").cast(pl.Utf8),
                pl.col("created_at").cast(pl.Datetime(time_unit="us")),
                pl.col("work_mode").cast(pl.Utf8),
                pl.col("module_code").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("attempt_number").cast(pl.Int64),
            ]
        )
        .filter(
            pl.col("user_id").is_not_null()
            & (pl.col("user_id").str.strip_chars() != "")
            & pl.col("created_at").is_not_null()
            & pl.col("work_mode").is_not_null()
            & (pl.col("work_mode").str.strip_chars() != "")
        )
        .with_columns(
            pl.col("user_id").str.strip_chars().alias("user_id"),
            pl.col("work_mode").str.strip_chars().alias("work_mode"),
            _normalize_optional_string("module_code"),
            _normalize_optional_string("exercise_id"),
            pl.col("attempt_number").fill_null(0).alias("attempt_number"),
        )
        .sort(["user_id", "created_at", "module_code", "exercise_id", "attempt_number"])
    )
    if normalized.height == 0:
        return normalized.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("cleaned_schema"),
            pl.lit(0, dtype=pl.Int64).alias("transition_count"),
        ).head(0)
    return normalized


def _annotate_segments(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return frame.with_columns(pl.lit(0, dtype=pl.Int64).alias("segment_index"))
    return (
        frame.sort(["user_id", "created_at", "module_code", "exercise_id", "attempt_number"])
        .with_columns(
            (
                pl.col("user_id").ne(pl.col("user_id").shift(1)).fill_null(True)
                | pl.col("work_mode").ne(pl.col("work_mode").shift(1)).fill_null(True)
            ).alias("_segment_start")
        )
        .with_columns(
            pl.col("_segment_start")
            .cast(pl.Int64)
            .cum_sum()
            .over("user_id")
            .alias("segment_index")
        )
        .drop("_segment_start")
    )


def _build_segment_summary(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return pl.DataFrame(
            {
                "user_id": [],
                "segment_index": [],
                "segment_work_mode": [],
                "segment_attempts": [],
                "segment_module_codes": [],
            },
            schema={
                "user_id": pl.Utf8,
                "segment_index": pl.Int64,
                "segment_work_mode": pl.Utf8,
                "segment_attempts": pl.Int64,
                "segment_module_codes": pl.List(pl.Utf8),
            },
        )
    annotated = _annotate_segments(frame)
    return (
        annotated.group_by(["user_id", "segment_index"], maintain_order=True)
        .agg(
            pl.first("work_mode").alias("segment_work_mode"),
            pl.len().alias("segment_attempts"),
            pl.col("module_code").drop_nulls().unique().sort().alias("segment_module_codes"),
        )
        .sort(["user_id", "segment_index"])
    )


def _build_user_paths_from_rows(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return _empty_user_paths_frame()

    segment_summary = _build_segment_summary(frame)
    schema_summary = (
        segment_summary.group_by("user_id", maintain_order=True)
        .agg(pl.col("segment_work_mode").alias("cleaned_modes"))
        .with_columns(
            pl.col("cleaned_modes").list.join(" -> ").alias("cleaned_schema"),
            (
                pl.when(pl.col("cleaned_modes").list.len() > 0)
                .then(pl.col("cleaned_modes").list.len() - 1)
                .otherwise(pl.lit(0))
            )
            .cast(pl.Int64)
            .alias("transition_count"),
        )
        .drop("cleaned_modes")
    )
    history_summary = frame.group_by("user_id", maintain_order=True).agg(
        pl.len().alias("retained_attempts"),
        pl.col("exercise_id").drop_nulls().n_unique().alias("retained_distinct_exercises"),
    )
    return (
        schema_summary.join(history_summary, on="user_id", how="inner")
        .select(
            [
                "user_id",
                "cleaned_schema",
                "transition_count",
                "retained_attempts",
                "retained_distinct_exercises",
            ]
        )
        .sort(["transition_count", "cleaned_schema", "user_id"])
    )


def _normalize_attempt_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select(
            [
                pl.col("user_id").cast(pl.Utf8),
                pl.col("created_at").cast(pl.Datetime(time_unit="us")),
                pl.col("work_mode").cast(pl.Utf8),
                pl.col("module_code").cast(pl.Utf8),
                pl.col("exercise_id").cast(pl.Utf8),
                pl.col("attempt_number").cast(pl.Int64),
            ]
        )
        .filter(
            pl.col("user_id").is_not_null()
            & (pl.col("user_id").str.strip_chars() != "")
            & pl.col("created_at").is_not_null()
            & pl.col("work_mode").is_not_null()
            & (pl.col("work_mode").str.strip_chars() != "")
        )
        .with_columns(
            pl.col("user_id").str.strip_chars().alias("user_id"),
            pl.col("work_mode").str.strip_chars().alias("work_mode"),
            _normalize_optional_string("module_code"),
            _normalize_optional_string("exercise_id"),
            pl.col("attempt_number").fill_null(0).alias("attempt_number"),
        )
    )


def _annotate_segments_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.sort(["user_id", "created_at", "module_code", "exercise_id", "attempt_number"])
        .with_columns(
            (
                pl.col("user_id").ne(pl.col("user_id").shift(1)).fill_null(True)
                | pl.col("work_mode").ne(pl.col("work_mode").shift(1)).fill_null(True)
            ).alias("_segment_start")
        )
        .with_columns(
            pl.col("_segment_start")
            .cast(pl.Int64)
            .cum_sum()
            .over("user_id")
            .alias("segment_index")
        )
        .drop("_segment_start")
    )


def _build_stage_record_from_values(
    *,
    stage_key: str,
    stage_label: str,
    students: int,
    attempts: int,
    module_codes: list[str],
    baseline_students: int,
    baseline_attempts: int,
) -> dict[str, Any]:
    return {
        "stage_key": stage_key,
        "stage_label": stage_label,
        "students": students,
        "attempts": attempts,
        "mean_attempts_per_student": float(attempts / students) if students else 0.0,
        "represented_modules": len(module_codes),
        "module_codes": ", ".join(module_codes),
        "student_share_vs_baseline": (students / baseline_students) if baseline_students else 0.0,
        "attempt_share_vs_baseline": (attempts / baseline_attempts) if baseline_attempts else 0.0,
    }


def _build_stage_module_attempts_from_aggregated(
    frame: pl.DataFrame,
    *,
    stage_key: str,
    stage_label: str,
) -> pl.DataFrame:
    if frame.height == 0:
        return _empty_stage_module_attempts_frame()
    return (
        frame.with_columns(
            pl.lit(stage_key).alias("stage_key"),
            pl.lit(stage_label).alias("stage_label"),
        )
        .select(["stage_key", "stage_label", "module_code", "attempts"])
        .sort(["stage_label", "attempts", "module_code"], descending=[False, True, False])
    )


def _collect_stage_metrics_from_lazy(lf: pl.LazyFrame) -> tuple[dict[str, int], pl.DataFrame, list[str]]:
    summary = _collect_lazy(
        lf.select(
            pl.col("user_id").n_unique().alias("students"),
            pl.len().alias("attempts"),
        )
    )
    students = int(summary.get_column("students").item()) if summary.height else 0
    attempts = int(summary.get_column("attempts").item()) if summary.height else 0
    module_attempts = _collect_lazy(
        lf.with_columns(_module_display_expr())
        .group_by("module_display")
        .agg(pl.len().alias("attempts"))
        .rename({"module_display": "module_code"})
        .sort(["attempts", "module_code"], descending=[True, False])
    )
    module_codes = (
        module_attempts.get_column("module_code").to_list() if module_attempts.height else []
    )
    return {"students": students, "attempts": attempts}, module_attempts, module_codes


def _collect_segment_tables(lf: pl.LazyFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    segmented = _annotate_segments_lazy(lf)
    segment_summary = _collect_lazy(
        segmented.group_by(["user_id", "segment_index"], maintain_order=True)
        .agg(
            pl.first("work_mode").alias("segment_work_mode"),
            pl.len().alias("segment_attempts"),
        )
        .sort(["user_id", "segment_index"])
    )
    segment_module_attempts = _collect_lazy(
        segmented.group_by(["user_id", "segment_index", "module_code"], maintain_order=True)
        .agg(pl.len().alias("attempts"))
        .sort(["user_id", "segment_index", "module_code"])
    )
    return segment_summary, segment_module_attempts


def _build_stage_summary_from_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    *,
    stage_key: str,
    stage_label: str,
    baseline_students: int,
    baseline_attempts: int,
) -> dict[str, Any]:
    students = int(segment_summary.select(pl.col("user_id").n_unique()).item()) if segment_summary.height else 0
    attempts = int(segment_summary.get_column("segment_attempts").sum()) if segment_summary.height else 0
    module_codes = []
    if segment_module_attempts.height:
        module_codes = (
            segment_module_attempts.with_columns(_module_display_expr())
            .select("module_display")
            .unique()
            .sort("module_display")
            .get_column("module_display")
            .to_list()
        )
    return _build_stage_record_from_values(
        stage_key=stage_key,
        stage_label=stage_label,
        students=students,
        attempts=attempts,
        module_codes=module_codes,
        baseline_students=baseline_students,
        baseline_attempts=baseline_attempts,
    )


def _build_stage_module_attempts_from_segments(
    segment_module_attempts: pl.DataFrame,
    *,
    stage_key: str,
    stage_label: str,
) -> pl.DataFrame:
    if segment_module_attempts.height == 0:
        return _empty_stage_module_attempts_frame()
    aggregated = (
        segment_module_attempts.with_columns(_module_display_expr())
        .group_by("module_display")
        .agg(pl.sum("attempts").alias("attempts"))
        .rename({"module_display": "module_code"})
        .sort(["attempts", "module_code"], descending=[True, False])
    )
    return _build_stage_module_attempts_from_aggregated(
        aggregated,
        stage_key=stage_key,
        stage_label=stage_label,
    )


def _build_user_paths_from_segments(
    segment_summary: pl.DataFrame,
    *,
    distinct_exercise_counts: pl.DataFrame | None = None,
) -> pl.DataFrame:
    if segment_summary.height == 0:
        return _empty_user_paths_frame()
    base = (
        segment_summary.group_by("user_id", maintain_order=True)
        .agg(
            pl.col("segment_work_mode").alias("cleaned_modes"),
            pl.sum("segment_attempts").alias("retained_attempts"),
        )
        .with_columns(
            pl.col("cleaned_modes").list.join(" -> ").alias("cleaned_schema"),
            (
                pl.when(pl.col("cleaned_modes").list.len() > 0)
                .then(pl.col("cleaned_modes").list.len() - 1)
                .otherwise(pl.lit(0))
            )
            .cast(pl.Int64)
            .alias("transition_count"),
        )
        .drop("cleaned_modes")
    )
    if distinct_exercise_counts is None:
        base = base.with_columns(
            pl.lit(0, dtype=pl.Int64).alias("retained_distinct_exercises")
        )
    else:
        base = base.join(distinct_exercise_counts, on="user_id", how="left").with_columns(
            pl.col("retained_distinct_exercises").fill_null(0)
        )
    return base.select(
        [
            "user_id",
            "cleaned_schema",
            "transition_count",
            "retained_attempts",
            "retained_distinct_exercises",
        ]
    ).sort(["transition_count", "cleaned_schema", "user_id"])


def _collect_distinct_exercise_counts_for_segments(
    fact_path: str,
    *,
    start_date_iso: str,
    end_date_iso: str,
    selected_modules: tuple[str, ...],
    max_retries: int,
    retry_filter_mode: str,
    retained_segments: pl.DataFrame,
) -> pl.DataFrame:
    if retained_segments.height == 0:
        return pl.DataFrame(
            {"user_id": [], "retained_distinct_exercises": []},
            schema={"user_id": pl.Utf8, "retained_distinct_exercises": pl.Int64},
        )
    base = _normalize_attempt_lazy(
        pl.scan_parquet(fact_path)
        .filter(
            (pl.col("date_utc") >= pl.lit(pl.Date(start_date_iso)))
            & (pl.col("date_utc") <= pl.lit(pl.Date(end_date_iso)))
        )
    )
    if selected_modules:
        base = base.filter(pl.col("module_code").is_in(list(selected_modules)))
    if max_retries > 0:
        offending_pairs = (
            base.filter(pl.col("exercise_id").is_not_null() & (pl.col("exercise_id") != ""))
            .group_by(["user_id", "exercise_id"])
            .agg(pl.len().alias("attempts"))
            .with_columns((pl.col("attempts") - 1).alias("retries"))
            .filter(pl.col("retries") > max_retries)
            .select(["user_id", "exercise_id"])
        )
        if retry_filter_mode == RETRY_FILTER_MODE_REMOVE_STUDENT:
            offending_users = offending_pairs.select("user_id").unique()
            base = base.join(offending_users, on="user_id", how="anti")
        else:
            base = base.join(offending_pairs, on=["user_id", "exercise_id"], how="anti")
    segmented = _annotate_segments_lazy(base)
    retained_keys = retained_segments.select(["user_id", "segment_index"]).lazy()
    return _collect_lazy(
        segmented.join(retained_keys, on=["user_id", "segment_index"], how="inner")
        .group_by("user_id")
        .agg(pl.col("exercise_id").drop_nulls().n_unique().alias("retained_distinct_exercises"))
    )


def apply_module_keep_filter(frame: pl.DataFrame, selected_modules: tuple[str, ...]) -> pl.DataFrame:
    """Keep only attempts in the selected modules."""
    normalized_modules = tuple(str(code).strip() for code in selected_modules if str(code or "").strip())
    if not normalized_modules:
        return frame.head(0)
    return frame.filter(pl.col("module_code").is_in(normalized_modules))


def apply_max_retry_filter(
    frame: pl.DataFrame,
    *,
    max_retries: int,
    retry_filter_mode: str,
) -> pl.DataFrame:
    """Drop either offending exercises or full students when retries exceed the selected limit."""
    threshold = int(max_retries)
    if frame.height == 0 or threshold < 0:
        return frame
    if threshold == 0:
        return frame

    offending_pairs = (
        frame.filter(pl.col("exercise_id").is_not_null() & (pl.col("exercise_id").str.strip_chars() != ""))
        .group_by(["user_id", "exercise_id"])
        .agg(pl.len().alias("attempts"))
        .with_columns((pl.col("attempts") - 1).alias("retries"))
        .filter(pl.col("retries") > threshold)
        .select(["user_id", "exercise_id"])
    )
    if offending_pairs.height == 0:
        return frame

    if retry_filter_mode == RETRY_FILTER_MODE_REMOVE_STUDENT:
        offending_users = offending_pairs.select("user_id").unique()
        return frame.join(offending_users, on="user_id", how="anti")

    return frame.join(offending_pairs, on=["user_id", "exercise_id"], how="anti")


def apply_placement_cleanup(
    frame: pl.DataFrame,
    *,
    min_placement_attempts: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Remove invalid short placement segments and the segment that immediately follows them."""
    if frame.height == 0:
        empty_rows = frame.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("cleaned_schema"),
            pl.lit(0, dtype=pl.Int64).alias("transition_count"),
        ).head(0)
        return empty_rows, _empty_user_paths_frame()

    threshold = max(1, int(min_placement_attempts))
    segment_summary = _build_segment_summary(frame)
    short_placements = segment_summary.filter(
        pl.col("segment_work_mode").is_in(sorted(PLACEMENT_WORK_MODES))
        & (pl.col("segment_attempts") < threshold)
    ).select(["user_id", "segment_index"])

    if short_placements.height == 0:
        retained_rows = frame.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("cleaned_schema"),
            pl.lit(0, dtype=pl.Int64).alias("transition_count"),
        ).head(0)
        user_paths = _build_user_paths_from_rows(frame)
        if user_paths.height == 0:
            return retained_rows, user_paths
        retained_rows = frame.join(
            user_paths.select(["user_id", "cleaned_schema", "transition_count"]),
            on="user_id",
            how="left",
        )
        return retained_rows, user_paths

    segments_to_drop = pl.concat(
        [
            short_placements,
            short_placements.with_columns((pl.col("segment_index") + 1).alias("segment_index")),
        ],
        how="vertical_relaxed",
    ).unique()

    retained_rows = (
        _annotate_segments(frame)
        .join(segments_to_drop, on=["user_id", "segment_index"], how="anti")
        .drop("segment_index")
        .sort(["user_id", "created_at", "module_code", "exercise_id", "attempt_number"])
    )
    if retained_rows.height == 0:
        empty_rows = frame.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("cleaned_schema"),
            pl.lit(0, dtype=pl.Int64).alias("transition_count"),
        ).head(0)
        return empty_rows, _empty_user_paths_frame()

    user_paths = _build_user_paths_from_rows(retained_rows)
    retained_rows = retained_rows.join(
        user_paths.select(["user_id", "cleaned_schema", "transition_count"]),
        on="user_id",
        how="left",
    )
    return retained_rows, user_paths


def apply_same_placement_module_repeat_filter(
    rows: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    enabled: bool,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Drop students who revisit the same module after the same placement mode."""
    if not enabled or rows.height == 0 or user_paths.height == 0:
        return rows, user_paths
    segment_summary = (
        _build_segment_summary(rows)
        .with_columns(
            pl.col("segment_work_mode").shift(-1).over("user_id").alias("next_work_mode"),
            pl.col("segment_module_codes").shift(-1).over("user_id").alias("next_module_codes"),
        )
        .sort(["user_id", "segment_index"])
    )
    placement_pairs = (
        segment_summary.filter(
            pl.col("segment_work_mode").is_in(sorted(PLACEMENT_WORK_MODES))
            & (pl.col("next_work_mode") == "zpdes")
        )
        .select(
            "user_id",
            pl.col("segment_work_mode").alias("placement_mode"),
            pl.col("next_module_codes").alias("module_code"),
        )
        .explode("module_code")
        .drop_nulls("module_code")
    )
    offending_users = (
        placement_pairs.group_by(["user_id", "placement_mode", "module_code"])
        .agg(pl.len().alias("occurrences"))
        .filter(pl.col("occurrences") > 1)
        .select("user_id")
        .unique()
    )
    if offending_users.height == 0:
        return rows, user_paths
    return rows.join(offending_users, on="user_id", how="anti"), user_paths.join(
        offending_users, on="user_id", how="anti"
    )


def apply_history_threshold(
    rows: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    min_history: int,
    history_basis: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Keep only students whose cleaned retained history meets the selected threshold."""
    threshold = max(1, int(min_history))
    basis = str(history_basis or HISTORY_BASIS_RAW_ATTEMPTS).strip()
    if rows.height == 0 or user_paths.height == 0:
        return rows.head(0), user_paths.head(0)
    if threshold <= 1:
        return rows, user_paths

    metric_column = (
        "retained_distinct_exercises"
        if basis == HISTORY_BASIS_DISTINCT_EXERCISES
        else "retained_attempts"
    )
    eligible_users = user_paths.filter(pl.col(metric_column) >= threshold).select("user_id")
    filtered_rows = rows.join(eligible_users, on="user_id", how="semi")
    filtered_paths = user_paths.join(eligible_users, on="user_id", how="semi")
    return filtered_rows, filtered_paths


def apply_transition_count_filter(
    rows: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    selected_transition_counts: tuple[int, ...],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Keep only students with an exact cleaned transition count when a filter is provided."""
    counts = tuple(sorted({int(value) for value in selected_transition_counts}))
    if not counts or rows.height == 0 or user_paths.height == 0:
        return rows, user_paths
    eligible_users = user_paths.filter(pl.col("transition_count").is_in(counts)).select("user_id")
    return rows.join(eligible_users, on="user_id", how="semi"), user_paths.join(
        eligible_users, on="user_id", how="semi"
    )


def apply_schema_size_threshold(
    rows: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    min_students_per_schema: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Remove schemas that are represented by fewer than the selected number of students."""
    threshold = max(1, int(min_students_per_schema))
    if rows.height == 0 or user_paths.height == 0 or threshold <= 1:
        return rows, user_paths

    eligible_schemas = (
        user_paths.group_by("cleaned_schema")
        .agg(pl.len().alias("students"))
        .filter(pl.col("students") >= threshold)
        .select("cleaned_schema")
    )
    if eligible_schemas.height == 0:
        return rows.head(0), user_paths.head(0)
    filtered_paths = user_paths.join(eligible_schemas, on="cleaned_schema", how="semi")
    eligible_users = filtered_paths.select("user_id")
    filtered_rows = rows.join(eligible_users, on="user_id", how="semi")
    return filtered_rows, filtered_paths


def apply_schema_filter(
    rows: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    selected_schemas: tuple[str, ...],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Keep only students whose cleaned path matches one of the selected exact schemas."""
    schemas = tuple(str(value).strip() for value in selected_schemas if str(value or "").strip())
    if not schemas or rows.height == 0 or user_paths.height == 0:
        return rows, user_paths
    eligible_users = user_paths.filter(pl.col("cleaned_schema").is_in(schemas)).select("user_id")
    return rows.join(eligible_users, on="user_id", how="semi"), user_paths.join(
        eligible_users, on="user_id", how="semi"
    )


def apply_placement_cleanup_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    *,
    min_placement_attempts: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if segment_summary.height == 0:
        return segment_summary, segment_module_attempts
    threshold = max(1, int(min_placement_attempts))
    short_placements = segment_summary.filter(
        pl.col("segment_work_mode").is_in(sorted(PLACEMENT_WORK_MODES))
        & (pl.col("segment_attempts") < threshold)
    ).select(["user_id", "segment_index"])
    if short_placements.height == 0:
        return segment_summary, segment_module_attempts
    segments_to_drop = pl.concat(
        [
            short_placements,
            short_placements.with_columns((pl.col("segment_index") + 1).alias("segment_index")),
        ],
        how="vertical_relaxed",
    ).unique()
    return (
        segment_summary.join(segments_to_drop, on=["user_id", "segment_index"], how="anti"),
        segment_module_attempts.join(segments_to_drop, on=["user_id", "segment_index"], how="anti"),
    )


def apply_same_placement_module_repeat_filter_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    *,
    enabled: bool,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if not enabled or segment_summary.height == 0:
        return segment_summary, segment_module_attempts
    segment_modules = (
        segment_module_attempts.drop_nulls("module_code")
        .group_by(["user_id", "segment_index"])
        .agg(pl.col("module_code").unique().sort().alias("segment_module_codes"))
    )
    annotated = (
        segment_summary.join(segment_modules, on=["user_id", "segment_index"], how="left")
        .sort(["user_id", "segment_index"])
        .with_columns(
            pl.col("segment_work_mode").shift(-1).over("user_id").alias("next_work_mode"),
            pl.col("segment_module_codes").shift(-1).over("user_id").alias("next_module_codes"),
        )
    )
    offending_users = (
        annotated.filter(
            pl.col("segment_work_mode").is_in(sorted(PLACEMENT_WORK_MODES))
            & (pl.col("next_work_mode") == "zpdes")
        )
        .select(
            "user_id",
            pl.col("segment_work_mode").alias("placement_mode"),
            pl.col("next_module_codes").alias("module_code"),
        )
        .explode("module_code")
        .drop_nulls("module_code")
        .group_by(["user_id", "placement_mode", "module_code"])
        .agg(pl.len().alias("occurrences"))
        .filter(pl.col("occurrences") > 1)
        .select("user_id")
        .unique()
    )
    if offending_users.height == 0:
        return segment_summary, segment_module_attempts
    return (
        segment_summary.join(offending_users, on="user_id", how="anti"),
        segment_module_attempts.join(offending_users, on="user_id", how="anti"),
    )


def apply_history_threshold_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    min_history: int,
    history_basis: str,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    threshold = max(1, int(min_history))
    if segment_summary.height == 0 or user_paths.height == 0 or threshold <= 1:
        return segment_summary, segment_module_attempts, user_paths
    metric_column = (
        "retained_distinct_exercises"
        if history_basis == HISTORY_BASIS_DISTINCT_EXERCISES
        else "retained_attempts"
    )
    eligible_users = user_paths.filter(pl.col(metric_column) >= threshold).select("user_id")
    return (
        segment_summary.join(eligible_users, on="user_id", how="semi"),
        segment_module_attempts.join(eligible_users, on="user_id", how="semi"),
        user_paths.join(eligible_users, on="user_id", how="semi"),
    )


def apply_transition_count_filter_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    selected_transition_counts: tuple[int, ...],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    counts = tuple(sorted({int(value) for value in selected_transition_counts}))
    if not counts or segment_summary.height == 0 or user_paths.height == 0:
        return segment_summary, segment_module_attempts, user_paths
    eligible_users = user_paths.filter(pl.col("transition_count").is_in(counts)).select("user_id")
    return (
        segment_summary.join(eligible_users, on="user_id", how="semi"),
        segment_module_attempts.join(eligible_users, on="user_id", how="semi"),
        user_paths.join(eligible_users, on="user_id", how="semi"),
    )


def apply_schema_size_threshold_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    min_students_per_schema: int,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    threshold = max(1, int(min_students_per_schema))
    if segment_summary.height == 0 or user_paths.height == 0 or threshold <= 1:
        return segment_summary, segment_module_attempts, user_paths
    eligible_schemas = (
        user_paths.group_by("cleaned_schema")
        .agg(pl.len().alias("students"))
        .filter(pl.col("students") >= threshold)
        .select("cleaned_schema")
    )
    if eligible_schemas.height == 0:
        empty_segments = segment_summary.head(0)
        empty_seg_mod = segment_module_attempts.head(0)
        empty_paths = user_paths.head(0)
        return empty_segments, empty_seg_mod, empty_paths
    filtered_paths = user_paths.join(eligible_schemas, on="cleaned_schema", how="semi")
    eligible_users = filtered_paths.select("user_id")
    return (
        segment_summary.join(eligible_users, on="user_id", how="semi"),
        segment_module_attempts.join(eligible_users, on="user_id", how="semi"),
        filtered_paths,
    )


def apply_schema_filter_segments(
    segment_summary: pl.DataFrame,
    segment_module_attempts: pl.DataFrame,
    user_paths: pl.DataFrame,
    *,
    selected_schemas: tuple[str, ...],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    schemas = tuple(str(value).strip() for value in selected_schemas if str(value or "").strip())
    if not schemas or segment_summary.height == 0 or user_paths.height == 0:
        return segment_summary, segment_module_attempts, user_paths
    eligible_users = user_paths.filter(pl.col("cleaned_schema").is_in(schemas)).select("user_id")
    return (
        segment_summary.join(eligible_users, on="user_id", how="semi"),
        segment_module_attempts.join(eligible_users, on="user_id", how="semi"),
        user_paths.join(eligible_users, on="user_id", how="semi"),
    )


def _module_display_expr(column: str = "module_code") -> pl.Expr:
    return (
        pl.when(pl.col(column).is_null() | (pl.col(column).cast(pl.Utf8).str.strip_chars() == ""))
        .then(pl.lit(UNKNOWN_MODULE_LABEL))
        .otherwise(pl.col(column).cast(pl.Utf8))
        .alias("module_display")
    )


def build_stage_summary(
    frame: pl.DataFrame,
    *,
    stage_key: str,
    stage_label: str,
    baseline_students: int,
    baseline_attempts: int,
) -> dict[str, Any]:
    """Summarize one stage of the cohort funnel."""
    students = int(frame.select(pl.col("user_id").n_unique()).item()) if frame.height else 0
    attempts = int(frame.height)
    modules = (
        sorted(
            {
                str(value).strip()
                for value in frame.get_column("module_code").drop_nulls().to_list()
                if str(value or "").strip()
            }
        )
        if frame.height
        else []
    )
    mean_attempts = (attempts / students) if students else 0.0
    return {
        "stage_key": stage_key,
        "stage_label": stage_label,
        "students": students,
        "attempts": attempts,
        "mean_attempts_per_student": float(mean_attempts),
        "represented_modules": len(modules),
        "module_codes": ", ".join(modules),
        "student_share_vs_baseline": (students / baseline_students) if baseline_students else 0.0,
        "attempt_share_vs_baseline": (attempts / baseline_attempts) if baseline_attempts else 0.0,
    }


def build_stage_module_attempts(
    frame: pl.DataFrame,
    *,
    stage_key: str,
    stage_label: str,
) -> pl.DataFrame:
    """Return per-module attempt counts for one stage."""
    if frame.height == 0:
        return pl.DataFrame(
            {
                "stage_key": [],
                "stage_label": [],
                "module_code": [],
                "attempts": [],
            },
            schema={
                "stage_key": pl.Utf8,
                "stage_label": pl.Utf8,
                "module_code": pl.Utf8,
                "attempts": pl.Int64,
            },
        )
    return (
        frame.with_columns(_module_display_expr())
        .group_by("module_display")
        .agg(pl.len().alias("attempts"))
        .rename({"module_display": "module_code"})
        .with_columns(
            pl.lit(stage_key).alias("stage_key"),
            pl.lit(stage_label).alias("stage_label"),
        )
        .select(["stage_key", "stage_label", "module_code", "attempts"])
        .sort(["stage_label", "attempts", "module_code"], descending=[False, True, False])
    )


def build_transition_options(user_paths: pl.DataFrame) -> pl.DataFrame:
    """Build transition-count options from the currently retained cleaned slice."""
    if user_paths.height == 0:
        return pl.DataFrame(
            {"transition_count": [], "students": [], "attempts": []},
            schema={"transition_count": pl.Int64, "students": pl.Int64, "attempts": pl.Int64},
        )
    return (
        user_paths.group_by("transition_count")
        .agg(
            pl.len().alias("students"),
            pl.sum("retained_attempts").alias("attempts"),
        )
        .sort("transition_count")
    )


def build_schema_options(user_paths: pl.DataFrame) -> pl.DataFrame:
    """Build exact-schema options from the currently retained cleaned slice."""
    if user_paths.height == 0:
        return pl.DataFrame(
            {"cleaned_schema": [], "students": [], "attempts": []},
            schema={"cleaned_schema": pl.Utf8, "students": pl.Int64, "attempts": pl.Int64},
        )
    return (
        user_paths.group_by("cleaned_schema")
        .agg(
            pl.len().alias("students"),
            pl.sum("retained_attempts").alias("attempts"),
        )
        .sort(["students", "attempts", "cleaned_schema"], descending=[True, True, False])
    )


def filter_cohort_view(
    attempt_rows: pl.DataFrame,
    *,
    selected_modules: tuple[str, ...],
    min_placement_attempts: int,
    min_history: int,
    history_basis: str,
    max_retries: int = -1,
    retry_filter_mode: str = RETRY_FILTER_MODE_REMOVE_EXERCISE,
    reject_same_placement_module_repeat: bool = False,
    selected_transition_counts: tuple[int, ...] = (),
    min_students_per_schema: int = 1,
    selected_schemas: tuple[str, ...] = (),
) -> CohortFilterResult:
    """Apply the full cohort filter pipeline and return all stage summaries."""
    baseline_rows = normalize_attempt_rows(attempt_rows)
    baseline_students = int(baseline_rows.select(pl.col("user_id").n_unique()).item()) if baseline_rows.height else 0
    baseline_attempts = int(baseline_rows.height)

    stage_frames: list[tuple[str, str, pl.DataFrame]] = []
    stage_module_frames: list[pl.DataFrame] = []

    def record_stage(stage_key: str, stage_label: str, frame: pl.DataFrame) -> None:
        stage_frames.append((stage_key, stage_label, frame))
        stage_module_frames.append(
            build_stage_module_attempts(frame, stage_key=stage_key, stage_label=stage_label)
        )

    record_stage("baseline", "Baseline", baseline_rows)

    module_rows = apply_module_keep_filter(baseline_rows, selected_modules)
    record_stage("modules", "Modules kept", module_rows)

    retry_filtered_rows = apply_max_retry_filter(
        module_rows,
        max_retries=max_retries,
        retry_filter_mode=retry_filter_mode,
    )
    record_stage("retries", "Retry filter", retry_filtered_rows)

    cleaned_rows, cleaned_user_paths = apply_placement_cleanup(
        retry_filtered_rows,
        min_placement_attempts=min_placement_attempts,
    )
    record_stage("placement", "Placement cleanup", cleaned_rows)

    repeat_filtered_rows, repeat_filtered_user_paths = apply_same_placement_module_repeat_filter(
        cleaned_rows,
        cleaned_user_paths,
        enabled=reject_same_placement_module_repeat,
    )
    record_stage("module_repeat", "Repeated module after same placement", repeat_filtered_rows)

    history_rows, history_user_paths = apply_history_threshold(
        repeat_filtered_rows,
        repeat_filtered_user_paths,
        min_history=min_history,
        history_basis=history_basis,
    )
    record_stage("history", "History threshold", history_rows)

    transition_options = build_transition_options(history_user_paths)

    transition_rows, transition_user_paths = apply_transition_count_filter(
        history_rows,
        history_user_paths,
        selected_transition_counts=selected_transition_counts,
    )
    record_stage("transitions", "Transition filter", transition_rows)

    schema_threshold_rows, schema_threshold_user_paths = apply_schema_size_threshold(
        transition_rows,
        transition_user_paths,
        min_students_per_schema=min_students_per_schema,
    )
    record_stage("schema_size", "Schema size threshold", schema_threshold_rows)

    schema_options = build_schema_options(schema_threshold_user_paths)

    final_rows, final_user_paths = apply_schema_filter(
        schema_threshold_rows,
        schema_threshold_user_paths,
        selected_schemas=selected_schemas,
    )
    record_stage("schemas", "Schema filter", final_rows)

    stage_summary = pl.DataFrame(
        [
            build_stage_summary(
                frame,
                stage_key=stage_key,
                stage_label=stage_label,
                baseline_students=baseline_students,
                baseline_attempts=baseline_attempts,
            )
            for stage_key, stage_label, frame in stage_frames
        ]
    )
    stage_module_attempts = pl.concat(stage_module_frames, how="vertical_relaxed")
    final_module_summary = build_final_module_summary(final_rows)
    final_attempts = int(final_rows.height)

    return CohortFilterResult(
        stage_summary=stage_summary,
        stage_module_attempts=stage_module_attempts,
        transition_options=transition_options,
        schema_options=schema_options,
        final_rows=final_rows,
        final_user_paths=final_user_paths,
        final_module_summary=final_module_summary,
        final_attempts=final_attempts,
        baseline_students=baseline_students,
        baseline_attempts=baseline_attempts,
    )


def compute_cohort_view_from_parquet(
    fact_path: str,
    *,
    start_date_iso: str,
    end_date_iso: str,
    selected_modules: tuple[str, ...],
    max_retries: int,
    retry_filter_mode: str,
    min_placement_attempts: int,
    reject_same_placement_module_repeat: bool,
    min_history: int,
    history_basis: str,
    selected_transition_counts: tuple[int, ...],
    min_students_per_schema: int,
    selected_schemas: tuple[str, ...],
) -> CohortFilterResult:
    start_date = pl.lit(start_date_iso).str.strptime(pl.Date, strict=True)
    end_date = pl.lit(end_date_iso).str.strptime(pl.Date, strict=True)
    base_scan = _normalize_attempt_lazy(
        pl.scan_parquet(fact_path).filter(
            (pl.col("date_utc") >= start_date) & (pl.col("date_utc") <= end_date)
        )
    )

    baseline_metrics, baseline_module_attempts_frame, baseline_module_codes = _collect_stage_metrics_from_lazy(
        base_scan
    )
    baseline_students = int(baseline_metrics["students"])
    baseline_attempts = int(baseline_metrics["attempts"])

    stage_records: list[dict[str, Any]] = [
        _build_stage_record_from_values(
            stage_key="baseline",
            stage_label="Baseline",
            students=baseline_students,
            attempts=baseline_attempts,
            module_codes=baseline_module_codes,
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    ]
    stage_module_frames: list[pl.DataFrame] = [
        _build_stage_module_attempts_from_aggregated(
            baseline_module_attempts_frame,
            stage_key="baseline",
            stage_label="Baseline",
        )
    ]

    module_scan = (
        base_scan.filter(pl.col("module_code").is_in(list(selected_modules)))
        if selected_modules
        else base_scan.head(0)
    )
    module_metrics, module_module_attempts_frame, module_codes = _collect_stage_metrics_from_lazy(module_scan)
    stage_records.append(
        _build_stage_record_from_values(
            stage_key="modules",
            stage_label="Modules kept",
            students=int(module_metrics["students"]),
            attempts=int(module_metrics["attempts"]),
            module_codes=module_codes,
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_aggregated(
            module_module_attempts_frame,
            stage_key="modules",
            stage_label="Modules kept",
        )
    )

    retry_scan = module_scan
    if max_retries > 0:
        offending_pairs = (
            module_scan.filter(pl.col("exercise_id").is_not_null() & (pl.col("exercise_id") != ""))
            .group_by(["user_id", "exercise_id"])
            .agg(pl.len().alias("attempts"))
            .with_columns((pl.col("attempts") - 1).alias("retries"))
            .filter(pl.col("retries") > max_retries)
            .select(["user_id", "exercise_id"])
        )
        if retry_filter_mode == RETRY_FILTER_MODE_REMOVE_STUDENT:
            retry_scan = module_scan.join(offending_pairs.select("user_id").unique(), on="user_id", how="anti")
        else:
            retry_scan = module_scan.join(offending_pairs, on=["user_id", "exercise_id"], how="anti")

    retry_metrics, retry_module_attempts_frame, retry_codes = _collect_stage_metrics_from_lazy(retry_scan)
    stage_records.append(
        _build_stage_record_from_values(
            stage_key="retries",
            stage_label="Retry filter",
            students=int(retry_metrics["students"]),
            attempts=int(retry_metrics["attempts"]),
            module_codes=retry_codes,
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_aggregated(
            retry_module_attempts_frame,
            stage_key="retries",
            stage_label="Retry filter",
        )
    )

    segment_summary, segment_module_attempts = _collect_segment_tables(retry_scan)

    placement_segments, placement_segment_modules = apply_placement_cleanup_segments(
        segment_summary,
        segment_module_attempts,
        min_placement_attempts=min_placement_attempts,
    )
    stage_records.append(
        _build_stage_summary_from_segments(
            placement_segments,
            placement_segment_modules,
            stage_key="placement",
            stage_label="Placement cleanup",
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_segments(
            placement_segment_modules,
            stage_key="placement",
            stage_label="Placement cleanup",
        )
    )

    repeat_segments, repeat_segment_modules = apply_same_placement_module_repeat_filter_segments(
        placement_segments,
        placement_segment_modules,
        enabled=reject_same_placement_module_repeat,
    )
    stage_records.append(
        _build_stage_summary_from_segments(
            repeat_segments,
            repeat_segment_modules,
            stage_key="module_repeat",
            stage_label="Repeated module after same placement",
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_segments(
            repeat_segment_modules,
            stage_key="module_repeat",
            stage_label="Repeated module after same placement",
        )
    )

    distinct_counts = None
    if history_basis == HISTORY_BASIS_DISTINCT_EXERCISES and repeat_segments.height:
        distinct_counts = _collect_distinct_exercise_counts_for_segments(
            fact_path,
            start_date_iso=start_date_iso,
            end_date_iso=end_date_iso,
            selected_modules=selected_modules,
            max_retries=max_retries,
            retry_filter_mode=retry_filter_mode,
            retained_segments=repeat_segments,
        )
    repeat_user_paths = _build_user_paths_from_segments(
        repeat_segments,
        distinct_exercise_counts=distinct_counts,
    )

    history_segments, history_segment_modules, history_user_paths = apply_history_threshold_segments(
        repeat_segments,
        repeat_segment_modules,
        repeat_user_paths,
        min_history=min_history,
        history_basis=history_basis,
    )
    stage_records.append(
        _build_stage_summary_from_segments(
            history_segments,
            history_segment_modules,
            stage_key="history",
            stage_label="History threshold",
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_segments(
            history_segment_modules,
            stage_key="history",
            stage_label="History threshold",
        )
    )

    transition_options = build_transition_options(history_user_paths)

    transition_segments, transition_segment_modules, transition_user_paths = apply_transition_count_filter_segments(
        history_segments,
        history_segment_modules,
        history_user_paths,
        selected_transition_counts=selected_transition_counts,
    )
    stage_records.append(
        _build_stage_summary_from_segments(
            transition_segments,
            transition_segment_modules,
            stage_key="transitions",
            stage_label="Transition filter",
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_segments(
            transition_segment_modules,
            stage_key="transitions",
            stage_label="Transition filter",
        )
    )

    schema_size_segments, schema_size_segment_modules, schema_size_user_paths = apply_schema_size_threshold_segments(
        transition_segments,
        transition_segment_modules,
        transition_user_paths,
        min_students_per_schema=min_students_per_schema,
    )
    stage_records.append(
        _build_stage_summary_from_segments(
            schema_size_segments,
            schema_size_segment_modules,
            stage_key="schema_size",
            stage_label="Schema size threshold",
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_segments(
            schema_size_segment_modules,
            stage_key="schema_size",
            stage_label="Schema size threshold",
        )
    )

    schema_options = build_schema_options(schema_size_user_paths)

    final_segments, final_segment_modules, final_user_paths = apply_schema_filter_segments(
        schema_size_segments,
        schema_size_segment_modules,
        schema_size_user_paths,
        selected_schemas=selected_schemas,
    )
    stage_records.append(
        _build_stage_summary_from_segments(
            final_segments,
            final_segment_modules,
            stage_key="schemas",
            stage_label="Schema filter",
            baseline_students=baseline_students,
            baseline_attempts=baseline_attempts,
        )
    )
    stage_module_frames.append(
        _build_stage_module_attempts_from_segments(
            final_segment_modules,
            stage_key="schemas",
            stage_label="Schema filter",
        )
    )

    final_module_summary = (
        final_segment_modules.with_columns(_module_display_expr())
        .group_by("module_display")
        .agg(pl.sum("attempts").alias("attempts"))
        .rename({"module_display": "module_code"})
        .sort(["attempts", "module_code"], descending=[True, False])
        if final_segment_modules.height
        else build_final_module_summary(pl.DataFrame())
    )
    final_attempts = (
        int(final_segments.get_column("segment_attempts").sum()) if final_segments.height else 0
    )
    stage_summary = pl.DataFrame(stage_records)
    stage_module_attempts = pl.concat(stage_module_frames, how="vertical_relaxed")
    empty_final_rows = base_scan.select(
        [
            pl.col("user_id"),
            pl.col("created_at"),
            pl.col("work_mode"),
            pl.col("module_code"),
            pl.col("exercise_id"),
            pl.col("attempt_number"),
        ]
    ).limit(0).collect()
    return CohortFilterResult(
        stage_summary=stage_summary,
        stage_module_attempts=stage_module_attempts,
        transition_options=transition_options,
        schema_options=schema_options,
        final_rows=empty_final_rows,
        final_user_paths=final_user_paths,
        final_module_summary=final_module_summary,
        final_attempts=final_attempts,
        baseline_students=baseline_students,
        baseline_attempts=baseline_attempts,
    )


def build_final_module_summary(final_rows: pl.DataFrame) -> pl.DataFrame:
    """Aggregate the final retained attempts by module."""
    if final_rows.height == 0:
        return pl.DataFrame(
            {"module_code": [], "attempts": []},
            schema={"module_code": pl.Utf8, "attempts": pl.Int64},
        )
    return (
        final_rows.with_columns(_module_display_expr())
        .group_by("module_display")
        .agg(pl.len().alias("attempts"))
        .rename({"module_display": "module_code"})
        .sort(["attempts", "module_code"], descending=[True, False])
    )


def build_final_schema_summary(final_user_paths: pl.DataFrame) -> pl.DataFrame:
    """Aggregate the final retained students by exact cleaned schema."""
    if final_user_paths.height == 0:
        return pl.DataFrame(
            {
                "cleaned_schema": [],
                "students": [],
                "attempts": [],
                "student_share": [],
                "attempt_share": [],
            },
            schema={
                "cleaned_schema": pl.Utf8,
                "students": pl.Int64,
                "attempts": pl.Int64,
                "student_share": pl.Float64,
                "attempt_share": pl.Float64,
            },
        )
    total_students = int(final_user_paths.height)
    total_attempts = int(final_user_paths.get_column("retained_attempts").sum())
    return build_schema_options(final_user_paths).with_columns(
        (pl.col("students") / total_students).alias("student_share"),
        (pl.col("attempts") / total_attempts).alias("attempt_share"),
    )


def build_schema_summary_vs_baseline(
    final_user_paths: pl.DataFrame,
    *,
    baseline_students: int,
    baseline_attempts: int,
) -> pl.DataFrame:
    """Aggregate final schemas with shares relative to the full unfiltered slice."""
    if final_user_paths.height == 0:
        return pl.DataFrame(
            {
                "cleaned_schema": [],
                "students": [],
                "attempts": [],
                "student_share": [],
                "attempt_share": [],
            },
            schema={
                "cleaned_schema": pl.Utf8,
                "students": pl.Int64,
                "attempts": pl.Int64,
                "student_share": pl.Float64,
                "attempt_share": pl.Float64,
            },
        )
    return build_schema_options(final_user_paths).with_columns(
        (
            pl.col("students") / baseline_students if baseline_students else pl.lit(0.0)
        ).alias("student_share"),
        (
            pl.col("attempts") / baseline_attempts if baseline_attempts else pl.lit(0.0)
        ).alias("attempt_share"),
    )

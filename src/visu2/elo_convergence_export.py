"""Export helpers for student Elo values after an early convergence prefix."""

from __future__ import annotations

from pathlib import Path

import polars as pl

CONVERGENCE_ELO_COLUMNS: tuple[str, ...] = (
    "user_id",
    "module_id",
    "module_code",
    "module_label",
    "total_attempts_in_module",
    "convergence_attempt",
    "student_elo_at_convergence",
    "has_convergence_elo",
)


def build_student_module_convergence_elo(
    events_path: Path,
    *,
    convergence_attempt: int = 50,
) -> pl.DataFrame:
    """Return one row per student-module with Elo at `convergence_attempt`.

    Student-module trajectories shorter than `convergence_attempt` are retained
    with a null `student_elo_at_convergence`, so downstream analyses can decide
    whether to exclude or separately describe them.
    """
    if convergence_attempt < 1:
        raise ValueError("convergence_attempt must be a positive integer.")
    if not events_path.exists():
        raise FileNotFoundError(f"Batch replay Elo events not found: {events_path}")

    keys = ["user_id", "module_id", "module_code", "module_label"]
    events = pl.scan_parquet(events_path).select(
        keys + ["attempt_ordinal", "student_elo_post"]
    )
    trajectory_summary = events.group_by(keys).agg(
        pl.col("attempt_ordinal").max().alias("total_attempts_in_module")
    )
    convergence_points = (
        events.filter(pl.col("attempt_ordinal") == convergence_attempt)
        .select(keys + [pl.col("student_elo_post").alias("student_elo_at_convergence")])
    )

    return (
        trajectory_summary.join(convergence_points, on=keys, how="left")
        .with_columns(
            pl.lit(convergence_attempt).alias("convergence_attempt"),
            pl.col("student_elo_at_convergence").is_not_null().alias("has_convergence_elo"),
        )
        .select(CONVERGENCE_ELO_COLUMNS)
        .sort(["module_code", "user_id"])
        .collect()
    )

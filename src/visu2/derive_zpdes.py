"""Derived builders specific to ZPDES progression cohort analyses."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import polars as pl

from .config import Settings
from .derive_catalog import catalog_activity_rank_frame
from .derive_common import as_lazy


def _empty_exercise_progression_events() -> pl.DataFrame:
    """Return an empty frame matching the exercise-progression runtime schema."""
    return pl.DataFrame(
        {
            "user_id": [],
            "created_at": [],
            "date_utc": [],
            "module_id": [],
            "module_code": [],
            "module_label": [],
            "objective_id": [],
            "objective_label": [],
            "activity_id": [],
            "activity_label": [],
            "exercise_id": [],
            "work_mode": [],
            "destination_rank": [],
            "exercise_first_attempt_outcome": [],
            "prior_attempt_count": [],
            "prior_before_activity_attempt_count": [],
            "prior_same_activity_attempt_count": [],
            "prior_later_activity_attempt_count": [],
        },
        schema={
            "user_id": pl.Utf8,
            "created_at": pl.Datetime(time_zone="UTC"),
            "date_utc": pl.Date,
            "module_id": pl.Utf8,
            "module_code": pl.Utf8,
            "module_label": pl.Utf8,
            "objective_id": pl.Utf8,
            "objective_label": pl.Utf8,
            "activity_id": pl.Utf8,
            "activity_label": pl.Utf8,
            "exercise_id": pl.Utf8,
            "work_mode": pl.Utf8,
            "destination_rank": pl.Int64,
            "exercise_first_attempt_outcome": pl.Int64,
            "prior_attempt_count": pl.Int64,
            "prior_before_activity_attempt_count": pl.Int64,
            "prior_same_activity_attempt_count": pl.Int64,
            "prior_later_activity_attempt_count": pl.Int64,
        },
    )


def _iter_exercise_progression_rows(source: pl.DataFrame) -> Iterator[dict[str, object]]:
    """Yield exercise-first-attempt progression rows from a sorted attempt stream.

    The input must be sorted by ``user_id``, ``module_code``, ``work_mode``,
    ``created_at``, ``activity_id``, ``exercise_id``, then ``attempt_number``.
    """
    if source.height == 0:
        return

    rows = source.iter_rows(named=False)
    current_group: tuple[str, str, str] | None = None
    current_timestamp: datetime | None = None
    seen_exercises: set[str] = set()
    activity_attempt_counts: dict[str, int] = {}
    rank_attempt_counts: dict[int, int] = {}
    prior_attempt_count = 0
    batch_first_rows: dict[str, tuple[object, ...]] = {}
    batch_rank_counts: dict[int, int] = {}
    batch_activity_counts: dict[str, int] = {}

    def flush_batch() -> Iterator[dict[str, object]]:
        nonlocal prior_attempt_count, batch_first_rows, batch_rank_counts, batch_activity_counts
        if not batch_first_rows:
            return

        for exercise_id, row in batch_first_rows.items():
            if exercise_id in seen_exercises:
                continue
            activity_id = str(row[6])
            destination_rank = int(row[12])
            before_count = sum(
                count for rank, count in rank_attempt_counts.items() if rank < destination_rank
            )
            same_activity_count = activity_attempt_counts.get(activity_id, 0)
            later_count = max(0, prior_attempt_count - before_count - same_activity_count)
            yield {
                "user_id": row[0],
                "module_id": row[1],
                "module_code": row[2],
                "module_label": row[3],
                "objective_id": row[4],
                "objective_label": row[5],
                "activity_id": activity_id,
                "activity_label": row[7],
                "exercise_id": exercise_id,
                "work_mode": row[9],
                "created_at": row[10],
                "date_utc": row[11],
                "destination_rank": destination_rank,
                "exercise_first_attempt_outcome": int(row[13]),
                "prior_attempt_count": prior_attempt_count,
                "prior_before_activity_attempt_count": before_count,
                "prior_same_activity_attempt_count": same_activity_count,
                "prior_later_activity_attempt_count": later_count,
            }

        for rank, count in batch_rank_counts.items():
            rank_attempt_counts[rank] = rank_attempt_counts.get(rank, 0) + count
            prior_attempt_count += count
        for activity_id, count in batch_activity_counts.items():
            activity_attempt_counts[activity_id] = activity_attempt_counts.get(activity_id, 0) + count
        seen_exercises.update(batch_first_rows.keys())
        batch_first_rows = {}
        batch_rank_counts = {}
        batch_activity_counts = {}

    for row in rows:
        group_key = (str(row[0]), str(row[2]), str(row[9]))
        timestamp = row[10]
        if current_group != group_key:
            if current_group is not None:
                yield from flush_batch()
            current_group = group_key
            current_timestamp = timestamp
            seen_exercises = set()
            activity_attempt_counts = {}
            rank_attempt_counts = {}
            prior_attempt_count = 0
            batch_first_rows = {}
            batch_rank_counts = {}
            batch_activity_counts = {}
        elif current_timestamp != timestamp:
            yield from flush_batch()
            current_timestamp = timestamp

        exercise_id = str(row[8])
        activity_id = str(row[6])
        destination_rank = int(row[12])
        batch_first_rows.setdefault(exercise_id, row)
        batch_rank_counts[destination_rank] = batch_rank_counts.get(destination_rank, 0) + 1
        batch_activity_counts[activity_id] = batch_activity_counts.get(activity_id, 0) + 1

    yield from flush_batch()


def build_zpdes_exercise_progression_events_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Build exercise-first-attempt progression events for the ZPDES cohort page."""
    rank_frame = catalog_activity_rank_frame(settings)
    if rank_frame.height == 0:
        return _empty_exercise_progression_events()

    source = (
        as_lazy(fact)
        .select(
            [
                "user_id",
                "module_code",
                "activity_id",
                "exercise_id",
                "work_mode",
                "created_at",
                "date_utc",
                "data_correct",
                "attempt_number",
            ]
        )
        .filter(
            pl.col("user_id").is_not_null(),
            pl.col("module_code").is_not_null(),
            pl.col("activity_id").is_not_null(),
            pl.col("exercise_id").is_not_null(),
            pl.col("work_mode").is_not_null(),
            pl.col("created_at").is_not_null(),
            pl.col("data_correct").is_not_null(),
        )
        .join(rank_frame.lazy(), on=["module_code", "activity_id"], how="inner")
        .with_columns(
            pl.coalesce([pl.col("date_utc"), pl.col("created_at").dt.date()]).alias("date_utc"),
            pl.col("data_correct").cast(pl.Int64).alias("exercise_first_attempt_outcome"),
            pl.col("attempt_number").fill_null(0).cast(pl.Int64).alias("attempt_number"),
        )
        .select(
            [
                "user_id",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "exercise_id",
                "work_mode",
                "created_at",
                "date_utc",
                "destination_rank",
                "exercise_first_attempt_outcome",
                "attempt_number",
            ]
        )
        .sort(
            [
                "user_id",
                "module_code",
                "work_mode",
                "created_at",
                "activity_id",
                "exercise_id",
                "attempt_number",
            ]
        )
        .collect()
    )
    if source.height == 0:
        return _empty_exercise_progression_events()

    rows = list(_iter_exercise_progression_rows(source))
    if not rows:
        return _empty_exercise_progression_events()

    return (
        pl.DataFrame(rows)
        .sort(
            [
                "date_utc",
                "module_code",
                "work_mode",
                "activity_id",
                "created_at",
                "user_id",
                "exercise_id",
            ]
        )
        .with_columns(
            pl.col("created_at").cast(pl.Datetime(time_zone="UTC")),
            pl.col("date_utc").cast(pl.Date),
            pl.col("destination_rank").cast(pl.Int64),
            pl.col("exercise_first_attempt_outcome").cast(pl.Int64),
            pl.col("prior_attempt_count").cast(pl.Int64),
            pl.col("prior_before_activity_attempt_count").cast(pl.Int64),
            pl.col("prior_same_activity_attempt_count").cast(pl.Int64),
            pl.col("prior_later_activity_attempt_count").cast(pl.Int64),
        )
    )

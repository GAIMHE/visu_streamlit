"""Derived builders specific to ZPDES transition-efficiency analyses."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import polars as pl

from .config import Settings
from .derive_catalog import catalog_activity_rank_frame
from .derive_common import as_lazy

ARRIVAL_BUCKET_BEFORE = "before"
ARRIVAL_BUCKET_AFTER_CANDIDATE = "after_candidate"
ARRIVAL_BUCKET_EXCLUDED = "excluded"


def _empty_first_arrival_events() -> pl.DataFrame:
    """Return an empty frame matching the first-arrival runtime schema."""
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
            "work_mode": [],
            "destination_rank": [],
            "first_arrival_outcome": [],
            "prior_attempt_count": [],
            "prior_before_attempt_count": [],
            "prior_later_attempt_count": [],
            "arrival_bucket_base": [],
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
            "work_mode": pl.Utf8,
            "destination_rank": pl.Int64,
            "first_arrival_outcome": pl.Int64,
            "prior_attempt_count": pl.Int64,
            "prior_before_attempt_count": pl.Int64,
            "prior_later_attempt_count": pl.Int64,
            "arrival_bucket_base": pl.Utf8,
        },
    )


def _iter_first_arrival_rows(
    source: pl.DataFrame,
) -> Iterator[dict[str, object]]:
    """Yield first-arrival cohort rows from a sorted attempt stream."""
    if source.height == 0:
        return

    rows = source.iter_rows(named=False)
    current_group: tuple[str, str, str] | None = None
    current_timestamp: datetime | None = None
    seen_activities: set[str] = set()
    counts_by_rank: dict[int, int] = {}
    prior_attempt_count = 0
    batch_first_rows: dict[str, tuple[object, ...]] = {}
    batch_rank_counts: dict[int, int] = {}

    def flush_batch() -> Iterator[dict[str, object]]:
        nonlocal prior_attempt_count, batch_first_rows, batch_rank_counts
        if not batch_first_rows:
            return

        for activity_id, row in batch_first_rows.items():
            if activity_id in seen_activities:
                continue
            rank = int(row[11])
            before_count = sum(count for prev_rank, count in counts_by_rank.items() if prev_rank < rank)
            same_rank_count = counts_by_rank.get(rank, 0)
            later_count = max(0, prior_attempt_count - before_count - same_rank_count)
            if before_count > 0 and later_count == 0:
                bucket = ARRIVAL_BUCKET_BEFORE
            elif later_count > 0:
                bucket = ARRIVAL_BUCKET_AFTER_CANDIDATE
            else:
                bucket = ARRIVAL_BUCKET_EXCLUDED
            yield {
                "user_id": row[0],
                "module_id": row[1],
                "module_code": row[2],
                "module_label": row[3],
                "objective_id": row[4],
                "objective_label": row[5],
                "activity_id": row[6],
                "activity_label": row[7],
                "work_mode": row[8],
                "created_at": row[9],
                "date_utc": row[10],
                "destination_rank": rank,
                "first_arrival_outcome": int(row[12]),
                "prior_attempt_count": prior_attempt_count,
                "prior_before_attempt_count": before_count,
                "prior_later_attempt_count": later_count,
                "arrival_bucket_base": bucket,
            }

        for rank, count in batch_rank_counts.items():
            counts_by_rank[rank] = counts_by_rank.get(rank, 0) + count
            prior_attempt_count += count
        seen_activities.update(batch_first_rows.keys())
        batch_first_rows = {}
        batch_rank_counts = {}

    for row in rows:
        group_key = (str(row[0]), str(row[2]), str(row[8]))
        timestamp = row[9]
        if current_group != group_key:
            if current_group is not None:
                yield from flush_batch()
            current_group = group_key
            current_timestamp = timestamp
            seen_activities = set()
            counts_by_rank = {}
            prior_attempt_count = 0
            batch_first_rows = {}
            batch_rank_counts = {}
        elif current_timestamp != timestamp:
            yield from flush_batch()
            current_timestamp = timestamp

        activity_id = str(row[6])
        rank = int(row[11])
        batch_first_rows.setdefault(activity_id, row)
        batch_rank_counts[rank] = batch_rank_counts.get(rank, 0) + 1

    yield from flush_batch()


def build_zpdes_first_arrival_events_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    """Build first-arrival cohort events for the ZPDES transition-efficiency page."""
    rank_frame = catalog_activity_rank_frame(settings)
    if rank_frame.height == 0:
        return _empty_first_arrival_events()

    source = (
        as_lazy(fact)
        .select(
            [
                "user_id",
                "module_code",
                "activity_id",
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
            pl.col("work_mode").is_not_null(),
            pl.col("created_at").is_not_null(),
            pl.col("data_correct").is_not_null(),
        )
        .join(rank_frame.lazy(), on=["module_code", "activity_id"], how="inner")
        .with_columns(
            pl.coalesce([pl.col("date_utc"), pl.col("created_at").dt.date()]).alias("date_utc"),
            pl.col("data_correct").cast(pl.Int64).alias("first_arrival_outcome"),
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
                "work_mode",
                "created_at",
                "date_utc",
                "destination_rank",
                "first_arrival_outcome",
                "attempt_number",
            ]
        )
        .sort(["user_id", "module_code", "work_mode", "created_at", "activity_id", "attempt_number"])
        .collect()
    )
    if source.height == 0:
        return _empty_first_arrival_events()

    rows = list(_iter_first_arrival_rows(source))
    if not rows:
        return _empty_first_arrival_events()

    return (
        pl.DataFrame(rows)
        .sort(["date_utc", "module_code", "work_mode", "activity_id", "created_at", "user_id"])
        .with_columns(
            pl.col("created_at").cast(pl.Datetime(time_zone="UTC")),
            pl.col("date_utc").cast(pl.Date),
            pl.col("destination_rank").cast(pl.Int64),
            pl.col("first_arrival_outcome").cast(pl.Int64),
            pl.col("prior_attempt_count").cast(pl.Int64),
            pl.col("prior_before_attempt_count").cast(pl.Int64),
            pl.col("prior_later_attempt_count").cast(pl.Int64),
        )
    )

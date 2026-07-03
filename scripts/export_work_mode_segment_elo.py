#!/usr/bin/env python3
"""Export batch Elo from the first window of eligible work-mode segments."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.config import ensure_artifact_directories, get_settings
from visu2.work_mode_segment_elo_export import build_work_mode_segment_elo_export


def _default_output_path(
    source_id: str,
    *,
    first_window_attempts: int,
    min_segment_attempts: int,
) -> Path:
    filename = (
        f"{source_id}_work_mode_segment_elo_first_{first_window_attempts}"
        f"_min_{min_segment_attempts}.parquet"
    )
    return ROOT_DIR / "artifacts" / "reports" / filename


def _write_frame(frame: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        frame.write_parquet(output_path)
        return
    if suffix == ".csv":
        frame.write_csv(output_path)
        return
    raise ValueError("Output path must end with .csv or .parquet.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export one batch-fitted student Elo per contiguous playlist/ZPDES "
            "student-module-work-mode segment, repeated on its initial event window."
        )
    )
    parser.add_argument("--source", default="mia", help="Runtime source id. Defaults to mia.")
    parser.add_argument(
        "--first-window-attempts",
        type=int,
        default=15,
        help="Number of initial retained attempts used in the batch Elo fit.",
    )
    parser.add_argument(
        "--min-segment-attempts",
        type=int,
        default=30,
        help="Minimum retained attempts required for a segment.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output path ending in .parquet or .csv.",
    )
    args = parser.parse_args()

    settings = get_settings(args.source)
    ensure_artifact_directories(settings)
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    exercise_elo_path = settings.artifacts_derived_dir / "agg_exercise_elo.parquet"
    output_path = args.out or _default_output_path(
        settings.source_id,
        first_window_attempts=args.first_window_attempts,
        min_segment_attempts=args.min_segment_attempts,
    )

    frame = build_work_mode_segment_elo_export(
        fact_path,
        exercise_elo_path,
        first_window_attempts=args.first_window_attempts,
        min_segment_attempts=args.min_segment_attempts,
    ).with_columns(pl.lit(settings.source_id).alias("source_id"))
    frame = frame.select(
        ["source_id", *[column for column in frame.columns if column != "source_id"]]
    )
    _write_frame(frame, output_path)

    segments = frame["segment_id"].n_unique() if frame.height else 0
    segments_with_elo = (
        frame.filter(pl.col("has_student_elo"))["segment_id"].n_unique()
        if frame.height
        else 0
    )
    print(f"Exported work-mode segment Elo to: {output_path}")
    print(f"Source: {settings.source_id} ({settings.source_label})")
    print(f"Rows: {frame.height:,}")
    print(f"Segments: {segments:,}")
    print(f"Segments with Elo: {segments_with_elo:,}")
    print(f"Initial attempts per segment: {args.first_window_attempts}")
    print(f"Minimum segment attempts: {args.min_segment_attempts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Export student-module Elo after a fixed convergence attempt."""

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
from visu2.elo_convergence_export import build_student_module_convergence_elo


def _default_output_path(source_id: str, convergence_attempt: int) -> Path:
    return (
        ROOT_DIR
        / "artifacts"
        / "reports"
        / f"{source_id}_student_module_elo_at_attempt_{convergence_attempt}.csv"
    )


def _write_frame(frame: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        frame.write_parquet(output_path)
        return
    if suffix in {"", ".csv"}:
        frame.write_csv(output_path)
        return
    raise ValueError("Output path must end with .csv or .parquet.")


def main() -> int:
    """Export convergence Elo for one source."""
    parser = argparse.ArgumentParser(
        description=(
            "Export one row per student-module with batch replay Elo at a fixed "
            "convergence attempt. Trajectories shorter than the threshold are retained "
            "with an empty Elo value."
        )
    )
    parser.add_argument(
        "--source",
        default="mia",
        help="Runtime source id. Defaults to mia.",
    )
    parser.add_argument(
        "--convergence-attempt",
        type=int,
        default=50,
        help="Attempt ordinal used as the convergence point. Defaults to 50.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Output file path. Defaults to "
            "artifacts/reports/<source>_student_module_elo_at_attempt_<N>.csv."
        ),
    )
    args = parser.parse_args()

    settings = get_settings(args.source)
    ensure_artifact_directories(settings)
    events_path = settings.artifacts_derived_dir / "student_elo_events_batch_replay.parquet"
    output_path = args.out or _default_output_path(settings.source_id, args.convergence_attempt)

    frame = build_student_module_convergence_elo(
        events_path,
        convergence_attempt=args.convergence_attempt,
    ).with_columns(pl.lit(settings.source_id).alias("source_id"))
    frame = frame.select(["source_id", *[col for col in frame.columns if col != "source_id"]])
    _write_frame(frame, output_path)

    n_rows = frame.height
    n_with_elo = frame.filter(pl.col("has_convergence_elo")).height
    n_without_elo = n_rows - n_with_elo
    print(f"Exported convergence Elo to: {output_path}")
    print(f"Source: {settings.source_id} ({settings.source_label})")
    print(f"Convergence attempt: {args.convergence_attempt}")
    print(f"Rows: {n_rows:,}")
    print(f"Rows with Elo at convergence: {n_with_elo:,}")
    print(f"Rows without enough attempts: {n_without_elo:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

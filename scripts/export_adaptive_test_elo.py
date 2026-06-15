#!/usr/bin/env python3
"""Export adaptive-test-based student Elo estimates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.adaptive_test_elo_export import build_student_module_adaptive_test_elo
from visu2.config import ensure_artifact_directories, get_settings


def _default_output_path(source_id: str, *, only_before_practice: bool) -> Path:
    suffix = "before_practice" if only_before_practice else "all"
    return ROOT_DIR / "artifacts" / "reports" / f"{source_id}_adaptive_test_elo_{suffix}.csv"


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
    """Export adaptive-test Elo for one source."""
    parser = argparse.ArgumentParser(
        description=(
            "Export one batch-fitted student Elo per adaptive-test student-module block, "
            "using fixed exercise difficulties from agg_exercise_elo.parquet."
        )
    )
    parser.add_argument(
        "--source",
        default="mia_module1",
        help="Runtime source id. Defaults to mia_module1.",
    )
    parser.add_argument(
        "--only-before-practice",
        action="store_true",
        help=(
            "Use only adaptive-test attempts that occur before first zpdes/playlist "
            "practice in the same module."
        ),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Output file path. Defaults to "
            "artifacts/reports/<source>_adaptive_test_elo_<all|before_practice>.csv."
        ),
    )
    args = parser.parse_args()

    settings = get_settings(args.source)
    ensure_artifact_directories(settings)
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    exercise_elo_path = settings.artifacts_derived_dir / "agg_exercise_elo.parquet"
    output_path = args.out or _default_output_path(
        settings.source_id,
        only_before_practice=args.only_before_practice,
    )

    frame = build_student_module_adaptive_test_elo(
        fact_path,
        exercise_elo_path,
        only_before_practice=args.only_before_practice,
    ).with_columns(pl.lit(settings.source_id).alias("source_id"))
    frame = frame.select(["source_id", *[column for column in frame.columns if column != "source_id"]])
    _write_frame(frame, output_path)

    rows = frame.height
    rows_with_elo = frame.filter(pl.col("has_adaptive_test_elo")).height
    rows_before_practice = frame.filter(pl.col("all_adaptive_test_attempts_before_first_practice")).height
    rows_with_practice = frame.filter(pl.col("has_same_module_practice")).height

    print(f"Exported adaptive-test Elo to: {output_path}")
    print(f"Source: {settings.source_id} ({settings.source_label})")
    print(f"Only before practice: {args.only_before_practice}")
    print(f"Rows: {rows:,}")
    print(f"Rows with adaptive-test Elo: {rows_with_elo:,}")
    print(f"Rows with same-module practice: {rows_with_practice:,}")
    print(f"Rows where all adaptive-test attempts are before first practice: {rows_before_practice:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

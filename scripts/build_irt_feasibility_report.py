#!/usr/bin/env python3
"""CLI entrypoint for the IRT feasibility Markdown report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.config import ensure_artifact_directories, get_settings
from visu2.irt_feasibility import build_irt_feasibility_report


def main() -> int:
    """Build the IRT feasibility report."""
    parser = argparse.ArgumentParser(description="Build the IRT feasibility report and appendices.")
    parser.add_argument(
        "--output-dir",
        default="artifacts/reports",
        help="Relative output directory for the Markdown report and appendices.",
    )
    args = parser.parse_args()

    settings = get_settings()
    ensure_artifact_directories(settings)
    output_dir = settings.root_dir / args.output_dir
    outputs = build_irt_feasibility_report(settings=settings, output_dir=output_dir)

    print("IRT feasibility outputs:")
    print(f"- markdown: {outputs.markdown_path}")
    print(f"- summary: {outputs.summary_path}")
    print(f"- exercise sparsity: {outputs.exercise_sparsity_path}")
    print(f"- overlap tails: {outputs.overlap_tails_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

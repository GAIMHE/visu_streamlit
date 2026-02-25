#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.checks import run_all_checks
from visu2.config import ensure_artifact_directories, get_settings
from visu2.reporting import write_json_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 0 data contracts and consistency checks.")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional report output path. Default: artifacts/reports/consistency_report.json",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any check fails.",
    )
    args = parser.parse_args()

    settings = get_settings()
    ensure_artifact_directories(settings)

    report = run_all_checks(settings)
    output_path = settings.consistency_report_path if args.output is None else settings.root_dir / args.output
    write_json_report(report, output_path)

    print(json.dumps(report, indent=2))
    print(f"\nReport written to: {output_path}")

    if args.strict and report["status"] != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

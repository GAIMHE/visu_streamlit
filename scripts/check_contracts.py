#!/usr/bin/env python3
"""CLI entrypoint for running source-aware data contract and consistency checks."""

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
from visu2.runtime_sources import DEFAULT_SOURCE_ID, list_runtime_sources
from visu2.source_builders import materialize_source_runtime_inputs


def _run_one_source(source_id: str, *, output_override: str | None, strict: bool) -> int:
    settings = get_settings(source_id)
    ensure_artifact_directories(settings)
    materialize_source_runtime_inputs(settings)
    report = run_all_checks(settings)
    output_path = settings.consistency_report_path if output_override is None else settings.root_dir / output_override
    write_json_report(report, output_path)

    print(json.dumps(report, indent=2))
    print(f"\nReport written to: {output_path}")

    if strict and report["status"] != "pass":
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run source-aware data contracts and consistency checks.")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional report output path. Default: source-local artifacts/local/<source>/artifacts/reports/consistency_report.json",
    )
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any check fails.")
    parser.add_argument(
        "--source",
        type=str,
        default=DEFAULT_SOURCE_ID,
        help=f"Runtime source id to check. Default: {DEFAULT_SOURCE_ID}",
    )
    parser.add_argument("--all-sources", action="store_true", help="Run checks for every registered runtime source.")
    args = parser.parse_args()

    source_ids = [spec.source_id for spec in list_runtime_sources()] if args.all_sources else [args.source]
    failures = 0
    for source_id in source_ids:
        failures += _run_one_source(source_id, output_override=args.output, strict=args.strict)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

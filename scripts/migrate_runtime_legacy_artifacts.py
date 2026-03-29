#!/usr/bin/env python3
"""Relocate non-runtime files from source runtime trees into local/legacy roots."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.artifact_migration import migrate_source_artifacts
from visu2.config import get_settings
from visu2.runtime_sources import DEFAULT_SOURCE_ID, get_runtime_source, list_runtime_sources


def _print_summary(source_id: str, summary) -> None:
    moved = summary.count("moved")
    would_move = summary.count("would_move")
    removed_duplicate = summary.count("removed_duplicate")
    would_remove_duplicate = summary.count("would_remove_duplicate")
    removed_runtime_copy = summary.count("removed_runtime_copy")
    would_remove_runtime_copy = summary.count("would_remove_runtime_copy")
    already_migrated = summary.count("already_migrated")
    skipped_existing = summary.count("skipped_existing")
    missing = summary.count("missing")
    print(
        f"[{source_id}] moved={moved} would_move={would_move} "
        f"removed_duplicate={removed_duplicate} would_remove_duplicate={would_remove_duplicate} "
        f"removed_runtime_copy={removed_runtime_copy} would_remove_runtime_copy={would_remove_runtime_copy} "
        f"already_migrated={already_migrated} skipped_existing={skipped_existing} missing={missing}"
    )
    for result in summary.results:
        print(f"- {result.bucket}:{result.status}: {result.relative_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help=f"Optional source id to migrate. Default: all registered sources. Example: {DEFAULT_SOURCE_ID}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would move without modifying any files.",
    )
    args = parser.parse_args()

    source_ids = (
        [get_runtime_source(args.source).source_id]
        if args.source
        else [spec.source_id for spec in list_runtime_sources()]
    )

    for source_id in source_ids:
        settings = get_settings(source_id)
        summary = migrate_source_artifacts(settings, dry_run=args.dry_run)
        _print_summary(source_id, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

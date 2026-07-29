"""Curate source-aligned raw screenshots for the MIAAM V2 release."""

from __future__ import annotations

import argparse
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

EXERCISE_SCREENSHOT_RE = re.compile(
    r"^(?P<exercise_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})(?:_(?P<variant>old|failed))?\.png$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ScreenshotIndex:
    canonical: dict[str, Path]
    old: dict[str, Path]
    failed: dict[str, Path]


@dataclass(frozen=True)
class CuratedSource:
    source: str
    expected: int
    canonical_inputs: int
    old_fallbacks: int
    output_bytes: int


def normalized_ids(frame: pd.DataFrame, source: str) -> set[str]:
    """Return normalized, non-null exercise IDs for one source."""
    values = frame.loc[frame["source"].eq(source), "exercise_id"].dropna()
    return {str(value).lower() for value in values}


def index_screenshots(directory: Path) -> ScreenshotIndex:
    """Index canonical and known variant screenshot names in a directory."""
    indexes: dict[str, dict[str, Path]] = {
        "canonical": {},
        "old": {},
        "failed": {},
    }
    for path in directory.iterdir():
        if not path.is_file():
            continue
        match = EXERCISE_SCREENSHOT_RE.fullmatch(path.name)
        if match is None:
            continue
        exercise_id = match.group("exercise_id").lower()
        variant = match.group("variant") or "canonical"
        if exercise_id in indexes[variant]:
            raise ValueError(
                f"Duplicate {variant} screenshot for {exercise_id}: "
                f"{indexes[variant][exercise_id]} and {path}"
            )
        indexes[variant][exercise_id] = path
    return ScreenshotIndex(**indexes)


def select_screenshot(index: ScreenshotIndex, exercise_id: str) -> tuple[Path, str]:
    """Prefer the canonical screenshot, then a usable `_old` fallback."""
    if exercise_id in index.canonical:
        return index.canonical[exercise_id], "canonical"
    if exercise_id in index.old:
        return index.old[exercise_id], "old"
    if exercise_id in index.failed:
        raise ValueError(
            f"Exercise {exercise_id} has only a `_failed` screenshot and cannot be released."
        )
    raise ValueError(f"No screenshot found for exercise {exercise_id}.")


def curate_source(
    *,
    source: str,
    exercise_ids: set[str],
    input_directory: Path,
    output_directory: Path,
) -> CuratedSource:
    """Copy exactly one canonical-name screenshot per expected exercise."""
    index = index_screenshots(input_directory)
    selections = {
        exercise_id: select_screenshot(index, exercise_id) for exercise_id in sorted(exercise_ids)
    }
    expected_names = {f"{exercise_id}.png" for exercise_id in exercise_ids}

    output_directory.mkdir(parents=True, exist_ok=True)
    existing_names = {path.name for path in output_directory.iterdir() if path.is_file()}
    extras = existing_names - expected_names
    if extras:
        raise ValueError(
            f"{output_directory} contains {len(extras)} unexpected files; refusing to delete them."
        )

    canonical_inputs = 0
    old_fallbacks = 0
    output_bytes = 0
    for exercise_id, (input_path, selection_type) in selections.items():
        output_path = output_directory / f"{exercise_id}.png"
        if not output_path.exists() or (output_path.stat().st_size != input_path.stat().st_size):
            shutil.copy2(input_path, output_path)
        if output_path.stat().st_size == 0:
            raise ValueError(f"Empty screenshot produced: {output_path}")
        output_bytes += output_path.stat().st_size
        canonical_inputs += selection_type == "canonical"
        old_fallbacks += selection_type == "old"

    actual_names = {path.name for path in output_directory.iterdir() if path.is_file()}
    if actual_names != expected_names:
        raise ValueError(
            f"Output mismatch for {source}: expected {len(expected_names)}, "
            f"found {len(actual_names)}."
        )

    return CuratedSource(
        source=source,
        expected=len(exercise_ids),
        canonical_inputs=canonical_inputs,
        old_fallbacks=old_fallbacks,
        output_bytes=output_bytes,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare raw AM, Adaptiv World, and Adaptiv College screenshots.",
    )
    parser.add_argument(
        "--v1-root",
        type=Path,
        default=Path("data_miaam/.cache/miaam_v1_raw"),
        help="Downloaded GAIMHE/MIAAM snapshot root.",
    )
    parser.add_argument(
        "--college-source",
        type=Path,
        default=Path("data_miaam/hf_release/screenshots/raw/adaptiv_college"),
    )
    parser.add_argument(
        "--exercise-table",
        type=Path,
        default=Path("data_miaam/maths_exercises_table.parquet"),
    )
    parser.add_argument(
        "--filtered-attempts",
        type=Path,
        default=Path("data_miaam/maths_data_filtered.parquet"),
    )
    parser.add_argument(
        "--release-root",
        type=Path,
        default=Path("data_miaam/hf_release"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    exercise_table = pd.read_parquet(
        args.exercise_table,
        columns=["exercise_id", "source"],
    )
    filtered_attempts = pd.read_parquet(
        args.filtered_attempts,
        columns=["exercise_id", "source"],
    )

    am_ids = normalized_ids(exercise_table, "am")
    world_ids = normalized_ids(exercise_table, "adaptiv_world")
    college_ids = normalized_ids(filtered_attempts, "adaptiv_college")

    v1_am_directory = args.v1_root / "data" / "screenshots" / "raw" / "am"
    v1_mia_directory = args.v1_root / "data" / "screenshots" / "raw" / "mia"
    output_root = args.release_root / "data" / "screenshots" / "raw"

    # V1 has 13 AM screenshots not used by V2. Intersecting with the available
    # historical files keeps only source-aligned assets while allowing V2-only AM
    # exercises, for which no historical screenshot exists, to remain uncovered.
    available_v1_am_ids = set(index_screenshots(v1_am_directory).canonical)
    am_ids &= available_v1_am_ids

    results = [
        curate_source(
            source="am",
            exercise_ids=am_ids,
            input_directory=v1_am_directory,
            output_directory=output_root / "am",
        ),
        curate_source(
            source="adaptiv_world",
            exercise_ids=world_ids,
            input_directory=v1_mia_directory,
            output_directory=output_root / "adaptiv_world",
        ),
        curate_source(
            source="adaptiv_college",
            exercise_ids=college_ids,
            input_directory=args.college_source,
            output_directory=output_root / "adaptiv_college",
        ),
    ]

    for result in results:
        print(
            f"{result.source}: {result.expected:,} screenshots "
            f"({result.canonical_inputs:,} canonical inputs, "
            f"{result.old_fallbacks:,} `_old` fallbacks, "
            f"{result.output_bytes:,} bytes)"
        )
    print(f"Total: {sum(result.expected for result in results):,} screenshots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

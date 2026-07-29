"""Build the 960-pixel-wide compressed screenshot tree for MIAAM V2."""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from PIL import Image

DEFAULT_SOURCES = ("am", "adaptiv_world", "adaptiv_college")


@dataclass(frozen=True)
class CompressedImage:
    input_bytes: int
    output_bytes: int
    upscaled: bool


@dataclass(frozen=True)
class CompressedSource:
    source: str
    images: int
    upscaled: int
    input_bytes: int
    output_bytes: int


def resized_dimensions(size: tuple[int, int], target_width: int) -> tuple[int, int]:
    """Return the V1-compatible target size at an exact width."""
    width, height = size
    if width <= 0 or height <= 0:
        raise ValueError(f"Invalid source dimensions: {size}")
    if target_width <= 0:
        raise ValueError(f"Target width must be positive, received {target_width}.")
    target_height = max(1, round(height * target_width / width))
    return target_width, target_height


def compress_image(
    input_path: Path,
    output_path: Path,
    *,
    target_width: int,
) -> CompressedImage:
    """Resize one PNG with LANCZOS and replace its output atomically."""
    input_bytes = input_path.stat().st_size
    if input_bytes == 0:
        raise ValueError(f"Input screenshot is empty: {input_path}")

    with Image.open(input_path) as image:
        if image.format != "PNG":
            raise ValueError(f"Expected a PNG image at {input_path}, found {image.format}.")
        image.load()
        source_width = image.width
        target_size = resized_dimensions(image.size, target_width)
        resized = image.resize(target_size, Image.Resampling.LANCZOS)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    temporary_path.unlink(missing_ok=True)
    try:
        resized.save(temporary_path, format="PNG", optimize=True)
        with Image.open(temporary_path) as written:
            if written.format != "PNG":
                raise ValueError(f"Compressed output is not PNG: {temporary_path}")
            if written.size != target_size:
                raise ValueError(
                    f"Compressed output has size {written.size}, expected {target_size}: "
                    f"{temporary_path}"
                )
            written.verify()
        temporary_path.replace(output_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    output_bytes = output_path.stat().st_size
    if output_bytes == 0:
        raise ValueError(f"Compressed screenshot is empty: {output_path}")
    return CompressedImage(
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        upscaled=source_width < target_width,
    )


def relative_png_paths(directory: Path) -> list[Path]:
    """Return sorted PNG paths relative to a source directory."""
    return sorted(
        path.relative_to(directory)
        for path in directory.rglob("*.png")
        if path.is_file()
    )


def compress_source(
    *,
    source: str,
    input_directory: Path,
    output_directory: Path,
    target_width: int,
    workers: int,
) -> CompressedSource:
    """Mirror and compress every PNG for one source without deleting extras."""
    if not input_directory.is_dir():
        raise FileNotFoundError(f"Missing raw screenshot directory: {input_directory}")
    relative_paths = relative_png_paths(input_directory)
    if not relative_paths:
        raise FileNotFoundError(f"No PNG screenshots found in {input_directory}")

    output_directory.mkdir(parents=True, exist_ok=True)
    expected_paths = set(relative_paths)
    existing_paths = {
        path.relative_to(output_directory)
        for path in output_directory.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    }
    unexpected_paths = existing_paths - expected_paths
    if unexpected_paths:
        raise ValueError(
            f"{output_directory} contains {len(unexpected_paths)} unexpected files; "
            "refusing to delete them."
        )

    def process(relative_path: Path) -> CompressedImage:
        return compress_image(
            input_directory / relative_path,
            output_directory / relative_path,
            target_width=target_width,
        )

    results: list[CompressedImage] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for index, result in enumerate(executor.map(process, relative_paths), start=1):
            results.append(result)
            if index % 500 == 0 or index == len(relative_paths):
                print(
                    f"{source}: compressed {index:,}/{len(relative_paths):,}",
                    flush=True,
                )

    actual_paths = set(relative_png_paths(output_directory))
    if actual_paths != expected_paths:
        raise ValueError(
            f"Output mismatch for {source}: expected {len(expected_paths)} PNGs, "
            f"found {len(actual_paths)}."
        )

    return CompressedSource(
        source=source,
        images=len(results),
        upscaled=sum(result.upscaled for result in results),
        input_bytes=sum(result.input_bytes for result in results),
        output_bytes=sum(result.output_bytes for result in results),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a V1-compatible, 960-pixel-wide compressed copy of the "
            "MIAAM V2 raw screenshot hierarchy."
        ),
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("data_miaam/hf_release/data/screenshots/raw"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_miaam/hf_release/data/screenshots/compressed"),
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=DEFAULT_SOURCES,
        default=list(DEFAULT_SOURCES),
    )
    parser.add_argument("--target-width", type=int, default=960)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(4, os.cpu_count() or 1),
        help="Concurrent image workers (default: up to 4).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.workers <= 0:
        raise ValueError(f"Worker count must be positive, received {args.workers}.")

    input_root = args.input_root.resolve()
    output_root = args.output_root.resolve()
    if output_root == input_root or input_root in output_root.parents:
        raise ValueError("Output root must not be the input root or one of its children.")

    results = [
        compress_source(
            source=source,
            input_directory=input_root / source,
            output_directory=output_root / source,
            target_width=args.target_width,
            workers=args.workers,
        )
        for source in args.sources
    ]

    for result in results:
        ratio = result.output_bytes / result.input_bytes
        print(
            f"{result.source}: {result.images:,} PNGs, "
            f"{result.upscaled:,} upscaled, "
            f"{result.input_bytes:,} -> {result.output_bytes:,} bytes "
            f"({ratio:.1%})"
        )
    print(f"Total: {sum(result.images for result in results):,} compressed screenshots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image, ImageChops

from scripts.compress_miaam_v2_screenshots import (
    compress_source,
    resized_dimensions,
)


def write_test_image(path: Path, size: tuple[int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size)
    for x in range(size[0]):
        for y in range(size[1]):
            image.putpixel((x, y), (x % 256, y % 256, (x + y) % 256))
    image.save(path, format="PNG")


def test_resized_dimensions_matches_v1_rounding() -> None:
    assert resized_dimensions((3840, 1698), 960) == (960, 424)
    assert resized_dimensions((5084, 2584), 960) == (960, 488)


def test_compress_source_mirrors_paths_and_uses_lanczos(tmp_path: Path) -> None:
    input_directory = tmp_path / "raw" / "am"
    output_directory = tmp_path / "compressed" / "am"
    input_path = input_directory / "nested" / "exercise.png"
    write_test_image(input_path, (80, 40))
    input_bytes = input_path.read_bytes()

    result = compress_source(
        source="am",
        input_directory=input_directory,
        output_directory=output_directory,
        target_width=40,
        workers=1,
    )

    output_path = output_directory / "nested" / "exercise.png"
    with Image.open(input_path) as source, Image.open(output_path) as compressed:
        expected = source.resize((40, 20), Image.Resampling.LANCZOS)
        assert compressed.size == (40, 20)
        assert ImageChops.difference(compressed, expected).getbbox() is None
    assert input_path.read_bytes() == input_bytes
    assert result.images == 1
    assert result.upscaled == 0


def test_compress_source_upscales_to_exact_contract_width(tmp_path: Path) -> None:
    input_directory = tmp_path / "raw" / "adaptiv_college"
    output_directory = tmp_path / "compressed" / "adaptiv_college"
    write_test_image(input_directory / "exercise.png", (30, 15))

    result = compress_source(
        source="adaptiv_college",
        input_directory=input_directory,
        output_directory=output_directory,
        target_width=60,
        workers=1,
    )

    with Image.open(output_directory / "exercise.png") as compressed:
        assert compressed.size == (60, 30)
    assert result.upscaled == 1


def test_compress_source_refuses_unexpected_output_files(tmp_path: Path) -> None:
    input_directory = tmp_path / "raw" / "am"
    output_directory = tmp_path / "compressed" / "am"
    write_test_image(input_directory / "exercise.png", (80, 40))
    output_directory.mkdir(parents=True)
    (output_directory / "unexpected.txt").write_text("keep me", encoding="utf-8")

    with pytest.raises(ValueError, match="unexpected files"):
        compress_source(
            source="am",
            input_directory=input_directory,
            output_directory=output_directory,
            target_width=40,
            workers=1,
        )

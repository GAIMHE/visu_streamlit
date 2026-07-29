from pathlib import Path

import pytest

from scripts.prepare_miaam_v2_screenshots import (
    curate_source,
    index_screenshots,
    select_screenshot,
)

ID_CANONICAL = "00000000-0000-4000-8000-000000000001"
ID_OLD = "00000000-0000-4000-8000-000000000002"
ID_FAILED = "00000000-0000-4000-8000-000000000003"


def test_curate_source_uses_old_fallback_and_canonical_output_name(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.mkdir()
    (source / f"{ID_CANONICAL}.png").write_bytes(b"canonical")
    (source / f"{ID_OLD}_old.png").write_bytes(b"old")

    result = curate_source(
        source="adaptiv_college",
        exercise_ids={ID_CANONICAL, ID_OLD},
        input_directory=source,
        output_directory=output,
    )

    assert result.expected == 2
    assert result.canonical_inputs == 1
    assert result.old_fallbacks == 1
    assert (output / f"{ID_CANONICAL}.png").read_bytes() == b"canonical"
    assert (output / f"{ID_OLD}.png").read_bytes() == b"old"


def test_failed_only_screenshot_is_rejected(tmp_path: Path) -> None:
    (tmp_path / f"{ID_FAILED}_failed.png").write_bytes(b"failed")
    index = index_screenshots(tmp_path)

    with pytest.raises(ValueError, match="only a `_failed` screenshot"):
        select_screenshot(index, ID_FAILED)


def test_curator_refuses_to_remove_unexpected_output(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.mkdir()
    output.mkdir()
    (source / f"{ID_CANONICAL}.png").write_bytes(b"canonical")
    (output / "unexpected.png").write_bytes(b"preserve")

    with pytest.raises(ValueError, match="refusing to delete"):
        curate_source(
            source="am",
            exercise_ids={ID_CANONICAL},
            input_directory=source,
            output_directory=output,
        )

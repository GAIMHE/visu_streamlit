"""Helpers for incremental source-local derived builds."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from .config import Settings
from .contracts import DERIVED_SCHEMA_VERSION
from .reporting import load_derived_manifest

BUILD_CACHE_VERSION = 1


def _normalized_relative_path(path: Path, *, base_dir: Path) -> str:
    """Return a stable repo-relative path string."""
    return path.resolve().relative_to(base_dir.resolve()).as_posix()


def _path_fingerprint(path: Path, *, base_dir: Path) -> dict[str, int | str]:
    """Describe one input file using cheap metadata suitable for cache reuse."""
    if not path.exists():
        raise FileNotFoundError(f"Required build input is missing: {path}")
    stat = path.stat()
    return {
        "path": _normalized_relative_path(path, base_dir=base_dir),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def build_source_input_snapshot(
    settings: Settings,
    raw_inputs: Mapping[str, Path],
) -> dict[str, dict[str, int | str]]:
    """Capture a cheap fingerprint of the raw inputs that feed one source build."""
    return {
        input_name: _path_fingerprint(settings.root_dir / relative_path, base_dir=settings.root_dir)
        for input_name, relative_path in sorted(raw_inputs.items())
    }


def materialized_input_paths(settings: Settings) -> tuple[Path, ...]:
    """Return the source-local inputs expected after materialization."""
    return (
        settings.parquet_path,
        settings.learning_catalog_path,
        settings.build_zpdes_rules_path,
        settings.exercises_json_path,
    )


def can_reuse_derived_build(
    *,
    settings: Settings,
    source_id: str,
    expected_tables: Sequence[str],
    sample_rows: int | None,
    source_input_snapshot: Mapping[str, object],
) -> tuple[bool, str]:
    """Check whether an existing build can be reused without rebuilding."""
    manifest_path = settings.derived_manifest_path
    if not manifest_path.exists():
        return False, "Derived manifest is missing."

    try:
        manifest = load_derived_manifest(manifest_path)
    except (FileNotFoundError, ValueError) as err:
        return False, f"Derived manifest is unreadable: {err}"

    if manifest.get("source_id") != source_id:
        return False, "Derived manifest source id does not match the requested source."
    if manifest.get("cache_version") != BUILD_CACHE_VERSION:
        return False, "Derived manifest cache version does not match the current builder."
    if manifest.get("schema_version") != DERIVED_SCHEMA_VERSION:
        return False, "Derived manifest schema version does not match the current contracts."

    build_context = manifest.get("build_context") or {}
    if build_context.get("sample_rows") != sample_rows:
        return False, "Derived manifest was built with a different sample_rows setting."

    if manifest.get("source_input_snapshot") != dict(source_input_snapshot):
        return False, "Raw source inputs changed since the last successful build."

    for materialized_input in materialized_input_paths(settings):
        if not materialized_input.exists():
            return False, f"Materialized build input is missing: {materialized_input}"

    table_profiles = manifest.get("tables") or {}
    for table_name in expected_tables:
        if table_name not in table_profiles:
            return False, f"Derived table is missing from the manifest: {table_name}"
        table_path = settings.artifacts_derived_dir / f"{table_name}.parquet"
        if not table_path.exists():
            return False, f"Derived output file is missing: {table_path}"

    return True, "Raw inputs unchanged and all expected runtime outputs already exist."

"""Helpers for relocating non-runtime artifacts out of source runtime trees."""

from __future__ import annotations

import filecmp
import shutil
from dataclasses import dataclass
from pathlib import Path

from .config import Settings, ensure_artifact_directories
from .runtime_sources import RuntimeSourceSpec, get_runtime_source


@dataclass(frozen=True, slots=True)
class ArtifactMoveResult:
    """One attempted artifact relocation result."""

    bucket: str
    relative_path: str
    source_path: Path
    target_path: Path
    status: str


@dataclass(frozen=True, slots=True)
class SourceArtifactMigrationSummary:
    """Migration summary for one source-local runtime tree."""

    source_id: str
    results: tuple[ArtifactMoveResult, ...]

    def count(self, status: str) -> int:
        """Return the number of results with one status."""
        return sum(1 for result in self.results if result.status == status)


def _unique_relative_paths(*groups: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for group in groups:
        for relative_path in group:
            if relative_path in seen:
                continue
            seen.add(relative_path)
            ordered.append(relative_path)
    return tuple(ordered)


def _relocate_one(
    *,
    bucket: str,
    runtime_root: Path,
    target_root: Path,
    relative_path: str,
    dry_run: bool,
) -> ArtifactMoveResult:
    source_path = runtime_root / relative_path
    target_path = target_root / relative_path
    if not source_path.exists():
        status = "already_migrated" if target_path.exists() else "missing"
        return ArtifactMoveResult(bucket, relative_path, source_path, target_path, status)
    if target_path.exists():
        if filecmp.cmp(source_path, target_path, shallow=False):
            if not dry_run:
                source_path.unlink()
            return ArtifactMoveResult(
                bucket,
                relative_path,
                source_path,
                target_path,
                "would_remove_duplicate" if dry_run else "removed_duplicate",
            )
        if not dry_run:
            source_path.unlink()
        return ArtifactMoveResult(
            bucket,
            relative_path,
            source_path,
            target_path,
            "would_remove_runtime_copy" if dry_run else "removed_runtime_copy",
        )
    if not dry_run:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(target_path))
    return ArtifactMoveResult(
        bucket,
        relative_path,
        source_path,
        target_path,
        "would_move" if dry_run else "moved",
    )


def migrate_source_artifacts(
    settings: Settings,
    *,
    source: RuntimeSourceSpec | None = None,
    dry_run: bool = False,
) -> SourceArtifactMigrationSummary:
    """Move local-build and legacy/debug files out of one runtime tree."""
    ensure_artifact_directories(settings)
    source_spec = source or get_runtime_source(settings.source_id)

    local_results = [
        _relocate_one(
            bucket="local_build",
            runtime_root=settings.runtime_root,
            target_root=settings.local_root,
            relative_path=relative_path,
            dry_run=dry_run,
        )
        for relative_path in _unique_relative_paths(source_spec.local_build_relative_paths)
    ]
    legacy_results = [
        _relocate_one(
            bucket="legacy",
            runtime_root=settings.runtime_root,
            target_root=settings.legacy_root,
            relative_path=relative_path,
            dry_run=dry_run,
        )
        for relative_path in _unique_relative_paths(
            source_spec.legacy_relative_paths,
            source_spec.legacy_cleanup_relative_paths,
        )
    ]
    return SourceArtifactMigrationSummary(
        source_id=source_spec.source_id,
        results=tuple([*local_results, *legacy_results]),
    )


__all__ = [
    "ArtifactMoveResult",
    "SourceArtifactMigrationSummary",
    "migrate_source_artifacts",
]

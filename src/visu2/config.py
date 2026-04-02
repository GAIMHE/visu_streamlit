"""
config.py

Define runtime, local-build, and legacy path settings for source-aware workflows.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .runtime_sources import get_runtime_source


@dataclass(frozen=True)
class Settings:
    """Structured settings for one source-aware runtime/build context."""

    root_dir: Path
    data_dir: Path
    resources_dir: Path
    artifacts_dir: Path
    artifacts_derived_dir: Path
    artifacts_reports_dir: Path
    parquet_path: Path
    learning_catalog_path: Path
    zpdes_rules_path: Path
    exercises_json_path: Path
    consistency_report_path: Path
    derived_manifest_path: Path
    runtime_root_dir: Path | None = None
    source_id: str = "main"
    source_label: str = "Adaptiv'Math Main"
    source_description: str = ""
    local_root_dir: Path | None = None
    legacy_root_dir: Path | None = None

    @property
    def runtime_root(self) -> Path:
        """Resolved runtime root for synchronized and built source-local assets."""
        return self.runtime_root_dir or self.root_dir

    @property
    def local_root(self) -> Path:
        """Resolved local-build root for non-runtime inputs and reports."""
        return self.local_root_dir or self.runtime_root

    @property
    def legacy_root(self) -> Path:
        """Resolved legacy/debug root for non-runtime artifacts."""
        return self.legacy_root_dir or self.runtime_root

    @property
    def local_data_dir(self) -> Path:
        """Local-build data directory."""
        return self.local_root / "data"

    @property
    def local_artifacts_dir(self) -> Path:
        """Local-build artifacts directory."""
        return self.local_root / "artifacts"

    @property
    def local_artifacts_reports_dir(self) -> Path:
        """Local-build reports directory."""
        return self.local_artifacts_dir / "reports"

    @property
    def local_zpdes_rules_path(self) -> Path:
        """Local-build ZPDES rules path used by checks/build helpers."""
        return self.local_data_dir / "zpdes_rules.json"

    @property
    def build_zpdes_rules_path(self) -> Path:
        """ZPDES rules path used by local build/check helpers for this source."""
        source = get_runtime_source(self.source_id)
        if "data/zpdes_rules.json" in source.local_build_relative_paths:
            return self.local_zpdes_rules_path
        return self.zpdes_rules_path

    @property
    def legacy_data_dir(self) -> Path:
        """Legacy data directory."""
        return self.legacy_root / "data"

    @property
    def legacy_artifacts_dir(self) -> Path:
        """Legacy artifacts directory."""
        return self.legacy_root / "artifacts"

    @property
    def legacy_artifacts_derived_dir(self) -> Path:
        """Legacy derived-artifact directory."""
        return self.legacy_artifacts_dir / "derived"

    @property
    def legacy_artifacts_reports_dir(self) -> Path:
        """Legacy reports directory."""
        return self.legacy_artifacts_dir / "reports"

    @property
    def hierarchy_resolution_report_path(self) -> Path:
        """Legacy report path for hierarchy-resolution audits."""
        return self.legacy_artifacts_reports_dir / "hierarchy_resolution_report.json"


def get_settings(source_id: str | None = None) -> Settings:
    """Return source-aware settings rooted in runtime/local/legacy trees."""
    root = Path(__file__).resolve().parents[2]
    source = get_runtime_source(source_id)
    runtime_root = source.runtime_root(root)
    local_root = source.local_root(root)
    legacy_root = source.legacy_root(root)
    data_dir = runtime_root / "data"
    artifacts_dir = runtime_root / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"
    local_data_dir = local_root / "data"
    local_reports_dir = local_root / "artifacts" / "reports"

    return Settings(
        root_dir=root,
        runtime_root_dir=runtime_root,
        local_root_dir=local_root,
        legacy_root_dir=legacy_root,
        source_id=source.source_id,
        source_label=source.label,
        source_description=source.description,
        data_dir=data_dir,
        resources_dir=root / "ressources",
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=local_data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=local_data_dir / "exercises.json",
        consistency_report_path=local_reports_dir / "consistency_report.json",
        derived_manifest_path=local_reports_dir / "derived_manifest.json",
    )


def ensure_artifact_directories(settings: Settings) -> None:
    """Ensure runtime, local-build, and legacy directories exist."""
    settings.runtime_root.mkdir(parents=True, exist_ok=True)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_derived_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_reports_dir.mkdir(parents=True, exist_ok=True)

    settings.local_root.mkdir(parents=True, exist_ok=True)
    settings.local_data_dir.mkdir(parents=True, exist_ok=True)
    settings.local_artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.local_artifacts_reports_dir.mkdir(parents=True, exist_ok=True)

    settings.legacy_root.mkdir(parents=True, exist_ok=True)
    settings.legacy_data_dir.mkdir(parents=True, exist_ok=True)
    settings.legacy_artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.legacy_artifacts_derived_dir.mkdir(parents=True, exist_ok=True)
    settings.legacy_artifacts_reports_dir.mkdir(parents=True, exist_ok=True)

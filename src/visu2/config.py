"""
config.py

Define runtime path settings and artifact directory bootstrap helpers.

Dependencies
------------
- dataclasses
- pathlib

Classes
-------
- Settings: Structured model for settings.

Functions
---------
- get_settings: Utility for get settings.
- ensure_artifact_directories: Utility for ensure artifact directories.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .runtime_sources import get_runtime_source


@dataclass(frozen=True)
class Settings:
    """Settings.

Notes
-----
This class is documented in NumPy style for consistency across the codebase.
"""
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

    @property
    def runtime_root(self) -> Path:
        """Resolved runtime root for synchronized and built source-local assets."""
        return self.runtime_root_dir or self.root_dir


def get_settings(source_id: str | None = None) -> Settings:
    """Get settings.


Returns
-------
Settings
        Result produced by this routine.

"""
    root = Path(__file__).resolve().parents[2]
    source = get_runtime_source(source_id)
    runtime_root = source.runtime_root(root)
    data_dir = runtime_root / "data"
    artifacts_dir = runtime_root / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"

    return Settings(
        root_dir=root,
        runtime_root_dir=runtime_root,
        source_id=source.source_id,
        source_label=source.label,
        source_description=source.description,
        data_dir=data_dir,
        resources_dir=root / "ressources",
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def ensure_artifact_directories(settings: Settings) -> None:
    """Ensure artifact directories.

Parameters
----------
settings : Settings
        Input parameter used by this routine.

Returns
-------
None
        Result produced by this routine.

"""
    settings.runtime_root.mkdir(parents=True, exist_ok=True)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_derived_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_reports_dir.mkdir(parents=True, exist_ok=True)

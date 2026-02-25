from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
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


def get_settings() -> Settings:
    root = Path(__file__).resolve().parents[2]
    data_dir = root / "data"
    artifacts_dir = root / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"

    return Settings(
        root_dir=root,
        data_dir=data_dir,
        resources_dir=root / "ressources",
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=data_dir / "adaptiv_math_history.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def ensure_artifact_directories(settings: Settings) -> None:
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_derived_dir.mkdir(parents=True, exist_ok=True)
    settings.artifacts_reports_dir.mkdir(parents=True, exist_ok=True)

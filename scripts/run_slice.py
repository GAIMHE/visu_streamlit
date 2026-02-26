#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.config import get_settings


def _required_artifacts(settings_root: Path) -> list[Path]:
    return [
        settings_root / "artifacts" / "reports" / "consistency_report.json",
        settings_root / "artifacts" / "derived" / "fact_attempt_core.parquet",
        settings_root / "artifacts" / "derived" / "agg_activity_daily.parquet",
        settings_root / "artifacts" / "derived" / "agg_objective_daily.parquet",
        settings_root / "artifacts" / "derived" / "agg_student_module_progress.parquet",
        settings_root / "artifacts" / "derived" / "agg_transition_edges.parquet",
        settings_root / "artifacts" / "derived" / "agg_module_usage_daily.parquet",
        settings_root / "artifacts" / "derived" / "agg_playlist_module_usage.parquet",
        settings_root / "artifacts" / "derived" / "agg_module_activity_usage.parquet",
        settings_root / "artifacts" / "derived" / "agg_exercise_daily.parquet",
    ]


def _smoke_import(app_path: Path) -> None:
    spec = importlib.util.spec_from_file_location("streamlit_app", app_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to create import spec for {app_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run or smoke-check the Streamlit thin slice app.")
    parser.add_argument(
        "--port",
        type=int,
        default=8501,
        help="Streamlit port.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Only validate artifact presence and import app module.",
    )
    args = parser.parse_args()

    settings = get_settings()
    app_path = settings.root_dir / "apps" / "learning_analytics_overview.py"

    if not app_path.exists():
        print(f"Missing app file: {app_path}")
        return 1

    missing = [path for path in _required_artifacts(settings.root_dir) if not path.exists()]
    if missing:
        print("Missing required artifacts:")
        for path in missing:
            print(f"- {path}")
        print("Run: python scripts/build_derived.py")
        return 1

    _smoke_import(app_path)
    if args.smoke:
        print("Smoke check successful.")
        return 0

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.headless=true",
        f"--server.port={args.port}",
    ]
    return subprocess.call(cmd, cwd=settings.root_dir)


if __name__ == "__main__":
    raise SystemExit(main())

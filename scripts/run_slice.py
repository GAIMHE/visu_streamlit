#!/usr/bin/env python3
"""CLI entrypoint for running or smoke-checking the source-aware Streamlit app."""

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
from visu2.runtime_sources import DEFAULT_SOURCE_ID, get_runtime_source


def _required_artifacts(source_id: str) -> list[Path]:
    settings = get_settings(source_id)
    source = get_runtime_source(source_id)
    return [settings.runtime_root / rel_path for rel_path in source.runtime_relative_paths]


def _smoke_import(app_path: Path) -> None:
    spec = importlib.util.spec_from_file_location("streamlit_app", app_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to create import spec for {app_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run or smoke-check the Streamlit app.")
    parser.add_argument("--port", type=int, default=8501, help="Streamlit port.")
    parser.add_argument("--smoke", action="store_true", help="Only validate artifact presence and import app module.")
    parser.add_argument(
        "--source",
        type=str,
        default=DEFAULT_SOURCE_ID,
        help=f"Runtime source id to validate. Default: {DEFAULT_SOURCE_ID}",
    )
    args = parser.parse_args()

    settings = get_settings(args.source)
    app_path = settings.root_dir / "apps" / "learning_analytics_overview.py"

    if not app_path.exists():
        print(f"Missing app file: {app_path}")
        return 1

    missing = [path for path in _required_artifacts(args.source) if not path.exists()]
    if missing:
        print(f"Missing required runtime files for source '{args.source}':")
        for path in missing:
            print(f"- {path}")
        print(f"Run: uv run python scripts/build_derived.py --source {args.source}")
        return 1

    if args.smoke:
        _smoke_import(app_path)
        print(f"Smoke check successful for source '{args.source}'.")
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

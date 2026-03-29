from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_run_slice_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "run_slice.py"
    spec = importlib.util.spec_from_file_location("run_slice_module", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_slice_smoke_mode_imports_but_does_not_launch(monkeypatch) -> None:
    module = _load_run_slice_module()
    smoke_calls: list[Path] = []
    launch_calls: list[list[str]] = []

    monkeypatch.setattr(module, "_required_artifacts", lambda source_id: [])
    monkeypatch.setattr(module, "_smoke_import", lambda app_path: smoke_calls.append(app_path))
    monkeypatch.setattr(
        module.subprocess,
        "call",
        lambda cmd, cwd=None: launch_calls.append(cmd) or 0,
    )
    monkeypatch.setattr(sys, "argv", ["run_slice.py", "--smoke"])

    assert module.main() == 0
    assert len(smoke_calls) == 1
    assert launch_calls == []


def test_run_slice_launch_mode_skips_bare_import(monkeypatch) -> None:
    module = _load_run_slice_module()
    smoke_calls: list[Path] = []
    launch_calls: list[list[str]] = []

    monkeypatch.setattr(module, "_required_artifacts", lambda source_id: [])
    monkeypatch.setattr(module, "_smoke_import", lambda app_path: smoke_calls.append(app_path))
    monkeypatch.setattr(
        module.subprocess,
        "call",
        lambda cmd, cwd=None: launch_calls.append(cmd) or 0,
    )
    monkeypatch.setattr(sys, "argv", ["run_slice.py", "--port", "8601"])

    assert module.main() == 0
    assert smoke_calls == []
    assert len(launch_calls) == 1

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from visu2.elo_convergence_export import build_student_module_convergence_elo


def _write_events(path: Path) -> None:
    rows: list[dict[str, object]] = []
    for attempt in range(1, 81):
        rows.append(
            {
                "user_id": "u1",
                "module_id": "module-1",
                "module_code": "M1",
                "module_label": "Module 1",
                "attempt_ordinal": attempt,
                "student_elo_post": 1500.0 + attempt,
            }
        )
    for attempt in range(1, 80):
        rows.append(
            {
                "user_id": "u2",
                "module_id": "module-1",
                "module_code": "M1",
                "module_label": "Module 1",
                "attempt_ordinal": attempt,
                "student_elo_post": 1400.0 + attempt,
            }
        )
    for attempt in range(1, 82):
        rows.append(
            {
                "user_id": "u1",
                "module_id": "module-2",
                "module_code": "M2",
                "module_label": "Module 2",
                "attempt_ordinal": attempt,
                "student_elo_post": 1600.0 + attempt,
            }
        )
    pl.DataFrame(rows).write_parquet(path)


def test_build_student_module_convergence_elo_keeps_short_trajectories(tmp_path: Path) -> None:
    events_path = tmp_path / "student_elo_events_batch_replay.parquet"
    _write_events(events_path)

    frame = build_student_module_convergence_elo(events_path, convergence_attempt=80)
    rows = {
        (row["user_id"], row["module_code"]): row
        for row in frame.to_dicts()
    }

    assert frame.height == 3
    assert rows[("u1", "M1")]["total_attempts_in_module"] == 80
    assert rows[("u1", "M1")]["student_elo_at_convergence"] == 1580.0
    assert rows[("u1", "M1")]["has_convergence_elo"] is True

    assert rows[("u2", "M1")]["total_attempts_in_module"] == 79
    assert rows[("u2", "M1")]["student_elo_at_convergence"] is None
    assert rows[("u2", "M1")]["has_convergence_elo"] is False

    assert rows[("u1", "M2")]["total_attempts_in_module"] == 81
    assert rows[("u1", "M2")]["student_elo_at_convergence"] == 1680.0


def test_build_student_module_convergence_elo_rejects_non_positive_attempt(tmp_path: Path) -> None:
    events_path = tmp_path / "events.parquet"
    _write_events(events_path)

    with pytest.raises(ValueError, match="positive integer"):
        build_student_module_convergence_elo(events_path, convergence_attempt=0)

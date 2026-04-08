from __future__ import annotations

import polars as pl

from visu2.student_picker_state import (
    initialize_student_picker_state,
    preferred_target_attempts,
)


def _student_summary_fixture() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "total_attempts": [40, 120, 260],
        }
    )


def test_preferred_target_attempts_uses_selected_student_attempt_count() -> None:
    student_summary = _student_summary_fixture()

    assert preferred_target_attempts(student_summary, "u2", 80) == 120
    assert preferred_target_attempts(student_summary, "missing", 80) == 80
    assert preferred_target_attempts(student_summary, None, 80) == 80


def test_initialize_student_picker_state_seeds_query_context_once() -> None:
    state: dict[str, object] = {"manual_key": "u-old"}

    initialize_student_picker_state(
        state,
        context_key="context_key",
        current_context=("main", "Sequential Replay Elo"),
        target_key="target_key",
        manual_key="manual_key",
        preferred_key="preferred_key",
        default_target=120,
        preferred_student_id="u2",
        min_attempts=20,
        max_attempts=300,
    )

    assert state["context_key"] == ("main", "Sequential Replay Elo")
    assert state["target_key"] == 120
    assert state["preferred_key"] == "u2"
    assert "manual_key" not in state

    state["target_key"] = 200
    state["manual_key"] = "typed"

    initialize_student_picker_state(
        state,
        context_key="context_key",
        current_context=("main", "Sequential Replay Elo"),
        target_key="target_key",
        manual_key="manual_key",
        preferred_key="preferred_key",
        default_target=60,
        preferred_student_id="u1",
        min_attempts=20,
        max_attempts=300,
    )

    assert state["target_key"] == 200
    assert state["manual_key"] == "typed"
    assert state["preferred_key"] == "u2"

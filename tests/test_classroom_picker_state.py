from __future__ import annotations

import polars as pl

from visu2.classroom_picker_state import (
    initialize_classroom_picker_state,
    preferred_classroom_option_index,
    preferred_target_students,
)


def _profiles_fixture() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mode_scope": ["zpdes", "zpdes", "zpdes"],
            "classroom_id": ["c1", "c2", "c3"],
            "students": [12, 24, 36],
            "attempts": [100, 200, 300],
        }
    )


def test_preferred_target_students_uses_selected_classroom_size() -> None:
    profiles = _profiles_fixture()

    assert preferred_target_students(profiles, "c2", 18) == 24
    assert preferred_target_students(profiles, "missing", 18) == 18
    assert preferred_target_students(profiles, None, 18) == 18


def test_initialize_classroom_picker_state_seeds_query_context_once() -> None:
    state: dict[str, object] = {
        "manual_key": "c-old",
        "selectbox_key": "Old option",
    }

    initialize_classroom_picker_state(
        state,
        context_key="context_key",
        current_context=("main", "zpdes"),
        target_key="target_key",
        manual_key="manual_key",
        selectbox_key="selectbox_key",
        preferred_key="preferred_key",
        default_target=24,
        preferred_classroom_id="c2",
        min_students=10,
        max_students=40,
    )

    assert state["context_key"] == ("main", "zpdes")
    assert state["target_key"] == 24
    assert state["preferred_key"] == "c2"
    assert "manual_key" not in state
    assert "selectbox_key" not in state

    state["target_key"] = 31
    state["manual_key"] = "typed"
    state["selectbox_key"] = "Current option"

    initialize_classroom_picker_state(
        state,
        context_key="context_key",
        current_context=("main", "zpdes"),
        target_key="target_key",
        manual_key="manual_key",
        selectbox_key="selectbox_key",
        preferred_key="preferred_key",
        default_target=12,
        preferred_classroom_id="c1",
        min_students=10,
        max_students=40,
    )

    assert state["target_key"] == 31
    assert state["manual_key"] == "typed"
    assert state["selectbox_key"] == "Current option"
    assert state["preferred_key"] == "c2"


def test_preferred_classroom_option_index_matches_preferred_id() -> None:
    option_map = {
        "Classroom c1": "c1",
        "Classroom c2": "c2",
        "Classroom c3": "c3",
    }

    assert preferred_classroom_option_index(option_map, "c2") == 1
    assert preferred_classroom_option_index(option_map, "missing") == 0
    assert preferred_classroom_option_index(option_map, None) == 0

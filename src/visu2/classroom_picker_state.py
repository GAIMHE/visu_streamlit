"""State helpers for classroom picker controls on classroom pages."""

from __future__ import annotations

from collections.abc import MutableMapping

import polars as pl


def _clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, int(value)))


def preferred_target_students(
    scoped_profiles: pl.DataFrame,
    preferred_classroom_id: str | None,
    fallback_target: int,
) -> int:
    """Return a target size seeded from a preferred classroom when available."""

    if preferred_classroom_id:
        matching_rows = scoped_profiles.filter(
            pl.col("classroom_id") == preferred_classroom_id
        )
        if matching_rows.height > 0:
            student_count = matching_rows["students"][0]
            if student_count is not None:
                return int(student_count)
    return int(fallback_target)


def initialize_classroom_picker_state(
    state: MutableMapping[str, object],
    *,
    context_key: str,
    current_context: tuple[str, str],
    target_key: str,
    manual_key: str,
    selectbox_key: str,
    preferred_key: str,
    default_target: int,
    preferred_classroom_id: str | None,
    min_students: int,
    max_students: int,
) -> None:
    """Initialize picker state once per page context."""

    min_value = max(1, int(min_students))
    max_value = max(min_value, int(max_students))
    clamped_default = _clamp_int(default_target, min_value, max_value)

    if state.get(context_key) != current_context:
        state[context_key] = current_context
        state[target_key] = clamped_default
        if preferred_classroom_id:
            state[preferred_key] = preferred_classroom_id
        else:
            state.pop(preferred_key, None)
        state.pop(manual_key, None)
        state.pop(selectbox_key, None)
        return

    current_target = state.get(target_key, clamped_default)
    state[target_key] = _clamp_int(int(current_target), min_value, max_value)


def preferred_classroom_option_index(
    option_map: dict[str, str],
    preferred_classroom_id: str | None,
) -> int:
    """Return the selectbox index for a preferred classroom id when present."""

    if not preferred_classroom_id:
        return 0
    for index, classroom_id in enumerate(option_map.values()):
        if classroom_id == preferred_classroom_id:
            return index
    return 0

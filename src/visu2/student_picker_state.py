"""State helpers for student picker controls on student pages."""

from __future__ import annotations

from collections.abc import MutableMapping

import polars as pl


def _clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, int(value)))


def preferred_target_attempts(
    student_summary: pl.DataFrame,
    preferred_student_id: str | None,
    fallback_target: int,
) -> int:
    """Return a target attempt count seeded from a preferred student when available."""

    if preferred_student_id:
        matching_rows = student_summary.filter(pl.col("user_id") == preferred_student_id)
        if matching_rows.height > 0:
            total_attempts = matching_rows["total_attempts"][0]
            if total_attempts is not None:
                return int(total_attempts)
    return int(fallback_target)


def initialize_student_picker_state(
    state: MutableMapping[str, object],
    *,
    context_key: str,
    current_context: tuple[str, ...],
    target_key: str,
    manual_key: str,
    preferred_key: str,
    default_target: int,
    preferred_student_id: str | None,
    min_attempts: int,
    max_attempts: int,
) -> None:
    """Initialize picker state once per page context."""

    min_value = max(1, int(min_attempts))
    max_value = max(min_value, int(max_attempts))
    clamped_default = _clamp_int(default_target, min_value, max_value)

    if state.get(context_key) != current_context:
        state[context_key] = current_context
        state[target_key] = clamped_default
        if preferred_student_id:
            state[preferred_key] = preferred_student_id
        else:
            state.pop(preferred_key, None)
        state.pop(manual_key, None)
        return

    current_target = state.get(target_key, clamped_default)
    state[target_key] = _clamp_int(int(current_target), min_value, max_value)

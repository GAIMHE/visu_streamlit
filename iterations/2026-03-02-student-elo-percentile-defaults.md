## Context / Scope

Adjust the default student selection on the `Student Elo Evolution` page so the percentile-based rule is applied consistently, including when only one or two students are eligible after filtering.

## Main Changes

- Removed the early-return shortcut in `src/visu2/student_elo.py` that bypassed percentile selection for small eligible sets.
- Kept the existing percentile targets (`75th`, `50th`, then `25th`) as the default-selection rule.
- Updated `tests/test_student_elo_page_logic.py`:
  - renamed the selector test to reflect percentile semantics
  - added a single-eligible-student edge-case test

## Important Decisions / Rationale

- The percentile rule was already the intended behavior for larger eligible sets; the inconsistency only appeared when the eligible pool was small.
- Removing the shortcut is simpler and more reliable than maintaining separate branches for small and large populations.
- This keeps `Student 1` / `Student 2` defaults representative rather than always defaulting to the heaviest users.

## Follow-Up Actions

- If the selected defaults still feel too extreme in practice, tune the target percentiles (for example `60th` + `30th`) rather than reintroducing volume-based defaults.

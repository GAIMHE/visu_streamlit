# Iteration: 2026-02-18 - Exercise Instruction from Drilldown Row Selection

## Context/Scope
- Improve Exercise Drilldown usability on the matrix page without changing derived schemas.
- Keep compact exercise table display (short IDs), but allow access to instruction text.

## Main Changes
- `apps/pages/2_objective_activity_matrix.py`
  - Added row selection on the exercise drilldown table (`st.dataframe(..., on_select="rerun", selection_mode="single-row")`).
  - Added instruction panel below the table:
    - Displays selected exercise short ID.
    - Displays instruction text from `exercise_label` for selected row.
  - Kept table visible columns compact (`exercise_short_id` + metrics), instruction shown only on click.
  - Added empty-state guidance when no row is selected.

## Important Decisions and Rationale
- Reused existing `exercise_label` (derived from `exercises.json` instruction) instead of adding new artifacts/columns.
- Kept instruction reveal as explicit row click to avoid UI clutter in the table itself.

## Validation
- `uv run python -m py_compile apps/pages/2_objective_activity_matrix.py` passed.
- `uv run pytest -q tests/test_objective_activity_matrix.py` passed.
- `uv run python scripts/run_slice.py --smoke` passed.

## Follow-up Actions
- If needed, add truncation + “expand” affordance for very long instruction text blocks.

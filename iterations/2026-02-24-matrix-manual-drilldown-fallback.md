# 2026-02-24 - Matrix manual drilldown fallback

## Context / scope
- User reported matrix click selection still empty for `exercise_balanced_success_rate`.
- Scope: keep click flow but add robust fallback so drilldown remains usable.

## Main changes made
- Updated `apps/pages/2_objective_activity_matrix.py`:
  - increased selector hit area (`selector_marker_size`) from fixed size to dynamic size based on row density.
  - added `metric` to selection context reset key.
  - added a **Manual drilldown selection** expander when no click selection is available:
    - objective selector
    - activity selector
    - load button to populate drilldown state.

## Important decisions and rationale
- Preserved existing click-based UX.
- Added deterministic manual path as reliability fallback for Plotly event payload inconsistencies.
- Kept fallback scoped to the “no selected cell” state so normal click flow is unaffected.

## Validation results
- `uv run pytest tests/test_objective_activity_matrix.py -q` -> pass
- `uv run pytest -q` -> pass
- `uv run python scripts/run_slice.py --smoke` -> pass
- Code-reviewer scripts executed:
  - `code_quality_checker.py`
  - `review_report_generator.py`

## Follow-up actions
- If needed, we can hide the manual selector behind a sidebar toggle to keep the page cleaner once click behavior is fully stable across environments.

# 2026-02-24 - Matrix selection fallback fix

## Context / scope
- User reported matrix cell selection worked on `Attempts` but not reliably on other metrics.
- Scope limited to matrix page interaction reliability.

## Main changes made
- Updated `apps/pages/2_objective_activity_matrix.py`:
  - extended `_extract_selected_cell(...)` with a fallback path using clicked `(y, x)` axis coordinates.
  - added `cell_lookup` map from rendered axis labels to stable `(objective_id, activity_id)`.
  - selection now resolves even when Plotly selection event does not carry usable `customdata`.

## Important decisions and rationale
- Kept existing click-based interaction model and overlay selector trace.
- Added robust fallback instead of replacing current extraction path, to preserve existing behavior and improve compatibility across metrics/traces.

## Validation results
- `uv run pytest tests/test_objective_activity_matrix.py -q` -> pass
- `uv run pytest -q` -> pass
- `uv run python scripts/run_slice.py --smoke` -> pass
- Code reviewer workflow executed:
  - `code_quality_checker.py`
  - `review_report_generator.py`

## Follow-up actions
- Optional: add a dedicated unit test for `_extract_selected_cell` event variants if this page gets more interaction modes.

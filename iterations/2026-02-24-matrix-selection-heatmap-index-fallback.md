# 2026-02-24 - Matrix selection heatmap-index fallback

## Context / scope
- User reported matrix drilldown still failed for `exercise_balanced_success_rate` after initial fallback.
- Scope: strengthen click-event parsing on matrix page only.

## Main changes made
- Updated `apps/pages/2_objective_activity_matrix.py` in `_extract_selected_cell(...)`:
  - added handling for heatmap-style `point_index=[row,col]`.
  - added fallback conversion for numeric `x`/`y` indices to axis labels.
  - reused existing `cell_lookup` map to resolve robustly to `(objective_id, activity_id)`.
- Updated extraction call to pass `x_labels` and `y_labels`.

## Important decisions and rationale
- Kept existing extraction flow first (`customdata`), then index/axis fallbacks.
- This avoids regressions while supporting more Plotly event payload variants.

## Validation results
- `uv run pytest tests/test_objective_activity_matrix.py -q` -> pass
- `uv run pytest -q` -> pass
- `uv run python scripts/run_slice.py --smoke` -> pass
- Code reviewer workflow executed:
  - `code_quality_checker.py`
  - `review_report_generator.py`

## Follow-up actions
- If any remaining edge case appears, capture one `Debug matrix selection` payload to add an explicit parser branch for that exact event shape.

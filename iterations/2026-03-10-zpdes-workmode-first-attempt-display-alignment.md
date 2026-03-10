# ZPDES Work-Mode First-Attempt Display Alignment

## Context
- The ZPDES transition-efficiency page showed `First-attempt success` from the all-mode activity aggregate.
- Hover cohorts were filtered to the selected work mode.
- This produced misleading comparisons, such as a low global node metric alongside strong `before` and `in-activity` cohort rates.

## Main Changes Made
- Changed the page-6 `First-attempt success` node metric to use the selected work mode's exercise-first-attempt events.
- Added hover fields for:
  - selected-mode first-attempt success
  - selected-mode first-attempt event count
- Kept Elo-based node coloring unchanged.
- Handled missing selected-mode node metrics safely in the Plotly color array.

## Important Decisions and Rationale
- The node metric and the hover cohorts must come from the same event pool to be comparable.
- The selected work-mode event count is shown explicitly so the user can see the denominator behind the displayed rate.
- The activity-level all-mode aggregate remains available elsewhere in the project, but not as the page-6 first-attempt node metric.

## Validation
- `uv run ruff check src/visu2/zpdes_transition_efficiency.py apps/pages/6_zpdes_transition_efficiency.py tests/test_zpdes_transition_efficiency.py`
- `uv run pytest tests/test_zpdes_transition_efficiency.py -q`

## Follow-Up Actions
- If needed later, add an explicit "all modes" baseline to hover for comparison with the selected work mode.

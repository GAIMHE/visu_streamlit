# Iteration: 2026-02-18 - Matrix Click-Only Drilldown Interaction

## Context/Scope
- Remove the tiny rectangle selection workflow on the Objective-Activity matrix page.
- Keep the existing heatmap visual style and hover readability.
- Make Exercise Drilldown open from direct single-click on a matrix cell.

## Main Changes
- `apps/pages/2_objective_activity_matrix.py`
  - Changed matrix interaction from drag selection to click-only behavior:
    - `dragmode` set to `False`.
    - `selection_mode` restricted to `("points",)`.
  - Removed box/lasso tools from modebar to align UI with click-only interaction:
    - `config.modeBarButtonsToRemove = ["select2d", "lasso2d"]`.
  - Kept existing selector overlay trace and drilldown session-state logic unchanged.
  - Kept hover tooltip content unchanged.

## Important Decisions and Rationale
- Hover events are not used as the drilldown trigger because Streamlit `st.plotly_chart(..., on_select="rerun")` surfaces selection events, not hover events, for callback state.
- Point selection is the simplest reliable trigger that preserves current chart rendering and avoids extra interaction friction.

## Validation
- `uv run python -m py_compile apps/pages/2_objective_activity_matrix.py` passed.
- `uv run pytest -q tests/test_objective_activity_matrix.py` passed.
- `uv run python scripts/run_slice.py --smoke` passed.

## Follow-up Actions
- If needed, add a small UI hint near the matrix title: "Single-click a cell to open exercise drilldown."

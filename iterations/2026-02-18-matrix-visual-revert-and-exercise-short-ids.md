# 2026-02-18 - Matrix Visual Revert and Exercise Short IDs

## Context and Scope
Follow-up UX fix after drilldown activation: restore matrix appearance closer to the original heatmap while keeping click-based selection working, and simplify exercise drilldown labels to short IDs.

## Main Changes Made
- Updated `apps/pages/2_objective_activity_matrix.py`:
  - restored heatmap-style rendering (`go.Heatmap`) for the matrix look
  - kept click selection via overlay selector trace
  - retained robust selection parsing fallback logic
  - drilldown exercise chart/table now use short exercise IDs (first 8 chars) instead of repeated instruction labels
- Updated `src/visu2/objective_activity_matrix.py`:
  - added `exercise_short_id` in drilldown payload
  - set `exercise_display_label` to short ID for concise visuals

## Important Decisions and Rationale
- Kept two-layer approach (display + selector) to balance reliable click events with the preferred heatmap appearance.
- Kept full exercise ID available in hover/details (when enabled) while default display uses short ID to reduce clutter.

## Follow-up Actions
- If any browser still fails click capture, consider optional "selection mode" toggle (heatmap overlay vs fully clickable scatter) for compatibility.

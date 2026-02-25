# 2026-02-17 - Objective Activity Matrix Heatmap

## Context and Scope
This iteration adds a new module-level matrix visualization page requested for faster structural reading of objective/activity performance. Scope is limited to one visualization family: objectives on Y, objective-local activity positions (A1..An) on X, and selectable metric coloring/annotation.

## Main Changes
- Added `src/visu2/objective_activity_matrix.py`.
- Added `apps/pages/2_objective_activity_matrix.py`.
- Added tests in `tests/test_objective_activity_matrix.py`.
- Updated `README.md` with a dedicated "Objective-Activity Matrix Heatmap" section.

## Important Decisions and Rationale
- Kept module-level only in this iteration to match the requested scope and avoid introducing extra interaction complexity.
- Implemented summary-first ordering for module/objective/activity when entities exist in `summary.json`, with deterministic lexical fallback for non-summary entities.
- Preserved weighted metric aggregation for rates (`success_rate`, `repeat_attempt_rate`) using attempts as weights to avoid bias from small-day rows.
- Added label fallback from IDs for degraded artifacts so the page remains usable even when optional label columns are absent.
- Used ragged matrix layout with local `A#` columns to reflect variable activity counts per objective.

## Follow-up Actions
- If needed, add optional objective-level filtering for large modules with many rows.
- Consider an alternate normalized color mode (per-objective z-score) in a later iteration for cross-row contrast.

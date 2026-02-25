# Iteration Log - Page 1 KPI Refresh and Work-Mode Analytics

## Date
2026-02-19

## Context / Scope
This iteration updates the first overview page (`apps/streamlit_app.py`) to improve top-level KPI readability and replace the first chart section with work-mode focused analytics.

## Main Changes Made
- Updated KPI cards on page 1:
  - kept `Attempts`
  - kept `Unique Students`
  - added `Unique Exercises`
  - kept `Success Rate`
  - removed `Median Duration (s)`
- Replaced the former `Activity Trend` block with a new `Work Mode Performance` section.
- Added work-mode selector (`Work modes shown`) with default = all observed work modes after filters.
- Added success-rate numeric table by work mode (percentages, no chart).
- Added `Work Mode Footprint and Depth` analytics:
  - grouped bar chart for width (`unique_modules_explored`, `unique_objectives_explored`, `unique_activities_explored`)
  - summary table with attempts, students, width metrics, median attempts per activity, repeat-attempt rate, success rate
- Added empty-state handling for missing filtered data and no selected work modes.
- Updated `README.md` to document page-1 KPI set and work-mode section.

## Important Decisions and Rationale
- Metric source remains `fact_attempt_core` with current page filters for deterministic consistency with other page-1 metrics.
- Success-rate output simplified to a numeric table to reduce visual noise and make mode comparison direct.
- Depth is operationalized as `median_attempts_per_activity` to avoid mixing activity breadth and raw attempt volume.
- Repeat-attempt rate remains `mean(attempt_number > 1)` to preserve continuity with existing app semantics.
- Raw parquet validation for `work_mode='playlist'` confirmed activity/objective are placeholder `"None"` strings in this source slice.

## Validation / Quality Checks
- Compiled updated page module:
  - `uv run python -m py_compile apps/streamlit_app.py`
- Smoke run:
  - `uv run python scripts/run_slice.py --smoke` (pass)
- Test suite:
  - `uv run pytest -q` (pass)
- Mandatory code-reviewer workflow:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Optionally add a mode-level date trend toggle (summary vs trend) if future analysis requires temporal comparison by work mode.
- Optionally add display-name mapping for work-mode labels if product language needs end-user wording.

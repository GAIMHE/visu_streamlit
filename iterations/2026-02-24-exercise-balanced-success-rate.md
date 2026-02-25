# 2026-02-24 - Exercise-balanced success rate metric

## Context / scope
- Added a second success-rate definition requested by user:
  - existing: attempt-weighted success rate
  - new: exercise-balanced success rate (each exercise weighted equally)
- Scope:
  - first overview page (`apps/streamlit_app.py`)
  - objective-activity matrix page (`apps/pages/2_objective_activity_matrix.py`)
  - matrix metric builder (`src/visu2/objective_activity_matrix.py`)
  - related tests/docs updates

## Main changes made
- `apps/streamlit_app.py`
  - Added KPI card: `Success Rate (exercise-balanced)`.
  - Kept existing `Success Rate (attempt-weighted)`.
  - In work-mode summary:
    - added `exercise_balanced_success_rate` per work mode
    - displayed both success-rate definitions in tables.
- `src/visu2/objective_activity_matrix.py`
  - Added metric id: `exercise_balanced_success_rate`.
  - Implemented activity-cell computation:
    1. compute exercise success over selected dates (attempt-weighted across days per exercise),
    2. average these per-exercise rates equally within each activity.
  - Extended drilldown metric mapping to accept the new metric.
- `apps/pages/2_objective_activity_matrix.py`
  - Added selector label: `Exercise-balanced success rate`.
  - Enabled metric only when `agg_exercise_daily` is available and compatible.
  - Passed `agg_exercise_daily` into matrix-cell builder.
  - Kept `[0,1]` color scaling for the new rate metric.
- `tests/test_objective_activity_matrix.py`
  - Added formula test for the new metric.
  - Added validation test for missing `agg_exercise_daily` input when metric is selected.
- Documentation:
  - Updated `README.md` metric lists/definitions.
  - Updated `ressources/STREAMLIT_FIGURES_GUIDE.md` references.

## Important decisions and rationale
- No derived schema changes were introduced.
- New metric is computed at runtime from existing derived inputs.
- Matrix metric is gated on `agg_exercise_daily` availability to avoid silent fallback to incorrect semantics.

## Validation results
- `uv run pytest tests/test_objective_activity_matrix.py -q` -> pass
- `uv run pytest -q` -> pass
- `uv run python scripts/run_slice.py --smoke` -> pass
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose` -> executed
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> executed

## Follow-up actions
- Optional: add small UI tooltip on both pages with explicit formulas for both success-rate definitions.

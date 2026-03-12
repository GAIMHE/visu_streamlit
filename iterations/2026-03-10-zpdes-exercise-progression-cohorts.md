# ZPDES Exercise Progression Cohorts

## Context
- The transition-efficiency page previously relied on activity first-arrival events.
- That model was too coarse for repeated first attempts on new exercises inside the same activity.
- The page needed to distinguish `before`, `after`, and `in-activity` cohorts at the event level.

## Main Changes Made
- Replaced the old activity-first-arrival artifact with `zpdes_exercise_progression_events.parquet`.
- Added `build_zpdes_exercise_progression_events_from_fact(...)` in `src/visu2/derive_zpdes.py`.
- Updated the page helper to aggregate three hover cohorts per activity:
  - `before`
  - `after`
  - `in-activity`
- Added both eligible-event counts and unique-student counts to hover metrics.
- Replaced the `after` threshold slider with a typed integer input.
- Updated build/runtime contracts, smoke requirements, HF sync paths, and documentation.

## Important Decisions and Rationale
- The cohort unit is now the first-ever attempt on a new exercise, not the first arrival to an activity.
- `after` is evaluated first and takes precedence when the later-attempt threshold is met.
- `in-activity` captures new-exercise attempts after prior work in the same activity when the `after` condition is not met.
- The page remains static and hover-based; no arrow overlay or click state was reintroduced.

## Validation
- `uv run ruff check apps src scripts tests`
- `uv run pytest -q`
- `uv run python scripts/build_derived.py --strict-checks`
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/run_slice.py --smoke`

## Follow-Up Actions
- Refresh HF runtime data with the rebuilt `zpdes_exercise_progression_events.parquet` artifact before deployment.
- Consider adding page-level summary cards if cohort density across modules becomes hard to compare from hover alone.

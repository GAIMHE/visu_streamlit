# 2026-02-17 - First Attempt Success Metric (Matrix)

## Context and Scope
Added a new matrix metric to make interpretation clearer: first-attempt success rate, alongside existing all-attempt success and repeat-attempt rate.

## Main Changes Made
- Updated `src/visu2/derive.py` to compute activity-daily:
  - `first_attempt_success_rate`
  - `first_attempt_count`
- Updated `src/visu2/contracts.py` so `agg_activity_daily` contract now includes these columns.
- Updated `src/visu2/objective_activity_matrix.py`:
  - added metric `first_attempt_success_rate`
  - added weighted aggregation using `first_attempt_count`
  - kept backward-safe behavior for non-first-attempt metrics when old artifacts miss new columns.
- Updated `apps/pages/2_objective_activity_matrix.py`:
  - added UI metric option "First-attempt success rate"
  - added explicit rebuild hint if metric cannot run because artifacts are stale.
- Added/updated tests:
  - `tests/test_derive_shapes.py`
  - `tests/test_objective_activity_matrix.py`
- Updated docs in `README.md` metric definitions.

## Important Decisions and Rationale
- Kept existing success-rate semantics unchanged (all attempts).
- Added first-attempt success as a separate metric rather than replacing existing success rate.
- Weighted matrix recomputation uses `first_attempt_count` (not total attempts) to avoid biased date-range aggregation.

## Follow-up Actions
- Optionally expose first-attempt success in other pages (KPIs/bottleneck) if needed.
- Consider adding a tooltip legend comparing "all-attempt success" vs "first-attempt success" directly in UI.

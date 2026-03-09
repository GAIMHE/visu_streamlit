# agg_exercise_elo

## Goal
Render a runtime-ready derived table for `agg_exercise_elo`.

## Physical Location
- `artifacts/derived/agg_exercise_elo.parquet`

## Producer
- `src/visu2/derive_elo.py::build_agg_exercise_elo_from_fact`

## Main Consumers
- `apps/pages/2_objective_activity_matrix.py` Elo drilldown branch.

## Required Columns (Contract)
- `exercise_id`
- `module_id`
- `module_code`
- `objective_id`
- `activity_id`
- `exercise_elo`
- `calibration_attempts`
- `calibration_success_rate`
- `calibrated`

## Label Columns (Runtime expectation)
- `module_label`
- `objective_label`
- `activity_label`
- `exercise_label`

## Metric / Computation Notes
- Exercise Elo calibration uses first attempts and symmetric Elo update.
- `p(correct) = 1 / (1 + 10^((R_exercise - R_student)/400))`

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

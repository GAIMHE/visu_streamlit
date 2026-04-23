# agg_exercise_daily

## Goal
Render a runtime-ready derived table for `agg_exercise_daily`.

## Physical Location
- `artifacts/derived/agg_exercise_daily.parquet`

## Producer
- `src/visu2/derive_aggregates.py::build_agg_exercise_daily_from_fact`

## Main Consumers
- `apps/pages/2_objective_activity_matrix.py` drilldown and exercise-balanced metric.

## Required Columns (Contract)
- `date_utc`
- `module_id`
- `module_code`
- `objective_id`
- `activity_id`
- `exercise_id`
- `attempts`
- `unique_students`
- `success_rate`
- `first_attempt_success_rate`
- `first_attempt_count`
- `median_duration`
- `repeat_attempt_rate`
- `retry_before_success_rate`
- `avg_attempt_number`

## Label Columns (Runtime expectation)
- `module_label`
- `objective_label`
- `activity_label`
- `exercise_label`

## Metric / Computation Notes
- Exercise-level metrics mirror activity-level formulas on exercise grouping keys.
- `retry_before_success_rate` is the share of attempts with attempt index > 1 where the same student had not yet succeeded on the same exercise before that attempt.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

# agg_activity_daily

## Goal
Render a runtime-ready derived table for `agg_activity_daily`.

## Physical Location
- `artifacts/derived/agg_activity_daily.parquet`

## Producer
- `src/visu2/derive_aggregates.py::build_agg_activity_daily_from_fact`

## Main Consumers
- `apps/streamlit_app.py`
- `apps/pages/2_objective_activity_matrix.py`
- `apps/pages/3_zpdes_transition_efficiency.py` (ZPDES first-attempt coloring)

## Required Columns (Contract)
- `date_utc`
- `activity_id`
- `objective_id`
- `module_id`
- `module_code`
- `attempts`
- `unique_students`
- `success_rate`
- `median_duration`
- `repeat_attempt_rate`
- `avg_attempt_number`

## Label Columns (Runtime expectation)
- `module_label`
- `objective_label`
- `activity_label`

## Metric / Computation Notes
- `success_rate` is attempt-weighted mean of correctness.
- `repeat_attempt_rate` is the share of attempts with attempt index > 1.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

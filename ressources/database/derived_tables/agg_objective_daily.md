# agg_objective_daily

## Goal
Render a runtime-ready derived table for `agg_objective_daily`.

## Physical Location
- `artifacts/derived/agg_objective_daily.parquet`

## Producer
- `src/visu2/derive_aggregates.py::build_agg_objective_daily_from_fact`

## Main Consumers
- Used by contracts/manifest and analytic extensions (not primary active chart input).

## Required Columns (Contract)
- `date_utc`
- `objective_id`
- `module_id`
- `module_code`
- `attempts`
- `unique_students`
- `success_rate`
- `median_duration`
- `repeat_attempt_rate`

## Label Columns (Runtime expectation)
- `module_label`
- `objective_label`

## Metric / Computation Notes
- Metrics are precomputed in derive builders and consumed directly by UI blocks.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

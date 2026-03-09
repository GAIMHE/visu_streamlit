# agg_module_activity_usage

## Goal
Render a runtime-ready derived table for `agg_module_activity_usage`.

## Physical Location
- `artifacts/derived/agg_module_activity_usage.parquet`

## Producer
- `src/visu2/derive_aggregates.py::build_agg_module_activity_usage_from_fact`

## Main Consumers
- Hidden usage page and runtime contract checks.

## Required Columns (Contract)
- `module_code`
- `activity_id`
- `attempts`
- `unique_students`
- `activity_share_within_module`

## Label Columns (Runtime expectation)
- `module_label`
- `activity_label`

## Metric / Computation Notes
- Metrics are precomputed in derive builders and consumed directly by UI blocks.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

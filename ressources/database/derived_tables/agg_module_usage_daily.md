# agg_module_usage_daily

## Goal
Render a runtime-ready derived table for `agg_module_usage_daily`.

## Physical Location
- `artifacts/derived/agg_module_usage_daily.parquet`

## Producer
- `src/visu2/derive_aggregates.py::build_agg_module_usage_daily_from_fact`

## Main Consumers
- Hidden usage page (`apps/disabled_pages/1_usage_playlist_engagement.py`) and runtime contract checks.

## Required Columns (Contract)
- `date_utc`
- `module_code`
- `attempts`
- `unique_students`

## Label Columns (Runtime expectation)
- `module_label`

## Metric / Computation Notes
- Metrics are precomputed in derive builders and consumed directly by UI blocks.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

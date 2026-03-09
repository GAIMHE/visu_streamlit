# fact_attempt_core

## Goal
Render a runtime-ready derived table for `fact_attempt_core`.

## Physical Location
- `artifacts/derived/fact_attempt_core.parquet`

## Producer
- `src/visu2/derive_fact.py::build_fact_attempt_core`

## Main Consumers
- `apps/streamlit_app.py`
- `apps/pages/4_classroom_progression_replay.py`
- `apps/pages/2_objective_activity_matrix.py`

## Required Columns (Contract)
- `created_at`
- `date_utc`
- `user_id`
- `objective_id`
- `activity_id`
- `exercise_id`
- `data_correct`
- `data_duration`
- `attempt_number`
- `module_code`

## Label Columns (Runtime expectation)
- `module_label`
- `objective_label`
- `activity_label`

## Metric / Computation Notes
- Metrics are precomputed in derive builders and consumed directly by UI blocks.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

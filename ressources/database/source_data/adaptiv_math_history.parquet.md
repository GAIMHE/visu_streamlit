# adaptiv_math_history.parquet

## Goal
Raw interaction history used as the pipeline input to derive runtime analytics artifacts.

## Physical Location
- `data/adaptiv_math_history.parquet`

## Producer
- External upstream export (loaded by `build_fact_attempt_core`).

## Main Consumers
- `src/visu2/derive_fact.py::build_fact_attempt_core`
- `scripts/check_contracts.py` for schema checks

## Main Fields
- Attempt-level identifiers (`user_id`, `exercise_id`, `activity_id`, `objective_id`).
- Timing fields (`created_at`, session or duration fields).
- Outcome fields (`data_correct`, `attempt_number`, work mode context).

## Notes
- This file is large and is not fetched by default in HF runtime sync for deployed mode.
- Runtime app pages consume derived tables, not this raw parquet directly.

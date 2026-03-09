# student_elo_events

## Goal
Render a runtime-ready derived table for `student_elo_events`.

## Physical Location
- `artifacts/derived/student_elo_events.parquet`

## Producer
- `src/visu2/derive_elo.py::build_student_elo_events_from_fact`

## Main Consumers
- `apps/pages/5_student_elo_evolution.py` replay chart.

## Required Columns (Contract)
- `user_id`
- `attempt_ordinal`
- `created_at`
- `date_utc`
- `work_mode`
- `module_code`
- `objective_id`
- `activity_id`
- `exercise_id`
- `outcome`
- `expected_success`
- `exercise_elo`
- `student_elo_pre`
- `student_elo_post`

## Metric / Computation Notes
- Student Elo replay uses fixed exercise Elo and per-attempt updates.
- `delta = K * (outcome - expected_success)` with `K=24` and base rating 1500.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

# student_elo_profiles

## Goal
Render a runtime-ready derived table for `student_elo_profiles`.

## Physical Location
- `artifacts/derived/student_elo_profiles.parquet`

## Producer
- `src/visu2/derive_elo.py::build_student_elo_profiles_from_events`

## Main Consumers
- `apps/pages/5_student_elo_evolution.py` selector and summary cards.

## Required Columns (Contract)
- `user_id`
- `total_attempts`
- `first_attempt_at`
- `last_attempt_at`
- `unique_modules`
- `unique_objectives`
- `unique_activities`
- `final_student_elo`
- `eligible_for_replay`

## Metric / Computation Notes
- Profiles summarize event-level replay output by user (`total_attempts`, `final_student_elo`, timespan).

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

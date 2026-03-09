# agg_student_module_progress

## Goal
Provide student-level progress aggregation by module for progression analytics and contract continuity.

## Physical Location
- `artifacts/derived/agg_student_module_progress.parquet`

## Producer
- `src/visu2/derive_aggregates.py::build_agg_student_module_progress_from_fact`

## Main Consumers
- Runtime contract checks and manifest tracking.
- Downstream progression analyses and ad-hoc slicing workflows.

## Required Columns (Contract)
- `date_utc`
- `work_mode`
- `module_code`
- `module_label`
- `user_id`
- `attempts`
- `unique_activities`
- `success_rate`
- `median_duration`
- `repeat_attempt_rate`

## Metric / Computation Notes
- Metrics are computed from `fact_attempt_core` grouped by student/module scope.
- `success_rate` is attempt-weighted from `data_correct`.
- `repeat_attempt_rate` captures the share of repeated attempts in the aggregation group.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

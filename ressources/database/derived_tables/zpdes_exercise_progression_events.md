# zpdes_exercise_progression_events

## Goal
Provide a runtime-ready event table for the static ZPDES transition-efficiency page.

## Physical Location
- `artifacts/derived/zpdes_exercise_progression_events.parquet`

## Producer
- `src/visu2/derive_zpdes.py::build_zpdes_exercise_progression_events_from_fact`

## Main Consumers
- `apps/pages/6_zpdes_transition_efficiency.py`

## Required Columns (Contract)
- `user_id`
- `created_at`
- `date_utc`
- `module_id`
- `module_code`
- `module_label`
- `objective_id`
- `objective_label`
- `activity_id`
- `activity_label`
- `exercise_id`
- `work_mode`
- `destination_rank`
- `exercise_first_attempt_outcome`
- `prior_attempt_count`
- `prior_before_activity_attempt_count`
- `prior_same_activity_attempt_count`
- `prior_later_activity_attempt_count`

## Metric / Computation Notes
- One row is emitted per `student x exercise x work_mode`, using the student's first-ever attempt on that exercise.
- `destination_rank` comes from canonical `learning_catalog.json` order inside the module.
- `prior_attempt_count` counts all strictly earlier attempts in the same module and work mode.
- `prior_before_activity_attempt_count` counts those prior attempts whose activity rank is lower than the destination activity rank.
- `prior_same_activity_attempt_count` counts those prior attempts on the same activity, regardless of exercise.
- `prior_later_activity_attempt_count` counts those prior attempts whose activity rank is higher than the destination activity rank.
- The page classifies rows at runtime into:
  - `before`
  - `after`
  - `in-activity`
  - `excluded`
  because the `after` threshold is user-controlled.

## Filters / Scope Semantics
- Date filtering in the page applies to the exercise first-attempt event date only.
- Earlier history used for classification may fall outside the selected page date range.
- Work-mode and module scoping are enforced before the event row is emitted.
- Same-timestamp attempts do not count as prior to one another.

## Validation Checks
- `uv run python scripts/build_derived.py --strict-checks`
- `uv run python scripts/check_contracts.py --strict`
- `uv run pytest -q`

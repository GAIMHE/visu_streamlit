# zpdes_first_arrival_events

## Goal
Provide a runtime-ready event table for the static ZPDES transition-efficiency page.

## Physical Location
- `artifacts/derived/zpdes_first_arrival_events.parquet`

## Producer
- `src/visu2/derive_zpdes.py::build_zpdes_first_arrival_events_from_fact`

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
- `work_mode`
- `destination_rank`
- `first_arrival_outcome`
- `prior_attempt_count`
- `prior_before_attempt_count`
- `prior_later_attempt_count`
- `arrival_bucket_base`

## Metric / Computation Notes
- One row is emitted per `student x destination activity x work_mode`.
- The event uses the student's first-ever arrival on that destination activity in that work mode.
- `destination_rank` comes from canonical `learning_catalog.json` order inside the module.
- `prior_attempt_count` counts all strictly earlier attempts in the same module and work mode.
- `prior_before_attempt_count` counts those prior attempts whose activity rank is lower than the destination rank.
- `prior_later_attempt_count` counts those prior attempts whose activity rank is higher than the destination rank.
- `arrival_bucket_base` is assigned as:
  - `before` if there is at least one prior earlier attempt and zero prior later attempts
  - `after_candidate` if there is at least one prior later attempt
  - `excluded` otherwise

## Filters / Scope Semantics
- Date filtering in the page applies to the first-arrival event date only.
- Earlier history used for classification may fall outside the selected page date range.
- Work-mode and module scoping are enforced before the first-arrival event is built.

## Validation Checks
- `uv run python scripts/build_derived.py --strict-checks`
- `uv run python scripts/check_contracts.py --strict`
- `uv run pytest -q`

# work_mode_transition_paths

## Goal
Render a compact runtime-ready derived table for the overview work-mode Sankey.

## Physical Location
- `artifacts/derived/work_mode_transition_paths.parquet`

## Producer
- `src/visu2/work_mode_transitions.py::build_work_mode_transition_paths`

## Main Consumers
- `apps/streamlit_app.py` work-mode transition Sankey on page 1.

## Required Columns (Contract)
- `user_id`
- `first_work_mode`
- `transition_count_total`
- `transition_1_mode`
- `transition_2_mode`
- `transition_3_mode`
- `continues_after_transition_3`

## Metric / Computation Notes
- One row per student.
- Histories are ordered from the raw parquet with `student_attempt_index`.
- `transition_count_total` counts work-mode changes, not attempts.
- Consecutive attempts in the same mode are compressed and do not create extra transitions.
- `transition_1_mode`, `transition_2_mode`, and `transition_3_mode` record the modes entered at the first three changes.
- `continues_after_transition_3` is `true` when the student changes mode more than three times.

## Filters / Scope Semantics
- This artifact is global and preserves full student histories.
- It is not pre-filtered by date, module, objective, activity, or work mode.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

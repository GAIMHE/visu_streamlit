# agg_transition_edges

## Goal
Render a runtime-ready derived table for `agg_transition_edges`.

## Physical Location
- `artifacts/derived/agg_transition_edges.parquet`

## Producer
- `src/visu2/transitions.py::build_transition_edges` via derive pipeline

## Main Consumers
- `apps/streamlit_app.py` path transitions chart.

## Required Columns (Contract)
- `date_utc`
- `from_activity_id`
- `to_activity_id`
- `transition_count`
- `success_conditioned_count`

## Label Columns (Runtime expectation)
- `from_activity_label`
- `to_activity_label`
- `from_module_label`

## Metric / Computation Notes
- `transition_count` tracks observed source->target transitions in ordered attempt paths.
- `success_conditioned_count` counts transitions associated with successful conditions.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

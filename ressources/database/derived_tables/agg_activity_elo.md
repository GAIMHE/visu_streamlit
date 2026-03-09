# agg_activity_elo

## Goal
Render a runtime-ready derived table for `agg_activity_elo`.

## Physical Location
- `artifacts/derived/agg_activity_elo.parquet`

## Producer
- `src/visu2/derive_elo.py::build_agg_activity_elo_from_exercise_elo`

## Main Consumers
- `apps/pages/2_objective_activity_matrix.py` activity Elo heatmap metric.

## Required Columns (Contract)
- `module_id`
- `module_code`
- `objective_id`
- `activity_id`
- `activity_mean_exercise_elo`
- `calibrated_exercise_count`
- `catalog_exercise_count`
- `calibration_coverage_ratio`

## Label Columns (Runtime expectation)
- `module_label`
- `objective_label`
- `activity_label`

## Metric / Computation Notes
- `activity_mean_exercise_elo` is mean calibrated exercise Elo within activity.
- Coverage fields expose calibrated vs catalog exercise counts.

## Filters / Scope Semantics
- Date/work-mode/module filters are applied in page-level query logic, not by mutating this artifact.

## Validation Checks
- `uv run python scripts/check_contracts.py --strict`
- `uv run python scripts/build_derived.py --strict-checks`

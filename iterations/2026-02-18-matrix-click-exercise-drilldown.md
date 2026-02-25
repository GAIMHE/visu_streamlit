# 2026-02-18 - Matrix Click Exercise Drilldown

## Context and Scope
Implemented click-to-drill behavior for the objective-activity matrix page so users can inspect exercise-level metrics for a selected activity cell under current module/date filters.

## Main Changes Made
- Added new derived table generation in `src/visu2/derive.py`:
  - `build_agg_exercise_daily_from_fact(...)`
  - joined exercise metadata from `data/exercises.json`
  - emitted `artifacts/derived/agg_exercise_daily.parquet`
- Added schema contracts in `src/visu2/contracts.py`:
  - `REQUIRED_AGG_COLUMNS["agg_exercise_daily"]`
  - runtime core/label column maps for `agg_exercise_daily`
- Updated manifest generation in `scripts/build_derived.py` to include `agg_exercise_daily`.
- Updated smoke required artifacts in `scripts/run_slice.py` to include `agg_exercise_daily`.
- Extended matrix utilities in `src/visu2/objective_activity_matrix.py`:
  - `build_exercise_drilldown_frame(...)` for weighted range aggregation at exercise level.
- Updated `apps/pages/2_objective_activity_matrix.py`:
  - enabled Plotly point selection with `on_select="rerun"`
  - persisted selected matrix cell in session state
  - added inline Exercise Drilldown section (chart + table)
  - added graceful handling for missing/incompatible drilldown artifacts.
- Updated docs in `README.md` for new artifact and click drilldown behavior.
- Added tests in `tests/test_derive_shapes.py`, `tests/test_objective_activity_matrix.py`, and contract coverage in `tests/test_contracts.py`.

## Important Decisions and Rationale
- Chose pre-aggregated `agg_exercise_daily` for stable, fast interaction and consistent metric semantics.
- Kept drilldown metric synchronized with matrix metric to reduce UI complexity and interpretation mismatch.
- Built exercise labels from instruction text snippets with ID suffix for readability plus disambiguation.
- Did not use `exercises.json` hierarchy placeholder fields for joins; drilldown uses observed fact rows filtered by selected activity.

## Follow-up Actions
- Optional: add objective-level and module-level exercise drilldown exports (CSV) from the same filtered panel.
- Optional: add toggle to switch drilldown sort between selected metric and attempts.

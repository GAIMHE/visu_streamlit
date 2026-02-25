# Iteration: 2026-02-17 - IDEE Feasible-Now Analytics Pack

## Context/Scope
- Integrate ideas from `ressources/IDEE Pistes d'analyses.docx` and `ressources/IDEE_Playlist_ANALYSIS.docx` into a feasible-now Phase 1 increment.
- Focus only on analyses supported by current in-repo data (usage exposure, playlist analytics, engagement).
- Keep pre/post analyses deferred due missing external IDEE post-test data.

## Main Changes
- Added new derived datasets in `src/visu2/derive.py` and build outputs:
  - `agg_module_usage_daily.parquet`
  - `agg_student_module_exposure.parquet`
  - `agg_playlist_module_usage.parquet`
  - `agg_module_activity_usage.parquet`
- Extended contracts in `src/visu2/contracts.py`:
  - required columns for all new datasets
  - runtime core/label schema checks for new tables
- Extended manifest generation in `scripts/build_derived.py` so `artifacts/reports/derived_manifest.json` includes all new tables.
- Updated `apps/streamlit_app.py` schema guard to account for new runtime tables.
- Added new Streamlit multipage view:
  - `apps/pages/1_usage_playlist_engagement.py`
  - sections: exposure overview, module trends, playlist analytics, module activity usage, diligent subgroup panel
  - configurable diligent thresholds (`min_attempts`, `min_active_days`, `min_total_time_minutes`)
- Updated smoke prerequisites in `scripts/run_slice.py` to include new derived files.
- Updated docs:
  - `README.md` with new outputs and Usage/Playlist/Engagement page instructions
  - `ressources/roadmap_refined.md` Phase 1 addendum for feasible-now IDEE analyses
- Expanded tests in `tests/test_derive_shapes.py` and `tests/test_contracts.py` for new shapes/contracts and exposure bucket boundaries.

## Decisions and Rationale
- Exposure effectiveness rule from IDEE notes is implemented as `attempts > 10`.
- Diligent subgroup is configurable in the UI for exploratory analysis, with defaults (`10`, `3`, `60 min`).
- Session proxy uses `session_duration` with row-wise fallback to `data_duration` where needed.
- Pre/post analyses are left out until post-test outcomes are contractually integrated.

## Follow-ups
- Add a dedicated contract and ingestion path for external IDEE pre/post outcomes.
- Add optional classroom/date-aware pre-aggregations for playlist tables to align all page filters at the same grain.
- Add app smoke import for all Streamlit pages (not only main entrypoint).

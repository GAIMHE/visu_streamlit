# Overview Runtime Compatibility Scope Fix

## Context
- The overview page started reporting `Artifact status: INCOMPATIBLE` after the introduction of `zpdes_first_arrival_events`.
- The new artifact is required by the ZPDES transition-efficiency page, but not by the overview page itself.
- The overview compatibility helper was validating the full runtime contract instead of the page's own dependencies.

## Main Changes
- Added `OVERVIEW_RUNTIME_TABLES` in `apps/streamlit_app.py` to define the tables actually used by the overview page.
- Updated `_collect_runtime_compatibility(...)` to accept a scoped `required_tables` list.
- Narrowed the overview page's required file existence check to:
  - `fact_attempt_core`
  - `agg_activity_daily`
  - `agg_objective_daily`
  - `agg_transition_edges`
  - `consistency_report.json`
- Narrowed the overview page's column/manifest compatibility check to the same table set.

## Important Decisions and Rationale
- The overview page should not be blocked by artifacts that belong exclusively to other pages.
- Global runtime validation still belongs in build, smoke, and page-specific guards; it should not be enforced by page 1.
- Manifest schema drift can still surface as a degraded state, but missing unrelated tables should no longer mark the overview as incompatible.

## Validation
- `uv run ruff check apps/streamlit_app.py tests`
- `uv run pytest -q`

## Follow-Up
- Finish the `zpdes_first_arrival_events` build path so page 6 and smoke checks can run against the new artifact end to end.

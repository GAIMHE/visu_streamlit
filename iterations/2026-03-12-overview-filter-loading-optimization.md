# 2026-03-12 Overview Filter Loading Optimization

## Context
Deployment on Streamlit Cloud still failed after page-scoped HF sync was reduced to a single startup file. The remaining cold-start bottleneck was the overview page eagerly reading full filter dimensions from `fact_attempt_core.parquet` just to populate the sidebar.

## Main changes made
- Added `CurriculumFilterDomain` in `apps/overview_shared.py`.
- Changed `load_fact_dimensions(...)` to load:
  - `min_date` / `max_date` via lazy aggregation
  - a compact distinct curriculum frame for module/objective/activity options
- Updated `render_curriculum_filters(...)` to accept either the compact domain or the previous eager frame shape.
- Updated `apps/streamlit_app.py` to use the compact overview filter domain.

## Important decisions and rationale
- Kept the visible overview behavior unchanged.
- Avoided a full eager parquet read on landing-page startup because that is the most plausible remaining cause of Streamlit Cloud health-check failure after startup sync was reduced to one file.
- Preserved compatibility for aggregate-backed pages by allowing `render_curriculum_filters(...)` to keep accepting a `pl.DataFrame`.

## Follow-up actions
- Redeploy and verify whether the Streamlit Cloud health check now succeeds on cold start.
- If deployment still fails, the next target should be reducing first-page computations built directly from `fact_attempt_core.parquet`.

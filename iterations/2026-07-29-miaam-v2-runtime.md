# MIAAM V2 runtime migration

## Context and scope

The existing MIAAM-only Streamlit application on the `neurips` branch was built
for MIAAM V1 (Adaptiv'Math plus MIA). MIAAM V2 retains the same three runtime
inputs but replaces MIA with Adaptiv World and Adaptiv College:

- `data_miaam/maths_data_filtered.parquet`
- `data_miaam/maths_exercises_table.parquet`
- `data_miaam/maths_dependencies.json`

This iteration keeps the application pages and interaction design unchanged and
updates only the source adapter, source description, and related regression
coverage.

## Main changes

- Accepted the V2 `objective_pedagogical_intent` exercise-table column while
  preserving compatibility with the legacy `objective_targeted_difficulties`
  column.
- Preserved the historical app module codes for Adaptiv'Math and Adaptiv World.
- Assigned Adaptiv College the collision-free app codes `M201`, `M202`, and
  `M203`, because its graph-local `M101`-`M103` codes overlap with World codes.
- Made fallback graph codes globally unique inside the combined runtime catalog.
- Corrected the consistency check so graph activities with no exercises do not
  contribute a synthetic null exercise identifier.
- Updated the displayed dataset label and description to MIAAM V2 and its three
  sources.
- Added regression coverage for the V2 pedagogical-intent schema and duplicate
  graph-code handling.

## Important decisions and rationale

- The internal runtime source ID remains `neurips`. Keeping it stable avoids
  changes to application state, artifact paths, and Streamlit's
  `VISU2_HF_SOURCES_JSON` key.
- Module-code normalization happens only in the visualization runtime adapter.
  The published MIAAM V2 source data and dependency graph remain unchanged.
- Adaptiv World keeps `M101` and `M105` so visual labels remain comparable with
  the V1 application. College receives a new `M2xx` range because module codes
  must be globally unique in the combined app.
- Runtime artifacts must be fully rebuilt because all aggregates are derived
  from the selected release, even though the visualization code is unchanged.

## Follow-up actions

- Upload the rebuilt `artifacts/sources/neurips/` runtime bundle to the selected
  Hugging Face runtime dataset repository.
- Point the Streamlit deployment's `neurips` runtime configuration at that
  repository and a pinned revision, then deploy the `miaam-v2` branch.

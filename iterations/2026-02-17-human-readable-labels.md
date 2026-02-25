# Iteration: 2026-02-17 - Human-Readable Labels in Thin Slice

## Context/Scope
- Improve the Streamlit thin slice so objectives and activities are shown with human-readable names.
- Keep filtering and joins stable by preserving ID-based logic under the hood.
- Align tests and contracts with label-aware derived schemas.

## Main Changes
- Updated hierarchy loaders to carry labels from `summary.json`:
  - `module_label`, `objective_label`, `activity_label` propagated into hierarchy frames.
- Updated derived builders to persist labels in:
  - `fact_attempt_core.parquet`
  - `agg_activity_daily.parquet`
  - `agg_objective_daily.parquet`
  - `agg_student_module_progress.parquet`
  - `agg_transition_edges.parquet`
- Updated contracts to require label columns in fact and aggregate outputs.
- Updated Streamlit UI (`apps/streamlit_app.py`):
  - Sidebar filters now show `Label [ID]` for module/objective/activity.
  - Bottleneck chart now uses labeled activity axis values.
  - Transition chart now uses labeled edge strings with ASCII separator `->`.
- Updated tests to include label-aware sample fixtures and contract expectations.

## Decisions and Rationale
- Chose `summary.json` as canonical label source to stay consistent with the existing hierarchy contract and source precedence rules.
- Preserved ID-based filter values internally to avoid changing downstream query semantics.
- Kept ID fallback display behavior when labels are missing so UI remains resilient.

## Follow-ups
- Add an explicit timestamp in the UI indicating which derived artifact build is currently loaded.
- Add regression tests for UI option-label formatting and label fallback behavior.
- Consider optional display-only enrichment from `modules_configGraphe.xlsx` for modules outside active summary scope.

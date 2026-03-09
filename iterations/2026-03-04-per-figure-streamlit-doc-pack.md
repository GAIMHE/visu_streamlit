# 2026-03-04 Per-Figure Streamlit Documentation Pack

## Context / Scope
- Add one Markdown file per analytical display block currently visible in the active Streamlit app.
- Cover active pages only.
- Exclude the hidden page in `apps/disabled_pages/`.

## Main Changes Made
- Created a new documentation folder:
  - `ressources/figures/`
- Added a folder index:
  - `ressources/figures/README.md`
- Added one file per visible analytical block across:
  - `apps/streamlit_app.py`
  - `apps/pages/2_objective_activity_matrix.py`
  - `apps/pages/3_zpdes_dependencies.py`
  - `apps/pages/4_classroom_progression_replay.py`
  - `apps/pages/5_student_elo_evolution.py`
- Added a pointer to the new figure docs in:
  - `ressources/README_INDEX.md`

## Important Decisions / Rationale
- Interpreted “per figure” as “per visible analytical display block”, not only Plotly charts.
- Included KPI cards, analytical tables, drilldown sections, and replay summary cards because they present computed analytical outputs.
- Kept the docs in English to align with the current top-level repo documentation.
- Kept the iteration documentation-only: no code, artifact, or deployment changes.

## Follow-Up Actions
- If the hidden usage page is re-enabled later, add the same per-block documentation for its visible sections.
- If the active Streamlit pages change, update the corresponding files in `ressources/figures/` so the documentation remains code-coupled.

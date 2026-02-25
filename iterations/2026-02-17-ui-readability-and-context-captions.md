# Iteration: 2026-02-17 - UI Readability and Section Context

## Context/Scope
- Improve readability in analytics views by reducing ID-heavy displays.
- Clarify module vs playlist rows in analytics tables.
- Add concise explanatory text before each section for interpretability.

## Main Changes
- `apps/streamlit_app.py`
  - Removed ID suffixes from bottleneck and path transition chart labels (labels now prioritize readable titles).
  - Added short explanatory captions before each main section.
- `apps/pages/1_usage_playlist_engagement.py`
  - Renamed section to `Module/Playlist Analytics`.
  - Added `work_mode` in table display.
  - For playlist rows, module code now displays as `non-applicable`.
  - Reworked table presentation to prioritize readable entity name and move raw ID to a separate column.
  - Added concise section captions across all major panels.
- `src/visu2/derive.py` / `src/visu2/contracts.py`
  - Added `work_mode` to playlist-module aggregate contract and derivation.

## Validation
- `uv run pytest -q` passed.
- `uv run python scripts/build_derived.py --strict-checks` passed.
- `uv run python scripts/run_slice.py --smoke` passed.

## Follow-ups
- Add a UI toggle to hide/show raw IDs globally in analytics tables.
- Replace deprecated `use_container_width` usages with `width='stretch'` in Streamlit charts/tables.

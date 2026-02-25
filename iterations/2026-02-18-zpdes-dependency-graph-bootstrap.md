# Iteration: 2026-02-18 - ZPDES Dependency Graph Bootstrap

## Context/Scope
- Implement the first dependency visualization for ZPDES unlock/deactivation logic.
- Build structure from `modules_configGraphe.xlsx` with metadata enrichment from `admathGraphe.json`.
- Add a module-level layered graph page with optional performance overlays.

## Main Changes
- Added dependency core module:
  - `src/visu2/zpdes_dependencies.py`
  - features:
    - parse prerequisites/deactivation expressions (including `%` thresholds)
    - normalize `dependency_nodes` and `dependency_edges` for one module
    - create ghost nodes for unresolved references
    - extract admath enrichment (`sr`, `lvl`) and attach to matching edges
    - attach optional overlay metrics from `agg_activity_daily`
    - compute supported module intersection across xlsx/admath/observed data
- Added Streamlit page:
  - `apps/pages/3_zpdes_dependencies.py`
  - features:
    - module/date/overlay controls
    - layered objective-lane graph with activity nodes
    - activation (solid blue) and deactivation (dashed red) edges
    - click node/edge detail panel
    - in-page dependency audit table
- Added tests:
  - `tests/test_zpdes_dependencies.py`
  - covers token parsing, ghost-node creation, threshold parsing, admath enrichment matching, and overlay metric weighting.
- Updated docs:
  - `README.md` section for ZPDES dependency graph page.

## Important Decisions and Rationale
- Source precedence implemented as `xlsx` topology first; admath only enriches existing edges/nodes.
- Objective/activity granularity chosen for readability while preserving operational detail.
- Overlay is optional by design to keep structure understandable by default.
- Ghost nodes are explicit to avoid silently dropping unresolved references.

## Validation
- `uv run python -m py_compile src/visu2/zpdes_dependencies.py apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`

## Follow-up Actions
- Improve edge direction cues (arrowheads) for very dense modules.
- Add optional filter for edge type (activation/deactivation) and threshold range.
- Add explicit conflict diagnostics where xlsx and admath rule values diverge.

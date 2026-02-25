# Iteration: 2026-02-18 - ZPDES Intra-objective Edge Clarity

## Context/Scope
- Improve readability when activity-to-activity dependencies occur within the same objective lane.
- Clarify dense same-line edges that previously overlapped as straight horizontal arrows.

## Main Changes
- `apps/pages/3_zpdes_dependencies.py`
  - Reworked edge rendering from grouped straight segments to per-edge traces.
  - Added curved spline rendering for intra-objective edges (same lane), with offset ranking to avoid overlap.
  - Kept activation/deactivation styling:
    - activation: solid blue
    - deactivation: dashed red
  - Kept directionality via arrow annotations and aligned arrowheads to curve end tangents.
  - Added sidebar toggle:
    - `Curve intra-objective edges` (default `True`)
  - Updated legend text to explain curvature behavior.
- `README.md`
  - Added control note for intra-objective edge curvature toggle.

## Important Decisions and Rationale
- Curved intra-lane edges were chosen over full rerouting to preserve the lane model while resolving overlap.
- Deactivation curves bend opposite to activation curves to keep edge type distinction even in dense lanes.

## Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py` passed.
- `uv run pytest -q tests/test_zpdes_dependencies.py` passed.
- `uv run python scripts/run_slice.py --smoke` passed.

## Follow-up Actions
- Optional: add edge bundling/opacity scaling for modules with very high edge density.

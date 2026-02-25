# Iteration: 2026-02-18 - ZPDES Edge Styling and Label Simplification

## Context/Scope
- Improve readability of dense same-objective dependencies.
- Reduce visual clutter from activity labels shown directly on nodes.

## Main Changes
- `apps/pages/3_zpdes_dependencies.py`
  - Activity node titles removed from canvas text; activity labels remain available in hover/details.
  - Intra-objective edges now:
    - use curved paths,
    - alternate above/below the lane for repeated same-span links,
    - use green color to distinguish from between-objective links.
  - Intra-objective deactivation keeps dashed style while staying green.
  - Between-objective edges keep existing semantics:
    - activation: solid blue
    - deactivation: dashed red
  - Legend text updated to reflect the new visual encoding.

## Important Decisions and Rationale
- Alternating curvature prevents same-lane overlap and keeps direction legible.
- Green intra-objective color creates immediate structural distinction from inter-objective dependencies.
- Removing activity labels from the canvas frees horizontal space and reduces collision; hover retains full detail.

## Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py` passed.
- `uv run pytest -q tests/test_zpdes_dependencies.py` passed.
- `uv run python scripts/run_slice.py --smoke` passed.

## Follow-up Actions
- Optionally add a compact toggle to show/hide objective labels as well for very large modules.

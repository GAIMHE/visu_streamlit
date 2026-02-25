# Iteration: 2026-02-18 - Exercise Placeholder Image in Drilldown

## Context/Scope
- Add a temporary screenshot placeholder in the matrix exercise drilldown.
- Keep instruction text as primary content and show image below it.

## Main Changes
- `apps/pages/2_objective_activity_matrix.py`
  - Added placeholder image rendering below selected exercise instruction.
  - Uses fixed path: `images/placeholder_exo.png`.
  - Added fallback caption if the image file is missing.

## Important Decisions and Rationale
- Single shared image is used for all exercises as a temporary UX placeholder.
- This avoids schema changes and keeps future migration to per-exercise screenshots simple.

## Validation
- `uv run python -m py_compile apps/pages/2_objective_activity_matrix.py` passed.
- `uv run pytest -q tests/test_objective_activity_matrix.py` passed.

## Follow-up Actions
- Replace shared placeholder with per-exercise screenshot mapping when images are available.

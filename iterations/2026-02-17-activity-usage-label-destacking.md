# Iteration: 2026-02-17 - Activity Usage Label De-stacking

## Context/Scope
- Fix stacked bars in `Activity Usage within Module` caused by non-unique activity display labels.
- Keep chart metric behavior unchanged.

## Main Changes Made
- `apps/pages/1_usage_playlist_engagement.py`
  - Added module-aware disambiguation fields for activity plotting labels.
  - Built `activity_display_plot` as a unique y-axis label:
    - default: readable activity label
    - duplicate label: `activity_label (module_display)`
    - repeated duplicate in same module context: `activity_label (module_display #n)`
  - Updated the bar chart y-axis from `activity_display` to `activity_display_plot`.

## Important Decisions and Rationale
- Avoided showing raw IDs in the axis to preserve readability.
- Disambiguated with module context first, then a minimal ordinal suffix only when required for uniqueness.

## Validation
- `uv run python -m py_compile apps/pages/1_usage_playlist_engagement.py` passed.
- `uv run pytest -q` passed (`12` tests).
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps` ran successfully.
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` ran successfully (template script reported no findings).

## Follow-up Actions
- Optionally add tooltip details (`activity_id`, `module_code`) while keeping axis clean, if analysts need stronger traceability.

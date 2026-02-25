# Iteration: 2026-02-17 - Bottleneck Multi-level ID-safe

## Context/Scope
- Revalidate Streamlit thin-slice behavior after recent label/stacking fixes.
- Upgrade `Bottleneck Candidates` to support `Module`, `Objective`, and `Activity` levels.
- Preserve canonical entity identity by grouping on IDs (not on readable labels).

## Main Changes Made
- `src/visu2/contracts.py`
  - Added `ACTIVE_CANONICAL_MODULE_CODES = ("M1", "M31", "M32", "M33", "M41", "M42", "M43")`.
- `src/visu2/bottleneck.py` (new)
  - Added `BOTTLENECK_LEVEL_CONFIG` for level-specific ID/label/context columns.
  - Added `apply_bottleneck_filters(...)` with context-aware semantics:
    - Module level: date + module filters.
    - Objective level: date + module + objective filters.
    - Activity level: date + module + objective + activity filters.
  - Enforced canonical module scope in bottleneck source filtering.
  - Added `build_bottleneck_frame(...)` using attempt-weighted aggregation.
  - Added `make_unique_plot_label(...)` to disambiguate label collisions using context and short ID fallback.
- `apps/streamlit_app.py`
  - Integrated new bottleneck helpers.
  - Added in-section bottleneck level selector (`Module | Objective | Activity`, default `Activity`).
  - Replaced label-merge bottleneck logic with ID-safe grouping.
  - Added canonical-scope empty-state message.
  - Kept path transition behavior unchanged.
- `tests/test_bottleneck_levels.py` (new)
  - Added pure-data tests for:
    - ID-safe grouping and unique plotting labels across all levels.
    - Context-aware filter semantics by level.
    - Canonical module scope enforcement.
- `tests/test_contracts.py`
  - Added assertion for `ACTIVE_CANONICAL_MODULE_CODES`.

## Important Decisions and Rationale
- Chose ID-first grouping to avoid semantic loss from merged labels.
- Kept bottleneck formula unchanged:
  - `bottleneck_score = 0.7 * (1 - success_rate) + 0.3 * repeat_attempt_rate`
- Kept chart full-width layout and existing transitions logic to minimize unrelated regressions.

## Validation
- `uv run pytest -q` passed (`16` tests).
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run python -m py_compile apps/streamlit_app.py src/visu2/bottleneck.py tests/test_bottleneck_levels.py` passed.
- Code-review skill scripts executed:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py .`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Optionally add a small UI badge to indicate when objective/activity filters are ignored due to selected bottleneck level.
- If needed, expose the canonical scope toggle as an explicit analyst control in a later iteration.

# Iteration: 2026-02-17 - Full-width Bottleneck/Transition Layout

## Context/Scope
- Improve readability of bottleneck and transition charts by giving each chart full horizontal space.
- Keep metrics, filters, and artifact contracts unchanged.

## Main Changes Made
- `apps/streamlit_app.py`
  - Replaced side-by-side layout (`st.columns`) with stacked full-width sections for:
    - `Bottleneck Candidates`
    - `Path Transitions`
  - Kept the same chart logic and controls.
  - Increased transition axis label truncation window from 24 to 36 chars to leverage full-width layout.

## Important Decisions and Rationale
- Prioritized visual clarity over dashboard density.
- Kept aggregation and scoring unchanged to avoid scope creep and preserve comparability.

## Validation
- `uv run python -m py_compile apps/streamlit_app.py` passed.
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run pytest -q` passed (`12` tests).
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps` ran successfully.
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` ran successfully (no findings from template script).

## Follow-up Actions
- If readability is still limited, next step is to add a mode toggle between `Top N bars` and a compact `table + single-bar focus` view.

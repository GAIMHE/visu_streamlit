# Iteration: 2026-02-17 - Bottleneck Label De-dup and Cross-objective Transitions

## Context/Scope
- Resolve confusing stacked bottleneck bars caused by repeated human-readable activity labels.
- Make path transitions focus on cross-objective flows rather than within-objective transitions.

## Main Changes Made
- `apps/streamlit_app.py`
  - Added `compose_group_hover_label(...)` helper for grouped-label hover behavior.
  - Refactored bottleneck aggregation to:
    - aggregate first at `activity_id + activity_label`, then consolidate by display label,
    - use attempt-weighted averages for `bottleneck_score`, `failure_rate`, and `repeat_attempt_rate`,
    - surface one displayed score per label (no stacked same-label artifacts),
    - expose grouped-ID context in hover and display `(<n> IDs)` suffix when a label maps to multiple IDs.
  - Updated path transition logic to prioritize cross-objective transitions:
    - filter to `same_objective_rate < 1.0` when available,
    - keep aggregation by unique edge and ranking by total `transition_count`.
  - Updated transition caption and empty-state message to explicitly reference cross-objective scope.

## Important Decisions and Rationale
- Kept existing metric formula unchanged; only changed grouping grain for display clarity.
- Used weighted averages to avoid bias from low-volume daily rows.
- Preserved fallback behavior if `same_objective_rate` is absent (no hard failure).

## Validation
- `uv run python -m py_compile apps/streamlit_app.py` passed.
- `uv run pytest -q` passed (`12` tests).
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps` ran successfully.
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` ran successfully (template script reported no findings).

## Follow-up Actions
- Add an explicit UI toggle: `Cross-objective only` vs `All transitions` for analyst flexibility.
- Consider showing `cross_objective_share` as a supplementary metric in transition hover.

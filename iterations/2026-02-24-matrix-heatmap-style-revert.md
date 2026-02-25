# Context / Scope
- User requested to restore the original matrix visual style (heatmap look) for `apps/pages/2_objective_activity_matrix.py`.
- Goal: bring back dense heatmap tiles while keeping activity-cell click drilldown usable.

# Main Changes
- Replaced scatter-only matrix rendering with a heatmap-first rendering path.
- Added a transparent scatter click layer above the heatmap to preserve point selection and drilldown behavior.
- Kept metric semantics, filters, and drilldown logic unchanged.

# Decisions and Rationale
- Used `go.Heatmap` for primary rendering to match the original visual appearance.
- Kept a transparent selector trace because Streamlit selection handling is trace-based and this preserves clickable cells without changing the visual design.
- Did not change any derived data contract or metric formulas in this iteration.

# Validation
- `uv run pytest tests/test_objective_activity_matrix.py -q` passed.
- `uv run python scripts/run_slice.py --smoke` passed.
- Code-reviewer pass executed:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages/2_objective_activity_matrix.py --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

# Follow-up Actions
- Confirm with user visual preference on tile density and selection feedback highlight intensity.
- If needed, tune click-layer marker size/opacity for browser-specific behavior.

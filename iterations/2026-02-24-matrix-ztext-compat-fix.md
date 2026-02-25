# Context / Scope
- Fixed runtime crash on matrix page: `KeyError: 'z_text'` in `apps/pages/2_objective_activity_matrix.py`.
- Issue appeared after switching matrix rendering paths where payload text key names diverged.

# Main Changes
- Added a compatibility fallback for matrix text payload:
  - first try `payload["z_text"]`
  - fallback to `payload["text_values"]`
  - fallback to empty list if neither key exists
- Updated both hover/customdata text extraction and heatmap text rendering to use this normalized key.

# Decisions and Rationale
- Kept compatibility in page logic rather than changing data builder contract.
- This avoids regressions across existing payload producers and keeps the page resilient to either key shape.

# Validation
- `uv run pytest tests/test_objective_activity_matrix.py -q` passed.
- `uv run python scripts/run_slice.py --smoke` passed.
- Code review scripts executed:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

# Follow-up
- If desired, we can standardize on one payload text key (`text_values`) across all consumers and remove compatibility fallback later.

# Context / Scope
- Fixed runtime crash on matrix page caused by Plotly property compatibility in `selected.marker`.
- Error: `Invalid property ... selected.Marker: 'line'`.

# Main Changes
- Updated click-overlay scatter trace in `apps/pages/2_objective_activity_matrix.py`.
- Removed unsupported `selected.marker.line`.
- Kept selection highlight using supported fields only:
  - `color`
  - `opacity`
  - `size`

# Decisions and Rationale
- Chose compatibility-first fix to support installed Plotly version without changing matrix behavior.
- Kept heatmap visual style and click drilldown logic unchanged.

# Validation
- `uv run pytest tests/test_objective_activity_matrix.py -q` passed.
- `uv run python scripts/run_slice.py --smoke` passed.
- Code-reviewer scripts executed successfully.

# Follow-up
- If a stronger visual selection outline is needed later, we can implement it with an additional highlight trace instead of `selected.marker.line`.

# 2026-02-24 - Matrix scatter click reliability

## Context / scope
- Matrix cell click selection was unreliable for some metrics (notably `exercise_balanced_success_rate`).
- Goal: make click-driven drilldown stable across all matrix metrics without changing metric formulas or artifacts.

## Main changes made
- Replaced matrix rendering in `apps/pages/2_objective_activity_matrix.py`:
  - removed dual-trace model (`go.Heatmap` + invisible selector scatter),
  - added single-trace model with one `go.Scatter` square marker per populated cell.
- Rendering details:
  - marker `symbol="square"`,
  - dynamic marker size from grid density (rows/cols) with clamped bounds,
  - marker color driven by `metric_value`,
  - kept existing color scale,
  - kept `[0, 1]` bounds for rate metrics.
- Kept same hover payload semantics (`objective/activity labels and IDs`, `A#`, metric text).
- Simplified selection parsing:
  - customdata first,
  - `(x, y)` axis fallback via deterministic cell lookup.
- Preserved manual drilldown expander as backup fallback.
- Updated `ressources/STREAMLIT_FIGURES_GUIDE.md` with matrix interaction note.

## Important decisions and rationale
- Chose scatter-only to remove ambiguity between heatmap and overlay event capture.
- Blank cells remain non-clickable by design (only populated cells are rendered).
- No backend changes: metric computations and derived schemas unchanged.

## Validation results
- `uv run pytest tests/test_objective_activity_matrix.py -q` -> pass
- `uv run pytest -q` -> pass
- `uv run python scripts/run_slice.py --smoke` -> pass
- Code-reviewer workflow executed:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up actions
- Optional: add focused unit tests for `_extract_selected_cell` with synthetic scatter event payloads.

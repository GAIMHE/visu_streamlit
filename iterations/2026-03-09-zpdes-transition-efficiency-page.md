# 2026-03-09 - ZPDES Transition Efficiency Page

## Context / Scope
- Added a new scientific-analysis page to study whether incoming transitions into an activity are common and associated with weak downstream performance.
- Kept the existing structural ZPDES page unchanged.
- Reused current metadata and derived artifacts without schema changes.

## Main Changes
- Added `apps/pages/6_zpdes_transition_efficiency.py`.
- Added `src/visu2/zpdes_transition_efficiency.py` for:
  - activity node metric attachment,
  - incoming transition normalization,
  - transition-augmented graph rendering.
- Added unit tests in `tests/test_zpdes_transition_efficiency.py`.
- Updated lightweight documentation:
  - `README.md`
  - `ressources/STREAMLIT_FIGURES_GUIDE.md`
  - `ressources/figures/README.md`
  - `ressources/figures/zpdes_transition_efficiency_graph.md`
  - `ressources/figures/zpdes_transition_efficiency_ranking_table.md`

## Important Decisions
- New page rather than modifying the existing dependency page.
- Node coloring supports:
  - weighted first-attempt success over the selected date range
  - activity mean exercise Elo (date-invariant)
- Arrow width is normalized within the clicked destination activity only.
- Ranked table uses:
  - `non_optimal_score = transition_probability * (1 - conditional_destination_success_rate)`
- Hidden-source transitions remain in the ranked table and in normalization, but are not drawn if their source activity is outside the visible objective subset.

## Validation
- `uv run pytest tests/test_zpdes_transition_efficiency.py -q`
- `uv run pytest -q`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up
- If work-mode-specific transition analysis is needed later, the transition artifact will need a dedicated schema extension.
- Self-loop transitions are currently excluded from the overlay analysis.

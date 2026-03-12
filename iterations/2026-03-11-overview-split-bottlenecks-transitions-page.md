# Overview Simplification + Bottlenecks/Transitions Page

## Context / Scope
- Simplify the visible overview so it focuses on top-level learning analytics only.
- Move the bottleneck and path-transition analysis to a dedicated page immediately after the overview.
- Keep metric logic and artifact contracts unchanged.

## Main Changes Made
- Simplified `apps/streamlit_app.py` to keep only:
  - KPI cards
  - Work Mode Summary table
- Removed from the visible overview page:
  - Data Quality Panel
  - Success Rate by Work Mode table
  - Exploration width chart
  - Bottleneck Candidates chart
  - Path Transitions chart
- Added `apps/pages/1_bottlenecks_and_transitions.py` with:
  - the existing bottleneck ranking block
  - the existing path-transition block
  - the same sidebar filter semantics as before
- Added `apps/overview_shared.py` for shared:
  - dashboard styling
  - curriculum filter rendering
  - label formatting helpers
  - fact-query construction
  - transition-edge loading
  - lightweight runtime core-column checks
- Updated `apps/figure_info.py` so the overview only documents the surviving blocks and the moved charts now belong to the new page.
- Updated visible navigation/docs in:
  - `README.md`
  - `ressources/STREAMLIT_FIGURES_GUIDE.md`
  - `ressources/figures/README.md`
  - moved figure docs for bottlenecks/transitions

## Important Decisions and Rationale
- Page 1 now depends only on `fact_attempt_core.parquet` because it no longer renders aggregate-based charts.
- The new page owns `agg_activity_daily.parquet` and `agg_transition_edges.parquet` because those artifacts are only needed for bottlenecks/transitions.
- Runtime compatibility checks were narrowed to page-specific core columns instead of using the removed overview-wide data-quality section.
- Transition filtering on the new page keeps the previous semantics:
  - date, module, and activity filters apply
  - objective filter does not change the transition query itself

## Validation
- `uv run ruff check apps src scripts tests`
- `uv run pytest -q`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Visually review the new page ordering and the simplified overview in a browser.
- If desired, rename or archive the now-unlisted figure docs for removed overview blocks.

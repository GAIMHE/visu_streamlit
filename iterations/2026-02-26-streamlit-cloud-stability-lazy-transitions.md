## Context / Scope
- Investigated Streamlit Cloud instability where the first page rendered briefly and then the app crashed with health-check EOF / generic app error.
- Focused on `apps/streamlit_app.py` runtime behavior under constrained Cloud memory.

## Main Changes
- Removed eager loading of `agg_transition_edges.parquet` from page-level aggregate preload.
- Added lazy transition query path:
  - new helper `load_top_transition_edges(...)` reads transitions with `scan_parquet`, applies filters, aggregates, and returns top-N only.
- Added `_collect_lazy(...)` helper and switched heavy LazyFrame collections to streaming-preferred execution:
  - KPI collection
  - exercise-balanced KPI collection
  - work-mode summary collection
  - transition top-N aggregation path
- Kept existing UI and metric semantics unchanged.

## Important Decisions and Rationale
- **Decision:** optimize transition pipeline first instead of changing visualization semantics.
  - **Why:** `agg_transition_edges` has high in-memory footprint when loaded eagerly, and transition aggregation was a likely Cloud crash point.
- **Decision:** use streaming-preferred collection where possible.
  - **Why:** reduce peak memory during large aggregations with no contract/schema changes.

## Validation
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run pytest -q` passed.
- Ran code-reviewer scripts:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Redeploy Streamlit Cloud and verify startup stability.
- If instability persists, next step is page-scoped HF sync subsets to reduce startup work further.

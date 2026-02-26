## Context / Scope
- Investigated instability isolated to page 2 (`Usage, Playlist and Engagement`) in Streamlit Cloud.
- Symptoms suggested runtime pressure on large fact-table queries.

## Main Changes
- Added streaming-preferred lazy collector in page 2:
  - `_collect_lazy(...)` with `collect(engine="streaming")` fallback.
- Replaced heavy eager collects with streaming-preferred collections:
  - `exposure_filtered` aggregation pipeline
  - `perf` diligent panel aggregation pipeline
- Added cached, filter-aware classroom list loader:
  - `load_classroom_values(fact_path, start_date, end_date, module_code)`
  - avoids full unfiltered classroom-id scan on each rerun.

## Important Decisions and Rationale
- **Decision:** keep UI/metrics unchanged; only adjust execution strategy.
  - **Why:** preserve analytical behavior while reducing peak memory and rerun cost.
- **Decision:** filter classroom list by active date/module context.
  - **Why:** better performance and more relevant selector options.

## Validation
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run pytest -q` passed.
- Ran code-reviewer scripts:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Redeploy and verify page 2 stays stable under Cloud health checks.
- If instability persists, next step is page-scoped HF sync subsets to avoid downloading non-needed files at startup.

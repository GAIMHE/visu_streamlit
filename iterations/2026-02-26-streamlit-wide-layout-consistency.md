## Context / Scope
- Deployed Streamlit pages appeared noticeably narrower than local rendering.
- Goal: enforce consistent full-width layout across all pages.

## Main Changes
- Added explicit `st.set_page_config(layout="wide")` to each page script:
  - `apps/pages/1_usage_playlist_engagement.py`
  - `apps/pages/2_objective_activity_matrix.py`
  - `apps/pages/3_zpdes_dependencies.py`
  - `apps/pages/4_classroom_progression_replay.py`
- Added page titles/icons in each page config call.

## Important Decisions and Rationale
- **Decision:** set page config per page rather than relying only on the main script.
  - **Why:** deployed multipage behavior can differ if only one entrypoint config is applied; per-page config removes ambiguity.

## Validation
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run pytest -q` passed.
- Code-reviewer scripts executed:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Redeploy and confirm width consistency on all pages in Streamlit Cloud.

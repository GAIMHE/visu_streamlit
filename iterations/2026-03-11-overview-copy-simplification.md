# Overview Page Copy Simplification

## Context / Scope
- Simplify the visible content of the first overview page without changing any metrics or filters.
- Remove the KPI explainer block and make the page read more like an external-facing introduction.
- Rewrite the work-mode explanation so it describes the modes and the table metrics in plain language.

## Main Changes Made
- Kept the page title `Learning Analytics Overview` unchanged.
- Removed the `Overview KPIs` subheader and its `Info` expander.
- Reduced the KPI strip to three cards only:
  - `Attempts`
  - `Unique Students`
  - `Unique Exercises`
- Removed the overview-page computation of the two success KPIs because they are no longer displayed.
- Added a three-paragraph dataset context block directly below the KPI cards.
- Rewrote the `Work Mode Summary` info panel to explain:
  - what each work mode means (`zpdes`, `playlist`, `adaptive-test`, `initial-test`)
  - what the main metrics mean in reader-facing language
  - how to interpret breadth, repetition, and the two success-rate columns

## Important Decisions and Rationale
- The success-rate KPI cards were removed entirely rather than left hidden, so the page no longer computes unused top-level success summaries.
- The work-mode info panel keeps the shared `Info` structure, but the wording now targets external readers rather than implementation details.
- The dataset blurb remains static page text instead of an expander because it is part of the overview narrative, not figure help.

## Validation
- `uv run ruff check apps/streamlit_app.py apps/figure_info.py`
- `uv run pytest -q`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Visually check whether the dataset blurb spacing feels right on the deployed layout.
- If desired, the same audience-facing rewrite can be applied to the other page `Info` panels.

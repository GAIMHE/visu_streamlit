# Streamlit Figure Info Pass

## Context
- The active Streamlit pages already had figure explanations, but they were scattered across captions, legends, and ad hoc prompts.
- The goal of this pass was to make figure interpretation easier for an external reader without changing any metric, filter, or chart behavior.

## Main Changes Made
- Added a shared app helper in `apps/figure_info.py` to render collapsed `Info` panels with a consistent structure:
  - What it shows
  - Why it matters
  - Metrics
  - Controls that affect it
  - How to read / interact
- Added one `Info` panel to every active analytical block documented in `ressources/figures/README.md`.
- Removed or reduced the main static explanatory captions that were previously always visible.
- Kept dynamic operational text outside the `Info` panels where it still matters:
  - warnings
  - compatibility errors
  - frame/state captions
  - selection prompts

## Important Decisions and Rationale
- The `Info` header stays exactly `Info` and is collapsed by default to keep pages visually lighter.
- A single shared content registry was used so the wording stays consistent across pages and future edits remain centralized.
- Page titles and block titles stay visible; the `Info` panels replace most static explanatory prose rather than duplicating it.
- The pass remains UI-only: no metric logic, artifact schema, or interaction semantics were changed.

## Validation
- `uv run ruff check apps src scripts tests`
- `uv run pytest -q`
- `uv run python scripts/run_slice.py --smoke`

## Follow-Up Actions
- Review the live pages once in a browser to confirm the collapsed `Info` sections feel well placed, especially inside the dependency audit expander.
- If some `Info` panels feel too dense, trim them without changing the shared structure.

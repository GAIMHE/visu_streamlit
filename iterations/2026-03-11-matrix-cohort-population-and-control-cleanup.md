# Matrix Page Control Cleanup and Cohort Population Filter

## Context / Scope
- Simplify the Objective-Activity Matrix page controls and remove redundant UI paths.
- Add a real cohort-population selector so matrix metrics can be restricted to playlist or ZPDES attempts.
- Keep the all-mode default behavior intact for users who do not opt into the cohort filter.

## Main Changes Made
- Removed the `Show cell values` checkbox and made cell values always visible in the heatmap.
- Removed the `Debug matrix selection` sidebar control.
- Removed the `Manual drilldown selection` fallback expander.
- Added a new sidebar control:
  - `Cohort population`
  - options: `All modes`, `ZPDES mode`, `Playlist mode`
- Extended the matrix builders so that when a specific cohort population is selected:
  - non-Elo matrix metrics are recomputed from `fact_attempt_core`
  - exercise drilldown rows are also recomputed from `fact_attempt_core`
- Kept `activity_mean_exercise_elo` global:
  - the metric ignores date and cohort filtering semantics beyond the surrounding page context
- Restricted `playlist_unique_exercises` to all-mode or playlist mode only.

## Important Decisions and Rationale
- `agg_activity_daily` and `agg_exercise_daily` do not carry `work_mode`, so a real cohort-population filter cannot be implemented from those artifacts alone.
- The cohort filter therefore uses a fact-backed branch instead of a cosmetic UI control.
- The default `All modes` path still uses the existing aggregate-backed implementation for speed.
- The manual drilldown fallback was removed because the matrix click state is already persisted in session state and should be the single interaction model.

## Validation
- `uv run ruff check apps src scripts tests`
- `uv run pytest -q`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py src/visu2 --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Visually verify that the new `Cohort population` field is clear for external readers.
- If needed later, consider renaming `playlist_unique_exercises` when the page is in playlist-only mode to make the metric label even more explicit.

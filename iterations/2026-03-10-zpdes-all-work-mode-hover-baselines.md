# ZPDES All-Work-Mode Hover Baselines

## Context
- The transition-efficiency page already colors activity nodes by a selected metric and computes progression cohorts for one selected work mode.
- The requested UI change was to always expose first-attempt success and event counts for every work mode in the hover text, regardless of the selected cohort population.

## Main Changes Made
- Aggregated per-activity first-attempt success and event counts for all supported work modes:
  - `adaptive-test`
  - `initial-test`
  - `playlist`
  - `zpdes`
- Added those four baseline summaries to the activity-node hover payload on page 6.
- Kept the node coloring behavior unchanged:
  - `First-attempt success` still uses the selected work mode
  - `Activity mean exercise Elo` remains global and date-insensitive
- Hardened the helper so the all-work-mode summary path returns a stable empty schema when progression events are absent or schema-light.

## Important Decisions and Rationale
- The hover now serves two roles:
  - a cross-mode baseline comparison
  - a selected-mode cohort breakdown
- Keeping node coloring on the selected mode preserves coherence with the `before` / `after` / `in-activity` cohorts.
- The empty-schema fix avoids spurious failures in tests and Elo-only paths, where first-attempt progression rows may be intentionally absent.

## Validation
- `uv run ruff check src/visu2/zpdes_transition_efficiency.py tests/test_zpdes_transition_efficiency.py apps/pages/6_zpdes_transition_efficiency.py`
- `uv run pytest tests/test_zpdes_transition_efficiency.py -q`
- `uv run pytest -q`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py src/visu2/zpdes_transition_efficiency.py --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-Up Actions
- If the all-work-mode hover gets too dense, collapse the baseline lines into a shorter grouped block or a secondary expander in a later iteration.

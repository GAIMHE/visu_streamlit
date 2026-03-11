# Bottlenecks and Transitions Info Panel Narrowing

## Context / Scope
- Simplify the `Info` panels on the Bottlenecks and Transitions page.
- Keep only the two content types requested by the reader:
  - what is displayed
  - what the metrics mean

## Main Changes Made
- Rewrote `bottlenecks_transitions_bottleneck_chart` info content to keep only:
  - `What`
  - `Metrics`
- Rewrote `bottlenecks_transitions_path_chart` info content to keep only:
  - `What`
  - `Metrics`
- Removed the extra sections about why the chart matters, controls, and how to read/interact.
- Rephrased metric explanations in plain language rather than implementation-first wording.

## Important Decisions and Rationale
- The goal was to match the simpler external-reader style now used on the overview page.
- The remaining text explains only what is visible and how the reported numbers are computed at a high level.

## Validation
- `uv run ruff check apps/figure_info.py`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/figure_info.py --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- If desired, the same `What` / `Metrics` reduction can be applied to the other analytical pages for consistency.

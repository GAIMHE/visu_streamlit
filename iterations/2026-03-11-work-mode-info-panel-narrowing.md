# Work Mode Info Panel Narrowing

## Context / Scope
- Reduce the first-page work-mode info panel to only the content requested by the reader.
- Remove all extra interpretation points from that `Info` block.

## Main Changes Made
- Replaced the previous multi-section work-mode info content with exactly two sections:
  - `Work modes`
  - `Success metrics`
- Removed all other bullets about controls, breadth, repetition, and interaction advice.
- Kept the work-mode definitions aligned with the current page wording.
- Kept the success-rate explanations focused on how the two success columns are computed.

## Important Decisions and Rationale
- The `Info` block is now intentionally narrow so it does not compete with the table itself.
- The content stays reader-facing and avoids developer terminology beyond the minimum needed to explain the two success metrics.

## Validation
- `uv run ruff check apps/figure_info.py`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/figure_info.py --verbose`

## Follow-up Actions
- If desired, the same narrowing can be applied to other first-page `Info` blocks so the whole page follows the same tone.

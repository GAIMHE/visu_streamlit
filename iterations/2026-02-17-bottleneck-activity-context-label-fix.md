# Iteration: 2026-02-17 - Bottleneck Activity Context Label Fix

## Context/Scope
- Fix confusing activity-level bottleneck axis labels where objective UUIDs appeared as disambiguation context.
- Preserve ID-safe grouping and multi-level bottleneck behavior.

## Main Changes Made
- `src/visu2/bottleneck.py`
  - Updated level config to separate context label and context ID columns:
    - `Module`: context label/id from `module_code`
    - `Objective`: context label from `module_label`, fallback ID `module_code`
    - `Activity`: context label from `objective_label`, fallback ID `objective_id`
  - Updated grouping to include both context columns.
  - Added deduplication of grouping columns to avoid duplicate keys when context label/id are same.
  - Updated context fallback chain to prefer readable labels before IDs.

## Important Decisions and Rationale
- Keep activity axis human-readable by default.
- Use IDs only as fallback/technical disambiguation, not as primary axis context.
- Maintain one bar per entity ID and existing weighted bottleneck formula.

## Validation
- `uv run pytest -q` passed (`16` tests).
- `uv run python scripts/run_slice.py --smoke` passed.
- Verified with data check: objective UUID `74dd2ddb-5aa7-489d-bcb2-28e1d6e5b420` no longer appears in activity-level axis labels.
- Code-review scripts executed:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py .`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- If needed, add a UI toggle for `disambiguation style`: context label only vs context label + short ID.

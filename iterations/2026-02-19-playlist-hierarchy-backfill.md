# Iteration Log - Playlist Hierarchy Backfill from Exercise IDs

## Date
2026-02-19

## Context / Scope
Playlist rows in raw `adaptiv_math_history.parquet` contain placeholder hierarchy values (`objective_id` / `activity_id` = `"None"`) while still carrying valid `exercise_id`. This caused downstream analytics to under-report explored hierarchy in playlist mode.

## Main Changes Made
- Updated `src/visu2/derive.py` (`build_fact_attempt_core`) to:
  - normalize placeholder ID strings (`"None"`, empty, `null`, `nan`) to null for core ID fields;
  - join canonical summary exercise hierarchy (`exercise_id -> activity/objective/module`);
  - backfill missing `activity_id`, `objective_id`, labels, and module identifiers from exercise mapping when available;
  - classify mapping provenance with refined values:
    - `summary_activity`
    - `summary_exercise`
    - `admath_fallback`
    - `unmapped`
- Added test `tests/test_fact_playlist_backfill.py` to validate playlist placeholder backfill behavior end-to-end on synthetic inputs.
- Updated documentation in `ressources/data.md` with playlist placeholder caveat and current backfill coverage metrics.

## Important Decisions and Rationale
- Raw source file in `data/` remains unchanged; backfill is applied in derived contract layer (`fact_attempt_core`) to preserve source immutability and reproducibility.
- `summary.json` remains hierarchy authority for backfill; no hierarchy join is taken from `exercises.json` placeholder fields.
- Backfill is applied whenever IDs are missing/placeholder, regardless of work mode; playlist mode is the dominant impact case.

## Validation / Quality Checks
- Unit test added and passing:
  - `uv run pytest -q tests/test_fact_playlist_backfill.py`
- Full tests:
  - `uv run pytest -q`
- Smoke check:
  - `uv run python scripts/run_slice.py --smoke`
- Sample derived build run:
  - `uv run python scripts/build_derived.py --sample-rows 200000`

## Mandatory Review Workflow
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py src/visu2/derive.py --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py tests/test_fact_playlist_backfill.py --verbose`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- Run full (non-sampled) `build_derived` to refresh production local artifacts before dashboard interpretation.
- Optionally expose mapping-source split in UI diagnostics for transparent playlist coverage monitoring.

# 2026-02-26 - Page-2 Artifact Pruning and Lean Contract

## Context and scope
- Objective: reduce runtime payload by slimming derived schemas used by page 2 and removing one unused artifact.
- Constraint: keep application behavior unchanged for existing pages.
- Decision: schema change accepted (no backward compatibility requirement for previous derived schema).

## Main changes made
1. `fact_attempt_core` was slimmed to runtime-used columns:
   - Kept: `created_at`, `date_utc`, `user_id`, `classroom_id`, `playlist_or_module_id`, `objective_id`, `objective_label`, `activity_id`, `activity_label`, `exercise_id`, `data_correct`, `data_duration`, `session_duration`, `work_mode`, `attempt_number`, `module_id`, `module_code`, `module_label`.
   - Removed: `teacher_id`, `module_long_title`, `student_attempt_index`, `first_attempt_success_rate`, `objective_id_summary`, `mapping_source`, `fallback_code_raw`.
2. Slimmed page-2 aggregates:
   - `agg_module_usage_daily` -> `date_utc`, `module_code`, `module_label`, `attempts`, `unique_students`.
   - `agg_playlist_module_usage` -> removed `module_id`, `median_duration`.
   - `agg_module_activity_usage` -> removed `module_id`, `unique_classrooms`, `unique_playlists`.
3. Removed unused artifact:
   - `agg_student_module_exposure.parquet` removed from derive output, manifest table set, runtime required lists, and HF sync required list.
4. Updated compatibility contracts:
   - `DERIVED_SCHEMA_VERSION` bumped to `phase0_thin_slice_v3_lean`.
   - `REQUIRED_FACT_COLUMNS`, `REQUIRED_AGG_COLUMNS`, `RUNTIME_CORE_COLUMNS`, `RUNTIME_LABEL_COLUMNS` updated accordingly.
5. App/runtime updates:
   - `apps/pages/1_usage_playlist_engagement.py` no longer loads/expects `agg_student_module_exposure`.
   - `apps/streamlit_app.py` no longer requires/checks that artifact.
   - `scripts/build_derived.py`, `scripts/run_slice.py`, `src/visu2/hf_sync.py` updated to reflect new artifact set.
6. Tests/docs updates:
   - Updated tests affected by schema changes and removed artifact.
   - Updated README page-2 dataset mapping for Exposure Overview.

## Important decisions and rationale
- Size reduction should prioritize the largest file (`fact_attempt_core`) because page-2-only aggregate pruning has minimal impact alone.
- Keep existing artifact names for remaining tables to avoid loader churn.
- Remove dead artifact (`agg_student_module_exposure`) to simplify runtime and HF sync bundle.

## Validation results
1. Build and contracts:
   - `uv run python scripts/build_derived.py --strict-checks` -> pass.
   - `uv run python scripts/check_contracts.py --strict` -> pass.
2. Runtime smoke:
   - `uv run python scripts/run_slice.py --smoke` -> pass.
3. Test suite:
   - `uv run pytest -q` -> pass (`45 passed`).
4. Mandatory review workflow:
   - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose` -> executed.
   - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> executed (template-level report, no findings raised by script).

## Follow-up actions
1. Publish regenerated derived artifacts + manifest to Hugging Face with a new pinned revision.
2. Update deployment secret `VISU2_HF_REVISION` to the new revision.
3. Optionally remove stale local `agg_student_module_exposure.parquet` files from previously generated bundles to avoid confusion.

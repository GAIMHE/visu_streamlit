# Page-Scoped HF Runtime Sync

## Context / Scope
- Reduce Streamlit Cloud cold-start pressure during deployment.
- The previous runtime bootstrap downloaded the entire HF runtime bundle at startup, including large page-specific artifacts such as student Elo events and ZPDES progression events.

## Main Changes
- Added `apps/runtime_paths.py` to centralize per-page runtime asset subsets.
- Updated `apps/runtime_bootstrap.py` so pages can request only the files they need.
- Updated `src/visu2/hf_sync.py` so `ensure_runtime_assets_from_hf(...)` accepts an optional `required_paths` subset while preserving full-sync behavior for CLI/manual workflows.
- Wired the active Streamlit pages to page-scoped runtime subsets:
  - overview
  - bottlenecks/transitions
  - objective-activity matrix
  - ZPDES transition efficiency
  - classroom replay
  - student Elo
- Added a test covering the required-paths subset behavior in `tests/test_hf_sync.py`.
- Updated deployment docs to note that runtime sync is now page-scoped.

## Important Decisions and Rationale
- Full runtime sync remains available when no subset is provided, so existing CLI behavior and schema snapshot tooling do not break.
- The landing page now syncs only `fact_attempt_core.parquet`, which materially reduces cold-start download volume.
- Heavy artifacts such as `student_elo_events.parquet` and `zpdes_exercise_progression_events.parquet` are deferred until their respective pages are opened.

## Follow-up Actions
- Redeploy the app and verify that the default page passes Streamlit Cloud health checks.
- If cold starts are still unstable, the next lever is to split or further slim the largest page-specific artifacts (`student_elo_events.parquet`, `fact_attempt_core.parquet`, `zpdes_exercise_progression_events.parquet`).

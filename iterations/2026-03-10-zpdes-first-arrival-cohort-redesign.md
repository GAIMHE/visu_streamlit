# 2026-03-10 ZPDES First-Arrival Cohort Redesign

## Context / Scope
- Replaced the arrow-based ZPDES transition-efficiency view with a static cohort-based analysis.
- Added a new derived runtime artifact so the page no longer recomputes pairwise transitions from `fact_attempt_core` at runtime.
- Kept the existing structural ZPDES layout and node-coloring options.

## Main Changes Made
- Added `src/visu2/derive_zpdes.py` with `build_zpdes_first_arrival_events_from_fact(...)`.
- Added `catalog_activity_rank_frame(...)` in `src/visu2/derive_catalog.py` to derive canonical module-local activity ranks from `learning_catalog.json`.
- Registered the new artifact `artifacts/derived/zpdes_first_arrival_events.parquet` in:
  - `src/visu2/derive.py`
  - `src/visu2/contracts.py`
  - `scripts/build_derived.py`
  - `scripts/run_slice.py`
  - `src/visu2/hf_sync.py`
- Rewrote `src/visu2/zpdes_transition_efficiency.py` to:
  - keep node metric coloring
  - aggregate hover-only `before` / `after` cohort metrics
  - remove runtime transition-edge recomputation and empirical arrow overlay
- Rewrote `apps/pages/6_zpdes_transition_efficiency.py` to:
  - load the new artifact
  - expose an exact later-attempt threshold slider
  - render a static structural graph with cohort hover metrics
  - remove click-selected state and the ranked table
- Replaced the previous transition-overlay tests with first-arrival builder and threshold tests in `tests/test_zpdes_transition_efficiency.py`.
- Updated README, figures docs, data handling docs, and the database reference pack for the new artifact and page semantics.

## Important Decisions and Rationale
- Used a first-arrival event artifact instead of a daily aggregate so the later-attempt threshold can stay exact and adjustable at runtime.
- Defined `before` and `after` using canonical module order from `learning_catalog.json`, not dependency-edge direction.
- Applied module and work-mode restrictions before cohort classification.
- Applied the page date filter to the destination first-arrival event date only; earlier history used for classification can precede the selected range.
- Removed the click/table overlay because the redesigned page is now a static comparative cohort view.

## Follow-up Actions
- Rebuild derived artifacts locally with `uv run python scripts/build_derived.py --strict-checks`.
- Refresh HF runtime data with the new `zpdes_first_arrival_events.parquet` artifact before deployment.
- Optionally add an explicit `excluded` cohort metric later if the page needs to audit how many students fall outside both hover groups.

# Iteration: 2026-02-17 - Schema Guard and Artifact Freshness

## Context/Scope
- Implement runtime schema guard in Streamlit to prevent crashes from stale derived artifacts.
- Add deterministic derived artifact manifest during build.
- Enforce warn+fallback behavior for label drift, and blocking behavior for core schema incompatibility.

## Main Changes
- Added runtime schema constants in `src/visu2/contracts.py`:
  - `DERIVED_MANIFEST_VERSION`
  - `DERIVED_SCHEMA_VERSION`
  - `RUNTIME_CORE_COLUMNS`
  - `RUNTIME_LABEL_COLUMNS`
- Added derived manifest helpers in `src/visu2/reporting.py`:
  - `write_derived_manifest(...)`
  - `load_derived_manifest(...)`
- Extended settings with `derived_manifest_path` in `src/visu2/config.py`.
- Updated `scripts/build_derived.py` to:
  - profile written parquet outputs (row count, columns, dtypes)
  - generate `artifacts/reports/derived_manifest.json`
  - include build context (`sample_rows`, `strict_checks`, `checks_status`)
- Updated `apps/streamlit_app.py` startup flow to:
  - validate runtime compatibility against core/label contracts
  - inspect manifest presence/schema/version and table entries
  - classify runtime status: `ok`, `degraded`, `incompatible`
  - show artifact health panel + explicit rebuild command
  - block rendering only for `incompatible`
  - keep app usable with fallback labels for `degraded`
- Updated documentation in `README.md` with an "Artifact Freshness & Schema Guard" section.

## Decisions and Rationale
- `warn + fallback` was preserved for non-core drift to keep analyst workflows unblocked.
- Missing core runtime columns now trigger a hard stop to avoid invalid analytics.
- Manifest-based checks were added for deterministic freshness and easier debugging.

## Follow-ups
- Add a lightweight CLI validator command to print compatibility status without launching Streamlit.
- Optionally include a manifest checksum for each table file to detect path-stable rewrites.
- Consider adding CI checks for manifest schema expectations in a later iteration.

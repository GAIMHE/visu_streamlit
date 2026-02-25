# Iteration: 2026-02-17 - Streamlit Label Compatibility Fix

## Context/Scope
- Fix runtime crash in Streamlit when derived artifacts are from an older schema without label columns.
- Preserve human-readable UI where possible while keeping backward compatibility.

## Main Changes
- Added compatibility normalization in `apps/streamlit_app.py` to synthesize missing label columns from IDs/codes:
  - `module_label`, `objective_label`, `activity_label`
  - `from_activity_label`, `to_activity_label`, `from_module_label`
- Added UI warning when app is running with fallback labels due to stale artifacts.
- Fixed transition module filtering to use `from_module_code` (instead of only `module_code`).

## Decisions and Rationale
- Backward compatibility was added in the app layer so older artifacts do not block exploration.
- Canonical path remains rebuilding artifacts with strict checks; compatibility mode is fallback-only.

## Follow-ups
- Rebuild all derived artifacts after schema changes with:
  - `uv run python scripts/build_derived.py --strict-checks`
- Consider adding a derived-schema version marker in report metadata and app startup checks.

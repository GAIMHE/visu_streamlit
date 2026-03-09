# 2026-03-04 Whole-Codebase Python Cleanup

## Context / Scope
- Broad Python cleanup pass focused on maintainability and import stability.
- Keep app behavior, runtime data contracts, and deployment flow unchanged.
- Preserve in-progress local edits in the Elo and HF sync modules.

## Main Changes
- Split oversized core modules into smaller internal helpers while keeping stable facades:
  - `src/visu2/derive.py` now delegates to `derive_common.py`, `derive_catalog.py`, `derive_fact.py`, `derive_aggregates.py`, and `derive_elo.py`.
  - `src/visu2/objective_activity_matrix.py` now delegates to `matrix_types.py`, `matrix_ordering.py`, `matrix_cells.py`, and `matrix_drilldown.py`.
  - `src/visu2/zpdes_dependencies.py` now delegates to `zpdes_types.py`, `zpdes_topology.py`, and `zpdes_overlays.py`.
- Added concise module/function docstrings on active page entrypoints and CLI scripts.
- Tightened some internal typing by using structured payload helpers in the Elo path and stable schema helpers in the matrix path.
- Replaced one `map_elements`-based drilldown formatter with native Polars expressions.
- Added a lightweight `ruff` baseline to `pyproject.toml`.

## Important Decisions / Rationale
- Kept `derive.py`, `objective_activity_matrix.py`, and `zpdes_dependencies.py` as import-compatible facades so app pages and tests do not need to change imports.
- Did not change any artifact schemas or HF runtime file contracts.
- Kept the disabled page under `apps/disabled_pages/` untouched; this pass focused on active runtime code.
- Limited lint rules to `E`, `F`, `I`, and `UP` to keep cleanup practical and avoid docstring-style churn.

## Follow-Up
- `apps/streamlit_app.py` and `apps/pages/3_zpdes_dependencies.py` still contain substantial page logic and can be split further later.
- `src/visu2/loaders.py` and `src/visu2/checks.py` remain good next cleanup candidates.
- If the `ruff` baseline surfaces more style issues later, tighten gradually rather than enabling broad rules all at once.

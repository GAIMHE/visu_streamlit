# Iteration Log - 2026-02-19 - Metadata Source Migration

## Context / Scope

Goal of this iteration:
- Replace runtime dependencies on `summary.json`, `admathGraphe.json`, and `modules_configGraphe.xlsx`
- Use `learning_catalog.json` + `zpdes_rules.json` as runtime metadata contracts
- Keep compatibility-first behavior for ZPDES topology (no-loss target for current app workflows)

## Main Changes

1. Runtime configuration and loaders
- Added canonical metadata paths in `src/visu2/config.py`:
  - `learning_catalog_path`
  - `zpdes_rules_path`
- Added metadata loaders/adapters in `src/visu2/loaders.py`:
  - `load_learning_catalog`
  - `load_zpdes_rules`
  - `catalog_to_summary_frames`
  - `catalog_id_index_frames`
  - `zpdes_code_maps`

2. Derive and checks migration
- Migrated `src/visu2/derive.py` hierarchy/label/code mapping paths to new metadata.
- Updated mapping provenance values:
  - `catalog_activity`
  - `catalog_exercise`
  - `rules_code_fallback`
  - `unmapped`
- Migrated `src/visu2/checks.py` to load `learning_catalog` + `zpdes_rules`.

3. Matrix page migration
- Switched `apps/pages/2_objective_activity_matrix.py` metadata loading from summary to learning catalog.
- Kept matrix output contract unchanged.

4. ZPDES runtime migration
- Updated `apps/pages/3_zpdes_dependencies.py` to use:
  - `list_supported_module_codes_from_metadata`
  - `build_dependency_tables_from_metadata`
- Removed direct runtime reads of xlsx/admath in page 3.
- Runtime now prefers `zpdes_rules.dependency_topology` when present, else rules-based fallback parsing.

5. Reproducibility tooling
- Added `scripts/build_derived_metadata.py`:
  - refreshes metadata meta/hash fields
  - can inject compatibility `dependency_topology` into `zpdes_rules.json` from legacy xlsx+admath sources
- Added `scripts/validate_metadata_contracts.py`:
  - validates required keys and structural integrity for both metadata files

6. Tests and docs
- Updated playlist backfill unit test to new metadata sources in `tests/test_fact_playlist_backfill.py`.
- Added migration-focused tests in `tests/test_metadata_migration.py`.
- Updated migration-facing docs:
  - `README.md`
  - `ressources/data.md`
  - `ressources/roadmap_refined.md`

## Important Decisions and Rationale

1. Compatibility-first migration
- Runtime behavior parity is prioritized over immediate cleanup of all legacy helper functions.
- Legacy parsers are retained for metadata build tooling only.

2. ZPDES topology contract
- Runtime accepts `dependency_topology` as authoritative when available.
- Rules-only fallback remains for environments where topology snapshot is absent.

3. No runtime dependency on legacy files
- Old files remain only for regeneration/parity workflows, not for app runtime execution.

## Validation / Checks Run

Planned validation for completion:
- `uv run python scripts/validate_metadata_contracts.py`
- `uv run pytest -q`
- `uv run python scripts/build_derived.py --strict-checks`
- `uv run python scripts/run_slice.py --smoke`
- Mandatory code-reviewer pass scripts

## Follow-up Actions

1. Regenerate metadata with compatibility topology:
- `uv run python scripts/build_derived_metadata.py --write`
2. Rebuild derived artifacts after migration:
- `uv run python scripts/build_derived.py --strict-checks`
3. Re-check ZPDES parity module-by-module with and without topology snapshot.

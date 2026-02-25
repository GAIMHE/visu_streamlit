# 2026-02-23 - Repo Architecture Hygiene

## Context / Scope

Goal of this iteration:
- perform a balanced repository cleanup focused on runtime clarity and maintainability
- remove legacy runtime helper surfaces no longer needed after standalone metadata migration
- clean documentation references and define one canonical roadmap
- remove obsolete presentation build tooling from active code/dependencies
- run full validation + smoke + tests + mandatory review scripts

Constraints:
- keep runtime behavior unchanged
- keep legacy raw data in `data/legacy/` as historical lineage (non-runtime)

## Main Changes Made

1. Runtime code pruning (`src/visu2/loaders.py`)
- removed legacy/unused helpers from active public surface:
  - `load_summary`
  - `summary_to_frames` (replaced by private internal helper path)
  - `XlsxExtract`
  - `load_xlsx_extract`
  - `load_graph_mapped_ids`
  - `load_graph_id_code_map`
- removed unused `pandas` import
- kept standalone loaders/adapters used by runtime:
  - `load_learning_catalog`
  - `load_zpdes_rules`
  - `catalog_to_summary_frames`
  - `catalog_id_index_frames`
  - `zpdes_code_maps`

2. Runtime code pruning (`src/visu2/zpdes_dependencies.py`)
- removed legacy xlsx/admath transition helpers:
  - `AdmathEnrichment`
  - `load_module_sheet_for_code`
  - `extract_admath_enrichment`
  - `list_supported_module_codes`
  - `build_dependency_tables_from_module_frame`
- removed unused imports tied to removed code paths
- retained metadata-driven graph generation and module selection via standalone metadata

3. Test updates for removed symbols
- `tests/test_contracts.py`:
  - switched hierarchy fixture test from legacy summary helper to `catalog_to_summary_frames`
- `tests/test_zpdes_dependencies.py`:
  - removed legacy-helper test dependencies
  - kept coverage for metadata-era behavior (token parsing, overlays, objective filtering)
  - updated fixture provenance labels from legacy (`xlsx`/`none`) to standalone-era (`catalog`/`rules`)

4. Tooling cleanup
- removed obsolete script: `scripts/build_roadmap_presentation.py`
- updated `pyproject.toml` dependencies:
  - removed direct presentation deps (`python-pptx`, `pillow`, `pyyaml`)
- regenerated `uv.lock`

5. Documentation / information architecture cleanup
- `README.md`:
  - canonical roadmap updated to `ressources/roadmap_6m.md`
  - removed presentation build section and missing presentation file references
  - added docs index reference (`ressources/README_INDEX.md`)
- updated deprecation path wording to `data/legacy/*` in:
  - `ressources/data.md`
  - `ressources/DERIVED_FILES_FORMAT.md`
- added canonical documentation index:
  - `ressources/README_INDEX.md`
- archived non-canonical context docs under `ressources/archive/`

## Important Decisions and Rationale

1. Balanced cleanup over hard purge
- remove dead runtime helpers and broken doc references now
- keep historical iteration logs and legacy raw files for lineage

2. Canonical roadmap declaration
- `ressources/roadmap_6m.md` is the single active roadmap target referenced in README/docs

3. Presentation tooling removal
- tooling and dependencies removed from active project surface because they are not part of current runtime goals

4. Standalone runtime contract preserved
- runtime/build remains centered on:
  - `data/adaptiv_math_history.parquet`
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
  - `data/exercises.json`

## Validation / Quality Results

1. Metadata contract validation
- `uv run python scripts/validate_metadata_contracts.py` -> PASS

2. Strict consistency checks
- `uv run python scripts/check_contracts.py --strict` -> PASS

3. Strict derived build
- `uv run python scripts/build_derived.py --strict-checks` -> PASS

4. Smoke run
- `uv run python scripts/run_slice.py --smoke` -> PASS
- observed expected Streamlit bare-mode warnings (`ScriptRunContext`) only

5. Test suite
- `uv run pytest -q` -> PASS

6. Mandatory code-reviewer workflow
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose` -> completed
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> completed (no critical findings)
- note: the checker script in this repo accepts one target path, so `.` was used for a full-project pass

## Follow-up Actions

1. Optional future tightening
- if needed, add a lightweight repository lint check that fails on missing local doc paths in canonical docs (`README.md`, `ressources/README_INDEX.md`)

2. Optional local cleanup command (for future runs)
- current run already removed local non-venv caches/bytecode with a Python cleanup fallback
- if needed again, use:
  - `python -c "from pathlib import Path; import shutil; [shutil.rmtree(p, ignore_errors=True) for p in [Path('.pytest_cache'), Path('apps/__pycache__'), Path('apps/pages/__pycache__'), Path('scripts/__pycache__'), Path('src/visu2/__pycache__'), Path('tests/__pycache__')]]"`

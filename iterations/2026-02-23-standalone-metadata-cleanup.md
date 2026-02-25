# 2026-02-23 - Standalone Metadata Cleanup

## Context / Scope

Goal of this iteration:
- enforce a standalone 4-file runtime/data contract
  - `data/adaptiv_math_history.parquet`
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
  - `data/exercises.json`
- remove remaining runtime/build dependencies on legacy metadata files
- sanitize metadata payloads so old source references are no longer embedded
- update checks/docs to the new standalone contract

Out of scope:
- changing pedagogical metrics/visualizations
- removing deprecated legacy raw files from repository storage

## Main Changes Made

1. Runtime config cleanup:
- updated `src/visu2/config.py`
- removed legacy `Settings` paths:
  - `summary_json_path`
  - `admath_graph_json_path`
  - `modules_xlsx_path`

2. Consistency checks migration:
- rewrote `src/visu2/checks.py` to use neutral metadata-health metrics
- removed legacy source-string parsing logic (`summary/xlsx/admath` source flags)
- row count keys now use `catalog_*` naming
- overlap metrics use standalone/neutral keys
- added fallback reads for old unresolved-link key names for backward compatibility

3. Baseline contract update:
- updated `src/visu2/contracts.py` baseline keys and expected values to match new check outputs

4. ZPDES links contract update:
- updated `src/visu2/zpdes_dependencies.py`
- module support selection now reads `links_to_catalog.rule_module_ids` first
- fallback to legacy `admath_module_ids` kept for backward compatibility

5. Tests update:
- updated `tests/test_fact_playlist_backfill.py` to match new `Settings` signature

6. Metadata sanitization tooling:
- added `scripts/sanitize_metadata_standalone.py`
- implemented in-place sanitization for:
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
- sanitization behavior:
  - normalize `meta.source_files` to standalone package files
  - set `meta.generated_by` to sanitizer script
  - refresh `meta.build_timestamp_utc`
  - set `meta.history_file_used` to null when present
  - normalize/clean source tags
  - remove `incoming_source` recursively
  - enforce forbidden legacy token removal in keys and values
  - rename legacy keys to neutral names (for example `only_in_xlsx` -> `only_in_secondary`)
  - normalize legacy provenance labels (`xlsx`/`admath`/`summary`) to neutral labels (`rules`/`catalog`)
  - ensure `links_to_catalog.rule_module_ids` exists

7. Legacy tool removal:
- deleted `scripts/build_derived_metadata.py`

8. Metadata validation enhancements:
- updated `scripts/validate_metadata_contracts.py`
- added checks for:
  - standalone `meta.source_files` paths
  - forbidden legacy filename/value tokens
  - forbidden legacy key tokens (`summary`/`xlsx`/`admath`)
  - required `links_to_catalog.rule_module_ids`
  - required neutral unresolved-link keys

9. Documentation updates:
- updated `README.md` metadata tooling commands
- rewrote `ressources/README_HF.md` to standalone 4-file package docs
- updated `ressources/data.md` migration/deprecation wording
- rewrote `ressources/DERIVED_FILES_FORMAT.md` to standalone contract

## Important Decisions and Rationale

1. Keep old raw legacy files in repo, but deprecate:
- preserves historical traceability
- avoids runtime/build coupling

2. In-place sanitize metadata JSONs:
- keeps canonical file paths stable for users/scripts
- avoids duplicate standalone-copy drift

3. Keep backward fallback for `admath_module_ids` in runtime:
- protects compatibility with older metadata snapshots
- new contract is still enforced through validation (`rule_module_ids`)

4. Keep top-level consistency report shape stable:
- no consumer break for `status/row_counts/null_counts/time_span_utc/checks`
- overlap metric keys were modernized

## Validation / Quality Results

1. Metadata validation:
- `uv run python scripts/validate_metadata_contracts.py` -> PASS

2. Contract checks:
- `uv run python scripts/check_contracts.py --strict` -> PASS

3. Derived build strict:
- `uv run python scripts/build_derived.py --strict-checks` -> PASS

4. Tests:
- `uv run pytest -q` -> PASS (`35 passed`)

5. App smoke:
- `uv run python scripts/run_slice.py --smoke` -> PASS
- observed expected Streamlit bare-mode warnings (`ScriptRunContext`) only

6. Runtime independence proof:
- temporarily renamed:
  - `data/summary.json`
  - `data/admathGraphe.json`
  - `data/modules_configGraphe.xlsx`
- ran strict build + smoke successfully while hidden
- restored all three files after validation

7. Code-reviewer required pass:
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py src --verbose` -> completed
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py scripts --verbose` -> completed
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py tests --verbose` -> completed
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> no critical findings reported
- note: tool script prints its own deprecation warning (`datetime.utcnow()`), external to migration scope

## Follow-up Actions

1. Optional cleanup:
- remove or archive deprecated legacy files from `data/` when distribution policy allows

2. Optional metadata contract tightening:
- remove runtime fallback to `admath_module_ids` after all deployed metadata snapshots carry `rule_module_ids`

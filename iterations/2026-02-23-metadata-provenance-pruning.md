# 2026-02-23 - Metadata Provenance Pruning

## Context / Scope

User request for standalone metadata files:
- remove provenance-only fields from distributed JSON payloads
- specifically remove fields like:
  - `source_primary`
  - `source_enrichment`
  - `only_in_primary`
  - `only_in_secondary`

Goal:
- keep runtime/build behavior unchanged
- keep the 4-file standalone package contract
- ensure these fields do not reappear after future sanitization runs

## Main Changes Made

1. Updated sanitizer behavior:
- file: `scripts/sanitize_metadata_standalone.py`
- added recursive key pruning for provenance-only keys:
  - `incoming_source`
  - `source_primary`
  - `source_enrichment`
  - `only_in_summary`
  - `only_in_xlsx`
  - `only_in_primary`
  - `only_in_secondary`

2. Strengthened metadata validation:
- file: `scripts/validate_metadata_contracts.py`
- added explicit forbidden-key validation for the same provenance key set
- validation now fails if these keys are present anywhere in metadata JSON trees

3. Re-sanitized metadata payloads in place:
- `data/learning_catalog.json`
- `data/zpdes_rules.json`

4. Revalidated pipeline compatibility:
- strict metadata contract validation
- strict derived build
- Streamlit smoke run
- full pytest suite

## Important Decisions and Rationale

1. Remove provenance fields entirely (not just rename):
- aligns with user requirement for clean public metadata files
- avoids exposing historical build lineage fields that are not needed by consumers

2. Keep structural/quality aggregates:
- retained conflict/coverage aggregate counters used by consistency checks
- removed detailed per-item provenance lists that were unnecessary for runtime

3. Preserve runtime compatibility:
- ZPDES loader already backfills missing optional columns when needed
- no page-level behavior regression from removing these keys in JSON artifacts

## Validation / Quality Results

1. Metadata contract validation:
- `uv run python scripts/validate_metadata_contracts.py` -> PASS

2. Derived build:
- `uv run python scripts/build_derived.py --strict-checks` -> PASS

3. App smoke:
- `uv run python scripts/run_slice.py --smoke` -> PASS
- expected Streamlit bare-mode warnings only

4. Tests:
- `uv run pytest -q` -> PASS

5. Code-reviewer workflow:
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py scripts --verbose` -> completed
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> no critical findings

## Follow-up Actions

1. Optional:
- remove source columns from ZPDES UI audit tables if you want to hide provenance concepts from the interface too

2. Distribution readiness:
- after final metadata review, package/publish the 4 standalone files for external users

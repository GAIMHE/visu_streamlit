# Iteration: 2026-02-17 - Summary-First Fallback Mapping (admath + xlsx labels)

## Context/Scope
- Keep `summary.json` as canonical primary hierarchy source.
- Add deterministic fallback mapping for rows not covered by summary, using `admathGraphe.json` ID->code mapping and xlsx code labels where available.
- Improve human-readable coverage in derived analytics without changing source precedence.

## Main Changes
- Extended `src/visu2/loaders.py`:
  - `XlsxExtract` now includes code->label maps and module code->id map.
  - `load_xlsx_extract(...)` now parses `code` + `short_title`/`long_title` for module/objective/activity label dictionaries.
  - Added `load_graph_id_code_map(...)` returning `{mapped_id: code}` from `admathGraphe.json`.
- Updated `src/visu2/derive.py` fact build pipeline:
  - summary join remains first.
  - fallback code resolution from `admath` by priority: activity_id -> objective_id -> exercise_id -> playlist_or_module_id.
  - regex extraction of module/objective/activity code prefixes from fallback code.
  - fills null `module_code/module_id/module_label`, plus `objective_label` and `activity_label` from xlsx fallback code labels.
  - adds trace columns: `mapping_source` (`summary|admath_fallback|unmapped`) and `fallback_code_raw`.

## Decisions and Rationale
- Source precedence remains unchanged: summary is canonical; fallback enriches only missing rows.
- Fallback is deterministic and auditable through provenance columns.
- Xlsx is used for label enrichment only, not canonical membership truth.

## Validation
- `uv run pytest -q` passed.
- `uv run python scripts/build_derived.py --strict-checks` passed.
- `uv run python scripts/run_slice.py --smoke` passed.
- Coverage improvement after rebuild:
  - `module_code` null rows reduced from ~14.14% to ~0.023%.
  - mapping_source counts: summary `5,378,682`; admath_fallback `884,272`; unmapped `1,440`.

## Follow-ups
- Surface `mapping_source` in UI tables to make fallback usage explicit.
- Add an optional diagnostics panel listing top unresolved IDs by volume.
- Consider narrowing fallback for playlist analytics when one playlist maps to multiple module codes.

# Metadata Files Format (Standalone Contract)

This document defines the canonical format and usage contract for:

- `data/learning_catalog.json`
- `data/zpdes_rules.json`

These files are designed to be used with:

- `data/adaptiv_math_history.parquet`
- `data/exercises.json`

## 1. Standalone Package Contract

The dataset/runtime contract is exactly four files:

1. `adaptiv_math_history.parquet`
2. `learning_catalog.json`
3. `zpdes_rules.json`
4. `exercises.json`

Runtime and artifact generation must not depend on deprecated legacy files.

## 2. `learning_catalog.json` Contract

### Required top-level keys

- `meta`
- `id_label_index`
- `modules`
- `exercise_to_hierarchy`
- `conflicts`
- `orphans`

### `meta.source_files` contract

`meta.source_files` must include entries for:

- `data/adaptiv_math_history.parquet`
- `data/learning_catalog.json`
- `data/zpdes_rules.json`
- `data/exercises.json`

### Hierarchy contract

- Nested hierarchy: `modules -> objectives -> activities -> exercise_ids`
- Canonical exercise join bridge:
  - `exercise_to_hierarchy[exercise_id] -> {activity_id, objective_id, module_id}`

### Label/code contract

- `id_label_index[id]` must provide:
  - `type`
  - `code`
  - `short_title`
  - `long_title`
  - `sources` (`catalog`/`rules` tags only)

### Provenance sanitization rules

- No `incoming_source` keys are allowed.
- No legacy filename values are allowed:
  - `summary.json`
  - `summary_1.json`
  - `admathGraphe.json`
  - `modules_configGraphe.xlsx`

## 3. `zpdes_rules.json` Contract

### Required top-level keys

- `meta`
- `module_rules`
- `map_id_code`
- `links_to_catalog`
- `unresolved_links`

### Optional key

- `dependency_topology`

### `map_id_code` contract

- `code_to_id`: map of rule code -> canonical ID
- `id_to_codes`: map of canonical ID -> list of rule codes

### `links_to_catalog` contract

Must include:
- `rule_module_ids` (list of module IDs present in rule payload)

May include:
- `id_links`
- `catalog_module_ids`

### Provenance sanitization rules

- No legacy filename values are allowed:
  - `summary.json`
  - `summary_1.json`
  - `admathGraphe.json`
  - `modules_configGraphe.xlsx`

## 4. Runtime Usage Guidance

### Hierarchy-aware analytics

1. Start from attempt facts in `adaptiv_math_history.parquet`.
2. Map `exercise_id` with `learning_catalog.exercise_to_hierarchy`.
3. Resolve labels/codes with `learning_catalog.id_label_index`.

### Rule-aware analytics

1. Use `zpdes_rules.module_rules` for adaptive rule payloads.
2. Use `zpdes_rules.map_id_code` for code/ID normalization.
3. Use `dependency_topology` when present for graph-ready nodes/edges.

### Exercise content

Use `exercises.json` for instruction/type/config payloads.
Do not use `exercises.json` placeholder hierarchy fields as hierarchy truth.

## 5. Validation Commands

Validate metadata contracts:

```bash
uv run python scripts/validate_metadata_contracts.py
```

Sanitize metadata in place to standalone contract:

```bash
uv run python scripts/sanitize_metadata_standalone.py
```

## 6. Deprecated Legacy Files

The following files are deprecated and non-runtime:

- `data/legacy/summary.json`
- `data/legacy/admathGraphe.json`
- `data/legacy/modules_configGraphe.xlsx`

They may remain in the repository for historical traceability only.

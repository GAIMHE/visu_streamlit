## Context / Scope

Fix structural ZPDES graph bugs caused by stale `dependency_topology` node metadata
in `data/zpdes_rules.json`.

The issue surfaced when empirical transition rows referenced activity ids that
were valid in `data/learning_catalog.json` and `fact_attempt_core.parquet`, but
the graph node payload still carried outdated ids/labels for the same node codes.

## Main Changes Made

- Updated `src/visu2/zpdes_topology.py` so `build_dependency_tables_from_metadata(...)`
  no longer trusts `dependency_topology.nodes` verbatim.
- Added canonical catalog-backed helpers:
  - `_selected_catalog_module(...)`
  - `_catalog_node_map_for_module(...)`
  - `_normalize_topology_tables(...)`
  - `_reconcile_topology_nodes_with_catalog(...)`
- The topology path now:
  - keeps topology edges,
  - reconciles node ids/labels/objective membership against
    `data/learning_catalog.json`,
  - adds catalog nodes missing from the topology node list.
- Added a regression test in `tests/test_zpdes_dependencies.py` covering:
  - stale topology node ids,
  - stale topology labels,
  - missing catalog nodes.

## Important Decisions and Rationale

- The fix is applied at the shared metadata loader level so it benefits:
  - the standard ZPDES dependency graph page,
  - the ZPDES transition-efficiency page,
  - any future consumer of `build_dependency_tables_from_metadata(...)`.
- Edge payloads from `dependency_topology` are preserved to avoid changing rule
  semantics or graph connectivity in this iteration.
- Canonical pedagogical identity comes from `learning_catalog.json`, which is the
  correct source of truth for activity ids, codes, and titles.

## Follow-up Actions

- Consider adding a lightweight metadata audit script that reports topology nodes
  whose ids or labels drift from the current learning catalog.
- Consider exposing reconciliation warnings in the ZPDES page debug/metadata panel
  so stale metadata is visible without reading logs.

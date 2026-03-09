# zpdes_rules.json

## Goal
Dependency rules and topology for activation/deactivation logic across nodes.

## Physical Location
- `data/zpdes_rules.json`

## Producer
- Standalone metadata package maintained in data contract.

## Main Consumers
- `src/visu2/zpdes_dependencies.py` topology and dependency extraction
- `apps/pages/3_zpdes_dependencies.py` graph and rule details
- `src/visu2/checks.py` metadata health checks

## Main Fields
- `dependency_topology` node/edge structure when present.
- `links_to_catalog.rule_module_ids` for module availability filtering.
- Rule attributes (`edge_type`, thresholds, source metadata).

## Notes
- Runtime behavior is metadata-driven; no xlsx/admath legacy runtime dependency remains.

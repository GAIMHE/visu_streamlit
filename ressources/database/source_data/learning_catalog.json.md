# learning_catalog.json

## Goal
Canonical hierarchy and labels for modules/objectives/activities/exercises.

## Physical Location
- `data/learning_catalog.json`

## Producer
- Standalone metadata package maintained in data contract.

## Main Consumers
- `src/visu2/loaders.py` catalog loaders
- `src/visu2/objective_activity_matrix.py` ordering and labels
- `src/visu2/zpdes_dependencies.py` hierarchy context
- `src/visu2/derive_catalog.py` mapping frames

## Main Fields
- Hierarchy maps (`module_code`, `objective_id`, `activity_id`, `exercise_id`).
- Display labels used in UI rendering and drilldowns.
- Catalog-level metadata used for consistency checks.

## Notes
- Legacy provenance file references are intentionally removed in standalone mode.

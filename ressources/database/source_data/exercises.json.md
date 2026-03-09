# exercises.json

## Goal
Exercise-level metadata used in drilldowns and Elo enrichment.

## Physical Location
- `data/exercises.json`

## Producer
- Standalone metadata package maintained in data contract.

## Main Consumers
- `src/visu2/derive_catalog.py::exercise_metadata_frame`
- `src/visu2/derive_elo.py` exercise Elo artifact enrichment
- `apps/pages/2_objective_activity_matrix.py` instruction panel context

## Main Fields
- `exercise_id`, labels/types, and textual metadata.
- Fields needed to map exercise-level rows to UI drilldown entries.

## Notes
- The matrix instruction panel currently shows label text and optional placeholder image.

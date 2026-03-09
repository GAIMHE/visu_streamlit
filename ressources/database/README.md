# Database Reference Pack

This folder is the canonical deep reference for runtime data handling in VISU2.

## Source Data
- `source_data/adaptiv_math_history.parquet.md`
- `source_data/exercises.json.md`
- `source_data/learning_catalog.json.md`
- `source_data/zpdes_rules.json.md`

## Derived Runtime Tables
- `derived_tables/agg_activity_daily.md`
- `derived_tables/agg_activity_elo.md`
- `derived_tables/agg_exercise_daily.md`
- `derived_tables/agg_exercise_elo.md`
- `derived_tables/agg_module_activity_usage.md`
- `derived_tables/agg_module_usage_daily.md`
- `derived_tables/agg_objective_daily.md`
- `derived_tables/agg_student_module_progress.md`
- `derived_tables/agg_playlist_module_usage.md`
- `derived_tables/agg_transition_edges.md`
- `derived_tables/fact_attempt_core.md`
- `derived_tables/student_elo_events.md`
- `derived_tables/student_elo_profiles.md`

## Reports and Contracts
- `reports_and_contracts/consistency_report.json.md`
- `reports_and_contracts/derived_manifest.json.md`

## Lineage
- `lineage/runtime_pipeline.md`
- `lineage/page_consumption_map.md`

## Maintenance Workflow
1. Rebuild artifacts: `uv run python scripts/build_derived.py --strict-checks`
2. Export schema snapshot: `uv run python scripts/export_schema_snapshot.py --strict`
3. Update docs when contract/table/page usage changes.

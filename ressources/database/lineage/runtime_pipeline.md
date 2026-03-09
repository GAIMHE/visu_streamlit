# Runtime Pipeline Lineage

## Source -> Fact -> Aggregates

1. `data/adaptiv_math_history.parquet` + standalone metadata (`learning_catalog.json`, `zpdes_rules.json`, `exercises.json`).
2. `src/visu2/derive_fact.py::build_fact_attempt_core` builds `fact_attempt_core`.
3. Aggregate builders in `src/visu2/derive_aggregates.py` and Elo builders in `src/visu2/derive_elo.py` produce runtime parquet artifacts.
4. `scripts/build_derived.py` writes reports (`consistency_report.json`, `derived_manifest.json`).
5. Streamlit pages read artifacts directly through loaders and cached page-level readers.

## Contract Anchors

- Schema version: `src/visu2/contracts.py::DERIVED_SCHEMA_VERSION`
- Required columns: `src/visu2/contracts.py::RUNTIME_CORE_COLUMNS`
- Runtime sync surface: `src/visu2/hf_sync.py::DEFAULT_RUNTIME_RELATIVE_PATHS`

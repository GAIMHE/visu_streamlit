# derived_manifest.json

## Goal
Track derived schema version, runtime tables, and artifact freshness metadata.

## Physical Location
- `artifacts/reports/derived_manifest.json`

## Producer
- `scripts/build_derived.py::write_derived_manifest`

## Main Consumers
- `apps/streamlit_app.py` compatibility panel
- `scripts/run_slice.py` smoke checks

## Core Keys
- `schema_version`
- `generated_at_utc`
- `tables`
- `paths`

## Validation
- Generated during strict derive build.
- Read at app startup for compatibility and data quality messaging.

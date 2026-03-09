# consistency_report.json

## Goal
Expose pass/fail quality checks, row/null summaries, and metadata health checks.

## Physical Location
- `artifacts/reports/consistency_report.json`

## Producer
- `scripts/build_derived.py` through checks/reporting helpers.

## Main Consumers
- `apps/streamlit_app.py` Data Quality panel.

## Core Keys
- `status`
- `row_counts`
- `null_counts`
- `time_span_utc`
- `checks`

## Validation
- Generated during strict derive build.
- Read at app startup for compatibility and data quality messaging.

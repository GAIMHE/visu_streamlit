# Iteration: 2026-02-18 - Sidebar Label and Artifact UI Cleanup

## Context/Scope
- Remove technical artifact-health information from user-facing analytics pages.
- Ensure the main launch flow uses a domain-specific app entrypoint label.

## Main Changes
- `apps/pages/1_usage_playlist_engagement.py`
  - Removed sidebar artifact-health panel (`status/schema/rebuild command`) from UI.
  - Kept hard-stop behavior when core schema is incompatible.
- `apps/learning_analytics_overview.py` and `scripts/run_slice.py`
  - Main launch path remains the dedicated overview entrypoint so the app is not launched from `apps/streamlit_app.py`.

## Important Decisions and Rationale
- Runtime schema checks are still enforced, but only blocking states are surfaced to users.
- Operational/debug information is intentionally hidden from learner-facing UI.

## Validation
- `uv run python -m py_compile apps/pages/1_usage_playlist_engagement.py apps/streamlit_app.py apps/learning_analytics_overview.py` passed.
- `uv run pytest -q tests/test_objective_activity_matrix.py` passed.
- `uv run python scripts/run_slice.py --smoke` passed.

## Follow-up Actions
- If old sidebar naming still appears, restart Streamlit using the helper command so the new entrypoint is applied.

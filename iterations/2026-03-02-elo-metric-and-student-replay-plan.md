# Context

Added a retrospective Elo analysis layer to support exercise/activity difficulty calibration and student-level trajectory replay in the Streamlit app.

# Main Changes Made

- Added new derived artifacts:
  - `agg_exercise_elo.parquet`
  - `agg_activity_elo.parquet`
  - `student_elo_events.parquet`
  - `student_elo_profiles.parquet`
- Extended the matrix metric engine with `activity_mean_exercise_elo`.
- Added exercise-level Elo drilldown support on the matrix page.
- Added a new page: `apps/pages/5_student_elo_evolution.py`.
- Extended runtime contracts, manifest generation, smoke checks, and HF sync allow-list for the new artifacts.
- Updated documentation for the new Elo layer and page coverage.

# Important Decisions and Rationale

- Stage A uses first attempts only to calibrate exercise difficulty, so exercise Elo reflects first-encounter challenge rather than retry persistence.
- Stage B replays all attempts per student against frozen exercise Elo, so the student curve captures attempt-by-attempt movement.
- Activity Elo is the simple mean of calibrated exercise Elo values; uncalibrated exercises are excluded and exposed through coverage fields.
- The matrix date filter remains visible for consistency but is ignored for the Elo metric because the calibration is global and fixed.
- The new student Elo page reads only derived artifacts to keep runtime behavior aligned with the rest of the deployed app.

# Follow-Up Actions

- Rebuild derived artifacts with `uv run python scripts/build_derived.py --strict-checks`.
- Upload refreshed `artifacts/` to the HF dataset repo and update `VISU2_HF_REVISION`.
- Monitor the size of `student_elo_events.parquet`; if it becomes a runtime bottleneck, introduce checkpointed trajectory artifacts in a later iteration.

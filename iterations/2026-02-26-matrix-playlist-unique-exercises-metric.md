## Context / Scope
- Added a new Objective-Activity matrix metric requested for playlist analysis.
- Goal: show, per activity cell, how many distinct exercises were used in playlist mode.

## Main Changes
- Extended matrix metric contract:
  - Added `playlist_unique_exercises` to `VALID_MATRIX_METRICS`.
  - Added UI label in page 2 metric selector: `Playlist unique exercises`.
- Matrix builder (`src/visu2/objective_activity_matrix.py`):
  - `build_objective_activity_cells(...)` now supports `playlist_unique_exercises`.
  - New computation uses `fact_attempt_core` filtered by:
    - selected module/date range
    - `work_mode == "playlist"`
    - non-null objective/activity/exercise IDs
  - Aggregation at `(module_code, objective_id, activity_id)` with:
    - `n_unique(exercise_id)` as metric value.
- Drilldown behavior:
  - `build_exercise_drilldown_frame(...)` supports `playlist_unique_exercises` using `fact_attempt_core` (playlist-filtered) for selected activity.
  - Drilldown rows remain exercise-level with attempts/success/repeat/first-attempt stats in playlist scope.
- Page integration (`apps/pages/2_objective_activity_matrix.py`):
  - Detects playlist-metric availability from `fact_attempt_core` required columns.
  - Passes lazy `fact_attempt_core` to matrix builder and drilldown builder.
  - Shows info message if playlist metric is unavailable.

## Important Decisions and Rationale
- **Decision:** compute playlist metric from `fact_attempt_core`, not `agg_exercise_daily`.
  - **Why:** `agg_exercise_daily` does not contain `work_mode`, so playlist-only logic is not recoverable there.
- **Decision:** keep integer formatting for this metric.
  - **Why:** metric semantics are counts of unique exercises.

## Validation
- `uv run python scripts/run_slice.py --smoke` passed.
- `uv run pytest tests/test_objective_activity_matrix.py -q` passed.
- `uv run pytest -q` passed.
- Code-reviewer scripts run:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose`
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze`

## Follow-up Actions
- If needed, add a dedicated derived aggregate for playlist-only exercise usage to avoid scanning `fact_attempt_core` for this metric on very constrained runtimes.

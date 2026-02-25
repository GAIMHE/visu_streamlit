# VISU2 - Phase 0 + Thin Streamlit Slice

This repository now includes the first implementation iteration aligned with the canonical roadmap in `ressources/roadmap_6m.md`:

- Phase 0 contracts and consistency checks
- Derived dataset builders in `artifacts/derived/`
- A Streamlit thin slice app that reads only derived artifacts

## Project Structure

- `src/visu2/` core data and validation logic
- `scripts/check_contracts.py` consistency report generator
- `scripts/build_derived.py` derived data pipeline
- `scripts/run_slice.py` Streamlit run/smoke helper
- `apps/learning_analytics_overview.py` main Streamlit overview page
- `apps/streamlit_app.py` overview implementation module
- `apps/pages/3_zpdes_dependencies.py` ZPDES dependency visualization page
- `apps/pages/4_classroom_progression_replay.py` classroom progression replay page
- `ressources/README_INDEX.md` canonical documentation index
- `tests/` unit and shape tests
- `artifacts/` local output folder (ignored in VCS)

## Environment Setup (`uv`)

```bash
uv sync
```

This creates `.venv` and installs dependencies from `pyproject.toml`.

## Run Phase 0 Checks

```bash
uv run python scripts/check_contracts.py --strict
```

Output:
- `artifacts/reports/consistency_report.json`

## Build Derived Datasets

```bash
uv run python scripts/build_derived.py --strict-checks
```

Optional fast run:

```bash
uv run python scripts/build_derived.py --sample-rows 200000
```

Outputs:
- `artifacts/derived/fact_attempt_core.parquet`
- `artifacts/derived/agg_activity_daily.parquet`
- `artifacts/derived/agg_objective_daily.parquet`
- `artifacts/derived/agg_student_module_progress.parquet`
- `artifacts/derived/agg_transition_edges.parquet`
- `artifacts/derived/agg_module_usage_daily.parquet`
- `artifacts/derived/agg_student_module_exposure.parquet`
- `artifacts/derived/agg_playlist_module_usage.parquet`
- `artifacts/derived/agg_module_activity_usage.parquet`
- `artifacts/derived/agg_exercise_daily.parquet`
- `artifacts/reports/derived_manifest.json`

## Artifact Freshness & Schema Guard

`scripts/build_derived.py` now writes `artifacts/reports/derived_manifest.json` with:
- manifest/schema versions
- build context (`sample_rows`, `strict_checks`, check status)
- per-table metadata (`row_count`, `columns`, `dtypes`)

At app startup, the overview app validates runtime compatibility:
- `ok`: core + label columns are present and manifest schema matches.
- `degraded`: core columns are valid, but label columns are missing and/or manifest is missing or mismatched.
- `incompatible`: one or more core runtime columns are missing.

`degraded` mode keeps the app usable with ID/code label fallback.
`incompatible` mode blocks chart rendering and asks for a rebuild.

Rebuild command:

```bash
uv run python scripts/build_derived.py --strict-checks
```

## Usage/Playlist/Engagement Page

A second Streamlit page is available under multipage navigation:
- `apps/pages/1_usage_playlist_engagement.py`

Datasets consumed:
- `agg_module_usage_daily`
- `agg_student_module_exposure`
- `agg_playlist_module_usage`
- `agg_module_activity_usage`
- `fact_attempt_core` (for filter-aware subgroup comparison)

Diligent subgroup thresholds are configurable in the sidebar:
- minimum attempts (default `10`)
- minimum active days (default `3`)
- minimum total time in minutes (default `60`)

Pre/post analyses from IDEE notes are intentionally deferred until external IDEE post-test data is integrated with a dedicated contract.

## Objective-Activity Matrix Heatmap

A third Streamlit page is available under multipage navigation:
- `apps/pages/2_objective_activity_matrix.py`

Data consumed:
- `artifacts/derived/agg_activity_daily.parquet`
- `artifacts/derived/agg_exercise_daily.parquet`
- `data/learning_catalog.json` (ordering authority for objective/activity layout)

Controls:
- module selector (all observed non-null `module_code` values)
- date range (UTC)
- metric (`attempts`, `success_rate`, `exercise_balanced_success_rate`, `repeat_attempt_rate`, `first_attempt_success_rate`)
- toggles for cell annotations and ID visibility in hover

Metric definitions:
- `attempts`: `sum(attempts)` at objective/activity level over selected dates
- `success_rate`: `sum(success_rate * attempts) / sum(attempts)`
- `exercise_balanced_success_rate`: mean of per-exercise success rates (each exercise weighted equally)
- `repeat_attempt_rate`: `sum(repeat_attempt_rate * attempts) / sum(attempts)`
- `first_attempt_success_rate`: `sum(first_attempt_success_rate * first_attempt_count) / sum(first_attempt_count)`

Click drilldown:
- Click a matrix activity cell to open inline exercise-level metrics for that activity.
- Drilldown metric follows the matrix metric.
- Exercise labels use instruction snippet + short ID suffix.

Interpretation:
- Y axis is objective labels.
- X axis is objective-local activity position (`A1..An`), not global activity IDs.

## ZPDES Dependency Graph

A fourth Streamlit page is available under multipage navigation:
- `apps/pages/3_zpdes_dependencies.py`

Data consumed:
- `data/learning_catalog.json` (node labels and hierarchy context)
- `data/zpdes_rules.json` (dependency rules and code/id maps)
- `artifacts/derived/agg_activity_daily.parquet` (optional node metric overlays)

View model:
- objective lanes + activity nodes
- activation dependencies: solid blue
- deactivation dependencies: dashed red
- ghost nodes created for unresolved prerequisite references

Controls:
- module selector (intersection of `zpdes_rules` module codes + observed derived modules)
- objective multi-select inside selected module (to reduce graph density)
- date range (for optional overlays)
- node overlay (`Structure only`, `Attempts`, `Success rate`, `Repeat attempt rate`)
- intra-objective edge curvature toggle (improves readability for same-lane dependencies)
- hover ID toggle

Detail interactions:
- click a node/edge in the graph to open rule details
- dependency audit table available in-page for traceability

Unlock semantics:
- Each dependency edge uses the **source node** as mastery signal.
- Source `activity` code means activity-level mastery condition.
- Source `objective` code means objective-level mastery condition.

## Classroom Progression Replay

A fifth Streamlit page is available under multipage navigation:
- `apps/pages/4_classroom_progression_replay.py`

Purpose:
- Replay classroom progression over time as a dynamic `activity x student` matrix.
- Matrix starts empty, then fills in synchronized steps across students.
- Cell color reflects cumulative success rate for each `(student, activity)`.

Controls:
- Work mode scope: `zpdes` (default), `playlist`, `all`.
- Classroom selector (with auto-default rules by mode).
- Date range (UTC).
- Replay controls: play/pause, reset, frame slider, speed, step size (attempts per student per frame), max frame cap.
- Color controls: mastery threshold (default `0.75`) and optional cell values.

Replay metric:
- `cumulative_success_rate(student, activity, t) = cumulative_correct_attempts / cumulative_attempts` up to replay frame `t`.
- Unseen cells remain blank.
- In each frame, every student advances by the same number of local attempts while preserving intra-student chronology.
- For large sequences, replay applies an effective step to respect `max_frames`.

## Streamlit Thin Slice

Main overview page highlights:
- KPI cards: attempts, unique students, unique exercises, attempt-weighted success rate, exercise-balanced success rate
- Work Mode Performance section:
  - success rates by work mode (attempt-weighted + exercise-balanced)
  - work-mode footprint/depth analytics (modules/objectives/activities explored, median attempts per activity, repeat-attempt rate)

Smoke check:

```bash
uv run python scripts/run_slice.py --smoke
```

Run app:

```bash
uv run python scripts/run_slice.py --port 8501
```

## Tests

```bash
uv run pytest
```

## Mandatory Review Skill Commands

Per `AGENTS.md`, run `code-reviewer` for non-trivial changes:

```bash
uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py src scripts apps tests --verbose
uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze
```
## Metadata Contract Tooling

Validate metadata contracts:

```bash
uv run python scripts/validate_metadata_contracts.py
```

Sanitize metadata to standalone 4-file contract (in-place):

```bash
uv run python scripts/sanitize_metadata_standalone.py
```

# VISU2

https://adaptivisu.streamlit.app/

## Project and Data Context
VISU2 is a learning analytics and visualization project built on Adaptiv'Math interaction traces.  
The repository supports an interactive web app for exploration, visualisation and analysis.  

## Runtime Data Contract (4 Main Sources)
The pedagogical hierarchy is: `module -> objective -> activity -> exercise`.

| File | Role in project | Typical usage in app/pipeline |
|---|---|---|
| `data/adaptiv_math_history.parquet` | Raw attempt-level history | Source table for checks and derived artifacts |
| `data/learning_catalog.json` | Canonical hierarchy + labels + exercise mapping | Ordering, readable labels, hierarchy joins |
| `data/zpdes_rules.json` | ZPDES dependency and activation/deactivation rules | Dependency graph structure and rule details |
| `data/exercises.json` | Exercise metadata and instruction content | Exercise drilldown details |

Legacy sources in `data/legacy/` are historical lineage only and are not runtime dependencies.

## Project Structure
```text
apps/               Streamlit entrypoint and multipage UI
src/visu2/          Core logic (contracts, loaders, derivation, analytics helpers)
scripts/            CLI scripts (checks, build, run, metadata validation/sanitization)
tests/              Unit and integration-style tests
ressources/         Documentation and planning artifacts
iterations/         Dated implementation logs
data/               Raw/runtime metadata inputs (plus legacy historical files)
artifacts/          Local generated outputs (derived parquet + reports)
```

## Environment Setup and Run Flow
1. Install environment and dependencies:
```bash
uv sync --dev
```

2. Validate contracts and consistency:
```bash
uv run python scripts/check_contracts.py --strict
```

3. Build derived artifacts:
```bash
uv run python scripts/build_derived.py --strict-checks
```

4. Run the Streamlit app:
```bash
uv run python scripts/run_slice.py --port 8501
```

5. Optional smoke check:
```bash
uv run python scripts/run_slice.py --smoke
```

6. Optional lint check:
```bash
uv run ruff check apps src scripts tests
```

7. Optional schema snapshot export (for database docs maintenance):
```bash
uv run python scripts/export_schema_snapshot.py --strict
```

Expected outputs after checks/build:
- `artifacts/reports/consistency_report.json`
- `artifacts/reports/derived_manifest.json`
- `artifacts/derived/*.parquet`

## Deploy with HF Runtime Assets
For deployment, keep code on GitHub and store runtime data in a private Hugging Face dataset repository.

Runtime sync is automatic at app startup when these keys are configured:
- `VISU2_HF_REPO_ID` (for example: `org/visu2-runtime-data`)
- `VISU2_HF_REVISION` (pinned tag/commit, for reproducible dashboards)
- `HF_TOKEN` (private dataset access token)
- Optional: `VISU2_HF_REPO_TYPE` (default: `dataset`)
- Optional: `VISU2_HF_ALLOW_PATTERNS_JSON` (advanced override)

If `VISU2_HF_REPO_ID` is not set, the app runs in local-only mode.

Files expected in the HF dataset layout:
- `data/learning_catalog.json`
- `data/zpdes_rules.json`
- `data/exercises.json`
- `artifacts/reports/consistency_report.json`
- `artifacts/reports/derived_manifest.json`
- `artifacts/derived/*.parquet` (all runtime aggregates)

Optional prefetch command (outside Streamlit):
```bash
uv run python scripts/sync_runtime_assets.py --repo-id <org/repo> --revision <tag> --strict
```

## Streamlit Pages and Figures

### Page 1: Learning Analytics Overview (`apps/streamlit_app.py`)
This page is the compact entrypoint into the dashboard.  
It keeps only the high-level volume KPIs and the work-mode summary table.

- KPI cards (attempts, unique students, unique exercises)  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`
- Work Mode Summary Table  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

### Page 2: Bottlenecks and Transitions (`apps/pages/1_bottlenecks_and_transitions.py`)
This page focuses on friction points and navigation flows.  
It keeps the same bottleneck ranking and transition logic that previously lived on the overview page.

- Bottleneck Candidates  
  Dataset: `artifacts/derived/agg_activity_daily.parquet`
- Path Transitions  
  Dataset: `artifacts/derived/agg_transition_edges.parquet` (with labels from derived aggregates)

### Page 3: Objective-Activity Matrix Heatmap (`apps/pages/2_objective_activity_matrix.py`)
This page shows module-internal structure with objectives on rows and local activity positions (`A1..An`) on columns.  
It supports metric comparison and click-based drilldown to exercise level.

- Objective-Activity matrix metrics (`attempts`, success metrics, repeat rate, playlist unique exercises)  
  Datasets: `artifacts/derived/agg_activity_daily.parquet`, `artifacts/derived/fact_attempt_core.parquet`
- Activity mean exercise Elo metric  
  Datasets: `artifacts/derived/agg_activity_elo.parquet`, `artifacts/derived/agg_exercise_elo.parquet`
- Exercise-balanced metric and exercise drilldown  
  Dataset: `artifacts/derived/agg_exercise_daily.parquet`
- Objective/activity ordering and labels  
  Dataset: `data/learning_catalog.json`
- Exercise instruction panel (row click in drilldown table)  
  Datasets: `artifacts/derived/agg_exercise_daily.parquet`, `artifacts/derived/agg_exercise_elo.parquet`, placeholder image `images/placeholder_exo.png`

### Page 4: ZPDES Transition Efficiency (`apps/pages/3_zpdes_transition_efficiency.py`)
This page uses the structural ZPDES lane layout and focuses on ZPDES-mode progression only.  
It is designed to show whether first attempts on new exercises differ for students coming from earlier content, already-later content, or prior work inside the same activity.

- Structural ZPDES layout with activity coloring  
  Datasets: `data/zpdes_rules.json`, `data/learning_catalog.json`, `artifacts/derived/agg_activity_daily.parquet`, `artifacts/derived/agg_activity_elo.parquet`
- Hover-based ZPDES first-attempt summaries plus before/after/in-activity cohort summaries  
  Dataset: `artifacts/derived/zpdes_exercise_progression_events.parquet`

### Page 5: Classroom Progression Replay (`apps/pages/4_classroom_progression_replay.py`)
This page replays class progression over time as a student-by-activity matrix.  
It helps inspect pace synchronization, divergence, and emerging bottlenecks in classroom contexts.

- Replay matrix (`student x activity`) and cumulative progression states  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

### Page 6: Student Elo Evolution (`apps/pages/5_student_elo_evolution.py`)
This page replays one or two student Elo trajectories over their own local attempt sequence.  
It is useful for comparing progression pace, stability, and recovery after failures against fixed exercise difficulty.

- Student Elo replay line chart  
  Dataset: `artifacts/derived/student_elo_events.parquet`
- Student selector and summary cards  
  Dataset: `artifacts/derived/student_elo_profiles.parquet`

## Further Reading
- `ressources/README_HF.md`
- `ressources/STREAMLIT_FIGURES_GUIDE.md`
- `ressources/figures/README.md`
- `ressources/DATA_HANDLING.md`
- `ressources/database/README.md`
- `ressources/DOCSTRING_CONVENTIONS.md`

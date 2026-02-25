# VISU2

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

Expected outputs after checks/build:
- `artifacts/reports/consistency_report.json`
- `artifacts/reports/derived_manifest.json`
- `artifacts/derived/*.parquet`

## Streamlit Pages and Figures

### Page 1: Learning Analytics Overview (`apps/streamlit_app.py`)
This page gives a high-level view of usage, performance, friction points, and navigation flows.  
It is designed to start broad (KPIs/work modes) and then move to bottlenecks and paths.

- KPI cards (attempts, unique students, unique exercises, success rate)  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`
- Work Mode Performance / Footprint & Depth  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`
- Bottleneck Candidates  
  Dataset: `artifacts/derived/agg_activity_daily.parquet`
- Path Transitions  
  Dataset: `artifacts/derived/agg_transition_edges.parquet` (with labels from derived aggregates)
- Data Quality Panel  
  Datasets: `artifacts/reports/consistency_report.json`, `artifacts/reports/derived_manifest.json`

### Page 2: Usage, Playlist and Engagement (`apps/pages/1_usage_playlist_engagement.py`)
This page focuses on exposure intensity, time trends, playlist/module usage patterns, and subgroup comparison.  
It is useful for understanding how learners and classes interact with content at module/activity levels.

- Exposure Overview  
  Dataset: `artifacts/derived/agg_student_module_exposure.parquet`
- Module Usage Trends (attempts and students over time)  
  Dataset: `artifacts/derived/agg_module_usage_daily.parquet`
- Module/Playlist Analytics  
  Dataset: `artifacts/derived/agg_playlist_module_usage.parquet`
- Activity Usage within Module  
  Dataset: `artifacts/derived/agg_module_activity_usage.parquet`
- Diligent Learners Panel (threshold-based subgroup)  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

### Page 3: Objective-Activity Matrix Heatmap (`apps/pages/2_objective_activity_matrix.py`)
This page shows module-internal structure with objectives on rows and local activity positions (`A1..An`) on columns.  
It supports metric comparison and click-based drilldown to exercise level.

- Objective-Activity matrix metrics  
  Dataset: `artifacts/derived/agg_activity_daily.parquet`
- Exercise-balanced metric and exercise drilldown  
  Dataset: `artifacts/derived/agg_exercise_daily.parquet`
- Objective/activity ordering and labels  
  Dataset: `data/learning_catalog.json`
- Exercise instruction panel (row click in drilldown table)  
  Datasets: `artifacts/derived/agg_exercise_daily.parquet`, placeholder image `images/placeholder_exo.png`

### Page 4: ZPDES Dependency Graph (`apps/pages/3_zpdes_dependencies.py`)
This page visualizes unlocking dependencies between objectives and activities, with optional metric overlays.  
It helps connect rule structure with observed performance.

- Dependency graph structure and rules  
  Dataset: `data/zpdes_rules.json`
- Labels and hierarchy context  
  Dataset: `data/learning_catalog.json`
- Optional node performance overlays  
  Dataset: `artifacts/derived/agg_activity_daily.parquet`

### Page 5: Classroom Progression Replay (`apps/pages/4_classroom_progression_replay.py`)
This page replays class progression over time as a student-by-activity matrix.  
It helps inspect pace synchronization, divergence, and emerging bottlenecks in classroom contexts.

- Replay matrix (`student x activity`) and cumulative progression states  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

## Further Reading
- `ressources/README_HF.md`

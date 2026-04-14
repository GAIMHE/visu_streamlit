# VISU2

https://adaptivisu.streamlit.app/

## Project and Data Context
VISU2 is a learning analytics and visualization project built on several student-interaction datasets.  
The main source is Adaptiv'Math mathematics data, and the repo also includes a MIA source and a Maureen source from a French-language remediation course.  
The repository supports a source-aware web app for exploration, visualisation and analysis.  
Active Streamlit figures now pair their visual output with:
- an `Info` expander for human-readable methodology,
- an `Analysis` expander for deterministic, page-scope findings tied to the figure theme, with ranked summaries, lightweight statistical checks when the page data supports them, and caveats when the evidence is too thin.

## Dataset Sources and Common Runtime Contract
The pedagogical hierarchy is: `module -> objective -> activity -> exercise`.

Current supported sources:
- `main`: large Adaptiv'Math mathematics source
- `mia_module1`: MIA single-module source
- `maureen_m16fr`: French-language remediation source

The shared runtime logic is anchored on the main reference files below, then normalized source by source through the build pipeline.

| File | Role in project | Typical usage in app/pipeline |
|---|---|---|
| `data/adaptiv_math_history.parquet` | Raw attempt-level history | Source table for checks and derived artifacts |
| `data/learning_catalog.json` | Canonical hierarchy + labels + exercise mapping | Ordering, readable labels, hierarchy joins |
| `data/zpdes_rules.json` | ZPDES dependency and activation/deactivation rules | Dependency graph structure and rule details |
| `data/exercises.json` | Exercise metadata and instruction content | Exercise drilldown details |

Legacy sources in `data/legacy/` are historical lineage only and are not runtime dependencies.
Source-local runtime namespaces under `artifacts/sources/<source_id>/data/` now normalize the attempt parquet to `student_interaction.parquet` for every source, even when the raw input file keeps its historical name.

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

For the practical runtime-data workflow, start with `ressources/DATA_HANDLING.md`.

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

The deployed app now uses page-scoped runtime sync:
- the landing page downloads only its own required assets
- heavier page-specific artifacts are downloaded when those pages are opened

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

The app now runs through a custom source-aware shell. Runtime assets are resolved per source under:

- `artifacts/sources/main/...`
- `artifacts/sources/mia_module1/...`
- `artifacts/sources/maureen_m16fr/...`

Only pages supported by the active source are shown in the UI.

Important note:
- the broader page list below is the reference landscape for the multipage app
- the current `inspection` branch may expose a reduced surface centered on the Cohort Filter Viewer

### Page 1: Learning Analytics Overview (`apps/streamlit_app.py`)
This page is the compact shell entrypoint into the dashboard.  
It keeps the high-level volume KPIs, the work-mode summary table, a concentration view of attempt volume across both content and students, and a global Sankey of work-mode changes across full student histories.

- KPI cards (attempts, unique students, unique exercises)  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`
- Work Mode Summary Table  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`
- Attempt Concentration chart + drilldown table  
  Datasets: `artifacts/derived/fact_attempt_core.parquet`, `data/learning_catalog.json`
- Work Mode Transitions Sankey  
  Dataset: `artifacts/derived/work_mode_transition_paths.parquet`

### Page 2: Bottlenecks and Transitions (`apps/page_modules/1_bottlenecks_and_transitions.py`)
This page focuses on friction points and navigation flows.  
It keeps the same bottleneck ranking and transition logic that previously lived on the overview page.

- Bottleneck Candidates  
  Dataset: `artifacts/derived/agg_activity_daily.parquet`
- Path Transitions  
  Dataset: `artifacts/derived/agg_transition_edges.parquet` (with labels from derived aggregates)

### Page 3: Objective-Activity Matrix Heatmap (`apps/page_modules/2_objective_activity_matrix.py`)
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

### Page 4: ZPDES Transition Efficiency (`apps/page_modules/3_zpdes_transition_efficiency.py`)
This page uses the structural ZPDES lane layout and focuses on ZPDES-mode progression only.  
It is designed to show whether first attempts on new exercises differ for students coming from earlier content, already-later content, or prior work inside the same activity.

- Structural ZPDES layout with activity coloring  
  Datasets: `data/zpdes_rules.json`, `data/learning_catalog.json`, `artifacts/derived/agg_activity_daily.parquet`, `artifacts/derived/agg_activity_elo.parquet`
- Hover-based ZPDES first-attempt summaries plus before/after/in-activity cohort summaries  
  Dataset: `artifacts/derived/zpdes_exercise_progression_events.parquet`

### Page 5: Module 1 Individual Path (`apps/page_modules/8_m1_individual_path.py`)
This page replays one student's full Module 1 path on top of the fixed M1 dependency layout.  
It is useful for understanding the order of visited activities, retries, and work-mode changes inside the M1 topology.

- Student-level M1 replay graph  
  Datasets: `artifacts/derived/fact_attempt_core.parquet`, `artifacts/derived/student_elo_profiles.parquet`, `data/learning_catalog.json`, `data/zpdes_rules.json`

### Page 6: Classroom Progression Replay (`apps/page_modules/4_classroom_progression_replay.py`)
This page replays class progression over time as a student-by-activity matrix.  
It helps inspect pace synchronization, divergence, and emerging bottlenecks in classroom contexts.
Classrooms are selected by target student count inside the current work-mode scope.

- Replay matrix (`student x activity`) and cumulative progression states  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

### Page 7: Student Elo Evolution (`apps/page_modules/5_student_elo_evolution.py`)
This page compares one student Elo trajectory over that student's own local attempt sequence under two fixed-difficulty systems.  
It is useful for comparing how the current retrospective item-Elo calibration and the new iterative offline calibration change the same student replay.

- Student Elo comparison replay line chart  
  Datasets: `artifacts/derived/student_elo_events.parquet`, `artifacts/derived/student_elo_events_iterative.parquet`
- Student selector and summary cards  
  Datasets: `artifacts/derived/student_elo_profiles.parquet`, `artifacts/derived/student_elo_profiles_iterative.parquet`
- Exercise-difficulty comparison  
  Datasets: `artifacts/derived/agg_exercise_elo.parquet`, `artifacts/derived/agg_exercise_elo_iterative.parquet`

### Page 8: Classroom Progression Sankey (`apps/page_modules/6_classroom_progression_sankey.py`)
This page renders a static, stage-based Sankey for one selected classroom.  
It keeps the replay page's classroom-selection workflow, but summarizes the final first-time activity paths instead of animating synchronized frames.

- Classroom activity-progression Sankey  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

### Page 9: Student Objective Spider (`apps/page_modules/7_student_objective_spider.py`)
This page profiles one selected student inside one selected module through an objective-level radar chart.  
It combines performance and breadth by overlaying all-attempt success rate with catalog-relative exercise coverage across every objective in the chosen module.

- Student objective radar chart  
  Datasets: `artifacts/derived/fact_attempt_core.parquet`, `data/learning_catalog.json`

### Internal Tool: Cohort Filter Viewer (`apps/page_modules/9_cohort_filter_viewer.py`)
This page is an internal cohort-definition and attrition tool.  
It is useful for checking how many students and attempts remain after applying filters on modules, history length, retries, and work-mode schemas.

- Cohort funnel, final-slice summaries, module shares, and schema shares  
  Dataset: `artifacts/derived/fact_attempt_core.parquet`

## Further Reading
- `ressources/README.md`
- `ressources/README_HF.md`
- `ressources/STREAMLIT_FIGURES_GUIDE.md`
- `ressources/figures/README.md`
- `ressources/DATA_HANDLING.md`
- `ressources/ELO.md`
- `ressources/database/README.md`
- `ressources/DOCSTRING_CONVENTIONS.md`

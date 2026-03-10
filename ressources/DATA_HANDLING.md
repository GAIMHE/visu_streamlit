# Data Handling in VISU2 (Raw Data, Derived Artifacts, Streamlit, Hugging Face)

This document explains, in practical terms, how data moves through this repository:
1. what the source files are,
2. what artifacts are and why they exist,
3. how the Streamlit app loads local runtime data,
4. how the deployed app fetches runtime data from Hugging Face,
5. how the Elo layer is derived and interpreted.

## 1) The 4 source files (inputs)

The project is built around a standalone 4-file package:

- `data/adaptiv_math_history.parquet`
  - The raw interaction history (one row = one attempt on one exercise).
  - This is the ground-truth behavior table.
- `data/learning_catalog.json`
  - The pedagogical structure and label dictionary.
  - Think: the curriculum map (`module -> objective -> activity -> exercise`).
- `data/zpdes_rules.json`
  - The dependency and unlocking rules used in ZPDES (adaptive sequencing).
- `data/exercises.json`
  - Exercise content and metadata (instructions, type, etc.).

Important: the Streamlit app is designed to run from derived artifacts for performance. It does not need to scan the full raw Parquet at runtime in deployment.

## 2) What are artifacts?

Artifacts are **derived (precomputed) data files** stored under `artifacts/`.

They exist for two reasons:
1. **Speed**: interactive dashboards should not re-scan or re-aggregate millions of rows for every user action.
2. **Consistency**: cleaning, mapping, and aggregation are done once with a deterministic pipeline, so every page uses the same definitions.

Artifacts are not random smaller dataframes. They are:
- either a cleaned, normalized core fact table used for filtering and drilldowns,
- or aggregated tables at a coarser grain (daily, per-activity, per-transition, per-student replay, etc.).

### 2.1 Artifact directories

- `artifacts/derived/`
  - Parquet tables used by the app.
- `artifacts/reports/`
  - JSON reports used for runtime health and reproducibility.

### 2.2 Main derived tables (high-level meaning)

Common patterns:
- `fact_*` tables: attempt-level (still large, but cleaner and consistent).
- `agg_*` tables: pre-aggregated metrics used for charts.

Current runtime set includes:
- `artifacts/derived/fact_attempt_core.parquet`
  - Attempt-level table (cleaned IDs/labels, playlist backfill logic applied).
  - This is what pages scan for KPIs, work-mode analytics, and some drilldowns.
- `artifacts/derived/agg_activity_daily.parquet`
  - One row per (day x activity); used for trends and bottleneck metrics.
- `artifacts/derived/agg_objective_daily.parquet`
  - One row per (day x objective).
- `artifacts/derived/agg_transition_edges.parquet`
  - Transition counts between activities (`from -> to`).
- `artifacts/derived/agg_exercise_daily.parquet`
  - One row per (day x exercise) within activity/objective/module context.
- `artifacts/derived/agg_exercise_elo.parquet`
  - One row per exercise with its fixed retrospective Elo difficulty estimate.
- `artifacts/derived/agg_activity_elo.parquet`
  - One row per activity with the mean of the calibrated exercise Elo values inside it.
- `artifacts/derived/student_elo_events.parquet`
  - One row per replayable student attempt, with pre/post Elo values.
- `artifacts/derived/student_elo_profiles.parquet`
  - One row per student for selector cards and replay filtering.
- `artifacts/derived/zpdes_first_arrival_events.parquet`
  - One row per `student x destination activity x work_mode`, capturing first-arrival outcome and prior-history counts used by the ZPDES transition-efficiency page.
- page-2 usage aggregates:
  - `agg_module_usage_daily.parquet`
  - `agg_playlist_module_usage.parquet`
  - `agg_module_activity_usage.parquet`

### 2.3 Reports: manifest + consistency report

- `artifacts/reports/derived_manifest.json`
  - A machine-readable inventory of derived tables:
    - list of tables,
    - row counts,
    - column names and dtypes,
    - a `schema_version`.
  - The app checks that `schema_version` matches `src/visu2/contracts.py:DERIVED_SCHEMA_VERSION`.
- `artifacts/reports/consistency_report.json`
  - Data-quality checks about the raw Parquet and metadata integrity (counts, null rates, mapping coverage, etc.).
  - Used by the UI Data Quality / Health panel.

## 3) How artifacts are built (local pipeline)

Artifacts are generated from the 4 source files using:

1. Run consistency checks (writes the report):
   - `uv run python scripts/check_contracts.py --strict`
2. Build derived tables and the manifest:
   - `uv run python scripts/build_derived.py --strict-checks`

Where the logic lives:
- `src/visu2/derive.py`: transforms raw and metadata into derived Parquet tables.
- `src/visu2/contracts.py`: defines required columns and the schema version.
- `src/visu2/checks.py`: builds `consistency_report.json`.

## 4) How Streamlit loads data (local paths)

All pages load data from local file paths (relative to repo root):
- metadata: `data/*.json`
- artifacts: `artifacts/derived/*.parquet`, `artifacts/reports/*.json`

Implementation notes:
- Pages typically use Polars:
  - `pl.read_parquet(...)` for smaller tables
  - `pl.scan_parquet(...)` for large tables + filtering (lazy execution)
- Streamlit caching:
  - `@st.cache_data` is used on load helpers so repeat navigation is faster.
  - Some lazy plans are collected with `engine="streaming"` to reduce peak memory.

## 5) How Hugging Face fits in (HF-backed runtime sync)

### 5.1 The core idea

The deployed app still reads the same local paths.
The difference is: at startup, it can download the required runtime files from a Hugging Face dataset repo into the local filesystem.

This is a sync-then-run model:
1. Download expected files into `data/` and `artifacts/`.
2. Run the app normally (no code path changes for loading).

### 5.2 Where this is implemented

- `apps/runtime_bootstrap.py`
  - Called at the start of each page.
  - Uses Streamlit secrets plus environment variables.
  - If HF sync fails: it hard-stops the app with an actionable error.
- `src/visu2/hf_sync.py`
  - Uses `huggingface_hub.snapshot_download(...)`.
  - Downloads only a controlled subset (allow-list patterns) into the repo root.

### 5.3 Required deployment secrets

Configured in Streamlit Cloud Secrets (TOML format):
- `VISU2_HF_REPO_ID` (HF dataset repo, e.g. `org/my_dataset`)
- `VISU2_HF_REVISION` (pin to a commit hash or tag for reproducibility)
- `HF_TOKEN` (token with access to the private HF dataset)

### 5.4 Required HF dataset layout

The HF dataset repository should contain the same relative paths as the app expects, for example:

```text
data/
  learning_catalog.json
  zpdes_rules.json
  exercises.json
artifacts/
  reports/
    consistency_report.json
    derived_manifest.json
  derived/
    fact_attempt_core.parquet
    agg_activity_daily.parquet
    agg_exercise_elo.parquet
    student_elo_events.parquet
    ...
```

Notes:
- `data/adaptiv_math_history.parquet` is typically **not** required at deployed runtime (unless you rebuild derived artifacts inside the deployment environment).
- Pinning `VISU2_HF_REVISION` means the app is reproducible: you control exactly which data snapshot is served.

## 6) Updating data in a deployment (refresh workflow)

Typical workflow:
1. Rebuild artifacts locally:
   - `uv run python scripts/build_derived.py --strict-checks`
2. Upload or sync `data/` and `artifacts/` to the HF dataset repo.
3. Create a new HF commit (or tag).
4. Update Streamlit secret `VISU2_HF_REVISION` to the new commit/tag.
5. Reboot the Streamlit app.

## 7) Elo-derived artifacts: what they mean

The Elo layer is built in two stages.

### 7.1 Stage A: exercise Elo calibration

Goal:
- estimate a fixed retrospective difficulty for each exercise.

How it works:
- use historical **first attempts only**,
- process them in chronological order,
- update both the student rating and the exercise rating,
- keep only the final exercise rating at the end.

What this produces:
- `agg_exercise_elo.parquet`
- then `agg_activity_elo.parquet` by averaging exercise Elo values inside each activity.

Interpretation:
- higher exercise Elo = harder exercise
- higher activity Elo = harder activity on average

### 7.2 Stage B: student Elo replay

Goal:
- reconstruct how a student's estimated level evolves over time.

How it works:
- freeze the exercise Elo values from Stage A,
- replay each student's attempts in their own chronological order,
- update only the student rating after each attempt.

What this produces:
- `student_elo_events.parquet` (attempt-by-attempt trajectory)
- `student_elo_profiles.parquet` (one-row-per-student summary)

Interpretation:
- this is a descriptive retrospective replay,
- it is useful for comparing trajectories,
- it is not a causal claim or a production online mastery model.

## 8) Troubleshooting checklist

If you see INCOMPATIBLE / missing core columns in the UI:
- Check `artifacts/reports/derived_manifest.json`:
  - `schema_version` must match `src/visu2/contracts.py:DERIVED_SCHEMA_VERSION`.
- Ensure the deployed app is running the correct GitHub commit:
  - schema errors often happen when code is updated but HF artifacts are not (or vice versa).
- Ensure `VISU2_HF_REVISION` points to the HF snapshot that contains the new derived artifacts.

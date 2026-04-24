# VISU2: Data and Runtime Guide

This is the main practical document for understanding how the project works.

It answers:
- what datasets the repo currently includes
- what the main data files are
- how those files relate to each other
- what artifacts are and why we use them
- how to rebuild and update runtime data locally and on Hugging Face

If you need more detail after this document:
- raw file reference: `ressources/data.md`
- metadata contract: `ressources/DERIVED_FILES_FORMAT.md`
- deployment/runtime package details: `ressources/README_HF.md`

---

## 1. Which datasets the repo currently includes

This repo is not limited to one dataset.

The current source-aware app supports:

- `main`
  - the large Adaptiv'Math mathematics source
  - raw attempts come from `data/adaptiv_math_history.parquet`
- `mia_module1`
  - a MIA researcher export now spanning multiple modules
  - raw attempts come from `data_MIA/986-neurips-mia_20260415_100024.csv`
  - extra structure comes from `data_MIA/config_mia.json`
- `maureen_m16fr`
  - a French-language remediation source
  - raw attempts come from `data_maureen/researcher_data_Comprendre les mots pour mieux les lire(in).csv`
  - extra structure comes from `data_maureen/M16FR_modules_config 1(M16-Fr).csv`

The goal of the pipeline is to normalize these source-specific inputs into a shared runtime contract so the same app pages can be reused when possible.

---

## 2. Main shared files and source-specific inputs

For the `main` source, the canonical raw/reference set is built around four files:

- `data/adaptiv_math_history.parquet`
  - raw attempt history
  - one row = one exercise attempt
- `data/learning_catalog.json`
  - curriculum map
  - canonical hierarchy:
    - `module -> objective -> activity -> exercise`
- `data/zpdes_rules.json`
  - ZPDES dependency and unlocking rules
- `data/exercises.json`
  - exercise content and interaction metadata

For `mia_module1` and `maureen_m16fr`, the raw inputs differ, but the derived pipeline still tries to produce the same app-facing structure.

At runtime, the app usually does **not** read the raw inputs directly.
It reads source-local derived folders under:

- `artifacts/sources/<source_id>/data/`
- `artifacts/sources/<source_id>/artifacts/derived/`

The normalized attempt file inside those source-local folders is:

- `data/student_interaction.parquet`

---

## 3. How the files relate to each other

The simplest way to think about the relationship is:

1. the source-specific interaction file tells us **what students did**
2. the catalog or config enrichment tells us **where each exercise sits in the curriculum**
3. `zpdes_rules.json` tells us **how adaptive progression is structured** when ZPDES is available
4. `exercises.json` tells us **what the exercise is** when exercise metadata is available

In practice:
- attempts come from a raw export or parquet
- readable pedagogical structure comes from the catalog/config layer
- rule and dependency information comes from ZPDES rules when available
- exercise instructions and types come from exercise metadata when available

For the `main` source, the canonical hierarchy reference is:

- `learning_catalog.json`

For `mia_module1` and `maureen_m16fr`, the pipeline first converts source-specific inputs into a compatible runtime view.

---

## 4. What artifacts are

Artifacts are precomputed files stored under `artifacts/`.

We use them because:
- the app must stay interactive
- the same cleaning and metric logic should be reused across pages
- the deployed app should not recompute everything from the raw sources

Main idea:

- raw files = source truth
- artifacts = app-ready derived data

Typical artifact types:

- `fact_*`
  - cleaned attempt-level tables used for filtering and drilldowns
- `agg_*`
  - pre-aggregated tables used for charts and summaries
- reports
  - consistency checks and manifest information

One important example:

- `fact_attempt_core.parquet`
  - the main cleaned attempt-level table used by many pages

In the source-aware layout, that lives under:

- `artifacts/sources/<source_id>/artifacts/derived/fact_attempt_core.parquet`

---

## 5. How the app uses the data

Typical flow:

1. source-specific raw files are validated
2. source-specific derived artifacts are built
3. Streamlit pages load the derived artifacts they need
4. pages also load metadata files when available, such as:
   - `learning_catalog.json`
   - `zpdes_rules.json`
   - `exercises.json`

The app usually reads:
- metadata from `artifacts/sources/<source_id>/data/`
- runtime tables from `artifacts/sources/<source_id>/artifacts/derived/`

This means the app depends on the derived pipeline being up to date.

---

## 6. How to run the app locally

Install dependencies:

```bash
uv sync --dev
```

Validate contracts:

```bash
uv run python scripts/check_contracts.py --strict
```

Build derived artifacts:

```bash
uv run python scripts/build_derived.py --strict-checks
```

Run the app:

```bash
uv run python scripts/run_slice.py --port 8501
```

Optional smoke check:

```bash
uv run python scripts/run_slice.py --smoke
```

If something looks inconsistent in the app, the first thing to check is usually whether the derived artifacts were rebuilt after the last code or data change.

---

## 7. How to rebuild data after a change

Use this when:
- the raw data changed
- metadata files changed
- metric logic changed
- a page needs newer derived tables

Recommended sequence:

```bash
uv run python scripts/check_contracts.py --strict
uv run python scripts/build_derived.py --strict-checks
```

If you want to rebuild one source explicitly:

```bash
uv run python scripts/build_derived.py --source main --strict-checks
uv run python scripts/build_derived.py --source mia_module1 --strict-checks
uv run python scripts/build_derived.py --source maureen_m16fr --strict-checks
```

Useful related scripts:

- `scripts/build_derived.py`
  - rebuilds runtime files
- `scripts/check_contracts.py`
  - validates data and contract consistency
- `scripts/run_slice.py`
  - runs the app or a smoke check
- `scripts/sync_runtime_assets.py`
  - sync helper for runtime assets

---

## 8. How Hugging Face fits in

The deployed app keeps:
- code on GitHub
- runtime data in one or more Hugging Face dataset repositories

The app still reads normal local paths internally.
The difference in deployment is that those files are first synchronized from Hugging Face.

So the deployment model is:

1. build artifacts locally
2. upload the runtime snapshot to Hugging Face
3. point the deployed app to that snapshot

This keeps runtime data versioned separately from code.

---

## 9. How to update Hugging Face runtime data

The runtime upload is source-aware.

Each dataset source has its own built runtime folder:

- `artifacts/sources/main/`
- `artifacts/sources/mia_module1/`
- `artifacts/sources/maureen_m16fr/`

Local-only files live outside the runtime upload tree:

- `artifacts/local/main/`
- `artifacts/local/mia_module1/`
- `artifacts/local/maureen_m16fr/`
- `artifacts/legacy/main/`
- `artifacts/legacy/mia_module1/`
- `artifacts/legacy/maureen_m16fr/`

Each Hugging Face dataset repository should receive the **contents** of one source folder at the repo root.

### Full recommended workflow

1. rebuild locally

```bash
uv run python scripts/build_derived.py --source main --strict-checks
uv run python scripts/build_derived.py --source mia_module1 --strict-checks
uv run python scripts/build_derived.py --source maureen_m16fr --strict-checks
```

2. log in if needed

```bash
hf auth login
```

3. upload each source runtime folder to its dataset repo

Existing examples:

```bash
hf upload GAIMHE/Adaptiv_Math ./artifacts/sources/main . --repo-type dataset
hf upload GAIMHE/M16 ./artifacts/sources/maureen_m16fr . --repo-type datase
hf upload GAIMHE/MIA2nd ./artifacts/sources/mia_module1 . --repo-type dataset
```

If the upload is large:

```bash
hf upload-large-folder GAIMHE/Adaptiv_Math --repo-type dataset ./artifacts/sources/main
hf upload GAIMHE/Adaptiv_Math ./artifacts/sources/mia_module1 . --repo-type dataset
hf upload-large-folder GAIMHE/M16 --repo-type dataset ./artifacts/sources/maureen_m16fr
```

4. update the revision used by the deployed app if needed

5. restart or redeploy the app

### Important upload rule

Do **not** upload the whole project root.

Upload only one source runtime folder at a time, for example:

- `./artifacts/sources/main`
- `./artifacts/sources/mia_module1`
- `./artifacts/sources/maureen_m16fr`

The target Hugging Face repo root should contain:

- `data/`
- `artifacts/derived/`

It should **not** contain:

- `artifacts/local/...`
- `artifacts/legacy/...`
- local reports under `artifacts/reports/`

### Streamlit secrets

Set `VISU2_HF_SOURCES_JSON` to something like:

```json
{
  "main": {
    "repo_id": "GAIMHE/Adaptiv_Math",
    "revision": "main"
  },
  "mia_module1": {
    "repo_id": "<your-mia-runtime-repo>",
    "revision": "main"
  },
  "maureen_m16fr": {
    "repo_id": "GAIMHE/M16",
    "revision": "main"
  }
}
```

Also set:

```text
HF_TOKEN=...
```

### Useful variants

If you want to force a rebuild before upload:

```bash
uv run python scripts/build_derived.py --source main --strict-checks --force
uv run python scripts/build_derived.py --source mia_module1 --strict-checks --force
uv run python scripts/build_derived.py --source maureen_m16fr --strict-checks --force
```

If you changed only a few derived tables and the raw inputs did **not** change, prefer a targeted build:

```bash
uv run python scripts/build_derived.py --source main --tables student_elo_events_batch_replay,student_elo_profiles_batch_replay --skip-checks
```

Notes:

- `--tables` expects runtime derived table names, comma-separated
- this lightweight path reuses existing local inputs
- if the raw inputs changed, run the full build instead

If you want to relocate older non-runtime files out of the runtime trees:

```bash
uv run python scripts/migrate_runtime_legacy_artifacts.py
```

Dry run:

```bash
uv run python scripts/migrate_runtime_legacy_artifacts.py --dry-run
```

For package-level details of what the runtime repository should contain, use:

- `ressources/README_HF.md`

---

## 10. One short summary

If you remember only one thing:

- the repo supports several dataset sources, not just the main Adaptiv'Math one
- raw files are the ground truth
- artifacts are the app-ready derived layer
- the app mostly runs from source-specific artifacts
- when code or data changes, rebuild artifacts first
- deployment uses Hugging Face as the runtime data store

# Update Runtime Data

This project now uses a **source-aware runtime layout**.

Each dataset source has its own built runtime folder:
- `artifacts/sources/main/`
- `artifacts/sources/maureen_m16fr/`

Local-only files now live outside the runtime upload tree:
- `artifacts/local/main/`
- `artifacts/local/maureen_m16fr/`
- `artifacts/legacy/main/`
- `artifacts/legacy/maureen_m16fr/`

Each Hugging Face dataset repo should receive the **contents** of one of those folders at the repo root.

## Full recommended sequence

### 1. Build locally first

```bash
uv run python scripts/build_derived.py --source main --strict-checks
uv run python scripts/build_derived.py --source maureen_m16fr --strict-checks
```

### 2. Login once if needed

```bash
hf auth login
```

### 3. Upload

Keep the existing main runtime repo:

```bash
hf upload GAIMHE/Adaptiv_Math ./artifacts/sources/main . --repo-type dataset
```

```bash
hf upload GAIMHE/M16 ./artifacts/sources/maureen_m16fr . --repo-type dataset
```

### 4. If the upload is large, use

```bash
hf upload-large-folder GAIMHE/Adaptiv_Math --repo-type dataset ./artifacts/sources/main
hf upload-large-folder GAIMHE/M16 --repo-type dataset ./artifacts/sources/maureen_m16fr
```

## Important notes

- Do **not** upload the whole repository root with `hf upload ... .` from the project root.
- Upload only:
  - `./artifacts/sources/main`
  - `./artifacts/sources/maureen_m16fr`
- The target HF repo root should contain:
  - `data/`
  - `artifacts/derived/`
- It should **not** include:
  - `artifacts/local/...`
  - `artifacts/legacy/...`
  - local build reports under `artifacts/reports/`

## Streamlit secrets

Set `VISU2_HF_SOURCES_JSON` to:

```json
{
  "main": {
    "repo_id": "GAIMHE/Adaptiv_Math",
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

## Runtime source mapping

- `main` -> `GAIMHE/Adaptiv_Math`
- `maureen_m16fr` -> `GAIMHE/M16`

## If you want to force a rebuild before upload

```bash
uv run python scripts/build_derived.py --source main --strict-checks --force
uv run python scripts/build_derived.py --source maureen_m16fr --strict-checks --force
```

## If you want to relocate older non-runtime files out of the runtime trees

```bash
uv run python scripts/migrate_runtime_legacy_artifacts.py
```

Dry run:

```bash
uv run python scripts/migrate_runtime_legacy_artifacts.py --dry-run
```

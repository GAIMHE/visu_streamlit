# Documentation Index

This index defines the canonical documentation structure for the repository.

## Canonical Roadmap

- `ressources/roadmap_6m.md`

## Core Data and Contract Docs

- `ressources/data.md`
- `ressources/DERIVED_FILES_FORMAT.md`
- `ressources/README_HF.md`

## Archived Context Documents

Historical context documents are kept under `ressources/archive/` and are non-canonical:

- `Roadmap for T2.2_ Visualization, Interaction & Analysis of Educational Data.pdf`
- `IDEE Pistes d'analyses.docx`
- `IDEE_Playlist_ANALYSIS.docx`
- `Colonnes+module+config.doc`

## Notes

- Runtime and artifact generation are based on the standalone 4-file data contract:
  - `data/adaptiv_math_history.parquet`
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
  - `data/exercises.json`
- Legacy raw sources are retained in `data/legacy/` for historical traceability only.

## Deployment Checklist (HF-backed runtime)

- Keep application code in GitHub.
- Keep runtime data/artifacts in a Hugging Face dataset repo.
- Configure deployment secrets:
  - `VISU2_HF_REPO_ID`
  - `VISU2_HF_REVISION`
  - `HF_TOKEN`
- Ensure HF repo includes:
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
  - `data/exercises.json`
  - `artifacts/reports/consistency_report.json`
  - `artifacts/reports/derived_manifest.json`
  - `artifacts/derived/*.parquet`

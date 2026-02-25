# Context / Scope
- Reworked `README.md` into a full onboarding document.
- Target structure was fixed: project/data context, 4 main sources, repo structure, setup/run flow, and page/figure guide with dataset mapping.

# Main Changes Made
- Replaced placeholder README content with a complete, structured document.
- Added a synthetic runtime data contract section for:
  - `data/adaptiv_math_history.parquet`
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
  - `data/exercises.json`
- Added an up-to-date project structure section aligned with current repo folders.
- Added executable environment/run flow using:
  - `uv sync --dev`
  - `scripts/check_contracts.py --strict`
  - `scripts/build_derived.py --strict-checks`
  - `scripts/run_slice.py --port 8501`
  - `scripts/run_slice.py --smoke`
- Added page-by-page plain-language explanations with explicit dataset mapping per figure/component.
- Added pointers to deeper docs under `ressources/`.

# Important Decisions and Rationale
- Kept README in English for consistency with current code/docs and broader team use.
- Kept medium-concise depth: practical and scannable without duplicating HF-level schema detail.
- Explicitly marked `data/legacy/` as historical only to avoid runtime ambiguity.

# Follow-up Actions
- Optional: add a short “Troubleshooting” section (missing artifacts, stale schema, rebuild steps).
- Optional: keep page descriptions synced when new Streamlit views are added.

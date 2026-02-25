# Context / Scope
- Build a French, execution-focused roadmap presentation (18 slides) for a mixed committee.
- Cover full trajectory: EDA, visualisations, analysis/testing, data/database handling, deployment.
- Include already implemented steps and next priorities.

# Main Changes Made
- Added presentation source files:
  - `ressources/presentation/slide_spec_fr.yaml`
  - `ressources/presentation/roadmap_t22_execution_fr_v1_outline.md`
- Added reproducible generator:
  - `scripts/build_roadmap_presentation.py`
- Generated deliverables:
  - `ressources/presentation/roadmap_t22_execution_fr_v1.pptx`
  - `ressources/presentation/assets/*.png` (16 generated visual assets)
- Updated `README.md` with a section documenting the generation command and outputs.
- Updated dependencies in `pyproject.toml` and lockfile for presentation tooling:
  - `python-pptx`, `pillow`, `pyyaml`

# Important Decisions and Rationale
- Used YAML spec + Python generator for reproducibility and easy future edits.
- Chose a custom clean 16:9 theme with explicit status tags (`Done`, `In progress`, `Next`) for committee readability.
- Kept speaker notes detailed on all slides (objective + script + source references).
- Generated deterministic local visual assets to avoid external template dependency.

# Validation Results
- `uv run python -m py_compile scripts/build_roadmap_presentation.py`
- `uv run python scripts/build_roadmap_presentation.py`
- Structural check: generated deck has exactly 18 slides.
- Notes check: all slides have non-empty speaker notes.

# Follow-up Actions
- Optional: produce a PDF export of the deck for sharing.
- Optional: add institutional branding variant if a template is provided.
- Optional: add thumbnail-based visual QA snapshots as an automated check step.

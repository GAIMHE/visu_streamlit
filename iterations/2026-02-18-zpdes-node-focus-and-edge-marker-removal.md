# Context / Scope
- Requested UX change on ZPDES dependency view:
  - remove grey edge midpoint markers,
  - click nodes only (activity/objective),
  - show rules for the selected unit,
  - fade unrelated units and arrows.

# Main Changes Made
- Updated selection parsing to accept only node selections.
- Removed invisible edge midpoint marker traces from graph rendering.
- Added node-focus rendering in `apps/pages/3_zpdes_dependencies.py`:
  - when a node is selected, only direct dependency neighborhood stays at full opacity,
  - unrelated nodes and edges are dimmed.
- Kept current edge styling and curved-arrow behavior intact.
- Updated rule panel wording to node-centric dependency interpretation.
- Added a `Clear focus` action in Rule Detail panel.

# Important Decisions and Rationale
- Kept dependency emphasis at one-hop neighborhood (incoming/outgoing of selected node) for readability.
- Used opacity-based fading (not hard filtering) so full topology context remains visible.
- Preserved existing objective/activity model and source semantics.

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- If needed, expand focus radius to two hops with a toggle.
- Optionally add edge labels in focused mode only to reduce clutter.

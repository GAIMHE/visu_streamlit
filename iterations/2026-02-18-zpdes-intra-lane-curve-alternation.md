# Context / Scope
- Improve readability of intra-objective (green) dependency edges in the ZPDES graph.
- User requested top/bottom alternation of curved arrows within each objective lane.

# Main Changes Made
- Updated curve alternation ranking in `apps/pages/3_zpdes_dependencies.py`:
  - previous behavior ranked by `(lane, span_min, span_max)`, which often reset rank and kept curves on one side.
  - new behavior ranks by `lane` only, so curved edges alternate top/bottom across that lane.

# Important Decisions and Rationale
- Kept existing curve amplitude/tier logic.
- Changed only the alternation key to achieve deterministic visual flipping within objective lanes with minimal behavior change.

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- If lane ordering still creates visual crossings for some modules, add optional “edge ordering mode” in sidebar (e.g., by source activity index).

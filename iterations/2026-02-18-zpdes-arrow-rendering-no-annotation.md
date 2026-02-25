# Context / Scope
- Follow-up fix for ZPDES dependency graph arrow rendering.
- Residual issue: duplicate-looking segment near arrowheads remained on some edges.

# Main Changes Made
- Replaced Plotly annotation arrows with explicit arrowhead geometry drawn as short line segments.
- Kept dependency edge itself as the only main curve/line.
- Arrowhead now uses the final edge tangent direction computed from the rendered endpoint segment.

# Important Decisions and Rationale
- Decision: do not use annotation shafts for arrows in this graph.
- Rationale: annotation shafts can create visible duplicate segments when overlaid on curved lines; custom arrowhead segments avoid that artifact and keep endpoint alignment deterministic.

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- Visual check on modules with dense intra-objective dependencies.
- If needed, tune arrowhead length/spread constants for readability at different zoom levels.

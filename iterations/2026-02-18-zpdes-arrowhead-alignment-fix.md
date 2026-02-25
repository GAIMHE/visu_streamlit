# Context / Scope
- Fix visual arrow alignment issues on the ZPDES dependency graph (`apps/pages/3_zpdes_dependencies.py`).
- Symptoms reported: curved green arrowheads not aligned with the curve, and straight blue arrows visually continuing past arrowheads.

# Main Changes Made
- Updated edge arrow shaft computation in `_build_graph` to use the exact direction of the final rendered line segment (`draw_points[-2] -> draw_points[-1]`).
- Kept edge endpoint trimming (`node_clearance`) and arrow placement at the trimmed endpoint, so the visible line ends at the arrowhead anchor.
- Preserved existing style semantics (blue/red between-objective, green intra-objective).

# Important Decisions and Rationale
- Chose segment-tangent-based arrow direction instead of path-distance sampling for the shaft.
- Reason: using the final rendered segment guarantees geometric consistency between the drawn line and annotation arrow orientation, especially on curved edges.

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- Visually re-check a module with dense intra-objective dependencies to confirm perceived alignment under multiple zoom levels.
- If minor visual offset remains, tune `arrow_shaft` and `node_clearance` constants per edge type.

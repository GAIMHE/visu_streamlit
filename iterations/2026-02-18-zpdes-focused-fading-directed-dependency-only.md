# Context / Scope
- Refine ZPDES focus-fading behavior to avoid highlighting unrelated branches.
- User example: selecting `M31O4A1` should not keep `M31O1A3 -> O2/O3` visible, since those branches are not part of `M31O4A1` dependency chain.

# Main Changes Made
- Updated focus-neighborhood logic in `apps/pages/3_zpdes_dependencies.py`:
  - replaced undirected connectivity expansion with directed dependency traversal.
  - highlighted set is now:
    - transitive ancestors of the selected unit (prerequisite chain),
    - transitive descendants of the selected unit (units depending on it).
- Sibling branches from ancestors are no longer highlighted by default.
- Kept objective-bridge behavior for activity selections on ancestor traversal.

# Important Decisions and Rationale
- Dependency relevance should follow edge direction, not generic graph connectivity.
- This matches pedagogical interpretation: "what unlocks this unit" and "what this unit unlocks".
- Prevents branch bleed (e.g., unrelated outputs of a common ancestor).

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- Optional toggle could be added later:
  - `strict chain` (current behavior) vs `full connected component`.

# Context / Scope
- Refine node detail behavior in ZPDES dependency view.
- User requests:
  - remove UI line showing `Source: xlsx + admath`,
  - improve incoming prerequisites for selected unit to include indirect chain dependencies (not only direct edges).

# Main Changes Made
- Removed `Source` metric from Rule Detail node header in `apps/pages/3_zpdes_dependencies.py`.
- Added transitive activation prerequisite resolver:
  - new helper `_collect_transitive_incoming_activation(...)`,
  - computes upstream activation chain with dependency depth.
- Rule Detail incoming table now:
  - includes indirect prerequisites with `dependency_depth`,
  - shows readable labels + codes (`from` and `to`),
  - uses selected node and, for activity nodes, its objective as seed target.
- Focus neighborhood logic now traverses graph connectivity from the focused node (and parent objective for activities), keeping linked nodes/edges visible and fading unlinked ones.

# Important Decisions and Rationale
- Indirect prerequisite interpretation is based on activation edges only (unlock logic).
- Activity selection includes objective seed so objective-level unlock rules are represented for activity rows like `M1O3A1`.
- Scope remains objective-filter aware (if upstream objectives are filtered out, they cannot appear in current chain view).

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- Optional: add a toggle to switch between `direct only` and `transitive` incoming dependencies.

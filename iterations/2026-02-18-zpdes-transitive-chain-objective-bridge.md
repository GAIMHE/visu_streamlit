# Context / Scope
- Follow-up fix for ZPDES Rule Detail incoming prerequisites.
- Reported issue: for `M1O3A1`, table showed `M1O2A2` but still missed upstream `M1O1A3`.

# Main Changes Made
- Updated transitive prerequisite resolver in `apps/pages/3_zpdes_dependencies.py`:
  - `_collect_transitive_incoming_activation(...)` now bridges activity prerequisites to their parent objective during traversal.
- Rule Detail incoming/outgoing computations now use module-level dependency tables (`edges`, `nodes_with_metrics`) instead of objective-filtered graph edges only.

# Important Decisions and Rationale
- The unlock chain is activation-based and can mix activity-level and objective-level rules.
- Since objective unlock can gate activities, transitive traversal must propagate through parent objective of intermediate activity nodes.
- Detail panel should reflect true module dependency logic even if the graph display is currently objective-subfiltered.

# Validation
- `uv run python -m py_compile apps/pages/3_zpdes_dependencies.py`
- `uv run pytest -q tests/test_zpdes_dependencies.py`
- `uv run python scripts/run_slice.py --smoke`
- `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py apps/pages`
- `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py`

# Follow-up Actions
- Optional: add explicit `is_inferred_via_objective_bridge` flag in incoming table for transparency.

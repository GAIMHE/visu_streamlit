## Context / Scope

Extend the `ZPDES Transition Efficiency` page so the empirical transition overlay can be restricted to either `zpdes` or `playlist` attempts only.

This was a page-side refinement. The structural graph layout, activity coloring logic, and ranked-table semantics remain unchanged.

## Main Changes Made

- Added a new sidebar control on `apps/pages/6_zpdes_transition_efficiency.py`:
  - `Transition population`
  - options:
    - `ZPDES mode`
    - `Playlist mode`
- Switched the empirical transition overlay on that page from `agg_transition_edges.parquet` to scoped recomputation from `fact_attempt_core.parquet`.
- Added `build_scoped_transition_edges_from_fact(...)` in `src/visu2/zpdes_transition_efficiency.py`.
- Added a regression test covering work-mode filtering semantics in `tests/test_zpdes_transition_efficiency.py`.
- Updated the relevant user-facing docs:
  - `README.md`
  - `ressources/STREAMLIT_FIGURES_GUIDE.md`
  - `ressources/figures/zpdes_transition_efficiency_graph.md`
  - `ressources/figures/zpdes_transition_efficiency_ranking_table.md`

## Important Decisions and Rationale

- The page now recomputes transitions from attempt-level fact data because `agg_transition_edges.parquet` does not carry `work_mode`.
- The recomputation preserves real chronology:
  - transitions are built on the full ordered attempt stream per user,
  - then filtered so both source and destination attempts use the selected work mode.
- This avoids fabricating same-mode transitions across hidden intervening attempts from another mode.
- No artifact rebuild was required because the needed `work_mode` field already exists in `fact_attempt_core.parquet`.

## Follow-up Actions

- If this page becomes too slow in deployment, build a dedicated `work_mode`-aware transition artifact instead of recomputing transitions page-side.
- If you later want `adaptive-test` or other modes, extend the selector and tests with the same chronology-preserving rule.

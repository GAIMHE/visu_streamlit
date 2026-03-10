## Context / Scope

Refine the `ZPDES Transition Efficiency` page with a visualization-only minimum
transition count threshold for empirical arrows.

The goal was to let users suppress noisy low-count arrows without changing the
underlying transition metrics, ranking table, or probability normalization.

## Main Changes Made

- Added a new sidebar numeric input on `apps/pages/6_zpdes_transition_efficiency.py`:
  - `Minimum transition count for arrows`
- Extended `build_incoming_transition_frame(...)` in
  `src/visu2/zpdes_transition_efficiency.py` with
  `min_transition_count_for_graph`.
- Added a row-level flag:
  - `passes_min_transition_count`
- Updated graph visibility logic so `visible_in_graph` now requires:
  - objective visibility, and
  - transition count at or above the selected threshold
- Added a regression test in `tests/test_zpdes_transition_efficiency.py`.
- Updated figure docs and the figures guide.

## Important Decisions and Rationale

- The threshold is visualization-only.
- Transition probabilities, conditional destination success, weighted summary
  metrics, and the ranked table continue to use all incoming transitions in
  scope.
- This preserves scientific consistency: hiding arrows does not change the
  quantitative interpretation of the clicked destination activity.

## Follow-up Actions

- If needed later, add a second optional threshold for table filtering, but keep
  it clearly separated from the metric-normalization logic.
- If users need more control, expose an additional toggle to show hidden rows
  grouped by reason:
  - hidden by objective filter
  - hidden by minimum-count threshold

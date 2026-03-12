# Context / Scope

- Reshape the classroom replay page so classroom selection is driven by classroom size rather than a long raw selector.

# Main Changes Made

- Added `select_classrooms_near_student_target(...)` to `src/visu2/classroom_progression.py`.
- Removed the date-range control from `apps/pages/4_classroom_progression_replay.py`.
- Added a main-panel sentence reporting the minimum and maximum classroom sizes for the selected work-mode scope.
- Added a main-panel `Target classroom size (students)` input.
- Replaced the full classroom selector with a selectbox showing only classrooms in the `target +/- 10%` band, including each classroom's activity and attempt counts.
- Kept replay behavior unchanged once a classroom is selected.
- Rewrote the replay page `Info` block to contain only:
  - `What it shows`
  - `Metrics`
- Updated the classroom replay documentation pages to match the new selection flow and replay-step wording.

# Important Decisions and Rationale

- The matching band is computed from the current work-mode scope only.
- The page no longer exposes a date filter, so classroom matching and replay both use the full classroom history within the selected scope.
- Matching classrooms are sorted by:
  - distance to target student count
  - activities descending
  - attempts descending
  - classroom ID ascending

# Follow-up Actions

- None unless the page later needs a second targeting mode based on activities or attempts instead of students.

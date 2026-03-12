# Context / Scope

- Replace manual student selection on the Student Elo page with attempt-target-based random sampling.

# Main Changes Made

- Removed the sidebar minimum-attempt field and full student multiselect from `apps/pages/5_student_elo_evolution.py`.
- Added a main-panel `Target attempt count` field.
- Added a visible sentence reporting the minimum and maximum attempt counts among replay-eligible students.
- Sampled up to two students from the `target +/- 10%` band and stored that sample in session state so routine reruns do not reshuffle the selection.
- Added `select_students_near_attempt_target(...)` to `src/visu2/student_elo.py`.
- Updated page info copy and figure documentation to match the new selection workflow.
- Added test coverage for the new attempt-target sampling helper.

# Important Decisions and Rationale

- Sampling is random when the target changes, but stable across ordinary reruns for the same target.
- The selection is restricted to `eligible_for_replay` students to stay aligned with the page's actual replay capability.
- If only one student matches the band, the page renders one trajectory instead of failing.
- If no students match, the page stops with a clear message rather than falling back to a different selection strategy.

# Follow-up Actions

- If needed, add an explicit `Resample students` button later. It is not included in this pass because the request only asked for automatic random selection from the attempt band.

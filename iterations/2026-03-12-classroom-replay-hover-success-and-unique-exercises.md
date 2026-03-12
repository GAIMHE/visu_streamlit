# Classroom Replay Hover Success and Unique Exercises

## Context / Scope
- Extend the classroom replay hover so it reports more interpretable cumulative cell statistics.
- Keep replay mechanics and color semantics unchanged.

## Main Changes
- Added cumulative `success_frames` to the replay payload.
- Added cumulative `unique_exercise_frames` to the replay payload.
- Updated the hover on the replay heatmap to show:
  - attempts in cell
  - successful attempts
  - cumulative success rate
  - unique exercises in cell
- Added regression assertions in `tests/test_classroom_progression.py` for the new payload fields and hover customdata.
- Updated the figure info and figure documentation to describe the new hover contents.

## Important Decisions and Rationale
- Successful-attempt counts are carried explicitly rather than inferred from rate × attempts.
- Unique exercises are tracked cumulatively per student-activity cell to distinguish repetition from breadth.
- The hover order mirrors the most natural reading path: attempts, successes, rate, then unique exercises.

## Follow-up Actions
- Visually confirm the hover wording in the live Streamlit app on a few classrooms with repeated exercises.

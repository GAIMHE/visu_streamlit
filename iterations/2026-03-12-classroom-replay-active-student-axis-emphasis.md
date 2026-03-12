# Classroom Replay Active Student Axis Emphasis

## Context / Scope
- Improve readability of the classroom replay by visually indicating which students are still receiving new attempts in the current frame.

## Main Changes
- Added `student_total_attempts` to the replay payload.
- Added frame-aware x-axis tick formatting in `build_heatmap_figure(...)`.
- Student labels are now bold only when that student contributes at least one new local attempt in the currently selected frame.
- Added a regression test covering the uneven-student case where one student becomes inactive earlier than another.
- Updated the figure info and detailed figure documentation to mention the bold-label behavior.

## Important Decisions and Rationale
- The active/inactive distinction is based on whether the current frame integrates any new attempts for that student, not whether the student has ever appeared in the classroom.
- Using x-axis label emphasis avoids adding another legend or annotation layer to an already dense figure.

## Follow-up Actions
- Visually verify that Plotly renders the bold tick labels cleanly in the deployed Streamlit environment.

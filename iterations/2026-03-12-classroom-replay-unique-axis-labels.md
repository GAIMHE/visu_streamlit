# Classroom Replay Unique Axis Labels

## Context / Scope
- Fix residual display issues on the classroom replay heatmap after moving cell values to a text overlay.
- The remaining bug appeared when multiple activities shared the same visible row label.

## Main Changes
- Added `_make_unique_axis_labels(...)` in `src/visu2/classroom_progression.py`.
- The replay payload now uniquifies duplicated activity row labels before Plotly rendering.
- Added a regression test covering duplicate activity labels and verifying that the text overlay follows the uniquified row labels.
- Updated the figure documentation to note the numeric suffix behavior for duplicate row labels.

## Important Decisions and Rationale
- Plotly categorical heatmaps collapse duplicate category labels onto the same visual coordinate.
- The correct fix is to make internal axis labels unique rather than trying to offset text after rendering.
- Numeric suffixes preserve readability while preventing row collisions.

## Follow-up Actions
- Visually verify one classroom with repeated activity labels in the Streamlit app.

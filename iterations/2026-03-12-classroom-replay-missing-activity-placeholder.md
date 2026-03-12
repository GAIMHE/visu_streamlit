# Classroom Replay Missing Activity Placeholder

## Context / Scope
- Investigate why some playlist classrooms showed students with no visible replay cells despite nonzero attempts in the classroom summary.

## Main Changes
- Normalized missing `activity_id` values to a stable replay placeholder key.
- Normalized missing activity labels to the visible placeholder `(missing activity metadata)`.
- Ensured replay ordering and matrix updates use the same normalized activity key.
- Added a regression test covering playlist-style rows with missing activity metadata.
- Updated the classroom replay figure documentation to mention the placeholder row.

## Important Decisions and Rationale
- The fact data for some playlist attempts genuinely lacks both `activity_id` and `activity_label`.
- Silently dropping those rows from the replay is misleading because the classroom summary still counts their attempts and students.
- Collapsing missing activity metadata into one explicit placeholder row is the least misleading behavior available with the current data.

## Follow-up Actions
- If this missing-activity pattern becomes analytically important, trace it back to the raw playlist data contract and decide whether artifact enrichment should backfill missing activity metadata earlier in the pipeline.

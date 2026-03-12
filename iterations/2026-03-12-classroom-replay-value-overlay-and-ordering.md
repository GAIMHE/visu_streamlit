# Classroom Replay Value Overlay and Ordering

## Context / Scope
- Refine the classroom replay page selection and display behavior.
- Keep the existing replay metrics and animation semantics unchanged.
- Fix the visual ambiguity reported for `Show cell values`.

## Main Changes
- Changed classroom match ordering inside the target-size band to:
  - activity coverage descending
  - attempts descending
  - students descending
  - classroom ID ascending
- Removed the user-facing mastery-threshold control from the page.
- Fixed the heatmap color reference threshold to `0.75`.
- Replaced direct `Heatmap.texttemplate` value rendering with a dedicated scatter-text overlay so cell percentages only appear on populated cells.
- Updated the classroom replay info copy and figure docs to reflect:
  - fixed threshold semantics
  - value labels only on populated cells

## Important Decisions and Rationale
- Sorting by curricular coverage first is more aligned with the user's screening goal than sorting by distance to the target size.
- The threshold slider was removed because the page no longer needs a tunable color interpretation for this workflow.
- Text rendering moved to a separate overlay because heatmap-native text placement was producing visually misleading labels on sparse matrices.

## Follow-up Actions
- Visually verify the value overlay on a few large classrooms in the live Streamlit app.
- If dense classrooms become hard to read with text enabled, consider auto-hiding text above a configurable matrix size.

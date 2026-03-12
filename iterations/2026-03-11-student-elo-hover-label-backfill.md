# Context / Scope

- Replace raw activity/module identifiers in the Student Elo hover with human-readable labels.

# Main Changes Made

- Added a catalog-backed label lookup loader in `src/visu2/student_elo.py`.
- Joined readable labels onto the Student Elo event payload at runtime.
- Updated the replay hover to show:
  - activity label
  - objective label
  - module label
- Kept the artifact contract unchanged by resolving labels from `learning_catalog.json` at runtime.
- Added regression tests for payload enrichment and hover-template fields.

# Important Decisions and Rationale

- The change was implemented as a runtime backfill rather than an artifact/schema change because the existing event artifact already had stable join keys.
- The payload builder now falls back cleanly to null label fields when no lookup is provided, so tests and helper reuse remain robust.

# Follow-up Actions

- None unless the exercise field should also be backfilled to a readable exercise label in a future pass.

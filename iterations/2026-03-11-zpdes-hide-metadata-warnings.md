# Context / Scope

- Remove the non-blocking metadata warning section from the ZPDES transition-efficiency page.

# Main Changes Made

- Deleted the bottom `Metadata warnings` expander from `apps/pages/3_zpdes_transition_efficiency.py`.
- Kept the blocking warning/error behavior unchanged when metadata is missing badly enough to stop the page.

# Important Decisions and Rationale

- Only the passive warning section was removed.
- The early-stop cases still surface warnings when the graph cannot be built, because that is operationally useful and not just cosmetic.

# Follow-up Actions

- None required unless the page should also suppress early-stop warning details in failure states.

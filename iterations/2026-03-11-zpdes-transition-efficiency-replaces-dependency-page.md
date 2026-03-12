# Context / Scope

- Replace the old `ZPDES Dependency Graph` page with the newer `ZPDES Transition Efficiency` page.
- Keep the transition-efficiency graph as the only ZPDES page in navigation.
- Simplify the page controls and copy so the page focuses on ZPDES-mode structural progression only.

# Main Changes Made

- Moved the transition-efficiency page to `apps/pages/3_zpdes_transition_efficiency.py`.
- Deleted `apps/pages/3_zpdes_dependencies.py`.
- Removed the following controls from the transition-efficiency page:
  - cohort population selector
  - date-range selector
  - intra-objective curve toggle
- Fixed the page to use `zpdes` mode only.
- Added the ZPDES explanatory paragraph directly under the page title.
- Narrowed the page `Info` content to two sections only:
  - `What`
  - `Metrics`
- Updated documentation so the active-page map, figure docs, and database docs all point to the new page path.
- Removed obsolete figure docs for the deleted dependency page.

# Important Decisions and Rationale

- The old dependency page was removed rather than kept in parallel because the user wanted a single ZPDES page in navigation.
- The page now uses full available ZPDES history internally instead of exposing a date-range control, which keeps the page simpler and consistent with its explanatory framing.
- The page sidebar still keeps module selection, objective filtering, hover-ID toggle, node-color metric selection, and the typed later-attempt threshold because those controls materially change interpretation.
- Obsolete dependency-page figure docs were deleted to avoid a misleading second source of truth in the documentation pack.

# Follow-up Actions

- Visually verify the new page ordering and copy in the local Streamlit app.
- If needed, tighten the hover text further once the page is reviewed in-browser.
- Push the code-only change to deployment; no artifact rebuild or HF refresh is required for this specific iteration.

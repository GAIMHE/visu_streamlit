# 2026-03-12 Streamlit Date Input Normalization Fix

## Context
Streamlit Cloud raised a `ValueError` on the landing page because `st.sidebar.date_input(...)` did not always return a two-date tuple. The overview page assumed tuple unpacking, which broke when Streamlit returned a single date.

## Main changes made
- Added `normalize_date_input_range(...)` to `apps/overview_shared.py`.
- Updated `render_curriculum_filters(...)` to normalize `date_input` outputs before unpacking.
- Updated `apps/pages/2_objective_activity_matrix.py` to use the same normalization helper.
- Added `tests/test_overview_shared.py` to cover single-date, range, and malformed selections.

## Important decisions and rationale
- Treated a single selected date as a one-day inclusive range.
- Kept the visible UI unchanged while making server-side handling robust.
- Patched the visible matrix page too because it had the same `date_input` unpack assumption.

## Follow-up actions
- Redeploy and confirm that the overview page now opens correctly on Streamlit Cloud.
- If similar date-input failures appear on other pages, apply the same helper there as well.

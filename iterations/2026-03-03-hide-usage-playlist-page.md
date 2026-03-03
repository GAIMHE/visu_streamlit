## Context / Scope

Temporarily hide the `Usage, Playlist and Engagement` Streamlit page from navigation because it is currently the most problematic page in deployment, while keeping its code and data pipeline intact for easy rollback.

## Main Changes

- Moved `apps/pages/1_usage_playlist_engagement.py` to `apps/disabled_pages/1_usage_playlist_engagement.py`.
- Left all artifact generation, HF sync, contracts, and documentation unchanged.

## Important Decisions / Rationale

- Streamlit multipage discovery is driven by `.py` files inside `apps/pages/`, so moving the file out of that folder is the smallest reliable change.
- This is a UI-only hide:
  - no schema changes
  - no rebuild required
  - no HF dataset refresh required
- The file is preserved unchanged so restoring the page later only requires moving it back.

## Follow-Up Actions

- If deployment stability improves, decide later whether to:
  - restore the page,
  - redesign it with lighter artifacts,
  - or retire it fully.

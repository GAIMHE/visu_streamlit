# Iteration: 2026-02-17 - Module/Playlist Analytics Work Mode Handling

## Context/Scope
- Clarify rows in analytics where `playlist_or_module_id` is actually a playlist container and not a mapped module.
- Improve table readability by avoiding ambiguous `None` module codes.

## Main Changes
- Updated playlist/module aggregation in `src/visu2/derive.py`:
  - Added `work_mode` classification per `playlist_or_module_id` group (`playlist`, `module`, `mixed`, `unknown`).
- Updated contracts in `src/visu2/contracts.py`:
  - `agg_playlist_module_usage` now requires `work_mode` in both required and runtime core columns.
- Updated UI in `apps/pages/1_usage_playlist_engagement.py`:
  - Renamed section to `Module/Playlist Analytics`.
  - Added `work_mode` column in the displayed table.
  - Displays `module_code = non-applicable` when `work_mode == playlist`.
- Updated tests in `tests/test_derive_shapes.py`:
  - Added `work_mode` fixtures and assertions for playlist aggregation.

## Validation
- `uv run pytest -q` passed.
- `uv run python scripts/build_derived.py --strict-checks` passed.
- Verified target ID `087663d4-0ce0-41a3-a299-2a71cf19bd78` in `agg_playlist_module_usage`:
  - `work_mode = playlist`
  - `module_code = null` (display now becomes `non-applicable` in UI)

## Follow-ups
- Optionally expose a filter toggle to show only `module`, only `playlist`, or both in Module/Playlist Analytics.

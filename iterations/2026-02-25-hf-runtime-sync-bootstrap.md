# Context / Scope
- Implemented HF-backed runtime data sync for deployed Streamlit usage.
- Goal: keep current local file-path loading logic unchanged while fetching runtime files from a private Hugging Face dataset repository at startup.

# Main Changes Made
- Added `src/visu2/hf_sync.py`:
  - `HFRepoConfig` and `SyncResult` dataclasses
  - `load_hf_repo_config(...)` (secrets/env parsing)
  - `ensure_runtime_assets_from_hf(...)` (snapshot download + required-file validation)
  - default runtime file contract list
- Added `apps/runtime_bootstrap.py`:
  - Streamlit cached bootstrap function
  - hard-fail UI behavior with actionable configuration guidance
- Wired bootstrap into all app entry/pages before data reads:
  - `apps/streamlit_app.py`
  - `apps/pages/1_usage_playlist_engagement.py`
  - `apps/pages/2_objective_activity_matrix.py`
  - `apps/pages/3_zpdes_dependencies.py`
  - `apps/pages/4_classroom_progression_replay.py`
- Added operational helper script:
  - `scripts/sync_runtime_assets.py`
- Added dependency:
  - `huggingface_hub` in `pyproject.toml`
- Added tests:
  - `tests/test_hf_sync.py`
- Updated docs:
  - `README.md`
  - `ressources/README_HF.md`
  - `ressources/README_INDEX.md`

# Important Decisions and Rationale
- Private HF dataset + token is enforced when HF repo config is enabled.
- Pinned revision required when HF sync is configured (reproducibility).
- Startup scope includes derived artifacts/reports and required metadata JSON only.
- Raw `adaptiv_math_history.parquet` is not required at deployed runtime.
- Sync failure is blocking to avoid partial/stale runtime state.

# Follow-up Actions
- Configure deployment secrets (`VISU2_HF_REPO_ID`, `VISU2_HF_REVISION`, `HF_TOKEN`) in the hosting platform.
- Publish/update runtime files in HF dataset with stable relative paths.
- Validate end-to-end deployment once secrets and repo layout are in place.

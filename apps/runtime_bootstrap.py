from __future__ import annotations

from collections.abc import Mapping

import streamlit as st

from visu2.config import get_settings
from visu2.hf_sync import (
    HFRepoConfig,
    SyncResult,
    ensure_runtime_assets_from_hf,
    load_hf_repo_config,
    local_only_sync_result,
)


def _secrets_mapping() -> Mapping[str, object] | None:
    try:
        return dict(st.secrets)
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def _cached_runtime_sync(config: HFRepoConfig | None) -> SyncResult:
    if config is None:
        return local_only_sync_result()
    settings = get_settings()
    return ensure_runtime_assets_from_hf(settings, config)


def bootstrap_runtime_assets() -> SyncResult:
    secrets = _secrets_mapping()
    try:
        config = load_hf_repo_config(secrets=secrets)
        return _cached_runtime_sync(config)
    except Exception as err:
        st.error("Runtime data synchronization failed.")
        st.markdown(
            "This deployment is configured to load runtime assets from a private "
            "Hugging Face dataset, but synchronization did not complete."
        )
        st.markdown("Required configuration keys:")
        st.markdown("- `VISU2_HF_REPO_ID`\n- `VISU2_HF_REVISION`\n- `HF_TOKEN`")
        st.markdown("Expected runtime files:")
        st.markdown(
            "- `data/learning_catalog.json`\n"
            "- `data/zpdes_rules.json`\n"
            "- `data/exercises.json`\n"
            "- `artifacts/reports/consistency_report.json`\n"
            "- `artifacts/reports/derived_manifest.json`\n"
            "- `artifacts/derived/*.parquet`"
        )
        st.code(str(err))
        st.stop()

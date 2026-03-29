"""Bootstrap runtime asset synchronization before page logic executes."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

import streamlit as st

from visu2.config import get_settings
from visu2.hf_sync import (
    HFRepoConfig,
    SyncResult,
    ensure_runtime_assets_from_hf,
    load_hf_repo_config,
    local_only_sync_result,
)
from visu2.runtime_sources import runtime_relative_paths_for_source


def _secrets_mapping() -> Mapping[str, object] | None:
    """Return Streamlit secrets as a plain mapping when available."""
    try:
        return dict(st.secrets)
    except Exception:
        return None


def _publish_runtime_secrets_to_env(secrets: Mapping[str, object] | None) -> None:
    """Mirror runtime-loading secrets into env so non-Streamlit helpers can reuse them."""
    if not secrets:
        return
    for key in (
        "VISU2_HF_SOURCES_JSON",
        "VISU2_HF_REPO_ID",
        "VISU2_HF_REVISION",
        "VISU2_HF_REPO_TYPE",
        "VISU2_HF_ALLOW_PATTERNS_JSON",
        "HF_TOKEN",
    ):
        value = secrets.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            os.environ[key] = text


@st.cache_resource(show_spinner=False)
def _cached_runtime_sync(
    source_id: str,
    config: HFRepoConfig | None,
    required_paths: tuple[str, ...] | None,
) -> SyncResult:
    """Cache one source-aware runtime synchronization result."""
    if config is None:
        return local_only_sync_result()
    settings = get_settings(source_id)
    return ensure_runtime_assets_from_hf(settings, config, required_paths=required_paths)


def _format_expected_paths_markdown(
    required_paths: tuple[str, ...] | None,
    *,
    source_id: str,
) -> str:
    """Format page-specific runtime expectations for UI error reporting."""
    if not required_paths:
        required_paths = runtime_relative_paths_for_source(source_id)
    return "\n".join(f"- `{path}`" for path in required_paths)


def bootstrap_runtime_assets(
    source_id: str,
    required_paths: Sequence[str] | None = None,
) -> SyncResult:
    """Synchronize only the selected source and runtime subset before rendering."""
    secrets = _secrets_mapping()
    _publish_runtime_secrets_to_env(secrets)
    required_tuple = tuple(
        str(path).strip() for path in (required_paths or ()) if str(path).strip()
    ) or None
    try:
        config = load_hf_repo_config(source_id=source_id, secrets=secrets)
        return _cached_runtime_sync(source_id, config, required_tuple)
    except Exception as err:
        st.error("Runtime data synchronization failed.")
        st.markdown(
            "This deployment is configured to load runtime assets from private "
            "Hugging Face dataset repositories, but synchronization did not complete."
        )
        st.markdown("Required configuration keys:")
        st.markdown("- `VISU2_HF_SOURCES_JSON` (preferred) or legacy `VISU2_HF_REPO_ID`/`VISU2_HF_REVISION`\n- `HF_TOKEN`")
        st.markdown("Expected runtime files:")
        st.markdown(_format_expected_paths_markdown(required_tuple, source_id=source_id))
        st.code(str(err))
        st.stop()

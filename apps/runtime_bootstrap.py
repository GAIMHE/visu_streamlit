"""
runtime_bootstrap.py

Bootstrap runtime asset synchronization before page logic executes.

Dependencies
------------
- collections
- streamlit
- visu2

Classes
-------
- None.

Functions
---------
- _secrets_mapping: Utility for secrets mapping.
- _format_expected_paths_markdown: Utility for format expected paths markdown.
- _cached_runtime_sync: Utility for cached runtime sync.
- bootstrap_runtime_assets: Utility for bootstrap runtime assets.
"""
from __future__ import annotations

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


def _secrets_mapping() -> Mapping[str, object] | None:
    """Secrets mapping.


Returns
-------
Mapping[str, object] | None
        Result produced by this routine.

"""
    try:
        return dict(st.secrets)
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def _cached_runtime_sync(
    config: HFRepoConfig | None,
    required_paths: tuple[str, ...] | None,
) -> SyncResult:
    """Cached runtime sync.

Parameters
----------
    config : HFRepoConfig | None
        Input parameter used by this routine.
    required_paths : tuple[str, ...] | None
        Input parameter used by this routine.

Returns
-------
SyncResult
        Result produced by this routine.

"""
    if config is None:
        return local_only_sync_result()
    settings = get_settings()
    return ensure_runtime_assets_from_hf(settings, config, required_paths=required_paths)


def _format_expected_paths_markdown(required_paths: tuple[str, ...] | None) -> str:
    """Format page-specific runtime expectations for UI error reporting.

    Parameters
    ----------
    required_paths : tuple[str, ...] | None
        Page-specific required runtime subset, or `None` for the legacy full set.

    Returns
    -------
    str
        Markdown bullet list of expected runtime files.
    """
    if not required_paths:
        return (
            "- `data/learning_catalog.json`\n"
            "- `data/zpdes_rules.json`\n"
            "- `data/exercises.json`\n"
            "- `artifacts/reports/consistency_report.json`\n"
            "- `artifacts/reports/derived_manifest.json`\n"
            "- `artifacts/derived/*.parquet`"
        )
    return "\n".join(f"- `{path}`" for path in required_paths)


def bootstrap_runtime_assets(required_paths: Sequence[str] | None = None) -> SyncResult:
    """Bootstrap runtime assets.

    Parameters
    ----------
    required_paths : Sequence[str] | None
        Optional page-specific runtime subset to synchronize before page logic
        executes.


Returns
-------
SyncResult
        Result produced by this routine.

"""
    secrets = _secrets_mapping()
    required_tuple = tuple(
        str(path).strip() for path in (required_paths or ()) if str(path).strip()
    ) or None
    try:
        config = load_hf_repo_config(secrets=secrets)
        return _cached_runtime_sync(config, required_tuple)
    except Exception as err:
        st.error("Runtime data synchronization failed.")
        st.markdown(
            "This deployment is configured to load runtime assets from a private "
            "Hugging Face dataset, but synchronization did not complete."
        )
        st.markdown("Required configuration keys:")
        st.markdown("- `VISU2_HF_REPO_ID`\n- `VISU2_HF_REVISION`\n- `HF_TOKEN`")
        st.markdown("Expected runtime files:")
        st.markdown(_format_expected_paths_markdown(required_tuple))
        st.code(str(err))
        st.stop()

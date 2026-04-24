"""Custom source-aware Streamlit shell for the learning analytics app."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from overview_shared import render_dashboard_style
from page_registry import (
    PAGE_SPEC_BY_ID,
    PageSpec,
    default_page_id_for_source,
    import_page_module,
    visible_pages_for_source,
)
from runtime_bootstrap import bootstrap_runtime_assets
from source_state import (
    get_active_page_id,
    get_active_source_id,
    set_active_page_id,
    set_active_source_id,
)

from visu2.runtime_sources import DEFAULT_SOURCE_ID, get_runtime_source

st.set_page_config(
    page_title="Learning Analytics Explorer",
    page_icon=":bar_chart:",
    layout="wide",
)


def _clear_page_data_cache() -> None:
    """Best-effort clear of Streamlit's data cache on navigation changes."""
    try:
        st.cache_data.clear()
    except Exception:
        return


def _source_option_label(source_id: str) -> str:
    source = get_runtime_source(source_id)
    return f"{source.label} [{source.source_id}]"


def _select_source() -> str:
    selected_source = DEFAULT_SOURCE_ID
    if get_active_source_id() != selected_source:
        set_active_source_id(selected_source)
        _clear_page_data_cache()
    st.sidebar.caption(f"Dataset source: {_source_option_label(selected_source)}")
    return selected_source


def _select_page(source_id: str) -> str:
    source = get_runtime_source(source_id)
    pages = visible_pages_for_source(source)
    default_page_id = default_page_id_for_source(source)
    visible_ids = [page.page_id for page in pages]
    label_by_id = {page.page_id: page.label for page in pages}
    requested_page_id = get_active_page_id(default_page_id)
    if requested_page_id not in visible_ids:
        show_redirect_info = requested_page_id != "home"
        requested_page_id = default_page_id
        set_active_page_id(requested_page_id)
        _clear_page_data_cache()
        if show_redirect_info:
            fallback_label = label_by_id.get(requested_page_id, requested_page_id)
            st.sidebar.info(
                f"That page is not available for the selected source. Showing {fallback_label} instead."
            )
    selected_page_id = st.sidebar.radio(
        "Page",
        options=visible_ids,
        index=visible_ids.index(requested_page_id),
        format_func=lambda page_id: label_by_id.get(page_id, page_id),
    )
    if selected_page_id != requested_page_id:
        set_active_page_id(selected_page_id)
        _clear_page_data_cache()
        st.rerun()
    return selected_page_id


def _render_page(source_id: str, page: PageSpec) -> None:
    bootstrap_runtime_assets(source_id, page.bootstrap_runtime_paths)
    module = import_page_module(page)
    renderer = getattr(module, "main", None)
    if not callable(renderer):
        raise RuntimeError(f"Page module {page.module_path} does not expose a callable main().")
    renderer()


def main() -> None:
    st.sidebar.title("Learning Analytics")
    render_dashboard_style()
    source_id = _select_source()
    selected_page_id = _select_page(source_id)

    page = PAGE_SPEC_BY_ID[selected_page_id]
    _render_page(source_id, page)


if __name__ == "__main__":
    main()

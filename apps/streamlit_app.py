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
from page_registry import PAGE_SPEC_BY_ID, PageSpec, import_page_module, visible_pages_for_source
from runtime_bootstrap import bootstrap_runtime_assets
from source_state import (
    get_active_page_id,
    get_active_source_id,
    set_active_page_id,
    set_active_source_id,
)

from visu2.runtime_sources import get_runtime_source, list_runtime_sources

st.set_page_config(
    page_title="Learning Analytics Explorer",
    page_icon=":bar_chart:",
    layout="wide",
)

HOME_PAGE_ID = "home"


def _source_option_label(source_id: str) -> str:
    source = get_runtime_source(source_id)
    return f"{source.label} [{source.source_id}]"


def _render_home(*, source_id: str) -> None:
    source = get_runtime_source(source_id)
    pages = visible_pages_for_source(source)
    render_dashboard_style()
    st.title("Learning Analytics Explorer")
    st.markdown(
        "Use the sidebar to choose a dataset source and then navigate to the supported analyses. "
        "Each source keeps its own runtime assets, so we only sync and load the files that matter "
        "for the current page."
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Active Source", source.label)
    c2.metric("Supported Pages", len(pages))
    c3.metric("Runtime Namespace", source.source_id)

    st.subheader("Source")
    st.write(source.description)
    st.code(str(source.runtime_root(ROOT_DIR)))

    st.subheader("Available Pages")
    for page in pages:
        st.markdown(f"- {page.icon} **{page.label}**")


def _select_source() -> str:
    source_specs = list_runtime_sources()
    source_ids = [spec.source_id for spec in source_specs]
    active_source_id = get_active_source_id()
    if active_source_id not in source_ids:
        active_source_id = source_ids[0]
    selected_source = st.sidebar.selectbox(
        "Dataset source",
        options=source_ids,
        index=source_ids.index(active_source_id),
        format_func=_source_option_label,
    )
    if selected_source != active_source_id:
        set_active_source_id(selected_source)
        st.rerun()
    return selected_source


def _select_page(source_id: str) -> str:
    source = get_runtime_source(source_id)
    pages = visible_pages_for_source(source)
    visible_ids = [HOME_PAGE_ID] + [page.page_id for page in pages]
    label_by_id = {HOME_PAGE_ID: "Home"}
    label_by_id.update({page.page_id: page.label for page in pages})
    requested_page_id = get_active_page_id(HOME_PAGE_ID)
    if requested_page_id not in visible_ids:
        requested_page_id = HOME_PAGE_ID
        set_active_page_id(requested_page_id)
        st.sidebar.info("That page is not available for the selected source. Showing Home instead.")
    selected_page_id = st.sidebar.radio(
        "Page",
        options=visible_ids,
        index=visible_ids.index(requested_page_id),
        format_func=lambda page_id: label_by_id.get(page_id, page_id),
    )
    if selected_page_id != requested_page_id:
        set_active_page_id(selected_page_id)
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
    source_id = _select_source()
    selected_page_id = _select_page(source_id)

    if selected_page_id == HOME_PAGE_ID:
        _render_home(source_id=source_id)
        return

    page = PAGE_SPEC_BY_ID[selected_page_id]
    _render_page(source_id, page)


if __name__ == "__main__":
    main()

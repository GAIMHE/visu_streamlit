from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

import streamlit_app
from page_registry import visible_pages_for_source

from visu2.runtime_sources import get_runtime_source


class _FakeSidebar:
    def __init__(self, *, selectbox_return: str | None = None, radio_return: str | None = None) -> None:
        self._selectbox_return = selectbox_return
        self._radio_return = radio_return
        self.info_messages: list[str] = []
        self.caption_messages: list[str] = []
        self.title_messages: list[str] = []

    def selectbox(self, *args, **kwargs):
        return self._selectbox_return

    def radio(self, *args, **kwargs):
        return self._radio_return

    def info(self, message: str) -> None:
        self.info_messages.append(message)

    def caption(self, message: str) -> None:
        self.caption_messages.append(message)

    def title(self, message: str) -> None:
        self.title_messages.append(message)


def test_select_source_clears_cache_when_source_changes(monkeypatch) -> None:
    sidebar = _FakeSidebar()
    events: list[str] = []

    monkeypatch.setattr(streamlit_app.st, "sidebar", sidebar)
    monkeypatch.setattr(streamlit_app, "get_active_source_id", lambda: "main")
    monkeypatch.setattr(streamlit_app, "set_active_source_id", lambda source_id: events.append(f"set:{source_id}"))
    monkeypatch.setattr(streamlit_app, "_clear_page_data_cache", lambda: events.append("clear"))

    selected = streamlit_app._select_source()

    assert selected == "neurips"
    assert events == ["set:neurips", "clear"]
    assert sidebar.caption_messages == ["Dataset source: MIAAM"]


def test_select_page_clears_cache_when_page_changes(monkeypatch) -> None:
    source = get_runtime_source("main")
    pages = visible_pages_for_source(source)
    visible_ids = [page.page_id for page in pages]
    assert "overview" in visible_ids
    assert "matrix" in visible_ids

    sidebar = _FakeSidebar(radio_return="matrix")
    events: list[str] = []

    monkeypatch.setattr(streamlit_app.st, "sidebar", sidebar)
    monkeypatch.setattr(streamlit_app, "get_active_page_id", lambda default_page_id="overview": "overview")
    monkeypatch.setattr(streamlit_app, "set_active_page_id", lambda page_id: events.append(f"set:{page_id}"))
    monkeypatch.setattr(streamlit_app, "_clear_page_data_cache", lambda: events.append("clear"))
    monkeypatch.setattr(streamlit_app.st, "rerun", lambda: events.append("rerun"))

    selected = streamlit_app._select_page("main")

    assert selected == "matrix"
    assert events == ["set:matrix", "clear", "rerun"]


def test_main_does_not_render_plot_appearance_controls(monkeypatch) -> None:
    sidebar = _FakeSidebar()
    events: list[str] = []

    monkeypatch.setattr(streamlit_app.st, "sidebar", sidebar)
    monkeypatch.setattr(streamlit_app, "render_dashboard_style", lambda: events.append("dashboard-style"))
    monkeypatch.setattr(streamlit_app, "_select_source", lambda: "neurips")
    monkeypatch.setattr(streamlit_app, "_select_page", lambda source_id: "overview")
    monkeypatch.setattr(
        streamlit_app,
        "_render_page",
        lambda source_id, page: events.append(f"render:{source_id}:{page.page_id}"),
    )

    streamlit_app.main()

    assert not hasattr(streamlit_app, "render_plotly_style_controls")
    assert sidebar.title_messages == ["Learning Analytics"]
    assert events == ["dashboard-style", "render:neurips:overview"]


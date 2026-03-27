"""Streamlit session/query helpers for source and page selection."""

from __future__ import annotations

import streamlit as st

from visu2.runtime_sources import DEFAULT_SOURCE_ID, RUNTIME_SOURCES

SOURCE_QUERY_KEY = "source"
PAGE_QUERY_KEY = "page"
SOURCE_SESSION_KEY = "visu2_active_source"
PAGE_SESSION_KEY = "visu2_active_page"


def _normalize_scalar(value: object) -> str | None:
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    text = str(value or "").strip()
    return text or None


def get_query_value(key: str) -> str | None:
    """Read one query-parameter value in a Streamlit-version-safe way."""
    try:
        return _normalize_scalar(st.query_params.get(key))
    except Exception:
        try:
            params = st.experimental_get_query_params()
        except Exception:
            return None
        value = params.get(key)
        return _normalize_scalar(value)


def set_query_value(key: str, value: str | None) -> None:
    """Persist one query-parameter value in a Streamlit-version-safe way."""
    try:
        if value is None:
            try:
                del st.query_params[key]
            except Exception:
                pass
            return
        st.query_params[key] = value
        return
    except Exception:
        pass

    try:
        params = st.experimental_get_query_params()
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
        st.experimental_set_query_params(**params)
    except Exception:
        return


def get_active_source_id() -> str:
    """Return the currently selected runtime source id."""
    query_value = get_query_value(SOURCE_QUERY_KEY)
    session_value = _normalize_scalar(st.session_state.get(SOURCE_SESSION_KEY))
    candidate = query_value or session_value or DEFAULT_SOURCE_ID
    if candidate not in RUNTIME_SOURCES:
        candidate = DEFAULT_SOURCE_ID
    st.session_state[SOURCE_SESSION_KEY] = candidate
    if query_value != candidate:
        set_query_value(SOURCE_QUERY_KEY, candidate)
    return candidate


def set_active_source_id(source_id: str) -> None:
    """Persist the chosen runtime source id to session and URL."""
    normalized = str(source_id or DEFAULT_SOURCE_ID).strip() or DEFAULT_SOURCE_ID
    if normalized not in RUNTIME_SOURCES:
        normalized = DEFAULT_SOURCE_ID
    st.session_state[SOURCE_SESSION_KEY] = normalized
    set_query_value(SOURCE_QUERY_KEY, normalized)


def get_active_page_id(default_page_id: str = "home") -> str:
    """Return the currently selected page id."""
    query_value = get_query_value(PAGE_QUERY_KEY)
    session_value = _normalize_scalar(st.session_state.get(PAGE_SESSION_KEY))
    candidate = query_value or session_value or default_page_id
    st.session_state[PAGE_SESSION_KEY] = candidate
    if query_value != candidate:
        set_query_value(PAGE_QUERY_KEY, candidate)
    return candidate


def set_active_page_id(page_id: str) -> None:
    """Persist the chosen page id to session and URL."""
    normalized = str(page_id or "home").strip() or "home"
    st.session_state[PAGE_SESSION_KEY] = normalized
    set_query_value(PAGE_QUERY_KEY, normalized)

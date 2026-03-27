"""Streamlit session/query helpers for source and page selection."""

from __future__ import annotations

from datetime import date

import streamlit as st

from visu2.runtime_sources import DEFAULT_SOURCE_ID, RUNTIME_SOURCES

SOURCE_QUERY_KEY = "source"
PAGE_QUERY_KEY = "page"
FILTER_START_QUERY_KEY = "start_date"
FILTER_END_QUERY_KEY = "end_date"
FILTER_MIN_ATTEMPTS_QUERY_KEY = "min_attempts"
SOURCE_SESSION_KEY = "visu2_active_source"
PAGE_SESSION_KEY = "visu2_active_page"
FILTER_START_SESSION_KEY = "visu2_filter_start_date"
FILTER_END_SESSION_KEY = "visu2_filter_end_date"
FILTER_MIN_ATTEMPTS_SESSION_KEY = "visu2_filter_min_attempts"


def _normalize_scalar(value: object) -> str | None:
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    text = str(value or "").strip()
    return text or None


def _filter_key(base_key: str, source_id: str) -> str:
    normalized_source = str(source_id or DEFAULT_SOURCE_ID).strip() or DEFAULT_SOURCE_ID
    return f"{base_key}::{normalized_source}"


def _parse_date_value(value: str | None) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


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


def get_filter_date_range(source_id: str) -> tuple[date | None, date | None]:
    """Return the persisted source-scoped global date filter."""
    start_query = _parse_date_value(get_query_value(_filter_key(FILTER_START_QUERY_KEY, source_id)))
    end_query = _parse_date_value(get_query_value(_filter_key(FILTER_END_QUERY_KEY, source_id)))
    start_session = _parse_date_value(
        _normalize_scalar(st.session_state.get(_filter_key(FILTER_START_SESSION_KEY, source_id)))
    )
    end_session = _parse_date_value(
        _normalize_scalar(st.session_state.get(_filter_key(FILTER_END_SESSION_KEY, source_id)))
    )
    start_date = start_query or start_session
    end_date = end_query or end_session
    if start_date is not None:
        st.session_state[_filter_key(FILTER_START_SESSION_KEY, source_id)] = start_date.isoformat()
        if start_query != start_date:
            set_query_value(_filter_key(FILTER_START_QUERY_KEY, source_id), start_date.isoformat())
    if end_date is not None:
        st.session_state[_filter_key(FILTER_END_SESSION_KEY, source_id)] = end_date.isoformat()
        if end_query != end_date:
            set_query_value(_filter_key(FILTER_END_QUERY_KEY, source_id), end_date.isoformat())
    return start_date, end_date


def set_filter_date_range(source_id: str, *, start_date: date, end_date: date) -> None:
    """Persist the source-scoped global date filter to session and URL."""
    start_text = start_date.isoformat()
    end_text = end_date.isoformat()
    st.session_state[_filter_key(FILTER_START_SESSION_KEY, source_id)] = start_text
    st.session_state[_filter_key(FILTER_END_SESSION_KEY, source_id)] = end_text
    set_query_value(_filter_key(FILTER_START_QUERY_KEY, source_id), start_text)
    set_query_value(_filter_key(FILTER_END_QUERY_KEY, source_id), end_text)


def get_filter_min_attempts(source_id: str) -> int:
    """Return the persisted source-scoped minimum visible attempts filter."""
    query_value = _normalize_scalar(get_query_value(_filter_key(FILTER_MIN_ATTEMPTS_QUERY_KEY, source_id)))
    session_value = _normalize_scalar(
        st.session_state.get(_filter_key(FILTER_MIN_ATTEMPTS_SESSION_KEY, source_id))
    )
    for candidate in (query_value, session_value):
        if candidate is None:
            continue
        try:
            value = max(1, int(candidate))
        except ValueError:
            continue
        st.session_state[_filter_key(FILTER_MIN_ATTEMPTS_SESSION_KEY, source_id)] = value
        if query_value != str(value):
            set_query_value(_filter_key(FILTER_MIN_ATTEMPTS_QUERY_KEY, source_id), str(value))
        return value
    return 1


def set_filter_min_attempts(source_id: str, min_attempts: int) -> None:
    """Persist the source-scoped minimum visible attempts filter."""
    normalized = max(1, int(min_attempts))
    st.session_state[_filter_key(FILTER_MIN_ATTEMPTS_SESSION_KEY, source_id)] = normalized
    set_query_value(_filter_key(FILTER_MIN_ATTEMPTS_QUERY_KEY, source_id), str(normalized))


def clear_filter_state(source_id: str) -> None:
    """Clear the source-scoped global filter state from session and URL."""
    for base_key in (
        FILTER_START_SESSION_KEY,
        FILTER_END_SESSION_KEY,
        FILTER_MIN_ATTEMPTS_SESSION_KEY,
    ):
        st.session_state.pop(_filter_key(base_key, source_id), None)
    for base_key in (
        FILTER_START_QUERY_KEY,
        FILTER_END_QUERY_KEY,
        FILTER_MIN_ATTEMPTS_QUERY_KEY,
    ):
        set_query_value(_filter_key(base_key, source_id), None)

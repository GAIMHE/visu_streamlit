"""Shared Plotly chart config helpers for Streamlit pages."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

DEFAULT_PLOTLY_CHART_CONFIG: dict[str, Any] = {
    "displaylogo": False,
    "toImageButtonOptions": {
        "format": "svg",
        "filename": "plotly-export",
        "scale": 4,
    },
}


def _merge_config(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge Plotly config dictionaries."""
    merged = deepcopy(base)
    for key, value in extra.items():
        if (
            isinstance(value, dict)
            and isinstance(merged.get(key), dict)
        ):
            merged[key] = _merge_config(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def build_plotly_chart_config(
    *,
    modebar_buttons_to_remove: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a shared Plotly config with SVG export enabled by default."""
    config = deepcopy(DEFAULT_PLOTLY_CHART_CONFIG)
    if modebar_buttons_to_remove:
        config["modeBarButtonsToRemove"] = list(modebar_buttons_to_remove)
    if extra:
        config = _merge_config(config, extra)
    return config

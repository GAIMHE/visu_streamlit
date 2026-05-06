"""Shared Plotly chart config helpers for Streamlit pages."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import plotly.graph_objects as go

DEFAULT_PLOTLY_CHART_CONFIG: dict[str, Any] = {
    "displaylogo": False,
    "toImageButtonOptions": {
        "format": "png",
        "filename": "plotly-export",
        "scale": 4,
    },
}

PLOTLY_EXPORT_FORMATS: tuple[str, ...] = ("png", "svg")
PLOTLY_EXPORT_FORMAT_SESSION_KEY = "visu2_plotly_export_format"

DEFAULT_PLOTLY_STYLE_SETTINGS: dict[str, Any] = {
    "enabled": False,
    "background_color": "#FFFFFF",
    "font_color": "#1E1B18",
    "title_font_size": 22,
    "axis_title_font_size": 14,
    "axis_tick_font_size": 12,
    "in_plot_text_font_size": 13,
    "horizontal_bar_spacing_scale": 1.0,
}

PLOTLY_STYLE_SESSION_KEYS: dict[str, str] = {
    "enabled": "visu2_plotly_style_enabled",
    "background_color": "visu2_plotly_style_background_color",
    "font_color": "visu2_plotly_style_font_color",
    "title_font_size": "visu2_plotly_style_title_font_size",
    "axis_title_font_size": "visu2_plotly_style_axis_title_font_size",
    "axis_tick_font_size": "visu2_plotly_style_axis_tick_font_size",
    "in_plot_text_font_size": "visu2_plotly_style_in_plot_text_font_size",
    "horizontal_bar_spacing_scale": "visu2_plotly_style_horizontal_bar_spacing_scale",
}

DEFAULT_HORIZONTAL_BAR_WIDTH = 0.72


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


def default_plotly_style_settings() -> dict[str, Any]:
    """Return a copy of the default global Plotly appearance settings."""
    return deepcopy(DEFAULT_PLOTLY_STYLE_SETTINGS)


def _coerce_style_settings(style: dict[str, Any] | None) -> dict[str, Any]:
    settings = default_plotly_style_settings()
    if style:
        settings.update(style)
    for key in (
        "title_font_size",
        "axis_title_font_size",
        "axis_tick_font_size",
        "in_plot_text_font_size",
    ):
        settings[key] = max(1, int(settings[key]))
    settings["horizontal_bar_spacing_scale"] = min(
        2.5,
        max(1.0, float(settings["horizontal_bar_spacing_scale"])),
    )
    settings["background_color"] = str(settings["background_color"])
    settings["font_color"] = str(settings["font_color"])
    settings["enabled"] = bool(settings["enabled"])
    return settings


def _normalize_export_format(export_format: object) -> str:
    normalized = str(export_format or "").strip().lower()
    if normalized in PLOTLY_EXPORT_FORMATS:
        return normalized
    return str(DEFAULT_PLOTLY_CHART_CONFIG["toImageButtonOptions"]["format"])


def _export_format_from_session_state(st_module: Any) -> str:
    return _normalize_export_format(
        st_module.session_state.get(PLOTLY_EXPORT_FORMAT_SESSION_KEY)
    )


def _style_from_session_state(st_module: Any) -> dict[str, Any]:
    style = default_plotly_style_settings()
    for setting_key, session_key in PLOTLY_STYLE_SESSION_KEYS.items():
        if session_key in st_module.session_state:
            style[setting_key] = st_module.session_state[session_key]
    return _coerce_style_settings(style)


def render_plotly_style_controls() -> None:
    """Render global Plotly appearance controls in the Streamlit sidebar."""
    import streamlit as st

    with st.sidebar.expander("Plot Appearance", expanded=False):
        st.selectbox(
            "Export image format",
            options=list(PLOTLY_EXPORT_FORMATS),
            index=PLOTLY_EXPORT_FORMATS.index(
                str(DEFAULT_PLOTLY_CHART_CONFIG["toImageButtonOptions"]["format"])
            ),
            format_func=lambda value: str(value).upper(),
            key=PLOTLY_EXPORT_FORMAT_SESSION_KEY,
            help="Controls the format used by Plotly's modebar camera export button.",
        )
        st.checkbox(
            "Customize all Plotly charts",
            value=bool(DEFAULT_PLOTLY_STYLE_SETTINGS["enabled"]),
            key=PLOTLY_STYLE_SESSION_KEYS["enabled"],
            help="Apply the style below to every Plotly chart rendered by the app.",
        )
        if not bool(st.session_state.get(PLOTLY_STYLE_SESSION_KEYS["enabled"], False)):
            st.caption("Enable customization to override chart backgrounds and font styling.")
            return

        st.color_picker(
            "Background color",
            value=str(DEFAULT_PLOTLY_STYLE_SETTINGS["background_color"]),
            key=PLOTLY_STYLE_SESSION_KEYS["background_color"],
        )
        st.color_picker(
            "Font color",
            value=str(DEFAULT_PLOTLY_STYLE_SETTINGS["font_color"]),
            key=PLOTLY_STYLE_SESSION_KEYS["font_color"],
        )
        st.slider(
            "Title font size",
            min_value=12,
            max_value=40,
            value=int(DEFAULT_PLOTLY_STYLE_SETTINGS["title_font_size"]),
            step=1,
            key=PLOTLY_STYLE_SESSION_KEYS["title_font_size"],
        )
        st.slider(
            "X/Y title font size",
            min_value=9,
            max_value=28,
            value=int(DEFAULT_PLOTLY_STYLE_SETTINGS["axis_title_font_size"]),
            step=1,
            key=PLOTLY_STYLE_SESSION_KEYS["axis_title_font_size"],
        )
        st.slider(
            "X/Y tick font size",
            min_value=8,
            max_value=24,
            value=int(DEFAULT_PLOTLY_STYLE_SETTINGS["axis_tick_font_size"]),
            step=1,
            key=PLOTLY_STYLE_SESSION_KEYS["axis_tick_font_size"],
        )
        st.slider(
            "In-plot text font size",
            min_value=8,
            max_value=32,
            value=int(DEFAULT_PLOTLY_STYLE_SETTINGS["in_plot_text_font_size"]),
            step=1,
            key=PLOTLY_STYLE_SESSION_KEYS["in_plot_text_font_size"],
            help="Controls labels drawn inside the chart area, such as bar-value labels.",
        )
        st.slider(
            "Horizontal bar row spacing",
            min_value=1.0,
            max_value=2.5,
            value=float(DEFAULT_PLOTLY_STYLE_SETTINGS["horizontal_bar_spacing_scale"]),
            step=0.1,
            key=PLOTLY_STYLE_SESSION_KEYS["horizontal_bar_spacing_scale"],
            help=(
                "Increases vertical room between rows in horizontal bar charts while "
                "keeping bar thickness roughly stable."
            ),
        )


def _trace_has_visible_text(trace: Any) -> bool:
    """Return whether a Plotly trace renders text labels in the chart area."""
    mode = str(getattr(trace, "mode", "") or "")
    if "text" in mode:
        return True
    for attr_name in ("text", "texttemplate", "textinfo"):
        value = getattr(trace, attr_name, None)
        if value is not None and str(value).strip() != "":
            return True
    return False


def _trace_has_per_point_text_color(trace: Any) -> bool:
    """Return whether a trace already carries a sequence of text colors."""
    textfont = getattr(trace, "textfont", None)
    color = getattr(textfont, "color", None)
    return isinstance(color, list | tuple)


def _is_horizontal_bar_trace(trace: Any) -> bool:
    """Return whether a trace is a horizontal bar trace."""
    return str(getattr(trace, "type", "") or "") == "bar" and str(
        getattr(trace, "orientation", "") or ""
    ) == "h"


def _count_trace_categories(values: Any) -> int:
    """Count non-empty category values in a Plotly coordinate sequence."""
    if values is None:
        return 0
    try:
        items = list(values)
    except TypeError:
        items = [values]
    return len({str(value) for value in items if str(value or "").strip()})


def _layout_vertical_margin_height(figure: go.Figure) -> int:
    """Return the figure's configured vertical margin height."""
    margin = getattr(figure.layout, "margin", None)
    if margin is None:
        return 0
    return int(getattr(margin, "t", 0) or 0) + int(getattr(margin, "b", 0) or 0)


def _base_horizontal_bar_width(trace: Any) -> float:
    """Return the category-width fraction to preserve when adding row spacing."""
    width = getattr(trace, "width", None)
    if isinstance(width, int | float):
        return max(0.02, min(1.0, float(width)))
    return DEFAULT_HORIZONTAL_BAR_WIDTH


def _apply_horizontal_bar_spacing(figure: go.Figure, spacing_scale: float) -> None:
    """Increase row spacing in horizontal bar charts while preserving bar thickness."""
    if spacing_scale <= 1.0:
        return
    horizontal_bars = [trace for trace in figure.data if _is_horizontal_bar_trace(trace)]
    if not horizontal_bars:
        return

    max_categories = max((_count_trace_categories(getattr(trace, "y", None)) for trace in horizontal_bars), default=0)
    if max_categories <= 0:
        return

    current_height = getattr(figure.layout, "height", None)
    base_height = int(current_height) if current_height is not None else 420
    extra_height = int(round((spacing_scale - 1.0) * max_categories * 34))
    new_height = base_height + extra_height
    figure.update_layout(height=new_height)

    vertical_margins = _layout_vertical_margin_height(figure)
    base_plot_height = max(1, base_height - vertical_margins)
    new_plot_height = max(1, new_height - vertical_margins)
    for trace in horizontal_bars:
        base_width = _base_horizontal_bar_width(trace)
        bar_width = max(0.02, min(base_width, base_width * base_plot_height / new_plot_height))
        trace.update(width=bar_width)


def apply_plotly_chart_style(
    figure_or_data: Any,
    style: dict[str, Any] | None = None,
) -> Any:
    """Apply global appearance settings to a Plotly figure-like object."""
    settings = _coerce_style_settings(style)
    if not settings["enabled"]:
        return figure_or_data
    try:
        figure = go.Figure(figure_or_data)
    except Exception:
        return figure_or_data

    background_color = str(settings["background_color"])
    font_color = str(settings["font_color"])
    title_font_size = int(settings["title_font_size"])
    axis_title_font_size = int(settings["axis_title_font_size"])
    axis_tick_font_size = int(settings["axis_tick_font_size"])
    in_plot_text_font_size = int(settings["in_plot_text_font_size"])
    horizontal_bar_spacing_scale = float(settings["horizontal_bar_spacing_scale"])

    title_text = getattr(figure.layout.title, "text", None)
    has_title_text = title_text is not None and str(title_text).strip() != ""

    layout_updates: dict[str, Any] = {
        "paper_bgcolor": background_color,
        "plot_bgcolor": background_color,
        "font": {"color": font_color},
        "legend_font_color": font_color,
        "legend_font_size": axis_tick_font_size,
    }
    if has_title_text:
        layout_updates["title_font_size"] = title_font_size
        layout_updates["title_font_color"] = font_color

    figure.update_layout(
        **layout_updates,
    )
    figure.update_xaxes(
        title_font_size=axis_title_font_size,
        title_font_color=font_color,
        tickfont_size=axis_tick_font_size,
        tickfont_color=font_color,
    )
    figure.update_yaxes(
        title_font_size=axis_title_font_size,
        title_font_color=font_color,
        tickfont_size=axis_tick_font_size,
        tickfont_color=font_color,
    )
    figure.update_polars(
        bgcolor=background_color,
        radialaxis_tickfont_size=axis_tick_font_size,
        radialaxis_tickfont_color=font_color,
        angularaxis_tickfont_size=axis_tick_font_size,
        angularaxis_tickfont_color=font_color,
    )
    for annotation in figure.layout.annotations or ():
        annotation.font.color = font_color
        annotation.font.size = axis_title_font_size
    for trace in figure.data:
        if not _trace_has_visible_text(trace):
            continue
        try:
            textfont_update: dict[str, Any] = {"size": in_plot_text_font_size}
            if not _trace_has_per_point_text_color(trace):
                textfont_update["color"] = font_color
            trace.update(textfont=textfont_update)
        except Exception:
            continue
    _apply_horizontal_bar_spacing(figure, horizontal_bar_spacing_scale)
    return figure


def apply_plotly_export_config(
    config: dict[str, Any] | None,
    export_format: object,
) -> dict[str, Any]:
    """Return chart config with the selected Plotly image export format applied."""
    merged = build_plotly_chart_config(extra=config or {})
    merged.setdefault("toImageButtonOptions", {})
    merged["toImageButtonOptions"]["format"] = _normalize_export_format(export_format)
    return merged


def install_plotly_style_hook() -> None:
    """Patch Streamlit's Plotly renderer so global style controls affect every chart."""
    import streamlit as st

    if bool(getattr(st, "_visu2_plotly_style_hook_installed", False)):
        return

    original_plotly_chart = st.plotly_chart

    def styled_plotly_chart(figure_or_data: Any = None, *args: Any, **kwargs: Any) -> Any:
        styled_figure = apply_plotly_chart_style(
            figure_or_data,
            _style_from_session_state(st),
        )
        kwargs["config"] = apply_plotly_export_config(
            kwargs.get("config"),
            _export_format_from_session_state(st),
        )
        return original_plotly_chart(styled_figure, *args, **kwargs)

    st.plotly_chart = styled_plotly_chart
    setattr(st, "_visu2_plotly_style_hook_installed", True)
    setattr(st, "_visu2_original_plotly_chart", original_plotly_chart)


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

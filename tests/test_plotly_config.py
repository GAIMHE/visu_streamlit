"""Tests for shared Plotly chart config helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.graph_objects as go
import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from plotly_config import (
    PLOTLY_EXPORT_FORMATS,
    apply_plotly_chart_style,
    apply_plotly_export_config,
    build_plotly_chart_config,
    default_plotly_style_settings,
)


def test_build_plotly_chart_config_sets_png_export_by_default() -> None:
    """Shared config should use PNG export by default."""
    config = build_plotly_chart_config()

    assert config["displaylogo"] is False
    assert PLOTLY_EXPORT_FORMATS[0] == "png"
    assert config["toImageButtonOptions"]["format"] == "png"
    assert config["toImageButtonOptions"]["scale"] == 4


def test_build_plotly_chart_config_merges_extra_options() -> None:
    """Extra options should merge without dropping the export defaults."""
    config = build_plotly_chart_config(
        modebar_buttons_to_remove=["select2d", "lasso2d"],
        extra={"toImageButtonOptions": {"filename": "custom-export"}},
    )

    assert config["modeBarButtonsToRemove"] == ["select2d", "lasso2d"]
    assert config["toImageButtonOptions"]["format"] == "png"
    assert config["toImageButtonOptions"]["scale"] == 4
    assert config["toImageButtonOptions"]["filename"] == "custom-export"


def test_apply_plotly_export_config_sets_png_without_dropping_defaults() -> None:
    """Global export controls should override only the export format."""
    config = apply_plotly_export_config(
        {"toImageButtonOptions": {"filename": "custom-export"}},
        "png",
    )

    assert config["displaylogo"] is False
    assert config["toImageButtonOptions"]["format"] == "png"
    assert config["toImageButtonOptions"]["scale"] == 4
    assert config["toImageButtonOptions"]["filename"] == "custom-export"


def test_apply_plotly_export_config_rejects_unknown_format() -> None:
    """Unknown export formats should fall back to the project default."""
    config = apply_plotly_export_config(None, "pdf")

    assert config["toImageButtonOptions"]["format"] == "png"


def test_apply_plotly_chart_style_skips_when_disabled() -> None:
    """Disabled style settings should leave the original figure object untouched."""
    figure = go.Figure(data=[go.Bar(x=["A"], y=[1])])
    style = default_plotly_style_settings()
    style["enabled"] = False

    styled = apply_plotly_chart_style(figure, style)

    assert styled is figure


def test_apply_plotly_chart_style_updates_cartesian_fonts_and_background() -> None:
    """Enabled style settings should update common Cartesian chart layout fields."""
    figure = go.Figure(data=[go.Bar(x=["A"], y=[1])])
    figure.update_layout(title="Example", xaxis_title="X axis", yaxis_title="Y axis")
    style = {
        "enabled": True,
        "background_color": "#123456",
        "font_color": "#ABCDEF",
        "title_font_size": 28,
        "axis_title_font_size": 18,
        "axis_tick_font_size": 11,
    }

    styled = apply_plotly_chart_style(figure, style)

    assert styled.layout.paper_bgcolor == "#123456"
    assert styled.layout.plot_bgcolor == "#123456"
    assert styled.layout.font.color == "#ABCDEF"
    assert styled.layout.title.font.size == 28
    assert styled.layout.title.font.color == "#ABCDEF"
    assert styled.layout.xaxis.title.font.size == 18
    assert styled.layout.xaxis.tickfont.size == 11
    assert styled.layout.yaxis.title.font.size == 18
    assert styled.layout.yaxis.tickfont.color == "#ABCDEF"


def test_apply_plotly_chart_style_does_not_create_empty_title() -> None:
    """Style controls should not create a Plotly title when the chart has none."""
    figure = go.Figure(data=[go.Bar(x=["A"], y=[1])])
    style = {
        "enabled": True,
        "background_color": "#FFFFFF",
        "font_color": "#223344",
        "title_font_size": 20,
        "axis_title_font_size": 15,
        "axis_tick_font_size": 13,
    }

    styled = apply_plotly_chart_style(figure, style)

    assert styled.layout.title.text is None
    assert "title" not in styled.to_plotly_json()["layout"]


def test_apply_plotly_chart_style_updates_in_plot_trace_text() -> None:
    """Enabled style settings should update value labels rendered inside charts."""
    figure = go.Figure(data=[go.Bar(x=["A"], y=[1], text=["1"], textposition="outside")])
    style = {
        "enabled": True,
        "background_color": "#FFFFFF",
        "font_color": "#445566",
        "title_font_size": 20,
        "axis_title_font_size": 15,
        "axis_tick_font_size": 13,
        "in_plot_text_font_size": 21,
    }

    styled = apply_plotly_chart_style(figure, style)

    assert styled.data[0].textfont.color == "#445566"
    assert styled.data[0].textfont.size == 21


def test_apply_plotly_chart_style_preserves_per_point_text_colors() -> None:
    """Per-cell contrast colors should survive global Plotly styling."""
    figure = go.Figure(
        data=[
            go.Scatter(
                x=["A", "B"],
                y=[1, 2],
                mode="text",
                text=["low", "high"],
                textfont={"color": ["#111111", "#FFFFFF"], "size": 11},
            )
        ]
    )
    style = {
        "enabled": True,
        "background_color": "#FFFFFF",
        "font_color": "#445566",
        "title_font_size": 20,
        "axis_title_font_size": 15,
        "axis_tick_font_size": 13,
        "in_plot_text_font_size": 21,
    }

    styled = apply_plotly_chart_style(figure, style)

    assert styled.data[0].textfont.color == ("#111111", "#FFFFFF")
    assert styled.data[0].textfont.size == 21


def test_apply_plotly_chart_style_scales_horizontal_bar_row_spacing() -> None:
    """Horizontal bar spacing should increase row room while preserving bar thickness."""
    figure = go.Figure(
        data=[
            go.Bar(
                x=[10, 20, 30],
                y=["First", "Second", "Third"],
                orientation="h",
            )
        ]
    )
    figure.update_layout(height=360)
    style = {
        "enabled": True,
        "background_color": "#FFFFFF",
        "font_color": "#445566",
        "title_font_size": 20,
        "axis_title_font_size": 15,
        "axis_tick_font_size": 13,
        "horizontal_bar_spacing_scale": 1.8,
    }

    styled = apply_plotly_chart_style(figure, style)

    assert styled.layout.height > 360
    assert styled.layout.bargap is None
    baseline_bar_pixels = 0.72 * (360 / 3)
    styled_bar_pixels = styled.data[0].width * (styled.layout.height / 3)
    assert styled_bar_pixels == pytest.approx(baseline_bar_pixels, rel=0.01)


def test_apply_plotly_chart_style_updates_polar_background_and_fonts() -> None:
    """Enabled style settings should also cover polar charts used by spider plots."""
    figure = go.Figure(data=[go.Scatterpolar(r=[1, 2], theta=["A", "B"])])
    style = {
        "enabled": True,
        "background_color": "#FFFFFF",
        "font_color": "#223344",
        "title_font_size": 20,
        "axis_title_font_size": 15,
        "axis_tick_font_size": 13,
    }

    styled = apply_plotly_chart_style(figure, style)

    assert styled.layout.polar.bgcolor == "#FFFFFF"
    assert styled.layout.polar.radialaxis.tickfont.size == 13
    assert styled.layout.polar.radialaxis.tickfont.color == "#223344"
    assert styled.layout.polar.angularaxis.tickfont.size == 13
    assert styled.layout.polar.angularaxis.tickfont.color == "#223344"

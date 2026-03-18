"""Shared Streamlit renderer for deterministic figure analysis blocks."""

from __future__ import annotations

import streamlit as st

from visu2.figure_analysis import FigureAnalysis


def render_figure_analysis(analysis: FigureAnalysis) -> None:
    """Render a collapsed analysis expander below a figure."""
    with st.expander("Analysis", expanded=False):
        if analysis.findings:
            st.markdown("**Findings**")
            for finding in analysis.findings:
                st.markdown(f"- {finding}")
        if analysis.caveats:
            st.markdown("**Caveats**")
            for caveat in analysis.caveats:
                st.markdown(f"- {caveat}")
        if not analysis.findings and not analysis.caveats:
            st.write("Not enough evidence for automated analysis in the current filter scope.")

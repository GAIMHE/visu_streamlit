"""Stable facade for ZPDES dependency parsing, topology, and overlays."""

from __future__ import annotations

from .zpdes_overlays import attach_overlay_metrics_to_nodes, filter_dependency_graph_by_objectives
from .zpdes_topology import (
    build_dependency_tables_from_metadata,
    list_supported_module_codes_from_metadata,
)
from .zpdes_types import parse_dependency_tokens

__all__ = [
    "parse_dependency_tokens",
    "build_dependency_tables_from_metadata",
    "list_supported_module_codes_from_metadata",
    "attach_overlay_metrics_to_nodes",
    "filter_dependency_graph_by_objectives",
]

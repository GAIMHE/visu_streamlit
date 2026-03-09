"""Stable facade for objective-activity matrix builders."""

from __future__ import annotations

from .matrix_cells import build_objective_activity_cells
from .matrix_drilldown import build_exercise_drilldown_frame, build_ragged_matrix_payload
from .matrix_types import VALID_MATRIX_METRICS, format_cell_value

__all__ = [
    "VALID_MATRIX_METRICS",
    "format_cell_value",
    "build_objective_activity_cells",
    "build_ragged_matrix_payload",
    "build_exercise_drilldown_frame",
]

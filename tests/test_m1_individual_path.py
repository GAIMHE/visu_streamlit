from __future__ import annotations

from datetime import datetime

import polars as pl

from visu2.m1_individual_path import (
    build_m1_individual_path_figure,
    build_m1_individual_path_payload,
)


def _nodes() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "module_code": ["M1"] * 4,
            "node_id": ["o1", "a1", "a2", "a3"],
            "node_code": ["M1O1", "M1O1A1", "M1O1A2", "M1O1A3"],
            "node_type": ["objective", "activity", "activity", "activity"],
            "label": ["Objective 1", "A1", "A2", "A3"],
            "objective_code": ["M1O1", "M1O1", "M1O1", "M1O1"],
            "activity_index": [None, 1, 2, 3],
            "init_open": [True, True, False, False],
            "source_primary": ["catalog"] * 4,
            "source_enrichment": ["rules"] * 4,
            "is_ghost": [False] * 4,
        }
    )


def _edges() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "module_code": ["M1", "M1"],
            "edge_id": ["e1", "e2"],
            "edge_type": ["activation", "activation"],
            "from_node_code": ["M1O1A1", "M1O1A2"],
            "to_node_code": ["M1O1A2", "M1O1A3"],
            "threshold_type": ["success_rate", "success_rate"],
            "threshold_value": [0.75, 0.75],
            "rule_text": ["r1", "r2"],
            "source_primary": ["rules", "rules"],
            "source_enrichment": ["rules", "rules"],
            "enrich_lvl": [None, None],
            "enrich_sr": [None, None],
        }
    )


def _events() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 9, 2, 0),
                datetime(2025, 1, 1, 9, 4, 0),
                datetime(2025, 1, 1, 9, 6, 0),
            ],
            "date_utc": ["2025-01-01"] * 4,
            "user_id": ["u1"] * 4,
            "objective_id": ["o1", "o1", "o1", "o1"],
            "objective_label": ["Objective 1"] * 4,
            "activity_id": ["a1", "a1", "missing", "a2"],
            "activity_label": ["A1", "A1", "Missing", "A2"],
            "exercise_id": ["e1", "e2", "e_missing", "e3"],
            "data_correct": [1, 0, 1, 1],
            "work_mode": ["adaptive-test", "zpdes", "playlist", "zpdes"],
            "attempt_number": [1, 2, 1, 1],
            "module_code": ["M1"] * 4,
            "module_label": ["Module 1"] * 4,
        }
    )


def test_build_m1_individual_path_payload_tracks_cutoffs_and_unmapped_attempts() -> None:
    payload = build_m1_individual_path_payload(_events(), ["u1"], 2, _nodes(), _edges())

    assert payload["frame_cutoffs"] == [0, 2, 4]
    series = payload["series"]["u1"]
    assert series["attempt_ordinal"] == [1, 2, 3, 4]
    assert series["mapped_attempt_total"] == 3
    assert series["unmapped_attempt_total"] == 1
    assert series["max_activity_attempt_count"] == 2


def test_build_m1_individual_path_figure_starts_grey_and_then_adds_arrow_overlay() -> None:
    payload = build_m1_individual_path_payload(_events(), ["u1"], 2, _nodes(), _edges())

    initial = build_m1_individual_path_figure(payload, frame_idx=0)
    assert any(
        getattr(getattr(trace, "marker", None), "color", None) == "rgba(122, 127, 137, 0.35)"
        for trace in initial.data
    )
    assert len(initial.layout.annotations or []) == 0

    final = build_m1_individual_path_figure(payload, frame_idx=2)
    color_traces = [
        trace
        for trace in final.data
        if getattr(getattr(trace, "marker", None), "showscale", False)
    ]
    assert len(color_traces) == 1
    overlay_trace = color_traces[0]
    assert list(overlay_trace.marker.color) == [0.5, 1.0]
    assert len(overlay_trace.marker.size) == 2
    assert len(final.layout.annotations or []) == 1


from __future__ import annotations

from datetime import date

import polars as pl

from visu2.zpdes_dependencies import (
    attach_overlay_metrics_to_nodes,
    filter_dependency_graph_by_objectives,
    parse_dependency_tokens,
)


def test_parse_dependency_tokens_with_percent_and_multivalue() -> None:
    tokens = parse_dependency_tokens("M31O1A1, M31O1A2(75%), M31O2")
    assert [t["code"] for t in tokens] == ["M31O1A1", "M31O1A2", "M31O2"]
    assert tokens[0]["threshold"] is None
    assert float(tokens[1]["threshold"]) == 0.75
    assert tokens[2]["threshold"] is None


def test_attach_overlay_metrics_is_weighted_for_objectives_and_activities() -> None:
    nodes = pl.DataFrame(
        {
            "module_code": ["M31", "M31", "M31"],
            "node_id": ["o1-id", "a1-id", "a2-id"],
            "node_code": ["M31O1", "M31O1A1", "M31O1A2"],
            "node_type": ["objective", "activity", "activity"],
            "label": ["Objective 1", "Activity 1", "Activity 2"],
            "objective_code": ["M31O1", "M31O1", "M31O1"],
            "activity_index": [None, 1, 2],
            "init_open": [True, True, False],
            "source_primary": ["catalog", "catalog", "catalog"],
            "source_enrichment": ["rules", "rules", "rules"],
            "is_ghost": [False, False, False],
        }
    )

    agg_activity_daily = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2)],
            "module_code": ["M31", "M31", "M31"],
            "objective_id": ["o1-id", "o1-id", "o1-id"],
            "activity_id": ["a1-id", "a2-id", "a1-id"],
            "attempts": [10, 30, 10],
            "success_rate": [0.8, 0.5, 0.9],
            "repeat_attempt_rate": [0.2, 0.4, 0.1],
        }
    )

    out = attach_overlay_metrics_to_nodes(
        nodes=nodes,
        agg_activity_daily=agg_activity_daily,
        module_code="M31",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )

    activity_1 = out.filter(pl.col("node_id") == "a1-id").to_dicts()[0]
    objective = out.filter(pl.col("node_id") == "o1-id").to_dicts()[0]

    # a1-id: attempts 20, success weighted (0.8*10 + 0.9*10)/20 = 0.85
    assert float(activity_1["overlay_attempts"]) == 20.0
    assert float(activity_1["overlay_success_rate"]) == 0.85

    # objective weighted over all activity rows: attempts 50
    # success = (0.8*10 + 0.5*30 + 0.9*10)/50 = 0.64
    assert float(objective["overlay_attempts"]) == 50.0
    assert abs(float(objective["overlay_success_rate"]) - 0.64) < 1e-9


def test_filter_dependency_graph_by_objectives_keeps_internal_edges_only() -> None:
    nodes = pl.DataFrame(
        {
            "module_code": ["M31", "M31", "M31", "M31"],
            "node_id": ["o1", "a1", "o2", "a2"],
            "node_code": ["M31O1", "M31O1A1", "M31O2", "M31O2A1"],
            "node_type": ["objective", "activity", "objective", "activity"],
            "label": ["O1", "A1", "O2", "A2"],
            "objective_code": ["M31O1", "M31O1", "M31O2", "M31O2"],
            "activity_index": [None, 1, None, 1],
            "init_open": [True, True, False, False],
            "source_primary": ["catalog", "catalog", "catalog", "catalog"],
            "source_enrichment": ["rules", "rules", "rules", "rules"],
            "is_ghost": [False, False, False, False],
        }
    )
    edges = pl.DataFrame(
        {
            "module_code": ["M31", "M31"],
            "edge_id": ["e1", "e2"],
            "edge_type": ["activation", "activation"],
            "from_node_code": ["M31O1A1", "M31O1A1"],
            "to_node_code": ["M31O1", "M31O2"],
            "threshold_type": ["success_rate", "success_rate"],
            "threshold_value": [0.75, 0.75],
            "rule_text": ["r1", "r2"],
            "source_primary": ["rules", "rules"],
            "source_enrichment": ["rules", "rules"],
            "enrich_lvl": [None, None],
            "enrich_sr": [None, None],
        }
    )

    filtered_nodes, filtered_edges = filter_dependency_graph_by_objectives(
        nodes=nodes,
        edges=edges,
        objective_codes=["M31O1"],
    )
    assert set(filtered_nodes["node_code"].to_list()) == {"M31O1", "M31O1A1"}
    assert filtered_edges.height == 1
    assert filtered_edges["edge_id"].to_list() == ["e1"]

from __future__ import annotations

import json

import polars as pl

from visu2.loaders import catalog_to_summary_frames, load_learning_catalog, load_zpdes_rules
from visu2.zpdes_dependencies import (
    build_dependency_tables_from_metadata,
    list_supported_module_codes_from_metadata,
)


def _write_json(path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_catalog_to_summary_frames_from_learning_catalog(tmp_path) -> None:
    catalog_path = tmp_path / "learning_catalog.json"
    payload = {
        "meta": {},
        "id_label_index": {},
        "modules": [
            {
                "id": "m1",
                "code": "M1",
                "title": {"short": "M1", "long": "Module 1"},
                "objectives": [
                    {
                        "id": "o1",
                        "code": "M1O1",
                        "title": {"short": "O1", "long": "Objective 1"},
                        "activities": [
                            {
                                "id": "a1",
                                "code": "M1O1A1",
                                "title": {"short": "A1", "long": "Activity 1"},
                                "exercise_ids": ["e1", "e2"],
                            }
                        ],
                    }
                ],
            }
        ],
        "exercise_to_hierarchy": {
            "e1": {"activity_id": "a1", "objective_id": "o1", "module_id": "m1"},
            "e2": {"activity_id": "a1", "objective_id": "o1", "module_id": "m1"},
        },
        "conflicts": {"coverage": {}},
        "orphans": [],
    }
    _write_json(catalog_path, payload)

    catalog = load_learning_catalog(catalog_path)
    frames = catalog_to_summary_frames(catalog)
    assert frames.modules.height == 1
    assert frames.objectives.height == 1
    assert frames.activities.height == 1
    assert frames.activity_exercises.height == 2
    assert set(frames.exercise_hierarchy["module_id"].drop_nulls().to_list()) == {"m1"}


def test_zpdes_metadata_module_listing_respects_observed_filter(tmp_path) -> None:
    rules_path = tmp_path / "zpdes_rules.json"
    catalog_path = tmp_path / "learning_catalog.json"
    _write_json(
        catalog_path,
        {
            "meta": {},
            "id_label_index": {},
            "modules": [],
            "exercise_to_hierarchy": {},
            "conflicts": {"coverage": {}},
            "orphans": [],
        },
    )
    _write_json(
        rules_path,
        {
            "meta": {},
            "module_rules": [
                {"module_code": "M1", "module_id": "m1", "node_rules": []},
                {"module_code": "M31", "module_id": "m31", "node_rules": []},
            ],
            "map_id_code": {"code_to_id": {}, "id_to_codes": {}},
            "links_to_catalog": {},
            "unresolved_links": {},
        },
    )

    codes = list_supported_module_codes_from_metadata(
        learning_catalog_path=catalog_path,
        zpdes_rules_path=rules_path,
        observed_module_codes={"M31", "M99"},
    )
    assert codes == ["M31"]


def test_dependency_tables_from_metadata_prefers_topology_snapshot(tmp_path) -> None:
    catalog_path = tmp_path / "learning_catalog.json"
    rules_path = tmp_path / "zpdes_rules.json"
    _write_json(
        catalog_path,
        {
            "meta": {},
            "id_label_index": {},
            "modules": [],
            "exercise_to_hierarchy": {},
            "conflicts": {"coverage": {}},
            "orphans": [],
        },
    )
    _write_json(
        rules_path,
        {
            "meta": {},
            "module_rules": [{"module_code": "M1", "module_id": "m1", "node_rules": []}],
            "map_id_code": {"code_to_id": {}, "id_to_codes": {}},
            "links_to_catalog": {},
            "unresolved_links": {},
            "dependency_topology": {
                "M1": {
                    "nodes": [
                        {
                            "module_code": "M1",
                            "node_id": "o1",
                            "node_code": "M1O1",
                            "node_type": "objective",
                            "label": "Objective 1",
                            "objective_code": "M1O1",
                            "activity_index": None,
                            "init_open": True,
                            "source_primary": "xlsx",
                            "source_enrichment": "admath",
                            "is_ghost": False,
                        }
                    ],
                    "edges": [
                        {
                            "module_code": "M1",
                            "edge_id": "e1",
                            "edge_type": "activation",
                            "from_node_code": "M1O1",
                            "to_node_code": "M1O1",
                            "threshold_type": "success_rate",
                            "threshold_value": 0.75,
                            "rule_text": "compat",
                            "source_primary": "xlsx",
                            "source_enrichment": "admath",
                            "enrich_lvl": 1,
                            "enrich_sr": 0.75,
                        }
                    ],
                }
            },
        },
    )

    nodes, edges, warnings = build_dependency_tables_from_metadata(
        module_code="M1",
        learning_catalog_path=catalog_path,
        zpdes_rules_path=rules_path,
    )
    assert warnings == []
    assert nodes.height == 1
    assert edges.height == 1
    assert nodes["node_code"].to_list() == ["M1O1"]
    assert edges["edge_id"].to_list() == ["e1"]


def test_dependency_tables_from_metadata_fallback_rules_parsing(tmp_path) -> None:
    catalog_path = tmp_path / "learning_catalog.json"
    rules_path = tmp_path / "zpdes_rules.json"
    _write_json(
        catalog_path,
        {
            "meta": {},
            "id_label_index": {
                "o1": {"type": "objective", "code": "M1O1", "short_title": "Objective 1", "long_title": None, "sources": []},
                "a1": {"type": "activity", "code": "M1O1A1", "short_title": "Activity 1", "long_title": None, "sources": []},
            },
            "modules": [
                {
                    "id": "m1",
                    "code": "M1",
                    "title": {"short": "Module 1", "long": "Module 1"},
                    "objectives": [
                        {
                            "id": "o1",
                            "code": "M1O1",
                            "title": {"short": "Objective 1", "long": "Objective 1"},
                            "activities": [
                                {
                                    "id": "a1",
                                    "code": "M1O1A1",
                                    "title": {"short": "Activity 1", "long": "Activity 1"},
                                    "exercise_ids": [],
                                }
                            ],
                        }
                    ],
                }
            ],
            "exercise_to_hierarchy": {},
            "conflicts": {"coverage": {}},
            "orphans": [],
        },
    )
    _write_json(
        rules_path,
        {
            "meta": {},
            "module_rules": [
                {
                    "module_code": "M1",
                    "module_id": "m1",
                    "map_id_code": {"M1O1": "o1", "M1O1A1": "a1"},
                    "node_rules": [
                        {
                            "id": "o1",
                            "code": "M1O1",
                            "type": "objective",
                            "rules": {"init_ssb": {"value": [1]}},
                        },
                        {
                            "id": "a1",
                            "code": "M1O1A1",
                            "type": "activity",
                            "rules": {
                                "requirements": [{"a1": {"o1": {"sr": [0.75], "lvl": [2]}}}]
                            },
                        },
                    ],
                }
            ],
            "map_id_code": {
                "code_to_id": {"M1O1": "o1", "M1O1A1": "a1"},
                "id_to_codes": {"o1": ["M1O1"], "a1": ["M1O1A1"]},
            },
            "links_to_catalog": {},
            "unresolved_links": {},
        },
    )

    nodes, edges, _warnings = build_dependency_tables_from_metadata(
        module_code="M1",
        learning_catalog_path=catalog_path,
        zpdes_rules_path=rules_path,
    )
    assert nodes.filter(pl.col("node_code") == "M1O1").height == 1
    assert edges.filter(pl.col("edge_type") == "activation").height == 1

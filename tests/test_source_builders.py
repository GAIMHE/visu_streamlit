"""Source-builder regressions for source-local runtime adapters."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from visu2.source_builders import (
    _build_maureen_catalog_and_raw,
    _build_multi_module_researcher_catalog_and_raw,
    _build_multi_module_researcher_zpdes_rules,
    _build_single_module_researcher_catalog_and_raw,
    _build_single_module_researcher_zpdes_rules,
    _neurips_catalog_payloads,
)


def test_neurips_catalog_payloads_resolve_duplicates_and_keep_unmapped_rows(tmp_path: Path) -> None:
    m1_id = "63e98e5f-94e3-4630-9704-076882d6de38"
    m31_id = "14fe4ca0-8fff-4c4a-bad2-6ef051eee349"
    attempts_path = tmp_path / "maths_data.parquet"
    exercises_path = tmp_path / "maths_exercises_table.csv"
    dependencies_path = tmp_path / "maths_dependencies.json"

    pl.DataFrame(
        {
            "user_id": ["u1", "u1", "u2", "u3", "u4", "u5"],
            "playlist_or_module_id": [m1_id, m1_id, "playlist-1", m1_id, "missing-context", m1_id],
            "exercise_id": [
                "exercise-shared",
                "exercise-shared",
                "exercise-shared",
                "exercise-dependency-only",
                "exercise-unmapped",
                "exercise-table-only",
            ],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 1, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 2, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 3, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 4, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 5, tzinfo=UTC),
            ],
            "data_correct": [True, False, True, True, False, True],
            "work_mode": ["zpdes", "zpdes", "playlist", "zpdes", "zpdes", "zpdes"],
            "data_answer": ["1", "2", "1", "3", "4", "5"],
            "data_duration": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "source": ["am", "am", "am", "am", "am", "am"],
            "attempt_index": [1, 2, 1, 1, 1, 1],
            "session_id": ["s1", "s1", "s2", "s3", "s4", "s5"],
            "created_at_session_time": [
                datetime(2025, 1, 1, 0, 0),
                datetime(2025, 1, 1, 0, 1),
                datetime(2025, 1, 1, 0, 2),
                datetime(2025, 1, 1, 0, 3),
                datetime(2025, 1, 1, 0, 4),
                datetime(2025, 1, 1, 0, 5),
            ],
        }
    ).write_parquet(attempts_path)

    pl.DataFrame(
        {
            "exercise_id": ["exercise-shared", "exercise-shared", "exercise-other", "exercise-table-only"],
            "gameplay_type": ["INPUT", "INPUT", "INPUT", "INPUT"],
            "instruction": ["Shared in M1", "Shared in M31", "Other", "Table only"],
            "question": ["Q1", "Q31", "Q2", "Q3"],
            "feedback": ["{}", "{}", "{}", "{}"],
            "module_id": [m1_id, m31_id, m1_id, m1_id],
            "module_name": ["Numbers", "Problems level 1", "Numbers", "Numbers"],
            "objective_id": ["objective-1", "objective-31", "objective-1", "objective-1"],
            "objective_name": ["Objective 1", "Objective 31", "Objective 1", "Objective 1"],
            "objective_targeted_difficulties": ["Long 1", "Long 31", "Long 1", "Long 1"],
            "activity_id": ["activity-1", "activity-31", "activity-2", "activity-3"],
            "activity_name": ["Activity 1", "Activity 31", "Activity 2", "Activity 3"],
            "source": ["am", "am", "am", "am"],
        }
    ).write_csv(exercises_path)

    dependencies_path.write_text(
        json.dumps(
            {
                "modules": {
                    m1_id: {
                        "objective_ids": ["objective-1"],
                        "objectives": {
                            "objective-1": {
                                "activity_ids": ["activity-1", "activity-2"],
                                "activities": {
                                    "activity-1": {
                                        "exercise_ids": ["exercise-shared"],
                                        "prerequisite_activity_ids": [],
                                        "unlocks_activity_ids": ["activity-2"],
                                    },
                                    "activity-2": {
                                        "exercise_ids": ["exercise-dependency-only"],
                                        "prerequisite_activity_ids": ["activity-1"],
                                        "unlocks_activity_ids": [],
                                    },
                                },
                            }
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    raw_attempts, catalog, rules, exercises_json, warnings = _neurips_catalog_payloads(
        attempts_parquet_path=attempts_path,
        exercises_csv_path=exercises_path,
        dependencies_json_path=dependencies_path,
        source_id="neurips",
    )

    shared_rows = raw_attempts.filter(pl.col("exercise_id") == "exercise-shared")
    assert set(shared_rows["module_id"].to_list()) == {m1_id}
    assert catalog["exercise_to_hierarchy"]["exercise-shared"]["module_id"] == m1_id
    assert catalog["exercise_to_hierarchy"]["exercise-dependency-only"]["activity_id"] == "activity-2"
    assert "exercise-unmapped" in catalog["orphans"]

    unmapped = raw_attempts.filter(pl.col("exercise_id") == "exercise-unmapped").row(0, named=True)
    assert unmapped["module_id"] is None
    assert unmapped["objective_id"] is None
    assert unmapped["activity_id"] is None
    table_only = raw_attempts.filter(pl.col("exercise_id") == "exercise-table-only").row(0, named=True)
    assert table_only["activity_id"] == "activity-3"

    topology = rules["dependency_topology"]["M1"]
    assert {node["node_code"] for node in topology["nodes"]} >= {"M1O1A1", "M1O1A2"}
    assert "M1O1A3" not in {node["node_code"] for node in topology["nodes"]}
    assert any(edge["from_node_code"] == "M1O1A1" and edge["to_node_code"] == "M1O1A2" for edge in topology["edges"])
    assert any(item["id"] == "exercise-dependency-only" for item in exercises_json["exercises"])
    assert any("duplicate exercise" in warning for warning in warnings)
    assert any("without NeurIPS hierarchy" in warning for warning in warnings)


def test_neurips_catalog_payloads_use_embedded_codes_from_unsorted_dependencies(
    tmp_path: Path,
) -> None:
    m1_id = "63e98e5f-94e3-4630-9704-076882d6de38"
    m101_id = "053df3ec-5501-4ad8-9917-a935bcf76740"
    data_dir = tmp_path / "data_neurips"
    data_dir.mkdir()
    attempts_path = data_dir / "maths_data.parquet"
    exercises_path = data_dir / "maths_exercises_table.csv"
    dependencies_path = data_dir / "maths_dependencies.json"

    pl.DataFrame(
        {
            "user_id": ["u-am", "u-am", "u-mia", "u-mia"],
            "playlist_or_module_id": [m1_id, m1_id, m101_id, m101_id],
            "exercise_id": ["am-ex-1", "am-ex-2", "mia-ex-1", "mia-ex-2"],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 1, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 2, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 3, tzinfo=UTC),
            ],
            "data_correct": [True, True, True, True],
            "work_mode": ["zpdes", "zpdes", "zpdes", "zpdes"],
            "data_answer": ["1", "2", "3", "4"],
            "data_duration": [1.0, 2.0, 3.0, 4.0],
            "source": ["am", "am", "mia", "mia"],
            "attempt_index": [1, 1, 1, 1],
            "session_id": ["s-am", "s-am", "s-mia", "s-mia"],
            "created_at_session_time": [
                datetime(2025, 1, 1, 0, 0),
                datetime(2025, 1, 1, 0, 1),
                datetime(2025, 1, 1, 0, 2),
                datetime(2025, 1, 1, 0, 3),
            ],
        }
    ).write_parquet(attempts_path)

    pl.DataFrame(
        {
            "exercise_id": ["am-ex-1", "am-ex-2", "mia-ex-1", "mia-ex-2"],
            "gameplay_type": ["INPUT", "INPUT", "INPUT", "INPUT"],
            "instruction": ["AM 1", "AM 2", "MIA 1", "MIA 2"],
            "question": ["Q1", "Q2", "Q3", "Q4"],
            "feedback": ["{}", "{}", "{}", "{}"],
            "module_id": [m1_id, m1_id, m101_id, m101_id],
            "module_name": ["AM M1", "AM M1", "MIA M101", "MIA M101"],
            "objective_id": ["am-o-1", "am-o-2", "mia-o-1", "mia-o-2"],
            "objective_name": ["AM O1", "AM O2", "MIA O1", "MIA O2"],
            "objective_targeted_difficulties": ["", "", "", ""],
            "activity_id": ["am-a-1", "am-a-2", "mia-a-1", "mia-a-2"],
            "activity_name": ["AM A1", "AM A2", "MIA A1", "MIA A2"],
            "source": ["am", "am", "mia", "mia"],
        }
    ).write_csv(exercises_path)

    dependencies_path.write_text(
        json.dumps(
            {
                "modules": {
                    m1_id: {
                        "code": "M1",
                        "title": {"short": "AM M1 from dependencies", "long": "AM M1 from dependencies"},
                        "objective_ids": ["am-o-2", "am-o-1"],
                        "objectives": {
                            "am-o-2": {
                                "code": "M1O2",
                                "title": {"short": "AM O2 from dependencies", "long": "AM O2 from dependencies"},
                                "activity_ids": ["am-a-2"],
                                "activities": {
                                    "am-a-2": {
                                        "code": "M1O2A1",
                                        "title": {
                                            "short": "AM A2 from dependencies",
                                            "long": "AM A2 from dependencies",
                                        },
                                        "exercise_ids": ["am-ex-2"],
                                        "prerequisite_activity_ids": ["am-a-1"],
                                        "unlocks_activity_ids": [],
                                    }
                                },
                            },
                            "am-o-1": {
                                "code": "M1O1",
                                "title": {"short": "AM O1 from dependencies", "long": "AM O1 from dependencies"},
                                "activity_ids": ["am-a-1"],
                                "activities": {
                                    "am-a-1": {
                                        "code": "M1O1A1",
                                        "title": {
                                            "short": "AM A1 from dependencies",
                                            "long": "AM A1 from dependencies",
                                        },
                                        "exercise_ids": ["am-ex-1"],
                                        "prerequisite_activity_ids": [],
                                        "unlocks_activity_ids": ["am-a-2"],
                                    }
                                },
                            },
                        },
                    },
                    m101_id: {
                        "code": "M101",
                        "title": {"short": "MIA M101 from dependencies", "long": "MIA M101 from dependencies"},
                        "objective_ids": ["mia-o-2", "mia-o-1"],
                        "objectives": {
                            "mia-o-2": {
                                "code": "M101O2",
                                "title": {
                                    "short": "MIA O2 from dependencies",
                                    "long": "MIA O2 from dependencies",
                                },
                                "activity_ids": ["mia-a-2"],
                                "activities": {
                                    "mia-a-2": {
                                        "code": "M101O2A1",
                                        "title": {
                                            "short": "MIA A2 from dependencies",
                                            "long": "MIA A2 from dependencies",
                                        },
                                        "exercise_ids": ["mia-ex-2"],
                                        "prerequisite_activity_ids": ["mia-a-1"],
                                        "unlocks_activity_ids": [],
                                    }
                                },
                            },
                            "mia-o-1": {
                                "code": "M101O1",
                                "title": {
                                    "short": "MIA O1 from dependencies",
                                    "long": "MIA O1 from dependencies",
                                },
                                "activity_ids": ["mia-a-1"],
                                "activities": {
                                    "mia-a-1": {
                                        "code": "M101O1A1",
                                        "title": {
                                            "short": "MIA A1 from dependencies",
                                            "long": "MIA A1 from dependencies",
                                        },
                                        "exercise_ids": ["mia-ex-1"],
                                        "prerequisite_activity_ids": [],
                                        "unlocks_activity_ids": ["mia-a-2"],
                                    }
                                },
                            },
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "learning_catalog.json").write_text(
        json.dumps(
            {
                "modules": [
                    {
                        "id": m1_id,
                        "code": "M1",
                        "objectives": [
                            {
                                "id": "am-o-1",
                                "code": "M1O9",
                                "activities": [{"id": "am-a-1", "code": "M1O9A1"}],
                            },
                            {
                                "id": "am-o-2",
                                "code": "M1O8",
                                "activities": [{"id": "am-a-2", "code": "M1O8A1"}],
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "data" / "zpdes_rules.json").write_text(
        json.dumps(
            {
                "dependency_topology": {
                    "M1": {
                        "nodes": [
                            {
                                "node_id": "am-o-1",
                                "node_code": "M1O1",
                                "node_type": "objective",
                                "label": "AM O1 from rules",
                            },
                            {
                                "node_id": "am-a-1",
                                "node_code": "M1O1A1",
                                "node_type": "activity",
                                "label": "AM A1 from rules",
                            },
                            {
                                "node_id": "am-o-2",
                                "node_code": "M1O2",
                                "node_type": "objective",
                                "label": "AM O2 from rules",
                            },
                            {
                                "node_id": "am-a-2",
                                "node_code": "M1O2A1",
                                "node_type": "activity",
                                "label": "AM A2 from rules",
                            },
                        ],
                        "edges": [],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "data_MIA").mkdir()
    (tmp_path / "data_MIA" / "config_mia.json").write_text(
        json.dumps(
            {
                "config": {
                    "module": {"m101": {"id": m101_id, "code": "M101"}},
                    "objective": {
                        "o1": {"id": "mia-o-1", "code": "M101O1"},
                        "o2": {"id": "mia-o-2", "code": "M101O2"},
                    },
                    "activity": {
                        "a1": {"id": "mia-a-1", "code": "M101O1A1"},
                        "a2": {"id": "mia-a-2", "code": "M101O2A1"},
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    _, learning_catalog, zpdes_rules, _, warnings = _neurips_catalog_payloads(
        attempts_parquet_path=attempts_path,
        exercises_csv_path=exercises_path,
        dependencies_json_path=dependencies_path,
        source_id="neurips",
    )

    modules_by_code = {module["code"]: module for module in learning_catalog["modules"]}
    assert [objective["code"] for objective in modules_by_code["M1"]["objectives"]] == ["M1O1", "M1O2"]
    assert [objective["title"]["short"] for objective in modules_by_code["M1"]["objectives"]] == [
        "AM O1 from dependencies",
        "AM O2 from dependencies",
    ]
    assert [objective["code"] for objective in modules_by_code["M101"]["objectives"]] == ["M101O1", "M101O2"]
    am_edges = {
        (edge["from_node_code"], edge["to_node_code"])
        for edge in zpdes_rules["dependency_topology"]["M1"]["edges"]
    }
    mia_edges = {
        (edge["from_node_code"], edge["to_node_code"])
        for edge in zpdes_rules["dependency_topology"]["M101"]["edges"]
    }
    assert ("M1O1A1", "M1O2A1") in am_edges
    assert ("M101O1A1", "M101O2A1") in mia_edges
    assert warnings == ()


def test_neurips_dependency_edges_resolve_reused_activity_ids_per_module(tmp_path: Path) -> None:
    m31_id = "14fe4ca0-8fff-4c4a-bad2-6ef051eee349"
    m33_id = "27709aa2-b055-4ed3-ac73-8dca783b4afe"
    shared_activity_id = "shared-activity-id"
    m31_target_activity_id = "m31-target-activity-id"
    m33_target_activity_id = "m33-target-activity-id"
    data_dir = tmp_path / "data_neurips"
    data_dir.mkdir()
    attempts_path = data_dir / "maths_data.parquet"
    exercises_path = data_dir / "maths_exercises_table.csv"
    dependencies_path = data_dir / "maths_dependencies.json"

    pl.DataFrame(
        {
            "user_id": ["u-m31", "u-m33"],
            "playlist_or_module_id": [m31_id, m33_id],
            "exercise_id": ["m31-target-ex", "m33-target-ex"],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 9, 1, tzinfo=UTC),
            ],
            "data_correct": [True, True],
            "work_mode": ["zpdes", "zpdes"],
            "data_answer": ["1", "2"],
            "data_duration": [1.0, 2.0],
            "source": ["am", "am"],
            "attempt_index": [1, 1],
            "session_id": ["s-m31", "s-m33"],
            "created_at_session_time": [
                datetime(2025, 1, 1, 0, 0),
                datetime(2025, 1, 1, 0, 1),
            ],
        }
    ).write_parquet(attempts_path)

    pl.DataFrame(
        {
            "exercise_id": ["m31-shared-ex", "m31-target-ex", "m33-shared-ex", "m33-target-ex"],
            "gameplay_type": ["INPUT", "INPUT", "INPUT", "INPUT"],
            "instruction": ["M31 shared", "M31 target", "M33 shared", "M33 target"],
            "question": ["Q1", "Q2", "Q3", "Q4"],
            "feedback": ["{}", "{}", "{}", "{}"],
            "module_id": [m31_id, m31_id, m33_id, m33_id],
            "module_name": ["AM M31", "AM M31", "AM M33", "AM M33"],
            "objective_id": ["m31-o-1", "m31-o-1", "m33-o-1", "m33-o-1"],
            "objective_name": ["M31 O1", "M31 O1", "M33 O1", "M33 O1"],
            "objective_targeted_difficulties": ["", "", "", ""],
            "activity_id": [
                shared_activity_id,
                m31_target_activity_id,
                shared_activity_id,
                m33_target_activity_id,
            ],
            "activity_name": ["Shared", "M31 target", "Shared", "M33 target"],
            "source": ["am", "am", "am", "am"],
        }
    ).write_csv(exercises_path)

    dependencies_path.write_text(
        json.dumps(
            {
                "modules": {
                    m31_id: {
                        "code": "M31",
                        "objective_ids": ["m31-o-1"],
                        "objectives": {
                            "m31-o-1": {
                                "code": "M31O1",
                                "activity_ids": [shared_activity_id, m31_target_activity_id],
                                "activities": {
                                    shared_activity_id: {
                                        "code": "M31O1A1",
                                        "exercise_ids": ["m31-shared-ex"],
                                        "prerequisite_activity_ids": [],
                                        "unlocks_activity_ids": [m31_target_activity_id],
                                    },
                                    m31_target_activity_id: {
                                        "code": "M31O1A2",
                                        "exercise_ids": ["m31-target-ex"],
                                        "prerequisite_activity_ids": [shared_activity_id],
                                        "unlocks_activity_ids": [],
                                    },
                                },
                            }
                        },
                    },
                    m33_id: {
                        "code": "M33",
                        "objective_ids": ["m33-o-1"],
                        "objectives": {
                            "m33-o-1": {
                                "code": "M33O1",
                                "activity_ids": [shared_activity_id, m33_target_activity_id],
                                "activities": {
                                    shared_activity_id: {
                                        "code": "M33O1A1",
                                        "exercise_ids": ["m33-shared-ex"],
                                        "prerequisite_activity_ids": [],
                                        "unlocks_activity_ids": [m33_target_activity_id],
                                    },
                                    m33_target_activity_id: {
                                        "code": "M33O1A2",
                                        "exercise_ids": ["m33-target-ex"],
                                        "prerequisite_activity_ids": [shared_activity_id],
                                        "unlocks_activity_ids": [],
                                    },
                                },
                            }
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "learning_catalog.json").write_text(
        json.dumps({"modules": [{"id": m31_id, "code": "M31"}, {"id": m33_id, "code": "M33"}]}),
        encoding="utf-8",
    )
    (tmp_path / "data" / "zpdes_rules.json").write_text(
        json.dumps(
            {
                "dependency_topology": {
                    "M31": {
                        "nodes": [
                            {"node_id": "m31-o-1", "node_code": "M31O1", "node_type": "objective"},
                            {"node_id": shared_activity_id, "node_code": "M31O1A1", "node_type": "activity"},
                            {
                                "node_id": m31_target_activity_id,
                                "node_code": "M31O1A2",
                                "node_type": "activity",
                            },
                        ],
                        "edges": [],
                    },
                    "M33": {
                        "nodes": [
                            {"node_id": "m33-o-1", "node_code": "M33O1", "node_type": "objective"},
                            {"node_id": shared_activity_id, "node_code": "M33O1A1", "node_type": "activity"},
                            {
                                "node_id": m33_target_activity_id,
                                "node_code": "M33O1A2",
                                "node_type": "activity",
                            },
                        ],
                        "edges": [],
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    _, _, zpdes_rules, _, warnings = _neurips_catalog_payloads(
        attempts_parquet_path=attempts_path,
        exercises_csv_path=exercises_path,
        dependencies_json_path=dependencies_path,
        source_id="neurips",
    )

    m31_edges = {
        (edge["from_node_code"], edge["to_node_code"])
        for edge in zpdes_rules["dependency_topology"]["M31"]["edges"]
    }
    m33_edges = {
        (edge["from_node_code"], edge["to_node_code"])
        for edge in zpdes_rules["dependency_topology"]["M33"]["edges"]
    }
    assert ("M31O1A1", "M31O1A2") in m31_edges
    assert ("M33O1A1", "M33O1A2") in m33_edges
    assert all(from_code.startswith("M31") and to_code.startswith("M31") for from_code, to_code in m31_edges)
    assert all(from_code.startswith("M33") and to_code.startswith("M33") for from_code, to_code in m33_edges)
    assert warnings == ()


def test_build_maureen_catalog_and_raw_uses_researcher_csv_and_preserves_classrooms(tmp_path: Path) -> None:
    config_path = tmp_path / "module_config.csv"
    config_path.write_text(
        "\n".join(
            [
                "type;shell;id;short_title;long_title;group_title;description;description_student;targeted_difficulties;status;available_in_specimen;code;prerequisites;init_open;stay_consec_min;nbMinStep;nbMaxComputeThreshold;activating_Threshold;deact_Threshold;deact_prerequisites;window_progression;prom_coeff;student_path;student_path_alt;mapping_nodes;states_definition;learning_items",
                "module;witch;module-1;Module short;Module long;;;;;visible;FAUX;M16;;;;;;;;;;;;;;;",
                "objective;witch;objective-1;Objective short;Objective long;;;;;visible;FAUX;M16O1;;Y;;;;;;;;;;;;;;;;",
                "activity;witch;activity-1;Activity short;Activity long;;;;;visible;FAUX;M16O1A1;;;;;;;;;;;;;;;11111111-1111-1111-1111-111111111111",
            ]
        ),
        encoding="utf-8",
    )
    attempts_path = tmp_path / "attempts.csv"
    attempts_path.write_text(
        "\n".join(
            [
                "UAI,classroom_id,teacher_id,user_id,playlist_or_module_id,objective_id,activity_id,exercise_id,module_short_title,module_long_title,created_at,login_time,is_initial_test,data_score,data_correct,data_nb_tries,data_test_context,data_answer,data_duration,session_duration,work_mode;",
                "0880021V,class-1,teacher-1,user-1,module-1,objective-1,activity-1,11111111-1111-1111-1111-111111111111,Comprendre,Module 16,2024-11-06 09:29:04.146000+00:00,2024-11-06 09:16:03.548000+00:00,False,1.0,True,1,zpdes,[1],19469.0,1078,zpdes;",
                "\"0880021V,class-1,teacher-1,user-1,module-1,objective-1,activity-extra,22222222-2222-2222-2222-222222222222,Comprendre,Module 16,2024-11-06 09:23:14.444000+00:00,2024-11-06 09:16:03.548000+00:00,False,0.0,False,1,adaptive-test,\"\"{'groups': [[{'color': '#016680', 'label': 'Groupe 1', 'selection': [{'end': 2, 'start': 0}], 'nbMinAnswers': None, 'correctAnswer': [[{'end': 4, 'start': 0, 'optional': False}]]}]]}\"\",45812.0,1078,adaptive-test;\"",
                "\"0880021V,class-1,teacher-1,user-1,module-1,objective-1,activity-extra,22222222-2222-2222-2222-222222222222,Comprendre,Module 16,2024-11-06 09:24:14.444000+00:00,2024-11-06 09:16:03.548000+00:00,False,1.0,True,2,adaptive-test,\"\"{'groups': [[{'color': '#016680', 'label': 'Groupe 1', 'selection': [{'end': 2, 'start': 0}], 'nbMinAnswers': None, 'correctAnswer': [[{'end': 4, 'start': 0, 'optional': False}]]}]]}\"\",45812.0,1078,adaptive-test;\"",
            ]
        ),
        encoding="utf-8",
    )

    raw_attempts, learning_catalog, zpdes_rules, exercises_json, warnings = _build_maureen_catalog_and_raw(
        attempts_path,
        config_path,
    )

    assert raw_attempts.height == 3
    assert set(raw_attempts["work_mode"].to_list()) == {"zpdes", "adaptive-test"}
    assert set(raw_attempts["classroom_id"].to_list()) == {"class-1"}
    assert set(raw_attempts["teacher_id"].to_list()) == {"teacher-1"}
    assert raw_attempts["module_id"].to_list() == ["module-1", "module-1", "module-1"]
    extra_rows = raw_attempts.filter(raw_attempts["activity_id"] == "activity-extra")
    assert extra_rows["attempt_number"].to_list() == [1, 2]
    assert raw_attempts["student_attempt_index"].to_list() == [1, 2, 3]
    assert any("Repaired 2 malformed row(s)" in warning for warning in warnings)

    module = learning_catalog["modules"][0]
    objective = module["objectives"][0]
    assert len(objective["activities"]) == 2
    synthetic_activity = [row for row in objective["activities"] if row["id"] == "activity-extra"][0]
    assert synthetic_activity["code"] == "M16O1A2"
    assert any("Added synthetic activity" in warning for warning in warnings)

    assert learning_catalog["exercise_to_hierarchy"]["22222222-2222-2222-2222-222222222222"]["activity_id"] == "activity-extra"
    assert zpdes_rules["map_id_code"]["code_to_id"]["M16O1A2"] == "activity-extra"
    exercise_ids = [row["id"] for row in exercises_json["exercises"]]
    assert "22222222-2222-2222-2222-222222222222" in exercise_ids


def test_build_single_module_researcher_catalog_and_raw_synthesizes_catalog(tmp_path: Path) -> None:
    attempts_path = tmp_path / "mia_attempts.csv"
    attempts_path.write_text(
        "\n".join(
            [
                "UAI,classroom_id,teacher_id,user_id,playlist_or_module_id,objective_id,activity_id,exercise_id,module_short_title,module_long_title,created_at,login_time,is_initial_test,data_score,data_correct,data_nb_tries,data_test_context,data_answer,data_duration,session_duration,work_mode;",
                "001,class-1,teacher-1,user-1,module-1,objective-1,activity-1,11111111-1111-1111-1111-111111111111,Sens des nombres,Module 1,2025-01-01 09:00:00+00:00,2025-01-01 08:59:00+00:00,False,1.0,True,1,zpdes,[1],10.0,100,zpdes;",
                "001,class-1,teacher-1,user-1,module-1,objective-1,activity-1,11111111-1111-1111-1111-111111111111,Sens des nombres,Module 1,2025-01-01 09:01:00+00:00,2025-01-01 08:59:00+00:00,False,1.0,True,2,zpdes,[1],11.0,100,zpdes;",
                "001,class-1,teacher-2,user-2,module-1,objective-1,activity-2,22222222-2222-2222-2222-222222222222,Sens des nombres,Module 1,2025-01-02 09:00:00+00:00,2025-01-02 08:59:00+00:00,False,0.0,False,1,adaptive-test,[0],12.0,200,adaptive-test;",
                "001,class-2,teacher-2,user-2,module-1,objective-2,activity-3,33333333-3333-3333-3333-333333333333,Sens des nombres,Module 1,2025-01-03 09:00:00+00:00,2025-01-03 08:59:00+00:00,False,1.0,True,1,revision,[1],13.0,300,revision;",
            ]
        ),
        encoding="utf-8",
    )

    raw_attempts, learning_catalog, exercises_json, warnings = (
        _build_single_module_researcher_catalog_and_raw(
            attempts_path,
            source_id="mia_module1",
        )
    )

    assert raw_attempts.height == 4
    assert set(raw_attempts["work_mode"].to_list()) == {"zpdes", "adaptive-test", "revision"}
    assert raw_attempts["module_id"].to_list() == ["module-1", "module-1", "module-1", "module-1"]
    repeated = raw_attempts.filter(raw_attempts["exercise_id"] == "11111111-1111-1111-1111-111111111111")
    assert repeated["attempt_number"].to_list() == [1, 2]
    assert raw_attempts["student_attempt_index"].to_list() == [1, 2, 1, 2]
    assert warnings == ()

    module = learning_catalog["modules"][0]
    assert module["code"] == "M1"
    assert module["title"]["short"] == "Sens des nombres"
    assert len(module["objectives"]) == 2
    assert module["objectives"][0]["code"] == "M1O01"
    assert module["objectives"][0]["title"]["short"] == "Objective 01"
    assert module["objectives"][0]["activities"][0]["code"] == "M1O01A01"
    assert module["objectives"][0]["activities"][1]["code"] == "M1O01A02"
    assert module["objectives"][1]["activities"][0]["code"] == "M1O02A01"
    assert learning_catalog["exercise_to_hierarchy"]["33333333-3333-3333-3333-333333333333"] == {
        "module_id": "module-1",
        "objective_id": "objective-2",
        "activity_id": "activity-3",
    }

    exercise_ids = [row["id"] for row in exercises_json["exercises"]]
    assert exercise_ids == [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        "33333333-3333-3333-3333-333333333333",
    ]


def test_build_single_module_researcher_catalog_and_raw_prefers_config_labels(tmp_path: Path) -> None:
    attempts_path = tmp_path / "mia_attempts.csv"
    attempts_path.write_text(
        "\n".join(
            [
                "UAI,classroom_id,teacher_id,user_id,playlist_or_module_id,objective_id,activity_id,exercise_id,module_short_title,module_long_title,created_at,login_time,is_initial_test,data_score,data_correct,data_nb_tries,data_test_context,data_answer,data_duration,session_duration,work_mode;",
                "001,class-1,teacher-1,user-1,module-101,objective-1,activity-1,11111111-1111-1111-1111-111111111111,Sens des nombres,Module 1,2025-01-01 09:00:00+00:00,2025-01-01 08:59:00+00:00,False,1.0,True,1,zpdes,[1],10.0,100,zpdes;",
                "001,class-1,teacher-2,user-2,module-101,objective-2,activity-2,22222222-2222-2222-2222-222222222222,Sens des nombres,Module 1,2025-01-02 09:00:00+00:00,2025-01-02 08:59:00+00:00,False,0.0,False,1,adaptive-test,[0],12.0,200,adaptive-test;",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config_mia.json"
    config_path.write_text(
        json.dumps(
            {
                "config": {
                    "module": {
                        "101": {
                            "id": "module-101",
                            "code": "M101",
                            "title": {
                                "short": "Réapprentissage du sens des nombres",
                                "long": "Module 1",
                            },
                            "visibilityStatus": "VISIBLE",
                        }
                    },
                    "objective": {
                        "obj-1": {
                            "id": "objective-1",
                            "code": "M101O1",
                            "title": {
                                "short": "Positionner des nombres entiers",
                                "long": "Objectif 1",
                            },
                            "visibilityStatus": "VISIBLE",
                        },
                        "obj-2": {
                            "id": "objective-2",
                            "code": "M101O2",
                            "title": {
                                "short": "Comparer des nombres entiers",
                                "long": "Objectif 2",
                            },
                            "visibilityStatus": "VISIBLE",
                        },
                    },
                    "activity": {
                        "act-1": {
                            "id": "activity-1",
                            "code": "M101O1A1",
                            "title": {
                                "short": "Positionner sur un segment",
                                "long": "Activité 1",
                            },
                            "visibilityStatus": "VISIBLE",
                        },
                        "act-2": {
                            "id": "activity-2",
                            "code": "M101O2A1",
                            "title": {
                                "short": "Comparer des nombres",
                                "long": "Activité 1",
                            },
                            "visibilityStatus": "VISIBLE",
                        },
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    raw_attempts, learning_catalog, exercises_json, warnings = (
        _build_single_module_researcher_catalog_and_raw(
            attempts_path,
            source_id="mia_module1",
            config_json_path=config_path,
        )
    )

    assert raw_attempts.height == 2
    assert any("using the config title" in warning for warning in warnings)
    module = learning_catalog["modules"][0]
    assert module["code"] == "M101"
    assert module["title"]["short"] == "Réapprentissage du sens des nombres"
    assert module["objectives"][0]["code"] == "M101O1"
    assert module["objectives"][0]["title"]["short"] == "Positionner des nombres entiers"
    assert module["objectives"][0]["activities"][0]["code"] == "M101O1A1"
    assert module["objectives"][0]["activities"][0]["title"]["short"] == "Positionner sur un segment"
    assert module["objectives"][1]["code"] == "M101O2"
    assert module["objectives"][1]["activities"][0]["code"] == "M101O2A1"
    assert learning_catalog["id_label_index"]["objective-1"]["short_title"] == "Positionner des nombres entiers"
    assert exercises_json["exercises"][0]["objectives"] == ["objective-1"]


def test_build_single_module_researcher_zpdes_rules_uses_module_config_graph(tmp_path: Path) -> None:
    learning_catalog = {
        "meta": {},
        "conflicts": {},
        "orphans": [],
        "exercise_to_hierarchy": {},
        "id_label_index": {
            "module-101": {"type": "module", "code": "M101", "short_title": "Module M101", "long_title": "Module M101", "sources": []},
            "objective-1": {"type": "objective", "code": "M101O1", "short_title": "Objective 1", "long_title": "Objective 1", "sources": []},
            "objective-2": {"type": "objective", "code": "M101O2", "short_title": "Objective 2", "long_title": "Objective 2", "sources": []},
            "activity-1": {"type": "activity", "code": "M101O1A1", "short_title": "A1", "long_title": "A1", "sources": []},
            "activity-2": {"type": "activity", "code": "M101O1A2", "short_title": "A2", "long_title": "A2", "sources": []},
            "activity-3": {"type": "activity", "code": "M101O2A1", "short_title": "B1", "long_title": "B1", "sources": []},
        },
        "modules": [
            {
                "id": "module-101",
                "code": "M101",
                "title": {"short": "Module M101", "long": "Module M101"},
                "objectives": [
                    {
                        "id": "objective-1",
                        "code": "M101O1",
                        "title": {"short": "Objective 1", "long": "Objective 1"},
                        "activities": [
                            {
                                "id": "activity-1",
                                "code": "M101O1A1",
                                "title": {"short": "A1", "long": "A1"},
                                "exercise_ids": [],
                            },
                            {
                                "id": "activity-2",
                                "code": "M101O1A2",
                                "title": {"short": "A2", "long": "A2"},
                                "exercise_ids": [],
                            },
                        ],
                    },
                    {
                        "id": "objective-2",
                        "code": "M101O2",
                        "title": {"short": "Objective 2", "long": "Objective 2"},
                        "activities": [
                            {
                                "id": "activity-3",
                                "code": "M101O2A1",
                                "title": {"short": "B1", "long": "B1"},
                                "exercise_ids": [],
                            }
                        ],
                    },
                ],
            }
        ],
    }
    config_path = tmp_path / "config_mia.json"
    config_path.write_text(
        json.dumps(
            {
                "config": {
                    "module": {},
                    "objective": {},
                    "activity": {},
                    "ai": {
                        "moduleConfig": {
                            "module-101": {
                                "module-101": {
                                    "subgroups": [["objective-1", "objective-2"]],
                                    "init_ssb": [[0]],
                                    "requirements": [
                                        {
                                            "objective-2": {
                                                "objective-1": {"sr": [0.75], "lvl": [1]}
                                            }
                                        }
                                    ],
                                },
                                "objective-1": {
                                    "subgroups": [["activity-1", "activity-2"]],
                                    "init_ssb": [[0]],
                                    "requirements": [
                                        {
                                            "activity-2": {
                                                "objective-1": {"sr": [0.75], "lvl": [0]}
                                            }
                                        }
                                    ],
                                },
                                "objective-2": {
                                    "subgroups": [["activity-3"]],
                                    "init_ssb": [[0]],
                                    "requirements": [{}],
                                },
                            }
                        }
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    zpdes_rules, warnings = _build_single_module_researcher_zpdes_rules(
        source_id="mia_module1",
        module_id="module-101",
        learning_catalog=learning_catalog,
        config_json_path=config_path,
    )

    assert warnings == ()
    assert zpdes_rules["module_rules"][0]["module_code"] == "M101"
    topology = zpdes_rules["dependency_topology"]["M101"]
    node_codes = {row["node_code"] for row in topology["nodes"]}
    assert {"M101O1", "M101O1A1", "M101O1A2", "M101O2", "M101O2A1"} <= node_codes
    init_open = {row["node_code"] for row in topology["nodes"] if row["init_open"]}
    assert "M101O1" in init_open
    assert "M101O1A1" in init_open
    edges = {(row["from_node_code"], row["to_node_code"]) for row in topology["edges"]}
    assert ("M101O1A1", "M101O1A2") in edges
    assert ("M101O1A2", "M101O2") in edges


def test_build_multi_module_researcher_catalog_and_rules_supports_playlist_rows(tmp_path: Path) -> None:
    attempts_path = tmp_path / "mia_multi_attempts.csv"
    attempts_path.write_text(
        "\n".join(
            [
                "UAI,classroom_id,teacher_id,user_id,playlist_or_module_id,objective_id,activity_id,exercise_id,module_short_title,module_long_title,created_at,login_time,is_initial_test,data_score,data_correct,data_nb_tries,data_test_context,data_answer,data_duration,session_duration,work_mode,progression_score,initial_test_max_success,initial_test_weighted_max_success,initial_test_success_rate,finished_module_mean_score,finished_module_graphe_coverage_rate",
                "001,class-1,teacher-1,user-1,module-1,objective-1,activity-1,11111111-1111-1111-1111-111111111111,Module 1,Module 1,2025-01-01 09:00:00+00:00,2025-01-01 08:59:00+00:00,False,1.0,True,1,zpdes,[1],10.0,100,zpdes,0.5,10,9,0.9,0.8,1.0",
                "001,class-1,teacher-1,user-1,playlist-xyz,,,22222222-2222-2222-2222-222222222222,Module 2,Module 2,2025-01-01 09:02:00+00:00,2025-01-01 08:59:00+00:00,False,0.0,False,1,playlist,[0],11.0,100,playlist,0.5,10,9,0.9,0.8,1.0",
                "001,class-1,teacher-2,user-2,module-2,objective-2,activity-2,22222222-2222-2222-2222-222222222222,Module 2,Module 2,2025-01-02 09:00:00+00:00,2025-01-02 08:59:00+00:00,False,0.0,False,1,adaptive-test,[0],12.0,200,adaptive-test,0.2,12,10,0.8,0.6,0.7",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config_mia.json"
    config_path.write_text(
        json.dumps(
            {
                "config": {
                    "module": {
                        "m1": {
                            "id": "module-1",
                            "code": "M1",
                            "title": {"short": "Module 1", "long": "Module 1"},
                            "visibilityStatus": "VISIBLE",
                        },
                        "m2": {
                            "id": "module-2",
                            "code": "M2",
                            "title": {"short": "Module 2", "long": "Module 2"},
                            "visibilityStatus": "VISIBLE",
                        },
                    },
                    "objective": {
                        "o1": {
                            "id": "objective-1",
                            "code": "M1O1",
                            "title": {"short": "Objective 1", "long": "Objective 1"},
                            "visibilityStatus": "VISIBLE",
                        },
                        "o2": {
                            "id": "objective-2",
                            "code": "M2O1",
                            "title": {"short": "Objective 2", "long": "Objective 2"},
                            "visibilityStatus": "VISIBLE",
                        },
                    },
                    "activity": {
                        "a1": {
                            "id": "activity-1",
                            "code": "M1O1A1",
                            "title": {"short": "Activity 1", "long": "Activity 1"},
                            "visibilityStatus": "VISIBLE",
                            "learning_items": ["11111111-1111-1111-1111-111111111111"],
                        },
                        "a2": {
                            "id": "activity-2",
                            "code": "M2O1A1",
                            "title": {"short": "Activity 2", "long": "Activity 2"},
                            "visibilityStatus": "VISIBLE",
                            "learning_items": ["22222222-2222-2222-2222-222222222222"],
                        },
                    },
                    "ai": {
                        "moduleConfig": {
                            "module-1": {
                                "module-1": {"subgroups": [["objective-1"]], "init_ssb": [[0]], "requirements": [{}]},
                                "objective-1": {"subgroups": [["activity-1"]], "init_ssb": [[0]], "requirements": [{}]},
                            },
                            "module-2": {
                                "module-2": {"subgroups": [["objective-2"]], "init_ssb": [[0]], "requirements": [{}]},
                                "objective-2": {"subgroups": [["activity-2"]], "init_ssb": [[0]], "requirements": [{}]},
                            },
                        }
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    raw_attempts, learning_catalog, exercises_json, warnings = _build_multi_module_researcher_catalog_and_raw(
        attempts_path,
        source_id="mia_module1",
        config_json_path=config_path,
    )

    assert raw_attempts.height == 3
    assert set(raw_attempts["module_id"].drop_nulls().to_list()) == {"module-1", "module-2"}
    playlist_row = raw_attempts.filter(raw_attempts["work_mode"] == "playlist").row(0, named=True)
    assert playlist_row["module_id"] == "module-2"
    assert playlist_row["objective_id"] == "objective-2"
    assert playlist_row["activity_id"] == "activity-2"
    assert playlist_row["progression_score"] == 0.5
    assert warnings == ()

    module_codes = {module["code"] for module in learning_catalog["modules"]}
    assert module_codes == {"M1", "M2"}
    assert exercises_json["exercises"][1]["modules"] == ["module-2"]

    zpdes_rules, zpdes_warnings = _build_multi_module_researcher_zpdes_rules(
        source_id="mia_module1",
        learning_catalog=learning_catalog,
        config_json_path=config_path,
    )

    assert zpdes_warnings == ()
    assert {row["module_code"] for row in zpdes_rules["module_rules"]} == {"M1", "M2"}
    assert set(zpdes_rules["dependency_topology"]) == {"M1", "M2"}

"""Source-builder regressions for source-local runtime adapters."""

from __future__ import annotations

import json
from pathlib import Path

from visu2.source_builders import (
    _build_maureen_catalog_and_raw,
    _build_multi_module_researcher_catalog_and_raw,
    _build_multi_module_researcher_zpdes_rules,
    _build_single_module_researcher_catalog_and_raw,
    _build_single_module_researcher_zpdes_rules,
)


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

"""Validate the IRT feasibility report helpers and output build."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl

from visu2.config import Settings
from visu2.irt_feasibility import (
    _articulation_points,
    _classify_topology,
    _early_late_delta_summary,
    _two_core_nodes,
    build_irt_feasibility_report,
)


def _build_settings(tmp_path: Path) -> Settings:
    """Build a temporary settings object with minimal metadata files."""
    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    derived_dir = artifacts_dir / "derived"
    reports_dir = artifacts_dir / "reports"
    resources_dir = tmp_path / "ressources"
    for path in (data_dir, derived_dir, reports_dir, resources_dir):
        path.mkdir(parents=True, exist_ok=True)

    catalog = {
        "meta": {},
        "id_label_index": {},
        "conflicts": {},
        "orphans": [],
        "exercise_to_hierarchy": {
            "e1": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"},
            "e2": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"},
            "e3": {"module_id": "m1", "objective_id": "o1", "activity_id": "a2"},
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
                                "exercise_ids": ["e1", "e2"],
                            },
                            {
                                "id": "a2",
                                "code": "M1O1A2",
                                "title": {"short": "Activity 2", "long": "Activity 2"},
                                "exercise_ids": ["e3"],
                            },
                        ],
                    }
                ],
            }
        ],
    }
    rules = {
        "meta": {},
        "module_rules": [],
        "map_id_code": {"code_to_id": {}, "id_to_codes": {}},
        "links_to_catalog": {},
        "unresolved_links": {},
        "dependency_topology": {
            "M1": {
                "nodes": [
                    {"node_code": "M1O1A1"},
                    {"node_code": "M1O1"},
                    {"node_code": "M1O1A2"},
                ],
                "edges": [
                    {"from_node_code": "M1O1A1", "to_node_code": "M1O1"},
                    {"from_node_code": "M1O1", "to_node_code": "M1O1A2"},
                ],
            }
        },
    }
    exercises = {
        "exercises": [
            {"id": "e1", "type": "MCQ", "instruction": {"$html": "Q1"}},
            {"id": "e2", "type": "MCQ", "instruction": {"$html": "Q2"}},
            {"id": "e3", "type": "INPUT", "instruction": {"$html": "Q3"}},
        ]
    }

    (data_dir / "learning_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    (data_dir / "zpdes_rules.json").write_text(json.dumps(rules), encoding="utf-8")
    (data_dir / "exercises.json").write_text(json.dumps(exercises), encoding="utf-8")

    return Settings(
        root_dir=tmp_path,
        data_dir=data_dir,
        resources_dir=resources_dir,
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=data_dir / "adaptiv_math_history.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def test_classify_topology_distinguishes_linear_and_disconnected() -> None:
    """The topology classifier should separate linear and disconnected DAGs."""
    linear = _classify_topology(
        {
            "nodes": [{"node_code": "A"}, {"node_code": "B"}, {"node_code": "C"}],
            "edges": [
                {"from_node_code": "A", "to_node_code": "B"},
                {"from_node_code": "B", "to_node_code": "C"},
            ],
        }
    )
    disconnected = _classify_topology(
        {
            "nodes": [{"node_code": "A"}, {"node_code": "B"}, {"node_code": "C"}],
            "edges": [{"from_node_code": "A", "to_node_code": "B"}],
        }
    )

    assert linear["structure"] == "linear"
    assert disconnected["structure"] == "disconnected_dag"


def test_projected_graph_helpers_find_articulations_and_two_core() -> None:
    """Projected-graph helpers should expose articulation and 2-core behavior."""
    adjacency = {
        "e1": {"e2"},
        "e2": {"e1", "e3", "e4"},
        "e3": {"e2"},
        "e4": {"e2", "e5"},
        "e5": {"e4"},
    }
    exercises = sorted(adjacency)

    assert _articulation_points(exercises, adjacency) == {"e2", "e4"}
    assert _two_core_nodes(exercises, adjacency) == set()


def test_early_late_delta_summary_detects_improvement() -> None:
    """Early/late delta should be positive when late outcomes improve."""
    attempts = pl.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u1", "u1"],
            "exercise_id": ["e1", "e2", "e3", "e4", "e5"],
            "created_at": [datetime(2025, 1, 1, 9, 0, 0)] * 5,
            "data_correct": [False, False, True, True, True],
            "attempt_number": [1, 1, 1, 1, 1],
            "student_attempt_index": [1, 2, 3, 4, 5],
            "work_mode": ["zpdes"] * 5,
        }
    )

    summary = _early_late_delta_summary(attempts)

    assert float(summary["mean_delta"]) > 0
    assert float(summary["median_delta"]) > 0


def test_build_irt_feasibility_report_writes_expected_outputs(tmp_path: Path) -> None:
    """The report builder should emit Markdown, JSON, and both CSV appendices."""
    settings = _build_settings(tmp_path)
    fact = pl.DataFrame(
        [
            {
                "user_id": "u1",
                "exercise_id": "e1",
                "work_mode": "zpdes",
                "created_at": datetime(2025, 1, 1, 9, 0, 0),
                "data_correct": True,
                "attempt_number": 1,
                "student_attempt_index": 1,
            },
            {
                "user_id": "u1",
                "exercise_id": "e2",
                "work_mode": "zpdes",
                "created_at": datetime(2025, 1, 1, 9, 5, 0),
                "data_correct": False,
                "attempt_number": 1,
                "student_attempt_index": 2,
            },
            {
                "user_id": "u1",
                "exercise_id": "e2",
                "work_mode": "zpdes",
                "created_at": datetime(2025, 1, 1, 9, 6, 0),
                "data_correct": True,
                "attempt_number": 2,
                "student_attempt_index": 3,
            },
            {
                "user_id": "u1",
                "exercise_id": "e3",
                "work_mode": "zpdes",
                "created_at": datetime(2025, 1, 1, 9, 10, 0),
                "data_correct": True,
                "attempt_number": 1,
                "student_attempt_index": 4,
            },
            {
                "user_id": "u2",
                "exercise_id": "e1",
                "work_mode": "zpdes",
                "created_at": datetime(2025, 1, 2, 10, 0, 0),
                "data_correct": False,
                "attempt_number": 1,
                "student_attempt_index": 1,
            },
            {
                "user_id": "u2",
                "exercise_id": "e3",
                "work_mode": "zpdes",
                "created_at": datetime(2025, 1, 2, 10, 5, 0),
                "data_correct": True,
                "attempt_number": 1,
                "student_attempt_index": 2,
            },
            {
                "user_id": "u3",
                "exercise_id": "e1",
                "work_mode": "initial-test",
                "created_at": datetime(2025, 1, 3, 11, 0, 0),
                "data_correct": True,
                "attempt_number": 1,
                "student_attempt_index": 1,
            },
        ]
    )
    fact.write_parquet(settings.parquet_path)

    outputs = build_irt_feasibility_report(settings)

    assert outputs.markdown_path.exists()
    assert outputs.summary_path.exists()
    assert outputs.exercise_sparsity_path.exists()
    assert outputs.overlap_tails_path.exists()

    markdown = outputs.markdown_path.read_text(encoding="utf-8")
    summary = json.loads(outputs.summary_path.read_text(encoding="utf-8"))
    sparsity = pl.read_csv(outputs.exercise_sparsity_path)
    overlap_tails = pl.read_csv(outputs.overlap_tails_path)

    assert "# IRT Feasibility Report" in markdown
    assert "## 3. Overlap / Identifiability" in markdown
    assert summary["dataset_scale"]["students"] == 3
    assert summary["dataset_scale"]["total_attempts"] == 7
    assert "first_exposure_count" in sparsity.columns
    assert "neighbor_exercise_count" in overlap_tails.columns

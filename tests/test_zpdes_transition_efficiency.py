"""Validate the static ZPDES transition-efficiency helper module."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from visu2.config import Settings
from visu2.derive import build_zpdes_exercise_progression_events_from_fact
from visu2.zpdes_transition_efficiency import (
    attach_progression_cohort_metrics_to_nodes,
    attach_transition_metric_to_nodes,
    build_transition_efficiency_figure,
)


def _build_settings(tmp_path: Path) -> Settings:
    """Return a minimal settings fixture with one ranked module catalog."""
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
        "exercise_to_hierarchy": {},
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
                                "title": {"short": "A1", "long": "Activity 1"},
                                "exercise_ids": [],
                            },
                            {
                                "id": "a2",
                                "code": "M1O1A2",
                                "title": {"short": "A2", "long": "Activity 2"},
                                "exercise_ids": [],
                            },
                        ],
                    },
                    {
                        "id": "o2",
                        "code": "M1O2",
                        "title": {"short": "Objective 2", "long": "Objective 2"},
                        "activities": [
                            {
                                "id": "a3",
                                "code": "M1O2A1",
                                "title": {"short": "A3", "long": "Activity 3"},
                                "exercise_ids": [],
                            }
                        ],
                    },
                    {
                        "id": "o3",
                        "code": "M1O3",
                        "title": {"short": "Objective 3", "long": "Objective 3"},
                        "activities": [
                            {
                                "id": "a4",
                                "code": "M1O3A1",
                                "title": {"short": "A4", "long": "Activity 4"},
                                "exercise_ids": [],
                            }
                        ],
                    },
                ],
            }
        ],
    }
    (data_dir / "learning_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    (data_dir / "zpdes_rules.json").write_text(json.dumps({}), encoding="utf-8")
    (data_dir / "exercises.json").write_text(json.dumps({"exercises": []}), encoding="utf-8")

    return Settings(
        root_dir=tmp_path,
        data_dir=data_dir,
        resources_dir=resources_dir,
        artifacts_dir=artifacts_dir,
        artifacts_derived_dir=derived_dir,
        artifacts_reports_dir=reports_dir,
        parquet_path=data_dir / "student_interaction.parquet",
        learning_catalog_path=data_dir / "learning_catalog.json",
        zpdes_rules_path=data_dir / "zpdes_rules.json",
        exercises_json_path=data_dir / "exercises.json",
        consistency_report_path=reports_dir / "consistency_report.json",
        derived_manifest_path=reports_dir / "derived_manifest.json",
    )


def _nodes() -> pl.DataFrame:
    """Return a small dependency-node fixture for transition-efficiency tests."""
    return pl.DataFrame(
        {
            "module_code": ["M1"] * 7,
            "node_id": ["o1", "a1", "a2", "o2", "a3", "o3", "a4"],
            "node_code": ["M1O1", "M1O1A1", "M1O1A2", "M1O2", "M1O2A1", "M1O3", "M1O3A1"],
            "node_type": ["objective", "activity", "activity", "objective", "activity", "objective", "activity"],
            "label": ["Objective 1", "A1", "A2", "Objective 2", "A3", "Objective 3", "A4"],
            "objective_code": ["M1O1", "M1O1", "M1O1", "M1O2", "M1O2", "M1O3", "M1O3"],
            "activity_index": [None, 1, 2, None, 1, None, 1],
            "init_open": [True, True, False, False, False, False, False],
            "source_primary": ["catalog"] * 7,
            "source_enrichment": ["rules"] * 7,
            "is_ghost": [False] * 7,
        }
    )


def _edges() -> pl.DataFrame:
    """Return a small structural-edge fixture."""
    return pl.DataFrame(
        {
            "module_code": ["M1", "M1", "M1"],
            "edge_id": ["e1", "e2", "e3"],
            "edge_type": ["activation", "activation", "activation"],
            "from_node_code": ["M1O1A1", "M1O1A2", "M1O2A1"],
            "to_node_code": ["M1O1A2", "M1O2A1", "M1O3A1"],
            "threshold_type": ["success_rate", "success_rate", "success_rate"],
            "threshold_value": [0.75, 0.75, 0.75],
            "rule_text": ["r1", "r2", "r3"],
            "source_primary": ["rules", "rules", "rules"],
            "source_enrichment": ["rules", "rules", "rules"],
            "enrich_lvl": [None, None, None],
            "enrich_sr": [None, None, None],
        }
    )


def _fact() -> pl.DataFrame:
    """Return an attempt fixture that exercises before/after/in-activity semantics."""
    return pl.DataFrame(
        {
            "created_at": [
                "2025-01-01T08:00:00+00:00",
                "2025-01-01T08:01:00+00:00",
                "2025-01-01T08:02:00+00:00",
                "2025-01-01T09:00:00+00:00",
                "2025-01-01T09:01:00+00:00",
                "2025-01-01T09:02:00+00:00",
                "2025-01-01T10:00:00+00:00",
                "2025-01-01T10:01:00+00:00",
                "2025-01-01T10:02:00+00:00",
                "2025-01-01T10:03:00+00:00",
                "2025-01-01T10:04:00+00:00",
                "2025-01-01T11:00:00+00:00",
                "2025-01-01T11:01:00+00:00",
                "2025-01-01T11:02:00+00:00",
                "2025-01-01T11:03:00+00:00",
                "2025-01-01T11:04:00+00:00",
                "2025-01-01T11:05:00+00:00",
                "2025-01-01T12:00:00+00:00",
                "2025-01-01T12:01:00+00:00",
                "2025-01-01T12:02:00+00:00",
                "2025-01-01T12:03:00+00:00",
                "2025-01-01T13:00:00+00:00",
                "2025-01-01T13:00:00+00:00",
                "2025-01-01T14:00:00+00:00",
                "2025-01-01T14:01:00+00:00",
            ],
            "date_utc": [date(2025, 1, 1)] * 25,
            "user_id": [
                "u_after",
                "u_after",
                "u_after",
                "u_before",
                "u_before",
                "u_before",
                "u_still",
                "u_still",
                "u_still",
                "u_still",
                "u_still",
                "u_becomes_after",
                "u_becomes_after",
                "u_becomes_after",
                "u_becomes_after",
                "u_becomes_after",
                "u_becomes_after",
                "u_threshold",
                "u_threshold",
                "u_threshold",
                "u_threshold",
                "u_same_ts",
                "u_same_ts",
                "u_mode_sep",
                "u_mode_sep",
            ],
            "classroom_id": [None] * 25,
            "playlist_or_module_id": [None] * 25,
            "objective_id": [
                "o3",
                "o2",
                "o2",
                "o1",
                "o2",
                "o2",
                "o1",
                "o2",
                "o2",
                "o1",
                "o2",
                "o1",
                "o2",
                "o2",
                "o3",
                "o2",
                "o2",
                "o3",
                "o3",
                "o2",
                "o2",
                "o3",
                "o2",
                "o3",
                "o2",
            ],
            "objective_label": [
                "Objective 3",
                "Objective 2",
                "Objective 2",
                "Objective 1",
                "Objective 2",
                "Objective 2",
                "Objective 1",
                "Objective 2",
                "Objective 2",
                "Objective 1",
                "Objective 2",
                "Objective 1",
                "Objective 2",
                "Objective 2",
                "Objective 3",
                "Objective 2",
                "Objective 2",
                "Objective 3",
                "Objective 3",
                "Objective 2",
                "Objective 2",
                "Objective 3",
                "Objective 2",
                "Objective 3",
                "Objective 2",
            ],
            "activity_id": [
                "a4",
                "a3",
                "a3",
                "a2",
                "a3",
                "a3",
                "a2",
                "a3",
                "a3",
                "a2",
                "a3",
                "a2",
                "a3",
                "a3",
                "a4",
                "a3",
                "a3",
                "a4",
                "a4",
                "a3",
                "a3",
                "a4",
                "a3",
                "a4",
                "a3",
            ],
            "activity_label": [
                "A4",
                "A3",
                "A3",
                "A2",
                "A3",
                "A3",
                "A2",
                "A3",
                "A3",
                "A2",
                "A3",
                "A2",
                "A3",
                "A3",
                "A4",
                "A3",
                "A3",
                "A4",
                "A4",
                "A3",
                "A3",
                "A4",
                "A3",
                "A4",
                "A3",
            ],
            "exercise_id": [
                "e_a4_1",
                "e_a3_1",
                "e_a3_2",
                "e_a2_1",
                "e_a3_3",
                "e_a3_4",
                "e_a2_2",
                "e_a3_5",
                "e_a3_6",
                "e_a2_3",
                "e_a3_7",
                "e_a2_4",
                "e_a3_8",
                "e_a3_9",
                "e_a4_2",
                "e_a3_10",
                "e_a3_10",
                "e_a4_3",
                "e_a4_4",
                "e_a3_11",
                "e_a3_12",
                "e_a4_5",
                "e_a3_13",
                "e_a4_6",
                "e_a3_14",
            ],
            "data_correct": [
                1,
                0,
                1,
                1,
                1,
                0,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                0,
                1,
                0,
                1,
                1,
                1,
                1,
                0,
                1,
                1,
                1,
                1,
            ],
            "data_duration": [None] * 25,
            "session_duration": [None] * 25,
            "work_mode": [
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "zpdes",
                "playlist",
                "zpdes",
            ],
            "attempt_number": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                2,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            ],
            "module_id": ["m1"] * 25,
            "module_code": ["M1"] * 25,
            "module_label": ["Module 1"] * 25,
        }
    ).with_columns(pl.col("created_at").str.to_datetime(time_zone="UTC"))


def test_build_zpdes_exercise_progression_events_emits_new_exercise_events(tmp_path: Path) -> None:
    """Check exact progression counts for new-exercise first attempts."""
    settings = _build_settings(tmp_path)
    events = build_zpdes_exercise_progression_events_from_fact(_fact(), settings=settings)

    a3_zpdes = events.filter((pl.col("activity_id") == "a3") & (pl.col("work_mode") == "zpdes"))
    assert a3_zpdes.height == 14
    assert a3_zpdes.filter(pl.col("exercise_id") == "e_a3_10").height == 1

    after_first = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_1").row(0, named=True)
    assert int(after_first["prior_attempt_count"]) == 1
    assert int(after_first["prior_before_activity_attempt_count"]) == 0
    assert int(after_first["prior_same_activity_attempt_count"]) == 0
    assert int(after_first["prior_later_activity_attempt_count"]) == 1

    after_second = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_2").row(0, named=True)
    assert int(after_second["prior_attempt_count"]) == 2
    assert int(after_second["prior_same_activity_attempt_count"]) == 1
    assert int(after_second["prior_later_activity_attempt_count"]) == 1

    before_first = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_3").row(0, named=True)
    assert int(before_first["prior_attempt_count"]) == 1
    assert int(before_first["prior_before_activity_attempt_count"]) == 1
    assert int(before_first["prior_same_activity_attempt_count"]) == 0
    assert int(before_first["prior_later_activity_attempt_count"]) == 0

    in_activity_later = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_7").row(0, named=True)
    assert int(in_activity_later["prior_attempt_count"]) == 4
    assert int(in_activity_later["prior_before_activity_attempt_count"]) == 2
    assert int(in_activity_later["prior_same_activity_attempt_count"]) == 2
    assert int(in_activity_later["prior_later_activity_attempt_count"]) == 0

    becomes_after = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_10").row(0, named=True)
    assert int(becomes_after["prior_attempt_count"]) == 4
    assert int(becomes_after["prior_before_activity_attempt_count"]) == 1
    assert int(becomes_after["prior_same_activity_attempt_count"]) == 2
    assert int(becomes_after["prior_later_activity_attempt_count"]) == 1

    same_ts = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_13").row(0, named=True)
    assert int(same_ts["prior_attempt_count"]) == 0

    mode_sep = a3_zpdes.filter(pl.col("exercise_id") == "e_a3_14").row(0, named=True)
    assert int(mode_sep["prior_attempt_count"]) == 0


def test_attach_transition_metric_to_nodes_uses_weighted_first_attempt_success() -> None:
    """Check that node metrics and hover summaries expose per-work-mode first-attempt stats."""
    nodes = _nodes()
    progression_events = pl.DataFrame(
        {
            "created_at": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1), date(2025, 1, 1)],
            "date_utc": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1), date(2025, 1, 1)],
            "user_id": ["u1", "u2", "u3", "u4"],
            "module_id": ["m1"] * 4,
            "module_code": ["M1"] * 4,
            "module_label": ["Module 1"] * 4,
            "objective_id": ["o1", "o1", "o2", "o2"],
            "objective_label": ["Objective 1", "Objective 1", "Objective 2", "Objective 2"],
            "activity_id": ["a1", "a1", "a3", "a3"],
            "activity_label": ["A1", "A1", "A3", "A3"],
            "exercise_id": ["e1", "e2", "e3", "e4"],
            "work_mode": ["zpdes", "zpdes", "playlist", "zpdes"],
            "destination_rank": [1, 1, 3, 3],
            "exercise_first_attempt_outcome": [1, 0, 0, 1],
            "prior_attempt_count": [0, 1, 0, 1],
            "prior_before_activity_attempt_count": [0, 1, 0, 1],
            "prior_same_activity_attempt_count": [0, 0, 0, 0],
            "prior_later_activity_attempt_count": [0, 0, 0, 0],
        }
    )
    out = attach_transition_metric_to_nodes(
        nodes=nodes,
        agg_activity_elo=pl.DataFrame(),
        progression_events=progression_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        metric="first_attempt_success_rate",
        work_mode="zpdes",
    )
    a1 = out.filter(pl.col("node_id") == "a1").to_dicts()[0]
    a3 = out.filter(pl.col("node_id") == "a3").to_dicts()[0]
    objective = out.filter(pl.col("node_id") == "o1").to_dicts()[0]
    assert abs(float(a1["transition_metric_value"]) - 0.5) < 1e-9
    assert int(a1["selected_mode_first_attempt_event_count"]) == 2
    assert abs(float(a1["zpdes_first_attempt_success_rate"]) - 0.5) < 1e-9
    assert int(a1["zpdes_first_attempt_event_count"]) == 2
    assert abs(float(a3["transition_metric_value"]) - 1.0) < 1e-9
    assert int(a3["selected_mode_first_attempt_event_count"]) == 1
    assert abs(float(a3["playlist_first_attempt_success_rate"]) - 0.0) < 1e-9
    assert int(a3["playlist_first_attempt_event_count"]) == 1
    assert abs(float(a3["zpdes_first_attempt_success_rate"]) - 1.0) < 1e-9
    assert int(a3["zpdes_first_attempt_event_count"]) == 1
    assert objective["transition_metric_value"] is None


def test_attach_transition_metric_to_nodes_uses_activity_elo_without_date_filter() -> None:
    """Check that Elo node coloring is joined directly from the activity Elo artifact."""
    nodes = _nodes()
    agg_activity_elo = pl.DataFrame(
        {
            "module_code": ["M1", "M1"],
            "activity_id": ["a1", "a3"],
            "activity_mean_exercise_elo": [1480.0, 1530.0],
        }
    )
    out = attach_transition_metric_to_nodes(
        nodes=nodes,
        agg_activity_elo=agg_activity_elo,
        progression_events=pl.DataFrame(),
        module_code="M1",
        start_date=date(2025, 2, 1),
        end_date=date(2025, 2, 2),
        metric="activity_mean_exercise_elo",
        work_mode="zpdes",
    )
    a3 = out.filter(pl.col("node_id") == "a3").to_dicts()[0]
    assert float(a3["transition_metric_value"]) == 1530.0


def test_attach_progression_cohort_metrics_to_nodes_reclassifies_with_threshold(tmp_path: Path) -> None:
    """Check event and unique-student counts for before/after/in-activity cohorts."""
    settings = _build_settings(tmp_path)
    progression_events = build_zpdes_exercise_progression_events_from_fact(_fact(), settings=settings)
    nodes = _nodes()

    low_threshold = attach_progression_cohort_metrics_to_nodes(
        nodes=nodes,
        progression_events=progression_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        work_mode="zpdes",
        later_attempt_threshold=1,
    )
    low_row = low_threshold.filter(pl.col("node_id") == "a3").row(0, named=True)
    assert int(low_row["before_event_count"]) == 3
    assert int(low_row["before_unique_students"]) == 3
    assert abs(float(low_row["before_success_rate"]) - 1.0) < 1e-9
    assert int(low_row["before_previous_attempts"]) == 3
    assert int(low_row["after_event_count"]) == 5
    assert int(low_row["after_unique_students"]) == 3
    assert abs(float(low_row["after_success_rate"]) - 0.4) < 1e-9
    assert int(low_row["after_previous_attempts"]) == 12
    assert int(low_row["in_activity_event_count"]) == 4
    assert int(low_row["in_activity_unique_students"]) == 3
    assert abs(float(low_row["in_activity_success_rate"]) - 0.5) < 1e-9
    assert int(low_row["in_activity_previous_attempts"]) == 10

    high_threshold = attach_progression_cohort_metrics_to_nodes(
        nodes=nodes,
        progression_events=progression_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        work_mode="zpdes",
        later_attempt_threshold=2,
    )
    high_row = high_threshold.filter(pl.col("node_id") == "a3").row(0, named=True)
    assert int(high_row["before_event_count"]) == 3
    assert int(high_row["before_unique_students"]) == 3
    assert abs(float(high_row["before_success_rate"]) - 1.0) < 1e-9
    assert int(high_row["before_previous_attempts"]) == 3
    assert int(high_row["after_event_count"]) == 2
    assert int(high_row["after_unique_students"]) == 1
    assert abs(float(high_row["after_success_rate"]) - 0.5) < 1e-9
    assert int(high_row["after_previous_attempts"]) == 5
    assert int(high_row["in_activity_event_count"]) == 6
    assert int(high_row["in_activity_unique_students"]) == 4
    assert abs(float(high_row["in_activity_success_rate"]) - 0.5) < 1e-9
    assert int(high_row["in_activity_previous_attempts"]) == 16


def test_build_transition_efficiency_figure_stays_structural_only(tmp_path: Path) -> None:
    """Check that the figure remains structural and hover-driven."""
    settings = _build_settings(tmp_path)
    progression_events = build_zpdes_exercise_progression_events_from_fact(_fact(), settings=settings)
    nodes = attach_transition_metric_to_nodes(
        nodes=_nodes(),
        agg_activity_elo=pl.DataFrame(),
        progression_events=progression_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        metric="first_attempt_success_rate",
        work_mode="zpdes",
    )
    nodes = attach_progression_cohort_metrics_to_nodes(
        nodes=nodes,
        progression_events=progression_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        work_mode="zpdes",
        later_attempt_threshold=1,
    )

    figure = build_transition_efficiency_figure(
        nodes=nodes,
        edges=_edges(),
        metric="first_attempt_success_rate",
        metric_label="First-attempt success",
        later_attempt_threshold=1,
        show_ids=False,
        curve_intra_objective_edges=True,
    )
    assert figure.data
    assert all(trace.name != "Observed incoming transition" for trace in figure.data)

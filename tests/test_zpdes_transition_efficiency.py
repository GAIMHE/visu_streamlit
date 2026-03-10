"""Validate the static ZPDES transition-efficiency helper module."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import polars as pl

from visu2.config import Settings
from visu2.derive import build_zpdes_first_arrival_events_from_fact
from visu2.zpdes_transition_efficiency import (
    attach_arrival_cohort_metrics_to_nodes,
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
        parquet_path=data_dir / "adaptiv_math_history.parquet",
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
            "module_code": ["M1"] * 5,
            "node_id": ["o1", "a1", "a2", "o2", "a3"],
            "node_code": ["M1O1", "M1O1A1", "M1O1A2", "M1O2", "M1O2A1"],
            "node_type": ["objective", "activity", "activity", "objective", "activity"],
            "label": ["Objective 1", "A1", "A2", "Objective 2", "A3"],
            "objective_code": ["M1O1", "M1O1", "M1O1", "M1O2", "M1O2"],
            "activity_index": [None, 1, 2, None, 1],
            "init_open": [True, True, False, False, False],
            "source_primary": ["catalog"] * 5,
            "source_enrichment": ["rules"] * 5,
            "is_ghost": [False] * 5,
        }
    )


def _edges() -> pl.DataFrame:
    """Return a small structural-edge fixture."""
    return pl.DataFrame(
        {
            "module_code": ["M1", "M1"],
            "edge_id": ["e1", "e2"],
            "edge_type": ["activation", "activation"],
            "from_node_code": ["M1O1A1", "M1O1A2"],
            "to_node_code": ["M1O1A2", "M1O2A1"],
            "threshold_type": ["success_rate", "success_rate"],
            "threshold_value": [0.75, 0.75],
            "rule_text": ["r1", "r2"],
            "source_primary": ["rules", "rules"],
            "source_enrichment": ["rules", "rules"],
            "enrich_lvl": [None, None],
            "enrich_sr": [None, None],
        }
    )


def _fact() -> pl.DataFrame:
    """Return a small attempt-level fixture for first-arrival cohort tests."""
    return pl.DataFrame(
        {
            "created_at": [
                "2025-01-01T08:00:00+00:00",
                "2025-01-01T08:01:00+00:00",
                "2025-01-01T08:02:00+00:00",
                "2025-01-01T09:00:00+00:00",
                "2025-01-01T09:01:00+00:00",
                "2025-01-01T10:00:00+00:00",
                "2025-01-01T11:00:00+00:00",
                "2025-01-01T11:00:00+00:00",
                "2025-01-01T12:00:00+00:00",
                "2025-01-01T12:01:00+00:00",
            ],
            "date_utc": [date(2025, 1, 1)] * 10,
            "user_id": [
                "u_before",
                "u_before",
                "u_before",
                "u_after",
                "u_after",
                "u_excluded",
                "u_same_ts",
                "u_same_ts",
                "u_mode_sep",
                "u_mode_sep",
            ],
            "classroom_id": [None] * 10,
            "playlist_or_module_id": [None] * 10,
            "objective_id": ["o1", "o1", "o1", "o2", "o1", "o1", "o2", "o1", "o2", "o1"],
            "objective_label": [
                "Objective 1",
                "Objective 1",
                "Objective 1",
                "Objective 2",
                "Objective 1",
                "Objective 1",
                "Objective 2",
                "Objective 1",
                "Objective 2",
                "Objective 1",
            ],
            "activity_id": ["a1", "a2", "a2", "a3", "a2", "a2", "a3", "a2", "a3", "a2"],
            "activity_label": ["A1", "A2", "A2", "A3", "A2", "A2", "A3", "A2", "A3", "A2"],
            "exercise_id": [None] * 10,
            "data_correct": [1, 1, 0, 1, 0, 1, 1, 0, 1, 1],
            "data_duration": [None] * 10,
            "session_duration": [None] * 10,
            "work_mode": [
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
            "attempt_number": [1, 1, 2, 1, 1, 1, 1, 1, 1, 1],
            "module_id": ["m1"] * 10,
            "module_code": ["M1"] * 10,
            "module_label": ["Module 1"] * 10,
        }
    ).with_columns(pl.col("created_at").str.to_datetime(time_zone="UTC"))


def test_build_zpdes_first_arrival_events_emits_one_row_per_destination_and_work_mode(tmp_path: Path) -> None:
    """Check strict first-arrival classification and work-mode isolation."""
    settings = _build_settings(tmp_path)
    events = build_zpdes_first_arrival_events_from_fact(_fact(), settings=settings)

    a2_zpdes = events.filter((pl.col("activity_id") == "a2") & (pl.col("work_mode") == "zpdes")).sort("user_id")
    assert a2_zpdes.height == 5

    before_row = a2_zpdes.filter(pl.col("user_id") == "u_before").row(0, named=True)
    assert before_row["arrival_bucket_base"] == "before"
    assert int(before_row["prior_attempt_count"]) == 1
    assert int(before_row["prior_before_attempt_count"]) == 1
    assert int(before_row["prior_later_attempt_count"]) == 0

    after_row = a2_zpdes.filter(pl.col("user_id") == "u_after").row(0, named=True)
    assert after_row["arrival_bucket_base"] == "after_candidate"
    assert int(after_row["prior_attempt_count"]) == 1
    assert int(after_row["prior_before_attempt_count"]) == 0
    assert int(after_row["prior_later_attempt_count"]) == 1

    excluded_row = a2_zpdes.filter(pl.col("user_id") == "u_excluded").row(0, named=True)
    assert excluded_row["arrival_bucket_base"] == "excluded"
    assert int(excluded_row["prior_attempt_count"]) == 0

    same_ts_row = a2_zpdes.filter(pl.col("user_id") == "u_same_ts").row(0, named=True)
    assert same_ts_row["arrival_bucket_base"] == "excluded"
    assert int(same_ts_row["prior_attempt_count"]) == 0

    mode_sep_row = a2_zpdes.filter(pl.col("user_id") == "u_mode_sep").row(0, named=True)
    assert mode_sep_row["arrival_bucket_base"] == "excluded"
    assert int(mode_sep_row["prior_attempt_count"]) == 0

    assert a2_zpdes.filter(pl.col("user_id") == "u_before").height == 1


def test_attach_transition_metric_to_nodes_uses_weighted_first_attempt_success() -> None:
    """Check that first-attempt node coloring uses a weighted date-filtered aggregation."""
    nodes = _nodes()
    agg_activity_daily = pl.DataFrame(
        {
            "date_utc": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)],
            "module_code": ["M1", "M1", "M1"],
            "activity_id": ["a1", "a1", "a2"],
            "first_attempt_success_rate": [0.6, 0.9, 0.5],
            "first_attempt_count": [10, 30, 20],
        }
    )
    out = attach_transition_metric_to_nodes(
        nodes=nodes,
        agg_activity_daily=agg_activity_daily,
        agg_activity_elo=pl.DataFrame(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        metric="first_attempt_success_rate",
    )
    a1 = out.filter(pl.col("node_id") == "a1").to_dicts()[0]
    objective = out.filter(pl.col("node_id") == "o1").to_dicts()[0]
    assert abs(float(a1["transition_metric_value"]) - 0.825) < 1e-9
    assert objective["transition_metric_value"] is None


def test_attach_transition_metric_to_nodes_uses_activity_elo_without_date_filter() -> None:
    """Check that Elo node coloring is joined directly from the activity Elo artifact."""
    nodes = _nodes()
    agg_activity_elo = pl.DataFrame(
        {
            "module_code": ["M1", "M1"],
            "activity_id": ["a1", "a2"],
            "activity_mean_exercise_elo": [1480.0, 1530.0],
        }
    )
    out = attach_transition_metric_to_nodes(
        nodes=nodes,
        agg_activity_daily=pl.DataFrame(),
        agg_activity_elo=agg_activity_elo,
        module_code="M1",
        start_date=date(2025, 2, 1),
        end_date=date(2025, 2, 2),
        metric="activity_mean_exercise_elo",
    )
    a2 = out.filter(pl.col("node_id") == "a2").to_dicts()[0]
    assert float(a2["transition_metric_value"]) == 1530.0


def test_attach_arrival_cohort_metrics_to_nodes_applies_exact_later_threshold() -> None:
    """Check that the later-attempt threshold shrinks only the after cohort."""
    nodes = _nodes()
    arrival_events = pl.DataFrame(
        {
            "created_at": [datetime(2025, 1, 1, 8, 0), datetime(2025, 1, 1, 9, 0), datetime(2025, 1, 1, 10, 0), datetime(2025, 1, 1, 11, 0), datetime(2025, 1, 1, 12, 0)],
            "date_utc": [date(2025, 1, 1)] * 5,
            "user_id": ["u1", "u2", "u3", "u4", "u5"],
            "module_id": ["m1"] * 5,
            "module_code": ["M1"] * 5,
            "module_label": ["Module 1"] * 5,
            "objective_id": ["o1"] * 5,
            "objective_label": ["Objective 1"] * 5,
            "activity_id": ["a2"] * 5,
            "activity_label": ["A2"] * 5,
            "work_mode": ["zpdes"] * 5,
            "destination_rank": [2] * 5,
            "first_arrival_outcome": [1, 0, 1, 0, 1],
            "prior_attempt_count": [1, 2, 3, 4, 5],
            "prior_before_attempt_count": [1, 1, 0, 0, 0],
            "prior_later_attempt_count": [0, 0, 1, 3, 2],
            "arrival_bucket_base": ["before", "before", "after_candidate", "after_candidate", "excluded"],
        }
    )

    low_threshold = attach_arrival_cohort_metrics_to_nodes(
        nodes=nodes,
        arrival_events=arrival_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        work_mode="zpdes",
        later_attempt_threshold=1,
    )
    low_row = low_threshold.filter(pl.col("node_id") == "a2").row(0, named=True)
    assert int(low_row["before_students"]) == 2
    assert abs(float(low_row["before_success_rate"]) - 0.5) < 1e-9
    assert int(low_row["before_previous_attempts"]) == 3
    assert int(low_row["after_students"]) == 2
    assert abs(float(low_row["after_success_rate"]) - 0.5) < 1e-9
    assert int(low_row["after_previous_attempts"]) == 7

    high_threshold = attach_arrival_cohort_metrics_to_nodes(
        nodes=nodes,
        arrival_events=arrival_events,
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        work_mode="zpdes",
        later_attempt_threshold=2,
    )
    high_row = high_threshold.filter(pl.col("node_id") == "a2").row(0, named=True)
    assert int(high_row["before_students"]) == 2
    assert abs(float(high_row["before_success_rate"]) - 0.5) < 1e-9
    assert int(high_row["before_previous_attempts"]) == 3
    assert int(high_row["after_students"]) == 1
    assert abs(float(high_row["after_success_rate"]) - 0.0) < 1e-9
    assert int(high_row["after_previous_attempts"]) == 4


def test_build_transition_efficiency_figure_stays_structural_only() -> None:
    """Check that the redesigned figure no longer adds empirical transition traces."""
    nodes = attach_transition_metric_to_nodes(
        nodes=_nodes(),
        agg_activity_daily=pl.DataFrame(
            {
                "date_utc": [date(2025, 1, 1)] * 3,
                "module_code": ["M1", "M1", "M1"],
                "activity_id": ["a1", "a2", "a3"],
                "first_attempt_success_rate": [0.6, 0.4, 0.5],
                "first_attempt_count": [10, 20, 30],
            }
        ),
        agg_activity_elo=pl.DataFrame(),
        module_code="M1",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        metric="first_attempt_success_rate",
    )
    nodes = attach_arrival_cohort_metrics_to_nodes(
        nodes=nodes,
        arrival_events=pl.DataFrame(
            {
                "created_at": [datetime(2025, 1, 1, 8, 0)],
                "date_utc": [date(2025, 1, 1)],
                "user_id": ["u1"],
                "module_id": ["m1"],
                "module_code": ["M1"],
                "module_label": ["Module 1"],
                "objective_id": ["o1"],
                "objective_label": ["Objective 1"],
                "activity_id": ["a2"],
                "activity_label": ["A2"],
                "work_mode": ["zpdes"],
                "destination_rank": [2],
                "first_arrival_outcome": [1],
                "prior_attempt_count": [1],
                "prior_before_attempt_count": [1],
                "prior_later_attempt_count": [0],
                "arrival_bucket_base": ["before"],
            }
        ),
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

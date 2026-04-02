from __future__ import annotations

from datetime import datetime

import polars as pl

from visu2.work_mode_transitions import (
    build_work_mode_transition_edge_frame,
    build_work_mode_transition_paths,
    build_work_mode_transition_sankey,
)


def _raw_attempts() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "user_id": [
                "u0",
                "u0",
                "u1",
                "u1",
                "u1",
                "u2",
                "u2",
                "u2",
                "u2",
                "u2",
                "u3",
                "u3",
                "u3",
                "u3",
            ],
            "student_attempt_index": [1, 2, 1, 2, 3, 1, 2, 3, 4, 5, 1, 2, 3, 4],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 9, 5, 0),
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 5, 0),
                datetime(2025, 1, 1, 10, 10, 0),
                datetime(2025, 1, 1, 11, 0, 0),
                datetime(2025, 1, 1, 11, 5, 0),
                datetime(2025, 1, 1, 11, 10, 0),
                datetime(2025, 1, 1, 11, 15, 0),
                datetime(2025, 1, 1, 11, 20, 0),
                datetime(2025, 1, 1, 12, 0, 0),
                datetime(2025, 1, 1, 12, 5, 0),
                datetime(2025, 1, 1, 12, 10, 0),
                datetime(2025, 1, 1, 12, 15, 0),
            ],
            "work_mode": [
                "zpdes",
                "zpdes",
                "initial-test",
                "playlist",
                "playlist",
                "initial-test",
                "zpdes",
                "playlist",
                "adaptive-test",
                "zpdes",
                "playlist",
                "adaptive-test",
                "adaptive-test",
                "playlist",
            ],
        }
    )


def test_build_work_mode_transition_paths_tracks_first_three_changes() -> None:
    paths = build_work_mode_transition_paths(_raw_attempts())
    rows = {row["user_id"]: row for row in paths.to_dicts()}

    assert rows["u0"]["first_work_mode"] == "zpdes"
    assert rows["u0"]["transition_count_total"] == 0
    assert rows["u0"]["transition_1_mode"] is None
    assert rows["u0"]["continues_after_transition_3"] is False

    assert rows["u1"]["first_work_mode"] == "initial-test"
    assert rows["u1"]["transition_count_total"] == 1
    assert rows["u1"]["transition_1_mode"] == "playlist"
    assert rows["u1"]["transition_2_mode"] is None

    assert rows["u2"]["first_work_mode"] == "initial-test"
    assert rows["u2"]["transition_count_total"] == 4
    assert rows["u2"]["transition_1_mode"] == "zpdes"
    assert rows["u2"]["transition_2_mode"] == "playlist"
    assert rows["u2"]["transition_3_mode"] == "adaptive-test"
    assert rows["u2"]["continues_after_transition_3"] is True

    assert rows["u3"]["first_work_mode"] == "playlist"
    assert rows["u3"]["transition_count_total"] == 2
    assert rows["u3"]["transition_1_mode"] == "adaptive-test"
    assert rows["u3"]["transition_2_mode"] == "playlist"


def test_build_work_mode_transition_edge_frame_reconciles_student_totals() -> None:
    paths = build_work_mode_transition_paths(_raw_attempts())
    edges = build_work_mode_transition_edge_frame(paths)

    stage_zero_total = edges.filter(pl.col("source_stage") == 0)["student_count"].sum()
    assert stage_zero_total == paths.height

    no_transition = edges.filter(pl.col("target_label") == "No transition").row(0, named=True)
    assert no_transition["student_count"] == 1

    more_than_three = edges.filter(pl.col("target_label") == "More than 3 transitions").row(0, named=True)
    assert more_than_three["student_count"] == 1


def test_build_work_mode_transition_edge_frame_can_hide_small_links() -> None:
    paths = build_work_mode_transition_paths(_raw_attempts())
    visible_edges = build_work_mode_transition_edge_frame(paths, min_student_count=2)

    assert visible_edges.height == 0


def test_build_work_mode_transition_sankey_returns_single_trace() -> None:
    figure = build_work_mode_transition_sankey(
        build_work_mode_transition_paths(_raw_attempts()),
        min_student_count=1,
    )
    assert len(figure.data) == 1
    assert figure.data[0].type == "sankey"
    assert figure.layout.font.family == "IBM Plex Sans, Arial, sans-serif"
    assert figure.layout.font.size == 16
    assert figure.layout.font.color == "#000000"
    assert figure.data[0].textfont.color == "#000000"

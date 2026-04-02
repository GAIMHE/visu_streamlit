from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl

from visu2.classroom_progression import MISSING_ACTIVITY_LABEL, SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
from visu2.classroom_progression_sankey import (
    build_classroom_activity_paths,
    build_classroom_activity_sankey_edges,
    build_classroom_activity_sankey_figure,
    max_classroom_activity_path_length,
)


def _fact_fixture() -> pl.DataFrame:
    ts0 = datetime(2025, 1, 1, 9, 0, tzinfo=UTC)

    def row(
        minutes: int,
        *,
        classroom_id: str,
        user_id: str,
        activity_id: str,
        activity_label: str | None,
        work_mode: str = "zpdes",
        exercise_id: str = "e1",
        attempt_number: int = 1,
    ) -> dict[str, object]:
        created_at = ts0 + timedelta(minutes=minutes)
        return {
            "created_at": created_at,
            "date_utc": created_at.date(),
            "user_id": user_id,
            "classroom_id": classroom_id,
            "work_mode": work_mode,
            "activity_id": activity_id,
            "activity_label": activity_label,
            "exercise_id": exercise_id,
            "attempt_number": attempt_number,
            "objective_id": "o1",
            "module_code": "M1",
        }

    return pl.DataFrame(
        [
            row(1, classroom_id="c1", user_id="u1", activity_id="a1", activity_label="Intro", exercise_id="e1"),
            row(2, classroom_id="c1", user_id="u1", activity_id="a1", activity_label="Intro", exercise_id="e2", attempt_number=2),
            row(3, classroom_id="c1", user_id="u1", activity_id="a2", activity_label="Shared", exercise_id="e3"),
            row(4, classroom_id="c1", user_id="u1", activity_id="a1", activity_label="Intro", exercise_id="e4", attempt_number=3),
            row(5, classroom_id="c1", user_id="u1", activity_id="a3", activity_label="Shared", exercise_id="e5"),
            row(1, classroom_id="c1", user_id="u2", activity_id="a2", activity_label="Shared", exercise_id="e6"),
            row(2, classroom_id="c1", user_id="u2", activity_id="a4", activity_label=None, exercise_id="e7"),
            row(3, classroom_id="c1", user_id="u2", activity_id="a2", activity_label="Shared", exercise_id="e8", attempt_number=2),
            row(4, classroom_id="c1", user_id="u2", activity_id="a3", activity_label="Shared", exercise_id="e9"),
            row(1, classroom_id="c1", user_id="u3", activity_id="a1", activity_label="Intro", exercise_id="e10"),
            row(2, classroom_id="c1", user_id="u3", activity_id="a5", activity_label="Wrap", exercise_id="e11"),
            row(5, classroom_id="c2", user_id="u4", activity_id="a9", activity_label="Other", exercise_id="e12"),
        ]
    )


def test_build_classroom_activity_paths_ignores_revisits_and_repeated_attempts() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )

    rows = {row["user_id"]: row for row in payload["student_paths"]}
    assert payload["student_count"] == 3
    assert rows["u1"]["activity_ids"] == ["a1", "a2", "a3"]
    assert rows["u2"]["activity_ids"] == ["a2", "a4", "a3"]
    assert rows["u3"]["activity_ids"] == ["a1", "a5"]


def test_build_classroom_activity_paths_handles_missing_and_duplicate_labels() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )

    assert payload["activity_full_labels"] == ["Intro", "Shared", MISSING_ACTIVITY_LABEL, "Wrap", "Shared"]
    assert payload["activity_display_labels"] == [
        "Intro",
        "Shared",
        MISSING_ACTIVITY_LABEL,
        "Wrap",
        "Shared [2]",
    ]


def test_build_classroom_activity_paths_uses_synthetic_classroom_when_missing() -> None:
    fact = _fact_fixture().with_columns(pl.lit(None, dtype=pl.Utf8).alias("classroom_id"))

    payload = build_classroom_activity_paths(
        fact,
        classroom_id=SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID,
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )

    assert payload["classroom_id"] == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
    assert payload["classroom_label"] == "All students"
    assert payload["student_count"] == 4


def test_build_classroom_activity_paths_all_data_spans_explicit_classrooms() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id=SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID,
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )

    assert payload["classroom_id"] == SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
    assert payload["classroom_label"] == "All students"
    assert payload["student_count"] == 4
    assert sorted(payload["student_ids"]) == ["u1", "u2", "u3", "u4"]


def test_build_classroom_activity_paths_uses_activity_codes_when_available() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        activity_code_lookup={
            "a1": "M1O1A1",
            "a2": "M1O1A2",
            "a3": "M1O2A1",
            "a5": "M1O3A1",
        },
    )

    assert payload["activity_display_labels"] == [
        "M1O1A1",
        "M1O1A2",
        MISSING_ACTIVITY_LABEL,
        "M1O3A1",
        "M1O2A1",
    ]


def test_build_classroom_activity_sankey_edges_classifies_terminal_and_overflow() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )
    edges = build_classroom_activity_sankey_edges(payload, visible_steps=2)

    assert edges.filter(pl.col("source_stage") == 0)["student_count"].sum() == payload["student_count"]
    assert edges.filter(pl.col("target_label") == "More than 2 activities")["student_count"].sum() == 2
    stop_after_two = edges.filter(pl.col("target_label") == "Stopped after 2 activities").row(0, named=True)
    assert stop_after_two["student_count"] == 1
    assert stop_after_two["classroom_share"] == 1 / 3
    assert stop_after_two["source_share"] == 1.0


def test_build_classroom_activity_sankey_edges_can_start_from_later_step() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )
    edges = build_classroom_activity_sankey_edges(payload, visible_steps=1, start_step=2)

    assert edges.filter(pl.col("source_stage") == 0)["student_count"].sum() == 3
    assert edges.filter(pl.col("target_label") == "More than 2 activities")["student_count"].sum() == 2
    stop_after_two = edges.filter(pl.col("target_label") == "Stopped after 2 activities").row(0, named=True)
    assert stop_after_two["student_count"] == 1


def test_max_classroom_activity_path_length_reads_selected_classroom_depth() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
    )

    assert max_classroom_activity_path_length(payload) == 3
    assert max_classroom_activity_path_length({"student_paths": []}) == 1
    assert max_classroom_activity_path_length(None) == 1


def test_build_classroom_activity_sankey_figure_returns_single_trace() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        activity_code_lookup={
            "a1": "M1O1A1",
            "a2": "M1O1A2",
            "a3": "M1O2A1",
            "a5": "M1O3A1",
        },
    )
    figure = build_classroom_activity_sankey_figure(payload, visible_steps=3)

    assert len(figure.data) == 1
    assert figure.data[0].type == "sankey"
    assert figure.layout.font.family == "IBM Plex Sans, Arial, sans-serif"
    assert figure.layout.font.size == 16
    assert figure.layout.font.color == "#000000"
    assert figure.data[0].textfont.color == "#000000"
    assert figure.data[0].node.label[0] == "M1O1A1"
    assert "Stopped after" in figure.data[0].node.label[-1]
    assert figure.data[0].node.customdata[0] == "Intro"


def test_build_classroom_activity_sankey_figure_can_render_later_window() -> None:
    payload = build_classroom_activity_paths(
        _fact_fixture(),
        classroom_id="c1",
        mode_scope="zpdes",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        activity_code_lookup={
            "a1": "M1O1A1",
            "a2": "M1O1A2",
            "a3": "M1O2A1",
            "a5": "M1O3A1",
        },
    )
    figure = build_classroom_activity_sankey_figure(payload, visible_steps=1, start_step=2)

    assert len(figure.data) == 1
    labels = list(figure.data[0].node.label)
    assert "M1O1A2" in labels
    assert "More than 2 activities" in labels

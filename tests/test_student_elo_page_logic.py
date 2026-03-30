"""
test_student_elo_page_logic.py

Validate student Elo page selection defaults and replay payload stepping.

Dependencies
------------
- datetime
- polars
- visu2

Classes
-------
- None.

Functions
---------
- _profiles: Utility for profiles.
- _events: Utility for events.
- test_select_default_students_uses_percentile_defaults: Test scenario for select default students uses percentile defaults.
- test_select_default_students_keeps_single_eligible_student: Test scenario for select default students keeps single eligible student.
- test_build_student_elo_payload_respects_step_size_and_final_point: Test scenario for build student elo payload respects step size and final point.
- test_build_student_elo_payload_keeps_single_student_valid: Test scenario for build student elo payload keeps single student valid.
- test_build_student_elo_figure_uses_synchronized_cutoff: Test scenario for build student elo figure uses synchronized cutoff.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl

from visu2.student_elo import (
    build_student_elo_comparison_figure,
    build_student_elo_comparison_payload,
    build_student_elo_figure,
    build_student_elo_payload,
    load_student_elo_label_lookup,
    merge_student_elo_label_lookups,
    modules_for_student,
    select_default_students,
    select_student_by_id,
    select_students_near_attempt_target,
    summarize_student_module_profiles,
)


def _profiles() -> pl.DataFrame:
    """Profiles.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "user_id": ["u2", "u1", "u3"],
            "total_attempts": [120, 240, 40],
            "first_attempt_at": [
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 8, 0, 0),
            ],
            "last_attempt_at": [
                datetime(2025, 1, 2, 10, 0, 0),
                datetime(2025, 1, 3, 9, 0, 0),
                datetime(2025, 1, 1, 8, 30, 0),
            ],
            "unique_modules": [2, 3, 1],
            "unique_objectives": [3, 5, 1],
            "unique_activities": [10, 15, 2],
            "final_student_elo": [1510.0, 1580.0, 1490.0],
            "eligible_for_replay": [True, True, True],
        }
    )


def _events() -> pl.DataFrame:
    """Events.


Returns
-------
pl.DataFrame
        Result produced by this routine.

"""
    return pl.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u2", "u2"],
            "attempt_ordinal": [1, 2, 3, 1, 2],
            "created_at": [
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 1, 9, 5, 0),
                datetime(2025, 1, 1, 9, 10, 0),
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 5, 0),
            ],
            "date_utc": [datetime(2025, 1, 1).date()] * 5,
            "work_mode": ["zpdes", "zpdes", "zpdes", "playlist", "playlist"],
            "module_code": ["M1", "M1", "M1", "M1", "M1"],
            "objective_id": ["o1", "o1", "o1", "o2", "o2"],
            "activity_id": ["a1", "a1", "a2", "a3", "a3"],
            "exercise_id": ["e1", "e2", "e3", "e4", "e5"],
            "outcome": [1.0, 0.0, 1.0, 1.0, 0.0],
            "expected_success": [0.5, 0.4, 0.6, 0.5, 0.5],
            "exercise_elo": [1500.0, 1520.0, 1490.0, 1510.0, 1510.0],
            "student_elo_pre": [1500.0, 1512.0, 1502.4, 1500.0, 1512.0],
            "student_elo_post": [1512.0, 1502.4, 1512.0, 1512.0, 1500.0],
        }
    )


def _module_profiles() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "user_id": ["u1", "u1", "u2"],
            "module_id": ["m1", "m2", "m1"],
            "module_code": ["M1", "M2", "M1"],
            "module_label": ["Module 1", "Module 2", "Module 1"],
            "total_attempts": [100, 40, 60],
            "first_attempt_at": [
                datetime(2025, 1, 1, 9, 0, 0),
                datetime(2025, 1, 2, 9, 0, 0),
                datetime(2025, 1, 1, 10, 0, 0),
            ],
            "last_attempt_at": [
                datetime(2025, 1, 3, 9, 0, 0),
                datetime(2025, 1, 4, 9, 0, 0),
                datetime(2025, 1, 2, 10, 0, 0),
            ],
            "unique_modules": [1, 1, 1],
            "unique_objectives": [3, 2, 2],
            "unique_activities": [8, 4, 5],
            "final_student_elo": [1560.0, 1495.0, 1510.0],
            "eligible_for_replay": [True, True, True],
        }
    )


def test_select_default_students_uses_percentile_defaults() -> None:
    """Test select default students uses percentile defaults.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    selected = select_default_students(_profiles(), min_attempts=100, max_students=2)
    assert selected == ["u1", "u2"]


def test_select_default_students_keeps_single_eligible_student() -> None:
    """Test select default students keeps single eligible student.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    selected = select_default_students(_profiles(), min_attempts=200, max_students=2)
    assert selected == ["u1"]


def test_select_students_near_attempt_target_samples_within_band() -> None:
    """Test attempt-target sampling returns only students inside the requested band."""
    selected = select_students_near_attempt_target(
        _profiles(),
        target_attempts=110,
        tolerance_ratio=0.10,
        max_students=2,
        seed=7,
    )
    assert selected == ["u2"]


def _label_lookup() -> pl.DataFrame:
    """Return a minimal readable label lookup for hover enrichment."""
    return pl.DataFrame(
        {
            "activity_id": ["a1", "a2", "a3"],
            "module_code": ["M1", "M1", "M1"],
            "module_label": ["Numbers", "Numbers", "Numbers"],
            "objective_id": ["o1", "o1", "o2"],
            "objective_label": ["Counting", "Counting", "Comparisons"],
            "activity_label": ["Activity One", "Activity Two", "Activity Three"],
        }
    )


def test_select_students_near_attempt_target_can_return_two_random_students() -> None:
    """Test attempt-target sampling returns two students when the band is wide enough."""
    selected = select_students_near_attempt_target(
        _profiles(),
        target_attempts=120,
        tolerance_ratio=1.0,
        max_students=2,
        seed=3,
    )
    assert len(selected) == 2
    assert set(selected).issubset({"u1", "u2", "u3"})


def test_select_student_by_id_returns_exact_match_when_eligible() -> None:
    """Test explicit student selection returns the typed eligible student ID."""
    assert select_student_by_id(_profiles(), "u2") == "u2"


def test_select_student_by_id_returns_none_for_unknown_or_ineligible_student() -> None:
    """Test explicit student selection rejects unknown or replay-ineligible students."""
    profiles = _profiles().with_columns(
        pl.when(pl.col("user_id") == "u3")
        .then(False)
        .otherwise(pl.col("eligible_for_replay"))
        .alias("eligible_for_replay")
    )
    assert select_student_by_id(profiles, "u3") is None
    assert select_student_by_id(profiles, "missing") is None


def test_summarize_student_module_profiles_rolls_up_student_totals() -> None:
    summary = summarize_student_module_profiles(_module_profiles())
    row = summary.filter(pl.col("user_id") == "u1").to_dicts()[0]

    assert row["total_attempts"] == 140
    assert row["unique_modules"] == 2
    assert row["eligible_for_replay"] is True


def test_modules_for_student_returns_rows_sorted_by_attempts() -> None:
    modules = modules_for_student(_module_profiles(), "u1")

    assert modules["module_code"].to_list() == ["M1", "M2"]


def test_build_student_elo_payload_respects_step_size_and_final_point() -> None:
    """Test build student elo payload respects step size and final point.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    payload = build_student_elo_payload(_events(), ["u1", "u2"], step_size=2)
    assert payload["frame_cutoffs"] == [0, 2, 3]
    assert payload["max_attempts"] == 3


def test_build_student_elo_payload_keeps_single_student_valid() -> None:
    """Test build student elo payload keeps single student valid.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    payload = build_student_elo_payload(_events(), ["u2"], step_size=10)
    assert payload["student_ids"] == ["u2"]
    assert payload["frame_cutoffs"] == [0, 2]


def test_build_student_elo_payload_backfills_readable_labels() -> None:
    """Test payload enrichment uses catalog-backed readable labels."""
    payload = build_student_elo_payload(
        _events(),
        ["u1"],
        step_size=10,
        label_lookup=_label_lookup(),
    )
    series = payload["series"]["u1"]
    assert series["activity_label"][:2] == ["Activity One", "Activity One"]
    assert series["objective_label"][:2] == ["Counting", "Counting"]
    assert series["module_label"][:2] == ["Numbers", "Numbers"]


def test_load_student_elo_label_lookup_includes_orphan_fallback_labels(tmp_path: Path) -> None:
    """Test orphan activity labels can be recovered from calibrated Elo metadata."""
    catalog = {
        "meta": {},
        "id_label_index": {},
        "conflicts": {},
        "orphans": [],
        "exercise_to_hierarchy": {"e1": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"}},
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
                                "exercise_ids": ["e1"],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    catalog_path = tmp_path / "learning_catalog.json"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    exercise_elo_path = tmp_path / "agg_exercise_elo.parquet"
    pl.DataFrame(
        {
            "exercise_id": ["e_orphan"],
            "activity_id": ["orphan_a"],
            "module_code": ["M1"],
            "module_label": ["Module 1"],
            "objective_id": ["orphan_o"],
            "objective_label": ["Unmapped initial-test objective (M1)"],
            "activity_label": ["Unmapped initial-test activity (M1)"],
        }
    ).write_parquet(exercise_elo_path)

    lookup = load_student_elo_label_lookup(catalog_path, exercise_elo_path)
    orphan_row = lookup.filter(
        (pl.col("activity_id") == "orphan_a")
        & (pl.col("objective_id") == "orphan_o")
        & (pl.col("module_code") == "M1")
    ).to_dicts()[0]

    assert orphan_row["objective_label"] == "Unmapped initial-test objective (M1)"
    assert orphan_row["activity_label"] == "Unmapped initial-test activity (M1)"


def test_merge_student_elo_label_lookups_keeps_fact_specific_context() -> None:
    """Test fact-derived labels can extend catalog/orphan lookup for reused exercises."""
    fact_lookup = pl.DataFrame(
        {
            "activity_id": ["a_m51"],
            "module_code": ["M51"],
            "module_label": ["Fractions level 1"],
            "objective_id": ["o_shared"],
            "objective_label": ["Shared objective"],
            "activity_label": ["Shared activity in M51"],
        }
    )
    base_lookup = pl.DataFrame(
        {
            "activity_id": ["a_m52"],
            "module_code": ["M52"],
            "module_label": ["Fractions level 2"],
            "objective_id": ["o_shared_2"],
            "objective_label": ["Shared objective"],
            "activity_label": ["Shared activity in M52"],
        }
    )

    merged = merge_student_elo_label_lookups(fact_lookup, base_lookup)

    assert merged.height == 2
    assert merged.filter(pl.col("module_code") == "M51").to_dicts()[0]["activity_label"] == "Shared activity in M51"
    assert merged.filter(pl.col("module_code") == "M52").to_dicts()[0]["activity_label"] == "Shared activity in M52"


def test_build_student_elo_figure_uses_synchronized_cutoff() -> None:
    """Test build student elo figure uses synchronized cutoff.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    payload = build_student_elo_payload(_events(), ["u1", "u2"], step_size=2)
    figure = build_student_elo_figure(payload, frame_idx=1)

    assert len(figure.data) == 2
    trace_lengths = [len(trace.x) for trace in figure.data]
    assert trace_lengths == [2, 2]


def test_build_student_elo_figure_hovertemplate_mentions_objective_and_module() -> None:
    """Test hover template exposes readable activity context fields."""
    payload = build_student_elo_payload(
        _events(),
        ["u1"],
        step_size=2,
        label_lookup=_label_lookup(),
    )
    figure = build_student_elo_figure(payload, frame_idx=1)
    hovertemplate = figure.data[0].hovertemplate
    assert "<b>Objective</b>" in hovertemplate
    assert "<b>Module</b>" in hovertemplate


def test_build_student_elo_comparison_payload_aligns_systems() -> None:
    """Test comparison payload keeps current and iterative traces synchronized."""
    iterative_events = _events().with_columns(
        (pl.col("student_elo_post") + 5.0).alias("student_elo_post"),
        (pl.col("student_elo_pre") + 5.0).alias("student_elo_pre"),
        (pl.col("exercise_elo") + 10.0).alias("exercise_elo"),
    )
    payload = build_student_elo_comparison_payload(
        _events(),
        iterative_events,
        ["u1", "u2"],
        step_size=2,
        label_lookup=_label_lookup(),
    )
    assert payload["student_ids"] == ["u1", "u2"]
    assert payload["systems"] == ("Current Elo", "Iterative Elo")
    assert payload["frame_cutoffs"] == [0, 2, 3]


def test_build_student_elo_comparison_payload_rejects_misaligned_attempts() -> None:
    """Test comparison payload raises on mismatched attempt ordinals."""
    iterative_events = _events().filter(~((pl.col("user_id") == "u1") & (pl.col("attempt_ordinal") == 3)))
    try:
        build_student_elo_comparison_payload(
            _events(),
            iterative_events,
            ["u1"],
            step_size=2,
            label_lookup=_label_lookup(),
        )
    except ValueError as exc:
        assert "do not" in str(exc)
    else:
        raise AssertionError("Expected comparison payload alignment error")


def test_build_student_elo_comparison_figure_uses_system_styles() -> None:
    """Test comparison figure renders both systems with distinct line styles."""
    iterative_events = _events().with_columns(
        (pl.col("student_elo_post") + 5.0).alias("student_elo_post"),
        (pl.col("student_elo_pre") + 5.0).alias("student_elo_pre"),
        (pl.col("exercise_elo") + 10.0).alias("exercise_elo"),
    )
    payload = build_student_elo_comparison_payload(
        _events(),
        iterative_events,
        ["u1"],
        step_size=2,
        label_lookup=_label_lookup(),
    )
    figure = build_student_elo_comparison_figure(payload, frame_idx=1)
    assert len(figure.data) == 2
    assert figure.data[0].line.dash == "solid"
    assert figure.data[1].line.dash == "dash"
    assert "<b>System</b>" in figure.data[0].hovertemplate


def test_build_student_elo_comparison_figure_can_filter_to_one_system() -> None:
    """Test comparison figure can render only the requested Elo system."""
    iterative_events = _events().with_columns(
        (pl.col("student_elo_post") + 5.0).alias("student_elo_post"),
        (pl.col("student_elo_pre") + 5.0).alias("student_elo_pre"),
        (pl.col("exercise_elo") + 10.0).alias("exercise_elo"),
    )
    payload = build_student_elo_comparison_payload(
        _events(),
        iterative_events,
        ["u1"],
        step_size=2,
        label_lookup=_label_lookup(),
    )
    figure = build_student_elo_comparison_figure(
        payload,
        frame_idx=1,
        visible_systems=("Current Elo",),
    )
    assert len(figure.data) == 1
    assert figure.data[0].name == "Current Elo"


def test_build_student_elo_comparison_figure_adds_gap_markers() -> None:
    """Test comparison figure can mark large timestamp gaps on the shared attempt axis."""
    gap_events = _events().with_columns(
        pl.when((pl.col("user_id") == "u1") & (pl.col("attempt_ordinal") == 2))
        .then(pl.lit(datetime(2025, 1, 12, 9, 5, 0)))
        .otherwise(pl.col("created_at"))
        .alias("created_at")
    )
    iterative_events = gap_events.with_columns(
        (pl.col("student_elo_post") + 5.0).alias("student_elo_post"),
        (pl.col("student_elo_pre") + 5.0).alias("student_elo_pre"),
        (pl.col("exercise_elo") + 10.0).alias("exercise_elo"),
    )
    payload = build_student_elo_comparison_payload(
        gap_events,
        iterative_events,
        ["u1"],
        step_size=2,
        label_lookup=_label_lookup(),
    )
    figure = build_student_elo_comparison_figure(payload, frame_idx=1, gap_days_threshold=7.0)
    assert len(figure.layout.shapes or []) == 1
    assert len(figure.layout.annotations or []) == 1
    assert figure.layout.annotations[0].text == "11d"


def test_build_student_elo_comparison_figure_colors_markers_by_module() -> None:
    """Test comparison figure uses module-based marker colors within a student path."""
    module_events = _events().with_columns(
        pl.when((pl.col("user_id") == "u1") & (pl.col("attempt_ordinal") == 3))
        .then(pl.lit("M2"))
        .otherwise(pl.col("module_code"))
        .alias("module_code")
    )
    iterative_events = module_events.with_columns(
        (pl.col("student_elo_post") + 5.0).alias("student_elo_post"),
        (pl.col("student_elo_pre") + 5.0).alias("student_elo_pre"),
        (pl.col("exercise_elo") + 10.0).alias("exercise_elo"),
    )
    payload = build_student_elo_comparison_payload(
        module_events,
        iterative_events,
        ["u1"],
        step_size=3,
    )
    figure = build_student_elo_comparison_figure(payload, frame_idx=1)
    current_colors = list(figure.data[0].marker.color)
    iterative_colors = list(figure.data[1].marker.color)
    assert len(set(current_colors)) == 2
    assert current_colors == iterative_colors


def test_build_student_elo_comparison_figure_uses_work_mode_marker_symbols() -> None:
    """Test comparison figure maps work modes to the requested marker shapes."""
    extended_events = pl.concat(
        [
            _events().filter(pl.col("user_id") == "u1"),
            pl.DataFrame(
                {
                    "user_id": ["u1"],
                    "attempt_ordinal": [4],
                    "created_at": [datetime(2025, 1, 1, 9, 15, 0)],
                    "date_utc": [datetime(2025, 1, 1).date()],
                    "work_mode": ["playlist"],
                    "module_code": ["M1"],
                    "objective_id": ["o2"],
                    "activity_id": ["a3"],
                    "exercise_id": ["e6"],
                    "outcome": [1.0],
                    "expected_success": [0.5],
                    "exercise_elo": [1515.0],
                    "student_elo_pre": [1512.0],
                    "student_elo_post": [1524.0],
                }
            ),
        ],
        how="diagonal_relaxed",
    ).with_columns(
        pl.when(pl.col("attempt_ordinal") == 1)
        .then(pl.lit("zpdes"))
        .when(pl.col("attempt_ordinal") == 2)
        .then(pl.lit("adaptive-test"))
        .when(pl.col("attempt_ordinal") == 3)
        .then(pl.lit("initial-test"))
        .otherwise(pl.col("work_mode"))
        .alias("work_mode")
    )
    iterative_events = extended_events.with_columns(
        (pl.col("student_elo_post") + 5.0).alias("student_elo_post"),
        (pl.col("student_elo_pre") + 5.0).alias("student_elo_pre"),
        (pl.col("exercise_elo") + 10.0).alias("exercise_elo"),
    )
    payload = build_student_elo_comparison_payload(
        extended_events,
        iterative_events,
        ["u1"],
        step_size=4,
    )
    figure = build_student_elo_comparison_figure(payload, frame_idx=1)
    expected = ["triangle-up", "circle", "square", "diamond"]
    assert list(figure.data[0].marker.symbol) == expected
    assert list(figure.data[1].marker.symbol) == expected

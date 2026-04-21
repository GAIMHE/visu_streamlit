from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT_DIR / "apps"
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

page_module = importlib.import_module("page_modules.9_cohort_filter_viewer")


def test_load_cohort_attempt_rows_filters_by_date_range(tmp_path: Path) -> None:
    fact_path = tmp_path / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "created_at": [datetime(2025, 1, 1, 8, 0, 0), datetime(2025, 1, 2, 8, 0, 0)],
            "date_utc": [datetime(2025, 1, 1).date(), datetime(2025, 1, 2).date()],
            "work_mode": ["adaptive-test", "zpdes"],
            "module_code": ["M1", "M1"],
            "activity_id": ["a1", "a2"],
            "exercise_id": ["e1", "e2"],
            "attempt_number": [1, 1],
        }
    ).write_parquet(fact_path)

    result = page_module._load_cohort_attempt_rows(
        fact_path,
        start_date_iso="2025-01-02",
        end_date_iso="2025-01-02",
    )

    assert result.height == 1
    assert result.item(0, "user_id") == "u2"
    assert result.item(0, "activity_id") == "a2"


def test_load_fact_module_options_includes_fact_only_modules(tmp_path: Path) -> None:
    fact_path = tmp_path / "fact_attempt_core.parquet"
    pl.DataFrame(
        {
            "module_code": ["M1", "M54", None, "M54"],
        }
    ).write_parquet(fact_path)

    result = page_module._load_fact_module_options(fact_path)

    assert result == ["M1", "M54"]


def test_load_activity_exercise_counts_aggregates_catalog_exercises(tmp_path: Path) -> None:
    catalog_path = tmp_path / "learning_catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "meta": {},
                "id_label_index": {},
                "exercise_to_hierarchy": {},
                "modules": [
                    {
                        "id": "module_1",
                        "code": "M1",
                        "title": {"short": "Module 1", "long": "Module 1"},
                        "objectives": [
                            {
                                "id": "objective_1",
                                "code": "O1",
                                "title": {"short": "Obj 1", "long": "Objective 1"},
                                "activities": [
                                    {
                                        "id": "activity_1",
                                        "code": "A1",
                                        "title": {"short": "Act 1", "long": "Activity 1"},
                                        "exercise_ids": ["e1"],
                                    },
                                    {
                                        "id": "activity_2",
                                        "code": "A2",
                                        "title": {"short": "Act 2", "long": "Activity 2"},
                                        "exercise_ids": ["e2", "e3"],
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = page_module._load_activity_exercise_counts(catalog_path)

    assert result.to_dicts() == [
        {"activity_id": "activity_1", "activity_exercise_count": 1},
        {"activity_id": "activity_2", "activity_exercise_count": 2},
    ]


def test_sync_multiselect_state_discards_stale_values(monkeypatch) -> None:
    session_state: dict[str, object] = {"cohort_key": ["M1", "M99"]}
    monkeypatch.setattr(page_module.st, "session_state", session_state)

    page_module._sync_multiselect_state("cohort_key", ["M1", "M31"], default_all=False)

    assert session_state["cohort_key"] == ["M1"]


def test_clamp_date_range_resets_invalid_order() -> None:
    min_date = datetime(2025, 1, 1).date()
    max_date = datetime(2025, 1, 31).date()

    start_date, end_date = page_module._clamp_date_range(
        datetime(2025, 2, 1).date(),
        datetime(2024, 12, 31).date(),
        min_date=min_date,
        max_date=max_date,
    )

    assert (start_date, end_date) == (min_date, max_date)


def test_format_schema_table_includes_share_columns() -> None:
    frame = pl.DataFrame(
        {
            "cleaned_schema": ["adaptive-test -> zpdes"],
            "students": [4],
            "attempts": [20],
            "student_share": [0.4],
            "attempt_share": [0.25],
        }
    )

    formatted = page_module._format_schema_table(frame)

    assert formatted.columns == ["Schema", "Students", "Student share", "Attempts", "Attempt share"]
    assert formatted.item(0, "Student share") == "40.0%"
    assert formatted.item(0, "Attempt share") == "25.0%"


def test_build_filter_payload_keeps_removed_work_modes() -> None:
    payload = page_module._build_filter_payload(
        start_date=datetime(2025, 1, 1).date(),
        end_date=datetime(2025, 1, 31).date(),
        selected_modules=("M1",),
        selected_removed_work_modes=("initial-test",),
        max_retries=-1,
        retry_filter_mode="remove_exercise",
        retry_small_activity_exemption_enabled=True,
        retry_small_activity_max_exercises=1,
        min_placement_attempts=1,
        reject_same_placement_module_repeat=False,
        min_history=1,
        history_basis="raw_attempts",
        selected_transition_counts=(),
        min_students_per_schema=1,
        selected_schemas=(),
        schema_filter_mode="keep_selected",
    )

    assert payload["selected_removed_work_modes"] == ("initial-test",)
    assert payload["retry_small_activity_exemption_enabled"] is True
    assert payload["retry_small_activity_max_exercises"] == 1
    assert payload["schema_filter_mode"] == "keep_selected"


def test_build_final_rows_preview_keeps_headers_and_caps_rows() -> None:
    frame = pl.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "created_at": [
                datetime(2025, 1, 1, 8, 0, 0),
                datetime(2025, 1, 1, 8, 1, 0),
                datetime(2025, 1, 1, 8, 2, 0),
            ],
            "work_mode": ["adaptive-test", "zpdes", "playlist"],
        }
    )

    preview = page_module._build_final_rows_preview(frame, row_limit=2)

    assert preview.columns == frame.columns
    assert preview.height == 2
    assert preview.get_column("user_id").to_list() == ["u1", "u2"]

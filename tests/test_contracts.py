"""
test_contracts.py

Validate runtime contracts and schema compatibility helpers.

Dependencies
------------
- visu2

Classes
-------
- None.

Functions
---------
- test_fact_contract_has_required_columns: Test scenario for fact contract has required columns.
- test_catalog_frames_builds_expected_hierarchy: Test scenario for catalog frames builds expected hierarchy.
- test_required_agg_contracts_include_idee_pack: Test scenario for required agg contracts include idee pack.
- test_active_canonical_module_codes_are_stable: Test scenario for active canonical module codes are stable.
"""
from __future__ import annotations

from visu2.contracts import (
    ACTIVE_CANONICAL_MODULE_CODES,
    REQUIRED_AGG_COLUMNS,
    REQUIRED_FACT_COLUMNS,
)
from visu2.loaders import catalog_to_summary_frames


def test_fact_contract_has_required_columns() -> None:
    """Test fact contract has required columns.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    expected = {
        "created_at",
        "date_utc",
        "user_id",
        "classroom_id",
        "playlist_or_module_id",
        "objective_id",
        "objective_label",
        "activity_id",
        "activity_label",
        "exercise_id",
        "data_correct",
        "data_duration",
        "session_duration",
        "work_mode",
        "attempt_number",
        "module_id",
        "module_code",
        "module_label",
    }
    assert expected.issubset(set(REQUIRED_FACT_COLUMNS))


def test_catalog_frames_builds_expected_hierarchy() -> None:
    """Test catalog frames builds expected hierarchy.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    catalog = {
        "meta": {},
        "id_label_index": {},
        "conflicts": {},
        "orphans": [],
        "exercise_to_hierarchy": {
            "e1": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"},
            "e2": {"module_id": "m1", "objective_id": "o1", "activity_id": "a1"},
        },
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
    }
    frames = catalog_to_summary_frames(catalog)
    assert frames.modules.height == 1
    assert frames.objectives.height == 1
    assert frames.activities.height == 1
    assert frames.activity_exercises.height == 2
    mapped_modules = set(frames.exercise_hierarchy["module_id"].drop_nulls().to_list())
    assert mapped_modules == {"m1"}


def test_required_agg_contracts_include_idee_pack() -> None:
    """Test required agg contracts include idee pack.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    expected_tables = {
        "hierarchy_context_lookup",
        "classroom_mode_profiles",
        "classroom_activity_summary_by_mode",
        "agg_module_usage_daily",
        "agg_playlist_module_usage",
        "agg_module_activity_usage",
        "agg_exercise_daily",
        "agg_exercise_elo",
        "agg_exercise_elo_iterative",
        "agg_activity_elo",
        "student_elo_events",
        "student_elo_profiles",
        "student_elo_events_batch_replay",
        "student_elo_profiles_batch_replay",
        "student_elo_events_iterative",
        "student_elo_profiles_iterative",
        "zpdes_exercise_progression_events",
        "work_mode_transition_paths",
    }
    assert expected_tables.issubset(set(REQUIRED_AGG_COLUMNS.keys()))


def test_active_canonical_module_codes_are_stable() -> None:
    """Test active canonical module codes are stable.


Returns
-------
None
        Result produced by this routine.


Examples
--------
    This function is validated through the test suite execution path.
"""
    assert ACTIVE_CANONICAL_MODULE_CODES == ("M1", "M31", "M32", "M33", "M41", "M42", "M43")

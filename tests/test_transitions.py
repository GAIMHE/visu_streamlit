"""
test_transitions.py

Validate transition edge extraction and ordering behavior.

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
- test_transition_edges_builds_expected_links: Test scenario for transition edges builds expected links.
"""
from __future__ import annotations

from datetime import datetime

import polars as pl

from visu2.transitions import build_transition_edges_from_fact


def test_transition_edges_builds_expected_links() -> None:
    """Test transition edges builds expected links.


Returns
-------
None
        Result produced by this routine.

Notes
-----
    Behavior is intentionally documented for maintainability and traceability.

Examples
--------
    This function is validated through the test suite execution path.
"""
    fact = pl.DataFrame(
        {
            "created_at": [
                datetime(2025, 1, 1, 8, 0, 0),
                datetime(2025, 1, 1, 8, 2, 0),
                datetime(2025, 1, 1, 8, 4, 0),
                datetime(2025, 1, 1, 10, 0, 0),
                datetime(2025, 1, 1, 10, 2, 0),
            ],
            "date_utc": [
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
                datetime(2025, 1, 1).date(),
            ],
            "user_id": ["u1", "u1", "u1", "u2", "u2"],
            "activity_id": ["a1", "a2", "a3", "a1", "a2"],
            "activity_label": ["Activity 1", "Activity 2", "Activity 3", "Activity 1", "Activity 2"],
            "objective_id": ["o1", "o1", "o2", "o1", "o1"],
            "module_id": ["m1", "m1", "m1", "m1", "m1"],
            "module_code": ["M1", "M1", "M1", "M1", "M1"],
            "module_label": ["Module 1", "Module 1", "Module 1", "Module 1", "Module 1"],
            "data_correct": [True, False, True, True, True],
        }
    )
    edges = build_transition_edges_from_fact(fact)
    assert edges.height > 0
    assert {"from_activity_id", "to_activity_id", "transition_count", "success_conditioned_count"}.issubset(
        set(edges.columns)
    )
    edge_set = set(zip(edges["from_activity_id"].to_list(), edges["to_activity_id"].to_list()))
    assert ("a1", "a2") in edge_set

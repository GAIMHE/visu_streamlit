"""Consistency-check regressions for source-local runtime artifacts."""

from __future__ import annotations

from visu2.checks import _catalog_integrity


def test_catalog_integrity_does_not_treat_empty_activity_as_an_exercise() -> None:
    catalog_payload = {
        "modules": [
            {
                "id": "module-1",
                "code": "M1",
                "title": {"short": "Module 1", "long": "Module 1"},
                "objectives": [
                    {
                        "id": "objective-1",
                        "code": "M1O1",
                        "title": {"short": "Objective 1", "long": "Objective 1"},
                        "activities": [
                            {
                                "id": "activity-1",
                                "code": "M1O1A1",
                                "title": {"short": "Activity 1", "long": "Activity 1"},
                                "exercise_ids": [],
                            }
                        ],
                    }
                ],
            }
        ],
        "id_label_index": {},
        "exercise_to_hierarchy": {},
    }

    integrity = _catalog_integrity(catalog_payload)

    assert integrity["catalog_exercise_ids_unique"] == 0
    assert integrity["catalog_exercise_ids"] == set()

"""Ordering and label helpers for the objective-activity matrix."""

from __future__ import annotations

from typing import Any


def safe_label(label: str | None, identifier: str | None) -> str:
    """Return a human-readable label with a stable ID fallback."""
    normalized = str(label or "").strip()
    if normalized:
        return normalized
    return str(identifier or "").strip()


def summary_maps(
    summary_payload: dict[str, Any],
    module_code: str,
) -> tuple[dict[str, int], dict[str, dict[str, int]], dict[str, str], dict[str, str]]:
    """Extract objective/activity ordering and labels from the catalog payload."""
    modules = summary_payload.get("modules") or []
    objectives = summary_payload.get("objectives") or []
    activities = summary_payload.get("activities") or []

    if isinstance(modules, list) and modules and isinstance(modules[0], dict) and "objectives" in modules[0]:
        objective_order_map: dict[str, int] = {}
        objective_activity_order_map: dict[str, dict[str, int]] = {}
        objective_label_map: dict[str, str] = {}
        activity_label_map: dict[str, str] = {}

        module_row = next(
            (
                row
                for row in modules
                if isinstance(row, dict) and str(row.get("code") or "").strip() == module_code
            ),
            None,
        )
        if not isinstance(module_row, dict):
            return objective_order_map, objective_activity_order_map, objective_label_map, activity_label_map

        for objective_idx, objective_row in enumerate(module_row.get("objectives") or []):
            if not isinstance(objective_row, dict):
                continue
            objective_id = str(objective_row.get("id") or "").strip()
            if not objective_id:
                continue
            objective_order_map[objective_id] = objective_idx
            title = objective_row.get("title") if isinstance(objective_row.get("title"), dict) else {}
            objective_label_map[objective_id] = safe_label(
                (title or {}).get("short") or (title or {}).get("long"),
                objective_id,
            )

            activity_order_map: dict[str, int] = {}
            for activity_idx, activity_row in enumerate(objective_row.get("activities") or []):
                if not isinstance(activity_row, dict):
                    continue
                activity_id = str(activity_row.get("id") or "").strip()
                if not activity_id:
                    continue
                activity_order_map[activity_id] = activity_idx
                a_title = activity_row.get("title") if isinstance(activity_row.get("title"), dict) else {}
                activity_label_map[activity_id] = safe_label(
                    (a_title or {}).get("short") or (a_title or {}).get("long"),
                    activity_id,
                )
            objective_activity_order_map[objective_id] = activity_order_map

        return objective_order_map, objective_activity_order_map, objective_label_map, activity_label_map

    module_row = next(
        (
            row
            for row in modules
            if isinstance(row, dict) and str(row.get("code") or "").strip() == module_code
        ),
        None,
    )

    objective_order_map: dict[str, int] = {}
    if isinstance(module_row, dict):
        for idx, objective_id in enumerate(module_row.get("objectiveIds") or []):
            objective_order_map[str(objective_id)] = idx

    objective_activity_order_map: dict[str, dict[str, int]] = {}
    objective_label_map: dict[str, str] = {}
    for row in objectives:
        if not isinstance(row, dict):
            continue
        objective_id = str(row.get("id") or "").strip()
        if not objective_id:
            continue
        title = row.get("title") if isinstance(row.get("title"), dict) else {}
        objective_label_map[objective_id] = safe_label(
            (title or {}).get("short") or (title or {}).get("long"),
            objective_id,
        )

        activity_order_map: dict[str, int] = {}
        for idx, activity_id in enumerate(row.get("activityIds") or []):
            activity_order_map[str(activity_id)] = idx
        objective_activity_order_map[objective_id] = activity_order_map

    activity_label_map: dict[str, str] = {}
    for row in activities:
        if not isinstance(row, dict):
            continue
        activity_id = str(row.get("id") or "").strip()
        if not activity_id:
            continue
        title = row.get("title") if isinstance(row.get("title"), dict) else {}
        activity_label_map[activity_id] = safe_label(
            (title or {}).get("short") or (title or {}).get("long"),
            activity_id,
        )

    return objective_order_map, objective_activity_order_map, objective_label_map, activity_label_map

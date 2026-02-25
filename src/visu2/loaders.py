from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class SummaryFrames:
    modules: pl.DataFrame
    objectives: pl.DataFrame
    activities: pl.DataFrame
    module_objectives: pl.DataFrame
    objective_activities: pl.DataFrame
    activity_exercises: pl.DataFrame
    activity_hierarchy: pl.DataFrame
    exercise_hierarchy: pl.DataFrame


@dataclass(frozen=True)
class CatalogIndexFrames:
    index: pl.DataFrame
    modules: pl.DataFrame
    objectives: pl.DataFrame
    activities: pl.DataFrame
    exercises: pl.DataFrame


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_learning_catalog(path: Path) -> dict:
    payload = load_json(path)
    required = {"meta", "id_label_index", "modules", "exercise_to_hierarchy"}
    missing = required - set(payload.keys())
    if missing:
        raise ValueError(f"learning_catalog.json missing keys: {sorted(missing)}")
    return payload


def load_zpdes_rules(path: Path) -> dict:
    payload = load_json(path)
    required = {"meta", "module_rules", "map_id_code", "links_to_catalog", "unresolved_links"}
    missing = required - set(payload.keys())
    if missing:
        raise ValueError(f"zpdes_rules.json missing keys: {sorted(missing)}")
    return payload


def load_exercises(path: Path) -> dict:
    payload = load_json(path)
    if "exercises" not in payload:
        raise ValueError("exercises.json missing key: exercises")
    return payload


def _explode_ids(df: pl.DataFrame, list_col: str, out_col: str) -> pl.DataFrame:
    return (
        df.select([c for c in df.columns if c != list_col] + [list_col])
        .explode(list_col)
        .rename({list_col: out_col})
    )


def _summary_like_to_frames(summary: dict) -> SummaryFrames:
    modules = pl.DataFrame(
        [
            {
                "module_id": row["id"],
                "module_code": row.get("code"),
                "module_title_short": (row.get("title") or {}).get("short"),
                "module_title_long": (row.get("title") or {}).get("long"),
                "objective_ids": row.get("objectiveIds") or [],
            }
            for row in summary["modules"]
        ]
    )
    objectives = pl.DataFrame(
        [
            {
                "objective_id": row["id"],
                "objective_code": row.get("code"),
                "objective_title_short": (row.get("title") or {}).get("short"),
                "objective_title_long": (row.get("title") or {}).get("long"),
                "activity_ids": row.get("activityIds") or [],
            }
            for row in summary["objectives"]
        ]
    )
    activities = pl.DataFrame(
        [
            {
                "activity_id": row["id"],
                "activity_code": row.get("code"),
                "activity_title_short": (row.get("title") or {}).get("short"),
                "activity_title_long": (row.get("title") or {}).get("long"),
                "exercise_ids": row.get("exerciseIds") or [],
            }
            for row in summary["activities"]
        ]
    )

    module_objectives = _explode_ids(modules, "objective_ids", "objective_id").select(
        ["module_id", "module_code", "module_title_short", "objective_id"]
    )
    objective_activities = _explode_ids(
        objectives, "activity_ids", "activity_id"
    ).select(["objective_id", "activity_id"])
    activity_exercises = _explode_ids(activities, "exercise_ids", "exercise_id").select(
        ["activity_id", "activity_code", "exercise_id"]
    )

    activity_hierarchy = (
        objective_activities.join(module_objectives, on="objective_id", how="left")
        .join(
            activities.select(["activity_id", "activity_code", "activity_title_short"]),
            on="activity_id",
            how="left",
        )
        .join(
            objectives.select(["objective_id", "objective_code", "objective_title_short"]),
            on="objective_id",
            how="left",
        )
        .rename(
            {
                "module_title_short": "module_label",
                "objective_title_short": "objective_label",
                "activity_title_short": "activity_label",
            }
        )
        .unique()
    )

    exercise_hierarchy = (
        activity_exercises.join(activity_hierarchy, on=["activity_id", "activity_code"], how="left")
        .select(
            [
                "exercise_id",
                "activity_id",
                "activity_code",
                "objective_id",
                "objective_code",
                "objective_label",
                "module_id",
                "module_code",
                "module_label",
                "activity_label",
            ]
        )
        .unique()
    )

    return SummaryFrames(
        modules=modules,
        objectives=objectives,
        activities=activities,
        module_objectives=module_objectives,
        objective_activities=objective_activities,
        activity_exercises=activity_exercises,
        activity_hierarchy=activity_hierarchy,
        exercise_hierarchy=exercise_hierarchy,
    )


def _title_short_long(title_obj: object) -> tuple[str | None, str | None]:
    if not isinstance(title_obj, dict):
        return None, None
    short = title_obj.get("short")
    long = title_obj.get("long")
    return (
        str(short).strip() if isinstance(short, str) and str(short).strip() else None,
        str(long).strip() if isinstance(long, str) and str(long).strip() else None,
    )


def catalog_to_summary_frames(catalog: dict) -> SummaryFrames:
    modules_rows: list[dict[str, object]] = []
    objectives_rows: list[dict[str, object]] = []
    activities_rows: list[dict[str, object]] = []

    modules = catalog.get("modules")
    if not isinstance(modules, list):
        raise ValueError("learning_catalog.modules must be a list")

    for module in modules:
        if not isinstance(module, dict):
            continue
        module_id = module.get("id")
        if not isinstance(module_id, str) or not module_id.strip():
            continue
        module_short, module_long = _title_short_long(module.get("title"))
        objective_ids: list[str] = []
        objective_items = module.get("objectives") if isinstance(module.get("objectives"), list) else []
        for objective in objective_items:
            if not isinstance(objective, dict):
                continue
            objective_id = objective.get("id")
            if not isinstance(objective_id, str) or not objective_id.strip():
                continue
            objective_ids.append(objective_id)
            objective_short, objective_long = _title_short_long(objective.get("title"))
            activity_ids: list[str] = []
            activity_items = (
                objective.get("activities") if isinstance(objective.get("activities"), list) else []
            )
            for activity in activity_items:
                if not isinstance(activity, dict):
                    continue
                activity_id = activity.get("id")
                if not isinstance(activity_id, str) or not activity_id.strip():
                    continue
                activity_ids.append(activity_id)
                activity_short, activity_long = _title_short_long(activity.get("title"))
                exercise_ids = [
                    str(exercise_id)
                    for exercise_id in (activity.get("exercise_ids") or [])
                    if isinstance(exercise_id, str) and exercise_id.strip()
                ]
                activities_rows.append(
                    {
                        "id": activity_id,
                        "code": activity.get("code"),
                        "title": {
                            "short": activity_short,
                            "long": activity_long,
                        },
                        "exerciseIds": exercise_ids,
                    }
                )

            objectives_rows.append(
                {
                    "id": objective_id,
                    "code": objective.get("code"),
                    "title": {
                        "short": objective_short,
                        "long": objective_long,
                    },
                    "activityIds": activity_ids,
                }
            )

        modules_rows.append(
            {
                "id": module_id,
                "code": module.get("code"),
                "title": {
                    "short": module_short,
                    "long": module_long,
                },
                "objectiveIds": objective_ids,
            }
        )

    summary_like = {
        "modules": modules_rows,
        "objectives": objectives_rows,
        "activities": activities_rows,
    }
    return _summary_like_to_frames(summary_like)


def catalog_id_index_frames(catalog: dict) -> CatalogIndexFrames:
    raw_index = catalog.get("id_label_index")
    if not isinstance(raw_index, dict):
        raise ValueError("learning_catalog.id_label_index must be an object")

    rows: list[dict[str, object]] = []
    for identifier, value in raw_index.items():
        if not isinstance(identifier, str) or not identifier.strip():
            continue
        if not isinstance(value, dict):
            continue
        rows.append(
            {
                "id": identifier,
                "type": value.get("type"),
                "code": value.get("code"),
                "short_title": value.get("short_title"),
                "long_title": value.get("long_title"),
                "sources": value.get("sources") if isinstance(value.get("sources"), list) else [],
            }
        )

    index = pl.DataFrame(rows) if rows else pl.DataFrame(
        {"id": [], "type": [], "code": [], "short_title": [], "long_title": [], "sources": []}
    )
    for column in ["id", "type", "code", "short_title", "long_title"]:
        if column in index.columns:
            index = index.with_columns(pl.col(column).cast(pl.Utf8))

    def _typed_frame(type_name: str) -> pl.DataFrame:
        if index.height == 0:
            return index
        return index.filter(pl.col("type") == type_name)

    return CatalogIndexFrames(
        index=index,
        modules=_typed_frame("module"),
        objectives=_typed_frame("objective"),
        activities=_typed_frame("activity"),
        exercises=_typed_frame("exercise"),
    )


def zpdes_code_maps(zpdes_rules: dict) -> dict[str, dict[str, object]]:
    raw = zpdes_rules.get("map_id_code")
    if not isinstance(raw, dict):
        raise ValueError("zpdes_rules.map_id_code must be an object")
    code_to_id = raw.get("code_to_id")
    id_to_codes = raw.get("id_to_codes")
    if not isinstance(code_to_id, dict) or not isinstance(id_to_codes, dict):
        raise ValueError("zpdes_rules.map_id_code must contain code_to_id and id_to_codes objects")
    return {
        "code_to_id": code_to_id,
        "id_to_codes": id_to_codes,
    }

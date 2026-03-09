"""Catalog and metadata joins used by derived-table builders."""

from __future__ import annotations

import polars as pl

from .config import Settings
from .derive_common import (
    ACTIVITY_CODE_RE,
    MODULE_CODE_RE,
    OBJECTIVE_CODE_RE,
    exercise_label_from_instruction,
)
from .loaders import (
    catalog_id_index_frames,
    catalog_to_summary_frames,
    load_exercises,
    load_learning_catalog,
    load_zpdes_rules,
    zpdes_code_maps,
)


def hierarchy_map_from_catalog(settings: Settings) -> pl.DataFrame:
    """Return the activity-to-hierarchy frame used for fact enrichment."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    frames = catalog_to_summary_frames(catalog)
    return frames.activity_hierarchy.select(
        [
            "activity_id",
            pl.col("objective_id").alias("objective_id_summary"),
            "objective_label",
            "activity_label",
            "module_id",
            "module_code",
            "module_label",
        ]
    ).unique()


def exercise_hierarchy_map_from_catalog(settings: Settings) -> pl.DataFrame:
    """Return the exercise-to-hierarchy frame used for playlist backfill and Elo."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    frames = catalog_to_summary_frames(catalog)
    exercise_to_hierarchy = catalog.get("exercise_to_hierarchy")
    if not isinstance(exercise_to_hierarchy, dict):
        exercise_to_hierarchy = {}

    rows: list[dict[str, str | None]] = []
    for exercise_id, mapping in exercise_to_hierarchy.items():
        if not isinstance(exercise_id, str) or not exercise_id.strip():
            continue
        if not isinstance(mapping, dict):
            continue
        rows.append(
            {
                "exercise_id": exercise_id,
                "activity_id_exercise_summary": mapping.get("activity_id"),
                "objective_id_exercise_summary": mapping.get("objective_id"),
                "module_id_exercise_summary": mapping.get("module_id"),
            }
        )

    if rows:
        map_df = pl.DataFrame(rows).unique(subset=["exercise_id"], keep="first")
    else:
        map_df = pl.DataFrame(
            {
                "exercise_id": [],
                "activity_id_exercise_summary": [],
                "objective_id_exercise_summary": [],
                "module_id_exercise_summary": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "activity_id_exercise_summary": pl.Utf8,
                "objective_id_exercise_summary": pl.Utf8,
                "module_id_exercise_summary": pl.Utf8,
            },
        )

    activity_label_lookup = (
        frames.activity_hierarchy.select(
            [
                "activity_id",
                "activity_label",
                "objective_label",
                "module_label",
                "module_code",
                "module_id",
                "objective_id",
            ]
        )
        .unique(subset=["activity_id"], keep="first")
        .rename(
            {
                "activity_id": "activity_id_exercise_summary",
                "activity_label": "activity_label_exercise_summary",
                "objective_label": "objective_label_exercise_summary",
                "module_label": "module_label_exercise_summary",
                "module_code": "module_code_exercise_summary",
                "module_id": "module_id_hier_activity",
                "objective_id": "objective_id_hier_activity",
            }
        )
    )

    return (
        map_df.join(activity_label_lookup, on="activity_id_exercise_summary", how="left")
        .with_columns(
            pl.coalesce(
                [pl.col("module_id_exercise_summary"), pl.col("module_id_hier_activity")]
            ).alias("module_id_exercise_summary"),
            pl.coalesce(
                [
                    pl.col("objective_id_exercise_summary"),
                    pl.col("objective_id_hier_activity"),
                ]
            ).alias("objective_id_exercise_summary"),
        )
        .drop(["module_id_hier_activity", "objective_id_hier_activity"])
        .unique(subset=["exercise_id"], keep="first")
    )


def code_preference_score(code: str) -> tuple[int, int, str]:
    """Sort activity codes ahead of objective/module codes when resolving IDs."""
    if ACTIVITY_CODE_RE.match(code):
        return (0, -len(code), code)
    if OBJECTIVE_CODE_RE.match(code):
        return (1, -len(code), code)
    if MODULE_CODE_RE.match(code):
        return (2, -len(code), code)
    return (3, -len(code), code)


def rules_id_code_frame(settings: Settings) -> pl.DataFrame:
    """Map graph IDs from rules metadata to the preferred pedagogical code."""
    rules = load_zpdes_rules(settings.zpdes_rules_path)
    maps = zpdes_code_maps(rules)
    id_to_codes = maps["id_to_codes"]
    rows: list[dict[str, str]] = []
    for graph_id, raw_codes in id_to_codes.items():
        if not isinstance(graph_id, str) or not graph_id.strip():
            continue
        if not isinstance(raw_codes, list):
            continue
        codes = [
            str(code).strip()
            for code in raw_codes
            if isinstance(code, str) and str(code).strip()
        ]
        if not codes:
            continue
        chosen_code = sorted(codes, key=code_preference_score)[0]
        rows.append({"graph_id": graph_id, "graph_code": chosen_code})
    if not rows:
        return pl.DataFrame(
            {"graph_id": [], "graph_code": []},
            schema={"graph_id": pl.Utf8, "graph_code": pl.Utf8},
        )
    return pl.DataFrame(rows).unique(subset=["graph_id"], keep="first")


def exercise_metadata_frame(settings: Settings) -> pl.DataFrame:
    """Return exercise labels/types derived from `exercises.json`."""
    payload = load_exercises(settings.exercises_json_path)
    rows: list[dict[str, object]] = []
    for exercise in payload.get("exercises", []):
        if not isinstance(exercise, dict):
            continue
        exercise_id = str(exercise.get("id") or "").strip()
        if not exercise_id:
            continue
        label = exercise_label_from_instruction(exercise.get("instruction"))
        exercise_type = str(exercise.get("type") or "").strip()
        rows.append(
            {
                "exercise_id": exercise_id,
                "exercise_label_meta": label if label else None,
                "exercise_type": exercise_type if exercise_type else None,
            }
        )
    if not rows:
        return pl.DataFrame(
            {
                "exercise_id": [],
                "exercise_label_meta": [],
                "exercise_type": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "exercise_label_meta": pl.Utf8,
                "exercise_type": pl.Utf8,
            },
        )
    return pl.DataFrame(rows).unique(subset=["exercise_id"], keep="first")


def exercise_catalog_elo_base_frame(settings: Settings) -> pl.DataFrame:
    """Return the catalog-backed base frame for exercise Elo outputs."""
    hierarchy = exercise_hierarchy_map_from_catalog(settings).select(
        [
            "exercise_id",
            pl.col("module_id_exercise_summary").alias("module_id"),
            pl.col("module_code_exercise_summary").alias("module_code"),
            pl.col("module_label_exercise_summary").alias("module_label"),
            pl.col("objective_id_exercise_summary").alias("objective_id"),
            pl.col("objective_label_exercise_summary").alias("objective_label"),
            pl.col("activity_id_exercise_summary").alias("activity_id"),
            pl.col("activity_label_exercise_summary").alias("activity_label"),
        ]
    )
    exercise_meta = exercise_metadata_frame(settings).rename({"exercise_label_meta": "exercise_label"})
    return (
        hierarchy.join(exercise_meta, on="exercise_id", how="left")
        .with_columns(
            pl.coalesce([pl.col("exercise_label"), pl.col("exercise_id")]).alias("exercise_label")
        )
        .unique(subset=["exercise_id"], keep="first")
    )


def catalog_code_frames(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return fallback code-to-id/label frames for module, objective, and activity codes."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    rules = load_zpdes_rules(settings.zpdes_rules_path)
    maps = zpdes_code_maps(rules)
    code_to_id = maps["code_to_id"]
    index_frames = catalog_id_index_frames(catalog)
    index = index_frames.index.select(["id", "short_title", "long_title"]).unique(
        subset=["id"], keep="first"
    )

    rows: list[dict[str, str | None]] = []
    for code, identifier in code_to_id.items():
        if not isinstance(code, str) or not code.strip():
            continue
        if not isinstance(identifier, str) or not identifier.strip():
            continue
        rows.append({"code": code, "id": identifier})

    codes_df = pl.DataFrame(rows) if rows else pl.DataFrame({"code": [], "id": []})
    if "code" in codes_df.columns:
        codes_df = codes_df.with_columns(pl.col("code").cast(pl.Utf8), pl.col("id").cast(pl.Utf8))
    codes_labeled = (
        codes_df.join(index, on="id", how="left")
        .with_columns(
            pl.coalesce([pl.col("short_title"), pl.col("long_title"), pl.col("code")]).alias(
                "label_fallback"
            )
        )
    )

    module_df = (
        codes_labeled.filter(pl.col("code").str.contains(r"^M\d+$"))
        .select(
            [
                pl.col("code").alias("module_code_fallback"),
                pl.col("id").alias("module_id_fallback"),
                pl.col("label_fallback").alias("module_label_fallback"),
            ]
        )
        .unique(subset=["module_code_fallback"], keep="first")
    )
    objective_df = (
        codes_labeled.filter(pl.col("code").str.contains(r"^M\d+O\d+$"))
        .select(
            [
                pl.col("code").alias("objective_code_fallback"),
                pl.col("id").alias("objective_id_fallback"),
                pl.col("label_fallback").alias("objective_label_fallback"),
            ]
        )
        .unique(subset=["objective_code_fallback"], keep="first")
    )
    activity_df = (
        codes_labeled.filter(pl.col("code").str.contains(r"^M\d+O\d+A\d+$"))
        .select(
            [
                pl.col("code").alias("activity_code_fallback"),
                pl.col("id").alias("activity_id_fallback"),
                pl.col("label_fallback").alias("activity_label_fallback"),
            ]
        )
        .unique(subset=["activity_code_fallback"], keep="first")
    )
    return module_df, objective_df, activity_df

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


def _catalog_label_expr(code_col: str, short_col: str, long_col: str) -> pl.Expr:
    """Return a readable label expression for one catalog id-index frame."""
    return pl.coalesce([pl.col(short_col), pl.col(long_col), pl.col(code_col)])


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


def catalog_id_lookup_frames(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return module/objective/activity lookups keyed by raw ids from id_label_index."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    index_frames = catalog_id_index_frames(catalog)

    def _lookup_frame(
        df: pl.DataFrame,
        *,
        key_name: str,
        code_name: str,
        label_name: str,
    ) -> pl.DataFrame:
        if df.height == 0:
            return pl.DataFrame(
                {key_name: [], code_name: [], label_name: []},
                schema={key_name: pl.Utf8, code_name: pl.Utf8, label_name: pl.Utf8},
            )
        return (
            df.select(
                [
                    pl.col("id").alias(key_name),
                    pl.col("code").alias(code_name),
                    _catalog_label_expr("code", "short_title", "long_title").alias(label_name),
                ]
            )
            .unique(subset=[key_name], keep="first")
        )

    return (
        _lookup_frame(
            index_frames.modules,
            key_name="module_id_lookup",
            code_name="module_code_lookup",
            label_name="module_label_lookup",
        ),
        _lookup_frame(
            index_frames.objectives,
            key_name="objective_id_lookup",
            code_name="objective_code_lookup",
            label_name="objective_label_lookup",
        ),
        _lookup_frame(
            index_frames.activities,
            key_name="activity_id_lookup",
            code_name="activity_code_lookup",
            label_name="activity_label_lookup",
        ),
    )


def exercise_hierarchy_map_from_catalog(settings: Settings) -> pl.DataFrame:
    """Return the exercise-to-hierarchy frame used for playlist backfill and Elo."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    module_lookup, objective_lookup, activity_lookup = catalog_id_lookup_frames(settings)
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

    return (
        map_df.join(
            module_lookup.rename(
                {
                    "module_id_lookup": "module_id_exercise_summary",
                    "module_code_lookup": "module_code_exercise_summary",
                    "module_label_lookup": "module_label_exercise_summary",
                }
            ),
            on="module_id_exercise_summary",
            how="left",
        )
        .join(
            objective_lookup.rename(
                {
                    "objective_id_lookup": "objective_id_exercise_summary",
                    "objective_code_lookup": "objective_code_exercise_summary",
                    "objective_label_lookup": "objective_label_exercise_summary",
                }
            ),
            on="objective_id_exercise_summary",
            how="left",
        )
        .join(
            activity_lookup.rename(
                {
                    "activity_id_lookup": "activity_id_exercise_summary",
                    "activity_code_lookup": "activity_code_exercise_summary",
                    "activity_label_lookup": "activity_label_exercise_summary",
                }
            ),
            on="activity_id_exercise_summary",
            how="left",
        )
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
    rules = load_zpdes_rules(settings.build_zpdes_rules_path)
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


def exercise_catalog_elo_context_frame(settings: Settings) -> pl.DataFrame:
    """Return one catalog-backed exercise row per module/objective/activity context."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    rows: list[dict[str, str | None]] = []
    for module in catalog.get("modules", []):
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("id") or "").strip() or None
        module_code = str(module.get("code") or "").strip() or None
        if not module_code:
            continue
        module_title = module.get("title") or {}
        module_label = (
            str(module_title.get("short") or module_title.get("long") or module_code).strip() or module_code
        )
        for objective in module.get("objectives", []):
            if not isinstance(objective, dict):
                continue
            objective_id = str(objective.get("id") or "").strip() or None
            objective_code = str(objective.get("code") or "").strip() or None
            objective_title = objective.get("title") or {}
            objective_label = (
                str(objective_title.get("short") or objective_title.get("long") or objective_code or "").strip()
                or objective_code
            )
            for activity in objective.get("activities", []):
                if not isinstance(activity, dict):
                    continue
                activity_id = str(activity.get("id") or "").strip() or None
                activity_code = str(activity.get("code") or "").strip() or None
                activity_title = activity.get("title") or {}
                activity_label = (
                    str(activity_title.get("short") or activity_title.get("long") or activity_code or "").strip()
                    or activity_code
                )
                exercise_ids = activity.get("exercise_ids") or []
                if not isinstance(exercise_ids, list):
                    continue
                for exercise_id in exercise_ids:
                    exercise_key = str(exercise_id or "").strip()
                    if not exercise_key:
                        continue
                    rows.append(
                        {
                            "exercise_id": exercise_key,
                            "module_id": module_id,
                            "module_code": module_code,
                            "module_label": module_label,
                            "objective_id": objective_id,
                            "objective_label": objective_label,
                            "activity_id": activity_id,
                            "activity_label": activity_label,
                        }
                    )

    if not rows:
        return pl.DataFrame(
            {
                "exercise_id": [],
                "module_id": [],
                "module_code": [],
                "module_label": [],
                "objective_id": [],
                "objective_label": [],
                "activity_id": [],
                "activity_label": [],
                "exercise_label": [],
                "exercise_type": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "module_id": pl.Utf8,
                "module_code": pl.Utf8,
                "module_label": pl.Utf8,
                "objective_id": pl.Utf8,
                "objective_label": pl.Utf8,
                "activity_id": pl.Utf8,
                "activity_label": pl.Utf8,
                "exercise_label": pl.Utf8,
                "exercise_type": pl.Utf8,
            },
        )

    exercise_meta = exercise_metadata_frame(settings).rename({"exercise_label_meta": "exercise_label"})
    return (
        pl.DataFrame(rows)
        .unique(
            subset=[
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
            ],
            keep="first",
        )
        .join(exercise_meta, on="exercise_id", how="left")
        .with_columns(
            pl.coalesce([pl.col("exercise_label"), pl.col("exercise_id")]).alias("exercise_label")
        )
    )


def catalog_activity_rank_frame(settings: Settings) -> pl.DataFrame:
    """Return the canonical module-local activity order from the learning catalog.

    The rank is a dense sequential index within each module. It is used for
    analyses that need a stable notion of "earlier" and "later" activity
    positions independent of ZPDES edge directions.
    """
    catalog = load_learning_catalog(settings.learning_catalog_path)
    rows: list[dict[str, object]] = []
    for module in catalog.get("modules", []):
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("id") or "").strip() or None
        module_code = str(module.get("code") or "").strip()
        if not module_code:
            continue
        module_title = module.get("title") or {}
        module_label = str(module_title.get("short") or module_title.get("long") or module_code).strip()
        destination_rank = 0
        for objective in module.get("objectives", []):
            if not isinstance(objective, dict):
                continue
            objective_id = str(objective.get("id") or "").strip() or None
            objective_code = str(objective.get("code") or "").strip()
            if not objective_code:
                continue
            objective_title = objective.get("title") or {}
            objective_label = str(
                objective_title.get("short") or objective_title.get("long") or objective_code
            ).strip()
            for activity in objective.get("activities", []):
                if not isinstance(activity, dict):
                    continue
                activity_id = str(activity.get("id") or "").strip()
                activity_code = str(activity.get("code") or "").strip()
                if not activity_id or not activity_code:
                    continue
                activity_title = activity.get("title") or {}
                activity_label = str(
                    activity_title.get("short") or activity_title.get("long") or activity_code
                ).strip()
                destination_rank += 1
                rows.append(
                    {
                        "module_id": module_id,
                        "module_code": module_code,
                        "module_label": module_label,
                        "objective_id": objective_id,
                        "objective_code": objective_code,
                        "objective_label": objective_label,
                        "activity_id": activity_id,
                        "activity_code": activity_code,
                        "activity_label": activity_label,
                        "destination_rank": destination_rank,
                    }
                )
    if not rows:
        return pl.DataFrame(
            {
                "module_id": [],
                "module_code": [],
                "module_label": [],
                "objective_id": [],
                "objective_code": [],
                "objective_label": [],
                "activity_id": [],
                "activity_code": [],
                "activity_label": [],
                "destination_rank": [],
            },
            schema={
                "module_id": pl.Utf8,
                "module_code": pl.Utf8,
                "module_label": pl.Utf8,
                "objective_id": pl.Utf8,
                "objective_code": pl.Utf8,
                "objective_label": pl.Utf8,
                "activity_id": pl.Utf8,
                "activity_code": pl.Utf8,
                "activity_label": pl.Utf8,
                "destination_rank": pl.Int64,
            },
        )
    return pl.DataFrame(rows).unique(subset=["activity_id"], keep="first")


def catalog_code_frames(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return fallback code-to-id/label frames for module, objective, and activity codes."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    rules = load_zpdes_rules(settings.build_zpdes_rules_path)
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

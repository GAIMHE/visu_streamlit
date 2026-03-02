from __future__ import annotations

import html
import math
import re
from pathlib import Path

import polars as pl

from .config import Settings, ensure_artifact_directories
from .contracts import REQUIRED_AGG_COLUMNS, REQUIRED_FACT_COLUMNS
from .loaders import (
    catalog_id_index_frames,
    catalog_to_summary_frames,
    load_exercises,
    load_learning_catalog,
    load_zpdes_rules,
    zpdes_code_maps,
)
from .transitions import build_transition_edges_from_fact

_ID_PLACEHOLDER_VALUES_LOWER = {"", "none", "null", "nan"}
_MODULE_CODE_RE = re.compile(r"^M\d+$")
_OBJECTIVE_CODE_RE = re.compile(r"^M\d+O\d+$")
_ACTIVITY_CODE_RE = re.compile(r"^M\d+O\d+A\d+$")
_ELO_BASE_RATING = 1500.0
_ELO_SCALE = 400.0
_ELO_K = 24.0


def _as_lazy(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return df.lazy() if isinstance(df, pl.DataFrame) else df


def _normalized_id_expr(column_name: str) -> pl.Expr:
    normalized = pl.col(column_name).cast(pl.Utf8).str.strip_chars()
    return (
        pl.when(
            normalized.is_null()
            | normalized.str.to_lowercase().is_in(list(_ID_PLACEHOLDER_VALUES_LOWER))
        )
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(normalized)
        .alias(column_name)
    )


def _hierarchy_map_from_catalog(settings: Settings) -> pl.DataFrame:
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


def _exercise_hierarchy_map_from_catalog(settings: Settings) -> pl.DataFrame:
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


def _code_preference_score(code: str) -> tuple[int, int, str]:
    if _ACTIVITY_CODE_RE.match(code):
        return (0, -len(code), code)
    if _OBJECTIVE_CODE_RE.match(code):
        return (1, -len(code), code)
    if _MODULE_CODE_RE.match(code):
        return (2, -len(code), code)
    return (3, -len(code), code)


def _rules_id_code_frame(settings: Settings) -> pl.DataFrame:
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
        chosen_code = sorted(codes, key=_code_preference_score)[0]
        rows.append({"graph_id": graph_id, "graph_code": chosen_code})
    if not rows:
        return pl.DataFrame(
            {"graph_id": [], "graph_code": []},
            schema={"graph_id": pl.Utf8, "graph_code": pl.Utf8},
        )
    return pl.DataFrame(rows).unique(subset=["graph_id"], keep="first")


def _strip_html(raw: str) -> str:
    text = html.unescape(raw or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _exercise_label_from_instruction(instruction: object) -> str:
    if isinstance(instruction, dict):
        raw_html = instruction.get("$html")
        if isinstance(raw_html, str):
            return _strip_html(raw_html)
    if isinstance(instruction, str):
        return _strip_html(instruction)
    return ""


def _exercise_metadata_frame(settings: Settings) -> pl.DataFrame:
    payload = load_exercises(settings.exercises_json_path)
    rows: list[dict[str, object]] = []
    for exercise in payload.get("exercises", []):
        if not isinstance(exercise, dict):
            continue
        exercise_id = str(exercise.get("id") or "").strip()
        if not exercise_id:
            continue
        label = _exercise_label_from_instruction(exercise.get("instruction"))
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


def _elo_expected_success(student_rating: float, exercise_rating: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((exercise_rating - student_rating) / _ELO_SCALE))


def _outcome_value(raw: object) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return 1.0 if raw else 0.0
    try:
        numeric = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return 1.0 if numeric > 0.0 else 0.0


def _exercise_catalog_elo_base_frame(settings: Settings) -> pl.DataFrame:
    hierarchy = _exercise_hierarchy_map_from_catalog(settings).select(
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
    exercise_meta = _exercise_metadata_frame(settings).rename(
        {"exercise_label_meta": "exercise_label"}
    )
    base = (
        hierarchy.join(exercise_meta, on="exercise_id", how="left")
        .with_columns(
            pl.coalesce([pl.col("exercise_label"), pl.col("exercise_id")]).alias("exercise_label")
        )
        .unique(subset=["exercise_id"], keep="first")
    )
    return base


def _catalog_code_frames(settings: Settings) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
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


def build_fact_attempt_core(settings: Settings, sample_rows: int | None = None) -> pl.DataFrame:
    hierarchy = _hierarchy_map_from_catalog(settings)
    exercise_hierarchy = _exercise_hierarchy_map_from_catalog(settings)
    graph_id_code = _rules_id_code_frame(settings)
    module_code_df, objective_code_df, activity_code_df = _catalog_code_frames(settings)
    lf = pl.scan_parquet(settings.parquet_path)
    if sample_rows is not None:
        lf = lf.limit(sample_rows)

    graph_by_activity = graph_id_code.rename({"graph_id": "activity_id", "graph_code": "graph_code_activity"})
    graph_by_objective = graph_id_code.rename(
        {"graph_id": "objective_id", "graph_code": "graph_code_objective"}
    )
    graph_by_exercise = graph_id_code.rename({"graph_id": "exercise_id", "graph_code": "graph_code_exercise"})
    graph_by_playlist = graph_id_code.rename(
        {"graph_id": "playlist_or_module_id", "graph_code": "graph_code_playlist"}
    )

    fact = (
        lf.with_columns(
            [
                _normalized_id_expr("playlist_or_module_id"),
                _normalized_id_expr("objective_id"),
                _normalized_id_expr("activity_id"),
                _normalized_id_expr("exercise_id"),
            ]
        )
        .join(hierarchy.lazy(), on="activity_id", how="left")
        .join(exercise_hierarchy.lazy(), on="exercise_id", how="left")
        .join(graph_by_activity.lazy(), on="activity_id", how="left")
        .join(graph_by_objective.lazy(), on="objective_id", how="left")
        .join(graph_by_exercise.lazy(), on="exercise_id", how="left")
        .join(graph_by_playlist.lazy(), on="playlist_or_module_id", how="left")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("graph_code_activity"),
                    pl.col("graph_code_objective"),
                    pl.col("graph_code_exercise"),
                    pl.col("graph_code_playlist"),
                ]
            ).alias("fallback_code_raw")
        )
        .with_columns(
            pl.col("fallback_code_raw")
            .cast(pl.Utf8)
            .str.extract(r"^(M\d+)", 1)
            .alias("module_code_fallback"),
            pl.col("fallback_code_raw")
            .cast(pl.Utf8)
            .str.extract(r"^(M\d+O\d+)", 1)
            .alias("objective_code_fallback"),
            pl.col("fallback_code_raw")
            .cast(pl.Utf8)
            .str.extract(r"^(M\d+O\d+A\d+)", 1)
            .alias("activity_code_fallback"),
        )
        .join(module_code_df.lazy(), on="module_code_fallback", how="left")
        .join(objective_code_df.lazy(), on="objective_code_fallback", how="left")
        .join(activity_code_df.lazy(), on="activity_code_fallback", how="left")
        .with_columns(
            pl.col("module_code").is_not_null().alias("has_activity_catalog_mapping"),
            pl.col("module_code_exercise_summary").is_not_null().alias("has_exercise_catalog_mapping"),
            pl.coalesce(
                [
                    pl.col("objective_id_summary"),
                    pl.col("objective_id_exercise_summary"),
                    pl.col("objective_id_fallback"),
                ]
            ).alias("objective_id_summary"),
            pl.coalesce(
                [
                    pl.col("activity_id"),
                    pl.col("activity_id_exercise_summary"),
                    pl.col("activity_id_fallback"),
                ]
            ).alias("activity_id"),
            pl.coalesce(
                [
                    pl.col("objective_id"),
                    pl.col("objective_id_summary"),
                    pl.col("objective_id_exercise_summary"),
                    pl.col("objective_id_fallback"),
                ]
            ).alias("objective_id"),
        )
        .with_columns(
            pl.coalesce(
                [
                    pl.col("module_code"),
                    pl.col("module_code_exercise_summary"),
                    pl.col("module_code_fallback"),
                ]
            ).alias("module_code"),
            pl.coalesce(
                [
                    pl.col("module_id"),
                    pl.col("module_id_exercise_summary"),
                    pl.col("module_id_fallback"),
                ]
            ).alias("module_id"),
            pl.coalesce(
                [
                    pl.col("module_label"),
                    pl.col("module_label_exercise_summary"),
                    pl.col("module_label_fallback"),
                ]
            ).alias("module_label"),
            pl.coalesce(
                [
                    pl.col("objective_label"),
                    pl.col("objective_label_exercise_summary"),
                    pl.col("objective_label_fallback"),
                ]
            ).alias("objective_label"),
            pl.coalesce(
                [
                    pl.col("activity_label"),
                    pl.col("activity_label_exercise_summary"),
                    pl.col("activity_label_fallback"),
                ]
            ).alias("activity_label"),
            pl.when(pl.col("has_activity_catalog_mapping"))
            .then(pl.lit("catalog_activity"))
            .when(pl.col("has_exercise_catalog_mapping"))
            .then(pl.lit("catalog_exercise"))
            .when(pl.col("module_code_fallback").is_not_null())
            .then(pl.lit("rules_code_fallback"))
            .otherwise(pl.lit("unmapped"))
            .alias("mapping_source"),
        )
        .with_columns(pl.col("created_at").dt.date().alias("date_utc"))
        .select(
            [
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
            ]
        )
        .collect()
    )
    return fact


def build_agg_activity_daily_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return (
        _as_lazy(fact)
        .with_columns(
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.col("data_correct").cast(pl.Float64))
            .otherwise(None)
            .alias("first_attempt_correct_value"),
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("first_attempt_flag"),
        )
        .group_by(
            [
                "date_utc",
                "activity_id",
                "activity_label",
                "objective_id",
                "objective_label",
                "module_id",
                "module_code",
                "module_label",
            ]
        )
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").n_unique().alias("unique_students"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("first_attempt_correct_value").mean().alias("first_attempt_success_rate"),
            pl.col("first_attempt_flag").sum().alias("first_attempt_count"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
            pl.col("attempt_number").mean().alias("avg_attempt_number"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_objective_daily_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return (
        _as_lazy(fact)
        .group_by(["date_utc", "objective_id", "objective_label", "module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").n_unique().alias("unique_students"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_student_module_progress_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    return (
        _as_lazy(fact)
        .group_by(["date_utc", "user_id", "module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("activity_id").n_unique().alias("unique_activities"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("attempt_number").mean().alias("avg_attempt_number"),
            pl.col("created_at").max().alias("last_attempt_at"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_module_usage_daily_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return (
        _as_lazy(fact)
        .group_by(["date_utc", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_playlist_module_usage_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return (
        _as_lazy(fact)
        .with_columns(
            pl.col("module_code")
            .is_not_null()
            .any()
            .over("playlist_or_module_id")
            .alias("playlist_has_mapped_module")
        )
        .filter(
            ~(pl.col("playlist_has_mapped_module") & pl.col("module_code").is_null())
        )
        .group_by(["playlist_or_module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
            pl.col("classroom_id").drop_nulls().n_unique().alias("unique_classrooms"),
            pl.col("activity_id").drop_nulls().n_unique().alias("unique_activities"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("work_mode")
            .drop_nulls()
            .n_unique()
            .alias("work_mode_unique_count"),
            pl.col("work_mode")
            .drop_nulls()
            .first()
            .alias("work_mode_first"),
        )
        .with_columns(
            pl.when(pl.col("work_mode_unique_count") == 0)
            .then(pl.lit("unknown"))
            .when(pl.col("work_mode_unique_count") == 1)
            .then(pl.col("work_mode_first"))
            .otherwise(pl.lit("mixed"))
            .alias("work_mode")
        )
        .drop(["work_mode_unique_count", "work_mode_first"])
        .sort(["module_code", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_module_activity_usage_from_fact(fact: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return (
        _as_lazy(fact)
        .group_by(["module_code", "module_label", "activity_id", "activity_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
        )
        .with_columns(
            (pl.col("attempts") / pl.col("attempts").sum().over("module_code")).alias(
                "activity_share_within_module"
            )
        )
        .sort(["module_code", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_exercise_daily_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    exercise_meta = _exercise_metadata_frame(settings)
    return (
        _as_lazy(fact)
        .with_columns(
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.col("data_correct").cast(pl.Float64))
            .otherwise(None)
            .alias("first_attempt_correct_value"),
            pl.when(pl.col("attempt_number") == 1)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("first_attempt_flag"),
        )
        .join(exercise_meta.lazy(), on="exercise_id", how="left")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("exercise_label_meta").cast(pl.Utf8),
                    pl.col("exercise_id").cast(pl.Utf8),
                ]
            ).alias("exercise_label")
        )
        .group_by(
            [
                "date_utc",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "exercise_id",
                "exercise_label",
                "exercise_type",
            ]
        )
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").n_unique().alias("unique_students"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("first_attempt_correct_value").mean().alias("first_attempt_success_rate"),
            pl.col("first_attempt_flag").sum().alias("first_attempt_count"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
            pl.col("attempt_number").mean().alias("avg_attempt_number"),
        )
        .sort(["date_utc", "attempts"], descending=[False, True])
        .collect()
    )


def build_agg_exercise_elo_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    base = _exercise_catalog_elo_base_frame(settings)
    if base.height == 0:
        return pl.DataFrame(
            {
                "exercise_id": [],
                "exercise_label": [],
                "exercise_type": [],
                "module_id": [],
                "module_code": [],
                "module_label": [],
                "objective_id": [],
                "objective_label": [],
                "activity_id": [],
                "activity_label": [],
                "exercise_elo": [],
                "calibration_attempts": [],
                "calibration_success_rate": [],
                "calibrated": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "exercise_label": pl.Utf8,
                "exercise_type": pl.Utf8,
                "module_id": pl.Utf8,
                "module_code": pl.Utf8,
                "module_label": pl.Utf8,
                "objective_id": pl.Utf8,
                "objective_label": pl.Utf8,
                "activity_id": pl.Utf8,
                "activity_label": pl.Utf8,
                "exercise_elo": pl.Float64,
                "calibration_attempts": pl.Int64,
                "calibration_success_rate": pl.Float64,
                "calibrated": pl.Boolean,
            },
        )

    valid_exercise_ids = base["exercise_id"].to_list()
    calibration = (
        _as_lazy(fact)
        .filter(
            (pl.col("attempt_number") == 1)
            & pl.col("created_at").is_not_null()
            & pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("data_correct").is_not_null()
            & pl.col("exercise_id").cast(pl.Utf8).is_in(valid_exercise_ids)
        )
        .select(["created_at", "user_id", "exercise_id", "data_correct"])
        .sort(["created_at", "user_id", "exercise_id"])
        .collect()
    )

    student_ratings: dict[str, float] = {}
    exercise_ratings: dict[str, float] = {}
    exercise_attempts: dict[str, int] = {}
    exercise_successes: dict[str, float] = {}

    for created_at, user_id, exercise_id, raw_outcome in calibration.iter_rows():
        if created_at is None:
            continue
        user_key = str(user_id).strip()
        exercise_key = str(exercise_id).strip()
        if not user_key or not exercise_key:
            continue
        outcome = _outcome_value(raw_outcome)
        if outcome is None:
            continue
        student_rating = student_ratings.get(user_key, _ELO_BASE_RATING)
        exercise_rating = exercise_ratings.get(exercise_key, _ELO_BASE_RATING)
        expected = _elo_expected_success(student_rating, exercise_rating)
        delta = _ELO_K * (outcome - expected)
        student_ratings[user_key] = student_rating + delta
        exercise_ratings[exercise_key] = exercise_rating - delta
        exercise_attempts[exercise_key] = exercise_attempts.get(exercise_key, 0) + 1
        exercise_successes[exercise_key] = exercise_successes.get(exercise_key, 0.0) + outcome

    stats_rows = [
        {
            "exercise_id": exercise_id,
            "exercise_elo": float(exercise_ratings[exercise_id]),
            "calibration_attempts": int(exercise_attempts[exercise_id]),
            "calibration_success_rate": float(exercise_successes[exercise_id]) / float(exercise_attempts[exercise_id]),
            "calibrated": True,
        }
        for exercise_id in exercise_attempts.keys()
    ]
    stats = (
        pl.DataFrame(stats_rows)
        if stats_rows
        else pl.DataFrame(
            {
                "exercise_id": [],
                "exercise_elo": [],
                "calibration_attempts": [],
                "calibration_success_rate": [],
                "calibrated": [],
            },
            schema={
                "exercise_id": pl.Utf8,
                "exercise_elo": pl.Float64,
                "calibration_attempts": pl.Int64,
                "calibration_success_rate": pl.Float64,
                "calibrated": pl.Boolean,
            },
        )
    )

    return (
        base.join(stats, on="exercise_id", how="left")
        .with_columns(
            pl.col("calibrated").fill_null(False),
            pl.col("calibration_attempts").fill_null(0).cast(pl.Int64),
            pl.when(pl.col("calibrated"))
            .then(pl.col("exercise_elo"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("exercise_elo"),
            pl.when(pl.col("calibrated"))
            .then(pl.col("calibration_success_rate"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("calibration_success_rate"),
            pl.coalesce([pl.col("exercise_type"), pl.lit("unknown")]).alias("exercise_type"),
        )
        .select(
            [
                "exercise_id",
                "exercise_label",
                "exercise_type",
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
                "exercise_elo",
                "calibration_attempts",
                "calibration_success_rate",
                "calibrated",
            ]
        )
        .sort(["module_code", "objective_id", "activity_id", "exercise_id"])
    )


def build_agg_activity_elo_from_exercise_elo(
    exercise_elo: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
) -> pl.DataFrame:
    del settings
    return (
        _as_lazy(exercise_elo)
        .group_by(
            [
                "module_id",
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
            ]
        )
        .agg(
            pl.col("exercise_elo").drop_nulls().mean().alias("activity_mean_exercise_elo"),
            pl.col("calibrated").cast(pl.Int64).sum().alias("calibrated_exercise_count"),
            pl.len().cast(pl.Int64).alias("catalog_exercise_count"),
        )
        .with_columns(
            pl.when(pl.col("catalog_exercise_count") > 0)
            .then(
                pl.col("calibrated_exercise_count").cast(pl.Float64)
                / pl.col("catalog_exercise_count").cast(pl.Float64)
            )
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("calibration_coverage_ratio")
        )
        .sort(["module_code", "objective_id", "activity_id"])
        .collect()
    )


def build_student_elo_events_from_fact(
    fact: pl.DataFrame | pl.LazyFrame,
    exercise_elo: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    exercise_frame = _as_lazy(exercise_elo).filter(
        pl.col("calibrated") & pl.col("exercise_elo").is_not_null()
    )
    exercise_map_df = (
        exercise_frame.select(["exercise_id", "exercise_elo"])
        .collect()
        .unique(subset=["exercise_id"], keep="first")
    )
    if exercise_map_df.height == 0:
        return pl.DataFrame(
            {
                "user_id": [],
                "attempt_ordinal": [],
                "created_at": [],
                "date_utc": [],
                "work_mode": [],
                "module_code": [],
                "objective_id": [],
                "activity_id": [],
                "exercise_id": [],
                "outcome": [],
                "expected_success": [],
                "exercise_elo": [],
                "student_elo_pre": [],
                "student_elo_post": [],
            },
            schema={
                "user_id": pl.Utf8,
                "attempt_ordinal": pl.Int64,
                "created_at": pl.Datetime,
                "date_utc": pl.Date,
                "work_mode": pl.Utf8,
                "module_code": pl.Utf8,
                "objective_id": pl.Utf8,
                "activity_id": pl.Utf8,
                "exercise_id": pl.Utf8,
                "outcome": pl.Float64,
                "expected_success": pl.Float64,
                "exercise_elo": pl.Float64,
                "student_elo_pre": pl.Float64,
                "student_elo_post": pl.Float64,
            },
        )

    valid_exercise_ids = exercise_map_df["exercise_id"].to_list()
    exercise_elo_map = {
        str(row["exercise_id"]): float(row["exercise_elo"])
        for row in exercise_map_df.to_dicts()
    }

    replay = (
        _as_lazy(fact)
        .filter(
            pl.col("created_at").is_not_null()
            & pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("data_correct").is_not_null()
            & pl.col("exercise_id").cast(pl.Utf8).is_in(valid_exercise_ids)
        )
        .select(
            [
                "user_id",
                "created_at",
                "date_utc",
                "work_mode",
                "module_code",
                "objective_id",
                "activity_id",
                "exercise_id",
                "attempt_number",
                "data_correct",
            ]
        )
        .sort(["user_id", "created_at", "exercise_id", "attempt_number"])
        .collect()
    )

    rows: list[dict[str, object]] = []
    current_user: str | None = None
    current_rating = _ELO_BASE_RATING
    current_ordinal = 0

    for (
        user_id,
        created_at,
        date_utc,
        work_mode,
        module_code,
        objective_id,
        activity_id,
        exercise_id,
        _attempt_number,
        raw_outcome,
    ) in replay.iter_rows():
        user_key = str(user_id).strip()
        exercise_key = str(exercise_id).strip()
        if not user_key or not exercise_key:
            continue
        outcome = _outcome_value(raw_outcome)
        if outcome is None:
            continue
        if current_user != user_key:
            current_user = user_key
            current_rating = _ELO_BASE_RATING
            current_ordinal = 0
        exercise_rating = exercise_elo_map.get(exercise_key)
        if exercise_rating is None:
            continue
        current_ordinal += 1
        student_elo_pre = current_rating
        expected_success = _elo_expected_success(student_elo_pre, exercise_rating)
        delta = _ELO_K * (outcome - expected_success)
        student_elo_post = student_elo_pre + delta
        current_rating = student_elo_post
        rows.append(
            {
                "user_id": user_key,
                "attempt_ordinal": current_ordinal,
                "created_at": created_at,
                "date_utc": date_utc,
                "work_mode": None if work_mode is None else str(work_mode),
                "module_code": None if module_code is None else str(module_code),
                "objective_id": None if objective_id is None else str(objective_id),
                "activity_id": None if activity_id is None else str(activity_id),
                "exercise_id": exercise_key,
                "outcome": outcome,
                "expected_success": expected_success,
                "exercise_elo": exercise_rating,
                "student_elo_pre": student_elo_pre,
                "student_elo_post": student_elo_post,
            }
        )

    if not rows:
        return pl.DataFrame(
            {
                "user_id": [],
                "attempt_ordinal": [],
                "created_at": [],
                "date_utc": [],
                "work_mode": [],
                "module_code": [],
                "objective_id": [],
                "activity_id": [],
                "exercise_id": [],
                "outcome": [],
                "expected_success": [],
                "exercise_elo": [],
                "student_elo_pre": [],
                "student_elo_post": [],
            }
        )
    return pl.DataFrame(rows).sort(["user_id", "attempt_ordinal"])


def build_student_elo_profiles_from_events(
    events: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame:
    return (
        _as_lazy(events)
        .sort(["user_id", "attempt_ordinal"])
        .group_by("user_id")
        .agg(
            pl.len().cast(pl.Int64).alias("total_attempts"),
            pl.col("created_at").min().alias("first_attempt_at"),
            pl.col("created_at").max().alias("last_attempt_at"),
            pl.col("module_code").drop_nulls().n_unique().cast(pl.Int64).alias("unique_modules"),
            pl.col("objective_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_objectives"),
            pl.col("activity_id").drop_nulls().n_unique().cast(pl.Int64).alias("unique_activities"),
            pl.col("student_elo_post").last().alias("final_student_elo"),
        )
        .with_columns(
            (pl.col("total_attempts") > 0).alias("eligible_for_replay")
        )
        .sort(["total_attempts", "final_student_elo", "user_id"], descending=[True, True, False])
        .collect()
    )


def _validate_required_columns(df: pl.DataFrame, required: list[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def write_derived_tables(settings: Settings, sample_rows: int | None = None) -> dict[str, Path]:
    ensure_artifact_directories(settings)
    outputs = {
        "fact_attempt_core": settings.artifacts_derived_dir / "fact_attempt_core.parquet",
        "agg_activity_daily": settings.artifacts_derived_dir / "agg_activity_daily.parquet",
        "agg_objective_daily": settings.artifacts_derived_dir / "agg_objective_daily.parquet",
        "agg_student_module_progress": settings.artifacts_derived_dir
        / "agg_student_module_progress.parquet",
        "agg_transition_edges": settings.artifacts_derived_dir / "agg_transition_edges.parquet",
        "agg_module_usage_daily": settings.artifacts_derived_dir / "agg_module_usage_daily.parquet",
        "agg_playlist_module_usage": settings.artifacts_derived_dir
        / "agg_playlist_module_usage.parquet",
        "agg_module_activity_usage": settings.artifacts_derived_dir
        / "agg_module_activity_usage.parquet",
        "agg_exercise_daily": settings.artifacts_derived_dir / "agg_exercise_daily.parquet",
        "agg_exercise_elo": settings.artifacts_derived_dir / "agg_exercise_elo.parquet",
        "agg_activity_elo": settings.artifacts_derived_dir / "agg_activity_elo.parquet",
        "student_elo_events": settings.artifacts_derived_dir / "student_elo_events.parquet",
        "student_elo_profiles": settings.artifacts_derived_dir / "student_elo_profiles.parquet",
    }

    fact = build_fact_attempt_core(settings, sample_rows=sample_rows)
    _validate_required_columns(fact, REQUIRED_FACT_COLUMNS, "fact_attempt_core")

    agg_activity = build_agg_activity_daily_from_fact(fact)
    agg_objective = build_agg_objective_daily_from_fact(fact)
    agg_student_module = build_agg_student_module_progress_from_fact(fact)
    agg_transition = build_transition_edges_from_fact(fact)
    agg_module_usage_daily = build_agg_module_usage_daily_from_fact(fact)
    agg_playlist_module_usage = build_agg_playlist_module_usage_from_fact(fact)
    agg_module_activity_usage = build_agg_module_activity_usage_from_fact(fact)
    agg_exercise_daily = build_agg_exercise_daily_from_fact(fact, settings=settings)
    agg_exercise_elo = build_agg_exercise_elo_from_fact(fact, settings=settings)
    agg_activity_elo = build_agg_activity_elo_from_exercise_elo(agg_exercise_elo, settings=settings)
    student_elo_events = build_student_elo_events_from_fact(fact, agg_exercise_elo)
    student_elo_profiles = build_student_elo_profiles_from_events(student_elo_events)

    _validate_required_columns(
        agg_activity, REQUIRED_AGG_COLUMNS["agg_activity_daily"], "agg_activity_daily"
    )
    _validate_required_columns(
        agg_objective, REQUIRED_AGG_COLUMNS["agg_objective_daily"], "agg_objective_daily"
    )
    _validate_required_columns(
        agg_student_module,
        REQUIRED_AGG_COLUMNS["agg_student_module_progress"],
        "agg_student_module_progress",
    )
    _validate_required_columns(
        agg_transition, REQUIRED_AGG_COLUMNS["agg_transition_edges"], "agg_transition_edges"
    )
    _validate_required_columns(
        agg_module_usage_daily,
        REQUIRED_AGG_COLUMNS["agg_module_usage_daily"],
        "agg_module_usage_daily",
    )
    _validate_required_columns(
        agg_playlist_module_usage,
        REQUIRED_AGG_COLUMNS["agg_playlist_module_usage"],
        "agg_playlist_module_usage",
    )
    _validate_required_columns(
        agg_module_activity_usage,
        REQUIRED_AGG_COLUMNS["agg_module_activity_usage"],
        "agg_module_activity_usage",
    )
    _validate_required_columns(
        agg_exercise_daily,
        REQUIRED_AGG_COLUMNS["agg_exercise_daily"],
        "agg_exercise_daily",
    )
    _validate_required_columns(
        agg_exercise_elo,
        REQUIRED_AGG_COLUMNS["agg_exercise_elo"],
        "agg_exercise_elo",
    )
    _validate_required_columns(
        agg_activity_elo,
        REQUIRED_AGG_COLUMNS["agg_activity_elo"],
        "agg_activity_elo",
    )
    _validate_required_columns(
        student_elo_events,
        REQUIRED_AGG_COLUMNS["student_elo_events"],
        "student_elo_events",
    )
    _validate_required_columns(
        student_elo_profiles,
        REQUIRED_AGG_COLUMNS["student_elo_profiles"],
        "student_elo_profiles",
    )

    fact.write_parquet(outputs["fact_attempt_core"])
    agg_activity.write_parquet(outputs["agg_activity_daily"])
    agg_objective.write_parquet(outputs["agg_objective_daily"])
    agg_student_module.write_parquet(outputs["agg_student_module_progress"])
    agg_transition.write_parquet(outputs["agg_transition_edges"])
    agg_module_usage_daily.write_parquet(outputs["agg_module_usage_daily"])
    agg_playlist_module_usage.write_parquet(outputs["agg_playlist_module_usage"])
    agg_module_activity_usage.write_parquet(outputs["agg_module_activity_usage"])
    agg_exercise_daily.write_parquet(outputs["agg_exercise_daily"])
    agg_exercise_elo.write_parquet(outputs["agg_exercise_elo"])
    agg_activity_elo.write_parquet(outputs["agg_activity_elo"])
    student_elo_events.write_parquet(outputs["student_elo_events"])
    student_elo_profiles.write_parquet(outputs["student_elo_profiles"])

    return outputs

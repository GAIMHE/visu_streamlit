"""Source-specific runtime input materialization for local builds."""

from __future__ import annotations

import csv
import json
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .config import Settings, ensure_artifact_directories
from .runtime_sources import get_runtime_source

UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

_MAUREEN_RESEARCHER_HEADERS: tuple[str, ...] = (
    "UAI",
    "classroom_id",
    "teacher_id",
    "user_id",
    "playlist_or_module_id",
    "objective_id",
    "activity_id",
    "exercise_id",
    "module_short_title",
    "module_long_title",
    "created_at",
    "login_time",
    "is_initial_test",
    "data_score",
    "data_correct",
    "data_nb_tries",
    "data_test_context",
    "data_answer",
    "data_duration",
    "session_duration",
    "work_mode",
)


@dataclass(frozen=True)
class SourceMaterializationReport:
    """Summary of one source-local input materialization run."""

    source_id: str
    input_paths: tuple[str, ...]
    warnings: tuple[str, ...] = ()


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _strip_trailing_semicolon(value: object) -> str:
    return str(value or "").strip().removesuffix(";").strip()


def _optional_text_expr(column: str) -> pl.Expr:
    normalized = pl.col(column).cast(pl.Utf8).str.strip_chars()
    return (
        pl.when(normalized.is_null() | normalized.is_in(["", "None", "none", "NULL", "null"]))
        .then(None)
        .otherwise(normalized)
    )


def _bool_expr(column: str) -> pl.Expr:
    normalized = pl.col(column).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return (
        pl.when(normalized == "true")
        .then(pl.lit(True))
        .when(normalized == "false")
        .then(pl.lit(False))
        .otherwise(None)
    )


def _code_sort_key(code: str | None) -> tuple[int, ...]:
    parts = re.findall(r"\d+", str(code or ""))
    if not parts:
        return (0,)
    return tuple(int(part) for part in parts)


def _parse_uuid_list(raw: object) -> list[str]:
    return [match.group(0).lower() for match in UUID_RE.finditer(str(raw or ""))]


def _copy_file(source_path: Path, destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)


def _load_maureen_config_rows(path: Path) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for raw_row in reader:
            row = {str(key).strip(): _clean_text(value) for key, value in raw_row.items()}
            rows.append(row)
    return rows


def _repair_maureen_researcher_row(raw_line: str, expected_length: int) -> list[str] | None:
    text = raw_line.rstrip("\r\n")
    if not text:
        return None
    text = _strip_trailing_semicolon(text)
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    left = text.split(",", 17)
    if len(left) != 18:
        return None
    head = left[:17]
    remainder = left[17]
    right = remainder.rsplit(",", 3)
    if len(right) != 4:
        return None
    data_answer, data_duration, session_duration, work_mode = right
    repaired = [*head, data_answer, data_duration, session_duration, work_mode]
    if len(repaired) != expected_length:
        return None
    return repaired


def _load_maureen_researcher_attempts(path: Path) -> tuple[pl.DataFrame, tuple[str, ...]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        header_line = handle.readline()
        if not header_line:
            raise ValueError("Maureen researcher CSV is empty.")
        parsed_header = [
            _strip_trailing_semicolon(value)
            for value in next(csv.reader([header_line], delimiter=","))
        ]
        if tuple(parsed_header) != _MAUREEN_RESEARCHER_HEADERS:
            raise ValueError(
                "Maureen researcher CSV has an unexpected header. "
                f"Expected {list(_MAUREEN_RESEARCHER_HEADERS)}, got {parsed_header}."
            )

        rows: list[dict[str, str | None]] = []
        repaired_rows = 0
        skipped_rows = 0
        for raw_line in handle:
            if not raw_line.strip():
                continue
            parsed = next(csv.reader([raw_line], delimiter=","))
            if len(parsed) != len(parsed_header):
                repaired = _repair_maureen_researcher_row(raw_line, len(parsed_header))
                if repaired is None:
                    skipped_rows += 1
                    continue
                parsed = repaired
                repaired_rows += 1
            normalized = {
                parsed_header[idx]: _clean_text(_strip_trailing_semicolon(parsed[idx]))
                for idx in range(len(parsed_header))
            }
            rows.append(normalized)

    attempts = pl.DataFrame(rows) if rows else pl.DataFrame(schema={name: pl.Utf8 for name in parsed_header})
    attempts = attempts.with_columns(
        _optional_text_expr("UAI").alias("UAI"),
        _optional_text_expr("classroom_id").alias("classroom_id"),
        _optional_text_expr("teacher_id").alias("teacher_id"),
        _optional_text_expr("user_id").alias("user_id"),
        _optional_text_expr("playlist_or_module_id").alias("playlist_or_module_id"),
        _optional_text_expr("objective_id").alias("objective_id"),
        _optional_text_expr("activity_id").alias("activity_id"),
        _optional_text_expr("exercise_id").alias("exercise_id"),
        _optional_text_expr("module_short_title").alias("module_short_title"),
        _optional_text_expr("module_long_title").alias("module_long_title"),
        _optional_text_expr("data_test_context").alias("data_test_context"),
        _optional_text_expr("work_mode").alias("work_mode"),
        _bool_expr("data_correct").alias("data_correct"),
        _bool_expr("is_initial_test").alias("is_initial_test"),
        pl.col("data_score").cast(pl.Float64, strict=False).alias("data_score"),
        pl.col("data_nb_tries").cast(pl.Int64, strict=False).alias("data_nb_tries"),
        pl.col("data_duration").cast(pl.Float64, strict=False).alias("data_duration"),
        pl.col("session_duration").cast(pl.Float64, strict=False).alias("session_duration"),
        pl.col("created_at")
        .map_elements(_parse_created_at, return_dtype=pl.Datetime(time_zone="UTC"))
        .alias("created_at"),
        pl.col("login_time")
        .map_elements(_parse_created_at, return_dtype=pl.Datetime(time_zone="UTC"))
        .alias("login_time"),
    ).with_columns(
        pl.coalesce([pl.col("work_mode"), pl.col("data_test_context")]).alias("work_mode"),
    )

    warnings: list[str] = []
    if repaired_rows > 0:
        warnings.append(f"Repaired {repaired_rows} malformed row(s) in the Maureen researcher CSV.")
    if skipped_rows > 0:
        warnings.append(f"Skipped {skipped_rows} row(s) that could not be repaired in the Maureen researcher CSV.")
    return attempts, tuple(warnings)


def _parse_created_at(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _activity_prefix(code: str | None) -> str | None:
    text = str(code or "").strip()
    match = re.match(r"^(M\d+O\d+)A\d+$", text)
    return match.group(1) if match else None


def _next_code(base_prefix: str, used_codes: set[str]) -> str:
    used_numbers = [
        int(match.group(1))
        for code in used_codes
        for match in [re.match(re.escape(base_prefix) + r"A(\d+)$", code)]
        if match is not None
    ]
    next_idx = max(used_numbers, default=0) + 1
    return f"{base_prefix}A{next_idx}"


def _build_maureen_catalog_and_raw(
    attempts_csv_path: Path,
    module_config_csv_path: Path,
) -> tuple[pl.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any], tuple[str, ...]]:
    config_rows = _load_maureen_config_rows(module_config_csv_path)
    attempts, parse_warnings = _load_maureen_researcher_attempts(attempts_csv_path)

    module_row = next(
        (row for row in config_rows if str(row.get("type") or "").strip().lower() == "module"),
        None,
    )
    if module_row is None:
        raise ValueError("Maureen module config is missing a module row.")

    module_id = str(module_row.get("id") or "").strip()
    module_code = str(module_row.get("code") or "").strip() or "M0"
    module_short = str(module_row.get("short_title") or module_code).strip()
    module_long = str(module_row.get("long_title") or module_short).strip()
    attempts = attempts.with_columns(
        pl.coalesce(
            [
                pl.col("playlist_or_module_id"),
                pl.lit(module_id, dtype=pl.Utf8),
            ]
        ).alias("module_id"),
        pl.coalesce(
            [
                pl.col("module_long_title"),
                pl.lit(module_long, dtype=pl.Utf8),
            ]
        ).alias("module_long_title"),
    ).sort(["user_id", "created_at", "exercise_id"])

    objective_rows = [
        row
        for row in config_rows
        if str(row.get("type") or "").strip().lower() == "objective"
    ]
    activity_rows = [
        row
        for row in config_rows
        if str(row.get("type") or "").strip().lower() == "activity"
    ]

    objectives_by_id: dict[str, dict[str, Any]] = {}
    objectives_by_code: dict[str, dict[str, Any]] = {}
    for row in objective_rows:
        objective_id = str(row.get("id") or "").strip()
        objective_code = str(row.get("code") or "").strip()
        if not objective_id or not objective_code:
            continue
        payload = {
            "id": objective_id,
            "code": objective_code,
            "short_title": str(row.get("short_title") or objective_code).strip(),
            "long_title": str(row.get("long_title") or str(row.get("short_title") or objective_code)).strip(),
            "status": str(row.get("status") or "visible").strip().lower(),
            "prerequisites": str(row.get("prerequisites") or "").strip(),
            "activity_ids": [],
        }
        objectives_by_id[objective_id] = payload
        objectives_by_code[objective_code] = payload

    activities_by_id: dict[str, dict[str, Any]] = {}
    objective_codes_used_by_activity: defaultdict[str, set[str]] = defaultdict(set)
    for row in activity_rows:
        activity_id = str(row.get("id") or "").strip()
        activity_code = str(row.get("code") or "").strip()
        if not activity_id or not activity_code:
            continue
        objective_code = _activity_prefix(activity_code)
        if objective_code is None or objective_code not in objectives_by_code:
            continue
        payload = {
            "id": activity_id,
            "code": activity_code,
            "objective_code": objective_code,
            "short_title": str(row.get("short_title") or activity_code).strip(),
            "long_title": str(row.get("long_title") or str(row.get("short_title") or activity_code)).strip(),
            "status": str(row.get("status") or "visible").strip().lower(),
            "exercise_ids": _parse_uuid_list(row.get("learning_items")),
            "synthetic": False,
        }
        activities_by_id[activity_id] = payload
        objectives_by_code[objective_code]["activity_ids"].append(activity_id)
        objective_codes_used_by_activity[objective_code].add(activity_code)

    attempt_rows = attempts.select(
        [
            "module_id",
            "objective_id",
            "activity_id",
            "exercise_id",
            "created_at",
        ]
    ).to_dicts()
    attempt_exercises_by_activity: defaultdict[str, list[str]] = defaultdict(list)
    attempt_objective_by_activity: dict[str, str] = {}
    objective_ids_seen_in_attempts: set[str] = set()
    warnings: list[str] = list(parse_warnings)
    for row in attempt_rows:
        objective_id = str(row.get("objective_id") or "").strip()
        activity_id = str(row.get("activity_id") or "").strip()
        exercise_id = str(row.get("exercise_id") or "").strip()
        if objective_id:
            objective_ids_seen_in_attempts.add(objective_id)
        if not activity_id:
            continue
        if objective_id and activity_id not in attempt_objective_by_activity:
            attempt_objective_by_activity[activity_id] = objective_id
        if exercise_id and exercise_id not in attempt_exercises_by_activity[activity_id]:
            attempt_exercises_by_activity[activity_id].append(exercise_id)

    config_exercise_ids = {
        exercise_id
        for activity_payload in activities_by_id.values()
        for exercise_id in activity_payload["exercise_ids"]
    }
    max_objective_code = max(
        [
            _code_sort_key(payload["code"])[1]
            for payload in objectives_by_id.values()
            if len(_code_sort_key(payload["code"])) > 1
        ],
        default=0,
    )
    for objective_id in sorted(objective_ids_seen_in_attempts):
        if objective_id in objectives_by_id:
            continue
        max_objective_code += 1
        synthetic_code = f"{module_code}O{max_objective_code}"
        payload = {
            "id": objective_id,
            "code": synthetic_code,
            "short_title": f"Unmapped objective {objective_id[:8]}",
            "long_title": f"Unmapped objective {objective_id}",
            "status": "synthetic",
            "prerequisites": "",
            "activity_ids": [],
        }
        objectives_by_id[objective_id] = payload
        objectives_by_code[synthetic_code] = payload
        warnings.append(f"Added synthetic objective for unmatched attempt objective_id={objective_id}.")

    for activity_id, objective_id in sorted(attempt_objective_by_activity.items()):
        if activity_id in activities_by_id:
            continue
        objective_payload = objectives_by_id.get(objective_id)
        if objective_payload is None:
            continue
        objective_code = str(objective_payload["code"])
        synthetic_code = _next_code(objective_code, objective_codes_used_by_activity[objective_code])
        objective_codes_used_by_activity[objective_code].add(synthetic_code)
        payload = {
            "id": activity_id,
            "code": synthetic_code,
            "objective_code": objective_code,
            "short_title": f"Unmapped activity {activity_id[:8]}",
            "long_title": f"Unmapped activity {activity_id}",
            "status": "synthetic",
            "exercise_ids": list(attempt_exercises_by_activity.get(activity_id, [])),
            "synthetic": True,
        }
        activities_by_id[activity_id] = payload
        objective_payload["activity_ids"].append(activity_id)
        warnings.append(f"Added synthetic activity for unmatched attempt activity_id={activity_id}.")

    for activity_id, payload in activities_by_id.items():
        existing = list(payload["exercise_ids"])
        for exercise_id in attempt_exercises_by_activity.get(activity_id, []):
            if exercise_id not in existing:
                existing.append(exercise_id)
        payload["exercise_ids"] = existing

    ordered_objectives = sorted(objectives_by_id.values(), key=lambda row: _code_sort_key(str(row["code"])))
    modules_payload: list[dict[str, Any]] = []
    exercise_to_hierarchy: dict[str, dict[str, str]] = {}
    id_label_index: dict[str, dict[str, Any]] = {
        module_id: {
            "type": "module",
            "code": module_code,
            "short_title": module_short,
            "long_title": module_long,
            "sources": ["maureen_config"],
        }
    }
    orphan_activity_ids: list[str] = []

    module_objectives: list[dict[str, Any]] = []
    for objective_payload in ordered_objectives:
        objective_id = str(objective_payload["id"])
        objective_code = str(objective_payload["code"])
        objective_short = str(objective_payload["short_title"])
        objective_long = str(objective_payload["long_title"])
        id_label_index[objective_id] = {
            "type": "objective",
            "code": objective_code,
            "short_title": objective_short,
            "long_title": objective_long,
            "sources": ["maureen_config"],
        }
        activity_entries: list[dict[str, Any]] = []
        ordered_activity_ids = sorted(
            list(objective_payload["activity_ids"]),
            key=lambda activity_id: _code_sort_key(str(activities_by_id[activity_id]["code"])),
        )
        for activity_id in ordered_activity_ids:
            activity_payload = activities_by_id[activity_id]
            activity_code = str(activity_payload["code"])
            activity_short = str(activity_payload["short_title"])
            activity_long = str(activity_payload["long_title"])
            id_label_index[activity_id] = {
                "type": "activity",
                "code": activity_code,
                "short_title": activity_short,
                "long_title": activity_long,
                "sources": ["maureen_config"] if not activity_payload.get("synthetic") else ["maureen_attempts"],
            }
            if activity_payload.get("synthetic"):
                orphan_activity_ids.append(activity_id)
            exercise_ids = list(activity_payload["exercise_ids"])
            for exercise_id in exercise_ids:
                if exercise_id not in id_label_index:
                    id_label_index[exercise_id] = {
                        "type": "exercise",
                        "code": None,
                        "short_title": None,
                        "long_title": None,
                        "sources": ["maureen_config"] if exercise_id in config_exercise_ids else ["maureen_attempts"],
                    }
                exercise_to_hierarchy[exercise_id] = {
                    "module_id": module_id,
                    "objective_id": objective_id,
                    "activity_id": activity_id,
                }
            activity_entries.append(
                {
                    "id": activity_id,
                    "code": activity_code,
                    "status": str(activity_payload["status"] or "visible"),
                    "title": {
                        "short": activity_short,
                        "long": activity_long,
                    },
                    "exercise_ids": exercise_ids,
                }
            )
        module_objectives.append(
            {
                "id": objective_id,
                "code": objective_code,
                "status": str(objective_payload["status"] or "visible"),
                "title": {
                    "short": objective_short,
                    "long": objective_long,
                },
                "activities": activity_entries,
            }
        )

    modules_payload.append(
        {
            "id": module_id,
            "code": module_code,
            "status": str(module_row.get("status") or "visible"),
            "title": {
                "short": module_short,
                "long": module_long,
            },
            "objectives": module_objectives,
        }
    )

    learning_catalog = {
        "meta": {
            "generated_by": "visu2 maureen adapter",
            "source_id": "maureen_m16fr",
            "build_timestamp_utc": datetime.now(UTC).isoformat(),
            "version": "maureen_runtime_v1",
            "source_files": [str(module_config_csv_path), str(attempts_csv_path)],
            "counts": {
                "modules": len(modules_payload),
                "objectives": len(ordered_objectives),
                "activities": len(activities_by_id),
                "exercises_unique": len(exercise_to_hierarchy),
            },
        },
        "conflicts": {
            "coverage": {
                "overlapping_activity_count": len([aid for aid, payload in activities_by_id.items() if payload["exercise_ids"]]),
                "primary_only_activity_count": 0,
                "secondary_only_activity_count": len(orphan_activity_ids),
                "overlap_membership_disagreement_count": 0,
            },
            "missing_references": {
                "missing_activity_ids_in_objectives": [],
                "missing_objective_ids_in_modules": [],
            },
            "source_disagreements": {
                "activity_exercise_membership_disagreements": [],
                "id_label_disagreements": [],
            },
            "secondary_mapping_candidates_for_orphans": {
                "unique_count": len(orphan_activity_ids),
                "ambiguous_count": 0,
            },
        },
        "orphans": [{"activity_id": activity_id} for activity_id in orphan_activity_ids],
        "id_label_index": id_label_index,
        "modules": modules_payload,
        "exercise_to_hierarchy": exercise_to_hierarchy,
    }

    code_to_id: dict[str, str] = {module_code: module_id}
    id_to_codes: defaultdict[str, list[str]] = defaultdict(list)
    id_to_codes[module_id].append(module_code)
    for objective_payload in ordered_objectives:
        code_to_id[str(objective_payload["code"])] = str(objective_payload["id"])
        id_to_codes[str(objective_payload["id"])] = [str(objective_payload["code"])]
    for activity_payload in activities_by_id.values():
        code_to_id[str(activity_payload["code"])] = str(activity_payload["id"])
        id_to_codes[str(activity_payload["id"])] = [str(activity_payload["code"])]

    zpdes_rules = {
        "meta": {
            "generated_by": "visu2 maureen adapter",
            "source_id": "maureen_m16fr",
            "build_timestamp_utc": datetime.now(UTC).isoformat(),
            "source_files": [str(module_config_csv_path)],
        },
        "module_rules": [
            {
                "module_id": module_id,
                "module_code": module_code,
                "dependency_topology": "branched_dag",
                "objective_rules": [
                    {
                        "objective_id": str(objective_payload["id"]),
                        "objective_code": str(objective_payload["code"]),
                        "prerequisites": str(objective_payload.get("prerequisites") or ""),
                    }
                    for objective_payload in ordered_objectives
                ],
            }
        ],
        "map_id_code": {
            "code_to_id": code_to_id,
            "id_to_codes": dict(id_to_codes),
        },
        "links_to_catalog": {
            "module_ids_linked": [module_id],
            "objective_ids_linked": [str(payload["id"]) for payload in ordered_objectives],
            "activity_ids_linked": sorted(activities_by_id),
        },
        "unresolved_links": {
            "rule_ids_missing_in_catalog": [],
            "catalog_module_ids_missing_in_rules": [],
            "codes_with_multiple_ids": {},
            "ids_with_multiple_codes": {},
        },
    }

    exercises_json = {
        "exercises": [
            {
                "id": exercise_id,
                "type": None,
                "instruction": None,
                "activities": [mapping["activity_id"]],
                "objectives": [mapping["objective_id"]],
                "modules": [mapping["module_id"]],
            }
            for exercise_id, mapping in sorted(exercise_to_hierarchy.items())
        ]
    }

    raw_attempts = (
        attempts.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("variation"),
            pl.lit(None, dtype=pl.Float64).alias("progression_score"),
            pl.lit(None, dtype=pl.Float64).alias("initial_test_max_success"),
            pl.lit(None, dtype=pl.Float64).alias("initial_test_weighted_max_success"),
            pl.lit(None, dtype=pl.Float64).alias("initial_test_success_rate"),
            pl.lit(None, dtype=pl.Float64).alias("finished_module_mean_score"),
            pl.lit(None, dtype=pl.Float64).alias("finished_module_graphe_coverage_rate"),
            pl.lit(None, dtype=pl.Boolean).alias("is_gar"),
        )
        .sort(["user_id", "created_at", "exercise_id"])
        .with_columns(
            pl.col("exercise_id").cum_count().over(["user_id", "exercise_id"]).alias("attempt_number"),
            pl.col("user_id").cum_count().over("user_id").alias("student_attempt_index"),
        )
        .select(
            [
                "user_id",
                "variation",
                "module_id",
                "objective_id",
                "activity_id",
                "exercise_id",
                "created_at",
                "login_time",
                "data_score",
                "data_correct",
                "work_mode",
                "data_test_context",
                "progression_score",
                "initial_test_max_success",
                "initial_test_weighted_max_success",
                "initial_test_success_rate",
                "finished_module_mean_score",
                "finished_module_graphe_coverage_rate",
                "is_gar",
                "teacher_id",
                "classroom_id",
                "playlist_or_module_id",
                "data_duration",
                "session_duration",
                "attempt_number",
                "student_attempt_index",
                "module_long_title",
            ]
        )
    )

    return raw_attempts, learning_catalog, zpdes_rules, exercises_json, tuple(sorted(set(warnings)))


def materialize_source_runtime_inputs(settings: Settings) -> SourceMaterializationReport:
    """Materialize one source into the source-local runtime data directory."""
    ensure_artifact_directories(settings)
    source = get_runtime_source(settings.source_id)

    if source.build_profile == "main":
        copied_paths: list[str] = []
        raw_parquet_src = settings.root_dir / source.raw_inputs["parquet"]
        learning_catalog_src = settings.root_dir / source.raw_inputs["learning_catalog"]
        zpdes_rules_src = settings.root_dir / source.raw_inputs["zpdes_rules"]
        exercises_src = settings.root_dir / source.raw_inputs["exercises"]
        for src_path, dst_path in [
            (raw_parquet_src, settings.parquet_path),
            (learning_catalog_src, settings.learning_catalog_path),
            (zpdes_rules_src, settings.zpdes_rules_path),
            (exercises_src, settings.exercises_json_path),
        ]:
            _copy_file(src_path, dst_path)
            copied_paths.append(str(dst_path))
        return SourceMaterializationReport(
            source_id=settings.source_id,
            input_paths=tuple(copied_paths),
        )

    if source.build_profile == "maureen":
        attempts_csv_path = settings.root_dir / source.raw_inputs["attempts_csv"]
        module_config_csv_path = settings.root_dir / source.raw_inputs["module_config_csv"]
        raw_attempts, learning_catalog, zpdes_rules, exercises_json, warnings = _build_maureen_catalog_and_raw(
            attempts_csv_path,
            module_config_csv_path,
        )
        raw_attempts.write_parquet(settings.parquet_path)
        settings.learning_catalog_path.write_text(
            json.dumps(learning_catalog, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        settings.zpdes_rules_path.write_text(
            json.dumps(zpdes_rules, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        settings.exercises_json_path.write_text(
            json.dumps(exercises_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return SourceMaterializationReport(
            source_id=settings.source_id,
            input_paths=(
                str(settings.parquet_path),
                str(settings.learning_catalog_path),
                str(settings.zpdes_rules_path),
                str(settings.exercises_json_path),
            ),
            warnings=warnings,
        )

    raise ValueError(f"Unsupported source build profile: {source.build_profile}")

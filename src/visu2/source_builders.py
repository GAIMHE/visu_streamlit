"""Source-specific runtime input materialization for local builds."""

from __future__ import annotations

import csv
import json
import re
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .config import Settings, ensure_artifact_directories
from .runtime_sources import get_runtime_source

_CSV_FIELD_SIZE_LIMIT = sys.maxsize
while True:
    try:
        csv.field_size_limit(_CSV_FIELD_SIZE_LIMIT)
        break
    except OverflowError:
        _CSV_FIELD_SIZE_LIMIT = int(_CSV_FIELD_SIZE_LIMIT / 10)

UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

NEURIPS_MODULE_CODE_BY_ID: dict[str, str] = {
    "63e98e5f-94e3-4630-9704-076882d6de38": "M1",
    "14fe4ca0-8fff-4c4a-bad2-6ef051eee349": "M31",
    "8ff53d40-9b1f-44c8-8646-f699fed002e9": "M32",
    "27709aa2-b055-4ed3-ac73-8dca783b4afe": "M33",
    "053df3ec-5501-4ad8-9917-a935bcf76740": "M101",
    "14321a7e-4ef7-4b6a-9ff8-99329e08d7a2": "M105",
    "d840c0c0-3e48-11f1-8e68-975e7ffdd3c5": "M999",
}

_RESEARCHER_REQUIRED_HEADERS: tuple[str, ...] = (
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
_RESEARCHER_OPTIONAL_HEADERS: tuple[str, ...] = (
    "progression_score",
    "initial_test_max_success",
    "initial_test_weighted_max_success",
    "initial_test_success_rate",
    "finished_module_mean_score",
    "finished_module_graphe_coverage_rate",
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
    if expected_length != len(_RESEARCHER_REQUIRED_HEADERS):
        return None
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
        required_missing = [name for name in _RESEARCHER_REQUIRED_HEADERS if name not in parsed_header]
        unexpected = [
            name
            for name in parsed_header
            if name not in set(_RESEARCHER_REQUIRED_HEADERS) | set(_RESEARCHER_OPTIONAL_HEADERS)
        ]
        if required_missing or unexpected:
            raise ValueError(
                "Maureen researcher CSV has an unexpected header. "
                f"Missing required headers={required_missing}, unexpected headers={unexpected}, got {parsed_header}."
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

    attempts = pl.DataFrame(
        rows,
        schema={name: pl.Utf8 for name in parsed_header},
    ) if rows else pl.DataFrame(schema={name: pl.Utf8 for name in parsed_header})
    missing_optional_headers = [name for name in _RESEARCHER_OPTIONAL_HEADERS if name not in attempts.columns]
    if missing_optional_headers:
        attempts = attempts.with_columns(
            [pl.lit(None, dtype=pl.Utf8).alias(name) for name in missing_optional_headers]
        )
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
        pl.col("progression_score").cast(pl.Float64, strict=False).alias("progression_score"),
        pl.col("initial_test_max_success").cast(pl.Float64, strict=False).alias("initial_test_max_success"),
        pl.col("initial_test_weighted_max_success")
        .cast(pl.Float64, strict=False)
        .alias("initial_test_weighted_max_success"),
        pl.col("initial_test_success_rate").cast(pl.Float64, strict=False).alias("initial_test_success_rate"),
        pl.col("finished_module_mean_score").cast(pl.Float64, strict=False).alias("finished_module_mean_score"),
        pl.col("finished_module_graphe_coverage_rate")
        .cast(pl.Float64, strict=False)
        .alias("finished_module_graphe_coverage_rate"),
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


def _module_prefix(code: str | None) -> str | None:
    text = str(code or "").strip()
    match = re.match(r"^(M\d+)", text)
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


def _next_objective_code(module_code: str, used_codes: set[str]) -> str:
    used_numbers = [
        int(match.group(1))
        for code in used_codes
        for match in [re.match(re.escape(module_code) + r"O(\d+)$", code)]
        if match is not None
    ]
    next_idx = max(used_numbers, default=0) + 1
    return f"{module_code}O{next_idx}"


def _choose_unique_value(
    values: list[str | None],
    *,
    label: str,
    default: str,
    warnings: list[str],
) -> str:
    normalized = sorted({str(value).strip() for value in values if str(value or "").strip()})
    if not normalized:
        return default
    if len(normalized) > 1:
        warnings.append(
            f"Multiple {label} values found; using the first in sorted order: {normalized[0]!r}."
    )
    return normalized[0]


def _json_title_pair(payload: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None
    title = payload.get("title")
    if not isinstance(title, dict):
        return None, None
    short_title = _clean_text(title.get("short"))
    long_title = _clean_text(title.get("long"))
    return short_title, long_title


def _load_single_module_researcher_config_payload(
    config_json_path: Path | None,
    *,
    required: bool = False,
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    if config_json_path is None:
        if required:
            raise ValueError("Single-module researcher topology requires a config_json_path.")
        return None, ()
    if not config_json_path.exists():
        if required:
            raise ValueError(f"MIA config file not found at {config_json_path}.")
        return None, (f"MIA config file not found at {config_json_path}; using synthetic labels.",)

    try:
        payload = json.loads(config_json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        if required:
            raise ValueError(f"Failed to parse MIA config JSON at {config_json_path}: {err}.") from err
        return None, (
            f"Failed to parse MIA config JSON at {config_json_path}: {err}. Using synthetic labels.",
        )

    config = payload.get("config")
    if not isinstance(config, dict):
        if required:
            raise ValueError(
                f"MIA config JSON at {config_json_path} is missing a top-level 'config' object."
            )
        return None, (
            f"MIA config JSON at {config_json_path} is missing a top-level 'config' object; using synthetic labels.",
        )
    return config, ()


def _load_single_module_researcher_config_metadata(
    config_json_path: Path | None,
    *,
    module_id: str,
) -> tuple[dict[str, Any] | None, dict[str, dict[str, Any]], dict[str, dict[str, Any]], tuple[str, ...]]:
    config, warnings = _load_single_module_researcher_config_payload(config_json_path)
    if config is None:
        return None, {}, {}, warnings

    module_entries = config.get("module")
    objective_entries = config.get("objective")
    activity_entries = config.get("activity")
    if (
        not isinstance(module_entries, dict)
        or not isinstance(objective_entries, dict)
        or not isinstance(activity_entries, dict)
    ):
        return None, {}, {}, (
            f"MIA config JSON at {config_json_path} is missing one of module/objective/activity maps; using synthetic labels.",
        )

    module_entry = next(
        (
            value
            for value in module_entries.values()
            if isinstance(value, dict) and _clean_text(value.get("id")) == module_id
        ),
        None,
    )
    if module_entry is None:
        return None, {}, {}, (
            f"MIA config JSON at {config_json_path} does not define module_id {module_id}; using synthetic labels.",
        )

    module_short, module_long = _json_title_pair(module_entry)
    module_code = _clean_text(module_entry.get("code"))
    module_metadata = {
        "code": module_code,
        "short_title": module_short,
        "long_title": module_long,
        "status": _clean_text(module_entry.get("visibilityStatus")) or "visible",
        "source_files": [str(config_json_path)],
    }

    objective_metadata_by_id: dict[str, dict[str, Any]] = {}
    for value in objective_entries.values():
        if not isinstance(value, dict):
            continue
        objective_id = _clean_text(value.get("id"))
        if objective_id is None:
            continue
        short_title, long_title = _json_title_pair(value)
        objective_metadata_by_id[objective_id] = {
            "code": _clean_text(value.get("code")),
            "short_title": short_title,
            "long_title": long_title,
            "status": _clean_text(value.get("visibilityStatus")) or "visible",
        }

    activity_metadata_by_id: dict[str, dict[str, Any]] = {}
    for value in activity_entries.values():
        if not isinstance(value, dict):
            continue
        activity_id = _clean_text(value.get("id"))
        if activity_id is None:
            continue
        short_title, long_title = _json_title_pair(value)
        activity_metadata_by_id[activity_id] = {
            "code": _clean_text(value.get("code")),
            "short_title": short_title,
            "long_title": long_title,
            "status": _clean_text(value.get("visibilityStatus")) or "visible",
        }

    return module_metadata, objective_metadata_by_id, activity_metadata_by_id, ()


def _load_multi_module_researcher_config_metadata(
    config_json_path: Path | None,
    *,
    required: bool = False,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, str]],
    tuple[str, ...],
]:
    config, warnings = _load_single_module_researcher_config_payload(
        config_json_path,
        required=required,
    )
    if config is None:
        return {}, {}, {}, {}, warnings

    module_entries = config.get("module")
    objective_entries = config.get("objective")
    activity_entries = config.get("activity")
    if (
        not isinstance(module_entries, dict)
        or not isinstance(objective_entries, dict)
        or not isinstance(activity_entries, dict)
    ):
        if required:
            raise ValueError(
                f"MIA config JSON at {config_json_path} is missing one of module/objective/activity maps."
            )
        return {}, {}, {}, {}, (
            f"MIA config JSON at {config_json_path} is missing one of module/objective/activity maps; using synthetic labels.",
        )

    modules_by_id: dict[str, dict[str, Any]] = {}
    module_id_by_code: dict[str, str] = {}
    for value in module_entries.values():
        if not isinstance(value, dict):
            continue
        module_id = _clean_text(value.get("id"))
        module_code = _clean_text(value.get("code"))
        if module_id is None or module_code is None:
            continue
        short_title, long_title = _json_title_pair(value)
        metadata = {
            "id": module_id,
            "code": module_code,
            "short_title": short_title or module_code,
            "long_title": long_title or (short_title or module_code),
            "status": _clean_text(value.get("visibilityStatus")) or "visible",
            "source_files": [str(config_json_path)] if config_json_path is not None else [],
        }
        modules_by_id[module_id] = metadata
        module_id_by_code[module_code] = module_id

    objectives_by_id: dict[str, dict[str, Any]] = {}
    objective_id_by_code: dict[str, str] = {}
    for value in objective_entries.values():
        if not isinstance(value, dict):
            continue
        objective_id = _clean_text(value.get("id"))
        objective_code = _clean_text(value.get("code"))
        module_code = _module_prefix(objective_code)
        module_id = module_id_by_code.get(module_code or "")
        if objective_id is None or objective_code is None or module_id is None:
            continue
        short_title, long_title = _json_title_pair(value)
        metadata = {
            "id": objective_id,
            "code": objective_code,
            "module_id": module_id,
            "module_code": module_code,
            "short_title": short_title or objective_code,
            "long_title": long_title or (short_title or objective_code),
            "status": _clean_text(value.get("visibilityStatus")) or "visible",
            "activity_ids": [],
        }
        objectives_by_id[objective_id] = metadata
        objective_id_by_code[objective_code] = objective_id

    activities_by_id: dict[str, dict[str, Any]] = {}
    exercise_to_hierarchy: dict[str, dict[str, str]] = {}
    for value in activity_entries.values():
        if not isinstance(value, dict):
            continue
        activity_id = _clean_text(value.get("id"))
        activity_code = _clean_text(value.get("code"))
        objective_code = _activity_prefix(activity_code)
        objective_id = objective_id_by_code.get(objective_code or "")
        objective_metadata = objectives_by_id.get(objective_id or "")
        if activity_id is None or activity_code is None or objective_metadata is None:
            continue
        short_title, long_title = _json_title_pair(value)
        exercise_ids = [
            str(exercise_id).strip()
            for exercise_id in (value.get("learning_items") or [])
            if str(exercise_id or "").strip()
        ]
        metadata = {
            "id": activity_id,
            "code": activity_code,
            "objective_id": objective_id,
            "objective_code": objective_code,
            "module_id": str(objective_metadata["module_id"]),
            "module_code": str(objective_metadata["module_code"]),
            "short_title": short_title or activity_code,
            "long_title": long_title or (short_title or activity_code),
            "status": _clean_text(value.get("visibilityStatus")) or "visible",
            "exercise_ids": exercise_ids,
            "synthetic": False,
        }
        activities_by_id[activity_id] = metadata
        objective_metadata["activity_ids"].append(activity_id)
        for exercise_id in exercise_ids:
            exercise_to_hierarchy[exercise_id] = {
                "module_id": str(objective_metadata["module_id"]),
                "objective_id": str(objective_id),
                "activity_id": activity_id,
            }

    return modules_by_id, objectives_by_id, activities_by_id, exercise_to_hierarchy, ()


def _index_list(value: object) -> list[int]:
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], list):
        value = value[0]
    if not isinstance(value, list):
        return []
    out: list[int] = []
    for item in value:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _ordered_ids_from_subgroup(
    payload: dict[str, Any] | None,
    fallback_ids: list[str],
) -> list[str]:
    if not isinstance(payload, dict):
        return fallback_ids
    subgroups = payload.get("subgroups")
    if isinstance(subgroups, list) and subgroups and isinstance(subgroups[0], list):
        raw_ids = [str(item).strip() for item in subgroups[0] if str(item or "").strip()]
        filtered = [item for item in raw_ids if item in set(fallback_ids)]
        if filtered:
            return filtered
    return fallback_ids


def _build_single_module_researcher_zpdes_rules(
    *,
    source_id: str,
    module_id: str,
    learning_catalog: dict[str, Any],
    config_json_path: Path,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    config, _warnings = _load_single_module_researcher_config_payload(config_json_path, required=True)
    assert config is not None  # guarded by required=True

    ai = config.get("ai")
    module_config = ai.get("moduleConfig") if isinstance(ai, dict) else None
    if not isinstance(module_config, dict):
        raise ValueError(
            f"MIA config JSON at {config_json_path} is missing config.ai.moduleConfig."
        )
    module_rules_payload = module_config.get(module_id)
    if not isinstance(module_rules_payload, dict):
        raise ValueError(
            f"MIA config JSON at {config_json_path} is missing config.ai.moduleConfig[{module_id!r}]."
        )

    catalog_module = next(
        (
            module
            for module in learning_catalog.get("modules", [])
            if isinstance(module, dict) and _clean_text(module.get("id")) == module_id
        ),
        None,
    )
    if not isinstance(catalog_module, dict):
        raise ValueError(f"learning_catalog does not contain module_id {module_id!r}.")

    module_code = _clean_text(catalog_module.get("code"))
    if module_code is None:
        raise ValueError(f"learning_catalog module {module_id!r} is missing a code.")

    objective_by_id: dict[str, dict[str, Any]] = {}
    activity_by_id: dict[str, dict[str, Any]] = {}
    activity_order_by_objective: dict[str, list[str]] = {}
    objective_order_fallback: list[str] = []
    for objective in catalog_module.get("objectives", []):
        if not isinstance(objective, dict):
            continue
        objective_id = _clean_text(objective.get("id"))
        if objective_id is None:
            continue
        objective_by_id[objective_id] = objective
        objective_order_fallback.append(objective_id)
        fallback_activity_ids: list[str] = []
        for activity in objective.get("activities", []):
            if not isinstance(activity, dict):
                continue
            activity_id = _clean_text(activity.get("id"))
            if activity_id is None:
                continue
            activity_by_id[activity_id] = activity
            fallback_activity_ids.append(activity_id)
        activity_order_by_objective[objective_id] = fallback_activity_ids

    module_level_payload = module_rules_payload.get(module_id)
    if not isinstance(module_level_payload, dict):
        raise ValueError(
            f"MIA config JSON at {config_json_path} is missing the module-level entry for {module_id!r}."
        )

    objective_order_ids = _ordered_ids_from_subgroup(module_level_payload, objective_order_fallback)
    objective_order_codes = [
        str(objective_by_id[objective_id]["code"])
        for objective_id in objective_order_ids
        if objective_id in objective_by_id
    ]
    initial_objective_indexes = set(_index_list(module_level_payload.get("init_ssb")))
    initial_objective_codes = {
        objective_order_codes[idx]
        for idx in initial_objective_indexes
        if 0 <= idx < len(objective_order_codes)
    }

    nodes: list[dict[str, object]] = []
    code_to_id: dict[str, str] = {}
    id_to_codes: defaultdict[str, list[str]] = defaultdict(list)

    def _register_code(identifier: str | None, code: str | None) -> None:
        if not identifier or not code:
            return
        code_to_id[code] = identifier
        id_to_codes[identifier].append(code)

    def _label(payload: dict[str, Any], fallback: str) -> str:
        title = payload.get("title")
        if isinstance(title, dict):
            short = _clean_text(title.get("short"))
            long = _clean_text(title.get("long"))
            if short:
                return short
            if long:
                return long
        return fallback

    _register_code(module_id, module_code)
    objective_activity_codes: dict[str, list[str]] = {}
    objective_rules_summary: list[dict[str, Any]] = []

    for objective_id in objective_order_ids:
        objective = objective_by_id.get(objective_id)
        if not isinstance(objective, dict):
            continue
        objective_code = str(objective.get("code") or "").strip()
        if not objective_code:
            continue
        objective_label = _label(objective, objective_code)
        objective_init = objective_code in initial_objective_codes
        _register_code(objective_id, objective_code)
        nodes.append(
            {
                "module_code": module_code,
                "node_id": objective_id,
                "node_code": objective_code,
                "node_type": "objective",
                "label": objective_label,
                "objective_code": objective_code,
                "activity_index": None,
                "init_open": objective_init,
                "source_primary": "mia_config",
                "source_enrichment": "catalog",
                "is_ghost": False,
            }
        )

        objective_config = module_rules_payload.get(objective_id)
        fallback_activity_ids = activity_order_by_objective.get(objective_id, [])
        ordered_activity_ids = _ordered_ids_from_subgroup(objective_config, fallback_activity_ids)
        ordered_activity_codes: list[str] = []
        initial_activity_indexes = set(_index_list(objective_config.get("init_ssb")) if isinstance(objective_config, dict) else [])
        initial_activity_codes: set[str] = set()
        for idx, activity_id in enumerate(ordered_activity_ids):
            activity = activity_by_id.get(activity_id)
            if not isinstance(activity, dict):
                continue
            activity_code = str(activity.get("code") or "").strip()
            if not activity_code:
                continue
            ordered_activity_codes.append(activity_code)
            if idx in initial_activity_indexes:
                initial_activity_codes.add(activity_code)
            _register_code(activity_id, activity_code)
            nodes.append(
                {
                    "module_code": module_code,
                    "node_id": activity_id,
                    "node_code": activity_code,
                    "node_type": "activity",
                    "label": _label(activity, activity_code),
                    "objective_code": objective_code,
                    "activity_index": idx + 1,
                    "init_open": objective_init and activity_code in initial_activity_codes,
                    "source_primary": "mia_config",
                    "source_enrichment": "catalog",
                    "is_ghost": False,
                }
            )
        objective_activity_codes[objective_id] = ordered_activity_codes
        objective_rules_summary.append(
            {
                "objective_id": objective_id,
                "objective_code": objective_code,
                "ordered_activity_codes": ordered_activity_codes,
                "initial_open_activity_codes": [
                    code for code in ordered_activity_codes if code in initial_activity_codes
                ],
            }
        )

    edges: list[dict[str, object]] = []
    edge_seen: set[tuple[str, str, str]] = set()

    def _ordered_activity_codes_for_objective(objective_id: str) -> list[str]:
        return list(objective_activity_codes.get(objective_id, []))

    def _source_activity_code(objective_id: str, level: int | None) -> str | None:
        ordered_codes = _ordered_activity_codes_for_objective(objective_id)
        if level is None or level < 0 or level >= len(ordered_codes):
            return None
        return ordered_codes[level]

    def _add_edge(
        *,
        edge_type: str,
        from_code: str | None,
        to_code: str | None,
        threshold_value: float | None,
        enrich_lvl: int | None,
        enrich_sr: float | None,
        rule_text: str,
    ) -> None:
        src = _clean_text(from_code)
        dst = _clean_text(to_code)
        if src is None or dst is None:
            return
        dedup = (edge_type, src, dst)
        if dedup in edge_seen:
            return
        edge_seen.add(dedup)
        edges.append(
            {
                "module_code": module_code,
                "edge_id": f"{module_code}:{edge_type}:{src}->{dst}:{len(edges) + 1}",
                "edge_type": edge_type,
                "from_node_code": src,
                "to_node_code": dst,
                "threshold_type": "success_rate" if threshold_value is not None else "unknown",
                "threshold_value": threshold_value,
                "rule_text": rule_text,
                "source_primary": "mia_config",
                "source_enrichment": "moduleConfig",
                "enrich_lvl": enrich_lvl,
                "enrich_sr": enrich_sr,
            }
        )

    module_requirements = module_level_payload.get("requirements")
    module_requirements = (
        module_requirements[0]
        if isinstance(module_requirements, list) and module_requirements and isinstance(module_requirements[0], dict)
        else {}
    )
    for target_objective_id in objective_order_ids:
        prereq_map = module_requirements.get(target_objective_id)
        if not isinstance(prereq_map, dict):
            continue
        target_objective = objective_by_id.get(target_objective_id)
        target_code = _clean_text(target_objective.get("code")) if isinstance(target_objective, dict) else None
        for prerequisite_objective_id in objective_order_ids:
            condition = prereq_map.get(prerequisite_objective_id)
            if not isinstance(condition, dict):
                continue
            sr = (
                float(condition["sr"][0])
                if isinstance(condition.get("sr"), list) and condition["sr"]
                else None
            )
            level = (
                int(condition["lvl"][0])
                if isinstance(condition.get("lvl"), list) and condition["lvl"]
                else None
            )
            prerequisite_objective = objective_by_id.get(prerequisite_objective_id)
            fallback_code = (
                _clean_text(prerequisite_objective.get("code"))
                if isinstance(prerequisite_objective, dict)
                else None
            )
            source_code = _source_activity_code(prerequisite_objective_id, level) or fallback_code
            _add_edge(
                edge_type="activation",
                from_code=source_code,
                to_code=target_code,
                threshold_value=sr,
                enrich_lvl=level,
                enrich_sr=sr,
                rule_text="moduleConfig.requirements",
            )

    for objective_id in objective_order_ids:
        objective_config = module_rules_payload.get(objective_id)
        if not isinstance(objective_config, dict):
            continue
        objective_requirements = objective_config.get("requirements")
        objective_requirements = (
            objective_requirements[0]
            if isinstance(objective_requirements, list)
            and objective_requirements
            and isinstance(objective_requirements[0], dict)
            else {}
        )
        for target_activity_id, prereq_map in objective_requirements.items():
            if not isinstance(prereq_map, dict):
                continue
            target_activity = activity_by_id.get(target_activity_id)
            target_code = _clean_text(target_activity.get("code")) if isinstance(target_activity, dict) else None
            for source_objective_id, condition in prereq_map.items():
                if not isinstance(condition, dict):
                    continue
                sr = (
                    float(condition["sr"][0])
                    if isinstance(condition.get("sr"), list) and condition["sr"]
                    else None
                )
                level = (
                    int(condition["lvl"][0])
                    if isinstance(condition.get("lvl"), list) and condition["lvl"]
                    else None
                )
                fallback_objective = objective_by_id.get(source_objective_id)
                fallback_code = (
                    _clean_text(fallback_objective.get("code"))
                    if isinstance(fallback_objective, dict)
                    else None
                )
                source_code = _source_activity_code(source_objective_id, level) or fallback_code
                _add_edge(
                    edge_type="activation",
                    from_code=source_code,
                    to_code=target_code,
                    threshold_value=sr,
                    enrich_lvl=level,
                    enrich_sr=sr,
                    rule_text="moduleConfig.objective_requirements",
                )

    return (
        {
            "meta": {
                "generated_by": "visu2 single-module researcher adapter",
                "source_id": source_id,
                "build_timestamp_utc": datetime.now(UTC).isoformat(),
                "version": "single_module_zpdes_runtime_v1",
                "source_files": [str(config_json_path)],
                "module_id": module_id,
                "module_code": module_code,
            },
            "module_rules": [
                {
                    "module_id": module_id,
                    "module_code": module_code,
                    "dependency_topology": "config_ai_moduleConfig",
                    "initial_open_objective_codes": sorted(initial_objective_codes),
                    "objective_rules": objective_rules_summary,
                    "node_rules": [],
                }
            ],
            "map_id_code": {
                "code_to_id": code_to_id,
                "id_to_codes": dict(id_to_codes),
            },
            "links_to_catalog": {
                "rule_module_ids": [module_id],
                "module_ids_linked": [module_id],
                "objective_ids_linked": objective_order_ids,
                "activity_ids_linked": sorted(activity_by_id),
            },
            "unresolved_links": {
                "rule_ids_missing_in_catalog": [],
                "catalog_module_ids_missing_in_rules": [],
                "codes_with_multiple_ids": {},
                "ids_with_multiple_codes": {},
            },
            "dependency_topology": {
                module_code: {
                    "nodes": nodes,
                    "edges": edges,
                }
            },
        },
        (),
    )


def _build_single_module_researcher_catalog_and_raw(
    attempts_csv_path: Path,
    *,
    source_id: str,
    config_json_path: Path | None = None,
    module_code: str = "M1",
) -> tuple[pl.DataFrame, dict[str, Any], dict[str, Any], tuple[str, ...]]:
    attempts, parse_warnings = _load_maureen_researcher_attempts(attempts_csv_path)
    if attempts.height == 0:
        raise ValueError("Single-module researcher CSV is empty after parsing.")

    warnings: list[str] = list(parse_warnings)
    module_ids = sorted(
        {str(value).strip() for value in attempts["playlist_or_module_id"].to_list() if str(value or "").strip()}
    )
    if len(module_ids) != 1:
        raise ValueError(
            "Single-module researcher source expects exactly one non-null playlist_or_module_id; "
            f"got {module_ids!r}."
        )
    module_id = module_ids[0]
    module_metadata, objective_metadata_by_id, activity_metadata_by_id, config_warnings = (
        _load_single_module_researcher_config_metadata(
            config_json_path,
            module_id=module_id,
        )
    )
    warnings.extend(config_warnings)
    module_code = _clean_text(module_metadata["code"]) if module_metadata else module_code
    if module_metadata and _clean_text(module_metadata.get("short_title")):
        module_short = str(module_metadata["short_title"])
        csv_module_shorts = sorted(
            {
                str(value).strip()
                for value in attempts["module_short_title"].to_list()
                if str(value or "").strip()
            }
        )
        if csv_module_shorts and csv_module_shorts != [module_short]:
            warnings.append(
                "MIA config short module title differs from the researcher CSV; using the config title "
                f"{module_short!r}."
            )
    else:
        module_short = _choose_unique_value(
            attempts["module_short_title"].to_list(),
            label="module_short_title",
            default=module_code,
            warnings=warnings,
        )
    if module_metadata and _clean_text(module_metadata.get("long_title")):
        module_long = str(module_metadata["long_title"])
        csv_module_longs = sorted(
            {
                str(value).strip()
                for value in attempts["module_long_title"].to_list()
                if str(value or "").strip()
            }
        )
        if csv_module_longs and csv_module_longs != [module_long]:
            warnings.append(
                "MIA config long module title differs from the researcher CSV; using the config title "
                f"{module_long!r}."
            )
    else:
        module_long = _choose_unique_value(
            attempts["module_long_title"].to_list(),
            label="module_long_title",
            default=module_short,
            warnings=warnings,
        )

    attempts = (
        attempts.with_columns(
            pl.lit(module_id, dtype=pl.Utf8).alias("module_id"),
            pl.coalesce(
                [
                    pl.col("module_long_title"),
                    pl.lit(module_long, dtype=pl.Utf8),
                ]
            ).alias("module_long_title"),
        )
        .sort(["user_id", "created_at", "exercise_id"])
    )

    objective_rows = (
        attempts.select(["objective_id", "created_at"])
        .filter(pl.col("objective_id").is_not_null())
        .group_by("objective_id")
        .agg(pl.col("created_at").min().alias("first_seen_utc"))
        .sort(["first_seen_utc", "objective_id"])
        .to_dicts()
    )
    if not objective_rows:
        raise ValueError("Single-module researcher source is missing objective_id values.")
    objective_rows.sort(
        key=lambda row: (
            0,
            _code_sort_key(objective_metadata_by_id[str(row["objective_id"])]["code"]),
            str(row["objective_id"]),
        )
        if str(row["objective_id"]) in objective_metadata_by_id
        and objective_metadata_by_id[str(row["objective_id"])].get("code")
        else (
            1,
            row["first_seen_utc"] or datetime.max.replace(tzinfo=UTC),
            str(row["objective_id"]),
        )
    )

    objectives_by_id: dict[str, dict[str, Any]] = {}
    objective_order: list[str] = []
    for idx, row in enumerate(objective_rows, start=1):
        objective_id = str(row["objective_id"])
        objective_metadata = objective_metadata_by_id.get(objective_id, {})
        code_suffix = f"{idx:02d}"
        objective_code = _clean_text(objective_metadata.get("code")) or f"{module_code}O{code_suffix}"
        objective_short = _clean_text(objective_metadata.get("short_title")) or f"Objective {code_suffix}"
        objective_long = _clean_text(objective_metadata.get("long_title")) or f"Synthetic objective {code_suffix}"
        objectives_by_id[objective_id] = {
            "id": objective_id,
            "code": objective_code,
            "short_title": objective_short,
            "long_title": objective_long,
            "status": _clean_text(objective_metadata.get("status"))
            or ("visible" if objective_metadata else "synthetic"),
            "activity_ids": [],
        }
        objective_order.append(objective_id)

    activity_rows = (
        attempts.select(["objective_id", "activity_id", "exercise_id", "created_at"])
        .filter(pl.col("objective_id").is_not_null() & pl.col("activity_id").is_not_null())
        .group_by(["objective_id", "activity_id"])
        .agg(
            pl.col("created_at").min().alias("first_seen_utc"),
            pl.col("exercise_id")
            .drop_nulls()
            .cast(pl.Utf8)
            .unique()
            .sort()
            .alias("exercise_ids"),
        )
        .sort(["objective_id", "first_seen_utc", "activity_id"])
        .to_dicts()
    )
    if not activity_rows:
        raise ValueError("Single-module researcher source is missing activity_id values.")
    activity_rows.sort(
        key=lambda row: (
            str(row["objective_id"]),
            0,
            _code_sort_key(activity_metadata_by_id[str(row["activity_id"])]["code"]),
            str(row["activity_id"]),
        )
        if str(row["activity_id"]) in activity_metadata_by_id
        and activity_metadata_by_id[str(row["activity_id"])].get("code")
        else (
            str(row["objective_id"]),
            1,
            row["first_seen_utc"] or datetime.max.replace(tzinfo=UTC),
            str(row["activity_id"]),
        )
    )

    activities_by_id: dict[str, dict[str, Any]] = {}
    activity_counts_by_objective: defaultdict[str, int] = defaultdict(int)
    for row in activity_rows:
        objective_id = str(row["objective_id"])
        activity_id = str(row["activity_id"])
        objective_payload = objectives_by_id.get(objective_id)
        if objective_payload is None:
            raise ValueError(f"Activity {activity_id} references unknown objective_id {objective_id}.")
        activity_counts_by_objective[objective_id] += 1
        suffix = f"{activity_counts_by_objective[objective_id]:02d}"
        objective_payload["activity_ids"].append(activity_id)
        activity_metadata = activity_metadata_by_id.get(activity_id, {})
        activity_code = _clean_text(activity_metadata.get("code")) or f"{objective_payload['code']}A{suffix}"
        activity_short = _clean_text(activity_metadata.get("short_title")) or f"Activity {suffix}"
        activity_long = _clean_text(activity_metadata.get("long_title")) or f"Synthetic activity {suffix}"
        activities_by_id[activity_id] = {
            "id": activity_id,
            "code": activity_code,
            "short_title": activity_short,
            "long_title": activity_long,
            "status": _clean_text(activity_metadata.get("status"))
            or ("visible" if activity_metadata else "synthetic"),
            "exercise_ids": [str(exercise_id) for exercise_id in row["exercise_ids"]],
        }

    modules_payload: list[dict[str, Any]] = []
    exercise_to_hierarchy: dict[str, dict[str, str]] = {}
    id_label_index: dict[str, dict[str, Any]] = {
        module_id: {
            "type": "module",
            "code": module_code,
            "short_title": module_short,
            "long_title": module_long,
            "sources": ["mia_attempts", *(["mia_config"] if module_metadata else [])],
        }
    }

    module_objectives: list[dict[str, Any]] = []
    for objective_id in objective_order:
        objective_payload = objectives_by_id[objective_id]
        id_label_index[objective_id] = {
            "type": "objective",
            "code": objective_payload["code"],
            "short_title": objective_payload["short_title"],
            "long_title": objective_payload["long_title"],
            "sources": ["mia_attempts"],
        }
        activity_entries: list[dict[str, Any]] = []
        for activity_id in objective_payload["activity_ids"]:
            activity_payload = activities_by_id[activity_id]
            id_label_index[activity_id] = {
                "type": "activity",
                "code": activity_payload["code"],
                "short_title": activity_payload["short_title"],
                "long_title": activity_payload["long_title"],
                "sources": ["mia_attempts"],
            }
            exercise_ids = list(activity_payload["exercise_ids"])
            for exercise_id in exercise_ids:
                if exercise_id not in id_label_index:
                    id_label_index[exercise_id] = {
                        "type": "exercise",
                        "code": None,
                        "short_title": None,
                        "long_title": None,
                        "sources": ["mia_attempts"],
                    }
                exercise_to_hierarchy[exercise_id] = {
                    "module_id": module_id,
                    "objective_id": objective_id,
                    "activity_id": activity_id,
                }
            activity_entries.append(
                {
                    "id": activity_id,
                    "code": activity_payload["code"],
                    "status": activity_payload["status"],
                    "title": {
                        "short": activity_payload["short_title"],
                        "long": activity_payload["long_title"],
                    },
                    "exercise_ids": exercise_ids,
                }
            )
        module_objectives.append(
            {
                "id": objective_id,
                "code": objective_payload["code"],
                "status": objective_payload["status"],
                "title": {
                    "short": objective_payload["short_title"],
                    "long": objective_payload["long_title"],
                },
                "activities": activity_entries,
            }
        )

    modules_payload.append(
        {
            "id": module_id,
            "code": module_code,
            "status": "visible",
            "title": {
                "short": module_short,
                "long": module_long,
            },
            "objectives": module_objectives,
        }
    )

    learning_catalog = {
        "meta": {
            "generated_by": "visu2 single-module researcher adapter",
                "source_id": source_id,
                "build_timestamp_utc": datetime.now(UTC).isoformat(),
                "version": "single_module_runtime_v2",
                "source_files": [
                    str(attempts_csv_path),
                    *(module_metadata["source_files"] if module_metadata else []),
                ],
                "counts": {
                    "modules": len(modules_payload),
                    "objectives": len(objectives_by_id),
                "activities": len(activities_by_id),
                "exercises_unique": len(exercise_to_hierarchy),
            },
        },
        "conflicts": {
            "coverage": {
                "overlapping_activity_count": 0,
                "primary_only_activity_count": len(activities_by_id),
                "secondary_only_activity_count": 0,
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
                "unique_count": 0,
                "ambiguous_count": 0,
            },
        },
        "orphans": [],
        "id_label_index": id_label_index,
        "modules": modules_payload,
        "exercise_to_hierarchy": exercise_to_hierarchy,
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

    return raw_attempts, learning_catalog, exercises_json, tuple(sorted(set(warnings)))


def _build_multi_module_researcher_catalog_and_raw(
    attempts_csv_path: Path,
    *,
    source_id: str,
    config_json_path: Path | None,
) -> tuple[pl.DataFrame, dict[str, Any], dict[str, Any], tuple[str, ...]]:
    attempts, parse_warnings = _load_maureen_researcher_attempts(attempts_csv_path)
    if attempts.height == 0:
        raise ValueError("Multi-module researcher CSV is empty after parsing.")

    (
        modules_by_id,
        objectives_by_id,
        activities_by_id,
        exercise_to_hierarchy,
        config_warnings,
    ) = _load_multi_module_researcher_config_metadata(config_json_path, required=True)
    if not modules_by_id:
        raise ValueError("Multi-module researcher source requires module metadata in config_mia.json.")

    warnings: list[str] = list(parse_warnings)
    warnings.extend(config_warnings)

    objective_codes_used_by_module: defaultdict[str, set[str]] = defaultdict(set)
    for metadata in objectives_by_id.values():
        objective_codes_used_by_module[str(metadata["module_code"])].add(str(metadata["code"]))

    activity_codes_used_by_objective: defaultdict[str, set[str]] = defaultdict(set)
    for metadata in activities_by_id.values():
        activity_codes_used_by_objective[str(metadata["objective_code"])].add(str(metadata["code"]))

    attempt_rows = attempts.select(
        [
            "playlist_or_module_id",
            "objective_id",
            "activity_id",
            "exercise_id",
            "created_at",
        ]
    ).to_dicts()
    attempt_exercises_by_activity: defaultdict[str, list[str]] = defaultdict(list)
    objective_ids_seen_in_attempts: set[str] = set()
    activity_ids_seen_in_attempts: set[str] = set()
    for row in attempt_rows:
        objective_id = str(row.get("objective_id") or "").strip()
        activity_id = str(row.get("activity_id") or "").strip()
        exercise_id = str(row.get("exercise_id") or "").strip()
        if objective_id:
            objective_ids_seen_in_attempts.add(objective_id)
        if activity_id:
            activity_ids_seen_in_attempts.add(activity_id)
        if activity_id and exercise_id and exercise_id not in attempt_exercises_by_activity[activity_id]:
            attempt_exercises_by_activity[activity_id].append(exercise_id)

    for objective_id in sorted(objective_ids_seen_in_attempts):
        if objective_id in objectives_by_id:
            continue
        objective_rows = attempts.filter(pl.col("objective_id") == objective_id)
        inferred_module_id = None
        if objective_rows.height:
            inferred_module_id = (
                objective_rows.select(pl.col("playlist_or_module_id"))
                .filter(pl.col("playlist_or_module_id").is_in(list(modules_by_id)))
                .head(1)
                .item(0, 0)
            )
        if inferred_module_id is None:
            continue
        module_metadata = modules_by_id[str(inferred_module_id)]
        module_code = str(module_metadata["code"])
        synthetic_code = _next_objective_code(module_code, objective_codes_used_by_module[module_code])
        objective_codes_used_by_module[module_code].add(synthetic_code)
        objectives_by_id[objective_id] = {
            "id": objective_id,
            "code": synthetic_code,
            "module_id": str(inferred_module_id),
            "module_code": module_code,
            "short_title": f"Unmapped objective {objective_id[:8]}",
            "long_title": f"Unmapped objective {objective_id}",
            "status": "synthetic",
            "activity_ids": [],
        }
        warnings.append(f"Added synthetic objective for unmatched MIA objective_id={objective_id}.")

    for activity_id in sorted(activity_ids_seen_in_attempts):
        if activity_id in activities_by_id:
            continue
        row = (
            attempts.filter(pl.col("activity_id") == activity_id)
            .select(["objective_id"])
            .filter(pl.col("objective_id").is_not_null())
            .head(1)
        )
        objective_id = row.item(0, 0) if row.height else None
        objective_metadata = objectives_by_id.get(str(objective_id or ""))
        if objective_metadata is None:
            continue
        objective_code = str(objective_metadata["code"])
        synthetic_code = _next_code(
            objective_code,
            activity_codes_used_by_objective[objective_code],
        )
        activity_codes_used_by_objective[objective_code].add(synthetic_code)
        activities_by_id[activity_id] = {
            "id": activity_id,
            "code": synthetic_code,
            "objective_id": str(objective_id),
            "objective_code": objective_code,
            "module_id": str(objective_metadata["module_id"]),
            "module_code": str(objective_metadata["module_code"]),
            "short_title": f"Unmapped activity {activity_id[:8]}",
            "long_title": f"Unmapped activity {activity_id}",
            "status": "synthetic",
            "exercise_ids": list(attempt_exercises_by_activity.get(activity_id, [])),
            "synthetic": True,
        }
        objective_metadata["activity_ids"].append(activity_id)
        for exercise_id in activities_by_id[activity_id]["exercise_ids"]:
            exercise_to_hierarchy[exercise_id] = {
                "module_id": str(objective_metadata["module_id"]),
                "objective_id": str(objective_id),
                "activity_id": activity_id,
            }
        warnings.append(f"Added synthetic activity for unmatched MIA activity_id={activity_id}.")

    for activity_id, metadata in activities_by_id.items():
        existing = list(metadata["exercise_ids"])
        for exercise_id in attempt_exercises_by_activity.get(activity_id, []):
            if exercise_id not in existing:
                existing.append(exercise_id)
                exercise_to_hierarchy[exercise_id] = {
                    "module_id": str(metadata["module_id"]),
                    "objective_id": str(metadata["objective_id"]),
                    "activity_id": activity_id,
                }
        metadata["exercise_ids"] = existing

    modules_payload: list[dict[str, Any]] = []
    id_label_index: dict[str, dict[str, Any]] = {}

    for module_metadata in sorted(modules_by_id.values(), key=lambda row: _code_sort_key(str(row["code"]))):
        module_id = str(module_metadata["id"])
        module_code = str(module_metadata["code"])
        module_short = str(module_metadata["short_title"])
        module_long = str(module_metadata["long_title"])
        id_label_index[module_id] = {
            "type": "module",
            "code": module_code,
            "short_title": module_short,
            "long_title": module_long,
            "sources": ["mia_config"],
        }
        module_objectives: list[dict[str, Any]] = []
        module_objective_rows = sorted(
            [
                metadata
                for metadata in objectives_by_id.values()
                if str(metadata["module_id"]) == module_id
            ],
            key=lambda row: _code_sort_key(str(row["code"])),
        )
        for objective_metadata in module_objective_rows:
            objective_id = str(objective_metadata["id"])
            objective_code = str(objective_metadata["code"])
            objective_short = str(objective_metadata["short_title"])
            objective_long = str(objective_metadata["long_title"])
            id_label_index[objective_id] = {
                "type": "objective",
                "code": objective_code,
                "short_title": objective_short,
                "long_title": objective_long,
                "sources": ["mia_config"] if objective_metadata["status"] != "synthetic" else ["mia_attempts"],
            }
            activity_entries: list[dict[str, Any]] = []
            objective_activity_rows = sorted(
                [
                    activities_by_id[activity_id]
                    for activity_id in objective_metadata["activity_ids"]
                    if activity_id in activities_by_id
                ],
                key=lambda row: _code_sort_key(str(row["code"])),
            )
            for activity_metadata in objective_activity_rows:
                activity_id = str(activity_metadata["id"])
                activity_code = str(activity_metadata["code"])
                activity_short = str(activity_metadata["short_title"])
                activity_long = str(activity_metadata["long_title"])
                id_label_index[activity_id] = {
                    "type": "activity",
                    "code": activity_code,
                    "short_title": activity_short,
                    "long_title": activity_long,
                    "sources": ["mia_config"] if not activity_metadata["synthetic"] else ["mia_attempts"],
                }
                exercise_ids = list(activity_metadata["exercise_ids"])
                for exercise_id in exercise_ids:
                    if exercise_id not in id_label_index:
                        id_label_index[exercise_id] = {
                            "type": "exercise",
                            "code": None,
                            "short_title": None,
                            "long_title": None,
                            "sources": ["mia_config"],
                        }
                activity_entries.append(
                    {
                        "id": activity_id,
                        "code": activity_code,
                        "status": str(activity_metadata["status"]),
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
                    "status": str(objective_metadata["status"]),
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
                "status": str(module_metadata["status"]),
                "title": {
                    "short": module_short,
                    "long": module_long,
                },
                "objectives": module_objectives,
            }
        )

    learning_catalog = {
        "meta": {
            "generated_by": "visu2 multi-module researcher adapter",
            "source_id": source_id,
            "build_timestamp_utc": datetime.now(UTC).isoformat(),
            "version": "multi_module_runtime_v1",
            "source_files": [str(attempts_csv_path), *(modules_by_id[next(iter(modules_by_id))]["source_files"] if modules_by_id else [])],
            "counts": {
                "modules": len(modules_payload),
                "objectives": len(objectives_by_id),
                "activities": len(activities_by_id),
                "exercises_unique": len(exercise_to_hierarchy),
            },
        },
        "conflicts": {
            "coverage": {
                "overlapping_activity_count": 0,
                "primary_only_activity_count": len([row for row in activities_by_id.values() if not row["synthetic"]]),
                "secondary_only_activity_count": len([row for row in activities_by_id.values() if row["synthetic"]]),
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
                "unique_count": 0,
                "ambiguous_count": 0,
            },
        },
        "orphans": [],
        "id_label_index": id_label_index,
        "modules": modules_payload,
        "exercise_to_hierarchy": exercise_to_hierarchy,
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

    module_lookup = pl.DataFrame(
        {
            "module_id_lookup": [str(metadata["id"]) for metadata in modules_by_id.values()],
            "module_long_title_lookup": [str(metadata["long_title"]) for metadata in modules_by_id.values()],
        }
    )
    objective_lookup = pl.DataFrame(
        {
            "objective_id_lookup": [objective_id for objective_id in objectives_by_id],
            "objective_module_id_lookup": [str(metadata["module_id"]) for metadata in objectives_by_id.values()],
        }
    )
    activity_lookup = pl.DataFrame(
        {
            "activity_id_lookup": [activity_id for activity_id in activities_by_id],
            "activity_objective_id_lookup": [str(metadata["objective_id"]) for metadata in activities_by_id.values()],
            "activity_module_id_lookup": [str(metadata["module_id"]) for metadata in activities_by_id.values()],
        }
    )
    exercise_lookup = pl.DataFrame(
        {
            "exercise_id_lookup": [exercise_id for exercise_id in exercise_to_hierarchy],
            "exercise_activity_id_lookup": [mapping["activity_id"] for mapping in exercise_to_hierarchy.values()],
            "exercise_objective_id_lookup": [mapping["objective_id"] for mapping in exercise_to_hierarchy.values()],
            "exercise_module_id_lookup": [mapping["module_id"] for mapping in exercise_to_hierarchy.values()],
        }
    )

    raw_attempts = (
        attempts.join(activity_lookup, left_on="activity_id", right_on="activity_id_lookup", how="left")
        .join(objective_lookup, left_on="objective_id", right_on="objective_id_lookup", how="left")
        .join(exercise_lookup, left_on="exercise_id", right_on="exercise_id_lookup", how="left")
        .with_columns(
            pl.when(pl.col("playlist_or_module_id").is_in(list(modules_by_id)))
            .then(pl.col("playlist_or_module_id"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("playlist_module_id_lookup")
        )
        .with_columns(
            pl.coalesce(
                [
                    pl.col("activity_id"),
                    pl.col("exercise_activity_id_lookup"),
                ]
            ).alias("activity_id"),
            pl.coalesce(
                [
                    pl.col("objective_id"),
                    pl.col("activity_objective_id_lookup"),
                    pl.col("exercise_objective_id_lookup"),
                ]
            ).alias("objective_id"),
            pl.coalesce(
                [
                    pl.col("activity_module_id_lookup"),
                    pl.col("objective_module_id_lookup"),
                    pl.col("exercise_module_id_lookup"),
                    pl.col("playlist_module_id_lookup"),
                ]
            ).alias("module_id"),
        )
        .join(module_lookup, left_on="module_id", right_on="module_id_lookup", how="left")
        .with_columns(
            pl.coalesce(
                [pl.col("module_long_title_lookup"), pl.col("module_long_title")]
            ).alias("module_long_title")
        )
        .drop(
            [
                "activity_id_lookup",
                "objective_id_lookup",
                "exercise_id_lookup",
                "activity_objective_id_lookup",
                "activity_module_id_lookup",
                "objective_module_id_lookup",
                "exercise_activity_id_lookup",
                "exercise_objective_id_lookup",
                "exercise_module_id_lookup",
                "playlist_module_id_lookup",
                "module_id_lookup",
                "module_long_title_lookup",
            ],
            strict=False,
        )
        .with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("variation"),
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

    unresolved_rows = raw_attempts.filter(pl.col("module_id").is_null()).height
    if unresolved_rows:
        warnings.append(
            f"Kept {unresolved_rows} row(s) without resolved module hierarchy; these rows remain available but outside the module catalog."
        )

    return raw_attempts, learning_catalog, exercises_json, tuple(sorted(set(warnings)))


def _build_multi_module_researcher_zpdes_rules(
    *,
    source_id: str,
    learning_catalog: dict[str, Any],
    config_json_path: Path,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    module_rules: list[dict[str, Any]] = []
    dependency_topology: dict[str, Any] = {}
    code_to_id: dict[str, str] = {}
    id_to_codes: defaultdict[str, list[str]] = defaultdict(list)
    module_ids_linked: list[str] = []
    objective_ids_linked: list[str] = []
    activity_ids_linked: list[str] = []
    warnings: list[str] = []

    for module in learning_catalog.get("modules", []):
        if not isinstance(module, dict):
            continue
        module_id = _clean_text(module.get("id"))
        if module_id is None:
            continue
        try:
            module_payload, module_warnings = _build_single_module_researcher_zpdes_rules(
                source_id=source_id,
                module_id=module_id,
                learning_catalog=learning_catalog,
                config_json_path=config_json_path,
            )
        except ValueError as err:
            warnings.append(str(err))
            continue
        warnings.extend(module_warnings)
        module_rules.extend(module_payload.get("module_rules", []))
        dependency_topology.update(module_payload.get("dependency_topology", {}))
        code_to_id.update(module_payload.get("map_id_code", {}).get("code_to_id", {}))
        for identifier, codes in module_payload.get("map_id_code", {}).get("id_to_codes", {}).items():
            for code in codes:
                if code not in id_to_codes[identifier]:
                    id_to_codes[identifier].append(code)
        links = module_payload.get("links_to_catalog", {})
        module_ids_linked.extend(links.get("module_ids_linked", []))
        objective_ids_linked.extend(links.get("objective_ids_linked", []))
        activity_ids_linked.extend(links.get("activity_ids_linked", []))

    return (
        {
            "meta": {
                "generated_by": "visu2 multi-module researcher adapter",
                "source_id": source_id,
                "build_timestamp_utc": datetime.now(UTC).isoformat(),
                "version": "multi_module_zpdes_runtime_v1",
                "source_files": [str(config_json_path)],
            },
            "module_rules": module_rules,
            "map_id_code": {
                "code_to_id": code_to_id,
                "id_to_codes": dict(id_to_codes),
            },
            "links_to_catalog": {
                "rule_module_ids": sorted({str(row.get("module_id")) for row in module_rules if row.get("module_id")}),
                "module_ids_linked": sorted(set(module_ids_linked)),
                "objective_ids_linked": sorted(set(objective_ids_linked)),
                "activity_ids_linked": sorted(set(activity_ids_linked)),
            },
            "unresolved_links": {
                "rule_ids_missing_in_catalog": [],
                "catalog_module_ids_missing_in_rules": [],
                "codes_with_multiple_ids": {},
                "ids_with_multiple_codes": {},
            },
            "dependency_topology": dependency_topology,
        },
        tuple(sorted(set(warnings))),
    )


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


def _first_non_empty(*values: object) -> str | None:
    """Return the first non-empty text value from a candidate list."""
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return None


def _sorted_module_ids(module_ids: set[str], module_code_by_id: dict[str, str]) -> list[str]:
    """Sort module IDs by their pedagogical code, then by ID for stability."""
    return sorted(module_ids, key=lambda module_id: (_code_sort_key(module_code_by_id[module_id]), module_id))


def _list_unique(values: list[str]) -> list[str]:
    """Return values in first-seen order without duplicates."""
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _add_neurips_dependency_code(code_by_id: dict[str, list[str]], identifier: str | None, code: str | None) -> None:
    """Add one possible pedagogical code carried by the dependency JSON."""
    if not identifier or not code:
        return
    code_by_id.setdefault(identifier, [])
    if code not in code_by_id[identifier]:
        code_by_id[identifier].append(code)


def _load_neurips_dependency_codes(dependencies: dict[str, Any]) -> dict[str, list[str]]:
    """Load pedagogical codes embedded in the self-contained dependency JSON."""
    code_by_id: dict[str, list[str]] = {}
    raw_modules = dependencies.get("modules")
    raw_modules = raw_modules if isinstance(raw_modules, dict) else {}
    for module_id, module_payload in raw_modules.items():
        if not isinstance(module_payload, dict):
            continue
        _add_neurips_dependency_code(code_by_id, str(module_id), _clean_text(module_payload.get("code")))
        objectives = module_payload.get("objectives")
        objectives = objectives if isinstance(objectives, dict) else {}
        for objective_id, objective_payload in objectives.items():
            if not isinstance(objective_payload, dict):
                continue
            _add_neurips_dependency_code(
                code_by_id,
                str(objective_id),
                _clean_text(objective_payload.get("code")),
            )
            activities = objective_payload.get("activities")
            activities = activities if isinstance(activities, dict) else {}
            for activity_id, activity_payload in activities.items():
                if not isinstance(activity_payload, dict):
                    continue
                _add_neurips_dependency_code(
                    code_by_id,
                    str(activity_id),
                    _clean_text(activity_payload.get("code")),
                )

    return code_by_id


def _dependency_title_pair(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return short/long titles embedded in one dependency JSON node."""
    title = payload.get("title")
    if isinstance(title, dict):
        short = _first_non_empty(title.get("short"), title.get("name"), title.get("label"))
        long = _first_non_empty(title.get("long"), title.get("description"), short)
        return short, long
    if isinstance(title, str):
        clean_title = _clean_text(title)
        return clean_title, clean_title
    label = _first_non_empty(
        payload.get("name"),
        payload.get("label"),
        payload.get("short_title"),
        payload.get("long_title"),
    )
    long = _first_non_empty(payload.get("long_title"), payload.get("description"), label)
    return label, long


def _load_neurips_dependency_titles(dependencies: dict[str, Any]) -> dict[str, tuple[str | None, str | None]]:
    """Load pedagogical titles embedded in the self-contained dependency JSON."""
    titles_by_id: dict[str, tuple[str | None, str | None]] = {}
    raw_modules = dependencies.get("modules")
    raw_modules = raw_modules if isinstance(raw_modules, dict) else {}
    for module_id, module_payload in raw_modules.items():
        if not isinstance(module_payload, dict):
            continue
        titles_by_id[str(module_id)] = _dependency_title_pair(module_payload)
        objectives = module_payload.get("objectives")
        objectives = objectives if isinstance(objectives, dict) else {}
        for objective_id, objective_payload in objectives.items():
            if not isinstance(objective_payload, dict):
                continue
            titles_by_id[str(objective_id)] = _dependency_title_pair(objective_payload)
            activities = objective_payload.get("activities")
            activities = activities if isinstance(activities, dict) else {}
            for activity_id, activity_payload in activities.items():
                if not isinstance(activity_payload, dict):
                    continue
                titles_by_id[str(activity_id)] = _dependency_title_pair(activity_payload)

    return titles_by_id


def _position_by_first_seen(values: list[str]) -> dict[str, int]:
    """Map each value to its first position in a repeated ordered list."""
    positions: dict[str, int] = {}
    for idx, value in enumerate(values):
        positions.setdefault(value, idx)
    return positions


def _neurips_dependency_code(
    identifier: str,
    dependency_code_by_id: dict[str, list[str]],
    *,
    expected_prefix: str,
) -> str | None:
    """Return a dependency-embedded code only when it matches the expected prefix."""
    codes = dependency_code_by_id.get(identifier) or []
    matching_codes = [code for code in codes if code.startswith(expected_prefix)]
    if not matching_codes:
        return None
    return matching_codes[-1]


def _neurips_dependency_title(
    identifier: str,
    dependency_title_by_id: dict[str, tuple[str | None, str | None]],
) -> tuple[str | None, str | None]:
    """Return short/long titles embedded in the dependency JSON."""
    return dependency_title_by_id.get(identifier, (None, None))


def _load_neurips_exercise_table(path: Path) -> pl.DataFrame:
    """Load the NeurIPS exercise table as normalized text columns."""
    required = {
        "exercise_id",
        "gameplay_type",
        "instruction",
        "question",
        "feedback",
        "module_id",
        "module_name",
        "objective_id",
        "objective_name",
        "objective_targeted_difficulties",
        "activity_id",
        "activity_name",
        "source",
    }
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"NeurIPS exercise table is missing columns: {missing}")
    return frame.with_columns(
        [_optional_text_expr(column).alias(column) for column in sorted(required)]
    )


def _neurips_module_code_map(
    exercise_rows: list[dict[str, Any]],
    dependencies: dict[str, Any],
) -> dict[str, str]:
    """Return stable app-compatible module codes for NeurIPS modules."""
    module_ids = {
        str(row.get("module_id") or "").strip()
        for row in exercise_rows
        if str(row.get("module_id") or "").strip()
    }
    raw_modules = dependencies.get("modules")
    if isinstance(raw_modules, dict):
        module_ids.update(str(module_id) for module_id in raw_modules if str(module_id).strip())

    code_by_id: dict[str, str] = {}
    fallback_cursor = 900
    for module_id in sorted(module_ids):
        module_payload = raw_modules.get(module_id) if isinstance(raw_modules, dict) else None
        dependency_code = _clean_text(module_payload.get("code")) if isinstance(module_payload, dict) else None
        if dependency_code:
            code_by_id[module_id] = dependency_code
            continue
        if module_id in NEURIPS_MODULE_CODE_BY_ID:
            code_by_id[module_id] = NEURIPS_MODULE_CODE_BY_ID[module_id]
            continue
        while f"M{fallback_cursor}" in set(code_by_id.values()):
            fallback_cursor += 1
        code_by_id[module_id] = f"M{fallback_cursor}"
        fallback_cursor += 1
    return code_by_id


def _neurips_candidate_rows(
    exercise_rows: list[dict[str, Any]],
    dependencies: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build exercise-to-hierarchy candidates from the exercise CSV and dependency JSON."""
    module_source_by_id: dict[str, str | None] = {}
    module_name_by_id: dict[str, str | None] = {}
    for row in exercise_rows:
        module_id = _clean_text(row.get("module_id"))
        if not module_id:
            continue
        module_source_by_id.setdefault(module_id, _clean_text(row.get("source")))
        module_name_by_id.setdefault(module_id, _clean_text(row.get("module_name")))

    rows: list[dict[str, Any]] = []
    for row in exercise_rows:
        exercise_id = _clean_text(row.get("exercise_id"))
        module_id = _clean_text(row.get("module_id"))
        objective_id = _clean_text(row.get("objective_id"))
        activity_id = _clean_text(row.get("activity_id"))
        if not exercise_id or not module_id or not objective_id or not activity_id:
            continue
        rows.append(
            {
                "exercise_id": exercise_id,
                "module_id": module_id,
                "objective_id": objective_id,
                "activity_id": activity_id,
                "source": _clean_text(row.get("source")),
                "has_exercise_metadata": True,
            }
        )

    raw_modules = dependencies.get("modules")
    raw_modules = raw_modules if isinstance(raw_modules, dict) else {}
    for module_id, module_payload in raw_modules.items():
        if not isinstance(module_payload, dict):
            continue
        module_key = str(module_id)
        objectives = module_payload.get("objectives")
        objectives = objectives if isinstance(objectives, dict) else {}
        for objective_id, objective_payload in objectives.items():
            if not isinstance(objective_payload, dict):
                continue
            objective_key = str(objective_id)
            activities = objective_payload.get("activities")
            activities = activities if isinstance(activities, dict) else {}
            for activity_id, activity_payload in activities.items():
                if not isinstance(activity_payload, dict):
                    continue
                activity_key = str(activity_id)
                for exercise_id in activity_payload.get("exercise_ids") or []:
                    exercise_key = str(exercise_id or "").strip()
                    if not exercise_key:
                        continue
                    rows.append(
                        {
                            "exercise_id": exercise_key,
                            "module_id": module_key,
                            "objective_id": objective_key,
                            "activity_id": activity_key,
                            "source": module_source_by_id.get(module_key),
                            "has_exercise_metadata": False,
                        }
                    )

    return [
        dict(row)
        for row in {
            (
                row["exercise_id"],
                row["module_id"],
                row["objective_id"],
                row["activity_id"],
                row.get("source"),
                bool(row.get("has_exercise_metadata")),
            ): row
            for row in rows
        }.values()
    ]


def _weighted_neurips_candidates(
    attempts_parquet_path: Path,
    candidate_rows: list[dict[str, Any]],
    module_code_by_id: dict[str, str],
) -> pl.DataFrame:
    """Attach attempt counts used to resolve duplicate exercise mappings."""
    if not candidate_rows:
        return pl.DataFrame(
            {
                "exercise_id": [],
                "module_id": [],
                "objective_id": [],
                "activity_id": [],
                "source": [],
                "has_exercise_metadata": [],
                "candidate_context_attempts": [],
                "candidate_source_attempts": [],
                "candidate_total_attempts": [],
                "module_code": [],
            }
        )

    candidates = pl.DataFrame(candidate_rows).with_columns(
        pl.col("has_exercise_metadata").cast(pl.Boolean),
        pl.col("module_id").replace(module_code_by_id).alias("module_code"),
    )
    context_counts = (
        pl.scan_parquet(attempts_parquet_path)
        .select(["source", "playlist_or_module_id", "exercise_id"])
        .group_by(["source", "playlist_or_module_id", "exercise_id"])
        .agg(pl.len().alias("candidate_context_attempts"))
        .collect()
    )
    source_counts = (
        pl.scan_parquet(attempts_parquet_path)
        .select(["source", "exercise_id"])
        .group_by(["source", "exercise_id"])
        .agg(pl.len().alias("candidate_source_attempts"))
        .collect()
    )
    total_counts = (
        pl.scan_parquet(attempts_parquet_path)
        .select(["exercise_id"])
        .group_by("exercise_id")
        .agg(pl.len().alias("candidate_total_attempts"))
        .collect()
    )
    return (
        candidates.join(
            context_counts,
            left_on=["source", "module_id", "exercise_id"],
            right_on=["source", "playlist_or_module_id", "exercise_id"],
            how="left",
        )
        .join(source_counts, on=["source", "exercise_id"], how="left")
        .join(total_counts, on="exercise_id", how="left")
        .with_columns(
            pl.col("candidate_context_attempts").fill_null(0),
            pl.col("candidate_source_attempts").fill_null(0),
            pl.col("candidate_total_attempts").fill_null(0),
        )
    )


def _best_neurips_lookup(
    candidates: pl.DataFrame,
    *,
    key_columns: tuple[str, ...],
    suffix: str,
) -> pl.DataFrame:
    """Choose one best hierarchy candidate per lookup key."""
    if candidates.height == 0:
        schema = {column: pl.Utf8 for column in key_columns}
        schema.update(
            {
                f"module_id_{suffix}": pl.Utf8,
                f"objective_id_{suffix}": pl.Utf8,
                f"activity_id_{suffix}": pl.Utf8,
                f"module_code_{suffix}": pl.Utf8,
            }
        )
        return pl.DataFrame(schema=schema)

    rows = candidates.to_dicts()
    rows.sort(
        key=lambda row: (
            *(str(row.get(column) or "") for column in key_columns),
            -int(row.get("candidate_context_attempts") or 0),
            -int(row.get("candidate_source_attempts") or 0),
            -int(row.get("candidate_total_attempts") or 0),
            -int(bool(row.get("has_exercise_metadata"))),
            _code_sort_key(str(row.get("module_code") or "")),
            str(row.get("objective_id") or ""),
            str(row.get("activity_id") or ""),
        )
    )
    seen: set[tuple[str, ...]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(str(row.get(column) or "") for column in key_columns)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                **{column: row.get(column) for column in key_columns},
                f"module_id_{suffix}": row.get("module_id"),
                f"objective_id_{suffix}": row.get("objective_id"),
                f"activity_id_{suffix}": row.get("activity_id"),
                f"module_code_{suffix}": row.get("module_code"),
            }
        )
    return pl.DataFrame(out)


def _neurips_catalog_payloads(
    *,
    attempts_parquet_path: Path,
    exercises_csv_path: Path,
    dependencies_json_path: Path,
    source_id: str,
) -> tuple[pl.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any], tuple[str, ...]]:
    """Build NeurIPS normalized attempts plus app metadata payloads."""
    exercise_frame = _load_neurips_exercise_table(exercises_csv_path)
    exercise_rows = exercise_frame.to_dicts()
    dependencies = json.loads(dependencies_json_path.read_text(encoding="utf-8"))
    module_code_by_id = _neurips_module_code_map(exercise_rows, dependencies)
    dependency_code_by_id = _load_neurips_dependency_codes(dependencies)
    dependency_title_by_id = _load_neurips_dependency_titles(dependencies)
    candidate_rows = _neurips_candidate_rows(exercise_rows, dependencies)
    weighted_candidates = _weighted_neurips_candidates(
        attempts_parquet_path,
        candidate_rows,
        module_code_by_id,
    )

    module_name_by_id: dict[str, str | None] = {}
    module_source_by_id: dict[str, str | None] = {}
    objective_name_by_id: dict[str, str | None] = {}
    objective_long_by_id: dict[str, str | None] = {}
    activity_name_by_id: dict[str, str | None] = {}
    exercise_metadata_by_id: dict[str, dict[str, Any]] = {}

    for row in exercise_rows:
        module_id = _clean_text(row.get("module_id"))
        objective_id = _clean_text(row.get("objective_id"))
        activity_id = _clean_text(row.get("activity_id"))
        exercise_id = _clean_text(row.get("exercise_id"))
        if module_id:
            module_name_by_id.setdefault(module_id, _clean_text(row.get("module_name")))
            module_source_by_id.setdefault(module_id, _clean_text(row.get("source")))
        if objective_id:
            objective_name_by_id.setdefault(objective_id, _clean_text(row.get("objective_name")))
            objective_long_by_id.setdefault(
                objective_id,
                _clean_text(row.get("objective_targeted_difficulties")),
            )
        if activity_id:
            activity_name_by_id.setdefault(activity_id, _clean_text(row.get("activity_name")))
        if exercise_id and exercise_id not in exercise_metadata_by_id:
            exercise_metadata_by_id[exercise_id] = {
                "id": exercise_id,
                "type": _clean_text(row.get("gameplay_type")),
                "instruction": _first_non_empty(row.get("instruction"), row.get("question")),
                "question": _clean_text(row.get("question")),
                "feedback": _clean_text(row.get("feedback")),
                "source": _clean_text(row.get("source")),
            }

    raw_modules = dependencies.get("modules")
    raw_modules = raw_modules if isinstance(raw_modules, dict) else {}
    module_ids = set(module_code_by_id)

    objective_ids_by_module: dict[str, list[str]] = defaultdict(list)
    activity_ids_by_objective: dict[str, list[str]] = defaultdict(list)
    exercise_ids_by_activity: dict[str, list[str]] = defaultdict(list)

    for module_id, module_payload in raw_modules.items():
        if not isinstance(module_payload, dict):
            continue
        module_key = str(module_id)
        for objective_id in module_payload.get("objective_ids") or []:
            objective_key = str(objective_id or "").strip()
            if objective_key:
                objective_ids_by_module[module_key].append(objective_key)
        objectives = module_payload.get("objectives")
        objectives = objectives if isinstance(objectives, dict) else {}
        for objective_id, objective_payload in objectives.items():
            if not isinstance(objective_payload, dict):
                continue
            objective_key = str(objective_id)
            for activity_id in objective_payload.get("activity_ids") or []:
                activity_key = str(activity_id or "").strip()
                if activity_key:
                    activity_ids_by_objective[objective_key].append(activity_key)
            activities = objective_payload.get("activities")
            activities = activities if isinstance(activities, dict) else {}
            for activity_id, activity_payload in activities.items():
                if not isinstance(activity_payload, dict):
                    continue
                activity_key = str(activity_id)
                exercise_ids_by_activity[activity_key].extend(
                    str(exercise_id)
                    for exercise_id in (activity_payload.get("exercise_ids") or [])
                    if str(exercise_id or "").strip()
                )

    for row in weighted_candidates.to_dicts():
        module_id = _clean_text(row.get("module_id"))
        objective_id = _clean_text(row.get("objective_id"))
        activity_id = _clean_text(row.get("activity_id"))
        exercise_id = _clean_text(row.get("exercise_id"))
        if not module_id or not objective_id or not activity_id or not exercise_id:
            continue
        objective_ids_by_module[module_id].append(objective_id)
        activity_ids_by_objective[objective_id].append(activity_id)
        exercise_ids_by_activity[activity_id].append(exercise_id)
        module_ids.add(module_id)

    module_ids_ordered = _sorted_module_ids(module_ids, module_code_by_id)
    objective_code_by_id: dict[str, str] = {}
    activity_code_by_id: dict[str, str] = {}
    modules_payload: list[dict[str, Any]] = []
    id_label_index: dict[str, dict[str, Any]] = {}

    for module_id in module_ids_ordered:
        module_code = module_code_by_id[module_id]
        dependency_module_short, dependency_module_long = _neurips_dependency_title(
            module_id,
            dependency_title_by_id,
        )
        module_label = dependency_module_short or module_name_by_id.get(module_id) or module_code
        module_long = dependency_module_long or module_label
        id_label_index[module_id] = {
            "type": "module",
            "code": module_code,
            "short_title": module_label,
            "long_title": module_long,
            "sources": ["maths_exercises_table", "maths_dependencies"],
        }
        objective_entries: list[dict[str, Any]] = []
        objective_ids = _list_unique(objective_ids_by_module.get(module_id, []))
        objective_positions = _position_by_first_seen(objective_ids_by_module.get(module_id, []))
        objective_ids = sorted(
            objective_ids,
            key=lambda objective_id: (
                0
                if (
                    dependency_objective_code := _neurips_dependency_code(
                        objective_id,
                        dependency_code_by_id,
                        expected_prefix=f"{module_code}O",
                    )
                )
                else 1,
                _code_sort_key(dependency_objective_code),
                objective_positions.get(objective_id, 10**9),
                objective_name_by_id.get(objective_id) or objective_id,
            ),
        )
        for objective_idx, objective_id in enumerate(objective_ids, start=1):
            objective_code = _neurips_dependency_code(
                objective_id,
                dependency_code_by_id,
                expected_prefix=f"{module_code}O",
            ) or f"{module_code}O{objective_idx}"
            objective_code_by_id[objective_id] = objective_code
            dependency_objective_short, dependency_objective_long = _neurips_dependency_title(
                objective_id,
                dependency_title_by_id,
            )
            objective_label = dependency_objective_short or objective_name_by_id.get(objective_id) or objective_code
            objective_long = dependency_objective_long or objective_long_by_id.get(objective_id)
            id_label_index[objective_id] = {
                "type": "objective",
                "code": objective_code,
                "short_title": objective_label,
                "long_title": objective_long,
                "sources": ["maths_exercises_table", "maths_dependencies"],
            }
            activity_entries: list[dict[str, Any]] = []
            activity_ids = _list_unique(activity_ids_by_objective.get(objective_id, []))
            activity_positions = _position_by_first_seen(activity_ids_by_objective.get(objective_id, []))
            activity_ids = sorted(
                activity_ids,
                key=lambda activity_id: (
                    0
                    if (
                        dependency_activity_code := _neurips_dependency_code(
                            activity_id,
                            dependency_code_by_id,
                            expected_prefix=f"{objective_code}A",
                        )
                    )
                    else 1,
                    _code_sort_key(dependency_activity_code),
                    activity_positions.get(activity_id, 10**9),
                    activity_name_by_id.get(activity_id) or activity_id,
                ),
            )
            for activity_idx, activity_id in enumerate(activity_ids, start=1):
                activity_code = _neurips_dependency_code(
                    activity_id,
                    dependency_code_by_id,
                    expected_prefix=f"{objective_code}A",
                ) or f"{objective_code}A{activity_idx}"
                activity_code_by_id[activity_id] = activity_code
                dependency_activity_short, dependency_activity_long = _neurips_dependency_title(
                    activity_id,
                    dependency_title_by_id,
                )
                activity_label = dependency_activity_short or activity_name_by_id.get(activity_id) or activity_code
                activity_long = dependency_activity_long or activity_label
                exercise_ids = _list_unique(exercise_ids_by_activity.get(activity_id, []))
                id_label_index[activity_id] = {
                    "type": "activity",
                    "code": activity_code,
                    "short_title": activity_label,
                    "long_title": activity_long,
                    "sources": ["maths_exercises_table", "maths_dependencies"],
                }
                activity_entries.append(
                    {
                        "id": activity_id,
                        "code": activity_code,
                        "title": {"short": activity_label, "long": activity_long},
                        "exercise_ids": exercise_ids,
                    }
                )
            objective_entries.append(
                {
                    "id": objective_id,
                    "code": objective_code,
                    "title": {"short": objective_label, "long": objective_long},
                    "activities": activity_entries,
                }
            )
        modules_payload.append(
            {
                "id": module_id,
                "code": module_code,
                "status": "visible",
                "title": {"short": module_label, "long": module_long},
                "source": module_source_by_id.get(module_id),
                "objectives": objective_entries,
            }
        )

    for exercise_id, metadata in exercise_metadata_by_id.items():
        id_label_index[exercise_id] = {
            "type": "exercise",
            "code": None,
            "short_title": _first_non_empty(metadata.get("instruction"), metadata.get("question"), exercise_id),
            "long_title": _first_non_empty(metadata.get("question"), metadata.get("instruction")),
            "sources": ["maths_exercises_table"],
        }

    canonical_lookup = _best_neurips_lookup(
        weighted_candidates,
        key_columns=("exercise_id",),
        suffix="canonical",
    )
    exercise_to_hierarchy = {
        str(row["exercise_id"]): {
            "module_id": row["module_id_canonical"],
            "objective_id": row["objective_id_canonical"],
            "activity_id": row["activity_id_canonical"],
        }
        for row in canonical_lookup.to_dicts()
        if row.get("exercise_id")
        and row.get("module_id_canonical")
        and row.get("objective_id_canonical")
        and row.get("activity_id_canonical")
    }

    dependency_only_exercises = set(exercise_to_hierarchy) - set(exercise_metadata_by_id)
    for exercise_id in sorted(dependency_only_exercises):
        exercise_metadata_by_id[exercise_id] = {
            "id": exercise_id,
            "type": None,
            "instruction": None,
            "question": None,
            "feedback": None,
            "source": "maths_dependencies",
        }
        id_label_index[exercise_id] = {
            "type": "exercise",
            "code": None,
            "short_title": exercise_id,
            "long_title": None,
            "sources": ["maths_dependencies"],
        }

    candidate_exercise_ids = set(exercise_to_hierarchy)
    attempted_exercises = (
        pl.scan_parquet(attempts_parquet_path)
        .select(pl.col("exercise_id").cast(pl.Utf8))
        .unique()
        .collect()
    )
    unmapped_attempt_exercise_ids = sorted(
        str(row["exercise_id"])
        for row in attempted_exercises.to_dicts()
        if row.get("exercise_id") not in candidate_exercise_ids
    )

    context_lookup = _best_neurips_lookup(
        weighted_candidates.filter(pl.col("source").is_not_null()),
        key_columns=("source", "module_id", "exercise_id"),
        suffix="context",
    ).rename({"module_id": "playlist_or_module_id"})
    source_lookup = _best_neurips_lookup(
        weighted_candidates.filter(pl.col("source").is_not_null()),
        key_columns=("source", "exercise_id"),
        suffix="source",
    )

    raw_attempts = (
        pl.scan_parquet(attempts_parquet_path)
        .with_columns(
            _optional_text_expr("source").alias("source"),
            _optional_text_expr("user_id").alias("user_id"),
            _optional_text_expr("playlist_or_module_id").alias("playlist_or_module_id"),
            _optional_text_expr("exercise_id").alias("exercise_id"),
            _optional_text_expr("work_mode").alias("work_mode"),
        )
        .join(context_lookup.lazy(), on=["source", "playlist_or_module_id", "exercise_id"], how="left")
        .join(source_lookup.lazy(), on=["source", "exercise_id"], how="left")
        .join(canonical_lookup.lazy(), on="exercise_id", how="left")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("module_id_context"),
                    pl.col("module_id_source"),
                    pl.col("module_id_canonical"),
                ]
            ).alias("module_id"),
            pl.coalesce(
                [
                    pl.col("objective_id_context"),
                    pl.col("objective_id_source"),
                    pl.col("objective_id_canonical"),
                ]
            ).alias("objective_id"),
            pl.coalesce(
                [
                    pl.col("activity_id_context"),
                    pl.col("activity_id_source"),
                    pl.col("activity_id_canonical"),
                ]
            ).alias("activity_id"),
        )
        .with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("variation"),
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("login_time"),
            pl.col("data_correct").cast(pl.Float64).alias("data_score"),
            pl.col("data_correct").cast(pl.Boolean).alias("data_correct"),
            pl.col("work_mode").alias("data_test_context"),
            pl.lit(None, dtype=pl.Float64).alias("progression_score"),
            pl.lit(None, dtype=pl.Float64).alias("initial_test_max_success"),
            pl.lit(None, dtype=pl.Float64).alias("initial_test_weighted_max_success"),
            pl.lit(None, dtype=pl.Float64).alias("initial_test_success_rate"),
            pl.lit(None, dtype=pl.Float64).alias("finished_module_mean_score"),
            pl.lit(None, dtype=pl.Float64).alias("finished_module_graphe_coverage_rate"),
            pl.lit(None, dtype=pl.Boolean).alias("is_gar"),
            pl.lit(None, dtype=pl.Utf8).alias("teacher_id"),
            pl.lit(None, dtype=pl.Utf8).alias("classroom_id"),
            pl.col("data_duration").cast(pl.Float64, strict=False).alias("data_duration"),
            pl.lit(None, dtype=pl.Float64).alias("session_duration"),
            pl.coalesce(
                [
                    pl.col("module_id_context").replace(module_name_by_id),
                    pl.col("module_id_source").replace(module_name_by_id),
                    pl.col("module_id_canonical").replace(module_name_by_id),
                ]
            ).alias("module_long_title"),
        )
        .sort(["user_id", "created_at", "exercise_id"])
        .with_columns(
            pl.coalesce(
                [
                    pl.col("attempt_index").cast(pl.Int64, strict=False),
                    pl.col("exercise_id").cum_count().over(["user_id", "exercise_id"]).cast(pl.Int64),
                ]
            ).alias("attempt_number"),
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
                "source",
                "session_id",
                "data_answer",
            ]
        )
        .collect()
    )

    code_to_id: dict[str, str] = {}
    id_to_codes: dict[str, list[str]] = defaultdict(list)
    for identifier, entry in id_label_index.items():
        code = _clean_text(entry.get("code"))
        if not code:
            continue
        code_to_id[code] = identifier
        id_to_codes[identifier].append(code)

    dependency_topology: dict[str, dict[str, list[dict[str, Any]]]] = {}
    unresolved_dependency_ids: set[str] = set()
    for module in modules_payload:
        module_code = str(module.get("code") or "")
        if not module_code or module_code == "M999":
            continue
        module_id = str(module.get("id") or "")
        raw_module = raw_modules.get(module_id, {})
        raw_module_objectives = raw_module.get("objectives", {}) if isinstance(raw_module, dict) else {}
        raw_module_objectives = raw_module_objectives if isinstance(raw_module_objectives, dict) else {}
        module_activity_code_by_id = {
            str(activity.get("id") or ""): str(activity.get("code") or "")
            for objective_entry in module.get("objectives") or []
            for activity in objective_entry.get("activities") or []
            if str(activity.get("id") or "")
            in raw_module_objectives.get(str(objective_entry.get("id")), {}).get("activities", {})
        }
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        edge_pairs: set[tuple[str, str]] = set()
        for objective in module.get("objectives") or []:
            raw_objective = raw_module_objectives.get(str(objective.get("id")), {})
            if not isinstance(raw_objective, dict) or not raw_objective:
                continue
            raw_activities = raw_objective.get("activities")
            raw_activities = raw_activities if isinstance(raw_activities, dict) else {}
            objective_code = str(objective.get("code") or "")
            nodes.append(
                {
                    "module_code": module_code,
                    "node_id": objective["id"],
                    "node_code": objective_code,
                    "node_type": "objective",
                    "label": (objective.get("title") or {}).get("short") or objective_code,
                    "objective_code": objective_code,
                    "activity_index": None,
                    "init_open": False,
                    "source_primary": "maths_dependencies",
                    "source_enrichment": "learning_catalog",
                    "is_ghost": False,
                }
            )
            dependency_activities = [
                activity
                for activity in objective.get("activities") or []
                if str(activity.get("id") or "") in raw_activities
            ]
            for activity_index, activity in enumerate(dependency_activities, start=1):
                activity_code = str(activity.get("code") or "")
                activity_id = str(activity.get("id") or "")
                raw_activity = raw_activities.get(activity_id, {})
                prerequisites = [
                    str(value)
                    for value in (raw_activity.get("prerequisite_activity_ids") or [])
                    if str(value or "").strip()
                ]
                nodes.append(
                    {
                        "module_code": module_code,
                        "node_id": activity_id,
                        "node_code": activity_code,
                        "node_type": "activity",
                        "label": (activity.get("title") or {}).get("short") or activity_code,
                        "objective_code": objective_code,
                        "activity_index": activity_index,
                        "init_open": not prerequisites,
                        "source_primary": "maths_dependencies",
                        "source_enrichment": "learning_catalog",
                        "is_ghost": False,
                    }
                )
                for prerequisite_id in prerequisites:
                    from_code = module_activity_code_by_id.get(prerequisite_id)
                    if not from_code:
                        unresolved_dependency_ids.add(prerequisite_id)
                        continue
                    if (from_code, activity_code) in edge_pairs:
                        continue
                    edge_pairs.add((from_code, activity_code))
                    edges.append(
                        {
                            "module_code": module_code,
                            "edge_id": f"{module_code}:prerequisite:{from_code}->{activity_code}",
                            "edge_type": "prerequisite",
                            "from_node_code": from_code,
                            "to_node_code": activity_code,
                            "threshold_type": None,
                            "threshold_value": None,
                            "rule_text": None,
                            "source_primary": "maths_dependencies",
                            "source_enrichment": None,
                            "enrich_lvl": None,
                            "enrich_sr": None,
                        }
                    )
                for unlocked_id in raw_activity.get("unlocks_activity_ids") or []:
                    to_code = module_activity_code_by_id.get(str(unlocked_id))
                    if not to_code:
                        unresolved_dependency_ids.add(str(unlocked_id))
                        continue
                    if (activity_code, to_code) in edge_pairs:
                        continue
                    edge_pairs.add((activity_code, to_code))
                    edges.append(
                        {
                            "module_code": module_code,
                            "edge_id": f"{module_code}:unlock:{activity_code}->{to_code}",
                            "edge_type": "unlock",
                            "from_node_code": activity_code,
                            "to_node_code": to_code,
                            "threshold_type": None,
                            "threshold_value": None,
                            "rule_text": None,
                            "source_primary": "maths_dependencies",
                            "source_enrichment": None,
                            "enrich_lvl": None,
                            "enrich_sr": None,
                        }
                    )
        if nodes or edges:
            dependency_topology[module_code] = {"nodes": nodes, "edges": edges}

    exercises_json = {
        "exercises": [
            {
                **metadata,
                "activities": [exercise_to_hierarchy[exercise_id]["activity_id"]]
                if exercise_id in exercise_to_hierarchy
                else [],
                "objectives": [exercise_to_hierarchy[exercise_id]["objective_id"]]
                if exercise_id in exercise_to_hierarchy
                else [],
                "modules": [exercise_to_hierarchy[exercise_id]["module_id"]]
                if exercise_id in exercise_to_hierarchy
                else [],
            }
            for exercise_id, metadata in sorted(exercise_metadata_by_id.items())
        ]
    }

    duplicate_exercise_ids = (
        exercise_frame.group_by("exercise_id")
        .agg(pl.len().alias("rows"))
        .filter(pl.col("rows") > 1)
        .select("exercise_id")
        .to_series()
        .to_list()
    )
    learning_catalog = {
        "meta": {
            "generated_by": "visu2 neurips maths adapter",
            "source_id": source_id,
            "build_timestamp_utc": datetime.now(UTC).isoformat(),
            "version": "neurips_maths_runtime_v1",
            "source_files": [
                str(attempts_parquet_path),
                str(exercises_csv_path),
                str(dependencies_json_path),
            ],
            "counts": {
                "modules": len(modules_payload),
                "objectives": len(objective_code_by_id),
                "activities": len(activity_code_by_id),
                "exercises_unique": len(exercise_to_hierarchy),
            },
        },
        "conflicts": {
            "coverage": {
                "overlapping_activity_count": 0,
                "primary_only_activity_count": len(activity_code_by_id),
                "secondary_only_activity_count": 0,
                "overlap_membership_disagreement_count": 0,
            },
            "missing_references": {
                "missing_activity_ids_in_objectives": [],
                "missing_objective_ids_in_modules": [],
            },
            "source_disagreements": {
                "activity_exercise_membership_disagreements": [],
                "id_label_disagreements": [],
                "duplicate_exercise_ids": sorted(str(value) for value in duplicate_exercise_ids),
            },
            "secondary_mapping_candidates_for_orphans": {
                "unique_count": 0,
                "ambiguous_count": 0,
            },
        },
        "orphans": unmapped_attempt_exercise_ids,
        "id_label_index": id_label_index,
        "modules": modules_payload,
        "exercise_to_hierarchy": exercise_to_hierarchy,
    }

    zpdes_rules = {
        "meta": {
            "generated_by": "visu2 neurips maths adapter",
            "source_id": source_id,
            "build_timestamp_utc": datetime.now(UTC).isoformat(),
            "version": "neurips_maths_zpdes_v1",
            "source_files": [str(dependencies_json_path)],
        },
        "module_rules": [
            {
                "module_code": module["code"],
                "module_id": module["id"],
                "node_rules": [],
            }
            for module in modules_payload
            if module.get("code") in dependency_topology
        ],
        "map_id_code": {
            "code_to_id": code_to_id,
            "id_to_codes": dict(id_to_codes),
        },
        "dependency_topology": dependency_topology,
        "links_to_catalog": {
            "rule_module_ids": [
                str(module["id"])
                for module in modules_payload
                if module.get("code") in dependency_topology
            ],
        },
        "unresolved_links": {
            "rule_ids_missing_in_catalog": sorted(unresolved_dependency_ids),
            "catalog_module_ids_missing_in_rules": [
                str(module["id"])
                for module in modules_payload
                if module.get("code") not in dependency_topology and module.get("code") != "M999"
            ],
            "codes_with_multiple_ids": {},
            "ids_with_multiple_codes": {},
        },
    }

    warnings: list[str] = []
    if unmapped_attempt_exercise_ids:
        warnings.append(
            f"Kept {len(unmapped_attempt_exercise_ids)} attempted exercise id(s) without NeurIPS hierarchy mapping."
        )
    if duplicate_exercise_ids:
        warnings.append(
            f"Resolved {len(duplicate_exercise_ids)} duplicate exercise id(s) with source/module and attempt-weighted precedence."
        )
    if unresolved_dependency_ids:
        warnings.append(
            f"Skipped {len(unresolved_dependency_ids)} dependency edge endpoint(s) missing from the catalog."
        )

    return raw_attempts, learning_catalog, zpdes_rules, exercises_json, tuple(sorted(set(warnings)))


def materialize_source_runtime_inputs(settings: Settings) -> SourceMaterializationReport:
    """Materialize one source into runtime and local-build directories."""
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
        settings.local_zpdes_rules_path.write_text(
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
                str(settings.local_zpdes_rules_path),
                str(settings.exercises_json_path),
            ),
            warnings=warnings,
        )

    if source.build_profile == "single_module_researcher":
        attempts_csv_path = settings.root_dir / source.raw_inputs["attempts_csv"]
        config_json_path = (
            settings.root_dir / source.raw_inputs["config_json"]
            if "config_json" in source.raw_inputs
            else None
        )
        raw_attempts, learning_catalog, exercises_json, warnings = (
            _build_single_module_researcher_catalog_and_raw(
                attempts_csv_path,
                source_id=settings.source_id,
                config_json_path=config_json_path,
            )
        )
        input_paths = [
            str(settings.parquet_path),
            str(settings.learning_catalog_path),
            str(settings.exercises_json_path),
        ]
        raw_attempts.write_parquet(settings.parquet_path)
        settings.learning_catalog_path.write_text(
            json.dumps(learning_catalog, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        settings.exercises_json_path.write_text(
            json.dumps(exercises_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        if config_json_path is not None:
            zpdes_rules, zpdes_warnings = _build_single_module_researcher_zpdes_rules(
                source_id=settings.source_id,
                module_id=str(learning_catalog["modules"][0]["id"]),
                learning_catalog=learning_catalog,
                config_json_path=config_json_path,
            )
            warnings = tuple(sorted(set((*warnings, *zpdes_warnings))))
            settings.zpdes_rules_path.write_text(
                json.dumps(zpdes_rules, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            input_paths.append(str(settings.zpdes_rules_path))
        return SourceMaterializationReport(
            source_id=settings.source_id,
            input_paths=tuple(input_paths),
            warnings=warnings,
        )

    if source.build_profile == "multi_module_researcher":
        attempts_csv_path = settings.root_dir / source.raw_inputs["attempts_csv"]
        config_json_path = (
            settings.root_dir / source.raw_inputs["config_json"]
            if "config_json" in source.raw_inputs
            else None
        )
        raw_attempts, learning_catalog, exercises_json, warnings = (
            _build_multi_module_researcher_catalog_and_raw(
                attempts_csv_path,
                source_id=settings.source_id,
                config_json_path=config_json_path,
            )
        )
        input_paths = [
            str(settings.parquet_path),
            str(settings.learning_catalog_path),
            str(settings.exercises_json_path),
        ]
        raw_attempts.write_parquet(settings.parquet_path)
        settings.learning_catalog_path.write_text(
            json.dumps(learning_catalog, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        settings.exercises_json_path.write_text(
            json.dumps(exercises_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        if config_json_path is not None:
            zpdes_rules, zpdes_warnings = _build_multi_module_researcher_zpdes_rules(
                source_id=settings.source_id,
                learning_catalog=learning_catalog,
                config_json_path=config_json_path,
            )
            warnings = tuple(sorted(set((*warnings, *zpdes_warnings))))
            settings.zpdes_rules_path.write_text(
                json.dumps(zpdes_rules, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            input_paths.append(str(settings.zpdes_rules_path))
        return SourceMaterializationReport(
            source_id=settings.source_id,
            input_paths=tuple(input_paths),
            warnings=warnings,
        )

    if source.build_profile == "neurips_maths":
        attempts_parquet_path = settings.root_dir / source.raw_inputs["parquet"]
        exercises_csv_path = settings.root_dir / source.raw_inputs["exercises_csv"]
        dependencies_json_path = settings.root_dir / source.raw_inputs["dependencies_json"]
        raw_attempts, learning_catalog, zpdes_rules, exercises_json, warnings = (
            _neurips_catalog_payloads(
                attempts_parquet_path=attempts_parquet_path,
                exercises_csv_path=exercises_csv_path,
                dependencies_json_path=dependencies_json_path,
                source_id=settings.source_id,
            )
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

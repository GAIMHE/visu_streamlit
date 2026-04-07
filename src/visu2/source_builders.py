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

    raise ValueError(f"Unsupported source build profile: {source.build_profile}")

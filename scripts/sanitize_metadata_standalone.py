#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

FORBIDDEN_SOURCE_TOKENS = (
    "summary.json",
    "summary_1.json",
    "admathgraphe.json",
    "modules_configgraphe.xlsx",
)

FORBIDDEN_KEY_TOKENS = (
    "summary",
    "xlsx",
    "admath",
)

# Provenance-style fields that are build-history artifacts and should not ship
# in standalone metadata payloads.
DROP_PROVENANCE_KEYS = {
    "incoming_source",
    "source_primary",
    "source_enrichment",
    "only_in_summary",
    "only_in_xlsx",
    "only_in_primary",
    "only_in_secondary",
}

STANDALONE_PACKAGE_FILES = (
    "adaptiv_math_history.parquet",
    "learning_catalog.json",
    "zpdes_rules.json",
    "exercises.json",
)

KEY_RENAMES = {
    "summary_activity_count": "primary_activity_count",
    "summary_only_activity_count": "primary_only_activity_count",
    "xlsx_activity_count_with_learning_items": "secondary_activity_count_with_learning_items",
    "xlsx_only_activity_count": "secondary_only_activity_count",
    "only_in_summary": "only_in_primary",
    "only_in_xlsx": "only_in_secondary",
    "summary_count": "primary_count",
    "xlsx_count": "secondary_count",
    "xlsx_candidate_activity_ids": "secondary_candidate_activity_ids",
    "admath_codes": "rules_codes",
    "admath_distinct_ids": "rules_distinct_ids",
    "admath_ids_linked_to_catalog": "rules_ids_linked_to_catalog",
    "admath_unresolved_ids": "rules_unresolved_ids",
    "modules_in_admath": "modules_in_rules",
    "admath_ids_missing_in_learning_catalog": "rule_ids_missing_in_catalog",
    "catalog_module_ids_missing_in_admath": "catalog_module_ids_missing_in_rules",
    "exercises_in_summary": "exercises_in_primary_catalog",
    "mappable_rows_summary_first": "mappable_rows_primary_first",
    "unmapped_distinct_exercises_summary_first": "unmapped_distinct_exercises_primary_first",
    "unmapped_rows_summary_first": "unmapped_rows_primary_first",
    "unmapped_with_ambiguous_xlsx_candidates": "unmapped_with_ambiguous_secondary_candidates",
    "unmapped_with_unique_xlsx_candidate": "unmapped_with_unique_secondary_candidate",
}

VALUE_RENAMES_EXACT = {
    "xlsx": "rules",
    "admath": "rules",
    "summary": "catalog",
    "summary+xlsx": "catalog+rules",
    "xlsx+admath": "rules",
    "summary+admath": "catalog+rules",
    "summary+xlsx+admath": "catalog+rules",
    "not_in_summary_and_not_in_xlsx": "not_in_primary_and_not_in_secondary",
    "not_in_summary_but_unique_xlsx_candidate": "not_in_primary_but_unique_secondary_candidate",
    "not_in_summary_and_ambiguous_xlsx_candidates": "not_in_primary_and_ambiguous_secondary_candidates",
}


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at root: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def _source_file_entries(data_dir: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for filename in STANDALONE_PACKAGE_FILES:
        file_path = data_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Missing standalone package file: {file_path}")
        entries.append(
            {
                "path": f"data/{filename}",
            }
        )
    return entries


def _drop_unwanted_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            key: _drop_unwanted_keys(value)
            for key, value in obj.items()
            if key not in DROP_PROVENANCE_KEYS
        }
    if isinstance(obj, list):
        return [_drop_unwanted_keys(item) for item in obj]
    return obj


def _rename_legacy_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for key, value in obj.items():
            key_txt = str(key)
            new_key = KEY_RENAMES.get(key_txt, key_txt)
            out[new_key] = _rename_legacy_keys(value)
        return out
    if isinstance(obj, list):
        return [_rename_legacy_keys(item) for item in obj]
    return obj


def _sanitize_legacy_source_strings(obj: Any) -> Any:
    if isinstance(obj, str):
        exact = VALUE_RENAMES_EXACT.get(obj)
        if exact is not None:
            return exact
        lower = obj.lower()
        exact_lower = VALUE_RENAMES_EXACT.get(lower)
        if exact_lower is not None:
            return exact_lower
        if not any(token in lower for token in FORBIDDEN_SOURCE_TOKENS):
            return obj
        tags: list[str] = []
        if "summary" in lower:
            tags.append("catalog")
        if "admath" in lower or "modules_configgraphe.xlsx" in lower:
            tags.append("rules")
        if not tags:
            return obj
        unique = sorted(set(tags))
        return unique[0] if len(unique) == 1 else "+".join(unique)
    if isinstance(obj, list):
        return [_sanitize_legacy_source_strings(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _sanitize_legacy_source_strings(value) for key, value in obj.items()}
    return obj


def _normalized_sources(sources: object, fallback_tag: str = "catalog") -> list[str]:
    tags: set[str] = set()
    if isinstance(sources, list):
        for source in sources:
            if not isinstance(source, str):
                continue
            source_l = source.lower()
            if "summary" in source_l or "learning_catalog" in source_l:
                tags.add("catalog")
            if (
                "admath" in source_l
                or "modules_configgraphe.xlsx" in source_l
                or "zpdes_rules" in source_l
            ):
                tags.add("rules")
            if "exercise" in source_l:
                tags.add("catalog")
    if not tags:
        tags.add(fallback_tag)
    return sorted(tags)


def _sanitize_learning_catalog(
    payload: dict[str, Any],
    source_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    sanitized = _sanitize_legacy_source_strings(
        _rename_legacy_keys(_drop_unwanted_keys(payload))
    )
    meta = sanitized.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta["source_files"] = source_entries
    meta["generated_by"] = "scripts/sanitize_metadata_standalone.py"
    meta["build_timestamp_utc"] = datetime.now(UTC).isoformat()
    if "history_file_used" in meta:
        meta["history_file_used"] = None
    meta["canonical_precedence"] = {
        "hierarchy": "learning_catalog.modules + learning_catalog.exercise_to_hierarchy",
        "rules": "zpdes_rules.module_rules + zpdes_rules.map_id_code",
        "exercise_content": "exercises.json",
        "attempt_events": "adaptiv_math_history.parquet",
    }
    sanitized["meta"] = meta

    id_label_index = sanitized.get("id_label_index")
    if isinstance(id_label_index, dict):
        for _, value in id_label_index.items():
            if not isinstance(value, dict):
                continue
            fallback = "catalog"
            value["sources"] = _normalized_sources(value.get("sources"), fallback_tag=fallback)

    return sanitized


def _sanitize_zpdes_rules(
    payload: dict[str, Any],
    source_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    sanitized = _sanitize_legacy_source_strings(
        _rename_legacy_keys(_drop_unwanted_keys(payload))
    )
    meta = sanitized.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta["source_files"] = source_entries
    meta["generated_by"] = "scripts/sanitize_metadata_standalone.py"
    meta["build_timestamp_utc"] = datetime.now(UTC).isoformat()
    if "history_file_used" in meta:
        meta["history_file_used"] = None
    sanitized["meta"] = meta

    links_to_catalog = sanitized.get("links_to_catalog")
    links_to_catalog = links_to_catalog if isinstance(links_to_catalog, dict) else {}

    module_rule_ids: set[str] = set()
    module_rules = sanitized.get("module_rules")
    if isinstance(module_rules, list):
        for item in module_rules:
            if not isinstance(item, dict):
                continue
            module_id = item.get("module_id")
            if isinstance(module_id, str) and module_id.strip():
                module_rule_ids.add(module_id.strip())

    # Keep deterministic output ordering for reproducibility.
    links_to_catalog["rule_module_ids"] = sorted(module_rule_ids)
    # Remove legacy naming now that the contract is standalone.
    links_to_catalog.pop("admath_module_ids", None)
    sanitized["links_to_catalog"] = links_to_catalog

    return sanitized


def _collect_forbidden_value_paths(obj: Any, prefix: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(obj, str):
        lower = obj.lower()
        if any(token in lower for token in FORBIDDEN_SOURCE_TOKENS):
            found.append((prefix, obj))
        return found
    if isinstance(obj, dict):
        for key, value in obj.items():
            child_prefix = f"{prefix}.{key}"
            found.extend(_collect_forbidden_value_paths(value, child_prefix))
        return found
    if isinstance(obj, list):
        for index, item in enumerate(obj):
            child_prefix = f"{prefix}[{index}]"
            found.extend(_collect_forbidden_value_paths(item, child_prefix))
        return found
    return found


def _collect_forbidden_key_paths(obj: Any, prefix: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_txt = str(key)
            key_lower = key_txt.lower()
            if any(token in key_lower for token in FORBIDDEN_KEY_TOKENS):
                found.append((prefix, key_txt))
            child_prefix = f"{prefix}.{key_txt}"
            found.extend(_collect_forbidden_key_paths(value, child_prefix))
    elif isinstance(obj, list):
        for index, item in enumerate(obj):
            found.extend(_collect_forbidden_key_paths(item, f"{prefix}[{index}]"))
    return found


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sanitize learning_catalog/zpdes_rules to a standalone 4-file metadata contract."
    )
    parser.add_argument(
        "--learning-catalog",
        type=Path,
        default=Path("data/learning_catalog.json"),
    )
    parser.add_argument(
        "--zpdes-rules",
        type=Path,
        default=Path("data/zpdes_rules.json"),
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Validate for forbidden legacy references without writing files.",
    )
    args = parser.parse_args()

    learning_catalog = _load_json(args.learning_catalog)
    zpdes_rules = _load_json(args.zpdes_rules)

    data_dir = args.learning_catalog.resolve().parent
    source_entries = _source_file_entries(data_dir)

    sanitized_catalog = _sanitize_learning_catalog(learning_catalog, source_entries)
    sanitized_rules = _sanitize_zpdes_rules(zpdes_rules, source_entries)

    catalog_value_hits = _collect_forbidden_value_paths(sanitized_catalog)
    rules_value_hits = _collect_forbidden_value_paths(sanitized_rules)
    catalog_key_hits = _collect_forbidden_key_paths(sanitized_catalog)
    rules_key_hits = _collect_forbidden_key_paths(sanitized_rules)
    if catalog_value_hits or rules_value_hits or catalog_key_hits or rules_key_hits:
        print("Found forbidden legacy filename references after sanitization:")
        for path, value in (catalog_value_hits + rules_value_hits):
            print(f"- {path}: {value}")
        for path, key in (catalog_key_hits + rules_key_hits):
            print(f"- {path} (key): {key}")
        return 1

    if args.check_only:
        print("Standalone metadata check: PASS")
        return 0

    _write_json(args.learning_catalog, sanitized_catalog)
    _write_json(args.zpdes_rules, sanitized_rules)
    print(f"Sanitized: {args.learning_catalog}")
    print(f"Sanitized: {args.zpdes_rules}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

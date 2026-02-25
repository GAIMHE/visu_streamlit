#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.loaders import load_learning_catalog, load_zpdes_rules

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

FORBIDDEN_PROVENANCE_KEYS = (
    "incoming_source",
    "source_primary",
    "source_enrichment",
    "only_in_summary",
    "only_in_xlsx",
    "only_in_primary",
    "only_in_secondary",
)
STANDALONE_PACKAGE_PATHS = {
    "data/adaptiv_math_history.parquet",
    "data/learning_catalog.json",
    "data/zpdes_rules.json",
    "data/exercises.json",
}


def _fail(errors: list[str], message: str) -> None:
    errors.append(message)


def _validate_source_files(meta: dict[str, Any], errors: list[str], owner: str) -> None:
    source_files = meta.get("source_files")
    if not isinstance(source_files, list):
        _fail(errors, f"{owner}.meta.source_files must be a list")
        return
    paths: set[str] = set()
    for idx, item in enumerate(source_files):
        if not isinstance(item, dict):
            _fail(errors, f"{owner}.meta.source_files[{idx}] must be an object")
            continue
        path = item.get("path")
        if not isinstance(path, str) or not path.strip():
            _fail(errors, f"{owner}.meta.source_files[{idx}].path must be a non-empty string")
            continue
        paths.add(path.strip())
    missing = sorted(STANDALONE_PACKAGE_PATHS - paths)
    if missing:
        _fail(errors, f"{owner}.meta.source_files missing standalone package paths: {missing}")


def _collect_forbidden_value_paths(obj: Any, prefix: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(obj, str):
        lower = obj.lower()
        if any(token in lower for token in FORBIDDEN_SOURCE_TOKENS):
            found.append((prefix, obj))
        return found
    if isinstance(obj, dict):
        for key, value in obj.items():
            found.extend(_collect_forbidden_value_paths(value, f"{prefix}.{key}"))
        return found
    if isinstance(obj, list):
        for idx, item in enumerate(obj):
            found.extend(_collect_forbidden_value_paths(item, f"{prefix}[{idx}]"))
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
            found.extend(_collect_forbidden_key_paths(value, f"{prefix}.{key_txt}"))
        return found
    if isinstance(obj, list):
        for idx, item in enumerate(obj):
            found.extend(_collect_forbidden_key_paths(item, f"{prefix}[{idx}]"))
        return found
    return found


def _collect_forbidden_provenance_key_paths(obj: Any, prefix: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(obj, dict):
        forbidden = {k.lower() for k in FORBIDDEN_PROVENANCE_KEYS}
        for key, value in obj.items():
            key_txt = str(key)
            key_lower = key_txt.lower()
            if key_lower in forbidden:
                found.append((prefix, key_txt))
            found.extend(_collect_forbidden_provenance_key_paths(value, f"{prefix}.{key_txt}"))
        return found
    if isinstance(obj, list):
        for idx, item in enumerate(obj):
            found.extend(_collect_forbidden_provenance_key_paths(item, f"{prefix}[{idx}]"))
        return found
    return found


def _validate_learning_catalog(payload: dict[str, Any], errors: list[str]) -> dict[str, int]:
    required = {"meta", "id_label_index", "modules", "exercise_to_hierarchy", "conflicts", "orphans"}
    missing = sorted(required - set(payload.keys()))
    if missing:
        _fail(errors, f"learning_catalog missing keys: {missing}")
        return {}

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        _fail(errors, "learning_catalog.meta must be an object")
    else:
        _validate_source_files(meta, errors, "learning_catalog")

    modules = payload.get("modules")
    if not isinstance(modules, list):
        _fail(errors, "learning_catalog.modules must be a list")
        return {}

    seen_module_ids: set[str] = set()
    seen_objective_ids: set[str] = set()
    seen_activity_ids: set[str] = set()
    nested_exercise_ids: set[str] = set()
    module_count = 0
    objective_count = 0
    activity_count = 0
    for module in modules:
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("id") or "").strip()
        if not module_id:
            _fail(errors, "module without id in learning_catalog.modules")
            continue
        module_count += 1
        if module_id in seen_module_ids:
            _fail(errors, f"duplicate module id in catalog modules: {module_id}")
        seen_module_ids.add(module_id)

        objectives = module.get("objectives")
        if objectives is None:
            objectives = []
        if not isinstance(objectives, list):
            _fail(errors, f"module {module_id} objectives must be a list")
            continue
        for objective in objectives:
            if not isinstance(objective, dict):
                continue
            objective_id = str(objective.get("id") or "").strip()
            if not objective_id:
                _fail(errors, f"objective without id under module {module_id}")
                continue
            objective_count += 1
            if objective_id in seen_objective_ids:
                _fail(errors, f"duplicate objective id in catalog modules: {objective_id}")
            seen_objective_ids.add(objective_id)

            activities = objective.get("activities")
            if activities is None:
                activities = []
            if not isinstance(activities, list):
                _fail(errors, f"objective {objective_id} activities must be a list")
                continue
            for activity in activities:
                if not isinstance(activity, dict):
                    continue
                activity_id = str(activity.get("id") or "").strip()
                if not activity_id:
                    _fail(errors, f"activity without id under objective {objective_id}")
                    continue
                activity_count += 1
                if activity_id in seen_activity_ids:
                    _fail(errors, f"duplicate activity id in catalog modules: {activity_id}")
                seen_activity_ids.add(activity_id)
                exercise_ids = activity.get("exercise_ids")
                if exercise_ids is None:
                    exercise_ids = []
                if not isinstance(exercise_ids, list):
                    _fail(errors, f"activity {activity_id} exercise_ids must be a list")
                    continue
                for exercise_id in exercise_ids:
                    exercise_text = str(exercise_id or "").strip()
                    if not exercise_text:
                        continue
                    nested_exercise_ids.add(exercise_text)

    exercise_map = payload.get("exercise_to_hierarchy")
    if not isinstance(exercise_map, dict):
        _fail(errors, "learning_catalog.exercise_to_hierarchy must be an object")
        exercise_map = {}
    mapped_exercise_ids = set(exercise_map.keys())

    if nested_exercise_ids != mapped_exercise_ids:
        only_nested = len(nested_exercise_ids - mapped_exercise_ids)
        only_flat = len(mapped_exercise_ids - nested_exercise_ids)
        _fail(
            errors,
            "exercise_to_hierarchy mismatch with nested modules: "
            f"only_nested={only_nested}, only_flat={only_flat}",
        )

    # Validate referenced IDs in exercise_to_hierarchy exist in nested hierarchy.
    for exercise_id, mapping in exercise_map.items():
        if not isinstance(mapping, dict):
            _fail(errors, f"exercise_to_hierarchy[{exercise_id}] must be an object")
            continue
        activity_id = str(mapping.get("activity_id") or "").strip()
        objective_id = str(mapping.get("objective_id") or "").strip()
        module_id = str(mapping.get("module_id") or "").strip()
        if activity_id and activity_id not in seen_activity_ids:
            _fail(errors, f"exercise {exercise_id} references unknown activity_id: {activity_id}")
        if objective_id and objective_id not in seen_objective_ids:
            _fail(errors, f"exercise {exercise_id} references unknown objective_id: {objective_id}")
        if module_id and module_id not in seen_module_ids:
            _fail(errors, f"exercise {exercise_id} references unknown module_id: {module_id}")

    return {
        "modules": module_count,
        "objectives": objective_count,
        "activities": activity_count,
        "exercise_to_hierarchy": len(mapped_exercise_ids),
    }


def _validate_zpdes_rules(payload: dict[str, Any], errors: list[str]) -> dict[str, int]:
    required = {"meta", "module_rules", "map_id_code", "links_to_catalog", "unresolved_links"}
    missing = sorted(required - set(payload.keys()))
    if missing:
        _fail(errors, f"zpdes_rules missing keys: {missing}")
        return {}

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        _fail(errors, "zpdes_rules.meta must be an object")
    else:
        _validate_source_files(meta, errors, "zpdes_rules")

    module_rules = payload.get("module_rules")
    if not isinstance(module_rules, list):
        _fail(errors, "zpdes_rules.module_rules must be a list")
        module_rules = []

    map_id_code = payload.get("map_id_code")
    if not isinstance(map_id_code, dict):
        _fail(errors, "zpdes_rules.map_id_code must be an object")
        map_id_code = {}
    code_to_id = map_id_code.get("code_to_id")
    id_to_codes = map_id_code.get("id_to_codes")
    if not isinstance(code_to_id, dict):
        _fail(errors, "zpdes_rules.map_id_code.code_to_id must be an object")
        code_to_id = {}
    if not isinstance(id_to_codes, dict):
        _fail(errors, "zpdes_rules.map_id_code.id_to_codes must be an object")
        id_to_codes = {}

    # Basic reciprocal sanity check.
    unresolved_pairs = 0
    for identifier, codes in id_to_codes.items():
        if not isinstance(codes, list):
            unresolved_pairs += 1
            continue
        for code in codes:
            if str(code) not in code_to_id:
                unresolved_pairs += 1
    if unresolved_pairs:
        _fail(errors, f"zpdes_rules map reciprocity mismatches: {unresolved_pairs}")

    links_to_catalog = payload.get("links_to_catalog")
    links_to_catalog = links_to_catalog if isinstance(links_to_catalog, dict) else {}
    rule_module_ids = links_to_catalog.get("rule_module_ids")
    if not isinstance(rule_module_ids, list):
        _fail(errors, "zpdes_rules.links_to_catalog.rule_module_ids must be a list")
    elif not rule_module_ids:
        _fail(errors, "zpdes_rules.links_to_catalog.rule_module_ids must not be empty")

    unresolved_links = payload.get("unresolved_links")
    unresolved_links = unresolved_links if isinstance(unresolved_links, dict) else {}
    if "rule_ids_missing_in_catalog" not in unresolved_links:
        _fail(errors, "zpdes_rules.unresolved_links.rule_ids_missing_in_catalog is required")
    if "catalog_module_ids_missing_in_rules" not in unresolved_links:
        _fail(errors, "zpdes_rules.unresolved_links.catalog_module_ids_missing_in_rules is required")

    topology = payload.get("dependency_topology")
    topology_count = 0
    if topology is not None:
        if not isinstance(topology, dict):
            _fail(errors, "zpdes_rules.dependency_topology must be an object when present")
        else:
            for module_code, module_entry in topology.items():
                if not isinstance(module_entry, dict):
                    _fail(errors, f"dependency_topology[{module_code}] must be an object")
                    continue
                nodes = module_entry.get("nodes")
                edges = module_entry.get("edges")
                if not isinstance(nodes, list):
                    _fail(errors, f"dependency_topology[{module_code}].nodes must be a list")
                    continue
                if not isinstance(edges, list):
                    _fail(errors, f"dependency_topology[{module_code}].edges must be a list")
                    continue
                topology_count += 1
    return {
        "module_rules": len(module_rules),
        "code_to_id": len(code_to_id),
        "id_to_codes": len(id_to_codes),
        "dependency_topology_modules": topology_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate learning_catalog/zpdes_rules contract shape.")
    parser.add_argument(
        "--learning-catalog",
        type=Path,
        default=ROOT_DIR / "data" / "learning_catalog.json",
    )
    parser.add_argument(
        "--zpdes-rules",
        type=Path,
        default=ROOT_DIR / "data" / "zpdes_rules.json",
    )
    args = parser.parse_args()

    errors: list[str] = []
    if not args.learning_catalog.exists():
        _fail(errors, f"Missing file: {args.learning_catalog}")
    if not args.zpdes_rules.exists():
        _fail(errors, f"Missing file: {args.zpdes_rules}")
    if errors:
        for err in errors:
            print(f"ERROR: {err}")
        return 1

    catalog = load_learning_catalog(args.learning_catalog)
    rules = load_zpdes_rules(args.zpdes_rules)

    forbidden_catalog = _collect_forbidden_value_paths(catalog)
    forbidden_rules = _collect_forbidden_value_paths(rules)
    forbidden_catalog_keys = _collect_forbidden_key_paths(catalog)
    forbidden_rules_keys = _collect_forbidden_key_paths(rules)
    forbidden_catalog_provenance = _collect_forbidden_provenance_key_paths(catalog)
    forbidden_rules_provenance = _collect_forbidden_provenance_key_paths(rules)
    if (
        forbidden_catalog
        or forbidden_rules
        or forbidden_catalog_keys
        or forbidden_rules_keys
        or forbidden_catalog_provenance
        or forbidden_rules_provenance
    ):
        for path, value in forbidden_catalog:
            _fail(errors, f"learning_catalog contains forbidden legacy source token at {path}: {value}")
        for path, value in forbidden_rules:
            _fail(errors, f"zpdes_rules contains forbidden legacy source token at {path}: {value}")
        for path, key in forbidden_catalog_keys:
            _fail(errors, f"learning_catalog contains forbidden legacy key token at {path}: {key}")
        for path, key in forbidden_rules_keys:
            _fail(errors, f"zpdes_rules contains forbidden legacy key token at {path}: {key}")
        for path, key in forbidden_catalog_provenance:
            _fail(errors, f"learning_catalog contains forbidden provenance key at {path}: {key}")
        for path, key in forbidden_rules_provenance:
            _fail(errors, f"zpdes_rules contains forbidden provenance key at {path}: {key}")

    catalog_summary = _validate_learning_catalog(catalog, errors)
    rules_summary = _validate_zpdes_rules(rules, errors)

    if errors:
        for err in errors:
            print(f"ERROR: {err}")
        return 1

    print("Metadata contracts: PASS")
    print(
        "learning_catalog counts: "
        f"modules={catalog_summary.get('modules', 0)}, "
        f"objectives={catalog_summary.get('objectives', 0)}, "
        f"activities={catalog_summary.get('activities', 0)}, "
        f"exercise_to_hierarchy={catalog_summary.get('exercise_to_hierarchy', 0)}"
    )
    print(
        "zpdes_rules counts: "
        f"module_rules={rules_summary.get('module_rules', 0)}, "
        f"code_to_id={rules_summary.get('code_to_id', 0)}, "
        f"id_to_codes={rules_summary.get('id_to_codes', 0)}, "
        f"dependency_topology_modules={rules_summary.get('dependency_topology_modules', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

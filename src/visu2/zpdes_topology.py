"""Topology normalization for ZPDES dependency metadata."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .loaders import (
    catalog_id_index_frames,
    load_learning_catalog,
    load_zpdes_rules,
    zpdes_code_maps,
)
from .zpdes_types import (
    clean_str,
    empty_edges_df,
    empty_nodes_df,
    first_int,
    first_numeric,
    is_init_open_from_rule,
    label_from_catalog_entry,
    node_type_from_code_strict,
    parse_activity_index,
    parse_objective_code,
    preferred_code_from_list,
)


def _selected_catalog_module(
    catalog_payload: dict[str, Any],
    module_code: str,
) -> dict[str, Any] | None:
    """Return the catalog module payload for one pedagogical module code."""
    module_code_norm = clean_str(module_code)
    return next(
        (
            module
            for module in (catalog_payload.get("modules") or [])
            if isinstance(module, dict) and clean_str(module.get("code")) == module_code_norm
        ),
        None,
    )


def _catalog_node_map_for_module(
    catalog_payload: dict[str, Any],
    module_code: str,
) -> dict[str, dict[str, object]]:
    """Build the canonical node map for a module directly from the learning catalog."""
    module_code_norm = clean_str(module_code)
    selected_catalog_module = _selected_catalog_module(catalog_payload, module_code_norm)
    node_map: dict[str, dict[str, object]] = {}
    if not isinstance(selected_catalog_module, dict):
        return node_map

    for objective in selected_catalog_module.get("objectives") or []:
        if not isinstance(objective, dict):
            continue
        objective_code = clean_str(objective.get("code"))
        objective_id = clean_str(objective.get("id"))
        if not objective_code:
            continue
        node_map[objective_code] = {
            "module_code": module_code_norm,
            "node_id": objective_id or None,
            "node_code": objective_code,
            "node_type": "objective",
            "label": label_from_catalog_entry(objective, objective_code),
            "objective_code": objective_code,
            "activity_index": None,
            "init_open": False,
            "source_primary": "catalog",
            "source_enrichment": "rules",
            "is_ghost": False,
        }
        for activity in objective.get("activities") or []:
            if not isinstance(activity, dict):
                continue
            activity_code = clean_str(activity.get("code"))
            activity_id = clean_str(activity.get("id"))
            if not activity_code:
                continue
            node_map[activity_code] = {
                "module_code": module_code_norm,
                "node_id": activity_id or None,
                "node_code": activity_code,
                "node_type": "activity",
                "label": label_from_catalog_entry(activity, activity_code),
                "objective_code": objective_code,
                "activity_index": parse_activity_index(activity_code),
                "init_open": False,
                "source_primary": "catalog",
                "source_enrichment": "rules",
                "is_ghost": False,
            }
    return node_map


def _normalize_topology_tables(
    nodes_raw: list[object],
    edges_raw: list[object],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Normalize dependency-topology payloads to the runtime node and edge schemas."""
    nodes_df = pl.DataFrame(nodes_raw) if nodes_raw else empty_nodes_df()
    edges_df = pl.DataFrame(edges_raw) if edges_raw else empty_edges_df()
    for column, dtype in empty_nodes_df().schema.items():
        if column not in nodes_df.columns:
            nodes_df = nodes_df.with_columns(pl.lit(None, dtype=dtype).alias(column))
    for column, dtype in empty_edges_df().schema.items():
        if column not in edges_df.columns:
            edges_df = edges_df.with_columns(pl.lit(None, dtype=dtype).alias(column))
    return (
        nodes_df.select(list(empty_nodes_df().schema.keys())),
        edges_df.select(list(empty_edges_df().schema.keys())),
    )


def _reconcile_topology_nodes_with_catalog(
    module_code: str,
    catalog_payload: dict[str, Any],
    nodes_df: pl.DataFrame,
) -> tuple[pl.DataFrame, list[str]]:
    """Reconcile topology nodes with canonical catalog ids, labels, and membership."""
    module_code_norm = clean_str(module_code)
    canonical_map = _catalog_node_map_for_module(catalog_payload, module_code_norm)
    topology_map = {
        clean_str(row.get("node_code")): row
        for row in nodes_df.to_dicts()
        if clean_str(row.get("node_code"))
    }
    warnings: list[str] = []
    corrected_count = 0
    added_count = 0

    merged_rows: list[dict[str, object]] = []
    for node_code in sorted(set(canonical_map) | set(topology_map)):
        topology_row = topology_map.get(node_code)
        canonical_row = canonical_map.get(node_code)
        if canonical_row is None:
            if topology_row is not None:
                merged_rows.append(dict(topology_row))
            continue

        merged = dict(canonical_row)
        if topology_row is not None:
            merged["init_open"] = bool(topology_row.get("init_open"))
            merged["source_primary"] = clean_str(topology_row.get("source_primary")) or merged["source_primary"]
            merged["source_enrichment"] = (
                clean_str(topology_row.get("source_enrichment")) or merged["source_enrichment"]
            )
            merged["is_ghost"] = bool(topology_row.get("is_ghost"))
            if (
                clean_str(topology_row.get("node_id")) != clean_str(canonical_row.get("node_id"))
                or clean_str(topology_row.get("label")) != clean_str(canonical_row.get("label"))
                or clean_str(topology_row.get("objective_code")) != clean_str(canonical_row.get("objective_code"))
            ):
                corrected_count += 1
        else:
            added_count += 1
        merged_rows.append(merged)

    if corrected_count:
        warnings.append(
            f"Reconciled {corrected_count} topology node(s) against canonical learning_catalog metadata."
        )
    if added_count:
        warnings.append(
            f"Added {added_count} catalog node(s) missing from dependency_topology."
        )

    if not merged_rows:
        return empty_nodes_df(), warnings
    return pl.DataFrame(merged_rows).select(list(empty_nodes_df().schema.keys())), warnings


def _build_dependency_tables_from_rules_payload(
    module_code: str,
    catalog_payload: dict[str, Any],
    rules_payload: dict[str, Any],
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    """Build dependency tables from rules payload.

Parameters
----------
module_code : str
        Input parameter used by this routine.
catalog_payload : dict[str, Any]
        Input parameter used by this routine.
rules_payload : dict[str, Any]
        Input parameter used by this routine.

Returns
-------
tuple[pl.DataFrame, pl.DataFrame, list[str]]
        Result produced by this routine.

"""
    module_code_norm = clean_str(module_code)
    warnings: list[str] = []

    rules_modules = rules_payload.get("module_rules")
    rules_modules = rules_modules if isinstance(rules_modules, list) else []
    module_rule = next(
        (
            row
            for row in rules_modules
            if isinstance(row, dict) and clean_str(row.get("module_code")) == module_code_norm
        ),
        None,
    )
    if not isinstance(module_rule, dict):
        return (
            empty_nodes_df(),
            empty_edges_df(),
            [f"Module '{module_code_norm}' not found in zpdes_rules.module_rules."],
        )

    code_to_id_local = module_rule.get("map_id_code")
    code_to_id_local = code_to_id_local if isinstance(code_to_id_local, dict) else {}
    id_to_codes_local: dict[str, list[str]] = {}
    for code, identifier in code_to_id_local.items():
        code_txt = clean_str(code)
        id_txt = clean_str(identifier)
        if not code_txt or not id_txt:
            continue
        id_to_codes_local.setdefault(id_txt, []).append(code_txt)

    global_maps = zpdes_code_maps(rules_payload)
    code_to_id_global = global_maps["code_to_id"]
    index_frames = catalog_id_index_frames(catalog_payload)
    id_to_label = {
        clean_str(row.get("id")): clean_str(row.get("short_title")) or clean_str(row.get("long_title"))
        for row in index_frames.index.to_dicts()
        if clean_str(row.get("id"))
    }

    node_map = _catalog_node_map_for_module(catalog_payload, module_code_norm)
    for node in node_map.values():
        if not clean_str(node.get("node_id")):
            code = clean_str(node.get("node_code"))
            node["node_id"] = code_to_id_global.get(code) or None

    node_rules = module_rule.get("node_rules")
    node_rules = node_rules if isinstance(node_rules, list) else []
    rule_by_code: dict[str, dict[str, object]] = {}
    for node in node_rules:
        if not isinstance(node, dict):
            continue
        code = clean_str(node.get("code"))
        node_id = clean_str(node.get("id"))
        if not code and node_id:
            code = preferred_code_from_list(id_to_codes_local.get(node_id, [])) or ""
        if not code:
            continue
        rule_by_code[code] = node
        if code not in node_map and node_type_from_code_strict(code) in {"objective", "activity"}:
            label = id_to_label.get(node_id) or code
            node_type = clean_str(node.get("type")) or node_type_from_code_strict(code)
            node_map[code] = {
                "module_code": module_code_norm,
                "node_id": node_id or None,
                "node_code": code,
                "node_type": node_type if node_type in {"objective", "activity"} else node_type_from_code_strict(code),
                "label": label,
                "objective_code": parse_objective_code(code, node_type=node_type),
                "activity_index": parse_activity_index(code),
                "init_open": False,
                "source_primary": "rules",
                "source_enrichment": "rules",
                "is_ghost": False,
            }

    def _resolve_code_from_id(identifier: object) -> str:
        """Resolve code from id.

Parameters
----------
identifier : object
            Input parameter used by this routine.

Returns
-------
str
            Result produced by this routine.

Notes
-----
        Behavior is intentionally documented for maintainability and traceability.
"""
        ident = clean_str(identifier)
        if not ident:
            return ""
        preferred = preferred_code_from_list(id_to_codes_local.get(ident, []))
        return preferred or ""

    edges: list[dict[str, object]] = []
    edge_seen: set[tuple[str, str, str, float | None]] = set()

    def _add_edge(
        edge_type: str,
        from_code: str,
        to_code: str,
        threshold: float | None,
        rule_text: str,
        enrich_lvl: int | None,
        enrich_sr: float | None,
    ) -> None:
        """Add edge.

Parameters
----------
edge_type : str
            Input parameter used by this routine.
from_code : str
            Input parameter used by this routine.
to_code : str
            Input parameter used by this routine.
threshold : float | None
            Input parameter used by this routine.
rule_text : str
            Input parameter used by this routine.
enrich_lvl : int | None
            Input parameter used by this routine.
enrich_sr : float | None
            Input parameter used by this routine.

Returns
-------
None
            Result produced by this routine.

Notes
-----
        Behavior is intentionally documented for maintainability and traceability.
"""
        src = clean_str(from_code)
        dst = clean_str(to_code)
        if not src or not dst:
            return
        threshold_norm = None if threshold is None else round(float(threshold), 6)
        dedup = (edge_type, src, dst, threshold_norm)
        if dedup in edge_seen:
            return
        edge_seen.add(dedup)
        edges.append(
            {
                "module_code": module_code_norm,
                "edge_id": f"{module_code_norm}:{edge_type}:{src}->{dst}:{len(edges)+1}",
                "edge_type": edge_type,
                "from_node_code": src,
                "to_node_code": dst,
                "threshold_type": "success_rate" if threshold_norm is not None else "unknown",
                "threshold_value": threshold_norm,
                "rule_text": rule_text,
                "source_primary": "rules",
                "source_enrichment": "rules",
                "enrich_lvl": enrich_lvl,
                "enrich_sr": enrich_sr,
            }
        )

    for code, node in rule_by_code.items():
        rules = node.get("rules") if isinstance(node.get("rules"), dict) else {}
        if not isinstance(rules, dict):
            continue
        if is_init_open_from_rule(rules.get("init_ssb")) and code in node_map:
            node_map[code]["init_open"] = True

        requirements = rules.get("requirements")
        if isinstance(requirements, list) and requirements and isinstance(requirements[0], dict):
            req_map = requirements[0]
            for target_id, source_map in req_map.items():
                target_code = _resolve_code_from_id(target_id) or code
                if not isinstance(source_map, dict):
                    continue
                for source_id, condition in source_map.items():
                    source_code = _resolve_code_from_id(source_id)
                    condition_dict = condition if isinstance(condition, dict) else {}
                    sr = first_numeric(condition_dict.get("sr")) if condition_dict else None
                    lvl = first_int(condition_dict.get("lvl")) if condition_dict else None
                    _add_edge("activation", source_code, target_code, sr, "requirements", lvl, sr)

        deact_requirements = rules.get("deact_requirements")
        if isinstance(deact_requirements, list) and deact_requirements and isinstance(deact_requirements[0], dict):
            deact_map = deact_requirements[0]
            for target_id, conditions in deact_map.items():
                target_code = _resolve_code_from_id(target_id) or code
                if isinstance(conditions, dict):
                    conditions = [conditions]
                if not isinstance(conditions, list):
                    continue
                for condition in conditions:
                    if not isinstance(condition, dict):
                        continue
                    source_code = _resolve_code_from_id(condition.get("dim"))
                    sr = first_numeric(condition.get("sr"))
                    lvl = first_int(condition.get("lvl"))
                    _add_edge(
                        "deactivation",
                        source_code,
                        target_code,
                        sr,
                        "deact_requirements",
                        lvl,
                        sr,
                    )

    incoming_activation_targets = {
        clean_str(edge["to_node_code"])
        for edge in edges
        if clean_str(edge.get("edge_type")) == "activation"
    }
    for node_code, node in node_map.items():
        if not bool(node.get("init_open")) and node_code not in incoming_activation_targets:
            node["init_open"] = True

    referenced_codes = {clean_str(edge["from_node_code"]) for edge in edges} | {
        clean_str(edge["to_node_code"]) for edge in edges
    }
    missing_codes = sorted(code_ref for code_ref in referenced_codes if code_ref and code_ref not in node_map)
    for ghost_code in missing_codes:
        ghost_type = node_type_from_code_strict(ghost_code)
        if ghost_type not in {"objective", "activity"}:
            continue
        ghost_id = code_to_id_global.get(ghost_code)
        node_map[ghost_code] = {
            "module_code": module_code_norm,
            "node_id": ghost_id,
            "node_code": ghost_code,
            "node_type": ghost_type,
            "label": id_to_label.get(clean_str(ghost_id)) or ghost_code,
            "objective_code": parse_objective_code(ghost_code, node_type=ghost_type),
            "activity_index": parse_activity_index(ghost_code),
            "init_open": False,
            "source_primary": "rules",
            "source_enrichment": "rules",
            "is_ghost": True,
        }
    if missing_codes:
        warnings.append(f"Created {len(missing_codes)} ghost node(s) for unresolved rules references.")

    nodes_df = (
        pl.DataFrame(list(node_map.values())).select(list(empty_nodes_df().schema.keys()))
        if node_map
        else empty_nodes_df()
    )
    edges_df = (
        pl.DataFrame(edges).select(list(empty_edges_df().schema.keys()))
        if edges
        else empty_edges_df()
    )
    return nodes_df, edges_df, warnings


def build_dependency_tables_from_metadata(
    module_code: str,
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    """Build dependency node/edge tables from the standalone metadata package."""
    catalog_payload = load_learning_catalog(learning_catalog_path)
    rules_payload = load_zpdes_rules(zpdes_rules_path)

    topology = rules_payload.get("dependency_topology")
    if isinstance(topology, dict):
        module_entry = topology.get(module_code)
        if isinstance(module_entry, dict):
            nodes_raw = module_entry.get("nodes")
            edges_raw = module_entry.get("edges")
            nodes_raw = nodes_raw if isinstance(nodes_raw, list) else []
            edges_raw = edges_raw if isinstance(edges_raw, list) else []
            if nodes_raw or edges_raw:
                nodes_df, edges_df = _normalize_topology_tables(nodes_raw=nodes_raw, edges_raw=edges_raw)
                nodes_df, warnings = _reconcile_topology_nodes_with_catalog(
                    module_code=module_code,
                    catalog_payload=catalog_payload,
                    nodes_df=nodes_df,
                )
                return (
                    nodes_df,
                    edges_df,
                    warnings,
                )

    return _build_dependency_tables_from_rules_payload(
        module_code=module_code,
        catalog_payload=catalog_payload,
        rules_payload=rules_payload,
    )


def list_supported_module_codes_from_metadata(
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
    observed_module_codes: set[str] | None = None,
) -> list[str]:
    """Return the supported module codes shared by the rule metadata and observed data."""
    del learning_catalog_path
    rules_payload = load_zpdes_rules(zpdes_rules_path)
    module_rules = rules_payload.get("module_rules")
    module_rules = module_rules if isinstance(module_rules, list) else []
    module_codes = {
        clean_str(row.get("module_code"))
        for row in module_rules
        if isinstance(row, dict) and clean_str(row.get("module_code"))
    }
    maps = zpdes_code_maps(rules_payload)
    code_to_id = maps["code_to_id"]
    links_to_catalog = rules_payload.get("links_to_catalog")
    links_to_catalog = links_to_catalog if isinstance(links_to_catalog, dict) else {}
    rule_module_ids = links_to_catalog.get("rule_module_ids")
    if not isinstance(rule_module_ids, list):
        rule_module_ids = links_to_catalog.get("admath_module_ids")
    if isinstance(rule_module_ids, list):
        allowed_ids = {clean_str(module_id) for module_id in rule_module_ids if clean_str(module_id)}
        if allowed_ids:
            module_codes = {
                code for code in module_codes if clean_str(code_to_id.get(code)) in allowed_ids
            }
    if observed_module_codes is not None:
        observed = {clean_str(code) for code in observed_module_codes if clean_str(code)}
        module_codes &= observed
    return sorted(
        module_codes,
        key=lambda code: int(code[1:]) if code.startswith("M") and code[1:].isdigit() else 10**9,
    )

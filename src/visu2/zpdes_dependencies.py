from __future__ import annotations

import math
import re
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from .loaders import catalog_id_index_frames, load_learning_catalog, load_zpdes_rules, zpdes_code_maps

MODULE_CODE_RE = re.compile(r"^M\d+$")
OBJECTIVE_CODE_RE = re.compile(r"^(M\d+O\d+)$")
ACTIVITY_CODE_RE = re.compile(r"^(M\d+O\d+)A(\d+)$")
DEPENDENCY_TOKEN_RE = re.compile(r"(M\d+O\d+(?:A\d+)?)(?:\((\d+(?:\.\d+)?)%\))?")


def _clean_str(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _to_float(value: object) -> float | None:
    text = _clean_str(value)
    if not text:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    if math.isnan(out):
        return None
    return out


def _first_numeric(value: object) -> float | None:
    if isinstance(value, list):
        for item in value:
            out = _first_numeric(item)
            if out is not None:
                return out
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return _to_float(value)


def _first_int(value: object) -> int | None:
    numeric = _first_numeric(value)
    if numeric is None:
        return None
    return int(round(numeric))


def parse_dependency_tokens(raw_value: object) -> list[dict[str, object]]:
    text = _clean_str(raw_value)
    if not text:
        return []

    tokens: list[dict[str, object]] = []
    seen: set[tuple[str, float | None]] = set()
    for match in DEPENDENCY_TOKEN_RE.finditer(text):
        code = _clean_str(match.group(1))
        if not code:
            continue
        pct_raw = _clean_str(match.group(2))
        threshold = (float(pct_raw) / 100.0) if pct_raw else None
        key = (code, threshold)
        if key in seen:
            continue
        seen.add(key)
        tokens.append({"code": code, "threshold": threshold})
    return tokens


def parse_activity_index(node_code: str) -> int | None:
    match = ACTIVITY_CODE_RE.match(_clean_str(node_code))
    if not match:
        return None
    return int(match.group(2))


def parse_objective_code(node_code: str, node_type: str | None = None) -> str | None:
    code = _clean_str(node_code)
    if not code:
        return None
    if node_type == "objective" and OBJECTIVE_CODE_RE.match(code):
        return code
    activity_match = ACTIVITY_CODE_RE.match(code)
    if activity_match:
        return activity_match.group(1)
    objective_match = OBJECTIVE_CODE_RE.match(code)
    if objective_match:
        return objective_match.group(1)
    return None


def _empty_nodes_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "module_code": [],
            "node_id": [],
            "node_code": [],
            "node_type": [],
            "label": [],
            "objective_code": [],
            "activity_index": [],
            "init_open": [],
            "source_primary": [],
            "source_enrichment": [],
            "is_ghost": [],
        },
        schema={
            "module_code": pl.Utf8,
            "node_id": pl.Utf8,
            "node_code": pl.Utf8,
            "node_type": pl.Utf8,
            "label": pl.Utf8,
            "objective_code": pl.Utf8,
            "activity_index": pl.Int64,
            "init_open": pl.Boolean,
            "source_primary": pl.Utf8,
            "source_enrichment": pl.Utf8,
            "is_ghost": pl.Boolean,
        },
    )


def _empty_edges_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "module_code": [],
            "edge_id": [],
            "edge_type": [],
            "from_node_code": [],
            "to_node_code": [],
            "threshold_type": [],
            "threshold_value": [],
            "rule_text": [],
            "source_primary": [],
            "source_enrichment": [],
            "enrich_lvl": [],
            "enrich_sr": [],
        },
        schema={
            "module_code": pl.Utf8,
            "edge_id": pl.Utf8,
            "edge_type": pl.Utf8,
            "from_node_code": pl.Utf8,
            "to_node_code": pl.Utf8,
            "threshold_type": pl.Utf8,
            "threshold_value": pl.Float64,
            "rule_text": pl.Utf8,
            "source_primary": pl.Utf8,
            "source_enrichment": pl.Utf8,
            "enrich_lvl": pl.Int64,
            "enrich_sr": pl.Float64,
        },
    )


def _label_from_catalog_entry(entry: object, fallback: str) -> str:
    if isinstance(entry, dict):
        title = entry.get("title")
        if isinstance(title, dict):
            short = _clean_str(title.get("short"))
            long = _clean_str(title.get("long"))
            if short:
                return short
            if long:
                return long
    return fallback


def _node_type_from_code_strict(code: str) -> str:
    normalized = _clean_str(code)
    if ACTIVITY_CODE_RE.match(normalized):
        return "activity"
    if OBJECTIVE_CODE_RE.match(normalized):
        return "objective"
    if MODULE_CODE_RE.match(normalized):
        return "module"
    return "unknown"


def _is_init_open_from_rule(rule_payload: object) -> bool:
    if isinstance(rule_payload, list):
        for item in rule_payload:
            if _is_init_open_from_rule(item):
                return True
        return False
    if isinstance(rule_payload, dict):
        for value in rule_payload.values():
            if _is_init_open_from_rule(value):
                return True
        return False
    if isinstance(rule_payload, (int, float)):
        return int(rule_payload) == 0
    text = _clean_str(rule_payload)
    return text == "0"


def _preferred_code_from_list(codes: list[str]) -> str | None:
    cleaned = [_clean_str(code) for code in codes if _clean_str(code)]
    if not cleaned:
        return None
    return sorted(cleaned, key=lambda code: (0 if ACTIVITY_CODE_RE.match(code) else 1 if OBJECTIVE_CODE_RE.match(code) else 2 if MODULE_CODE_RE.match(code) else 3, -len(code), code))[0]


def _build_dependency_tables_from_rules_payload(
    module_code: str,
    catalog_payload: dict[str, Any],
    rules_payload: dict[str, Any],
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    module_code_norm = _clean_str(module_code)
    warnings: list[str] = []

    rules_modules = rules_payload.get("module_rules")
    rules_modules = rules_modules if isinstance(rules_modules, list) else []
    module_rule = next(
        (
            row
            for row in rules_modules
            if isinstance(row, dict) and _clean_str(row.get("module_code")) == module_code_norm
        ),
        None,
    )
    if not isinstance(module_rule, dict):
        return _empty_nodes_df(), _empty_edges_df(), [f"Module '{module_code_norm}' not found in zpdes_rules.module_rules."]

    code_to_id_local = module_rule.get("map_id_code")
    code_to_id_local = code_to_id_local if isinstance(code_to_id_local, dict) else {}
    id_to_codes_local: dict[str, list[str]] = {}
    for code, identifier in code_to_id_local.items():
        code_txt = _clean_str(code)
        id_txt = _clean_str(identifier)
        if not code_txt or not id_txt:
            continue
        id_to_codes_local.setdefault(id_txt, []).append(code_txt)

    global_maps = zpdes_code_maps(rules_payload)
    code_to_id_global = global_maps["code_to_id"]
    index_frames = catalog_id_index_frames(catalog_payload)
    id_index_rows = index_frames.index.to_dicts()
    id_to_label = {
        _clean_str(row.get("id")): _clean_str(row.get("short_title")) or _clean_str(row.get("long_title"))
        for row in id_index_rows
        if _clean_str(row.get("id"))
    }

    selected_catalog_module = next(
        (
            module
            for module in (catalog_payload.get("modules") or [])
            if isinstance(module, dict) and _clean_str(module.get("code")) == module_code_norm
        ),
        None,
    )

    node_map: dict[str, dict[str, object]] = {}
    if isinstance(selected_catalog_module, dict):
        for objective in selected_catalog_module.get("objectives") or []:
            if not isinstance(objective, dict):
                continue
            objective_code = _clean_str(objective.get("code"))
            objective_id = _clean_str(objective.get("id")) or code_to_id_global.get(objective_code)
            if not objective_code:
                continue
            node_map[objective_code] = {
                "module_code": module_code_norm,
                "node_id": objective_id or None,
                "node_code": objective_code,
                "node_type": "objective",
                "label": _label_from_catalog_entry(objective, objective_code),
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
                activity_code = _clean_str(activity.get("code"))
                activity_id = _clean_str(activity.get("id")) or code_to_id_global.get(activity_code)
                if not activity_code:
                    continue
                node_map[activity_code] = {
                    "module_code": module_code_norm,
                    "node_id": activity_id or None,
                    "node_code": activity_code,
                    "node_type": "activity",
                    "label": _label_from_catalog_entry(activity, activity_code),
                    "objective_code": objective_code,
                    "activity_index": parse_activity_index(activity_code),
                    "init_open": False,
                    "source_primary": "catalog",
                    "source_enrichment": "rules",
                    "is_ghost": False,
                }

    node_rules = module_rule.get("node_rules")
    node_rules = node_rules if isinstance(node_rules, list) else []
    rule_by_code: dict[str, dict[str, object]] = {}
    for node in node_rules:
        if not isinstance(node, dict):
            continue
        code = _clean_str(node.get("code"))
        node_id = _clean_str(node.get("id"))
        if not code and node_id:
            code = _preferred_code_from_list(id_to_codes_local.get(node_id, [])) or ""
        if not code:
            continue
        rule_by_code[code] = node
        if code not in node_map and _node_type_from_code_strict(code) in {"objective", "activity"}:
            label = id_to_label.get(node_id) or code
            node_type = _clean_str(node.get("type")) or _node_type_from_code_strict(code)
            node_map[code] = {
                "module_code": module_code_norm,
                "node_id": node_id or None,
                "node_code": code,
                "node_type": node_type if node_type in {"objective", "activity"} else _node_type_from_code_strict(code),
                "label": label,
                "objective_code": parse_objective_code(code, node_type=node_type),
                "activity_index": parse_activity_index(code),
                "init_open": False,
                "source_primary": "rules",
                "source_enrichment": "rules",
                "is_ghost": False,
            }

    def _resolve_code_from_id(identifier: object) -> str:
        ident = _clean_str(identifier)
        if not ident:
            return ""
        preferred = _preferred_code_from_list(id_to_codes_local.get(ident, []))
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
        src = _clean_str(from_code)
        dst = _clean_str(to_code)
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
        init_ssb = rules.get("init_ssb")
        if _is_init_open_from_rule(init_ssb) and code in node_map:
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
                    sr = _first_numeric(condition_dict.get("sr")) if condition_dict else None
                    lvl = _first_int(condition_dict.get("lvl")) if condition_dict else None
                    _add_edge(
                        edge_type="activation",
                        from_code=source_code,
                        to_code=target_code,
                        threshold=sr,
                        rule_text="requirements",
                        enrich_lvl=lvl,
                        enrich_sr=sr,
                    )

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
                    sr = _first_numeric(condition.get("sr"))
                    lvl = _first_int(condition.get("lvl"))
                    _add_edge(
                        edge_type="deactivation",
                        from_code=source_code,
                        to_code=target_code,
                        threshold=sr,
                        rule_text="deact_requirements",
                        enrich_lvl=lvl,
                        enrich_sr=sr,
                    )

    incoming_activation_targets = {
        _clean_str(edge["to_node_code"])
        for edge in edges
        if _clean_str(edge.get("edge_type")) == "activation"
    }
    for node_code, node in node_map.items():
        if not bool(node.get("init_open")) and node_code not in incoming_activation_targets:
            node["init_open"] = True

    referenced_codes = {
        _clean_str(edge["from_node_code"]) for edge in edges
    } | {_clean_str(edge["to_node_code"]) for edge in edges}
    missing_codes = sorted(
        code_ref for code_ref in referenced_codes if code_ref and code_ref not in node_map
    )
    for ghost_code in missing_codes:
        ghost_type = _node_type_from_code_strict(ghost_code)
        if ghost_type not in {"objective", "activity"}:
            continue
        ghost_id = code_to_id_global.get(ghost_code)
        node_map[ghost_code] = {
            "module_code": module_code_norm,
            "node_id": ghost_id,
            "node_code": ghost_code,
            "node_type": ghost_type,
            "label": id_to_label.get(_clean_str(ghost_id)) or ghost_code,
            "objective_code": parse_objective_code(ghost_code, node_type=ghost_type),
            "activity_index": parse_activity_index(ghost_code),
            "init_open": False,
            "source_primary": "rules",
            "source_enrichment": "rules",
            "is_ghost": True,
        }
    if missing_codes:
        warnings.append(
            f"Created {len(missing_codes)} ghost node(s) for unresolved rules references."
        )

    nodes_df = (
        pl.DataFrame(list(node_map.values())).select(
            [
                "module_code",
                "node_id",
                "node_code",
                "node_type",
                "label",
                "objective_code",
                "activity_index",
                "init_open",
                "source_primary",
                "source_enrichment",
                "is_ghost",
            ]
        )
        if node_map
        else _empty_nodes_df()
    )
    edges_df = pl.DataFrame(edges).select(
        [
            "module_code",
            "edge_id",
            "edge_type",
            "from_node_code",
            "to_node_code",
            "threshold_type",
            "threshold_value",
            "rule_text",
            "source_primary",
            "source_enrichment",
            "enrich_lvl",
            "enrich_sr",
        ]
    ) if edges else _empty_edges_df()
    return nodes_df, edges_df, warnings


def build_dependency_tables_from_metadata(
    module_code: str,
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
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
                nodes_df = pl.DataFrame(nodes_raw) if nodes_raw else _empty_nodes_df()
                edges_df = pl.DataFrame(edges_raw) if edges_raw else _empty_edges_df()
                # Ensure compatibility columns exist.
                for column, dtype in _empty_nodes_df().schema.items():
                    if column not in nodes_df.columns:
                        nodes_df = nodes_df.with_columns(pl.lit(None, dtype=dtype).alias(column))
                for column, dtype in _empty_edges_df().schema.items():
                    if column not in edges_df.columns:
                        edges_df = edges_df.with_columns(pl.lit(None, dtype=dtype).alias(column))
                nodes_df = nodes_df.select(list(_empty_nodes_df().schema.keys()))
                edges_df = edges_df.select(list(_empty_edges_df().schema.keys()))
                return nodes_df, edges_df, []

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
    rules_payload = load_zpdes_rules(zpdes_rules_path)
    module_rules = rules_payload.get("module_rules")
    module_rules = module_rules if isinstance(module_rules, list) else []
    module_codes = {
        _clean_str(row.get("module_code"))
        for row in module_rules
        if isinstance(row, dict) and _clean_str(row.get("module_code"))
    }
    maps = zpdes_code_maps(rules_payload)
    code_to_id = maps["code_to_id"]
    links_to_catalog = rules_payload.get("links_to_catalog")
    links_to_catalog = links_to_catalog if isinstance(links_to_catalog, dict) else {}
    rule_module_ids = links_to_catalog.get("rule_module_ids")
    if not isinstance(rule_module_ids, list):
        # Backward-compatible fallback for pre-migration payloads.
        rule_module_ids = links_to_catalog.get("admath_module_ids")
    if isinstance(rule_module_ids, list):
        allowed_ids = {_clean_str(module_id) for module_id in rule_module_ids if _clean_str(module_id)}
        if allowed_ids:
            module_codes = {
                code for code in module_codes if _clean_str(code_to_id.get(code)) in allowed_ids
            }
    if observed_module_codes is not None:
        observed = {_clean_str(code) for code in observed_module_codes if _clean_str(code)}
        module_codes &= observed
    return sorted(module_codes, key=lambda code: int(code[1:]) if code.startswith("M") and code[1:].isdigit() else 10**9)


def attach_overlay_metrics_to_nodes(
    nodes: pl.DataFrame,
    agg_activity_daily: pl.DataFrame | pl.LazyFrame,
    module_code: str,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    frame = agg_activity_daily.collect() if isinstance(agg_activity_daily, pl.LazyFrame) else agg_activity_daily
    required = {"date_utc", "module_code", "objective_id", "activity_id", "attempts", "success_rate", "repeat_attempt_rate"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"agg_activity_daily is missing required columns: {missing}")

    filtered = frame.filter(
        (pl.col("module_code") == module_code)
        & (pl.col("date_utc") >= pl.lit(start_date))
        & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if filtered.height == 0:
        return nodes.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("overlay_attempts"),
            pl.lit(None, dtype=pl.Float64).alias("overlay_success_rate"),
            pl.lit(None, dtype=pl.Float64).alias("overlay_repeat_attempt_rate"),
        )

    activity_metrics = (
        filtered.group_by("activity_id")
        .agg(
            pl.sum("attempts").cast(pl.Float64).alias("activity_attempts"),
            ((pl.col("success_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("activity_success_rate"),
            ((pl.col("repeat_attempt_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("activity_repeat_attempt_rate"),
        )
        .rename({"activity_id": "node_id"})
    )

    objective_metrics = (
        filtered.group_by("objective_id")
        .agg(
            pl.sum("attempts").cast(pl.Float64).alias("objective_attempts"),
            ((pl.col("success_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("objective_success_rate"),
            ((pl.col("repeat_attempt_rate") * pl.col("attempts")).sum() / pl.col("attempts").sum())
            .cast(pl.Float64)
            .alias("objective_repeat_attempt_rate"),
        )
        .rename({"objective_id": "node_id"})
    )

    return (
        nodes.join(activity_metrics, on="node_id", how="left")
        .join(objective_metrics, on="node_id", how="left")
        .with_columns(
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("activity_attempts"))
            .when(pl.col("node_type") == "objective")
            .then(pl.col("objective_attempts"))
            .otherwise(None)
            .alias("overlay_attempts"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("activity_success_rate"))
            .when(pl.col("node_type") == "objective")
            .then(pl.col("objective_success_rate"))
            .otherwise(None)
            .alias("overlay_success_rate"),
            pl.when(pl.col("node_type") == "activity")
            .then(pl.col("activity_repeat_attempt_rate"))
            .when(pl.col("node_type") == "objective")
            .then(pl.col("objective_repeat_attempt_rate"))
            .otherwise(None)
            .alias("overlay_repeat_attempt_rate"),
        )
        .drop(
            [
                "activity_attempts",
                "activity_success_rate",
                "activity_repeat_attempt_rate",
                "objective_attempts",
                "objective_success_rate",
                "objective_repeat_attempt_rate",
            ]
        )
    )


def filter_dependency_graph_by_objectives(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    objective_codes: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    selected = {str(code).strip() for code in objective_codes if str(code).strip()}
    if not selected:
        return nodes.head(0), edges.head(0)

    filtered_nodes = nodes.filter(pl.col("objective_code").is_in(list(selected)))
    valid_node_codes = set(
        filtered_nodes.select("node_code")["node_code"].cast(pl.Utf8).to_list()
    )
    filtered_edges = edges.filter(
        pl.col("from_node_code").is_in(list(valid_node_codes))
        & pl.col("to_node_code").is_in(list(valid_node_codes))
    )
    return filtered_nodes, filtered_edges

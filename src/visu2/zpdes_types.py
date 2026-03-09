"""Shared parsing helpers and typed empty frames for ZPDES dependencies."""

from __future__ import annotations

import math
import re

import polars as pl

MODULE_CODE_RE = re.compile(r"^M\d+$")
OBJECTIVE_CODE_RE = re.compile(r"^(M\d+O\d+)$")
ACTIVITY_CODE_RE = re.compile(r"^(M\d+O\d+)A(\d+)$")
DEPENDENCY_TOKEN_RE = re.compile(r"(M\d+O\d+(?:A\d+)?)(?:\((\d+(?:\.\d+)?)%\))?")


def clean_str(value: object) -> str:
    """Normalize nullable, NaN-like values to a stripped string."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def to_float(value: object) -> float | None:
    """Convert a scalar-like value to float with NaN/empty protection."""
    text = clean_str(value)
    if not text:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    if math.isnan(out):
        return None
    return out


def first_numeric(value: object) -> float | None:
    """Return the first numeric value found in a nested structure."""
    if isinstance(value, list):
        for item in value:
            out = first_numeric(item)
            if out is not None:
                return out
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return to_float(value)


def first_int(value: object) -> int | None:
    """Return the first numeric value rounded to an integer."""
    numeric = first_numeric(value)
    if numeric is None:
        return None
    return int(round(numeric))


def parse_dependency_tokens(raw_value: object) -> list[dict[str, object]]:
    """Parse raw dependency strings into code/threshold tokens."""
    text = clean_str(raw_value)
    if not text:
        return []

    tokens: list[dict[str, object]] = []
    seen: set[tuple[str, float | None]] = set()
    for match in DEPENDENCY_TOKEN_RE.finditer(text):
        code = clean_str(match.group(1))
        if not code:
            continue
        pct_raw = clean_str(match.group(2))
        threshold = (float(pct_raw) / 100.0) if pct_raw else None
        key = (code, threshold)
        if key in seen:
            continue
        seen.add(key)
        tokens.append({"code": code, "threshold": threshold})
    return tokens


def parse_activity_index(node_code: str) -> int | None:
    """Extract the local activity index from a code like `M1O2A3`."""
    match = ACTIVITY_CODE_RE.match(clean_str(node_code))
    if not match:
        return None
    return int(match.group(2))


def parse_objective_code(node_code: str, node_type: str | None = None) -> str | None:
    """Resolve the objective code for objective/activity nodes."""
    code = clean_str(node_code)
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


def empty_nodes_df() -> pl.DataFrame:
    """Return an empty dependency-node frame with the runtime schema."""
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


def empty_edges_df() -> pl.DataFrame:
    """Return an empty dependency-edge frame with the runtime schema."""
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


def label_from_catalog_entry(entry: object, fallback: str) -> str:
    """Choose the best short/long catalog label with a code fallback."""
    if isinstance(entry, dict):
        title = entry.get("title")
        if isinstance(title, dict):
            short = clean_str(title.get("short"))
            long = clean_str(title.get("long"))
            if short:
                return short
            if long:
                return long
    return fallback


def node_type_from_code_strict(code: str) -> str:
    """Infer node type directly from a pedagogical code."""
    normalized = clean_str(code)
    if ACTIVITY_CODE_RE.match(normalized):
        return "activity"
    if OBJECTIVE_CODE_RE.match(normalized):
        return "objective"
    if MODULE_CODE_RE.match(normalized):
        return "module"
    return "unknown"


def is_init_open_from_rule(rule_payload: object) -> bool:
    """Interpret legacy `init_ssb`-style values as start-open markers."""
    if isinstance(rule_payload, list):
        return any(is_init_open_from_rule(item) for item in rule_payload)
    if isinstance(rule_payload, dict):
        return any(is_init_open_from_rule(value) for value in rule_payload.values())
    if isinstance(rule_payload, (int, float)):
        return int(rule_payload) == 0
    return clean_str(rule_payload) == "0"


def preferred_code_from_list(codes: list[str]) -> str | None:
    """Choose the most specific code from a list of alternative IDs."""
    cleaned = [clean_str(code) for code in codes if clean_str(code)]
    if not cleaned:
        return None
    return sorted(
        cleaned,
        key=lambda code: (
            0
            if ACTIVITY_CODE_RE.match(code)
            else 1
            if OBJECTIVE_CODE_RE.match(code)
            else 2
            if MODULE_CODE_RE.match(code)
            else 3,
            -len(code),
            code,
        ),
    )[0]

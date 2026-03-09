"""Shared helpers and constants for derived-table builders."""

from __future__ import annotations

import html
import math
import re

import polars as pl

ID_PLACEHOLDER_VALUES_LOWER = {"", "none", "null", "nan"}
MODULE_CODE_RE = re.compile(r"^M\d+$")
OBJECTIVE_CODE_RE = re.compile(r"^M\d+O\d+$")
ACTIVITY_CODE_RE = re.compile(r"^M\d+O\d+A\d+$")
ELO_BASE_RATING = 1500.0
ELO_SCALE = 400.0
ELO_K = 24.0


def as_lazy(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    """Normalize eager and lazy Polars inputs to a LazyFrame."""
    return df.lazy() if isinstance(df, pl.DataFrame) else df


def normalized_id_expr(column_name: str) -> pl.Expr:
    """Normalize placeholder-like identifier values to null."""
    normalized = pl.col(column_name).cast(pl.Utf8).str.strip_chars()
    return (
        pl.when(
            normalized.is_null()
            | normalized.str.to_lowercase().is_in(list(ID_PLACEHOLDER_VALUES_LOWER))
        )
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(normalized)
        .alias(column_name)
    )


def strip_html(raw: str) -> str:
    """Convert HTML-ish exercise content into readable plain text."""
    text = html.unescape(raw or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def exercise_label_from_instruction(instruction: object) -> str:
    """Build a short text label from an exercise instruction payload."""
    if isinstance(instruction, dict):
        raw_html = instruction.get("$html")
        if isinstance(raw_html, str):
            return strip_html(raw_html)
    if isinstance(instruction, str):
        return strip_html(instruction)
    return ""


def elo_expected_success(student_rating: float, exercise_rating: float) -> float:
    """Classic Elo probability of success against a fixed-difficulty exercise."""
    return 1.0 / (1.0 + 10.0 ** ((exercise_rating - student_rating) / ELO_SCALE))


def outcome_value(raw: object) -> float | None:
    """Normalize correctness-like values to 0.0/1.0 for Elo and aggregates."""
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

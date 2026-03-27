"""Query source-local or remote runtime parquet files with DuckDB."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from .classroom_progression import SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID
from .config import Settings
from .hf_sync import HFRepoConfig, load_hf_repo_config

FilterClause = tuple[str, str, Any]
OrderClause = tuple[str, bool]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SUPPORTED_OPERATORS = {"=", "!=", ">", ">=", "<", "<=", "between", "in"}


def _validate_identifier(identifier: str) -> str:
    text = str(identifier or "").strip()
    if not text or not _IDENTIFIER_RE.match(text):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return text


def _normalize_relative_path(relative_path: str | Path) -> str:
    text = str(relative_path or "").strip().replace("\\", "/")
    if not text:
        raise ValueError("relative_path must be a non-empty string.")
    return text


def _build_hf_runtime_url(repo_id: str, revision: str, relative_path: str | Path) -> str:
    normalized = _normalize_relative_path(relative_path)
    return f"hf://datasets/{repo_id}@{revision}/{normalized}"


def resolve_runtime_parquet_reference(
    settings: Settings,
    relative_path: str | Path,
) -> tuple[str, HFRepoConfig | None]:
    """Return a local parquet path when present, otherwise a remote HF URL."""
    normalized = _normalize_relative_path(relative_path)
    local_path = settings.runtime_root / normalized
    if local_path.exists():
        return str(local_path), None

    config = load_hf_repo_config(source_id=settings.source_id)
    if config is None:
        raise FileNotFoundError(
            f"Runtime parquet '{normalized}' is not available locally and no HF runtime source is configured."
        )
    return _build_hf_runtime_url(config.repo_id, config.revision, normalized), config


def _empty_frame(columns: Sequence[str]) -> pl.DataFrame:
    return pl.DataFrame({str(column): [] for column in columns})


def _load_httpfs(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        connection.execute("LOAD httpfs")
    except duckdb.Error:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")


def _configure_hf_secret(connection: duckdb.DuckDBPyConnection, config: HFRepoConfig) -> None:
    token = str(config.token or "").replace("'", "''")
    if not token:
        raise ValueError("HF_TOKEN is required for remote parquet queries.")
    _load_httpfs(connection)
    connection.execute(
        f"CREATE OR REPLACE SECRET hf_runtime (TYPE HUGGINGFACE, TOKEN '{token}')"
    )


def _coerce_in_values(value: Any) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes)):
        values = [value]
    elif isinstance(value, Iterable):
        values = list(value)
    else:
        values = [value]
    normalized = tuple(item for item in values if item is not None and str(item).strip() != "")
    return normalized


def _build_where_clause(filters: Sequence[FilterClause]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for column, operator, value in filters:
        column_name = _validate_identifier(column)
        op = str(operator or "").strip().lower()
        if op not in _SUPPORTED_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {operator!r}")
        if op == "in":
            values = _coerce_in_values(value)
            if not values:
                return "WHERE 1 = 0", []
            placeholders = ", ".join("?" for _ in values)
            clauses.append(f"{column_name} IN ({placeholders})")
            params.extend(values)
            continue
        if op == "between":
            if not isinstance(value, Sequence) or len(value) != 2:
                raise ValueError("between filters require a 2-item sequence.")
            clauses.append(f"{column_name} BETWEEN ? AND ?")
            params.extend([value[0], value[1]])
            continue
        clauses.append(f"{column_name} {operator} ?")
        params.append(value)
    if not clauses:
        return "", params
    return "WHERE " + " AND ".join(clauses), params


def _build_order_clause(order_by: Sequence[OrderClause]) -> str:
    if not order_by:
        return ""
    parts = []
    for column, descending in order_by:
        column_name = _validate_identifier(column)
        parts.append(f"{column_name} {'DESC' if descending else 'ASC'}")
    return "ORDER BY " + ", ".join(parts)


def query_runtime_parquet(
    settings: Settings,
    relative_path: str | Path,
    *,
    columns: Sequence[str],
    filters: Sequence[FilterClause] = (),
    order_by: Sequence[OrderClause] = (),
) -> pl.DataFrame:
    """Run a projected/filtering DuckDB query against one runtime parquet file."""
    projected_columns = [_validate_identifier(column) for column in columns]
    if not projected_columns:
        raise ValueError("columns must contain at least one projected column.")

    where_sql, filter_params = _build_where_clause(filters)
    if where_sql == "WHERE 1 = 0":
        return _empty_frame(projected_columns)

    reference, config = resolve_runtime_parquet_reference(settings, relative_path)
    order_sql = _build_order_clause(order_by)
    sql = (
        f"SELECT {', '.join(projected_columns)} "
        f"FROM read_parquet(?) "
        f"{where_sql} "
        f"{order_sql}"
    ).strip()

    connection = duckdb.connect(database=":memory:")
    try:
        if config is not None:
            _configure_hf_secret(connection, config)
        result = connection.execute(sql, [reference, *filter_params]).arrow()
        return pl.from_arrow(result)
    finally:
        connection.close()


def query_student_elo_events(
    settings: Settings,
    *,
    relative_path: str,
    user_ids: Sequence[str],
    columns: Sequence[str],
) -> pl.DataFrame:
    """Query only the requested student Elo event rows."""
    normalized_ids = tuple(
        str(user_id).strip() for user_id in user_ids if str(user_id or "").strip()
    )
    if not normalized_ids:
        return _empty_frame(columns)
    return query_runtime_parquet(
        settings,
        relative_path,
        columns=columns,
        filters=(("user_id", "in", normalized_ids),),
        order_by=(
            ("user_id", False),
            ("attempt_ordinal", False),
        ),
    )


def query_student_fact_label_lookup(
    settings: Settings,
    *,
    user_ids: Sequence[str],
    relative_path: str = "artifacts/derived/fact_attempt_core.parquet",
) -> pl.DataFrame:
    """Query readable activity/objective/module labels for selected students."""
    normalized_ids = tuple(
        str(user_id).strip() for user_id in user_ids if str(user_id or "").strip()
    )
    columns = (
        "activity_id",
        "module_code",
        "module_label",
        "objective_id",
        "objective_label",
        "activity_label",
    )
    if not normalized_ids:
        return _empty_frame(columns)
    return (
        query_runtime_parquet(
            settings,
            relative_path,
            columns=columns,
            filters=(("user_id", "in", normalized_ids),),
        )
        .filter(
            pl.col("activity_id").is_not_null()
            & pl.col("objective_id").is_not_null()
            & pl.col("module_code").is_not_null()
        )
        .unique(subset=["activity_id", "objective_id", "module_code"], keep="first")
    )


def query_fact_attempts(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
    columns: Sequence[str],
    module_code: str | None = None,
    objective_id: str | None = None,
    activity_id: str | None = None,
    work_mode: str | None = None,
    classroom_id: str | None = None,
    min_student_attempts: int = 1,
    relative_path: str = "artifacts/derived/fact_attempt_core.parquet",
) -> pl.DataFrame:
    """Query one filtered fact-table slice, optionally enforcing a minimum attempt count per student."""
    projected_columns = [_validate_identifier(column) for column in columns]
    if not projected_columns:
        raise ValueError("columns must contain at least one projected column.")

    where_filters: list[FilterClause] = [
        ("date_utc", ">=", start_date),
        ("date_utc", "<=", end_date),
    ]
    if str(module_code or "").strip():
        where_filters.append(("module_code", "=", str(module_code).strip()))
    if str(objective_id or "").strip():
        where_filters.append(("objective_id", "=", str(objective_id).strip()))
    if str(activity_id or "").strip():
        where_filters.append(("activity_id", "=", str(activity_id).strip()))
    if str(work_mode or "").strip() and str(work_mode).strip() != "all":
        where_filters.append(("work_mode", "=", str(work_mode).strip()))
    classroom_key = str(classroom_id or "").strip()
    if classroom_key and classroom_key != SYNTHETIC_ALL_STUDENTS_CLASSROOM_ID:
        where_filters.append(("classroom_id", "=", classroom_key))

    where_sql, filter_params = _build_where_clause(where_filters)
    reference, config = resolve_runtime_parquet_reference(settings, relative_path)

    order_columns = (
        "created_at",
        "user_id",
        "activity_id",
        "exercise_id",
        "attempt_number",
    )
    filtered_projection = list(projected_columns)
    for helper_column in (*order_columns, "user_id"):
        if helper_column not in filtered_projection:
            filtered_projection.append(helper_column)

    min_attempts = max(1, int(min_student_attempts))
    eligible_join = ""
    params: list[Any] = [reference, *filter_params]
    if min_attempts > 1:
        eligible_join = (
            "INNER JOIN eligible_students USING (user_id) "
        )
        params.append(min_attempts)

    sql = (
        "WITH filtered AS ("
        f"SELECT {', '.join(filtered_projection)} "
        "FROM read_parquet(?) "
        f"{where_sql}"
        "), "
        "eligible_students AS ("
        "SELECT user_id "
        "FROM filtered "
        "WHERE user_id IS NOT NULL AND trim(CAST(user_id AS VARCHAR)) <> '' "
        "GROUP BY user_id "
        "HAVING COUNT(*) >= ?"
        ") "
        f"SELECT {', '.join(projected_columns)} "
        "FROM filtered "
        f"{eligible_join}"
        "ORDER BY created_at ASC, user_id ASC, activity_id ASC, exercise_id ASC, attempt_number ASC"
    )

    if min_attempts <= 1:
        sql = (
            "WITH filtered AS ("
            f"SELECT {', '.join(filtered_projection)} "
            "FROM read_parquet(?) "
            f"{where_sql}"
            ") "
            f"SELECT {', '.join(projected_columns)} "
            "FROM filtered "
            "ORDER BY created_at ASC, user_id ASC, activity_id ASC, exercise_id ASC, attempt_number ASC"
        )

    connection = duckdb.connect(database=":memory:")
    try:
        if config is not None:
            _configure_hf_secret(connection, config)
        result = connection.execute(sql, params).arrow()
        return pl.from_arrow(result)
    finally:
        connection.close()


def query_fact_attempts_for_classroom(
    settings: Settings,
    *,
    classroom_id: str,
    mode_scope: str,
    start_date: date,
    end_date: date,
    columns: Sequence[str],
    min_student_attempts: int = 1,
    relative_path: str = "artifacts/derived/fact_attempt_core.parquet",
) -> pl.DataFrame:
    """Query only the needed classroom slice from the fact table."""
    return query_fact_attempts(
        settings,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        classroom_id=classroom_id,
        work_mode=mode_scope,
        min_student_attempts=min_student_attempts,
        relative_path=relative_path,
    )


__all__ = [
    "FilterClause",
    "OrderClause",
    "_build_hf_runtime_url",
    "query_fact_attempts",
    "query_fact_attempts_for_classroom",
    "query_runtime_parquet",
    "query_student_fact_label_lookup",
    "query_student_elo_events",
    "resolve_runtime_parquet_reference",
]

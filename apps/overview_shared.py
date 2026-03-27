"""Shared filter, label, and runtime helpers for overview-style Streamlit pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import streamlit as st

from visu2.contracts import RUNTIME_CORE_COLUMNS


@dataclass(frozen=True, slots=True)
class CurriculumFilters:
    """Selected curriculum filters shared across overview-style pages."""

    start_date: date
    end_date: date
    module_code: str | None
    objective_id: str | None
    activity_id: str | None


@dataclass(frozen=True, slots=True)
class CurriculumFilterDomain:
    """Compact curriculum metadata used to populate overview sidebar filters."""

    min_date: date
    max_date: date
    curriculum_frame: pl.DataFrame


def render_dashboard_style() -> None:
    """Apply the shared dashboard typography and background styling."""
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;9..144,700&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
:root {
  --bg1: #f2f6f2;
  --bg2: #dbe6da;
  --ink: #17221b;
  --accent: #1e7a52;
  --accent-2: #2148a4;
  --panel: rgba(255, 255, 255, 0.78);
}
.stApp {
  background:
    radial-gradient(1300px 500px at 88% -10%, rgba(30, 122, 82, 0.25), transparent 60%),
    radial-gradient(900px 400px at -10% 0%, rgba(33, 72, 164, 0.18), transparent 55%),
    linear-gradient(180deg, var(--bg1), var(--bg2));
  color: var(--ink);
}
h1, h2, h3 {
  font-family: "Fraunces", Georgia, serif !important;
  color: var(--ink);
}
div, p, label {
  font-family: "IBM Plex Sans", sans-serif !important;
}
[data-testid="stMetric"] {
  background: var(--panel);
  border: 1px solid rgba(23, 34, 27, 0.10);
  border-radius: 14px;
  padding: 0.85rem;
}
[data-testid="stSidebar"] {
  border-right: 1px solid rgba(23, 34, 27, 0.15);
}
</style>
""",
        unsafe_allow_html=True,
    )


def collect_lazy(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a lazy Polars plan with streaming when available."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect()


def normalize_date_input_range(value: object) -> tuple[date, date] | None:
    """Normalize Streamlit date-input outputs into a stable inclusive date range."""
    if isinstance(value, date):
        return (value, value)
    if isinstance(value, (list, tuple)):
        if len(value) == 0:
            return None
        if len(value) == 1 and isinstance(value[0], date):
            return (value[0], value[0])
        if len(value) >= 2 and isinstance(value[0], date) and isinstance(value[1], date):
            return (value[0], value[1])
    return None


def render_date_range_input(
    min_date: date,
    max_date: date,
    *,
    label: str = "Date range (UTC)",
    key_prefix: str | None = None,
) -> tuple[date, date]:
    """Render a sidebar date range input with stable reset behavior when bounds change."""
    if min_date is None or max_date is None:
        st.error("No dated rows available to populate filters.")
        st.stop()

    widget_key = f"{key_prefix}_date_range" if key_prefix else None
    signature_key = f"{key_prefix}_date_range_signature" if key_prefix else None
    signature = (min_date.isoformat(), max_date.isoformat())

    if widget_key and signature_key and st.session_state.get(signature_key) != signature:
        st.session_state[widget_key] = (min_date, max_date)
        st.session_state[signature_key] = signature

    selected_range = st.sidebar.date_input(
        label,
        value=st.session_state.get(widget_key, (min_date, max_date)) if widget_key else (min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key=widget_key,
    )
    normalized_range = normalize_date_input_range(selected_range)
    if normalized_range is None:
        st.error("Please provide a valid start and end date.")
        st.stop()
    return normalized_range


@st.cache_data(show_spinner=False)
def parquet_columns(path: Path) -> list[str]:
    """Return parquet column names without materializing the full table."""
    return list(pq.ParquetFile(path).schema_arrow.names)


def collect_core_compatibility(
    table_columns: dict[str, list[str]],
    required_tables: tuple[str, ...],
) -> dict[str, object]:
    """Check whether required runtime core columns are available for the current page."""
    missing_core_by_table: dict[str, list[str]] = {}
    for table_name in required_tables:
        required_cols = RUNTIME_CORE_COLUMNS.get(table_name, [])
        actual_cols = set(table_columns.get(table_name, []))
        missing = [col for col in required_cols if col not in actual_cols]
        if missing:
            missing_core_by_table[table_name] = missing

    return {
        "status": "incompatible" if missing_core_by_table else "ok",
        "missing_core_by_table": missing_core_by_table,
    }


def format_missing_table_columns(missing_by_table: dict[str, list[str]]) -> str:
    """Format missing runtime columns as a markdown bullet list."""
    return "\n".join(
        f"- `{table_name}`: {', '.join(columns)}"
        for table_name, columns in missing_by_table.items()
    )


def label_or_id(label: str | None, identifier: str | None) -> str:
    """Return the most readable label available for a curriculum entity."""
    label_text = (label or "").strip()
    identifier_text = (identifier or "").strip()
    return label_text or identifier_text or "Unknown"


def format_option(label: str | None, identifier: str | None) -> str:
    """Format a selectbox option with label first and identifier fallback."""
    base = label_or_id(label, identifier)
    if label and identifier and label.strip() and identifier.strip() and label.strip() != identifier.strip():
        return f"{label.strip()} ({identifier.strip()})"
    return base


def format_axis_label(text: str | None, max_chars: int = 48) -> str:
    """Shorten long axis labels while keeping them human-readable."""
    normalized = (text or "").strip()
    if not normalized:
        return "Unknown"
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 1].rstrip() + "…"


def compose_hover_label(full_label: str | None, identifier: str | None, show_ids: bool) -> str:
    """Compose a hover label with an optional identifier suffix."""
    base = label_or_id(full_label, identifier)
    identifier_text = (identifier or "").strip()
    if show_ids and identifier_text and base != identifier_text:
        return f"{base} [{identifier_text}]"
    return base


def ensure_label_columns(
    frame: pl.DataFrame,
    label_to_fallback: dict[str, str],
) -> tuple[pl.DataFrame, list[str]]:
    """Guarantee that label columns exist, backfilling them from identifier columns when needed."""
    missing: list[str] = []
    normalized = frame
    for label_col, fallback_col in label_to_fallback.items():
        if label_col not in normalized.columns:
            missing.append(label_col)
            if fallback_col in normalized.columns:
                normalized = normalized.with_columns(
                    pl.col(fallback_col).cast(pl.Utf8).alias(label_col)
                )
            else:
                normalized = normalized.with_columns(pl.lit(None, dtype=pl.Utf8).alias(label_col))
        elif fallback_col in normalized.columns:
            normalized = normalized.with_columns(
                pl.coalesce(
                    [
                        pl.col(label_col).cast(pl.Utf8),
                        pl.col(fallback_col).cast(pl.Utf8),
                    ]
                ).alias(label_col)
            )
    return normalized, missing


@st.cache_data(show_spinner=False)
def load_fact_dimensions(fact_path: Path) -> CurriculumFilterDomain:
    """Load a compact curriculum domain for the overview sidebar filters."""
    date_bounds = collect_lazy(
        pl.scan_parquet(fact_path).select(
            pl.col("date_utc").min().alias("min_date"),
            pl.col("date_utc").max().alias("max_date"),
        )
    )
    min_date = date_bounds.item(0, "min_date")
    max_date = date_bounds.item(0, "max_date")
    if min_date is None or max_date is None:
        st.error("No dated rows available to populate filters.")
        st.stop()

    curriculum_frame = collect_lazy(
        pl.scan_parquet(fact_path)
        .select(
            [
                "module_code",
                "module_label",
                "objective_id",
                "objective_label",
                "activity_id",
                "activity_label",
            ]
        )
        .unique()
        .sort(["module_code", "objective_id", "activity_id"])
    )
    curriculum_frame, _ = ensure_label_columns(
        curriculum_frame,
        {
            "module_label": "module_code",
            "objective_label": "objective_id",
            "activity_label": "activity_id",
        },
    )
    return CurriculumFilterDomain(
        min_date=min_date,
        max_date=max_date,
        curriculum_frame=curriculum_frame,
    )


def render_curriculum_filters(
    dimension_source: pl.DataFrame | CurriculumFilterDomain,
    *,
    sidebar_header: str = "Filters",
) -> CurriculumFilters:
    """Render the shared sidebar date/module/objective/activity filters."""
    if isinstance(dimension_source, CurriculumFilterDomain):
        min_date = dimension_source.min_date
        max_date = dimension_source.max_date
        dimension_frame = dimension_source.curriculum_frame
    else:
        dimension_frame = dimension_source
        min_date = dimension_frame["date_utc"].min()
        max_date = dimension_frame["date_utc"].max()

    if dimension_frame.is_empty():
        st.error("No rows available to populate filters.")
        st.stop()

    if min_date is None or max_date is None:
        st.error("No dated rows available to populate filters.")
        st.stop()

    st.sidebar.header(sidebar_header)
    start_date, end_date = render_date_range_input(min_date, max_date, key_prefix="curriculum_filters")

    module_frame = (
        dimension_frame.select(["module_code", "module_label"])
        .drop_nulls("module_code")
        .unique()
        .sort("module_code")
    )
    module_options_map = {"All": None}
    for row in module_frame.to_dicts():
        module_options_map[format_option(row.get("module_label"), row.get("module_code"))] = row.get(
            "module_code"
        )
    selected_module = st.sidebar.selectbox("Module", list(module_options_map.keys()))
    module_filter = module_options_map[selected_module]

    filtered_objectives = dimension_frame
    if module_filter:
        filtered_objectives = filtered_objectives.filter(pl.col("module_code") == module_filter)
    objective_frame = (
        filtered_objectives.select(["objective_id", "objective_label"])
        .drop_nulls("objective_id")
        .unique()
        .sort("objective_id")
    )
    objective_options_map = {"All": None}
    for row in objective_frame.to_dicts():
        objective_options_map[
            format_option(row.get("objective_label"), row.get("objective_id"))
        ] = row.get("objective_id")
    selected_objective = st.sidebar.selectbox("Objective", list(objective_options_map.keys()))
    objective_filter = objective_options_map[selected_objective]

    filtered_activities = filtered_objectives
    if objective_filter:
        filtered_activities = filtered_activities.filter(pl.col("objective_id") == objective_filter)
    activity_frame = (
        filtered_activities.select(["activity_id", "activity_label"])
        .drop_nulls("activity_id")
        .unique()
        .sort("activity_id")
    )
    activity_options_map = {"All": None}
    for row in activity_frame.to_dicts():
        activity_options_map[
            format_option(row.get("activity_label"), row.get("activity_id"))
        ] = row.get("activity_id")
    selected_activity = st.sidebar.selectbox("Activity", list(activity_options_map.keys()))
    activity_filter = activity_options_map[selected_activity]

    return CurriculumFilters(
        start_date=start_date,
        end_date=end_date,
        module_code=module_filter,
        objective_id=objective_filter,
        activity_id=activity_filter,
    )


def build_fact_query(
    fact_path: Path,
    start_date: date,
    end_date: date,
    module_code: str | None,
    objective_id: str | None,
    activity_id: str | None,
) -> pl.LazyFrame:
    """Build a fact-table lazy query constrained by the shared curriculum filters."""
    lf = pl.scan_parquet(fact_path).filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_code:
        lf = lf.filter(pl.col("module_code") == module_code)
    if objective_id:
        lf = lf.filter(pl.col("objective_id") == objective_id)
    if activity_id:
        lf = lf.filter(pl.col("activity_id") == activity_id)
    return lf


def apply_filters(
    frame: pl.DataFrame,
    start_date: date,
    end_date: date,
    module_code: str | None,
    objective_id: str | None,
    activity_id: str | None,
    *,
    activity_from_col: str = "activity_id",
    module_col: str = "module_code",
) -> pl.DataFrame:
    """Apply the shared curriculum filters to an eager aggregate frame."""
    filtered = frame.filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_code and module_col in filtered.columns:
        filtered = filtered.filter(pl.col(module_col) == module_code)
    if objective_id and "objective_id" in filtered.columns:
        filtered = filtered.filter(pl.col("objective_id") == objective_id)
    if activity_id and activity_from_col in filtered.columns:
        filtered = filtered.filter(pl.col(activity_from_col) == activity_id)
    return filtered


@st.cache_data(show_spinner=False)
def load_top_transition_edges(
    transition_path: Path,
    start_date: date,
    end_date: date,
    module_code: str | None,
    activity_id: str | None,
    top_n: int,
    has_same_objective_rate: bool,
) -> pl.DataFrame:
    """Load the most frequent cross-objective transition edges for the current scope."""
    edge_group_cols = [
        "from_activity_id",
        "to_activity_id",
        "from_activity_label",
        "to_activity_label",
    ]
    lf = pl.scan_parquet(transition_path).filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_code:
        lf = lf.filter(pl.col("from_module_code") == module_code)
    if activity_id:
        lf = lf.filter(pl.col("from_activity_id") == activity_id)
    if has_same_objective_rate:
        lf = lf.filter(pl.col("same_objective_rate") < 1.0)
    lf = lf.group_by(edge_group_cols).agg(
        pl.sum("transition_count").alias("transition_count"),
        pl.sum("success_conditioned_count").alias("success_conditioned_count"),
    )
    lf = lf.sort("transition_count", descending=True).head(max(1, int(top_n)))
    return collect_lazy(lf)

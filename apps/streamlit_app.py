from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import plotly.express as px
import polars as pl
import pyarrow.parquet as pq
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from visu2.bottleneck import apply_bottleneck_filters, build_bottleneck_frame
from visu2.contracts import (
    ACTIVE_CANONICAL_MODULE_CODES,
    DERIVED_SCHEMA_VERSION,
    RUNTIME_CORE_COLUMNS,
    RUNTIME_LABEL_COLUMNS,
)
from visu2.config import get_settings
from visu2.reporting import load_derived_manifest
from runtime_bootstrap import bootstrap_runtime_assets


st.set_page_config(
    page_title="Learning Analytics Overview",
    page_icon=":bar_chart:",
    layout="wide",
)


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


@st.cache_data(show_spinner=False)
def load_report(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data(show_spinner=False)
def load_aggregates(derived_dir: Path) -> dict[str, pl.DataFrame]:
    return {
        "activity": pl.read_parquet(derived_dir / "agg_activity_daily.parquet"),
        "objective": pl.read_parquet(derived_dir / "agg_objective_daily.parquet"),
    }


def _collect_lazy(lf: pl.LazyFrame) -> pl.DataFrame:
    """Prefer streaming execution on large lazy plans to reduce peak memory."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect()


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
    return _collect_lazy(lf)


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _collect_runtime_compatibility(
    table_columns: dict[str, list[str]],
    manifest_path: Path,
) -> dict[str, object]:
    missing_core_by_table: dict[str, list[str]] = {}
    missing_labels_by_table: dict[str, list[str]] = {}
    manifest_messages: list[str] = []
    manifest_schema_version: str | None = None

    for table_name, required_cols in RUNTIME_CORE_COLUMNS.items():
        actual_cols = set(table_columns.get(table_name, []))
        missing = [col for col in required_cols if col not in actual_cols]
        if missing:
            missing_core_by_table[table_name] = missing

    for table_name, label_cols in RUNTIME_LABEL_COLUMNS.items():
        actual_cols = set(table_columns.get(table_name, []))
        missing = [col for col in label_cols if col not in actual_cols]
        if missing:
            missing_labels_by_table[table_name] = missing

    try:
        manifest = load_derived_manifest(manifest_path)
        manifest_schema_version = str(manifest.get("schema_version") or "")
        if manifest_schema_version != DERIVED_SCHEMA_VERSION:
            manifest_messages.append(
                f"Manifest schema_version mismatch: found '{manifest_schema_version}', expected '{DERIVED_SCHEMA_VERSION}'."
            )

        manifest_tables = manifest.get("tables") if isinstance(manifest, dict) else None
        if isinstance(manifest_tables, dict):
            required_manifest_tables = set(RUNTIME_CORE_COLUMNS.keys())
            for table_name in sorted(required_manifest_tables):
                if table_name not in manifest_tables:
                    manifest_messages.append(f"Manifest missing required table entry: {table_name}.")
            for table_name, actual_cols in table_columns.items():
                entry = manifest_tables.get(table_name)
                if not isinstance(entry, dict):
                    manifest_messages.append(f"Manifest missing table entry: {table_name}.")
                    continue
                manifest_cols = entry.get("columns")
                if not isinstance(manifest_cols, list):
                    manifest_messages.append(f"Manifest table '{table_name}' has invalid columns payload.")
                    continue
                if list(manifest_cols) != list(actual_cols):
                    manifest_messages.append(f"Manifest columns drift detected for table '{table_name}'.")
    except FileNotFoundError:
        manifest_messages.append("Derived manifest is missing.")
    except ValueError as err:
        manifest_messages.append(f"Derived manifest is invalid: {err}")

    status = "ok"
    if missing_core_by_table:
        status = "incompatible"
    elif missing_labels_by_table or manifest_messages:
        status = "degraded"

    return {
        "status": status,
        "missing_core_by_table": missing_core_by_table,
        "missing_labels_by_table": missing_labels_by_table,
        "manifest_schema_version": manifest_schema_version,
        "manifest_messages": manifest_messages,
    }


def _format_missing_table_columns(missing_by_table: dict[str, list[str]]) -> str:
    if not missing_by_table:
        return "None"
    lines = []
    for table_name in sorted(missing_by_table.keys()):
        cols = ", ".join(missing_by_table[table_name])
        lines.append(f"- `{table_name}`: {cols}")
    return "\n".join(lines)


def _label_or_id(label: str | None, identifier: str | None) -> str:
    if isinstance(label, str) and label.strip():
        return label.strip()
    return str(identifier or "")


def _format_option(label: str | None, identifier: str | None) -> str:
    base = _label_or_id(label, identifier)
    if isinstance(identifier, str) and identifier:
        return f"{base} [{identifier}]"
    return base


def format_axis_label(text: str | None, max_chars: int = 48) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return "(unlabeled)"
    if len(normalized) <= max_chars:
        return normalized
    return f"{normalized[: max_chars - 1].rstrip()}..."


def compose_hover_label(full_label: str | None, identifier: str | None, show_ids: bool) -> str:
    normalized = str(full_label or "").strip()
    if not normalized:
        normalized = str(identifier or "(unlabeled)")
    if show_ids and identifier:
        return f"{normalized}<br>ID: {identifier}"
    return normalized


def _ensure_label_columns(
    frame: pl.DataFrame,
    label_to_fallback: dict[str, str],
) -> tuple[pl.DataFrame, list[str]]:
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


def build_fact_query(
    fact_path: Path,
    start_date: date,
    end_date: date,
    module_code: str | None,
    objective_id: str | None,
    activity_id: str | None,
) -> pl.LazyFrame:
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
    activity_from_col: str = "activity_id",
    module_col: str = "module_code",
) -> pl.DataFrame:
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


def main() -> None:
    bootstrap_runtime_assets()
    settings = get_settings()
    derived_dir = settings.artifacts_derived_dir
    report_path = settings.consistency_report_path
    manifest_path = settings.derived_manifest_path
    fact_path = derived_dir / "fact_attempt_core.parquet"
    transition_path = derived_dir / "agg_transition_edges.parquet"
    module_usage_daily_path = derived_dir / "agg_module_usage_daily.parquet"
    playlist_module_usage_path = derived_dir / "agg_playlist_module_usage.parquet"
    module_activity_usage_path = derived_dir / "agg_module_activity_usage.parquet"
    exercise_daily_path = derived_dir / "agg_exercise_daily.parquet"

    required = [
        derived_dir / "agg_activity_daily.parquet",
        derived_dir / "agg_objective_daily.parquet",
        transition_path,
        module_usage_daily_path,
        playlist_module_usage_path,
        module_activity_usage_path,
        exercise_daily_path,
        fact_path,
        report_path,
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        st.error("Missing derived artifacts. Run `python scripts/build_derived.py` first.")
        st.code("\n".join(str(p) for p in missing))
        st.stop()

    report = load_report(report_path)
    data = load_aggregates(derived_dir)
    compatibility = _collect_runtime_compatibility(
        table_columns={
            "fact_attempt_core": _parquet_columns(fact_path),
            "agg_activity_daily": data["activity"].columns,
            "agg_objective_daily": data["objective"].columns,
            "agg_transition_edges": _parquet_columns(transition_path),
            "agg_module_usage_daily": _parquet_columns(module_usage_daily_path),
            "agg_playlist_module_usage": _parquet_columns(playlist_module_usage_path),
            "agg_module_activity_usage": _parquet_columns(module_activity_usage_path),
            "agg_exercise_daily": _parquet_columns(exercise_daily_path),
        },
        manifest_path=manifest_path,
    )

    st.title("Learning Analytics Overview")
    st.caption("Interactive exploration of progression, bottlenecks, and learning paths.")
    status = str(compatibility["status"])
    missing_core_by_table = compatibility["missing_core_by_table"]
    if status == "incompatible":
        st.error(
            "Artifact status: INCOMPATIBLE. One or more core columns are missing. "
            "Rebuild artifacts with `uv run python scripts/build_derived.py --strict-checks`."
        )
        st.markdown("**Missing core columns:**")
        st.markdown(_format_missing_table_columns(missing_core_by_table))
        st.stop()

    activity, _ = _ensure_label_columns(
        data["activity"],
        {
            "module_label": "module_code",
            "objective_label": "objective_id",
            "activity_label": "activity_id",
        },
    )
    transition_columns = set(_parquet_columns(transition_path))
    transition_has_same_objective_rate = "same_objective_rate" in transition_columns

    min_date = activity["date_utc"].min()
    max_date = activity["date_utc"].max()
    if min_date is None or max_date is None:
        st.error("No data available in aggregate tables.")
        st.stop()

    st.sidebar.header("Filters")
    start_date, end_date = st.sidebar.date_input(
        "Date range (UTC)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("Please provide a valid start and end date.")
        st.stop()

    module_frame = (
        activity.select(["module_code", "module_label"])
        .drop_nulls("module_code")
        .unique()
        .sort("module_code")
    )
    module_options_map = {"All": None}
    for row in module_frame.to_dicts():
        module_options_map[_format_option(row.get("module_label"), row.get("module_code"))] = row.get(
            "module_code"
        )
    selected_module = st.sidebar.selectbox("Module", list(module_options_map.keys()))
    module_filter = module_options_map[selected_module]

    filtered_objectives_for_module = activity
    if module_filter:
        filtered_objectives_for_module = filtered_objectives_for_module.filter(
            pl.col("module_code") == module_filter
        )
    objective_frame = (
        filtered_objectives_for_module.select(["objective_id", "objective_label"])
        .drop_nulls("objective_id")
        .unique()
        .sort("objective_id")
    )
    objective_options_map = {"All": None}
    for row in objective_frame.to_dicts():
        objective_options_map[
            _format_option(row.get("objective_label"), row.get("objective_id"))
        ] = row.get("objective_id")
    selected_objective = st.sidebar.selectbox("Objective", list(objective_options_map.keys()))
    objective_filter = objective_options_map[selected_objective]

    filtered_activities_for_objective = filtered_objectives_for_module
    if objective_filter:
        filtered_activities_for_objective = filtered_activities_for_objective.filter(
            pl.col("objective_id") == objective_filter
        )
    activity_frame = (
        filtered_activities_for_objective.select(["activity_id", "activity_label"])
        .drop_nulls("activity_id")
        .unique()
        .sort("activity_id")
    )
    activity_options_map = {"All": None}
    for row in activity_frame.to_dicts():
        activity_options_map[_format_option(row.get("activity_label"), row.get("activity_id"))] = row.get(
            "activity_id"
        )
    selected_activity = st.sidebar.selectbox("Activity", list(activity_options_map.keys()))
    activity_filter = activity_options_map[selected_activity]

    st.sidebar.subheader("Chart Controls")
    top_n_bottlenecks = int(
        st.sidebar.slider("Top bottleneck entities", min_value=5, max_value=50, value=15, step=1)
    )
    top_n_transitions = int(
        st.sidebar.slider("Top transitions", min_value=5, max_value=50, value=15, step=1)
    )
    min_attempts_for_bottleneck = int(
        st.sidebar.number_input(
            "Min attempts for bottleneck",
            min_value=1,
            max_value=10_000,
            value=30,
            step=1,
        )
    )
    show_ids = bool(st.sidebar.checkbox("Show IDs in hover", value=False))

    filtered_activity = apply_filters(
        activity, start_date, end_date, module_filter, objective_filter, activity_filter
    )
    fact_query = build_fact_query(
        fact_path=fact_path,
        start_date=start_date,
        end_date=end_date,
        module_code=module_filter,
        objective_id=objective_filter,
        activity_id=activity_filter,
    )
    kpi = fact_query.select(
        pl.len().alias("attempts"),
        pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
        pl.col("exercise_id").drop_nulls().n_unique().alias("unique_exercises"),
        pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
    )
    kpi = _collect_lazy(kpi).to_dicts()[0]
    kpi_exercise_balanced = (
        fact_query.filter(pl.col("exercise_id").is_not_null())
        .group_by("exercise_id")
        .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("exercise_success_rate"))
        .select(pl.col("exercise_success_rate").mean().alias("exercise_balanced_success_rate"))
    )
    kpi_exercise_balanced = _collect_lazy(kpi_exercise_balanced)
    kpi_exercise_balanced_value = (
        float(kpi_exercise_balanced["exercise_balanced_success_rate"][0])
        if kpi_exercise_balanced.height > 0
        and kpi_exercise_balanced["exercise_balanced_success_rate"][0] is not None
        else 0.0
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Attempts", f"{int(kpi['attempts']):,}")
    c2.metric("Unique Students", f"{int(kpi['unique_students']):,}")
    c3.metric("Unique Exercises", f"{int(kpi['unique_exercises']):,}")
    c4.metric("Success Rate (attempt-weighted)", f"{(kpi['success_rate'] or 0.0) * 100:.2f}%")
    c5.metric("Success Rate (exercise-balanced)", f"{kpi_exercise_balanced_value * 100:.2f}%")

    st.subheader("Work Mode Performance")
    st.caption(
        "Success and exploration metrics below compare work modes for the active date/module/objective/activity filters. "
        "Two success definitions are shown: attempt-weighted and exercise-balanced."
    )
    work_mode_summary = (
        fact_query.filter(pl.col("work_mode").is_not_null())
        .group_by("work_mode")
        .agg(
            pl.len().alias("attempts"),
            pl.col("user_id").drop_nulls().n_unique().alias("unique_students"),
            pl.col("module_code").drop_nulls().n_unique().alias("unique_modules_explored"),
            pl.col("objective_id").drop_nulls().n_unique().alias("unique_objectives_explored"),
            pl.col("activity_id").drop_nulls().n_unique().alias("unique_activities_explored"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
        )
        .join(
            fact_query.filter(pl.col("work_mode").is_not_null() & pl.col("exercise_id").is_not_null())
            .group_by(["work_mode", "exercise_id"])
            .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("exercise_success_rate"))
            .group_by("work_mode")
            .agg(
                pl.col("exercise_success_rate")
                .mean()
                .alias("exercise_balanced_success_rate")
            ),
            on="work_mode",
            how="left",
        )
        .join(
            fact_query.filter(pl.col("work_mode").is_not_null() & pl.col("activity_id").is_not_null())
            .group_by(["work_mode", "activity_id"])
            .agg(pl.len().alias("activity_attempts"))
            .group_by("work_mode")
            .agg(pl.col("activity_attempts").median().alias("median_attempts_per_activity")),
            on="work_mode",
            how="left",
        )
        .sort("success_rate", descending=True)
    )
    work_mode_summary = _collect_lazy(work_mode_summary)
    if work_mode_summary.height == 0:
        st.info("No work mode rows available after filters.")
    else:
        available_work_modes = work_mode_summary["work_mode"].to_list()
        selected_work_modes = st.multiselect(
            "Work modes shown",
            options=available_work_modes,
            default=available_work_modes,
        )
        dropped_modes = [mode for mode in selected_work_modes if mode not in available_work_modes]
        if dropped_modes:
            st.warning(
                "Some selected work modes have no rows in the current filter context and were removed: "
                + ", ".join(sorted(dropped_modes))
            )
            selected_work_modes = [mode for mode in selected_work_modes if mode in available_work_modes]
        if not selected_work_modes:
            st.info("Select at least one work mode to render the charts.")
        else:
            selected_work_mode_summary = work_mode_summary.filter(
                pl.col("work_mode").is_in(selected_work_modes)
            )
            if selected_work_mode_summary.height == 0:
                st.info("No rows available for the selected work modes.")
            else:
                st.markdown("**Success Rate by Work Mode (selected period)**")
                success_table = (
                    selected_work_mode_summary.select(
                        [
                            "work_mode",
                            "attempts",
                            "success_rate",
                            "exercise_balanced_success_rate",
                        ]
                    )
                    .sort("success_rate", descending=True)
                    .to_pandas()
                )
                success_table["success_rate"] = success_table["success_rate"].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                success_table["exercise_balanced_success_rate"] = success_table[
                    "exercise_balanced_success_rate"
                ].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                st.dataframe(
                    success_table,
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "work_mode": "Work mode",
                        "attempts": st.column_config.NumberColumn("Attempts", format="%d"),
                        "success_rate": "Success rate (attempt-weighted)",
                        "exercise_balanced_success_rate": "Success rate (exercise-balanced)",
                    },
                )

                st.markdown("**Work Mode Footprint and Depth**")
                st.caption(
                    "Width describes unique modules/objectives/activities explored. "
                    "Depth uses median attempts per activity plus repeat-attempt rate."
                )
                width_plot = selected_work_mode_summary.select(
                    [
                        "work_mode",
                        "unique_modules_explored",
                        "unique_objectives_explored",
                        "unique_activities_explored",
                    ]
                ).to_pandas()
                width_plot = width_plot.melt(
                    id_vars="work_mode",
                    value_vars=[
                        "unique_modules_explored",
                        "unique_objectives_explored",
                        "unique_activities_explored",
                    ],
                    var_name="footprint_metric",
                    value_name="count",
                )
                width_labels = {
                    "unique_modules_explored": "Modules explored",
                    "unique_objectives_explored": "Objectives explored",
                    "unique_activities_explored": "Activities explored",
                }
                width_plot["footprint_metric"] = width_plot["footprint_metric"].map(width_labels)
                fig_width = px.bar(
                    width_plot,
                    x="work_mode",
                    y="count",
                    color="footprint_metric",
                    barmode="group",
                    title="Exploration width by work mode",
                    labels={
                        "work_mode": "Work mode",
                        "count": "Distinct count",
                        "footprint_metric": "Footprint metric",
                    },
                )
                fig_width.update_layout(
                    margin={"l": 24, "r": 24, "t": 56, "b": 24},
                    legend_title_text="Footprint metric",
                )
                fig_width.update_yaxes(showgrid=True, gridcolor="rgba(23,34,27,0.14)")
                st.plotly_chart(fig_width, width='stretch')

                summary_table = selected_work_mode_summary.select(
                    [
                        "work_mode",
                        "attempts",
                        "unique_students",
                        "unique_modules_explored",
                        "unique_objectives_explored",
                        "unique_activities_explored",
                        "median_attempts_per_activity",
                        "repeat_attempt_rate",
                        "success_rate",
                        "exercise_balanced_success_rate",
                    ]
                ).sort("attempts", descending=True).to_pandas()
                summary_display = summary_table.copy()
                summary_display["repeat_attempt_rate"] = summary_display["repeat_attempt_rate"].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                summary_display["success_rate"] = summary_display["success_rate"].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                summary_display["exercise_balanced_success_rate"] = summary_display[
                    "exercise_balanced_success_rate"
                ].map(
                    lambda value: f"{(float(value) if value is not None else 0.0) * 100.0:.2f}%"
                )
                st.dataframe(
                    summary_display,
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "work_mode": "Work mode",
                        "attempts": st.column_config.NumberColumn("Attempts", format="%d"),
                        "unique_students": st.column_config.NumberColumn("Unique students", format="%d"),
                        "unique_modules_explored": st.column_config.NumberColumn(
                            "Unique modules explored", format="%d"
                        ),
                        "unique_objectives_explored": st.column_config.NumberColumn(
                            "Unique objectives explored", format="%d"
                        ),
                        "unique_activities_explored": st.column_config.NumberColumn(
                            "Unique activities explored", format="%d"
                        ),
                        "median_attempts_per_activity": st.column_config.NumberColumn(
                            "Median attempts per activity",
                            format="%.2f",
                        ),
                        "repeat_attempt_rate": "Repeat attempt rate",
                        "success_rate": "Success rate (attempt-weighted)",
                        "exercise_balanced_success_rate": "Success rate (exercise-balanced)",
                    },
                )

    st.subheader("Bottleneck Candidates")
    bottleneck_level = st.radio(
        "Bottleneck level",
        options=["Module", "Objective", "Activity"],
        horizontal=True,
        index=2,
    )
    st.caption(
        f"This view ranks {bottleneck_level.lower()} entities where learners struggle most, combining failure frequency and repeat-attempt signals."
    )
    if bottleneck_level == "Module":
        st.caption("Module level applies date + module filters and ignores objective/activity filters.")
    elif bottleneck_level == "Objective":
        st.caption("Objective level applies date + module + objective filters and ignores activity filter.")
    else:
        st.caption("Activity level applies date + module + objective + activity filters.")
    bottleneck_source = apply_bottleneck_filters(
        frame=activity,
        start_date=start_date,
        end_date=end_date,
        module_code=module_filter,
        objective_id=objective_filter,
        activity_id=activity_filter,
        level=bottleneck_level,
    )
    bottleneck_df = build_bottleneck_frame(
        filtered_activity=bottleneck_source,
        level=bottleneck_level,
        min_attempts=min_attempts_for_bottleneck,
        top_n=top_n_bottlenecks,
    )
    if bottleneck_df.empty:
        canonical_scope = ", ".join(ACTIVE_CANONICAL_MODULE_CODES)
        st.info(
            f"No bottleneck rows after filters in canonical module scope ({canonical_scope})."
        )
    else:
        bottleneck_df["entity_axis_label"] = bottleneck_df["entity_plot_label"].map(
            lambda s: format_axis_label(str(s), max_chars=72)
        )
        axis_collision_count = (
            bottleneck_df.groupby("entity_axis_label")["entity_id"].transform("size").astype(int)
        )
        bottleneck_df["entity_axis_label"] = [
            label if int(collision_count) <= 1 else f"{label} #{str(entity_id)[:8]}"
            for label, collision_count, entity_id in zip(
                bottleneck_df["entity_axis_label"],
                axis_collision_count,
                bottleneck_df["entity_id"],
                strict=False,
            )
        ]
        bottleneck_df["entity_hover"] = [
            compose_hover_label(label, entity_id, show_ids)
            for label, entity_id in zip(
                bottleneck_df["entity_label_raw"],
                bottleneck_df["entity_id"],
                strict=False,
            )
        ]
        bottleneck_df["score_text"] = bottleneck_df["bottleneck_score"].map(lambda x: f"{x:.2f}")
        chart_rows = len(bottleneck_df.index)
        chart_height = max(420, 30 * chart_rows)
        fig_bottleneck = px.bar(
            bottleneck_df.sort_values("bottleneck_score", ascending=True),
            x="bottleneck_score",
            y="entity_axis_label",
            orientation="h",
            color="failure_rate",
            color_continuous_scale="YlGnBu",
            text="score_text",
            custom_data=[
                "entity_hover",
                "level",
                "attempts",
                "failure_rate",
                "repeat_attempt_rate",
            ],
            title=f"Top {bottleneck_level.lower()} bottleneck candidates by combined score",
            labels={
                "bottleneck_score": "Bottleneck score",
                "entity_axis_label": f"{bottleneck_level} entity",
                "failure_rate": "Failure rate",
            },
        )
        fig_bottleneck.update_traces(
            textposition="outside",
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Level: %{customdata[1]}<br>"
                "Bottleneck score: %{x:.3f}<br>"
                "Attempts: %{customdata[2]:,}<br>"
                "Failure rate: %{customdata[3]:.2%}<br>"
                "Repeat attempt rate: %{customdata[4]:.2%}<extra></extra>"
            ),
        )
        fig_bottleneck.update_layout(
            height=chart_height,
            margin={"l": 340, "r": 20, "t": 56, "b": 36},
            font={"size": 13},
            coloraxis_colorbar={"title": "Failure rate"},
        )
        fig_bottleneck.update_xaxes(showgrid=True, gridcolor="rgba(23,34,27,0.14)")
        fig_bottleneck.update_yaxes(showgrid=False)
        st.plotly_chart(fig_bottleneck, width='stretch')

    st.subheader("Path Transitions")
    st.caption(
        "This view shows the most common activity-to-activity paths across different objectives in the selected period."
    )
    transition_edges = load_top_transition_edges(
        transition_path=transition_path,
        start_date=start_date,
        end_date=end_date,
        module_code=module_filter,
        activity_id=activity_filter,
        top_n=top_n_transitions,
        has_same_objective_rate=transition_has_same_objective_rate,
    ).to_pandas()
    if transition_edges.empty:
        st.info("No cross-objective transition rows after filters.")
    else:
        transition_edges["from_display_raw"] = [
            _label_or_id(src_label, src_id)
            for src_label, src_id in zip(
                transition_edges.get(
                    "from_activity_label", transition_edges["from_activity_id"]
                ),
                transition_edges["from_activity_id"],
                strict=False,
            )
        ]
        transition_edges["to_display_raw"] = [
            _label_or_id(dst_label, dst_id)
            for dst_label, dst_id in zip(
                transition_edges.get("to_activity_label", transition_edges["to_activity_id"]),
                transition_edges["to_activity_id"],
                strict=False,
            )
        ]
        transition_edges["edge_base"] = [
            f"{format_axis_label(_label_or_id(src_label, src_id), max_chars=36)} -> "
            f"{format_axis_label(_label_or_id(dst_label, dst_id), max_chars=36)}"
            for src_label, src_id, dst_label, dst_id in zip(
                transition_edges.get(
                    "from_activity_label", transition_edges["from_activity_id"]
                ),
                transition_edges["from_activity_id"],
                transition_edges.get("to_activity_label", transition_edges["to_activity_id"]),
                transition_edges["to_activity_id"],
                strict=False,
            )
        ]
        edge_collision_count = (
            transition_edges.groupby("edge_base")["from_activity_id"].transform("size").astype(int)
        )
        transition_edges["edge"] = [
            edge_base
            if int(collision_count) <= 1
            else f"{edge_base} #{str(from_id)[:8]}->{str(to_id)[:8]}"
            for edge_base, collision_count, from_id, to_id in zip(
                transition_edges["edge_base"],
                edge_collision_count,
                transition_edges["from_activity_id"],
                transition_edges["to_activity_id"],
                strict=False,
            )
        ]
        transition_edges["from_hover"] = [
            compose_hover_label(_label_or_id(label, edge_id), edge_id, show_ids)
            for label, edge_id in zip(
                transition_edges["from_activity_label"],
                transition_edges["from_activity_id"],
                strict=False,
            )
        ]
        transition_edges["to_hover"] = [
            compose_hover_label(_label_or_id(label, edge_id), edge_id, show_ids)
            for label, edge_id in zip(
                transition_edges["to_activity_label"],
                transition_edges["to_activity_id"],
                strict=False,
            )
        ]
        transition_edges["count_text"] = transition_edges["transition_count"].map(
            lambda x: f"{int(x):,}"
        )
        edge_rows = len(transition_edges.index)
        edge_height = max(420, 30 * edge_rows)
        fig_edges = px.bar(
            transition_edges.sort_values("transition_count", ascending=True),
            x="transition_count",
            y="edge",
            orientation="h",
            color="success_conditioned_count",
            color_continuous_scale="Viridis",
            text="count_text",
            custom_data=[
                "from_hover",
                "to_hover",
                "transition_count",
                "success_conditioned_count",
            ],
            title="Top cross-objective activity transitions by count",
            labels={
                "transition_count": "Transition count",
                "edge": "Activity path",
                "success_conditioned_count": "Successful destination attempts (count)",
            },
        )
        fig_edges.update_traces(
            textposition="outside",
            hovertemplate=(
                "<b>From</b>: %{customdata[0]}<br>"
                "<b>To</b>: %{customdata[1]}<br>"
                "Transitions: %{customdata[2]:,}<br>"
                "Successful destination attempts: %{customdata[3]:,}<extra></extra>"
            ),
        )
        fig_edges.update_layout(
            height=edge_height,
            margin={"l": 340, "r": 20, "t": 56, "b": 36},
            font={"size": 13},
            coloraxis_colorbar={"title": "Successful destination attempts (count)"},
        )
        fig_edges.update_xaxes(showgrid=True, gridcolor="rgba(23,34,27,0.14)")
        fig_edges.update_yaxes(showgrid=False)
        st.plotly_chart(fig_edges, width='stretch')

    st.subheader("Data Quality Panel")
    st.caption("Contract and consistency checks from the latest build are shown below.")
    status = report.get("status", "unknown")
    if status == "pass":
        st.success("Consistency status: PASS")
    else:
        st.error("Consistency status: FAIL")

    failed_checks = [
        {"check": name, "actual": payload.get("actual"), "expected": payload.get("expected")}
        for name, payload in (report.get("checks") or {}).items()
        if not payload.get("pass", False)
    ]
    if failed_checks:
        st.dataframe(pl.DataFrame(failed_checks), width='stretch')
    else:
        st.caption("All configured checks passed.")


if __name__ == "__main__":
    main()

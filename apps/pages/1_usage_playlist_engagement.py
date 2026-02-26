from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import plotly.express as px
import polars as pl
import pyarrow.parquet as pq
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from visu2.config import get_settings
from visu2.contracts import DERIVED_SCHEMA_VERSION, RUNTIME_CORE_COLUMNS
from visu2.reporting import load_derived_manifest
from runtime_bootstrap import bootstrap_runtime_assets


st.markdown(
    """
<style>
h1, h2, h3 {
  font-family: "Fraunces", Georgia, serif !important;
}
div, p, label {
  font-family: "IBM Plex Sans", sans-serif !important;
}
[data-testid="stMetric"] {
  border: 1px solid rgba(23, 34, 27, 0.10);
  border-radius: 14px;
  padding: 0.85rem;
}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def load_tables(derived_dir: Path) -> dict[str, pl.DataFrame]:
    return {
        "module_daily": pl.read_parquet(derived_dir / "agg_module_usage_daily.parquet"),
        "student_exposure": pl.read_parquet(derived_dir / "agg_student_module_exposure.parquet"),
        "playlist_module": pl.read_parquet(derived_dir / "agg_playlist_module_usage.parquet"),
        "module_activity": pl.read_parquet(derived_dir / "agg_module_activity_usage.parquet"),
    }


def _collect_lazy(lf: pl.LazyFrame) -> pl.DataFrame:
    """Prefer streaming execution on large lazy plans to reduce peak memory."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect()


@st.cache_data(show_spinner=False)
def load_classroom_values(
    fact_path: Path,
    start_date: date,
    end_date: date,
    module_code: str | None,
) -> list[str]:
    lf = pl.scan_parquet(fact_path).filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_code:
        lf = lf.filter(pl.col("module_code") == module_code)
    df = _collect_lazy(
        lf.select("classroom_id")
        .drop_nulls()
        .filter(pl.col("classroom_id") != "None")
        .unique()
        .sort("classroom_id")
    )
    return df["classroom_id"].to_list()


def _parquet_columns(path: Path) -> list[str]:
    return list(pq.ParquetFile(path).schema_arrow.names)


def _label_or_id(label: str | None, identifier: str | None) -> str:
    if isinstance(label, str) and label.strip():
        return label.strip()
    return str(identifier or "")


def _format_option(label: str | None, identifier: str | None) -> str:
    base = _label_or_id(label, identifier)
    if isinstance(identifier, str) and identifier:
        return f"{base} [{identifier}]"
    return base


def _compatibility_status(
    table_columns: dict[str, list[str]],
    manifest_path: Path,
) -> tuple[str, dict[str, list[str]], list[str], str]:
    missing_core: dict[str, list[str]] = {}
    messages: list[str] = []
    schema_version = "missing"

    required_tables = {
        "fact_attempt_core",
        "agg_module_usage_daily",
        "agg_student_module_exposure",
        "agg_playlist_module_usage",
        "agg_module_activity_usage",
    }
    for table_name in required_tables:
        required_cols = RUNTIME_CORE_COLUMNS.get(table_name, [])
        actual_cols = set(table_columns.get(table_name, []))
        missing = [col for col in required_cols if col not in actual_cols]
        if missing:
            missing_core[table_name] = missing

    try:
        manifest = load_derived_manifest(manifest_path)
        schema_version = str(manifest.get("schema_version") or "missing")
        if schema_version != DERIVED_SCHEMA_VERSION:
            messages.append(
                f"Manifest schema mismatch: found '{schema_version}', expected '{DERIVED_SCHEMA_VERSION}'."
            )
        manifest_tables = manifest.get("tables")
        if isinstance(manifest_tables, dict):
            for table_name in required_tables:
                if table_name not in manifest_tables:
                    messages.append(f"Manifest missing table: {table_name}.")
    except FileNotFoundError:
        messages.append("Derived manifest missing.")
    except ValueError as err:
        messages.append(f"Derived manifest invalid: {err}")

    if missing_core:
        return "incompatible", missing_core, messages, schema_version
    if messages:
        return "degraded", missing_core, messages, schema_version
    return "ok", missing_core, messages, schema_version


def _build_filtered_fact(
    fact_path: Path,
    start_date: date,
    end_date: date,
    module_code: str | None,
    classroom_id: str | None,
    playlist_id: str | None,
) -> pl.LazyFrame:
    lf = pl.scan_parquet(fact_path).filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_code:
        lf = lf.filter(pl.col("module_code") == module_code)
    if classroom_id:
        lf = lf.filter(pl.col("classroom_id") == classroom_id)
    if playlist_id:
        lf = lf.filter(pl.col("playlist_or_module_id") == playlist_id)
    return lf


def main() -> None:
    bootstrap_runtime_assets()
    settings = get_settings()
    derived_dir = settings.artifacts_derived_dir
    fact_path = derived_dir / "fact_attempt_core.parquet"
    manifest_path = settings.derived_manifest_path

    required = [
        fact_path,
        derived_dir / "agg_module_usage_daily.parquet",
        derived_dir / "agg_student_module_exposure.parquet",
        derived_dir / "agg_playlist_module_usage.parquet",
        derived_dir / "agg_module_activity_usage.parquet",
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        st.error("Missing derived artifacts for IDEE analysis page.")
        st.code("\n".join(str(p) for p in missing))
        st.stop()

    data = load_tables(derived_dir)
    module_daily = data["module_daily"]
    student_exposure = data["student_exposure"]
    playlist_module = data["playlist_module"]
    module_activity = data["module_activity"]

    status, missing_core, _messages, _schema_version = _compatibility_status(
        table_columns={
            "fact_attempt_core": _parquet_columns(fact_path),
            "agg_module_usage_daily": module_daily.columns,
            "agg_student_module_exposure": student_exposure.columns,
            "agg_playlist_module_usage": playlist_module.columns,
            "agg_module_activity_usage": module_activity.columns,
        },
        manifest_path=manifest_path,
    )

    st.title("Usage, Playlist and Engagement (IDEE Feasible-Now)")
    st.caption(
        "Pre/post analyses are intentionally deferred until external IDEE post-test datasets are integrated."
    )

    if status == "incompatible":
        st.error("Core schema is incompatible. Rebuild artifacts.")
        for table_name, cols in missing_core.items():
            st.markdown(f"- `{table_name}` missing: {', '.join(cols)}")
        st.stop()

    min_date = module_daily["date_utc"].min()
    max_date = module_daily["date_utc"].max()
    if min_date is None or max_date is None:
        st.error("No data available.")
        st.stop()

    st.sidebar.header("Filters")
    start_date, end_date = st.sidebar.date_input(
        "Date range (UTC)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("Please select a valid date range.")
        st.stop()

    module_frame = (
        module_daily.select(["module_code", "module_label"])
        .drop_nulls("module_code")
        .unique()
        .sort("module_code")
    )
    module_options = {"All": None}
    for row in module_frame.to_dicts():
        module_options[_format_option(row.get("module_label"), row.get("module_code"))] = row.get(
            "module_code"
        )
    selected_module = st.sidebar.selectbox("Module", list(module_options.keys()))
    module_filter = module_options[selected_module]

    classroom_values = load_classroom_values(
        fact_path=fact_path,
        start_date=start_date,
        end_date=end_date,
        module_code=module_filter,
    )
    classroom_options = ["All"] + classroom_values
    selected_classroom = st.sidebar.selectbox("Classroom (optional)", classroom_options)
    classroom_filter = None if selected_classroom == "All" else selected_classroom

    playlist_frame = playlist_module
    if module_filter:
        playlist_frame = playlist_frame.filter(pl.col("module_code") == module_filter)
    playlist_values = (
        playlist_frame.select("playlist_or_module_id")
        .drop_nulls()
        .unique()
        .sort("playlist_or_module_id")["playlist_or_module_id"]
        .to_list()
    )
    playlist_options = ["All"] + playlist_values
    selected_playlist = st.sidebar.selectbox("Playlist (optional)", playlist_options)
    playlist_filter = None if selected_playlist == "All" else selected_playlist

    st.sidebar.header("Diligent Thresholds")
    min_attempts = int(st.sidebar.number_input("Min attempts", min_value=1, value=10, step=1))
    min_active_days = int(st.sidebar.number_input("Min active days", min_value=1, value=3, step=1))
    min_total_time_minutes = int(
        st.sidebar.number_input("Min total time (minutes)", min_value=1, value=60, step=5)
    )

    fact_filtered = _build_filtered_fact(
        fact_path=fact_path,
        start_date=start_date,
        end_date=end_date,
        module_code=module_filter,
        classroom_id=classroom_filter,
        playlist_id=playlist_filter,
    )

    exposure_filtered = (
        fact_filtered.with_columns(
            pl.coalesce(
                [
                    pl.col("session_duration").cast(pl.Float64),
                    pl.col("data_duration").cast(pl.Float64),
                ]
            ).alias("attempt_time_seconds")
        )
        .group_by(["user_id", "module_id", "module_code", "module_label"])
        .agg(
            pl.len().alias("attempts"),
            pl.col("activity_id").drop_nulls().n_unique().alias("unique_activities"),
            pl.col("date_utc").drop_nulls().n_unique().alias("active_days"),
            pl.col("attempt_time_seconds").sum().alias("total_time_seconds"),
        )
        .with_columns(
            (pl.col("attempts") > 10).alias("is_effective_user"),
            pl.when(pl.col("attempts") <= 10)
            .then(pl.lit("low<=10"))
            .when(pl.col("attempts") <= 50)
            .then(pl.lit("mid11-50"))
            .otherwise(pl.lit("high>50"))
            .alias("exposure_bucket"),
        )
    )
    exposure_filtered = _collect_lazy(exposure_filtered)

    st.subheader("Exposure Overview")
    st.caption("This section segments student-module exposure using attempt volume and highlights effective usage.")
    students_in_scope = int(exposure_filtered["user_id"].n_unique()) if exposure_filtered.height else 0
    effective_share = (
        float(exposure_filtered["is_effective_user"].cast(pl.Float64).mean()) if exposure_filtered.height else 0.0
    )
    median_attempts = (
        float(exposure_filtered["attempts"].median()) if exposure_filtered.height else 0.0
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Students in scope", f"{students_in_scope:,}")
    c2.metric("Effective users (>10 attempts)", f"{effective_share * 100:.2f}%")
    c3.metric("Median attempts / student-module", f"{median_attempts:.1f}")

    if exposure_filtered.height == 0:
        st.info("No exposure rows after filters.")
    else:
        exposure_plot = (
            exposure_filtered.group_by(["module_code", "module_label", "exposure_bucket"])
            .agg(pl.len().alias("student_modules"))
            .with_columns(
                pl.struct(["module_label", "module_code"])
                .map_elements(lambda s: _label_or_id(s["module_label"], s["module_code"]), return_dtype=pl.Utf8)
                .alias("module_display")
            )
            .sort(["module_display", "exposure_bucket"])
            .to_pandas()
        )
        fig_exposure = px.bar(
            exposure_plot,
            x="module_display",
            y="student_modules",
            color="exposure_bucket",
            barmode="stack",
            title="Exposure bucket distribution by module",
        )
        st.plotly_chart(fig_exposure, width='stretch')

    st.subheader("Module Usage Trends")
    st.caption("Daily attempts and student reach are shown to track module usage intensity over time.")
    module_daily_filtered = module_daily.filter(
        (pl.col("date_utc") >= pl.lit(start_date)) & (pl.col("date_utc") <= pl.lit(end_date))
    )
    if module_filter:
        module_daily_filtered = module_daily_filtered.filter(pl.col("module_code") == module_filter)
    if module_daily_filtered.height == 0:
        st.info("No module usage rows after filters.")
    else:
        module_daily_plot = (
            module_daily_filtered.with_columns(
                pl.struct(["module_label", "module_code"])
                .map_elements(lambda s: _label_or_id(s["module_label"], s["module_code"]), return_dtype=pl.Utf8)
                .alias("module_display")
            )
            .to_pandas()
        )
        fig_attempts = px.line(
            module_daily_plot,
            x="date_utc",
            y="attempts",
            color="module_display",
            title="Attempts over time by module",
        )
        st.plotly_chart(fig_attempts, width='stretch')
        fig_students = px.area(
            module_daily_plot,
            x="date_utc",
            y="unique_students",
            color="module_display",
            title="Unique students over time by module",
        )
        st.plotly_chart(fig_students, width='stretch')

    st.subheader("Module/Playlist Analytics")
    st.caption(
        "This table summarizes usage per module-or-playlist identifier. "
        "When the row is a playlist, module_code is marked as non-applicable."
    )
    st.caption("Date/classroom filters do not apply to this pre-aggregated table grain.")
    playlist_filtered = playlist_module
    if module_filter:
        playlist_filtered = playlist_filtered.filter(pl.col("module_code") == module_filter)
    if playlist_filter:
        playlist_filtered = playlist_filtered.filter(pl.col("playlist_or_module_id") == playlist_filter)
    top_playlists = (
        playlist_filtered.sort("attempts", descending=True)
        .head(25)
        .with_columns(
            pl.coalesce(
                [
                    pl.col("module_label").cast(pl.Utf8),
                    pl.col("module_code").cast(pl.Utf8),
                    pl.lit("Playlist"),
                ]
            ).alias("entity_name"),
            pl.col("playlist_or_module_id").alias("entity_id"),
            pl.when(pl.col("work_mode") == "playlist")
            .then(pl.lit("non-applicable"))
            .otherwise(
                pl.coalesce(
                    [
                        pl.col("module_code").cast(pl.Utf8),
                        pl.lit("non-applicable"),
                    ]
                )
            )
            .alias("module_code")
        )
        .select(
            [
                "entity_name",
                "work_mode",
                "module_code",
                "entity_id",
                "attempts",
                "unique_students",
                "unique_classrooms",
                "unique_activities",
                "success_rate",
            ]
        )
        .rename({"entity_id": "playlist_or_module_id"})
        .to_pandas()
    )
    if top_playlists.empty:
        st.info("No playlist rows after filters.")
    else:
        st.dataframe(top_playlists, width='stretch')
        playlist_intensity = (
            playlist_filtered.group_by(["module_code", "module_label"])
            .agg(pl.col("unique_activities").mean().alias("avg_activities_per_playlist"))
            .with_columns(
                pl.struct(["module_label", "module_code"])
                .map_elements(lambda s: _label_or_id(s["module_label"], s["module_code"]), return_dtype=pl.Utf8)
                .alias("module_display")
            )
            .sort("avg_activities_per_playlist", descending=True)
            .to_pandas()
        )
        fig_intensity = px.bar(
            playlist_intensity,
            x="module_display",
            y="avg_activities_per_playlist",
            title="Average activities per playlist by module",
            color="avg_activities_per_playlist",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig_intensity, width='stretch')

    st.subheader("Activity Usage within Module")
    st.caption("Activities are ranked within module context by attempts or unique students.")
    metric_choice = st.radio(
        "Rank activities by",
        options=["attempts", "unique_students"],
        horizontal=True,
    )
    activity_filtered = module_activity
    if module_filter:
        activity_filtered = activity_filtered.filter(pl.col("module_code") == module_filter)
    top_activities = (
        activity_filtered.sort(metric_choice, descending=True)
        .head(25)
        .with_columns(
            pl.struct(["activity_label", "activity_id"])
            .map_elements(lambda s: _label_or_id(s["activity_label"], s["activity_id"]), return_dtype=pl.Utf8)
            .alias("activity_display"),
            pl.struct(["module_label", "module_code"])
            .map_elements(lambda s: _label_or_id(s["module_label"], s["module_code"]), return_dtype=pl.Utf8)
            .alias("module_display"),
        )
        .with_columns(
            pl.len().over("activity_display").alias("label_count"),
            pl.col("activity_id").cum_count().over(["activity_display", "module_display"]).alias("dup_idx"),
        )
        .with_columns(
            pl.when(pl.col("label_count") <= 1)
            .then(pl.col("activity_display"))
            .when(pl.col("dup_idx") <= 1)
            .then(
                pl.concat_str(
                    [
                        pl.col("activity_display"),
                        pl.lit(" ("),
                        pl.col("module_display"),
                        pl.lit(")"),
                    ]
                )
            )
            .otherwise(
                pl.concat_str(
                    [
                        pl.col("activity_display"),
                        pl.lit(" ("),
                        pl.col("module_display"),
                        pl.lit(" #"),
                        pl.col("dup_idx").cast(pl.Utf8),
                        pl.lit(")"),
                    ]
                )
            )
            .alias("activity_display_plot")
        )
        .to_pandas()
    )
    if top_activities.empty:
        st.info("No activity rows after filters.")
    else:
        fig_activities = px.bar(
            top_activities.sort_values(metric_choice, ascending=True),
            x=metric_choice,
            y="activity_display_plot",
            orientation="h",
            color="activity_share_within_module",
            color_continuous_scale="Viridis",
            title="Top activities by selected metric",
        )
        st.plotly_chart(fig_activities, width='stretch')

    st.subheader("Diligent Learners Panel")
    st.caption(
        "Diligent learners are defined by configurable thresholds on attempts, active days, and total time."
    )
    if exposure_filtered.height == 0:
        st.info("No exposure rows available for diligent analysis.")
        return

    exposure_diligent = exposure_filtered.with_columns(
        (
            (pl.col("attempts") >= min_attempts)
            & (pl.col("active_days") >= min_active_days)
            & (pl.col("total_time_seconds") >= min_total_time_minutes * 60.0)
        ).alias("is_diligent")
    )
    diligent_count = int(exposure_diligent.filter(pl.col("is_diligent")).height)
    diligent_share = diligent_count / max(exposure_diligent.height, 1)
    d1, d2 = st.columns(2)
    d1.metric("Diligent student-module pairs", f"{diligent_count:,}")
    d2.metric("Diligent share", f"{diligent_share * 100:.2f}%")

    diligent_flags = exposure_diligent.select(["user_id", "module_code", "is_diligent"])
    perf = (
        fact_filtered.join(diligent_flags.lazy(), on=["user_id", "module_code"], how="left")
        .with_columns(pl.col("is_diligent").fill_null(False))
        .group_by("is_diligent")
        .agg(
            pl.len().alias("attempts"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"),
            pl.col("data_duration").median().alias("median_duration"),
            (pl.col("attempt_number") > 1).cast(pl.Float64).mean().alias("repeat_attempt_rate"),
        )
        .sort("is_diligent", descending=True)
        .with_columns(
            pl.when(pl.col("is_diligent"))
            .then(pl.lit("diligent"))
            .otherwise(pl.lit("non_diligent"))
            .alias("group")
        )
    )
    perf = _collect_lazy(perf).to_pandas()
    st.dataframe(
        perf[["group", "attempts", "success_rate", "median_duration", "repeat_attempt_rate"]],
        width='stretch',
    )


if __name__ == "__main__":
    main()

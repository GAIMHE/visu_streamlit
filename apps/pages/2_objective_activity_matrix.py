from __future__ import annotations

import sys
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
APPS_DIR = ROOT_DIR / "apps"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

from visu2.config import get_settings
from visu2.contracts import RUNTIME_CORE_COLUMNS
from visu2.loaders import load_learning_catalog
from visu2.objective_activity_matrix import (
    VALID_MATRIX_METRICS,
    build_exercise_drilldown_frame,
    build_objective_activity_cells,
    build_ragged_matrix_payload,
)
from runtime_bootstrap import bootstrap_runtime_assets

st.set_page_config(
    page_title="Objective Activity Matrix",
    page_icon=":bar_chart:",
    layout="wide",
)


st.markdown(
    """
<style>
h1, h2, h3 {
  font-family: "Fraunces", Georgia, serif !important;
}
div, p, label {
  font-family: "IBM Plex Sans", sans-serif !important;
}
</style>
""",
    unsafe_allow_html=True,
)


METRIC_LABELS = {
    "attempts": "Attempts",
    "success_rate": "Success rate",
    "exercise_balanced_success_rate": "Exercise-balanced success rate",
    "repeat_attempt_rate": "Repeat attempt rate",
    "first_attempt_success_rate": "First-attempt success rate",
    "playlist_unique_exercises": "Playlist unique exercises",
}


@st.cache_data(show_spinner=False)
def load_activity_daily(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_exercise_daily(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_catalog_payload(path: Path) -> dict:
    return load_learning_catalog(path)


def _label_or_id(label: str | None, identifier: str | None) -> str:
    if isinstance(label, str) and label.strip():
        return label.strip()
    return str(identifier or "")


def _format_option(label: str | None, identifier: str | None) -> str:
    base = _label_or_id(label, identifier)
    if isinstance(identifier, str) and identifier:
        return f"{base} [{identifier}]"
    return base


def _truncate_axis_label(text: str, max_chars: int = 48) -> str:
    normalized = str(text or "").strip()
    if len(normalized) <= max_chars:
        return normalized
    return f"{normalized[: max_chars - 1].rstrip()}..."


def _dedupe_labels(labels: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    output: list[str] = []
    for label in labels:
        counts[label] = counts.get(label, 0) + 1
        if counts[label] == 1:
            output.append(label)
        else:
            output.append(f"{label} #{counts[label]}")
    return output


def _ensure_label_columns(frame: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
    missing: list[str] = []
    normalized = frame

    fallback_map = {
        "module_label": "module_code",
        "objective_label": "objective_id",
        "activity_label": "activity_id",
    }
    for label_col, fallback_col in fallback_map.items():
        if label_col not in normalized.columns:
            missing.append(label_col)
            normalized = normalized.with_columns(
                pl.col(fallback_col).cast(pl.Utf8).alias(label_col)
            )
        else:
            normalized = normalized.with_columns(
                pl.coalesce(
                    [
                        pl.col(label_col).cast(pl.Utf8),
                        pl.col(fallback_col).cast(pl.Utf8),
                    ]
                ).alias(label_col)
            )
    return normalized, missing


def _extract_selected_cell(
    event: object,
    cell_lookup: dict[tuple[str, str], dict[str, str]] | None = None,
    x_labels: list[str] | None = None,
    y_labels: list[str] | None = None,
) -> dict[str, str] | None:
    if not isinstance(event, dict):
        return None
    selection = event.get("selection")
    if not isinstance(selection, dict):
        return None
    points = selection.get("points")
    if not isinstance(points, list) or not points:
        return None
    first_point = points[0]
    if not isinstance(first_point, dict):
        return None
    customdata = first_point.get("customdata")
    if isinstance(customdata, (list, tuple)) and len(customdata) >= 4:
        objective_label = str(customdata[0] or "").strip()
        objective_id = str(customdata[1] or "").strip()
        activity_label = str(customdata[2] or "").strip()
        activity_id = str(customdata[3] or "").strip()
        if objective_id and activity_id:
            return {
                "objective_label": objective_label,
                "objective_id": objective_id,
                "activity_label": activity_label,
                "activity_id": activity_id,
            }

    # Fallback: resolve clicked matrix cell from x/y axis coordinates.
    x_raw = first_point.get("x")
    y_raw = first_point.get("y")
    x_value = str(x_raw or "").strip()
    y_value = str(y_raw or "").strip()
    point_index = first_point.get("point_index")
    if (
        isinstance(point_index, (list, tuple))
        and len(point_index) == 2
        and x_labels is not None
        and y_labels is not None
    ):
        row_idx, col_idx = point_index
        if isinstance(row_idx, int) and isinstance(col_idx, int):
            if 0 <= col_idx < len(x_labels):
                x_value = str(x_labels[col_idx])
            if 0 <= row_idx < len(y_labels):
                y_value = str(y_labels[row_idx])
    if not x_value and isinstance(x_raw, (int, float)) and x_labels is not None:
        x_idx = int(x_raw)
        if 0 <= x_idx < len(x_labels):
            x_value = str(x_labels[x_idx])
    if not y_value and isinstance(y_raw, (int, float)) and y_labels is not None:
        y_idx = int(y_raw)
        if 0 <= y_idx < len(y_labels):
            y_value = str(y_labels[y_idx])
    if cell_lookup is not None and x_value and y_value:
        mapped = cell_lookup.get((y_value, x_value))
        if mapped:
            objective_id = str(mapped.get("objective_id") or "").strip()
            activity_id = str(mapped.get("activity_id") or "").strip()
            if objective_id and activity_id:
                return {
                    "objective_label": str(mapped.get("objective_label") or objective_id),
                    "objective_id": objective_id,
                    "activity_label": str(mapped.get("activity_label") or activity_id),
                    "activity_id": activity_id,
                }
    return None


def main() -> None:
    bootstrap_runtime_assets()
    settings = get_settings()
    activity_path = settings.artifacts_derived_dir / "agg_activity_daily.parquet"
    exercise_daily_path = settings.artifacts_derived_dir / "agg_exercise_daily.parquet"
    fact_path = settings.artifacts_derived_dir / "fact_attempt_core.parquet"
    catalog_path = settings.learning_catalog_path

    if not activity_path.exists():
        st.error("Missing derived artifact: agg_activity_daily.parquet")
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    activity_raw = load_activity_daily(activity_path)
    missing_core = [
        col
        for col in RUNTIME_CORE_COLUMNS["agg_activity_daily"]
        if col not in activity_raw.columns
    ]
    if missing_core:
        st.error("Matrix page is incompatible: required core columns are missing.")
        st.markdown("- " + "\n- ".join(f"`{col}`" for col in missing_core))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    activity, missing_labels = _ensure_label_columns(activity_raw)
    summary_payload = load_catalog_payload(catalog_path)
    has_first_attempt_columns = {
        "first_attempt_success_rate",
        "first_attempt_count",
    }.issubset(set(activity.columns))
    fact_columns = set()
    if fact_path.exists():
        try:
            import pyarrow.parquet as pq

            fact_columns = set(pq.ParquetFile(fact_path).schema_arrow.names)
        except Exception:
            fact_columns = set()
    has_playlist_unique_metric = {
        "date_utc",
        "module_code",
        "objective_id",
        "activity_id",
        "exercise_id",
        "work_mode",
    }.issubset(fact_columns)

    min_date = activity["date_utc"].min()
    max_date = activity["date_utc"].max()
    if min_date is None or max_date is None:
        st.info("No rows available in agg_activity_daily.")
        st.stop()

    module_frame = (
        activity.select(["module_code", "module_label"])
        .drop_nulls("module_code")
        .unique()
        .sort("module_code")
    )
    if module_frame.height == 0:
        st.info("No module_code values available in agg_activity_daily.")
        st.stop()

    module_options_map: dict[str, str] = {}
    module_code_to_display: dict[str, str] = {}
    for row in module_frame.to_dicts():
        module_code = row.get("module_code")
        module_display = _label_or_id(row.get("module_label"), module_code)
        module_options_map[_format_option(row.get("module_label"), module_code)] = str(module_code)
        module_code_to_display[str(module_code)] = module_display

    st.title("Objective-Activity Matrix Heatmap")
    st.caption(
        "Rows are objectives from one selected module. Columns are objective-local activity positions (A1..An)."
    )
    st.caption(
        "Color and optional cell text encode the selected metric across the selected date range."
    )
    if missing_labels:
        st.warning(
            "Some label columns are missing in derived artifacts and were rebuilt from IDs: "
            + ", ".join(sorted(missing_labels))
        )
    if not has_first_attempt_columns:
        st.info(
            "First-attempt success metric is unavailable with current artifacts. "
            "Rebuild derived data to enable it: `uv run python scripts/build_derived.py --strict-checks`."
        )

    exercise_daily: pl.DataFrame | None = None
    exercise_daily_status = "ok"
    if not exercise_daily_path.exists():
        exercise_daily_status = "missing"
    else:
        exercise_daily = load_exercise_daily(exercise_daily_path)
        missing_exercise_core = [
            col
            for col in RUNTIME_CORE_COLUMNS["agg_exercise_daily"]
            if col not in exercise_daily.columns
        ]
        if missing_exercise_core:
            exercise_daily_status = "incompatible"
            st.warning(
                "Exercise drilldown table has incompatible schema; rebuild artifacts to enable drilldown."
            )
            st.markdown("- " + "\n- ".join(f"`{col}`" for col in missing_exercise_core))
            st.code("uv run python scripts/build_derived.py --strict-checks")
    has_exercise_balanced_metric = exercise_daily_status == "ok"
    if not has_exercise_balanced_metric:
        st.info(
            "Exercise-balanced success metric is unavailable because `agg_exercise_daily` is missing or incompatible."
        )
    if not has_playlist_unique_metric:
        st.info(
            "Playlist unique exercises metric is unavailable because `fact_attempt_core` is missing or incompatible."
        )

    st.sidebar.header("Matrix Controls")
    selected_module_label = st.sidebar.selectbox(
        "Module",
        options=list(module_options_map.keys()),
    )
    selected_module_code = module_options_map[selected_module_label]
    selected_module_display = module_code_to_display.get(selected_module_code, selected_module_code)

    start_date, end_date = st.sidebar.date_input(
        "Date range (UTC)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("Please select a valid start and end date.")
        st.stop()

    available_metrics = [
        metric
        for metric in VALID_MATRIX_METRICS
        if metric != "first_attempt_success_rate" or has_first_attempt_columns
    ]
    if not has_exercise_balanced_metric:
        available_metrics = [
            metric for metric in available_metrics if metric != "exercise_balanced_success_rate"
        ]
    if not has_playlist_unique_metric:
        available_metrics = [
            metric for metric in available_metrics if metric != "playlist_unique_exercises"
        ]
    metric_display_options = [METRIC_LABELS[metric] for metric in available_metrics]
    selected_metric_display = st.sidebar.selectbox(
        "Metric",
        options=metric_display_options,
        index=0,
    )
    metric = next(
        metric_key
        for metric_key, metric_label in METRIC_LABELS.items()
        if metric_label == selected_metric_display and metric_key in available_metrics
    )

    show_cell_values = bool(st.sidebar.checkbox("Show cell values", value=True))
    show_ids_in_hover = bool(st.sidebar.checkbox("Show IDs in hover", value=False))

    try:
        fact_lf = pl.scan_parquet(fact_path) if has_playlist_unique_metric else None
        cells_df = build_objective_activity_cells(
            agg_activity_daily=activity,
            module_code=selected_module_code,
            start_date=start_date,
            end_date=end_date,
            metric=metric,
            summary_payload=summary_payload,
            agg_exercise_daily=exercise_daily,
            fact_attempt_core=fact_lf,
        )
    except ValueError as err:
        st.error(str(err))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()
    # Keep only objectives with a human-readable label. If label fallback equals ID,
    # we treat it as unmapped and hide it from this matrix view.
    cells_df = cells_df.filter(
        pl.col("objective_label").is_not_null()
        & (pl.col("objective_label").str.strip_chars() != "")
        & (pl.col("objective_label") != pl.col("objective_id"))
    )
    if cells_df.height == 0:
        st.info(
            "No objective/activity rows with mapped human-readable objective labels "
            "for the selected module and date range."
        )
        st.stop()

    payload = build_ragged_matrix_payload(cells_df)
    if not payload["x_labels"] or not payload["y_labels"]:
        st.info("No matrix cells to render for current filters.")
        st.stop()

    y_axis_labels = _dedupe_labels(
        [_truncate_axis_label(label, max_chars=52) for label in payload["y_labels"]]
    )
    objective_axis_label = {
        objective_id: y_label
        for objective_id, y_label in zip(payload["objective_ids"], y_axis_labels, strict=False)
    }

    metric_colorbar_title = METRIC_LABELS[metric]
    hover_template = (
        "<b>Objective</b>: %{customdata[0]}<br>"
        + ("Objective ID: %{customdata[1]}<br>" if show_ids_in_hover else "")
        + "<b>Activity</b>: %{customdata[2]} (%{customdata[4]})<br>"
        + ("Activity ID: %{customdata[3]}<br>" if show_ids_in_hover else "")
        + f"<b>{metric_colorbar_title}</b>: %{{customdata[5]}}"
        + "<extra></extra>"
    )

    z_min = (
        0.0
        if metric
        in {
            "success_rate",
            "exercise_balanced_success_rate",
            "repeat_attempt_rate",
            "first_attempt_success_rate",
        }
        else None
    )
    z_max = (
        1.0
        if metric
        in {
            "success_rate",
            "exercise_balanced_success_rate",
            "repeat_attempt_rate",
            "first_attempt_success_rate",
        }
        else None
    )

    cell_points = cells_df.with_columns(
        pl.col("objective_id")
        .map_elements(lambda oid: objective_axis_label.get(str(oid), ""), return_dtype=pl.Utf8)
        .alias("axis_objective_label")
    ).select(
        [
            "objective_id",
            "objective_label",
            "axis_objective_label",
            "activity_id",
            "activity_label",
            "activity_col_label",
            "metric_value",
            "metric_text",
        ]
    )
    render_rows = cell_points.to_dicts()
    cell_lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in render_rows:
        axis_objective = str(row.get("axis_objective_label") or "").strip()
        activity_col_label = str(row.get("activity_col_label") or "").strip()
        if not axis_objective or not activity_col_label:
            continue
        cell_lookup[(axis_objective, activity_col_label)] = {
            "objective_label": str(row.get("objective_label") or row.get("objective_id") or ""),
            "objective_id": str(row.get("objective_id") or ""),
            "activity_label": str(row.get("activity_label") or row.get("activity_id") or ""),
            "activity_id": str(row.get("activity_id") or ""),
        }
    left_margin = min(
        300,
        max(170, int(max(len(label) for label in y_axis_labels) * 4.6)),
    )
    figure_height = max(460, 48 * len(payload["y_labels"]) + 140)
    approx_cell_height = (figure_height - 140) / max(1, len(y_axis_labels))
    approx_plot_width = max(980, 1320 - left_margin)
    approx_cell_width = approx_plot_width / max(1, len(payload["x_labels"]))
    selector_marker_size = max(18, min(96, int(min(approx_cell_height, approx_cell_width) * 1.05)))

    objective_id_by_axis_label: dict[str, str] = {
        y_axis_labels[idx]: str(objective_id)
        for idx, objective_id in enumerate(payload["objective_ids"])
    }
    objective_label_by_axis_label: dict[str, str] = {
        y_axis_labels[idx]: str(payload["y_labels"][idx])
        for idx in range(min(len(payload["y_labels"]), len(y_axis_labels)))
    }
    metric_text_grid = payload.get("z_text")
    if not isinstance(metric_text_grid, list):
        metric_text_grid = payload.get("text_values")
    if not isinstance(metric_text_grid, list):
        metric_text_grid = []

    customdata_grid: list[list[list[str]]] = []
    for row_idx, axis_objective_label in enumerate(y_axis_labels):
        objective_id = objective_id_by_axis_label.get(axis_objective_label, "")
        objective_label = objective_label_by_axis_label.get(
            axis_objective_label,
            objective_id or axis_objective_label,
        )
        customdata_row: list[list[str]] = []
        for col_idx, activity_col_label in enumerate(payload["x_labels"]):
            mapped = cell_lookup.get((axis_objective_label, str(activity_col_label)))
            metric_text = ""
            if row_idx < len(metric_text_grid) and col_idx < len(metric_text_grid[row_idx]):
                raw_metric_text = metric_text_grid[row_idx][col_idx]
                metric_text = "" if raw_metric_text is None else str(raw_metric_text)
            if mapped is None:
                customdata_row.append(
                    [
                        str(objective_label),
                        str(objective_id),
                        str(activity_col_label),
                        "",
                        str(activity_col_label),
                        metric_text,
                    ]
                )
            else:
                customdata_row.append(
                    [
                        str(mapped.get("objective_label") or objective_label),
                        str(mapped.get("objective_id") or objective_id),
                        str(mapped.get("activity_label") or mapped.get("activity_id") or activity_col_label),
                        str(mapped.get("activity_id") or ""),
                        str(activity_col_label),
                        metric_text,
                    ]
                )
        customdata_grid.append(customdata_row)

    selector_customdata = [
        [
            str(row.get("objective_label") or row.get("objective_id") or ""),
            str(row.get("objective_id") or ""),
            str(row.get("activity_label") or row.get("activity_id") or ""),
            str(row.get("activity_id") or ""),
            str(row.get("activity_col_label") or ""),
            str(row.get("metric_text") or ""),
        ]
        for row in render_rows
    ]

    heatmap_kwargs: dict[str, object] = {
        "z": payload["z_values"],
        "x": payload["x_labels"],
        "y": y_axis_labels,
        "customdata": customdata_grid,
        "colorscale": [
            [0.0, "#f6f2e6"],
            [0.5, "#9f86c0"],
            [1.0, "#2b0a8f"],
        ],
        "colorbar": {"title": metric_colorbar_title},
        "hovertemplate": hover_template,
        "hoverongaps": False,
        "xgap": 0,
        "ygap": 0,
    }
    if z_min is not None:
        heatmap_kwargs["zmin"] = z_min
    if z_max is not None:
        heatmap_kwargs["zmax"] = z_max
    if show_cell_values:
        heatmap_kwargs["text"] = metric_text_grid
        heatmap_kwargs["texttemplate"] = "%{text}"
        heatmap_kwargs["textfont"] = {"size": 11, "color": "#111"}

    fig = go.Figure()
    fig.add_trace(go.Heatmap(**heatmap_kwargs))
    # Transparent click layer to keep reliable point selection while preserving
    # the original heatmap visual style.
    fig.add_trace(
        go.Scatter(
            x=[str(row.get("activity_col_label") or "") for row in render_rows],
            y=[str(row.get("axis_objective_label") or "") for row in render_rows],
            mode="markers",
            customdata=selector_customdata,
            hovertemplate=hover_template,
            showlegend=False,
            marker={
                "symbol": "square",
                "size": selector_marker_size,
                "color": "rgba(0,0,0,0.001)",
                "line": {"width": 0.0},
            },
            selected={
                "marker": {
                    "color": "rgba(44, 96, 175, 0.18)",
                    "opacity": 0.22,
                    "size": selector_marker_size + 2,
                }
            },
            unselected={"marker": {"opacity": 0.001}},
        )
    )
    fig.update_layout(
        title=f"{selected_module_display} - {selected_metric_display} by objective/activity position",
        xaxis_title="Activity position inside each objective (A1..An)",
        yaxis_title="Objectives",
        height=figure_height,
        margin={"l": left_margin, "r": 24, "t": 70, "b": 70},
        font={"size": 13},
        clickmode="event+select",
        dragmode=False,
    )
    fig.update_xaxes(
        side="bottom",
        tickangle=-35,
        showgrid=False,
        categoryorder="array",
        categoryarray=payload["x_labels"],
    )
    fig.update_yaxes(
        autorange="reversed",
        showgrid=False,
        categoryorder="array",
        categoryarray=y_axis_labels,
    )

    context_key = "objective_activity_matrix_selection_context"
    selected_cell_key = "objective_activity_matrix_selected_cell"
    current_context = {
        "module_code": selected_module_code,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "metric": metric,
    }
    if st.session_state.get(context_key) != current_context:
        st.session_state[context_key] = current_context
        st.session_state.pop(selected_cell_key, None)

    event = st.plotly_chart(
        fig,
        key="objective_activity_matrix",
        width='stretch',
        on_select="rerun",
        selection_mode=("points",),
        config={
            "modeBarButtonsToRemove": ["select2d", "lasso2d"],
        },
    )
    selected_from_event = _extract_selected_cell(
        event,
        cell_lookup=cell_lookup,
        x_labels=payload["x_labels"],
        y_labels=y_axis_labels,
    )
    if selected_from_event is not None:
        st.session_state[selected_cell_key] = selected_from_event

    show_selection_debug = bool(st.sidebar.checkbox("Debug matrix selection", value=False))
    if show_selection_debug:
        st.sidebar.write(event)

    st.subheader("Exercise Drilldown")
    if exercise_daily_status == "missing":
        st.info(
            "Exercise drilldown is unavailable because `agg_exercise_daily.parquet` is missing. "
            "Run `uv run python scripts/build_derived.py --strict-checks`."
        )
        return
    if exercise_daily_status == "incompatible":
        st.info("Exercise drilldown is disabled until artifacts are rebuilt.")
        return

    selected_cell = st.session_state.get(selected_cell_key)
    if not isinstance(selected_cell, dict):
        with st.expander("Manual drilldown selection", expanded=False):
            st.caption(
                "If matrix click selection does not register, choose an objective and activity manually."
            )
            objective_manual_rows = (
                cells_df.select(["objective_id", "objective_label", "objective_row_label"])
                .unique()
                .sort(["objective_row_label", "objective_id"])
                .to_dicts()
            )
            objective_manual_map = {
                f"{str(row.get('objective_row_label') or row.get('objective_label') or row.get('objective_id'))} [{row.get('objective_id')}]": row
                for row in objective_manual_rows
            }
            if objective_manual_map:
                objective_manual_label = st.selectbox(
                    "Objective (manual)",
                    options=list(objective_manual_map.keys()),
                    key=f"manual_objective::{selected_module_code}::{start_date}::{end_date}::{metric}",
                )
                objective_manual = objective_manual_map[objective_manual_label]
                activity_manual_rows = (
                    cells_df.filter(pl.col("objective_id") == str(objective_manual.get("objective_id")))
                    .select(["activity_id", "activity_label", "activity_col_label"])
                    .unique()
                    .sort(["activity_col_label", "activity_id"])
                    .to_dicts()
                )
                activity_manual_map = {
                    f"{str(row.get('activity_col_label') or '')} - {str(row.get('activity_label') or row.get('activity_id'))} [{row.get('activity_id')}]": row
                    for row in activity_manual_rows
                }
                if activity_manual_map:
                    activity_manual_label = st.selectbox(
                        "Activity (manual)",
                        options=list(activity_manual_map.keys()),
                        key=f"manual_activity::{selected_module_code}::{start_date}::{end_date}::{metric}",
                    )
                    if st.button("Load exercise drilldown", key="manual_drilldown_load"):
                        activity_manual = activity_manual_map[activity_manual_label]
                        st.session_state[selected_cell_key] = {
                            "objective_label": str(
                                objective_manual.get("objective_label")
                                or objective_manual.get("objective_id")
                                or ""
                            ),
                            "objective_id": str(objective_manual.get("objective_id") or ""),
                            "activity_label": str(
                                activity_manual.get("activity_label")
                                or activity_manual.get("activity_id")
                                or ""
                            ),
                            "activity_id": str(activity_manual.get("activity_id") or ""),
                        }
                        st.rerun()
    if not isinstance(selected_cell, dict):
        st.info("Click a matrix cell to view exercise-level metrics for that activity.")
        return

    c1, c2 = st.columns([4, 1])
    c1.caption(
        f"Exercise metrics for activity: **{selected_cell.get('activity_label') or selected_cell.get('activity_id')}**"
    )
    if c2.button("Clear selection", key="clear_matrix_cell_selection"):
        st.session_state.pop(selected_cell_key, None)
        st.rerun()

    try:
        drilldown = build_exercise_drilldown_frame(
            agg_exercise_daily=exercise_daily,
            module_code=selected_module_code,
            objective_id=str(selected_cell.get("objective_id") or ""),
            activity_id=str(selected_cell.get("activity_id") or ""),
            start_date=start_date,
            end_date=end_date,
            metric=metric,
            fact_attempt_core=(pl.scan_parquet(fact_path) if has_playlist_unique_metric else None),
        )
    except ValueError as err:
        st.error(str(err))
        st.code("uv run python scripts/build_derived.py --strict-checks")
        return

    if drilldown.height == 0:
        st.info("No exercise-level rows for the selected activity in this date range.")
        return

    drilldown_table = drilldown.select(
        [
            "exercise_short_id",
            "exercise_id",
            "exercise_label",
            "exercise_type",
            "attempts",
            "success_rate",
            "first_attempt_success_rate",
            "repeat_attempt_rate",
            "median_duration",
        ]
    )
    drilldown_table_event = st.dataframe(
        drilldown_table.select(
            [
                "exercise_short_id",
                "exercise_type",
                "attempts",
                "success_rate",
                "first_attempt_success_rate",
                "repeat_attempt_rate",
                "median_duration",
            ]
        ).to_pandas(),
        width='stretch',
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=(
            "exercise_drilldown_table::"
            f"{selected_module_code}::{selected_cell.get('objective_id')}::"
            f"{selected_cell.get('activity_id')}::{start_date}::{end_date}"
        ),
    )

    selected_row_idx: int | None = None
    if isinstance(drilldown_table_event, dict):
        selection = drilldown_table_event.get("selection")
        if isinstance(selection, dict):
            rows = selection.get("rows")
            if isinstance(rows, list) and rows:
                first_row = rows[0]
                if isinstance(first_row, int):
                    selected_row_idx = first_row

    if selected_row_idx is None:
        st.caption("Click a row in the exercise table to show the instruction text.")
        return

    table_rows = drilldown_table.to_dicts()
    if selected_row_idx < 0 or selected_row_idx >= len(table_rows):
        st.caption("Selected row is no longer available for current filters.")
        return

    selected_exercise = table_rows[selected_row_idx]
    selected_exercise_short = str(selected_exercise.get("exercise_short_id") or "")
    selected_instruction = str(selected_exercise.get("exercise_label") or "").strip()
    placeholder_image_path = ROOT_DIR / "images" / "placeholder_exo.png"

    st.markdown(f"**Instruction - Exercise `{selected_exercise_short}`**")
    if selected_instruction:
        st.write(selected_instruction)
    else:
        st.caption("No instruction text available for this exercise.")

    st.markdown("**Exercise Screenshot (Placeholder)**")
    if placeholder_image_path.exists():
        st.image(str(placeholder_image_path), width='stretch')
    else:
        st.caption("Placeholder image not found: `images/placeholder_exo.png`")


if __name__ == "__main__":
    main()

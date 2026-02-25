from __future__ import annotations

import math
import sys
from datetime import date
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from visu2.config import get_settings
from visu2.zpdes_dependencies import (
    attach_overlay_metrics_to_nodes,
    build_dependency_tables_from_metadata,
    filter_dependency_graph_by_objectives,
    list_supported_module_codes_from_metadata,
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


OVERLAY_OPTIONS = {
    "Structure only": None,
    "Attempts": "overlay_attempts",
    "Success rate": "overlay_success_rate",
    "Repeat attempt rate": "overlay_repeat_attempt_rate",
}


def _truncate(text: str, max_chars: int = 44) -> str:
    out = str(text or "").strip()
    if len(out) <= max_chars:
        return out
    return f"{out[: max_chars - 1].rstrip()}..."


def _label_or_id(label: str | None, identifier: str | None) -> str:
    label_txt = str(label or "").strip()
    if label_txt:
        return label_txt
    return str(identifier or "")


def _objective_sort_key(code: str | None) -> tuple[int, int, str]:
    text = str(code or "")
    if text.startswith("M") and "O" in text:
        try:
            module_num = int(text.split("O")[0][1:])
            objective_num = int(text.split("O")[1].split("A")[0])
            return (0, module_num * 1000 + objective_num, text)
        except ValueError:
            return (1, 10**9, text)
    return (1, 10**9, text)


def _fmt_metric(metric_name: str, value: float | None) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float) and math.isnan(value):
        return "n/a"
    if metric_name == "overlay_attempts":
        return f"{int(round(float(value))):,}"
    return f"{float(value):.1%}"


def _fmt_threshold(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.0%}"


def _collect_transitive_incoming_activation(
    edges: pl.DataFrame,
    seed_targets: set[str],
    objective_by_node: dict[str, str] | None = None,
) -> pl.DataFrame:
    if edges.height == 0 or not seed_targets:
        return pl.DataFrame(
            {
                "edge_type": [],
                "from_node_code": [],
                "to_node_code": [],
                "threshold_value": [],
                "source_enrichment": [],
                "enrich_sr": [],
                "enrich_lvl": [],
                "dependency_depth": [],
            }
        )
    rows = edges.filter(pl.col("edge_type") == "activation").to_dicts()
    incoming: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        to_code = str(row.get("to_node_code") or "").strip()
        if not to_code:
            continue
        incoming.setdefault(to_code, []).append(row)

    queue: list[tuple[str, int]] = [(target, 0) for target in sorted(seed_targets)]
    visited_targets: set[str] = set()
    seen_edges: set[str] = set()
    out: list[dict[str, object]] = []

    while queue:
        target, depth = queue.pop(0)
        if target in visited_targets:
            continue
        visited_targets.add(target)
        for edge in incoming.get(target, []):
            from_code = str(edge.get("from_node_code") or "").strip()
            edge_key = str(edge.get("edge_id") or f"{from_code}->{target}")
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)
            row = dict(edge)
            row["dependency_depth"] = depth + 1
            out.append(row)
            if from_code and from_code not in visited_targets:
                queue.append((from_code, depth + 1))
            if objective_by_node is not None:
                parent_objective = str(objective_by_node.get(from_code, "") or "").strip()
                if parent_objective and parent_objective != from_code and parent_objective not in visited_targets:
                    queue.append((parent_objective, depth + 1))

    if not out:
        return pl.DataFrame(
            {
                "edge_type": [],
                "from_node_code": [],
                "to_node_code": [],
                "threshold_value": [],
                "source_enrichment": [],
                "enrich_sr": [],
                "enrich_lvl": [],
                "dependency_depth": [],
            }
        )

    return (
        pl.DataFrame(out)
        .with_columns(pl.col("dependency_depth").cast(pl.Int64))
        .sort(["dependency_depth", "to_node_code", "from_node_code"])
    )


def _extract_selection(event: object) -> dict[str, str] | None:
    if not isinstance(event, dict):
        return None
    selection = event.get("selection")
    if not isinstance(selection, dict):
        return None
    points = selection.get("points")
    if not isinstance(points, list) or not points:
        return None
    point = points[0]
    if not isinstance(point, dict):
        return None
    custom = point.get("customdata")
    if not isinstance(custom, (list, tuple)) or len(custom) < 2:
        return None
    item_type = str(custom[0] or "").strip()
    item_id = str(custom[1] or "").strip()
    if item_type == "node" and item_id:
        return {"item_type": item_type, "item_id": item_id}
    return None


def _polyline_length(points: list[tuple[float, float]]) -> float:
    total = 0.0
    for idx in range(len(points) - 1):
        x0, y0 = points[idx]
        x1, y1 = points[idx + 1]
        total += math.hypot(x1 - x0, y1 - y0)
    return total


def _point_on_polyline(points: list[tuple[float, float]], distance: float) -> tuple[float, float]:
    if not points:
        return (0.0, 0.0)
    if len(points) == 1:
        return points[0]
    target = max(0.0, float(distance))
    traversed = 0.0
    for idx in range(len(points) - 1):
        x0, y0 = points[idx]
        x1, y1 = points[idx + 1]
        seg = math.hypot(x1 - x0, y1 - y0)
        if seg <= 1e-12:
            continue
        if traversed + seg >= target:
            ratio = (target - traversed) / seg
            return (x0 + ratio * (x1 - x0), y0 + ratio * (y1 - y0))
        traversed += seg
    return points[-1]


def _trim_polyline(points: list[tuple[float, float]], keep_length: float) -> list[tuple[float, float]]:
    if not points:
        return []
    if len(points) == 1:
        return points[:]
    target = max(0.0, float(keep_length))
    traversed = 0.0
    out: list[tuple[float, float]] = [points[0]]
    for idx in range(len(points) - 1):
        x0, y0 = points[idx]
        x1, y1 = points[idx + 1]
        seg = math.hypot(x1 - x0, y1 - y0)
        if seg <= 1e-12:
            continue
        if traversed + seg < target:
            out.append((x1, y1))
            traversed += seg
            continue
        ratio = (target - traversed) / seg
        ratio = max(0.0, min(1.0, ratio))
        out.append((x0 + ratio * (x1 - x0), y0 + ratio * (y1 - y0)))
        return out
    return points[:]


def _quadratic_curve_points(
    start: tuple[float, float],
    control: tuple[float, float],
    end: tuple[float, float],
    steps: int,
) -> list[tuple[float, float]]:
    n = max(8, int(steps))
    out: list[tuple[float, float]] = []
    x0, y0 = start
    cx, cy = control
    x1, y1 = end
    for i in range(n + 1):
        t = i / n
        mt = 1.0 - t
        x = (mt * mt * x0) + (2.0 * mt * t * cx) + (t * t * x1)
        y = (mt * mt * y0) + (2.0 * mt * t * cy) + (t * t * y1)
        out.append((x, y))
    return out


def _build_graph(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    overlay_metric_col: str | None,
    overlay_display: str,
    show_ids: bool,
    curve_intra_objective_edges: bool,
    focus_node_code: str | None = None,
) -> go.Figure:
    if nodes.height == 0:
        return go.Figure()

    nodes_rows = nodes.to_dicts()
    edges_rows = edges.to_dicts()

    objective_codes = sorted(
        {str(r.get("objective_code") or "") for r in nodes_rows if str(r.get("objective_code") or "").strip()},
        key=_objective_sort_key,
    )
    if not objective_codes:
        objective_codes = ["(no objective lane)"]
    lane_pos = {code: idx for idx, code in enumerate(objective_codes)}

    max_activity_idx = max(
        [int(r["activity_index"]) for r in nodes_rows if r.get("activity_index") is not None] + [1]
    )

    node_pos: dict[str, tuple[float, float]] = {}
    for row in nodes_rows:
        node_code = str(row.get("node_code") or "")
        node_type = str(row.get("node_type") or "")
        objective_code = str(row.get("objective_code") or "") or objective_codes[0]
        y = float(-lane_pos.get(objective_code, len(objective_codes)))
        if node_type == "objective":
            x = 0.0
        else:
            idx = row.get("activity_index")
            x = float(idx if isinstance(idx, int) and idx > 0 else 1)
        node_pos[node_code] = (x, y)

    same_lane_edge_rank: dict[float, int] = {}

    fig = go.Figure()
    activation_legend_added = False
    deactivation_legend_added = False
    intra_legend_added = False
    node_clearance = 0.16
    arrow_shaft = 0.22

    focus_code = str(focus_node_code or "").strip()
    node_meta_by_code = {str(r.get("node_code") or ""): r for r in nodes_rows}
    valid_node_codes = set(node_meta_by_code.keys())
    has_focus = focus_code in valid_node_codes
    related_node_codes: set[str] = set(valid_node_codes) if not has_focus else {focus_code}
    related_edge_keys: set[str] = set()
    if has_focus:
        focus_row = node_meta_by_code.get(focus_code, {})
        ancestor_seeds = {focus_code}
        if str(focus_row.get("node_type") or "") == "activity":
            objective_code = str(focus_row.get("objective_code") or "").strip()
            if objective_code:
                ancestor_seeds.add(objective_code)

        incoming: dict[str, list[tuple[str, str]]] = {}
        outgoing: dict[str, list[tuple[str, str]]] = {}
        for edge in edges_rows:
            edge_key = str(
                edge.get("edge_id")
                or f"{edge.get('from_node_code')}->{edge.get('to_node_code')}:{edge.get('edge_type')}"
            )
            from_code = str(edge.get("from_node_code") or "").strip()
            to_code = str(edge.get("to_node_code") or "").strip()
            if not from_code or not to_code:
                continue
            incoming.setdefault(to_code, []).append((edge_key, from_code))
            outgoing.setdefault(from_code, []).append((edge_key, to_code))

        # Ancestor traversal (only prerequisite direction into the selected unit).
        ancestor_nodes = set(ancestor_seeds)
        queue = list(ancestor_seeds)
        while queue:
            node_code = queue.pop(0)
            for edge_key, parent in incoming.get(node_code, []):
                related_edge_keys.add(edge_key)
                if parent not in ancestor_nodes:
                    ancestor_nodes.add(parent)
                    queue.append(parent)
                parent_obj = str(node_meta_by_code.get(parent, {}).get("objective_code") or "").strip()
                if parent_obj and parent_obj not in ancestor_nodes:
                    ancestor_nodes.add(parent_obj)
                    queue.append(parent_obj)

        # Descendant traversal (only units that depend on the selected unit).
        descendant_nodes = {focus_code}
        queue = [focus_code]
        while queue:
            node_code = queue.pop(0)
            for edge_key, child in outgoing.get(node_code, []):
                related_edge_keys.add(edge_key)
                if child not in descendant_nodes:
                    descendant_nodes.add(child)
                    queue.append(child)

        related_node_codes = ancestor_nodes | descendant_nodes

    for edge in edges_rows:
        from_code = str(edge.get("from_node_code") or "")
        to_code = str(edge.get("to_node_code") or "")
        if from_code not in node_pos or to_code not in node_pos:
            continue
        edge_key = str(
            edge.get("edge_id")
            or f"{edge.get('from_node_code')}->{edge.get('to_node_code')}:{edge.get('edge_type')}"
        )
        is_related = (not has_focus) or (edge_key in related_edge_keys)
        x0, y0 = node_pos[from_code]
        x1, y1 = node_pos[to_code]
        edge_type = str(edge.get("edge_type") or "activation")

        is_same_lane = abs(y0 - y1) < 1e-9
        use_curve = curve_intra_objective_edges and is_same_lane and abs(x1 - x0) > 0.45
        if use_curve:
            # Alternate arc direction within each objective lane.
            rank_key = round(y0, 2)
            rank = same_lane_edge_rank.get(rank_key, 0)
            same_lane_edge_rank[rank_key] = rank + 1
            tier = rank // 2
            curve_height = 0.28 + 0.06 * max(0, abs(x1 - x0) - 1) + 0.10 * tier
            curve_sign = 1.0 if rank % 2 == 0 else -1.0
            mx = (x0 + x1) / 2.0
            my = (y0 + y1) / 2.0 + curve_sign * curve_height
            path_points = _quadratic_curve_points(
                (x0, y0), (mx, my), (x1, y1), steps=int(max(12, abs(x1 - x0) * 14))
            )
        else:
            path_points = [(x0, y0), (x1, y1)]

        total_len = _polyline_length(path_points)
        if total_len <= 1e-9:
            continue
        draw_len = max(0.0, total_len - node_clearance)
        if draw_len <= 1e-9:
            continue
        draw_points = _trim_polyline(path_points, draw_len)
        head_x, head_y = draw_points[-1]
        if len(draw_points) >= 2:
            prev_x, prev_y = draw_points[-2]
        elif len(path_points) >= 2:
            prev_x, prev_y = path_points[-2]
        else:
            prev_x, prev_y = head_x - 1.0, head_y
        seg_dx = head_x - prev_x
        seg_dy = head_y - prev_y
        seg_len = math.hypot(seg_dx, seg_dy)
        if seg_len <= 1e-9:
            ux, uy = 1.0, 0.0
        else:
            ux, uy = seg_dx / seg_len, seg_dy / seg_len

        if use_curve:
            line_cfg = {
                "color": "#1f7a4f",
                "width": 2.1,
                "dash": "dash" if edge_type == "deactivation" else "solid",
            }
            show_legend = not intra_legend_added
            intra_legend_added = True
            trace_name = "Intra-objective dependency"
            arrow_color = "#1f7a4f"
        else:
            if edge_type == "deactivation":
                line_cfg = {"color": "#b44545", "width": 2.0, "dash": "dash"}
                show_legend = not deactivation_legend_added
                deactivation_legend_added = True
                trace_name = "Deactivation dependency"
                arrow_color = "#b44545"
            else:
                line_cfg = {"color": "#3f5aa8", "width": 2.2}
                show_legend = not activation_legend_added
                activation_legend_added = True
                trace_name = "Activation dependency"
                arrow_color = "#3f5aa8"

        fig.add_trace(
            go.Scatter(
                x=[p[0] for p in draw_points],
                y=[p[1] for p in draw_points],
                mode="lines",
                line=line_cfg,
                hoverinfo="skip",
                name=trace_name,
                showlegend=show_legend,
                opacity=1.0 if is_related else 0.20,
            )
        )

        # Draw arrow head as two short segments to avoid annotation shaft artifacts.
        head_back = arrow_shaft * 0.55
        head_span = head_back * 0.6
        px, py = -uy, ux
        left_x = head_x - ux * head_back + px * head_span
        left_y = head_y - uy * head_back + py * head_span
        right_x = head_x - ux * head_back - px * head_span
        right_y = head_y - uy * head_back - py * head_span
        fig.add_trace(
            go.Scatter(
                x=[left_x, head_x, right_x],
                y=[left_y, head_y, right_y],
                mode="lines",
                line={"color": arrow_color, "width": 1.7},
                hoverinfo="skip",
                showlegend=False,
                opacity=1.0 if is_related else 0.20,
            )
        )

    node_df = pl.DataFrame(nodes_rows).with_columns(
        pl.col("node_code").cast(pl.Utf8),
        pl.col("label").cast(pl.Utf8),
        pl.col("node_type").cast(pl.Utf8),
        pl.col("objective_code").cast(pl.Utf8),
        pl.when(pl.col("is_ghost")).then(pl.lit("Ghost")).otherwise(pl.col("node_type")).alias("legend_type"),
    )
    node_df = node_df.with_columns(
        pl.col("node_code")
        .map_elements(lambda c: node_pos.get(str(c), (0.0, 0.0))[0], return_dtype=pl.Float64)
        .alias("x"),
        pl.col("node_code")
        .map_elements(lambda c: node_pos.get(str(c), (0.0, 0.0))[1], return_dtype=pl.Float64)
        .alias("y"),
        pl.col("node_code")
        .cast(pl.Utf8)
        .is_in(list(related_node_codes))
        .alias("is_related"),
    )
    node_rows = node_df.to_dicts()

    node_hover = (
        "<b>%{customdata[3]}</b><br>"
        + "Type: %{customdata[2]}<br>"
        + ("ID: %{customdata[4]}<br>" if show_ids else "")
        + "Code: %{customdata[1]}<br>"
        + "Objective lane: %{customdata[5]}<br>"
        + "Init open: %{customdata[6]}<br>"
        + "Ghost node: %{customdata[7]}<br>"
        + "Attempts: %{customdata[8]}<br>"
        + "Success rate: %{customdata[9]}<br>"
        + "Repeat attempt rate: %{customdata[10]}"
        + "<extra></extra>"
    )

    colorscale = (
        [[0.0, "#eff3f8"], [1.0, "#2148a4"]]
        if overlay_metric_col == "overlay_attempts"
        else (
            [[0.0, "#edf7ec"], [1.0, "#1f7a4f"]]
            if overlay_metric_col == "overlay_success_rate"
            else (
                [[0.0, "#fff4de"], [1.0, "#d4483b"]]
                if overlay_metric_col == "overlay_repeat_attempt_rate"
                else None
            )
        )
    )
    overlay_numeric_vals = [
        float(r.get(overlay_metric_col))
        for r in node_rows
        if overlay_metric_col is not None
        and isinstance(r.get(overlay_metric_col), (int, float))
        and not math.isnan(float(r.get(overlay_metric_col)))
    ]
    cmin = min(overlay_numeric_vals) if overlay_numeric_vals else None
    cmax = max(overlay_numeric_vals) if overlay_numeric_vals else None

    def _add_node_trace(subset_rows: list[dict[str, object]], faded: bool, show_colorbar: bool) -> None:
        if not subset_rows:
            return
        if overlay_metric_col is None:
            marker_color = [
                "#7b7b7b"
                if bool(r.get("is_ghost"))
                else ("#2e5ea7" if str(r.get("node_type")) == "objective" else "#1f7a4f")
                for r in subset_rows
            ]
        else:
            marker_color = [r.get(overlay_metric_col) for r in subset_rows]
        marker_symbols = [
            "diamond-open" if bool(r.get("is_ghost")) else ("square" if str(r.get("node_type")) == "objective" else "circle")
            for r in subset_rows
        ]
        marker_sizes = [18 if str(r.get("node_type")) == "objective" else 14 for r in subset_rows]
        node_customdata = []
        for r in subset_rows:
            node_customdata.append(
                [
                    "node",
                    str(r.get("node_code") or ""),
                    str(r.get("node_type") or ""),
                    str(r.get("label") or ""),
                    str(r.get("node_id") or ""),
                    str(r.get("objective_code") or ""),
                    "yes" if bool(r.get("init_open")) else "no",
                    "yes" if bool(r.get("is_ghost")) else "no",
                    _fmt_metric("overlay_attempts", r.get("overlay_attempts")),
                    _fmt_metric("overlay_success_rate", r.get("overlay_success_rate")),
                    _fmt_metric("overlay_repeat_attempt_rate", r.get("overlay_repeat_attempt_rate")),
                ]
            )
        marker_cfg: dict[str, object] = {
            "size": marker_sizes,
            "symbol": marker_symbols,
            "line": {
                "width": 1.6 if not faded else 1.0,
                "color": "#1b1d22" if not faded else "rgba(27,29,34,0.35)",
            },
            "color": marker_color,
            "showscale": overlay_metric_col is not None and show_colorbar,
            "colorbar": {"title": overlay_display},
        }
        if overlay_metric_col is not None:
            marker_cfg["colorscale"] = colorscale
            if cmin is not None and cmax is not None:
                marker_cfg["cmin"] = cmin
                marker_cfg["cmax"] = cmax
        fig.add_trace(
            go.Scatter(
                x=[r["x"] for r in subset_rows],
                y=[r["y"] for r in subset_rows],
                mode="markers+text",
                text=[
                    _truncate(str(r.get("label") or r.get("node_code") or ""), 36)
                    if str(r.get("node_type")) == "objective"
                    else ""
                    for r in subset_rows
                ],
                textposition="top center",
                textfont={"size": 11, "color": "rgba(35,40,50,0.40)" if faded else "rgba(35,40,50,0.95)"},
                customdata=node_customdata,
                hovertemplate=node_hover,
                name="Nodes",
                showlegend=False,
                marker=marker_cfg,
                opacity=0.26 if faded else 1.0,
            )
        )

    related_rows = [r for r in node_rows if bool(r.get("is_related"))]
    faded_rows = [r for r in node_rows if not bool(r.get("is_related"))]
    if has_focus:
        _add_node_trace(faded_rows, faded=True, show_colorbar=False)
    _add_node_trace(related_rows, faded=False, show_colorbar=True)

    fig.update_layout(
        height=max(520, 80 * len(objective_codes) + 140),
        margin={"l": 130, "r": 40, "t": 30, "b": 60},
        xaxis_title="Activity position within objective lane",
        yaxis_title="Objective lanes",
        clickmode="event+select",
        dragmode=False,
        plot_bgcolor="rgba(255,255,255,0.65)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(40,40,40,0.10)",
        zeroline=False,
        tickmode="linear",
        dtick=1,
        range=[-0.6, float(max_activity_idx) + 0.8],
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(40,40,40,0.10)",
        zeroline=False,
        tickvals=[-lane_pos[c] for c in objective_codes],
        ticktext=objective_codes,
    )
    return fig


@st.cache_data(show_spinner=False)
def load_activity_daily(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_dependency_tables(
    module_code: str,
    learning_catalog_path: Path,
    zpdes_rules_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    return build_dependency_tables_from_metadata(
        module_code=module_code,
        learning_catalog_path=learning_catalog_path,
        zpdes_rules_path=zpdes_rules_path,
    )


def main() -> None:
    settings = get_settings()
    activity_path = settings.artifacts_derived_dir / "agg_activity_daily.parquet"
    if not activity_path.exists():
        st.error("Missing derived artifact: agg_activity_daily.parquet")
        st.code("uv run python scripts/build_derived.py --strict-checks")
        st.stop()

    activity = load_activity_daily(activity_path)
    required_cols = {
        "date_utc",
        "module_code",
        "objective_id",
        "activity_id",
        "attempts",
        "success_rate",
        "repeat_attempt_rate",
    }
    missing_cols = sorted(required_cols - set(activity.columns))
    if missing_cols:
        st.error("Dependency view cannot run: missing required columns in agg_activity_daily.")
        st.markdown("- " + "\n- ".join(f"`{c}`" for c in missing_cols))
        st.stop()

    observed_modules = {
        str(code)
        for code in activity.select(pl.col("module_code").drop_nulls().unique())["module_code"].to_list()
        if str(code).strip()
    }
    module_codes = list_supported_module_codes_from_metadata(
        settings.learning_catalog_path,
        settings.zpdes_rules_path,
        observed_module_codes=observed_modules,
    )
    if not module_codes:
        st.error("No modules available for ZPDES dependency visualization.")
        st.stop()

    min_date = activity["date_utc"].min()
    max_date = activity["date_utc"].max()
    if min_date is None or max_date is None:
        st.info("No data available in `agg_activity_daily`.")
        st.stop()

    st.title("ZPDES Dependency Graph")
    st.caption(
        "Objective lanes with activity-level dependency edges. Solid blue = activation, dashed red = deactivation."
    )
    st.caption(
        "Structure is sourced from `learning_catalog.json` + `zpdes_rules.json` (or `dependency_topology` when present)."
    )
    st.caption(
        "Unlock conditions apply to the source node of each edge: if the source is an activity code, "
        "the rule targets activity-level mastery; if the source is an objective code, it targets objective-level mastery."
    )

    st.sidebar.header("ZPDES Controls")
    selected_module = st.sidebar.selectbox("Module", module_codes, index=0)
    start_date, end_date = st.sidebar.date_input(
        "Date range (UTC)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("Please select a valid date range.")
        st.stop()

    overlay_name = st.sidebar.selectbox("Node overlay", list(OVERLAY_OPTIONS.keys()), index=0)
    overlay_col = OVERLAY_OPTIONS[overlay_name]
    curve_intra_objective_edges = bool(
        st.sidebar.checkbox("Curve intra-objective edges", value=True)
    )
    show_ids = bool(st.sidebar.checkbox("Show IDs in hover", value=False))
    show_debug = bool(st.sidebar.checkbox("Debug graph selection", value=False))

    nodes, edges, warnings = load_dependency_tables(
        selected_module, settings.learning_catalog_path, settings.zpdes_rules_path
    )
    if nodes.height == 0:
        st.warning("No dependency nodes found for selected module.")
        if warnings:
            st.info("\n".join(f"- {w}" for w in warnings))
        st.stop()

    objective_options = sorted(
        {
            str(code)
            for code in nodes.select(pl.col("objective_code").drop_nulls().unique())["objective_code"].to_list()
            if str(code).strip()
        },
        key=_objective_sort_key,
    )
    selected_objectives = st.sidebar.multiselect(
        "Objectives in module",
        options=objective_options,
        default=objective_options,
    )
    if not selected_objectives:
        st.info("Select at least one objective to render the dependency graph.")
        st.stop()

    nodes_with_metrics = attach_overlay_metrics_to_nodes(
        nodes=nodes,
        agg_activity_daily=activity,
        module_code=selected_module,
        start_date=start_date,
        end_date=end_date,
    )
    filtered_nodes, filtered_edges = filter_dependency_graph_by_objectives(
        nodes=nodes_with_metrics,
        edges=edges,
        objective_codes=selected_objectives,
    )
    if filtered_nodes.height == 0:
        st.info("No nodes available for selected objectives.")
        st.stop()

    if warnings:
        st.warning("\n".join(f"- {w}" for w in warnings))

    ghost_count = int(filtered_nodes.filter(pl.col("is_ghost") == True).height)  # noqa: E712
    if ghost_count > 0:
        st.info(
            f"{ghost_count} ghost node(s) were created for unresolved references. "
            "They are shown with a diamond outline."
        )

    context_key = "zpdes_graph_context"
    selection_key = "zpdes_graph_selection"
    current_context = {
        "module": selected_module,
        "start": str(start_date),
        "end": str(end_date),
        "overlay": overlay_name,
        "objectives": ",".join(sorted(selected_objectives)),
    }
    if st.session_state.get(context_key) != current_context:
        st.session_state[context_key] = current_context
        st.session_state.pop(selection_key, None)

    selected_item = st.session_state.get(selection_key)
    focus_node_code = None
    if isinstance(selected_item, dict) and selected_item.get("item_type") == "node":
        focus_node_code = str(selected_item.get("item_id") or "").strip() or None

    fig = _build_graph(
        nodes=filtered_nodes,
        edges=filtered_edges,
        overlay_metric_col=overlay_col,
        overlay_display=overlay_name,
        show_ids=show_ids,
        curve_intra_objective_edges=curve_intra_objective_edges,
        focus_node_code=focus_node_code,
    )

    event = st.plotly_chart(
        fig,
        width='stretch',
        key="zpdes_dependency_graph",
        on_select="rerun",
        selection_mode=("points",),
        config={"modeBarButtonsToRemove": ["select2d", "lasso2d"]},
    )
    selected = _extract_selection(event)
    if selected and selected != st.session_state.get(selection_key):
        st.session_state[selection_key] = selected
        st.rerun()
    if show_debug:
        st.sidebar.write(event)

    st.markdown("**Legend**")
    st.markdown(
        "- Node shapes: square objective, circle activity, diamond-outline ghost reference.\n"
        "- Between-objective edges: solid blue activation, dashed red deactivation.\n"
        "- Intra-objective edges: curved green (dashed if deactivation), alternating above/below for readability.\n"
        "- Click a node to focus its dependency neighborhood (unrelated nodes/edges fade).\n"
        "- `Init open = yes` means activity/objective is available at start (`init_open = Y`).\n"
        f"- Overlay: **{overlay_name}** (toggle in sidebar)."
    )

    st.subheader("Rule Detail")
    selected_item = st.session_state.get(selection_key)
    if not isinstance(selected_item, dict):
        st.info("Click a node in the graph to inspect dependency rules for that unit.")
    else:
        item_type = selected_item.get("item_type")
        item_id = selected_item.get("item_id")
        if item_type == "node":
            node_row = filtered_nodes.filter(pl.col("node_code") == str(item_id))
            if node_row.height == 0:
                st.info("Selected node is not available in current view.")
            else:
                row = node_row.to_dicts()[0]
                st.markdown(f"**Node:** {row.get('label')} (`{row.get('node_code')}`)")
                if st.button("Clear focus", key="zpdes_clear_focus"):
                    st.session_state.pop(selection_key, None)
                    st.rerun()
                c1, c2 = st.columns(2)
                c1.metric("Type", str(row.get("node_type")))
                c2.metric("Init open", "yes" if bool(row.get("init_open")) else "no")
                st.caption(
                    f"Attempts: {_fmt_metric('overlay_attempts', row.get('overlay_attempts'))} | "
                    f"Success rate: {_fmt_metric('overlay_success_rate', row.get('overlay_success_rate'))} | "
                    f"Repeat attempt rate: {_fmt_metric('overlay_repeat_attempt_rate', row.get('overlay_repeat_attempt_rate'))}"
                )
                detail_nodes = nodes_with_metrics
                detail_edges = edges
                node_label_map = {
                    str(nr.get("node_code") or ""): _label_or_id(
                        str(nr.get("label") or ""),
                        str(nr.get("node_code") or ""),
                    )
                    for nr in detail_nodes.to_dicts()
                }
                objective_by_node = {
                    str(nr.get("node_code") or ""): str(nr.get("objective_code") or "")
                    for nr in detail_nodes.to_dicts()
                }
                seed_targets = {str(item_id)}
                if str(row.get("node_type") or "") == "activity":
                    objective_code = str(row.get("objective_code") or "").strip()
                    if objective_code:
                        seed_targets.add(objective_code)
                incoming = _collect_transitive_incoming_activation(
                    detail_edges,
                    seed_targets,
                    objective_by_node=objective_by_node,
                )
                outgoing = detail_edges.filter(pl.col("from_node_code") == str(item_id))
                st.markdown("**Dependencies required to unlock this unit (incoming)**")
                if incoming.height == 0:
                    st.info("No activation prerequisite chain found for this unit in the current objective scope.")
                else:
                    incoming_view = incoming.with_columns(
                        pl.col("from_node_code")
                        .map_elements(lambda v: node_label_map.get(str(v), str(v)), return_dtype=pl.Utf8)
                        .alias("from_label"),
                        pl.col("to_node_code")
                        .map_elements(lambda v: node_label_map.get(str(v), str(v)), return_dtype=pl.Utf8)
                        .alias("to_label"),
                    ).select(
                        [
                            "dependency_depth",
                            "from_label",
                            "from_node_code",
                            "to_label",
                            "to_node_code",
                            "threshold_value",
                            "source_enrichment",
                            "enrich_sr",
                            "enrich_lvl",
                        ]
                    )
                    st.dataframe(incoming_view.to_pandas(), width='stretch', hide_index=True)
                st.markdown("**Units unlocked by this unit (outgoing)**")
                if outgoing.height == 0:
                    st.info("No direct outgoing dependency from this unit in the current objective scope.")
                else:
                    outgoing_view = outgoing.with_columns(
                        pl.col("to_node_code")
                        .map_elements(lambda v: node_label_map.get(str(v), str(v)), return_dtype=pl.Utf8)
                        .alias("to_label")
                    ).select(
                        [
                            "edge_type",
                            "to_label",
                            "to_node_code",
                            "threshold_value",
                            "source_enrichment",
                            "enrich_sr",
                            "enrich_lvl",
                        ]
                    )
                    st.dataframe(outgoing_view.to_pandas(), width='stretch', hide_index=True)
        else:
            st.info("Click a node in the graph to inspect dependency rules for that unit.")

    with st.expander("Dependency Audit Table", expanded=False):
        node_label_map = {
            str(row.get("node_code") or ""): _label_or_id(row.get("label"), row.get("node_code"))
            for row in filtered_nodes.to_dicts()
        }
        edge_table = filtered_edges.with_columns(
            pl.col("from_node_code")
            .map_elements(lambda v: node_label_map.get(str(v), str(v)), return_dtype=pl.Utf8)
            .alias("from_label"),
            pl.col("to_node_code")
            .map_elements(lambda v: node_label_map.get(str(v), str(v)), return_dtype=pl.Utf8)
            .alias("to_label"),
        ).select(
            [
                "edge_type",
                "from_label",
                "from_node_code",
                "to_label",
                "to_node_code",
                "threshold_value",
                "source_primary",
                "source_enrichment",
                "enrich_sr",
                "enrich_lvl",
            ]
        )
        st.dataframe(edge_table.to_pandas(), width='stretch', hide_index=True)


if __name__ == "__main__":
    main()

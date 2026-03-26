"""Generate a reproducible Markdown audit for Rasch / IRT feasibility."""

from __future__ import annotations

import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from .config import Settings, ensure_artifact_directories
from .loaders import (
    catalog_to_summary_frames,
    load_exercises,
    load_learning_catalog,
    load_zpdes_rules,
)
from .reporting import write_json_report

DECI_PROBS = tuple(i / 10 for i in range(11))
SUCCESS_BAND = (0.65, 0.85)


@dataclass(frozen=True)
class IRTFeasibilityOutputs:
    """Output paths written by the IRT feasibility report build."""

    markdown_path: Path
    summary_path: Path
    exercise_sparsity_path: Path
    overlap_tails_path: Path


class UnionFind:
    """Small union-find helper for connected-component summaries."""

    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, node: int) -> int:
        """Find the representative for one node."""
        parent = self.parent[node]
        if parent != node:
            self.parent[node] = self.find(parent)
        return self.parent[node]

    def union(self, left: int, right: int) -> None:
        """Union two components."""
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1


def _utc_now() -> str:
    """Return the current UTC timestamp as an ISO string."""
    return datetime.now(UTC).isoformat()


def _empty_numeric_distribution() -> dict[str, float | int | None]:
    """Return an empty numeric distribution payload."""
    payload: dict[str, float | int | None] = {
        "count": 0,
        "mean": None,
        "std": None,
    }
    for probability in DECI_PROBS:
        label = "min" if probability == 0 else "max" if probability == 1 else f"p{int(probability * 100):02d}"
        payload[label] = None
    payload["median"] = None
    return payload


def _numeric_distribution(values: pl.Series) -> dict[str, float | int | None]:
    """Build min/decile/median/max summaries for one numeric series."""
    clean = values.drop_nulls()
    if clean.len() == 0:
        return _empty_numeric_distribution()

    payload: dict[str, float | int | None] = {
        "count": int(clean.len()),
        "mean": float(clean.mean()),
        "std": float(clean.std()) if clean.len() > 1 else 0.0,
    }
    for probability in DECI_PROBS:
        value = clean.quantile(probability, interpolation="nearest")
        if value is None:
            cast_value: float | int | None = None
        elif clean.dtype.is_integer():
            cast_value = int(value)
        else:
            cast_value = float(value)
        if probability == 0:
            payload["min"] = cast_value
        elif probability == 1:
            payload["max"] = cast_value
        else:
            payload[f"p{int(probability * 100):02d}"] = cast_value
    payload["median"] = payload.get("p50")
    return payload


def _distribution_table_row(label: str, distribution: dict[str, Any]) -> dict[str, str]:
    """Format one distribution summary row for Markdown rendering."""
    return {
        "Metric": label,
        "Min": _format_number(distribution.get("min")),
        "P10": _format_number(distribution.get("p10")),
        "Median": _format_number(distribution.get("median")),
        "P90": _format_number(distribution.get("p90")),
        "Max": _format_number(distribution.get("max")),
        "Mean": _format_number(distribution.get("mean")),
    }


def _format_number(value: Any, digits: int = 2) -> str:
    """Format numeric values for Markdown."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "-"
        if abs(value - round(value)) < 1e-9:
            return f"{int(round(value)):,}"
        return f"{value:,.{digits}f}"
    return str(value)


def _format_percent(value: Any, digits: int = 1) -> str:
    """Format a ratio as a percentage string."""
    if value is None:
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def _markdown_table(rows: list[dict[str, Any]], headers: list[str]) -> str:
    """Render a compact Markdown table from row dictionaries."""
    if not rows:
        return "_No rows to report._"
    header_row = "| " + " | ".join(headers) + " |"
    divider_row = "| " + " | ".join("---" for _ in headers) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(str(row.get(header, "-")) for header in headers) + " |")
    return "\n".join([header_row, divider_row, *body])


def _load_attempts(parquet_path: Path) -> pl.DataFrame:
    """Load the minimum attempt columns needed by the report."""
    return (
        pl.scan_parquet(parquet_path)
        .select(
            [
                "user_id",
                "exercise_id",
                "work_mode",
                "created_at",
                "data_correct",
                "attempt_number",
                "student_attempt_index",
            ]
        )
        .filter(
            pl.col("user_id").is_not_null()
            & (pl.col("user_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("exercise_id").is_not_null()
            & (pl.col("exercise_id").cast(pl.Utf8).str.strip_chars() != "")
            & pl.col("created_at").is_not_null()
            & pl.col("data_correct").is_not_null()
            & pl.col("attempt_number").is_not_null()
            & pl.col("student_attempt_index").is_not_null()
        )
        .collect(engine="streaming")
    )


def _build_catalog_frames(
    settings: Settings,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Load canonical metadata and return reusable hierarchy frames."""
    catalog = load_learning_catalog(settings.learning_catalog_path)
    rules = load_zpdes_rules(settings.zpdes_rules_path)
    exercises = load_exercises(settings.exercises_json_path)
    frames = catalog_to_summary_frames(catalog)
    exercise_hierarchy = frames.exercise_hierarchy.select(
        [
            "exercise_id",
            "module_id",
            "module_code",
            "module_label",
            "objective_id",
            "objective_code",
            "objective_label",
            "activity_id",
            "activity_code",
            "activity_label",
        ]
    ).unique(subset=["exercise_id"], keep="first")
    activity_hierarchy = frames.activity_hierarchy.select(
        [
            "module_id",
            "module_code",
            "module_label",
            "objective_id",
            "objective_code",
            "objective_label",
            "activity_id",
            "activity_code",
            "activity_label",
        ]
    ).unique()
    return exercise_hierarchy, activity_hierarchy, catalog, rules, exercises


def _build_first_exposure_rows(attempts: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build global and ZPDES-only first exposure tables."""
    attempt_starts = attempts.filter(pl.col("attempt_number") == 1).sort(
        ["user_id", "exercise_id", "created_at", "work_mode"]
    )
    global_first = attempt_starts.unique(subset=["user_id", "exercise_id"], keep="first")
    zpdes_first = (
        attempt_starts.filter(pl.col("work_mode") == "zpdes")
        .sort(["user_id", "exercise_id", "created_at"])
        .unique(subset=["user_id", "exercise_id"], keep="first")
    )
    return global_first, zpdes_first


def _build_retry_enriched_rows(attempts: pl.DataFrame) -> pl.DataFrame:
    """Attach exposure segmentation for retry summaries."""
    ordered = attempts.sort(["user_id", "exercise_id", "created_at", "attempt_number"])
    return (
        ordered.with_columns(
            pl.col("attempt_number").shift(1).over(["user_id", "exercise_id"]).alias("prev_attempt_number")
        )
        .with_columns(
            pl.when(
                pl.col("prev_attempt_number").is_null() | (pl.col("attempt_number") <= pl.col("prev_attempt_number"))
            )
            .then(1)
            .otherwise(0)
            .cast(pl.Int64)
            .alias("is_exposure_start")
        )
        .with_columns(
            (pl.col("is_exposure_start").cum_sum().over(["user_id", "exercise_id"]) - 1).alias("exposure_index")
        )
    )


def _build_zpdes_progress_rows(zpdes_first: pl.DataFrame, exercise_hierarchy: pl.DataFrame) -> pl.DataFrame:
    """Attach canonical hierarchy plus within-student ZPDES progress quantiles."""
    return (
        zpdes_first.join(exercise_hierarchy, on="exercise_id", how="left")
        .sort(["user_id", "created_at", "exercise_id"])
        .with_columns(pl.len().over("user_id").alias("user_zpdes_first_exposure_count"))
        .with_columns(pl.cum_count("exercise_id").over("user_id").alias("user_zpdes_exposure_ordinal"))
        .with_columns(
            (
                pl.col("user_zpdes_exposure_ordinal").cast(pl.Float64)
                / pl.col("user_zpdes_first_exposure_count").cast(pl.Float64)
            ).alias("user_zpdes_progress_quantile")
        )
    )


def _component_sizes(adjacency: dict[str, set[str]]) -> list[int]:
    """Return component sizes for an undirected adjacency mapping."""
    visited: set[str] = set()
    sizes: list[int] = []
    for node in adjacency:
        if node in visited:
            continue
        queue = deque([node])
        visited.add(node)
        size = 0
        while queue:
            current = queue.popleft()
            size += 1
            for neighbor in adjacency.get(current, set()):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                queue.append(neighbor)
        sizes.append(size)
    return sorted(sizes, reverse=True)


def _is_directed_acyclic(adjacency: dict[str, set[str]]) -> bool:
    """Return whether a directed adjacency list is acyclic."""
    indegree = {node: 0 for node in adjacency}
    for targets in adjacency.values():
        for target in targets:
            indegree[target] = indegree.get(target, 0) + 1
    queue = deque([node for node, degree in indegree.items() if degree == 0])
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for neighbor in adjacency.get(node, set()):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)
    return visited == len(indegree)


def _classify_topology(module_entry: dict[str, Any]) -> dict[str, Any]:
    """Classify one canonical dependency topology snapshot."""
    nodes = module_entry.get("nodes") if isinstance(module_entry, dict) else []
    edges = module_entry.get("edges") if isinstance(module_entry, dict) else []
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []
    node_codes = [
        str(node.get("node_code")).strip()
        for node in nodes
        if isinstance(node, dict) and str(node.get("node_code")).strip()
    ]
    unique_node_codes = sorted(set(node_codes))
    adjacency: dict[str, set[str]] = {code: set() for code in unique_node_codes}
    reverse: dict[str, set[str]] = {code: set() for code in unique_node_codes}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = str(edge.get("from_node_code") or "").strip()
        target = str(edge.get("to_node_code") or "").strip()
        if not source or not target:
            continue
        adjacency.setdefault(source, set()).add(target)
        adjacency.setdefault(target, set())
        reverse.setdefault(target, set()).add(source)
        reverse.setdefault(source, set())

    undirected: dict[str, set[str]] = {code: set() for code in adjacency}
    for source, targets in adjacency.items():
        for target in targets:
            undirected[source].add(target)
            undirected[target].add(source)

    component_sizes = _component_sizes(undirected)
    is_connected = len(component_sizes) <= 1
    is_acyclic = _is_directed_acyclic(adjacency)
    max_in_degree = max((len(reverse.get(code, set())) for code in adjacency), default=0)
    max_out_degree = max((len(adjacency.get(code, set())) for code in adjacency), default=0)
    root_count = sum(1 for code in adjacency if len(reverse.get(code, set())) == 0)

    if not is_acyclic:
        structure = "cyclic"
    elif not is_connected:
        structure = "disconnected_dag"
    elif max_in_degree <= 1 and max_out_degree <= 1:
        structure = "linear"
    elif max_in_degree <= 1 and root_count == 1:
        structure = "tree"
    else:
        structure = "general_dag"

    return {
        "node_count": len(adjacency),
        "edge_count": sum(len(targets) for targets in adjacency.values()),
        "weak_component_count": len(component_sizes),
        "largest_component_size": max(component_sizes, default=0),
        "structure": structure,
        "is_acyclic": is_acyclic,
        "max_in_degree": max_in_degree,
        "max_out_degree": max_out_degree,
    }


def _projected_graph_components(exercises: list[str], adjacency: dict[str, set[str]]) -> dict[str, int]:
    """Assign connected-component ids on an undirected exercise graph."""
    component_ids: dict[str, int] = {}
    component_index = 0
    for exercise_id in exercises:
        if exercise_id in component_ids:
            continue
        queue = deque([exercise_id])
        component_ids[exercise_id] = component_index
        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, set()):
                if neighbor in component_ids:
                    continue
                component_ids[neighbor] = component_index
                queue.append(neighbor)
        component_index += 1
    return component_ids


def _two_core_nodes(exercises: list[str], adjacency: dict[str, set[str]]) -> set[str]:
    """Return the node set that survives 2-core peeling."""
    degrees = {exercise_id: len(adjacency.get(exercise_id, set())) for exercise_id in exercises}
    queue = deque([exercise_id for exercise_id, degree in degrees.items() if degree < 2])
    removed: set[str] = set()
    while queue:
        node = queue.popleft()
        if node in removed:
            continue
        removed.add(node)
        for neighbor in adjacency.get(node, set()):
            if neighbor in removed:
                continue
            degrees[neighbor] -= 1
            if degrees[neighbor] == 1:
                queue.append(neighbor)
    return {exercise_id for exercise_id in exercises if exercise_id not in removed}


def _articulation_points(exercises: list[str], adjacency: dict[str, set[str]]) -> set[str]:
    """Return articulation points for an undirected graph."""
    discovery: dict[str, int] = {}
    low: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    articulation: set[str] = set()
    time = 0

    def visit(node: str) -> None:
        nonlocal time
        time += 1
        discovery[node] = time
        low[node] = time
        child_count = 0
        for neighbor in adjacency.get(node, set()):
            if neighbor not in discovery:
                parent[neighbor] = node
                child_count += 1
                visit(neighbor)
                low[node] = min(low[node], low[neighbor])
                if parent.get(node) is None and child_count > 1:
                    articulation.add(node)
                if parent.get(node) is not None and low[neighbor] >= discovery[node]:
                    articulation.add(node)
            elif neighbor != parent.get(node):
                low[node] = min(low[node], discovery[neighbor])

    for exercise_id in exercises:
        if exercise_id in discovery:
            continue
        parent[exercise_id] = None
        visit(exercise_id)
    return articulation


def _bipartite_component_summary(zpdes_first: pl.DataFrame) -> dict[str, Any]:
    """Compute global connected-component summaries on the observed ZPDES bipartite graph."""
    students = sorted(set(zpdes_first["user_id"].to_list()))
    exercises = sorted(set(zpdes_first["exercise_id"].to_list()))
    student_index = {user_id: idx for idx, user_id in enumerate(students)}
    exercise_index = {exercise_id: idx + len(students) for idx, exercise_id in enumerate(exercises)}
    union_find = UnionFind(len(students) + len(exercises))

    for user_id, exercise_id in zpdes_first.select(["user_id", "exercise_id"]).iter_rows():
        union_find.union(student_index[str(user_id)], exercise_index[str(exercise_id)])

    component_students: dict[int, int] = defaultdict(int)
    component_exercises: dict[int, int] = defaultdict(int)
    for user_id in students:
        component_students[union_find.find(student_index[user_id])] += 1
    for exercise_id in exercises:
        component_exercises[union_find.find(exercise_index[exercise_id])] += 1

    ordered_components = sorted(
        (
            {
                "root": root,
                "student_count": component_students.get(root, 0),
                "exercise_count": component_exercises.get(root, 0),
            }
            for root in set(component_students) | set(component_exercises)
        ),
        key=lambda row: (row["student_count"] + row["exercise_count"], row["exercise_count"]),
        reverse=True,
    )
    exercise_component_share = 0.0
    student_component_share = 0.0
    if ordered_components and exercises:
        exercise_component_share = ordered_components[0]["exercise_count"] / len(exercises)
    if ordered_components and students:
        student_component_share = ordered_components[0]["student_count"] / len(students)
    return {
        "component_count": len(ordered_components),
        "largest_component_exercise_share": exercise_component_share,
        "largest_component_student_share": student_component_share,
        "components": ordered_components[:10],
    }


def _distribution_from_histogram(histogram: Counter[int]) -> dict[str, float | int | None]:
    """Build a numeric distribution from a histogram of integer counts."""
    if not histogram:
        return _empty_numeric_distribution()
    ordered = sorted(histogram.items())
    total = sum(count for _, count in ordered)
    mean = sum(value * count for value, count in ordered) / total
    variance = sum(((value - mean) ** 2) * count for value, count in ordered) / total

    def quantile(probability: float) -> int:
        target = max(1, int(math.ceil(probability * total)))
        cumulative = 0
        for value, count in ordered:
            cumulative += count
            if cumulative >= target:
                return value
        return ordered[-1][0]

    payload: dict[str, float | int | None] = {
        "count": total,
        "mean": mean,
        "std": math.sqrt(variance),
        "min": ordered[0][0],
        "max": ordered[-1][0],
    }
    for probability in DECI_PROBS[1:-1]:
        payload[f"p{int(probability * 100):02d}"] = quantile(probability)
    payload["median"] = payload.get("p50")
    return payload


def _component_ids_from_matrix(adjacency: list[bytearray]) -> list[int]:
    """Return connected-component ids for a compact adjacency matrix."""
    node_count = len(adjacency)
    component_ids = [-1] * node_count
    current_component = 0
    for start in range(node_count):
        if component_ids[start] != -1:
            continue
        queue = deque([start])
        component_ids[start] = current_component
        while queue:
            node = queue.popleft()
            row = adjacency[node]
            for neighbor, linked in enumerate(row):
                if not linked or component_ids[neighbor] != -1:
                    continue
                component_ids[neighbor] = current_component
                queue.append(neighbor)
        current_component += 1
    return component_ids


def _two_core_from_matrix(adjacency: list[bytearray], degrees: list[int]) -> set[int]:
    """Return the node ids that survive 2-core peeling on an adjacency matrix."""
    working_degree = list(degrees)
    removed = [False] * len(adjacency)
    queue = deque(index for index, degree in enumerate(working_degree) if degree < 2)
    while queue:
        node = queue.popleft()
        if removed[node]:
            continue
        removed[node] = True
        row = adjacency[node]
        for neighbor, linked in enumerate(row):
            if not linked or removed[neighbor]:
                continue
            working_degree[neighbor] -= 1
            if working_degree[neighbor] == 1:
                queue.append(neighbor)
    return {index for index, was_removed in enumerate(removed) if not was_removed}


def _articulation_from_matrix(adjacency: list[bytearray]) -> set[int]:
    """Return articulation points for an adjacency matrix."""
    node_count = len(adjacency)
    discovery = [-1] * node_count
    low = [-1] * node_count
    parent = [-1] * node_count
    articulation: set[int] = set()
    time = 0

    def visit(node: int) -> None:
        nonlocal time
        time += 1
        discovery[node] = time
        low[node] = time
        child_count = 0
        for neighbor, linked in enumerate(adjacency[node]):
            if not linked:
                continue
            if discovery[neighbor] == -1:
                parent[neighbor] = node
                child_count += 1
                visit(neighbor)
                low[node] = min(low[node], low[neighbor])
                if parent[node] == -1 and child_count > 1:
                    articulation.add(node)
                if parent[node] != -1 and low[neighbor] >= discovery[node]:
                    articulation.add(node)
            elif neighbor != parent[node]:
                low[node] = min(low[node], discovery[neighbor])

    for node in range(node_count):
        if discovery[node] == -1:
            visit(node)
    return articulation


def _topology_summary(rules: dict[str, Any]) -> dict[str, Any]:
    """Summarize canonical dependency topology by module."""
    topology = rules.get("dependency_topology")
    if not isinstance(topology, dict):
        return {"modules": {}, "module_table": []}
    module_rows = []
    modules: dict[str, Any] = {}
    for module_code in sorted(topology):
        summary = _classify_topology(topology[module_code] if isinstance(topology[module_code], dict) else {})
        modules[module_code] = summary
        module_rows.append(
            {
                "Module": module_code,
                "Structure": summary["structure"],
                "Nodes": _format_number(summary["node_count"]),
                "Edges": _format_number(summary["edge_count"]),
                "Weak components": _format_number(summary["weak_component_count"]),
                "Max in-degree": _format_number(summary["max_in_degree"]),
                "Max out-degree": _format_number(summary["max_out_degree"]),
            }
        )
    return {"modules": modules, "module_table": module_rows}


def _coverage_summary(
    zpdes_first: pl.DataFrame,
    exercise_hierarchy: pl.DataFrame,
    activity_hierarchy: pl.DataFrame,
) -> dict[str, Any]:
    """Summarize empirical ZPDES path coverage against the canonical catalog."""
    mappable = zpdes_first.join(exercise_hierarchy, on="exercise_id", how="left")
    module_catalog_exercises = (
        exercise_hierarchy.group_by("module_code").agg(pl.col("exercise_id").n_unique().alias("catalog_exercise_count"))
    )
    module_catalog_activities = (
        activity_hierarchy.group_by("module_code").agg(pl.col("activity_id").n_unique().alias("catalog_activity_count"))
    )
    student_modules = (
        mappable.filter(pl.col("module_code").is_not_null())
        .group_by("user_id")
        .agg(pl.col("module_code").unique().alias("entered_modules"))
    )

    module_exercise_lookup = {
        str(row["module_code"]): int(row["catalog_exercise_count"])
        for row in module_catalog_exercises.to_dicts()
    }
    module_activity_lookup = {
        str(row["module_code"]): int(row["catalog_activity_count"])
        for row in module_catalog_activities.to_dicts()
    }

    student_coverage = (
        mappable.filter(pl.col("module_code").is_not_null())
        .group_by("user_id")
        .agg(
            pl.col("exercise_id").n_unique().alias("seen_exercises"),
            pl.col("activity_id").n_unique().alias("seen_activities"),
        )
        .join(student_modules, on="user_id", how="left")
    )
    if student_coverage.height == 0:
        return {
            "student_count": 0,
            "mean_seen_exercises": None,
            "mean_seen_activities": None,
            "mean_exercise_coverage_full_catalog": None,
            "mean_exercise_coverage_entered_modules": None,
            "mean_activity_coverage_full_catalog": None,
            "mean_activity_coverage_entered_modules": None,
            "unmapped_first_exposure_count": int(zpdes_first.height),
            "observed_catalog_exercises": 0,
        }

    entered_module_exercise_counts: list[int] = []
    entered_module_activity_counts: list[int] = []
    for modules in student_coverage["entered_modules"].to_list():
        codes = [str(code) for code in (modules or []) if str(code)]
        entered_module_exercise_counts.append(sum(module_exercise_lookup.get(code, 0) for code in codes))
        entered_module_activity_counts.append(sum(module_activity_lookup.get(code, 0) for code in codes))

    student_coverage = student_coverage.with_columns(
        [
            pl.Series("entered_module_exercise_count", entered_module_exercise_counts),
            pl.Series("entered_module_activity_count", entered_module_activity_counts),
        ]
    ).with_columns(
        [
            (pl.col("seen_exercises") / pl.lit(float(exercise_hierarchy["exercise_id"].n_unique()))).alias(
                "exercise_coverage_full_catalog"
            ),
            pl.when(pl.col("entered_module_exercise_count") > 0)
            .then(pl.col("seen_exercises") / pl.col("entered_module_exercise_count"))
            .otherwise(None)
            .alias("exercise_coverage_entered_modules"),
            (pl.col("seen_activities") / pl.lit(float(activity_hierarchy["activity_id"].n_unique()))).alias(
                "activity_coverage_full_catalog"
            ),
            pl.when(pl.col("entered_module_activity_count") > 0)
            .then(pl.col("seen_activities") / pl.col("entered_module_activity_count"))
            .otherwise(None)
            .alias("activity_coverage_entered_modules"),
        ]
    )

    mapped_count = int(mappable.filter(pl.col("module_code").is_not_null()).height)
    return {
        "student_count": int(student_coverage.height),
        "mean_seen_exercises": float(student_coverage["seen_exercises"].mean()),
        "mean_seen_activities": float(student_coverage["seen_activities"].mean()),
        "mean_exercise_coverage_full_catalog": float(student_coverage["exercise_coverage_full_catalog"].mean()),
        "mean_exercise_coverage_entered_modules": float(
            student_coverage["exercise_coverage_entered_modules"].drop_nulls().mean()
        ),
        "mean_activity_coverage_full_catalog": float(student_coverage["activity_coverage_full_catalog"].mean()),
        "mean_activity_coverage_entered_modules": float(
            student_coverage["activity_coverage_entered_modules"].drop_nulls().mean()
        ),
        "unmapped_first_exposure_count": int(zpdes_first.height - mapped_count),
        "observed_catalog_exercises": int(
            mappable.filter(pl.col("module_code").is_not_null())["exercise_id"].n_unique()
        ),
    }


def _module_overlap_analytics(
    zpdes_first: pl.DataFrame,
    exercise_hierarchy: pl.DataFrame,
    scratch_dir: Path | None = None,
) -> tuple[dict[str, Any], pl.DataFrame]:
    """Compute module-scoped overlap distributions and tail diagnostics."""
    mappable = (
        zpdes_first.join(exercise_hierarchy, on="exercise_id", how="inner")
        .select(
            [
                "user_id",
                "exercise_id",
                "module_code",
                "module_label",
                "objective_code",
                "activity_code",
                "user_zpdes_progress_quantile",
            ]
        )
        .unique(subset=["user_id", "exercise_id"], keep="first")
    )
    exposure_summary = (
        mappable.group_by(["module_code", "module_label", "exercise_id", "objective_code", "activity_code"])
        .agg(
            pl.len().alias("first_attempt_students"),
            pl.col("user_zpdes_progress_quantile").median().alias("median_progress_quantile"),
            pl.col("user_zpdes_progress_quantile").quantile(0.1, interpolation="nearest").alias(
                "p10_progress_quantile"
            ),
            pl.col("user_zpdes_progress_quantile").quantile(0.9, interpolation="nearest").alias(
                "p90_progress_quantile"
            ),
        )
        .sort(["module_code", "exercise_id"])
    )

    scratch_dir = Path("artifacts/.duckdb_tmp") if scratch_dir is None else scratch_dir
    scratch_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    try:
        connection.execute(f"PRAGMA temp_directory='{scratch_dir.as_posix()}'")
        connection.execute("PRAGMA memory_limit='512MB'")
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")

        module_summaries: dict[str, Any] = {}
        tail_rows: list[dict[str, Any]] = []
        module_lookup = {
            row["module_code"]: row["module_label"]
            for row in exposure_summary.select(["module_code", "module_label"]).unique().to_dicts()
        }
        exposure_by_module: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in exposure_summary.to_dicts():
            exposure_by_module[str(row["module_code"])].append(row)

        for module_code in sorted(exposure_by_module):
            module_rows = mappable.filter(pl.col("module_code") == module_code).select(["user_id", "exercise_id"])
            exercises = sorted(row["exercise_id"] for row in exposure_by_module[module_code])
            exercise_index = {exercise_id: idx for idx, exercise_id in enumerate(exercises)}
            adjacency = [bytearray(len(exercises)) for _ in exercises]
            degrees = [0] * len(exercises)
            overlap_hist: Counter[int] = Counter()
            weight_hists: list[Counter[int]] = [Counter() for _ in exercises]

            connection.register("module_rows", module_rows.to_arrow())
            batch_size = 50 if len(exercises) > 1000 else 100
            for offset in range(0, len(exercises), batch_size):
                batch = pl.DataFrame({"exercise_id": exercises[offset : offset + batch_size]})
                connection.register("batch_exercises", batch.to_arrow())
                pair_query = """
                    SELECT
                        left_rows.exercise_id AS exercise_left,
                        right_rows.exercise_id AS exercise_right,
                        COUNT(*) AS shared_students
                    FROM module_rows AS left_rows
                    JOIN batch_exercises AS batch_rows
                      ON left_rows.exercise_id = batch_rows.exercise_id
                    JOIN module_rows AS right_rows
                      ON left_rows.user_id = right_rows.user_id
                     AND left_rows.exercise_id < right_rows.exercise_id
                    GROUP BY 1, 2
                """
                batch_rows = connection.execute(pair_query).fetchall()
                if not batch_rows:
                    connection.unregister("batch_exercises")
                    continue
                for left, right, shared_students in batch_rows:
                    left_idx = exercise_index[str(left)]
                    right_idx = exercise_index[str(right)]
                    shared = int(shared_students)
                    if adjacency[left_idx][right_idx] == 0:
                        adjacency[left_idx][right_idx] = 1
                        adjacency[right_idx][left_idx] = 1
                        degrees[left_idx] += 1
                        degrees[right_idx] += 1
                    overlap_hist[shared] += 1
                    weight_hists[left_idx][shared] += 1
                    weight_hists[right_idx][shared] += 1
                connection.unregister("batch_exercises")
            connection.unregister("module_rows")

            component_ids = _component_ids_from_matrix(adjacency)
            component_sizes = Counter(component_ids)
            articulation_exact = len(exercises) <= 1000
            articulation = _articulation_from_matrix(adjacency) if articulation_exact else set()
            two_core = _two_core_from_matrix(adjacency, degrees)
            overlap_distribution = _distribution_from_histogram(overlap_hist)
            largest_component_size = max(component_sizes.values(), default=0)
            giant_component_share = (largest_component_size / len(exercises)) if exercises else 0.0
            two_core_share = (len(two_core) / len(exercises)) if exercises else 0.0
            module_summaries[module_code] = {
                "module_label": module_lookup.get(module_code),
                "exercise_count": len(exercises),
                "overlap_pair_count": int(sum(overlap_hist.values())),
                "overlap_distribution": overlap_distribution,
                "projected_component_count": len(component_sizes),
                "projected_giant_component_share": giant_component_share,
                "projected_two_core_share": two_core_share,
                "projected_articulation_count": len(articulation) if articulation_exact else None,
                "projected_articulation_exact": articulation_exact,
            }

            neighbor_counts = degrees
            neighbor_threshold = int(
                pl.Series("neighbor_count", neighbor_counts).quantile(0.1, interpolation="nearest") or 0
            )
            for row in exposure_by_module[module_code]:
                exercise_id = str(row["exercise_id"])
                exercise_idx = exercise_index[exercise_id]
                weight_hist = weight_hists[exercise_idx]
                neighbor_count = degrees[exercise_idx]
                is_tail = (
                    neighbor_count <= neighbor_threshold
                    or component_sizes.get(component_ids[exercise_idx], 0) < largest_component_size
                    or float(row.get("p10_progress_quantile") or 0.0) >= 0.9
                )
                if not is_tail:
                    continue
                neighbor_distribution = _distribution_from_histogram(weight_hist)
                tail_rows.append(
                    {
                        "module_code": module_code,
                        "module_label": module_lookup.get(module_code),
                        "exercise_id": exercise_id,
                        "objective_code": row.get("objective_code"),
                        "activity_code": row.get("activity_code"),
                        "first_attempt_students": int(row.get("first_attempt_students") or 0),
                        "neighbor_exercise_count": neighbor_count,
                        "min_shared_students_to_neighbor": int(neighbor_distribution.get("min") or 0),
                        "median_shared_students_to_neighbor": int(neighbor_distribution.get("median") or 0),
                        "max_shared_students_to_neighbor": int(neighbor_distribution.get("max") or 0),
                        "projected_component_id": component_ids[exercise_idx],
                        "projected_component_exercise_count": component_sizes.get(component_ids[exercise_idx], 0),
                        "projected_is_articulation": exercise_idx in articulation,
                        "projected_in_two_core": exercise_idx in two_core,
                        "median_progress_quantile": float(row.get("median_progress_quantile") or 0.0),
                        "p10_progress_quantile": float(row.get("p10_progress_quantile") or 0.0),
                        "p90_progress_quantile": float(row.get("p90_progress_quantile") or 0.0),
                        "narrow_slice_flag": float(row.get("p10_progress_quantile") or 0.0) >= 0.9,
                    }
                )
    finally:
        connection.close()

    tail_frame = (
        pl.DataFrame(tail_rows)
        if tail_rows
        else pl.DataFrame(
            {
                "module_code": [],
                "module_label": [],
                "exercise_id": [],
                "objective_code": [],
                "activity_code": [],
                "first_attempt_students": [],
                "neighbor_exercise_count": [],
                "min_shared_students_to_neighbor": [],
                "median_shared_students_to_neighbor": [],
                "max_shared_students_to_neighbor": [],
                "projected_component_id": [],
                "projected_component_exercise_count": [],
                "projected_is_articulation": [],
                "projected_in_two_core": [],
                "median_progress_quantile": [],
                "p10_progress_quantile": [],
                "p90_progress_quantile": [],
                "narrow_slice_flag": [],
            }
        )
    )
    if tail_frame.height > 0:
        tail_frame = tail_frame.sort(
            [
                "module_code",
                "neighbor_exercise_count",
                "projected_component_exercise_count",
                "p10_progress_quantile",
                "first_attempt_students",
                "exercise_id",
            ]
        )
    return module_summaries, tail_frame


def _dataset_scale_summary(
    attempts: pl.DataFrame,
    first_global: pl.DataFrame,
    exercise_hierarchy: pl.DataFrame,
) -> tuple[dict[str, Any], pl.DataFrame]:
    """Summarize overall dataset scale and build the exercise sparsity appendix."""
    attempts_per_student = attempts.group_by("user_id").agg(pl.len().alias("attempt_count")).sort("user_id")
    attempts_per_exercise = attempts.group_by("exercise_id").agg(pl.len().alias("attempt_count")).sort("exercise_id")
    first_exposure_per_exercise = (
        first_global.group_by("exercise_id")
        .agg(
            pl.len().alias("first_exposure_count"),
            pl.col("data_correct").cast(pl.Float64).mean().alias("first_exposure_success_rate"),
        )
        .sort("exercise_id")
    )
    exercise_sparsity = (
        attempts_per_exercise.join(first_exposure_per_exercise, on="exercise_id", how="full", coalesce=True)
        .join(exercise_hierarchy, on="exercise_id", how="left")
        .with_columns(
            pl.col("attempt_count").fill_null(0).cast(pl.Int64),
            pl.col("first_exposure_count").fill_null(0).cast(pl.Int64),
        )
        .select(
            [
                "exercise_id",
                "module_code",
                "objective_code",
                "activity_code",
                "attempt_count",
                "first_exposure_count",
                "first_exposure_success_rate",
            ]
        )
        .sort(["first_exposure_count", "attempt_count", "exercise_id"])
    )
    summary = {
        "students": int(attempts["user_id"].n_unique()),
        "attempted_exercises": int(attempts["exercise_id"].n_unique()),
        "canonical_mapped_exercises_attempted": int(
            attempts.join(exercise_hierarchy.select(["exercise_id"]).unique(), on="exercise_id", how="inner")[
                "exercise_id"
            ].n_unique()
        ),
        "total_attempts": int(attempts.height),
        "attempts_per_student_distribution": _numeric_distribution(attempts_per_student["attempt_count"]),
        "attempts_per_exercise_distribution": _numeric_distribution(attempts_per_exercise["attempt_count"]),
        "first_exposures_per_exercise_distribution": _numeric_distribution(exercise_sparsity["first_exposure_count"]),
        "exercises_below_20_first_exposures": int(
            exercise_sparsity.filter(pl.col("first_exposure_count") < 20).height
        ),
    }
    return summary, exercise_sparsity


def _success_rate_summary(zpdes_first: pl.DataFrame) -> dict[str, Any]:
    """Summarize first-exposure success-rate distributions inside ZPDES."""
    exercise_rates = (
        zpdes_first.group_by("exercise_id")
        .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"))
        .sort("exercise_id")
    )
    student_rates = (
        zpdes_first.group_by("user_id")
        .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("success_rate"))
        .sort("user_id")
    )
    lower, upper = SUCCESS_BAND
    return {
        "exercise_distribution": _numeric_distribution(exercise_rates["success_rate"]),
        "student_distribution": _numeric_distribution(student_rates["success_rate"]),
        "exercise_mean": float(exercise_rates["success_rate"].mean()),
        "student_mean": float(student_rates["success_rate"].mean()),
        "exercise_std": float(exercise_rates["success_rate"].std()),
        "student_std": float(student_rates["success_rate"].std()),
        "exercise_band_share_65_85": float(
            exercise_rates.filter(pl.col("success_rate").is_between(lower, upper, closed="both")).height
            / max(exercise_rates.height, 1)
        ),
        "student_band_share_65_85": float(
            student_rates.filter(pl.col("success_rate").is_between(lower, upper, closed="both")).height
            / max(student_rates.height, 1)
        ),
    }


def _early_late_delta_summary(attempts: pl.DataFrame) -> dict[str, Any]:
    """Compare early vs late success within student histories."""
    student_totals = attempts.group_by("user_id").agg(pl.len().alias("total_attempts"))
    enriched = attempts.join(student_totals, on="user_id", how="left").with_columns(
        pl.max_horizontal(pl.lit(1), (pl.col("total_attempts") * 0.2).ceil().cast(pl.Int64)).alias("segment_size")
    )
    early = (
        enriched.filter(pl.col("student_attempt_index") <= pl.col("segment_size"))
        .group_by("user_id")
        .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("early_success_rate"))
    )
    late = (
        enriched.filter(pl.col("student_attempt_index") > (pl.col("total_attempts") - pl.col("segment_size")))
        .group_by("user_id")
        .agg(pl.col("data_correct").cast(pl.Float64).mean().alias("late_success_rate"))
    )
    deltas = (
        student_totals.join(early, on="user_id", how="left")
        .join(late, on="user_id", how="left")
        .with_columns((pl.col("late_success_rate") - pl.col("early_success_rate")).alias("delta"))
    )
    return {
        "student_count": int(deltas.height),
        "delta_distribution": _numeric_distribution(deltas["delta"]),
        "mean_delta": float(deltas["delta"].mean()),
        "median_delta": float(deltas["delta"].median()),
        "share_improved": float(deltas.filter(pl.col("delta") > 0).height / max(deltas.height, 1)),
    }


def _trajectory_growth_summary(attempts: pl.DataFrame, first_global: pl.DataFrame) -> dict[str, Any]:
    """Summarize trajectory length, duration, and early/late improvement pressure."""
    first_last = attempts.group_by("user_id").agg(
        pl.col("created_at").min().alias("first_seen_at"),
        pl.col("created_at").max().alias("last_seen_at"),
        pl.col("exercise_id").n_unique().alias("distinct_exercises"),
        pl.len().alias("attempt_count"),
    )
    first_last = first_last.with_columns(
        ((pl.col("last_seen_at") - pl.col("first_seen_at")).dt.total_days()).alias("elapsed_days")
    )
    first_exposure_only = first_global.sort(["user_id", "student_attempt_index"]).with_columns(
        pl.cum_count("exercise_id").over("user_id").alias("student_first_exposure_index")
    )
    first_exposure_delta = _early_late_delta_summary(
        first_exposure_only.select(
            [
                "user_id",
                "exercise_id",
                "created_at",
                "data_correct",
                "attempt_number",
                "work_mode",
                pl.col("student_first_exposure_index").alias("student_attempt_index"),
            ]
        )
    )
    return {
        "attempts_per_student_distribution": _numeric_distribution(first_last["attempt_count"]),
        "distinct_exercises_per_student_distribution": _numeric_distribution(first_last["distinct_exercises"]),
        "elapsed_days_distribution": _numeric_distribution(first_last["elapsed_days"].fill_null(0)),
        "all_attempts_early_late": _early_late_delta_summary(attempts),
        "first_exposure_early_late": first_exposure_delta,
    }


def _retry_summary(attempts: pl.DataFrame) -> dict[str, Any]:
    """Summarize retry behavior from all attempt rows."""
    enriched = _build_retry_enriched_rows(attempts)
    exposure_summary = (
        enriched.group_by(["user_id", "exercise_id", "exposure_index"])
        .agg(
            pl.col("attempt_number").max().alias("max_attempt_number"),
            pl.col("data_correct").first().alias("first_attempt_correct"),
            pl.len().alias("exposure_attempt_count"),
        )
        .sort(["user_id", "exercise_id", "exposure_index"])
    )
    first_exposure = exposure_summary.filter(pl.col("exposure_index") == 0).with_columns(
        (pl.col("max_attempt_number") - 1).alias("retry_count")
    )
    first_failures = first_exposure.filter(pl.col("first_attempt_correct") == False)  # noqa: E712
    second_attempt_success = enriched.filter(pl.col("attempt_number") == 2)["data_correct"].cast(pl.Float64)
    return {
        "first_exposure_failure_retry_share": float(
            first_failures.filter(pl.col("retry_count") > 0).height / max(first_failures.height, 1)
        ),
        "first_exposure_retry_distribution": _numeric_distribution(first_exposure["retry_count"]),
        "second_attempt_success_rate": float(second_attempt_success.mean()) if second_attempt_success.len() else None,
        "first_attempt_success_rate": float(
            exposure_summary["first_attempt_correct"].cast(pl.Float64).mean()
        )
        if exposure_summary.height
        else None,
        "reused_user_exercise_share": float(
            exposure_summary.group_by(["user_id", "exercise_id"]).len().filter(pl.col("len") > 1).height
            / max(exposure_summary.select(["user_id", "exercise_id"]).unique().height, 1)
        ),
    }


def _metadata_summary(
    catalog: dict[str, Any],
    exercises: dict[str, Any],
    exercise_sparsity: pl.DataFrame,
    retry_summary: dict[str, Any],
) -> dict[str, Any]:
    """Audit available exercise metadata and reuse signals."""
    exercise_records = exercises.get("exercises") if isinstance(exercises.get("exercises"), list) else []
    exercise_types = Counter(
        str(record.get("type") or "").strip()
        for record in exercise_records
        if isinstance(record, dict) and str(record.get("type") or "").strip()
    )
    matching_paths: set[str] = set()

    def walk(value: Any, prefix: str = "") -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_prefix = f"{prefix}.{key}" if prefix else str(key)
                lowered = child_prefix.lower()
                if any(token in lowered for token in ("difficulty", "skill", "topic", "category", "domain", "level")):
                    matching_paths.add(child_prefix)
                walk(child, child_prefix)
        elif isinstance(value, list):
            for child in value[:3]:
                walk(child, f"{prefix}[]")

    for record in exercise_records[:250]:
        if isinstance(record, dict):
            walk(record)

    modules = catalog.get("modules") if isinstance(catalog.get("modules"), list) else []
    return {
        "catalog_modules": len(modules),
        "catalog_exercises": len(catalog.get("exercise_to_hierarchy") or {}),
        "attempted_unmapped_exercises": int(exercise_sparsity.filter(pl.col("module_code").is_null()).height),
        "exercise_types": dict(sorted(exercise_types.items())),
        "has_hierarchy_labels": True,
        "has_expert_difficulty": False,
        "difficulty_like_fields_found": sorted(matching_paths),
        "reused_user_exercise_share": retry_summary["reused_user_exercise_share"],
    }


def _recommendation(summary: dict[str, Any]) -> dict[str, str]:
    """Build a deterministic recommendation paragraph."""
    overlap = summary["overlap_structure"]
    scale = summary["dataset_scale"]
    growth = summary["trajectory_growth"]
    success = summary["success_rate_signal"]

    giant_component = overlap["bipartite_components"]["largest_component_exercise_share"]
    sparse_share = scale["exercises_below_20_first_exposures"] / max(scale["attempted_exercises"], 1)
    growth_delta = abs(growth["all_attempts_early_late"]["mean_delta"])
    exercise_variance = success["exercise_std"]

    if giant_component < 0.9:
        headline = "A single global Rasch scale looks weakly identified."
        detail = (
            "The observed ZPDES graph is not close to one giant connected component, so exercise difficulty is safer "
            "to compare within modules or connected subgraphs than on one global scale."
        )
    elif sparse_share > 0.35:
        headline = "Connectivity is workable, but sparsity is a major constraint."
        detail = (
            "A module-scoped Rasch baseline is still useful, but many exercises have too few first exposures for "
            "stable item estimates, so shrinkage or partial pooling would matter immediately."
        )
    elif growth_delta > 0.05:
        headline = "A Rasch baseline is feasible, but static theta is likely too coarse."
        detail = (
            "The overlap structure is strong enough to fit item difficulty, yet students show meaningful within-history "
            "performance change, which argues for a dynamic ability model after the baseline fit."
        )
    elif exercise_variance < 0.05:
        headline = "The data support a Rasch baseline, but the success-rate signal is intentionally compressed."
        detail = (
            "Because ZPDES keeps students near the target challenge band, item difficulty can still be estimated, "
            "but uncertainty will stay higher than in a freely sampled test bank."
        )
    else:
        headline = "A Rasch baseline looks feasible on the current data."
        detail = (
            "Connectivity and exposure are strong enough for a baseline item-difficulty fit, with dynamic ability "
            "extensions reserved for the next step if growth pressure matters analytically."
        )
    return {"headline": headline, "detail": detail}


def _report_markdown(summary: dict[str, Any]) -> str:
    """Render the final Markdown report."""
    scale = summary["dataset_scale"]
    topology = summary["path_structure"]["canonical_topology"]
    coverage = summary["path_structure"]["empirical_coverage"]
    overlap = summary["overlap_structure"]
    success = summary["success_rate_signal"]
    growth = summary["trajectory_growth"]
    retries = summary["retry_behavior"]
    metadata = summary["exercise_metadata"]
    recommendation = summary["recommendation"]

    sections: list[str] = [
        "# IRT Feasibility Report",
        "",
        f"_Generated at {summary['generated_at_utc']}_",
        "",
        "## Executive Summary",
        "",
        f"- Dataset scale: {scale['students']:,} students, {scale['attempted_exercises']:,} attempted exercises, and {scale['total_attempts']:,} total attempts.",
        f"- ZPDES first-exposure overlap: {overlap['bipartite_components']['component_count']:,} connected component(s), with {_format_percent(overlap['bipartite_components']['largest_component_exercise_share'])} of exercises in the largest one.",
        f"- Sparsity: {scale['exercises_below_20_first_exposures']:,} exercises have fewer than 20 first exposures.",
        f"- Growth pressure: mean early-vs-late success delta on all attempts is {_format_percent(growth['all_attempts_early_late']['mean_delta'])}.",
        f"- Retry signal: {_format_percent(retries['first_exposure_failure_retry_share'])} of first-exposure failures lead to at least one retry.",
        f"- Recommendation: **{recommendation['headline']}** {recommendation['detail']}",
        "",
        "## 1. Data Scale",
        "",
        f"- Students: {scale['students']:,}",
        f"- Attempted exercises in the raw parquet: {scale['attempted_exercises']:,}",
        f"- Canonical mapped exercises attempted: {scale['canonical_mapped_exercises_attempted']:,}",
        f"- Total attempts: {scale['total_attempts']:,}",
        f"- Exercises with fewer than 20 first exposures: {scale['exercises_below_20_first_exposures']:,}",
        "",
        _markdown_table(
            [
                _distribution_table_row("Attempts per student", scale["attempts_per_student_distribution"]),
                _distribution_table_row("Attempts per exercise", scale["attempts_per_exercise_distribution"]),
                _distribution_table_row("First exposures per exercise", scale["first_exposures_per_exercise_distribution"]),
            ],
            headers=["Metric", "Min", "P10", "Median", "P90", "Max", "Mean"],
        ),
        "",
        "## 2. Structure of the Learning Path",
        "",
        f"- Canonical catalog size: {metadata['catalog_exercises']:,} exercises across {metadata['catalog_modules']:,} modules.",
        f"- Mean unique ZPDES exercises seen per student: {_format_number(coverage['mean_seen_exercises'])}",
        f"- Mean unique ZPDES activities seen per student: {_format_number(coverage['mean_seen_activities'])}",
        f"- Mean exercise coverage vs full catalog: {_format_percent(coverage['mean_exercise_coverage_full_catalog'])}",
        f"- Mean exercise coverage vs entered modules only: {_format_percent(coverage['mean_exercise_coverage_entered_modules'])}",
        f"- Mean activity coverage vs full catalog: {_format_percent(coverage['mean_activity_coverage_full_catalog'])}",
        f"- Mean activity coverage vs entered modules only: {_format_percent(coverage['mean_activity_coverage_entered_modules'])}",
        f"- Unmapped ZPDES first exposures: {coverage['unmapped_first_exposure_count']:,}",
        "",
        _markdown_table(
            topology["module_table"],
            headers=["Module", "Structure", "Nodes", "Edges", "Weak components", "Max in-degree", "Max out-degree"],
        ),
        "",
        "## 3. Overlap / Identifiability",
        "",
        f"- Observed ZPDES bipartite graph components: {overlap['bipartite_components']['component_count']:,}",
        f"- Largest-component share of exercises: {_format_percent(overlap['bipartite_components']['largest_component_exercise_share'])}",
        f"- Largest-component share of students: {_format_percent(overlap['bipartite_components']['largest_component_student_share'])}",
        "",
        _markdown_table(
            overlap["module_overlap_table"],
            headers=[
                "Module",
                "Pairs with overlap",
                "Median shared students",
                "P90 shared students",
                "Projected components",
                "Giant component share",
                "2-core share",
                "Articulation points",
            ],
        ),
        "",
        "Articulation counts are reported exactly on smaller projected graphs; for very large dense module projections they are left blank and the giant-component / 2-core summaries remain the primary robustness diagnostics.",
        "",
        "The overlap appendix lists low-neighborhood exercises, exercises outside the giant projected component, and exercises whose exposures concentrate very late in student trajectories.",
        "",
        "## 4. Success-Rate Signal",
        "",
        f"- Per-exercise first-exposure mean success: {_format_percent(success['exercise_mean'])}",
        f"- Per-student first-exposure mean success: {_format_percent(success['student_mean'])}",
        f"- Per-exercise success-rate std: {_format_number(success['exercise_std'])}",
        f"- Per-student success-rate std: {_format_number(success['student_std'])}",
        f"- Exercises inside the 65%-85% band: {_format_percent(success['exercise_band_share_65_85'])}",
        f"- Students inside the 65%-85% band: {_format_percent(success['student_band_share_65_85'])}",
        "",
        _markdown_table(
            [
                _distribution_table_row("Per-exercise ZPDES first-exposure success", success["exercise_distribution"]),
                _distribution_table_row("Per-student ZPDES first-exposure success", success["student_distribution"]),
            ],
            headers=["Metric", "Min", "P10", "Median", "P90", "Max", "Mean"],
        ),
        "",
        "## 5. Trajectory Length and Ability-Growth Pressure",
        "",
        _markdown_table(
            [
                _distribution_table_row("Attempts per student", growth["attempts_per_student_distribution"]),
                _distribution_table_row("Distinct exercises per student", growth["distinct_exercises_per_student_distribution"]),
                _distribution_table_row("Elapsed days from first to last attempt", growth["elapsed_days_distribution"]),
            ],
            headers=["Metric", "Min", "P10", "Median", "P90", "Max", "Mean"],
        ),
        "",
        _markdown_table(
            [
                {
                    "Segment": "All attempts",
                    "Mean delta": _format_percent(growth["all_attempts_early_late"]["mean_delta"]),
                    "Median delta": _format_percent(growth["all_attempts_early_late"]["median_delta"]),
                    "Share improved": _format_percent(growth["all_attempts_early_late"]["share_improved"]),
                },
                {
                    "Segment": "First exposures only",
                    "Mean delta": _format_percent(growth["first_exposure_early_late"]["mean_delta"]),
                    "Median delta": _format_percent(growth["first_exposure_early_late"]["median_delta"]),
                    "Share improved": _format_percent(growth["first_exposure_early_late"]["share_improved"]),
                },
            ],
            headers=["Segment", "Mean delta", "Median delta", "Share improved"],
        ),
        "",
        "## 6. Retry Behavior",
        "",
        f"- First-exposure failures followed by a retry: {_format_percent(retries['first_exposure_failure_retry_share'])}",
        f"- First-attempt success rate across exposure starts: {_format_percent(retries['first_attempt_success_rate'])}",
        f"- Second-attempt success rate: {_format_percent(retries['second_attempt_success_rate'])}",
        f"- User-exercise pairs with multiple exposure starts: {_format_percent(retries['reused_user_exercise_share'])}",
        "",
        _markdown_table(
            [_distribution_table_row("Retries in the first exposure per user-exercise", retries["first_exposure_retry_distribution"])],
            headers=["Metric", "Min", "P10", "Median", "P90", "Max", "Mean"],
        ),
        "",
        "## 7. Exercise Metadata",
        "",
        f"- Canonical hierarchy and labels available: {_format_number(metadata['has_hierarchy_labels'])}",
        f"- Explicit expert difficulty field available: {_format_number(metadata['has_expert_difficulty'])}",
        f"- Attempted exercises absent from the canonical catalog: {metadata['attempted_unmapped_exercises']:,}",
        f"- Difficulty/skill/topic-like fields discovered in `exercises.json`: {', '.join(metadata['difficulty_like_fields_found']) if metadata['difficulty_like_fields_found'] else 'none'}",
        "",
        _markdown_table(
            [{"Exercise type": key, "Count": _format_number(value)} for key, value in sorted(metadata["exercise_types"].items(), key=lambda item: item[1], reverse=True)],
            headers=["Exercise type", "Count"],
        ),
        "",
        "## Conclusion",
        "",
        f"**{recommendation['headline']}**",
        "",
        recommendation["detail"],
        "",
        "Operationally, this means the current data are best treated as support for a Rasch-style baseline only when the overlap graph is sufficiently connected and sparse exercises are handled cautiously. When growth pressure is non-trivial, a dynamic student model should remain the preferred follow-up.",
    ]
    return "\n".join(sections) + "\n"


def build_irt_feasibility_report(
    settings: Settings,
    output_dir: Path | None = None,
) -> IRTFeasibilityOutputs:
    """Build the IRT feasibility report and write all requested outputs."""
    ensure_artifact_directories(settings)
    reports_dir = settings.artifacts_reports_dir if output_dir is None else output_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    exercise_hierarchy, activity_hierarchy, catalog, rules, exercises = _build_catalog_frames(settings)
    attempts = _load_attempts(settings.parquet_path)
    first_global, zpdes_first = _build_first_exposure_rows(attempts)
    zpdes_progress = _build_zpdes_progress_rows(zpdes_first, exercise_hierarchy)

    dataset_scale, exercise_sparsity = _dataset_scale_summary(attempts, first_global, exercise_hierarchy)
    canonical_topology = _topology_summary(rules)
    empirical_coverage = _coverage_summary(zpdes_progress, exercise_hierarchy, activity_hierarchy)
    bipartite_components = _bipartite_component_summary(zpdes_progress)
    module_overlap_summary, overlap_tails = _module_overlap_analytics(
        zpdes_progress,
        exercise_hierarchy,
        scratch_dir=reports_dir / ".duckdb_tmp",
    )
    success_rate_signal = _success_rate_summary(zpdes_progress)
    trajectory_growth = _trajectory_growth_summary(attempts, first_global)
    retry_behavior = _retry_summary(attempts)
    exercise_metadata = _metadata_summary(catalog, exercises, exercise_sparsity, retry_behavior)

    module_overlap_table = []
    for module_code in sorted(module_overlap_summary):
        row = module_overlap_summary[module_code]
        overlap_distribution = row["overlap_distribution"]
        module_overlap_table.append(
            {
                "Module": module_code,
                "Pairs with overlap": _format_number(row["overlap_pair_count"]),
                "Median shared students": _format_number(overlap_distribution.get("median")),
                "P90 shared students": _format_number(overlap_distribution.get("p90")),
                "Projected components": _format_number(row["projected_component_count"]),
                "Giant component share": _format_percent(row["projected_giant_component_share"]),
                "2-core share": _format_percent(row["projected_two_core_share"]),
                "Articulation points": _format_number(row["projected_articulation_count"]),
            }
        )

    summary: dict[str, Any] = {
        "generated_at_utc": _utc_now(),
        "inputs": {
            "parquet_path": str(settings.parquet_path),
            "learning_catalog_path": str(settings.learning_catalog_path),
            "zpdes_rules_path": str(settings.zpdes_rules_path),
            "exercises_json_path": str(settings.exercises_json_path),
        },
        "scope": {
            "dataset_scale": "all_attempts + global first exposures",
            "path_structure": "zpdes first exposures",
            "overlap_structure": "zpdes first exposures",
            "success_rate_signal": "zpdes first exposures",
            "trajectory_growth": "all attempts + global first exposures",
            "retry_behavior": "all attempts",
            "exercise_metadata": "learning_catalog + exercises.json + reuse audit",
        },
        "dataset_scale": dataset_scale,
        "path_structure": {
            "canonical_topology": canonical_topology,
            "empirical_coverage": empirical_coverage,
        },
        "overlap_structure": {
            "bipartite_components": bipartite_components,
            "module_summaries": module_overlap_summary,
            "module_overlap_table": module_overlap_table,
            "tail_row_count": int(overlap_tails.height),
        },
        "success_rate_signal": success_rate_signal,
        "trajectory_growth": trajectory_growth,
        "retry_behavior": retry_behavior,
        "exercise_metadata": exercise_metadata,
    }
    summary["recommendation"] = _recommendation(summary)

    markdown_path = reports_dir / "irt_feasibility_report.md"
    summary_path = reports_dir / "irt_feasibility_summary.json"
    exercise_sparsity_path = reports_dir / "irt_feasibility_exercise_sparsity.csv"
    overlap_tails_path = reports_dir / "irt_feasibility_overlap_tails.csv"

    markdown_path.write_text(_report_markdown(summary), encoding="utf-8")
    write_json_report(summary, summary_path)
    exercise_sparsity.write_csv(exercise_sparsity_path)
    overlap_tails.write_csv(overlap_tails_path)

    return IRTFeasibilityOutputs(
        markdown_path=markdown_path,
        summary_path=summary_path,
        exercise_sparsity_path=exercise_sparsity_path,
        overlap_tails_path=overlap_tails_path,
    )


__all__ = [
    "IRTFeasibilityOutputs",
    "UnionFind",
    "_articulation_points",
    "_classify_topology",
    "_early_late_delta_summary",
    "_two_core_nodes",
    "build_irt_feasibility_report",
]

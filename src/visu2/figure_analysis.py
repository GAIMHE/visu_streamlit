from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from math import ceil, erfc, isnan, sqrt
from statistics import mean
from typing import Any

import pandas as pd
import polars as pl

RATE_MIN_OBSERVATIONS = 20
COHORT_MIN_STUDENTS = 5
TRANSITION_MIN_OBSERVATIONS = 20
DRILLDOWN_MIN_ATTEMPTS = 10
TOP_LIST_LIMIT = 5
INSUFFICIENT_EVIDENCE_MESSAGE = "Not enough evidence for automated analysis in the current filter scope."
LOW_IS_WORSE_RATE_METRICS = {
    "success_rate",
    "exercise_balanced_success_rate",
    "first_attempt_success_rate",
}
HIGH_IS_WORSE_RATE_METRICS = {"repeat_attempt_rate"}


@dataclass(frozen=True, slots=True)
class FigureAnalysis:
    findings: tuple[str, ...] = ()
    interpretation: str | None = None
    discussion: str | None = None
    caveats: tuple[str, ...] = ()


def _insufficient(extra: list[str] | None = None) -> FigureAnalysis:
    caveats = [INSUFFICIENT_EVIDENCE_MESSAGE]
    if extra:
        caveats.extend(extra)
    return FigureAnalysis(caveats=tuple(caveats))


def _as_polars(frame: pl.DataFrame | pd.DataFrame | None) -> pl.DataFrame:
    if frame is None:
        return pl.DataFrame()
    if isinstance(frame, pl.DataFrame):
        return frame.clone()
    if isinstance(frame, pd.DataFrame):
        return pl.from_pandas(frame)
    raise TypeError(f"Unsupported frame type: {type(frame)!r}")


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError:
            return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        number = float(value)
        if isnan(number):
            return None
        return number
    return None


def _format_pct(value: float | None, digits: int = 1) -> str:
    return "n/a" if value is None else f"{value * 100:.{digits}f}%"


def _format_num(value: int | float | None, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.{digits}f}"


def _format_p_value(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value < 0.001:
        return "p<0.001"
    return f"p={value:.3f}"


def _two_proportion_p_value(
    success_a: int | float | None,
    total_a: int | float | None,
    success_b: int | float | None,
    total_b: int | float | None,
) -> float | None:
    sa = _safe_float(success_a)
    ta = _safe_float(total_a)
    sb = _safe_float(success_b)
    tb = _safe_float(total_b)
    if None in {sa, ta, sb, tb}:
        return None
    if ta <= 0 or tb <= 0:
        return None
    pa = sa / ta
    pb = sb / tb
    pooled = (sa + sb) / (ta + tb)
    variance = pooled * (1 - pooled) * ((1 / ta) + (1 / tb))
    if variance <= 0:
        return None
    z = (pa - pb) / sqrt(variance)
    return erfc(abs(z) / sqrt(2))


def _mean_p_value(mean_value: int | float | None, std_value: int | float | None, count: int | float | None) -> float | None:
    mean_float = _safe_float(mean_value)
    std_float = _safe_float(std_value)
    count_float = _safe_float(count)
    if None in {mean_float, std_float, count_float}:
        return None
    if count_float <= 1 or std_float <= 0:
        return None
    standard_error = std_float / sqrt(count_float)
    if standard_error <= 0:
        return None
    z = mean_float / standard_error
    return erfc(abs(z) / sqrt(2))


def _approx_successes(rate: int | float | None, total: int | float | None) -> int | None:
    rate_float = _safe_float(rate)
    total_float = _safe_float(total)
    if rate_float is None or total_float is None or total_float < 0:
        return None
    return int(round(rate_float * total_float))


def _top_rank_text(
    frame: pl.DataFrame,
    *,
    label_fn,
    metric_fn,
    limit: int = TOP_LIST_LIMIT,
) -> str | None:
    if frame.height == 0:
        return None
    rows = frame.head(min(limit, frame.height)).to_dicts()
    parts = [f"{idx}. {label_fn(row)} ({metric_fn(row)})" for idx, row in enumerate(rows, start=1)]
    return "; ".join(parts)


def _normalize_sentence(text: str | None) -> str | None:
    value = str(text or "").strip()
    if not value:
        return None
    if value[-1] not in ".!?":
        value = f"{value}."
    return value


def _sentence_to_clause(text: str) -> str:
    clause = text.strip()
    while clause and clause[-1] in ".!?":
        clause = clause[:-1]
    if clause and clause[0].isupper():
        clause = clause[0].lower() + clause[1:]
    return clause


def build_discussion_paragraph(analysis: FigureAnalysis) -> str | None:
    """Build a short results/discussion-style paragraph from an analysis payload."""
    if analysis.discussion is not None:
        return _normalize_sentence(analysis.discussion)

    interpretation = _normalize_sentence(analysis.interpretation)
    selected_findings = [
        _sentence_to_clause(text)
        for text in analysis.findings[:3]
        if str(text or "").strip()
    ]
    findings_clause = None
    if selected_findings:
        if len(selected_findings) == 1:
            findings_clause = selected_findings[0]
        elif len(selected_findings) == 2:
            findings_clause = f"{selected_findings[0]}; and {selected_findings[1]}"
        else:
            findings_clause = "; ".join(selected_findings[:-1]) + f"; and {selected_findings[-1]}"

    if interpretation and findings_clause:
        body = interpretation[:-1] if interpretation.endswith((".", "!", "?")) else interpretation
        return f"Taken together, these results suggest that {body[0].lower() + body[1:] if body else body}. In practical terms, this pattern is reflected by {findings_clause}."
    if interpretation:
        body = interpretation[:-1] if interpretation.endswith((".", "!", "?")) else interpretation
        return f"Taken together, these results suggest that {body[0].lower() + body[1:] if body else body}."
    if findings_clause:
        return f"Taken together, the current evidence indicates a consistent pattern: {findings_clause}."
    return None


def _label(row: dict[str, Any], objective_key: str = "objective_label", activity_key: str = "activity_label") -> str:
    objective = str(row.get(objective_key) or "").strip()
    activity = str(row.get(activity_key) or row.get("activity_id") or "").strip()
    if objective and activity:
        return f"{objective} -> {activity}"
    return activity or objective or "Unknown"


def _module_activity_label(row: dict[str, Any]) -> str:
    module = str(row.get("module_label") or row.get("module_code") or "").strip()
    inner = _label(row)
    if module and inner and inner != "Unknown":
        return f"{module} -> {inner}"
    return module or inner


def _level_plural(level_label: str) -> str:
    text = str(level_label or "").strip().lower()
    if text == "activity":
        return "activities"
    if text == "objective":
        return "objectives"
    if text == "exercise":
        return "exercises"
    if text == "module":
        return "modules"
    return f"{text}s"


def analyze_overview_kpis(
    *,
    attempts: int,
    unique_students: int,
    unique_exercises: int,
    mean_distinct_exercises_per_student: float | None = None,
    retry_attempt_rate: float | None = None,
    retry_after_success_share: float | None = None,
    retry_after_failure_share: float | None = None,
) -> FigureAnalysis:
    if attempts <= 0 or unique_students <= 0 or unique_exercises <= 0:
        return _insufficient()
    findings: list[str] = []
    if mean_distinct_exercises_per_student is not None:
        findings.append(
            "On average, each student attempts "
            f"{_format_num(mean_distinct_exercises_per_student)} distinct exercises in this slice."
        )
    if retry_attempt_rate is not None:
        findings.append(
            "Across the full source dataset, "
            f"{_format_pct(retry_attempt_rate)} of attempt rows are retries of a previously seen exercise in the same context."
        )
    if retry_after_success_share is not None and retry_after_failure_share is not None:
        findings.append(
            "Among those retries, "
            f"{_format_pct(retry_after_success_share)} follow a first-try success and "
            f"{_format_pct(retry_after_failure_share)} follow a first-try failure."
        )
    return FigureAnalysis(findings=tuple(findings), interpretation=None, discussion="")


def analyze_overview_concentration(
    entity_summary: pl.DataFrame | pd.DataFrame | None,
    bucket_summary: pl.DataFrame | pd.DataFrame | None,
    *,
    level_label: str,
    basis_label: str = "Content concentration",
    student_scope_label: str | None = None,
) -> FigureAnalysis:
    entities = _as_polars(entity_summary)
    buckets = _as_polars(bucket_summary)
    if entities.height == 0 or buckets.height == 0:
        return _insufficient(["Concentration analysis requires at least one visible entity in the selected scope."])
    normalized_basis = str(basis_label or "").strip().lower()
    normalized_student_scope = str(student_scope_label or "").strip().lower()

    if normalized_basis == "student concentration" and normalized_student_scope == "all attempts":
        sorted_buckets = buckets.sort("bucket_order")
        top_10 = float(sorted_buckets.filter(pl.col("bucket_order") == 1)["attempt_share"].sum())
        top_20 = float(sorted_buckets.filter(pl.col("bucket_order") <= 2)["attempt_share"].sum())
        top_50 = float(sorted_buckets.filter(pl.col("bucket_order") <= 5)["attempt_share"].sum())
        bottom_50 = float(sorted_buckets.filter(pl.col("bucket_order") > 5)["attempt_share"].sum())
        findings = [
            f"The top 10% of students contribute {_format_pct(top_10)} of visible attempts.",
            f"The top 20% of students contribute {_format_pct(top_20)} of visible attempts.",
            f"The top 50% of students contribute {_format_pct(top_50)} of visible attempts.",
            f"The bottom 50% of students contribute {_format_pct(bottom_50)} of visible attempts.",
        ]
        student_ranking = _top_rank_text(
            entities.sort(["attempt_share", "attempts", "user_id"], descending=[True, True, False]),
            label_fn=lambda row: str(row["user_id"]),
            metric_fn=lambda row: f"{_format_pct(_safe_float(row['attempt_share']))} of attempts",
        )
        if student_ranking:
            findings.append(f"Most active students: {student_ranking}.")
        if top_10 >= 0.40 or top_20 >= 0.65:
            interpretation = "Attempt volume is concentrated in a relatively small subset of students."
        elif top_50 <= 0.70:
            interpretation = "Attempt volume is broadly shared across students rather than being driven by only a small core group."
        else:
            interpretation = "Attempt volume is moderately concentrated across students, with a visible core group but still substantial activity beyond it."
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, discussion="")

    if normalized_basis == "student concentration":
        sorted_buckets = buckets.sort("bucket_order")
        plural = _level_plural(level_label)
        top_10 = float(sorted_buckets.filter(pl.col("bucket_order") == 1)["attempt_share"].sum())
        top_20 = float(sorted_buckets.filter(pl.col("bucket_order") <= 2)["attempt_share"].sum())
        top_50 = float(sorted_buckets.filter(pl.col("bucket_order") <= 5)["attempt_share"].sum())
        findings = [
            f"Across visible {plural}, the top 10% student bucket contributes {_format_pct(top_10)} of attempts.",
            f"Across visible {plural}, the top 20% student buckets contribute {_format_pct(top_20)} of attempts.",
            f"Across visible {plural}, the top 50% student buckets contribute {_format_pct(top_50)} of attempts.",
        ]
        supported = entities.filter(
            (pl.col("attempts") >= RATE_MIN_OBSERVATIONS)
            & (pl.col("unique_students") >= COHORT_MIN_STUDENTS)
        )
        if supported.height > 0:
            median_top_10 = supported.select(pl.col("top_10_students_share").median()).item()
            over_half_share = float(
                supported.select((pl.col("top_10_students_share") >= 0.5).cast(pl.Float64).mean()).item()
            )
            findings.append(
                f"The median {level_label.lower()} gets {_format_pct(_safe_float(median_top_10))} of its attempts from its own top 10% students."
            )
            findings.append(
                f"{_format_pct(over_half_share)} of supported {plural} get at least half of their attempts from their top 10% students."
            )
            concentrated_text = _top_rank_text(
                supported.sort(
                    ["top_10_students_share", "attempts", "label"],
                    descending=[True, True, False],
                ),
                label_fn=lambda row: str(row["label"]),
                metric_fn=lambda row: f"top-10%-students share {_format_pct(_safe_float(row['top_10_students_share']))}",
            )
            if concentrated_text:
                findings.append(f"Most student-concentrated {plural}: {concentrated_text}.")
            if (median_top_10 or 0.0) >= 0.45 or over_half_share >= 0.40:
                interpretation = f"Many visible {plural} depend strongly on a narrow subset of their own students."
            else:
                interpretation = f"Student effort inside visible {plural} is present in a core group, but the within-{level_label.lower()} workload is not overwhelmingly monopolized by just a few students."
        else:
            interpretation = (
                f"Within-{level_label.lower()} student concentration can be computed, but there are not enough supported {plural} to make strong normalized comparisons."
            )
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, discussion="")

    if str(level_label).lower() == "module":
        ranked_modules = entities.sort(["attempt_share", "attempts", "label"], descending=[True, True, False])
        top_module = ranked_modules.row(0, named=True)
        top_two = float(ranked_modules.head(min(2, ranked_modules.height))["attempt_share"].sum())
        top_three = float(ranked_modules.head(min(3, ranked_modules.height))["attempt_share"].sum())
        findings = [
            f"The largest visible module is {top_module['label']}, covering {_format_pct(_safe_float(top_module['attempt_share']))} of attempts.",
            f"The top 2 modules cover {_format_pct(top_two)} of the visible attempt volume.",
            f"The top 3 modules cover {_format_pct(top_three)} of the visible attempt volume.",
        ]
        ranking_text = _top_rank_text(
            ranked_modules,
            label_fn=lambda row: str(row["label"]),
            metric_fn=lambda row: f"{_format_pct(_safe_float(row['attempt_share']))} of attempts",
        )
        if ranking_text:
            findings.append(f"Module concentration ranking: {ranking_text}.")
        interpretation = (
            "Visible module traffic is steeply concentrated in a few modules."
            if top_two >= 0.70
            else "Visible module traffic is spread across multiple modules rather than dominated by only one or two."
        )
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, discussion="")

    sorted_buckets = buckets.sort("bucket_order")
    plural = _level_plural(level_label)
    top_10 = float(sorted_buckets.filter(pl.col("bucket_order") == 1)["attempt_share"].sum())
    top_20 = float(sorted_buckets.filter(pl.col("bucket_order") <= 2)["attempt_share"].sum())
    top_30 = float(sorted_buckets.filter(pl.col("bucket_order") <= 3)["attempt_share"].sum())
    top_50 = float(sorted_buckets.filter(pl.col("bucket_order") <= 5)["attempt_share"].sum())
    bottom_50 = float(sorted_buckets.filter(pl.col("bucket_order") > 5)["attempt_share"].sum())
    findings = [
        f"At {level_label.lower()} level, the top 10% bucket covers {_format_pct(top_10)} of visible attempts.",
        f"The top 20% of {plural} cover {_format_pct(top_20)} of visible attempts.",
        f"The top 30% of {plural} cover {_format_pct(top_30)} of visible attempts.",
        f"The top 50% of {plural} cover {_format_pct(top_50)} of visible attempts.",
        f"The bottom 50% of {plural} cover {_format_pct(bottom_50)} of visible attempts.",
    ]
    bucket_ranking = _top_rank_text(
        sorted_buckets.sort(["attempt_share", "bucket_order"], descending=[True, False]),
        label_fn=lambda row: str(row["bucket_label"]),
        metric_fn=lambda row: f"{_format_pct(_safe_float(row['attempt_share']))} of attempts",
    )
    if bucket_ranking:
        findings.append(f"Bucket concentration ranking: {bucket_ranking}.")
    top_entities = _top_rank_text(
        entities.sort(["attempt_share", "attempts", "label"], descending=[True, True, False]),
        label_fn=lambda row: str(row["label"]),
        metric_fn=lambda row: f"{_format_pct(_safe_float(row['attempt_share']))} of attempts",
    )
    if top_entities:
        findings.append(f"Most-used {plural}: {top_entities}.")
    if top_10 >= 0.50 or top_20 >= 0.75:
        interpretation = f"Attempt volume is steeply concentrated at the {level_label.lower()} level: a small leading slice carries most of the visible activity."
    elif top_50 <= 0.70:
        interpretation = f"Attempt volume is relatively diffuse at the {level_label.lower()} level: even the top half of entities does not dominate the visible activity."
    else:
        interpretation = f"Attempt volume is moderately concentrated at the {level_label.lower()} level, with a clear leading slice but still meaningful activity in the long tail."
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, discussion="")


def analyze_work_mode_summary(frame: pl.DataFrame | pd.DataFrame | None) -> FigureAnalysis:
    table = _as_polars(frame)
    if table.height == 0:
        return _insufficient()
    supported = table.filter(pl.col("attempts") >= RATE_MIN_OBSERVATIONS).with_columns(
        pl.col("success_rate").cast(pl.Float64),
        pl.col("exercise_balanced_success_rate").cast(pl.Float64),
    )
    if supported.height == 0:
        return _insufficient(["Work-mode comparisons are only surfaced when a mode has at least 20 attempts."])
    dominant = supported.sort(["attempts", "work_mode"], descending=[True, False]).row(0, named=True)
    balanced = supported.filter(pl.col("exercise_balanced_success_rate").is_not_null()).sort(["exercise_balanced_success_rate", "attempts", "work_mode"], descending=[True, True, False])
    gap_rank = supported.with_columns((pl.col("success_rate") - pl.col("exercise_balanced_success_rate")).abs().alias("gap")).sort(["gap", "attempts", "work_mode"], descending=[True, True, False])
    findings = [
        f"{dominant['work_mode']} is the dominant mode in the current slice with {_format_num(dominant['attempts'])} attempts.",
    ]
    if gap_rank.height > 0:
        gap_row = gap_rank.row(0, named=True)
        gap = _safe_float(gap_row['gap']) or 0.0
        findings.append(
            f"The largest difference between attempt-weighted and exercise-balanced success is in {gap_row['work_mode']} ({_format_pct(gap)}). Attempt-weighted success tells us what share of all attempts succeed, so high-traffic exercises matter more; exercise-balanced success gives each exercise the same weight, so it is easier to read as the success level of a typical exercise."
        )
        interpretation = f"{gap_row['work_mode']} looks more uneven than the other visible modes: its weighted and balanced success diverge, which plausibly means a narrower subset of exercises is driving the aggregate result." if gap >= 0.10 else "The visible work modes look fairly consistent across the two success views, so the main differences appear to come from overall performance level rather than extreme exercise skew."
    else:
        interpretation = "The visible work modes can be ranked by success, but the current slice does not provide enough non-null balanced-success rows to discuss skew between weighting schemes."
    if supported.height >= 2:
        weighted_table = supported.sort(["success_rate", "attempts", "work_mode"], descending=[True, True, False])
        pair_text = _top_rank_text(
            weighted_table,
            label_fn=lambda row: str(row["work_mode"]),
            metric_fn=lambda row: f"weighted success {_format_pct(_safe_float(row['success_rate']))} over {_format_num(row['attempts'])} attempts",
        )
        if pair_text:
            findings.append(
                f"Attempt-weighted success ranking, which reflects the success rate over all attempts: {pair_text}."
            )
        if balanced.height > 0:
            balanced_text = _top_rank_text(
                balanced,
                label_fn=lambda row: str(row["work_mode"]),
                metric_fn=lambda row: f"exercise-balanced success {_format_pct(_safe_float(row['exercise_balanced_success_rate']))}",
            )
            if balanced_text:
                findings.append(
                    f"Exercise-balanced success ranking, which gives each exercise the same weight before averaging: {balanced_text}."
                )
        volume_text = _top_rank_text(
            supported.sort(["attempts", "work_mode"], descending=[True, False]),
            label_fn=lambda row: str(row["work_mode"]),
            metric_fn=lambda row: f"{_format_num(row['attempts'])} attempts, {_format_num(row['unique_students'], digits=0)} students",
        )
        if volume_text:
            findings.append(f"Work-mode volume ranking: {volume_text}.")
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, discussion="")


def analyze_work_mode_transitions(paths: pl.DataFrame | pd.DataFrame | None) -> FigureAnalysis:
    table = _as_polars(paths)
    if table.height == 0:
        return _insufficient(["Work-mode transition analysis requires at least one student history."])

    mode_labels = {
        "adaptive-test": "Adaptive-test",
        "initial-test": "Initial-test",
        "playlist": "Playlist",
        "zpdes": "ZPDES",
    }

    def display_mode(value: object) -> str:
        text = str(value or "").strip()
        return mode_labels.get(text, text or "Unknown")

    total_students = table.height
    no_transition_count = int(table.filter(pl.col("transition_count_total") == 0).height)
    no_transition_share = no_transition_count / total_students
    more_than_three_count = int(table.filter(pl.col("continues_after_transition_3")).height)
    more_than_three_share = more_than_three_count / total_students

    first_modes = (
        table.group_by("first_work_mode")
        .agg(pl.len().alias("student_count"))
        .with_columns((pl.col("student_count") / pl.lit(total_students)).alias("student_share"))
        .sort(["student_count", "first_work_mode"], descending=[True, False])
    )
    first_mode_text = _top_rank_text(
        first_modes,
        label_fn=lambda row: display_mode(row["first_work_mode"]),
        metric_fn=lambda row: f"{_format_num(row['student_count'])} students, {_format_pct(_safe_float(row['student_share']))} of all students",
        limit=min(10, first_modes.height),
    )

    first_transition_pairs = (
        table.filter(pl.col("transition_1_mode").is_not_null())
        .group_by(["first_work_mode", "transition_1_mode"])
        .agg(pl.len().alias("student_count"))
        .with_columns((pl.col("student_count") / pl.lit(total_students)).alias("student_share"))
        .sort(["student_count", "first_work_mode", "transition_1_mode"], descending=[True, False, False])
    )
    second_transition_pairs = (
        table.filter(pl.col("transition_2_mode").is_not_null())
        .group_by(["transition_1_mode", "transition_2_mode"])
        .agg(pl.len().alias("student_count"))
        .with_columns((pl.col("student_count") / pl.lit(total_students)).alias("student_share"))
        .sort(["student_count", "transition_1_mode", "transition_2_mode"], descending=[True, False, False])
    )
    third_transition_pairs = (
        table.filter(pl.col("transition_3_mode").is_not_null())
        .group_by(["transition_2_mode", "transition_3_mode"])
        .agg(pl.len().alias("student_count"))
        .with_columns((pl.col("student_count") / pl.lit(total_students)).alias("student_share"))
        .sort(["student_count", "transition_2_mode", "transition_3_mode"], descending=[True, False, False])
    )

    findings = [
        f"Students who never change work mode represent {_format_pct(no_transition_share)} of the full history scope.",
        f"Students who continue changing mode after the displayed third transition represent {_format_pct(more_than_three_share)} of the full history scope.",
    ]
    if first_mode_text:
        findings.append(f"Initial mode distribution: {first_mode_text}.")

    if first_transition_pairs.height > 0 and int(first_transition_pairs.row(0, named=True)["student_count"]) >= TRANSITION_MIN_OBSERVATIONS:
        first_transition_text = _top_rank_text(
            first_transition_pairs,
            label_fn=lambda row: f"{display_mode(row['first_work_mode'])} -> {display_mode(row['transition_1_mode'])}",
            metric_fn=lambda row: f"{_format_num(row['student_count'])} students, {_format_pct(_safe_float(row['student_share']))} of all students",
            limit=min(10, first_transition_pairs.height),
        )
        if first_transition_text:
            findings.append(f"Most common first transitions: {first_transition_text}.")

    if second_transition_pairs.height > 0 and int(second_transition_pairs.row(0, named=True)["student_count"]) >= TRANSITION_MIN_OBSERVATIONS:
        second_transition_text = _top_rank_text(
            second_transition_pairs,
            label_fn=lambda row: f"{display_mode(row['transition_1_mode'])} -> {display_mode(row['transition_2_mode'])}",
            metric_fn=lambda row: f"{_format_num(row['student_count'])} students, {_format_pct(_safe_float(row['student_share']))} of all students",
            limit=min(10, second_transition_pairs.height),
        )
        if second_transition_text:
            findings.append(f"Most common second transitions: {second_transition_text}.")

    if third_transition_pairs.height > 0 and int(third_transition_pairs.row(0, named=True)["student_count"]) >= TRANSITION_MIN_OBSERVATIONS:
        third_transition_text = _top_rank_text(
            third_transition_pairs,
            label_fn=lambda row: f"{display_mode(row['transition_2_mode'])} -> {display_mode(row['transition_3_mode'])}",
            metric_fn=lambda row: f"{_format_num(row['student_count'])} students, {_format_pct(_safe_float(row['student_share']))} of all students",
            limit=min(10, third_transition_pairs.height),
        )
        if third_transition_text:
            findings.append(f"Most common third transitions: {third_transition_text}.")

    displayed_paths = table.with_columns(
        pl.when(pl.col("transition_count_total") == 0)
        .then(pl.concat_str([pl.col("first_work_mode"), pl.lit(" -> No transition")]))
        .when(pl.col("transition_count_total") == 1)
        .then(
            pl.concat_str(
                [
                    pl.col("first_work_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_1_mode"),
                    pl.lit(" -> Stopped after 1"),
                ]
            )
        )
        .when(pl.col("transition_count_total") == 2)
        .then(
            pl.concat_str(
                [
                    pl.col("first_work_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_1_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_2_mode"),
                    pl.lit(" -> Stopped after 2"),
                ]
            )
        )
        .when(pl.col("continues_after_transition_3"))
        .then(
            pl.concat_str(
                [
                    pl.col("first_work_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_1_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_2_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_3_mode"),
                    pl.lit(" -> More than 3 transitions"),
                ]
            )
        )
        .otherwise(
            pl.concat_str(
                [
                    pl.col("first_work_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_1_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_2_mode"),
                    pl.lit(" -> "),
                    pl.col("transition_3_mode"),
                    pl.lit(" -> Stopped after 3"),
                ]
            )
        )
        .alias("displayed_path")
    ).with_columns(
        pl.col("displayed_path")
        .str.replace_all("adaptive-test", "Adaptive-test")
        .str.replace_all("initial-test", "Initial-test")
        .str.replace_all("playlist", "Playlist")
        .str.replace_all("zpdes", "ZPDES")
    )
    top_paths = (
        displayed_paths.group_by("displayed_path")
        .agg(pl.len().alias("student_count"))
        .with_columns((pl.col("student_count") / pl.lit(total_students)).alias("student_share"))
        .sort(["student_count", "displayed_path"], descending=[True, False])
    )
    path_text = _top_rank_text(
        top_paths,
        label_fn=lambda row: str(row["displayed_path"]),
        metric_fn=lambda row: f"{_format_num(row['student_count'])} students, {_format_pct(_safe_float(row['student_share']))} of all students",
        limit=min(10, top_paths.height),
    )
    if path_text:
        findings.append(f"Most common displayed paths: {path_text}.")

    one_or_fewer_share = float(table.filter(pl.col("transition_count_total") <= 1).height) / total_students
    if one_or_fewer_share >= 0.75 and more_than_three_share < 0.10:
        interpretation = "Most students either stay in their initial work mode or change once, so repeated cross-mode cycling looks uncommon in the full history."
    elif more_than_three_share >= 0.10:
        interpretation = "Repeated cross-mode cycling is not rare in the full history, so work-mode use appears more interleaved than a simple start-and-settle pattern."
    else:
        interpretation = "The full-history picture mixes stable single-mode use with a visible minority of students who continue moving between modes over time."
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_bottleneck_chart(frame: pl.DataFrame | pd.DataFrame | None) -> FigureAnalysis:
    table = _as_polars(frame)
    if table.height == 0:
        return _insufficient()
    supported = table.filter(pl.col("attempts") >= RATE_MIN_OBSERVATIONS).with_columns(
        pl.col("failure_rate").cast(pl.Float64),
        pl.col("repeat_attempt_rate").cast(pl.Float64),
        pl.col("bottleneck_score").cast(pl.Float64),
        (pl.col("bottleneck_score") * pl.col("attempts")).cast(pl.Float64).alias("impact_score"),
    )
    if supported.height == 0:
        return _insufficient(["Bottleneck commentary is only surfaced when candidates have at least 20 attempts."])
    ranked = supported.sort(
        ["bottleneck_score", "attempts", "entity_label_raw"],
        descending=[True, True, False],
    )
    impact_ranked = supported.sort(
        ["impact_score", "attempts", "entity_label_raw"],
        descending=[True, True, False],
    )
    top = ranked.row(0, named=True)
    top_impact = impact_ranked.row(0, named=True)
    findings = [f"The strongest visible bottleneck candidate is {top['entity_label_raw']} with a combined score of {_format_num(_safe_float(top['bottleneck_score']), digits=2)} across {_format_num(top['attempts'])} attempts."]
    findings.append(
        f"The highest raw-impact bottleneck is {top_impact['entity_label_raw']} with an impact score of {_format_num(_safe_float(top_impact['impact_score']), digits=1)}."
    )
    failure = _safe_float(top['failure_rate']) or 0.0
    repeat = _safe_float(top['repeat_attempt_rate']) or 0.0
    if failure >= repeat + 0.10:
        findings.append(f"For {top['entity_label_raw']}, the signal is mainly failure-driven ({_format_pct(failure)} failure vs {_format_pct(repeat)} repeat-attempt rate).")
        driver = 'difficulty'
    elif repeat >= failure + 0.10:
        findings.append(f"For {top['entity_label_raw']}, repeat attempts are unusually elevated ({_format_pct(repeat)}) relative to failure ({_format_pct(failure)}).")
        driver = 'persistence'
    else:
        findings.append(f"For {top['entity_label_raw']}, failure ({_format_pct(failure)}) and repeat attempts ({_format_pct(repeat)}) both contribute materially.")
        driver = 'mixed'
    total_score = float(impact_ranked['impact_score'].sum())
    top_three_share = float(impact_ranked.head(min(3, impact_ranked.height))['impact_score'].sum()) / total_score if total_score > 0 else 0.0
    if top_three_share >= 0.60:
        findings.append(f"The displayed bottleneck mass is concentrated: the top 3 rows account for {_format_pct(top_three_share)} of the visible score.")
        spread = 'localized'
    else:
        findings.append(f"The displayed bottleneck mass is spread across several rows: the top 3 rows account for {_format_pct(top_three_share)} of the visible score.")
        spread = 'broad'
    raw_text = _top_rank_text(
        impact_ranked,
        label_fn=lambda row: str(row["entity_label_raw"]),
        metric_fn=lambda row: f"impact {_format_num(_safe_float(row['impact_score']), digits=1)}",
    )
    if raw_text:
        findings.append(f"Top raw-impact bottlenecks: {raw_text}.")
    normalized_text = _top_rank_text(
        ranked,
        label_fn=lambda row: str(row["entity_label_raw"]),
        metric_fn=lambda row: f"score {_format_num(_safe_float(row['bottleneck_score']), digits=2)}, {_format_num(row['attempts'])} attempts",
    )
    if normalized_text:
        findings.append(f"Top normalized bottlenecks: {normalized_text}.")
    driver_rank = supported.with_columns(
        (pl.col("failure_rate") - pl.col("repeat_attempt_rate")).abs().alias("driver_gap")
    ).sort(["driver_gap", "attempts", "entity_label_raw"], descending=[True, True, False])
    driver_text = _top_rank_text(
        driver_rank,
        label_fn=lambda row: str(row["entity_label_raw"]),
        metric_fn=lambda row: (
            f"failure-led by {_format_pct((_safe_float(row['failure_rate']) or 0.0) - (_safe_float(row['repeat_attempt_rate']) or 0.0))}"
            if (_safe_float(row["failure_rate"]) or 0.0) >= (_safe_float(row["repeat_attempt_rate"]) or 0.0)
            else f"repeat-led by {_format_pct((_safe_float(row['repeat_attempt_rate']) or 0.0) - (_safe_float(row['failure_rate']) or 0.0))}"
        ),
    )
    if driver_text:
        findings.append(f"Strongest driver asymmetries: {driver_text}.")
    if driver == 'difficulty' and spread == 'localized':
        interpretation = 'This looks more like a localized difficulty point than a general persistence pattern. A plausible explanation is that a small number of entities are genuinely hard in the current slice.'
    elif driver == 'persistence' and spread == 'broad':
        interpretation = 'This looks more like a broad revisit pattern than a single failing choke point. A plausible explanation is that learners keep cycling through several entities before consolidating progress.'
    else:
        interpretation = 'The visible bottleneck pattern mixes failure and revisit behavior. A plausible explanation is that both intrinsic difficulty and repeated practice are contributing in the current scope.'
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)

def analyze_transition_chart(frame: pl.DataFrame | pd.DataFrame | None) -> FigureAnalysis:
    table = _as_polars(frame)
    if table.height == 0:
        return _insufficient()
    table = table.with_columns(
        pl.col('transition_count').cast(pl.Int64),
        pl.col('success_conditioned_count').cast(pl.Int64),
        pl.when(pl.col('transition_count') > 0).then(pl.col('success_conditioned_count') / pl.col('transition_count')).otherwise(None).alias('destination_success_rate'),
        pl.when(pl.col('transition_count') > 0)
        .then(pl.col('transition_count') * (1 - (pl.col('success_conditioned_count') / pl.col('transition_count'))))
        .otherwise(0.0)
        .alias('fragility_impact'),
    )
    total = int(table['transition_count'].sum())
    if total < TRANSITION_MIN_OBSERVATIONS:
        return _insufficient(["Transition commentary is only surfaced when the current slice contains at least 20 transitions."])
    ranked = table.sort(['transition_count', 'from_activity_label', 'to_activity_label'], descending=[True, False, False])
    top = ranked.row(0, named=True)
    findings = [f"The most common displayed cross-objective transition is {top['from_activity_label']} -> {top['to_activity_label']} with {_format_num(top['transition_count'])} transitions ({_format_pct((_safe_float(top['transition_count']) or 0.0) / total)} of the visible total)."]
    top_three_share = float(ranked.head(min(3, ranked.height))['transition_count'].sum()) / total
    if top_three_share >= 0.60:
        findings.append(f"A small number of paths dominate the visible flow: the top 3 transitions account for {_format_pct(top_three_share)} of the displayed total.")
        concentration = 'high'
    else:
        findings.append(f"The visible flow is more distributed: the top 3 transitions account for {_format_pct(top_three_share)} of the displayed total.")
        concentration = 'moderate'
    weak_common = ranked.filter(pl.col('transition_count') >= TRANSITION_MIN_OBSERVATIONS).sort(['destination_success_rate', 'transition_count', 'from_activity_label', 'to_activity_label'], descending=[False, True, False, False])
    fragile = ranked.filter(pl.col('transition_count') >= TRANSITION_MIN_OBSERVATIONS).sort(
        ['fragility_impact', 'transition_count', 'from_activity_label', 'to_activity_label'],
        descending=[True, True, False, False],
    )
    if fragile.height > 0:
        fragile_top = fragile.row(0, named=True)
        findings.append(
            f"The highest raw-impact fragile transition is {fragile_top['from_activity_label']} -> {fragile_top['to_activity_label']} with {_format_num(_safe_float(fragile_top['fragility_impact']), digits=1)} estimated failing destination transitions."
        )
    weak_rate = None
    if weak_common.height > 0:
        weak = weak_common.row(0, named=True)
        weak_rate = _safe_float(weak['destination_success_rate']) or 0.0
        findings.append(f"Among common transitions, {weak['from_activity_label']} -> {weak['to_activity_label']} leads to the weakest observed destination success ({_format_pct(weak_rate)}).")
        remaining_success = int(table["success_conditioned_count"].sum()) - int(weak["success_conditioned_count"])
        remaining_total = int(table["transition_count"].sum()) - int(weak["transition_count"])
        p_value = _two_proportion_p_value(
            int(weak["success_conditioned_count"]),
            int(weak["transition_count"]),
            remaining_success,
            remaining_total,
        )
        if p_value is not None and remaining_total > 0:
            findings.append(
                f"For that weakest common transition, the destination-success gap versus the rest of the visible flow is {_format_pct(weak_rate - (remaining_success / remaining_total))}; a two-proportion test gives {_format_p_value(p_value)}."
            )
    volume_text = _top_rank_text(
        ranked,
        label_fn=lambda row: f"{row['from_activity_label']} -> {row['to_activity_label']}",
        metric_fn=lambda row: f"{_format_num(row['transition_count'])} transitions",
    )
    if volume_text:
        findings.append(f"Highest-volume transitions: {volume_text}.")
    if fragile.height > 0:
        fragile_text = _top_rank_text(
            fragile,
            label_fn=lambda row: f"{row['from_activity_label']} -> {row['to_activity_label']}",
            metric_fn=lambda row: f"fragility impact {_format_num(_safe_float(row['fragility_impact']), digits=1)}",
        )
        if fragile_text:
            findings.append(f"Highest fragile-transition impact: {fragile_text}.")
    if weak_rate is not None and weak_rate < 0.50 and concentration == 'high':
        interpretation = 'A few transition paths dominate the visible traffic, and at least one of them ends in weak destination success. A plausible explanation is that this scope contains a sequencing bridge that many learners take but do not yet handle well.'
    elif weak_rate is not None and weak_rate < 0.50:
        interpretation = 'Some common transition paths still land in weaker destination success. A plausible explanation is that the bridge between activities is uneven rather than uniformly problematic.'
    else:
        interpretation = 'The visible transition pattern does not show a single clearly weak bridge. The main story in this slice is path volume rather than a strongly failing transition.'
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_matrix_heatmap(cells_df: pl.DataFrame | pd.DataFrame | None, *, metric: str, module_label: str) -> FigureAnalysis:
    cells = _as_polars(cells_df)
    if cells.height == 0:
        return _insufficient()
    cells = cells.with_columns(pl.col('metric_value').cast(pl.Float64))
    ranked_low = cells.sort(['metric_value', 'objective_id', 'activity_col_idx'], descending=[False, False, False])
    ranked_high = cells.sort(['metric_value', 'objective_id', 'activity_col_idx'], descending=[True, False, False])
    if metric == 'activity_mean_exercise_elo':
        hardest = ranked_high.row(0, named=True)
        easiest = ranked_low.row(0, named=True)
        findings = [
            f"In {module_label}, the hardest visible activity by mean exercise Elo is {_label(hardest)} ({_format_num(_safe_float(hardest['metric_value']), digits=0)}).",
            f"The easiest visible activity is {_label(easiest)} ({_format_num(_safe_float(easiest['metric_value']), digits=0)}).",
        ]
        hardest_text = _top_rank_text(
            ranked_high,
            label_fn=_label,
            metric_fn=lambda row: f"{_format_num(_safe_float(row['metric_value']), digits=0)} Elo",
        )
        if hardest_text:
            findings.append(f"Highest-Elo activities: {hardest_text}.")
        objective_rank = cells.group_by(['objective_id', 'objective_label']).agg(pl.col('metric_value').mean().alias('objective_metric')).sort(['objective_metric', 'objective_id'], descending=[True, False])
        if objective_rank.height > 0:
            findings.append(f"{objective_rank.row(0, named=True)['objective_label']} has the highest average visible Elo in this slice.")
            objective_text = _top_rank_text(
                objective_rank,
                label_fn=lambda row: str(row["objective_label"]),
                metric_fn=lambda row: f"mean Elo {_format_num(_safe_float(row['objective_metric']), digits=0)}",
            )
            if objective_text:
                findings.append(f"Hardest objectives by mean Elo: {objective_text}.")
        position_means = cells.group_by('activity_col_idx').agg(pl.col('metric_value').mean().alias('mean_metric')).sort('activity_col_idx')
        midpoint = int(position_means['activity_col_idx'].median() or 1)
        early_mean = _safe_float(position_means.filter(pl.col('activity_col_idx') <= midpoint)['mean_metric'].mean())
        late_mean = _safe_float(position_means.filter(pl.col('activity_col_idx') > midpoint)['mean_metric'].mean())
        interpretation = 'The module looks back-loaded in visible difficulty.' if late_mean is not None and early_mean is not None and late_mean > early_mean else 'Visible difficulty is mixed across the module rather than strictly increasing by position.'
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)
    if metric in LOW_IS_WORSE_RATE_METRICS:
        lowest = ranked_low.row(0, named=True)
        highest = ranked_high.row(0, named=True)
        findings = [
            f"The weakest visible cell is {_label(lowest)} at {_format_pct(_safe_float(lowest['metric_value']))}.",
            f"The strongest visible cell is {_label(highest)} at {_format_pct(_safe_float(highest['metric_value']))}.",
        ]
        weak_text = _top_rank_text(
            ranked_low,
            label_fn=_label,
            metric_fn=lambda row: _format_pct(_safe_float(row["metric_value"])),
        )
        if weak_text:
            findings.append(f"Weakest visible activities for this metric: {weak_text}.")
        strong_text = _top_rank_text(
            ranked_high,
            label_fn=_label,
            metric_fn=lambda row: _format_pct(_safe_float(row["metric_value"])),
        )
        if strong_text:
            findings.append(f"Strongest visible activities for this metric: {strong_text}.")
        objective_rank = cells.group_by(['objective_id', 'objective_label']).agg(pl.col('metric_value').mean().alias('objective_metric')).sort(['objective_metric', 'objective_id'], descending=[False, False])
        if objective_rank.height > 0:
            findings.append(f"{objective_rank.row(0, named=True)['objective_label']} has the weakest average visible value for this metric.")
            objective_text = _top_rank_text(
                objective_rank,
                label_fn=lambda row: str(row["objective_label"]),
                metric_fn=lambda row: _format_pct(_safe_float(row["objective_metric"])),
            )
            if objective_text:
                findings.append(f"Weakest objectives for this metric: {objective_text}.")
        position_means = cells.group_by('activity_col_idx').agg(pl.col('metric_value').mean().alias('mean_metric')).sort('activity_col_idx')
        midpoint = int(position_means['activity_col_idx'].median() or 1)
        early_mean = _safe_float(position_means.filter(pl.col('activity_col_idx') <= midpoint)['mean_metric'].mean())
        late_mean = _safe_float(position_means.filter(pl.col('activity_col_idx') > midpoint)['mean_metric'].mean())
        if late_mean is not None and early_mean is not None and late_mean + 0.05 < early_mean:
            interpretation = 'The visible module slice looks back-loaded in difficulty: later activity positions tend to have weaker outcomes.'
        elif late_mean is not None and early_mean is not None and late_mean > early_mean + 0.05:
            interpretation = 'The visible module slice looks front-loaded in difficulty: earlier activity positions tend to be weaker than later ones.'
        else:
            interpretation = 'Visible performance looks uneven but not strongly monotonic across activity positions.'
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)
    if metric == 'repeat_attempt_rate':
        highest = ranked_high.row(0, named=True)
        lowest = ranked_low.row(0, named=True)
        findings = [
            f"The highest repeat-attempt pressure is on {_label(highest)} at {_format_pct(_safe_float(highest['metric_value']))}.",
            f"The lowest repeat-attempt pressure is on {_label(lowest)} at {_format_pct(_safe_float(lowest['metric_value']))}.",
        ]
        repeat_text = _top_rank_text(
            ranked_high,
            label_fn=_label,
            metric_fn=lambda row: _format_pct(_safe_float(row["metric_value"])),
        )
        if repeat_text:
            findings.append(f"Highest repeat-attempt activities: {repeat_text}.")
        objective_rank = cells.group_by(['objective_id', 'objective_label']).agg(pl.col('metric_value').mean().alias('objective_metric')).sort(['objective_metric', 'objective_id'], descending=[True, False])
        if objective_rank.height > 0:
            findings.append(f"{objective_rank.row(0, named=True)['objective_label']} carries the highest average repeat-attempt rate in the visible slice.")
        return FigureAnalysis(findings=tuple(findings), interpretation='High repeat-attempt pockets can plausibly signal sticky content or deliberate practice loops. The visible slice suggests that this pattern is concentrated rather than uniform.')
    highest = ranked_high.row(0, named=True)
    findings = [f"The largest visible value is on {_label(highest)} ({_format_num(_safe_float(highest['metric_value']))})."]
    value_text = _top_rank_text(
        ranked_high,
        label_fn=_label,
        metric_fn=lambda row: _format_num(_safe_float(row["metric_value"])),
    )
    if value_text:
        findings.append(f"Highest visible activities for this metric: {value_text}.")
    objective_rank = cells.group_by(['objective_id', 'objective_label']).agg(pl.col('metric_value').mean().alias('objective_metric')).sort(['objective_metric', 'objective_id'], descending=[True, False])
    if objective_rank.height > 0:
        findings.append(f"{objective_rank.row(0, named=True)['objective_label']} has the highest average visible value for this metric.")
    total_metric = float(cells['metric_value'].sum()) if cells.height > 0 else 0.0
    if total_metric > 0 and metric in {'attempts', 'playlist_unique_exercises'}:
        share = (_safe_float(highest['metric_value']) or 0.0) / total_metric
        findings.append(f"The top visible cell represents {_format_pct(share)} of the displayed total for this metric.")
        interpretation = 'The visible usage is fairly concentrated in a few cells.' if share >= 0.20 else 'The visible usage is spread across multiple cells rather than dominated by one activity.'
    else:
        interpretation = 'The visible slice highlights where this metric is highest, without a strong front-to-back progression trend.'
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)

def analyze_matrix_drilldown_table(drilldown: pl.DataFrame | pd.DataFrame | None, *, metric: str, activity_label: str | None = None) -> FigureAnalysis:
    if drilldown is None:
        return _insufficient(["Click a populated cell to generate exercise-level analysis."])
    table = _as_polars(drilldown)
    if table.height == 0:
        return _insufficient(["No exercise rows are available for the current drilldown selection."])
    activity_txt = str(activity_label or 'this activity')
    if metric == 'activity_mean_exercise_elo':
        supported = table.filter(pl.col('calibration_attempts').fill_null(0) >= DRILLDOWN_MIN_ATTEMPTS)
        if supported.height < 2:
            return _insufficient([f"{activity_txt} does not have enough calibrated exercise rows for direct comparison."])
        ranked = supported.sort(['exercise_elo', 'calibration_attempts', 'exercise_id'], descending=[True, True, False])
        hardest = ranked.row(0, named=True)
        easiest = ranked.row(ranked.height - 1, named=True)
        spread = (_safe_float(hardest['exercise_elo']) or 0.0) - (_safe_float(easiest['exercise_elo']) or 0.0)
        findings = [
            f"The hardest calibrated exercise in {activity_txt} is {hardest['exercise_short_id']} ({_format_num(_safe_float(hardest['exercise_elo']), digits=0)} Elo).",
            f"The easiest calibrated exercise is {easiest['exercise_short_id']} ({_format_num(_safe_float(easiest['exercise_elo']), digits=0)} Elo).",
            f"The visible Elo spread inside the activity is {_format_num(spread, digits=0)} points.",
        ]
        elo_text = _top_rank_text(
            ranked,
            label_fn=lambda row: str(row["exercise_short_id"]),
            metric_fn=lambda row: f"{_format_num(_safe_float(row['exercise_elo']), digits=0)} Elo",
        )
        if elo_text:
            findings.append(f"Hardest calibrated exercises: {elo_text}.")
        interpretation = 'The activity looks internally heterogeneous, so its average Elo is plausibly driven by a mix of clearly easier and harder exercises.' if spread >= 120 else 'The calibrated exercises in this activity are relatively homogeneous, so the activity-level Elo looks broadly shared across items.'
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)
    supported = table.filter(pl.col('attempts').fill_null(0) >= DRILLDOWN_MIN_ATTEMPTS)
    if supported.height < 2:
        return _insufficient([f"{activity_txt} needs at least two exercises with 10 or more attempts for direct comparison."])
    metric_column = {
        'attempts': 'attempts',
        'playlist_unique_exercises': 'attempts',
        'success_rate': 'success_rate',
        'exercise_balanced_success_rate': 'success_rate',
        'repeat_attempt_rate': 'repeat_attempt_rate',
        'first_attempt_success_rate': 'first_attempt_success_rate',
    }.get(metric, 'metric_value')
    descending = metric in LOW_IS_WORSE_RATE_METRICS or metric in HIGH_IS_WORSE_RATE_METRICS or metric in {"attempts", "playlist_unique_exercises"}
    ranked = supported.sort([metric_column, 'attempts', 'exercise_short_id'], descending=[descending, True, False])
    top = ranked.row(0, named=True)
    bottom = ranked.row(ranked.height - 1, named=True)
    spread = abs((_safe_float(top[metric_column]) or 0.0) - (_safe_float(bottom[metric_column]) or 0.0))
    if metric in {'attempts', 'playlist_unique_exercises'}:
        findings = [
            f"The busiest exercise in {activity_txt} is {top['exercise_short_id']} with {_format_num(top['attempts'])} attempts.",
            f"The lightest sufficiently observed exercise is {bottom['exercise_short_id']} with {_format_num(bottom['attempts'])} attempts.",
            f"The visible spread inside the activity is {_format_num(spread, digits=0)} attempts.",
        ]
        attempt_text = _top_rank_text(
            ranked,
            label_fn=lambda row: str(row["exercise_short_id"]),
            metric_fn=lambda row: f"{_format_num(row['attempts'])} attempts",
        )
        if attempt_text:
            findings.append(f"Exercise volume ranking: {attempt_text}.")
        interpretation = 'The activity looks driven by a subset of exercises.' if spread >= 20 else 'The exercise traffic inside this activity looks relatively even.'
        return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)
    if metric == 'repeat_attempt_rate':
        findings = [
            f"The highest repeat-attempt rate in {activity_txt} is on {top['exercise_short_id']} ({_format_pct(_safe_float(top[metric_column]))}).",
            f"The lowest repeat-attempt rate is on {bottom['exercise_short_id']} ({_format_pct(_safe_float(bottom[metric_column]))}).",
            f"The visible spread inside the activity is {_format_pct(spread)}.",
        ]
        repeat_text = _top_rank_text(
            ranked,
            label_fn=lambda row: str(row["exercise_short_id"]),
            metric_fn=lambda row: _format_pct(_safe_float(row[metric_column])),
        )
        if repeat_text:
            findings.append(f"Repeat-attempt ranking: {repeat_text}.")
        interpretation = 'The activity looks internally uneven in persistence pressure, so repeated retries are plausibly concentrated on a smaller subset of exercises.' if spread >= 0.20 else 'Repeat-attempt behavior looks relatively homogeneous across the visible exercises.'
    else:
        findings = [
            f"The strongest sufficiently observed exercise in {activity_txt} is {top['exercise_short_id']} ({_format_pct(_safe_float(top[metric_column]))}).",
            f"The weakest sufficiently observed exercise is {bottom['exercise_short_id']} ({_format_pct(_safe_float(bottom[metric_column]))}).",
            f"The visible spread inside the activity is {_format_pct(spread)}.",
        ]
        rate_text = _top_rank_text(
            ranked if metric in HIGH_IS_WORSE_RATE_METRICS else supported.sort([metric_column, 'attempts', 'exercise_short_id'], descending=[False, True, False]),
            label_fn=lambda row: str(row["exercise_short_id"]),
            metric_fn=lambda row: _format_pct(_safe_float(row[metric_column])),
        )
        if rate_text:
            label_prefix = "Highest" if metric in HIGH_IS_WORSE_RATE_METRICS else "Lowest"
            findings.append(f"{label_prefix} exercises for this metric: {rate_text}.")
        top_successes = _approx_successes(top[metric_column], top["attempts"])
        bottom_successes = _approx_successes(bottom[metric_column], bottom["attempts"])
        p_value = _two_proportion_p_value(
            top_successes,
            top["attempts"],
            bottom_successes,
            bottom["attempts"],
        )
        if p_value is not None:
            findings.append(
                f"The gap between the strongest and weakest sufficiently observed exercises is {_format_pct(spread)}; a two-proportion test gives {_format_p_value(p_value)}."
            )
        interpretation = 'The activity looks internally uneven, so the activity-level result is plausibly being pulled by a smaller set of exercises.' if spread >= 0.20 else 'The exercise-level results look relatively homogeneous, so the activity-level result appears broad rather than driven by a single outlier.'
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_zpdes_transition_efficiency(nodes: pl.DataFrame | pd.DataFrame | None, *, later_attempt_threshold: int) -> FigureAnalysis:
    frame = _as_polars(nodes)
    if frame.height == 0:
        return _insufficient()
    activities = frame.filter(pl.col('node_type') == 'activity')
    if activities.height == 0:
        return _insufficient()
    findings: list[str] = []
    supported_success = activities.filter(pl.col('zpdes_first_attempt_event_count').fill_null(0) >= RATE_MIN_OBSERVATIONS)
    if supported_success.height > 0:
        weakest = supported_success.sort(['zpdes_first_attempt_success_rate', 'zpdes_first_attempt_event_count', 'label'], descending=[False, True, False]).row(0, named=True)
        findings.append(f"The weakest sufficiently observed ZPDES first-attempt result is on {weakest['label']} at {_format_pct(_safe_float(weakest['zpdes_first_attempt_success_rate']))} across {_format_num(weakest['zpdes_first_attempt_event_count'])} events.")
    gap_candidates = activities.filter((pl.col('before_unique_students').fill_null(0) >= COHORT_MIN_STUDENTS) & (pl.col('after_unique_students').fill_null(0) >= COHORT_MIN_STUDENTS) & (pl.col('after_event_count').fill_null(0) >= RATE_MIN_OBSERVATIONS) & pl.col('before_success_rate').is_not_null() & pl.col('after_success_rate').is_not_null()).with_columns((pl.col('after_success_rate') - pl.col('before_success_rate')).alias('after_before_gap'))
    if gap_candidates.height > 0:
        weakest_gap = gap_candidates.sort(['after_before_gap', 'after_event_count', 'label'], descending=[False, True, False]).row(0, named=True)
        findings.append(f"The largest before/after gap is on {weakest_gap['label']}: before students succeed at {_format_pct(_safe_float(weakest_gap['before_success_rate']))}, while after students (threshold {_format_num(later_attempt_threshold, digits=0)}) succeed at {_format_pct(_safe_float(weakest_gap['after_success_rate']))}.")
    in_activity_candidates = activities.filter((pl.col('before_unique_students').fill_null(0) >= COHORT_MIN_STUDENTS) & (pl.col('in_activity_unique_students').fill_null(0) >= COHORT_MIN_STUDENTS) & pl.col('before_success_rate').is_not_null() & pl.col('in_activity_success_rate').is_not_null()).with_columns((pl.col('in_activity_success_rate') - pl.col('before_success_rate')).alias('in_activity_gap'))
    if in_activity_candidates.height > 0:
        best = in_activity_candidates.sort(['in_activity_gap', 'in_activity_event_count', 'label'], descending=[True, True, False]).row(0, named=True)
        gap = _safe_float(best['in_activity_gap']) or 0.0
        if gap >= 0.05:
            findings.append(f"In-activity returns improve on {best['label']}: in-activity success is {_format_pct(_safe_float(best['in_activity_success_rate']))} versus {_format_pct(_safe_float(best['before_success_rate']))} for before students.")
        else:
            weakest = in_activity_candidates.sort(['in_activity_gap', 'in_activity_event_count', 'label'], descending=[False, True, False]).row(0, named=True)
            findings.append(f"In-activity progress is relatively flat on {weakest['label']}: in-activity success is {_format_pct(_safe_float(weakest['in_activity_success_rate']))} versus {_format_pct(_safe_float(weakest['before_success_rate']))} for before students.")
    if not findings:
        return _insufficient(["This page only promotes cohort comparisons when rate estimates have at least 20 events and both cohorts include at least 5 unique students."])
    interpretation = 'Large before/after gaps can plausibly signal sequencing friction, while stronger in-activity outcomes can plausibly indicate that students recover once they stay inside the activity.'
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_zpdes_transition_population(
    summary: pl.DataFrame | pd.DataFrame | None,
    *,
    later_attempt_threshold: int,
) -> FigureAnalysis:
    table = _as_polars(summary)
    if table.height == 0:
        return _insufficient()
    supported_before = (
        table.filter(
            (pl.col("before_event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS)
            & (pl.col("before_unique_students").fill_null(0) >= COHORT_MIN_STUDENTS)
            & pl.col("before_success_rate").is_not_null()
        )
        .with_columns(
            (pl.col("before_event_count") * (1 - pl.col("before_success_rate"))).alias("before_failure_impact")
        )
    )
    findings: list[str] = []
    if supported_before.height > 0:
        raw = supported_before.sort(
            ["before_failure_impact", "before_event_count", "module_code", "activity_label"],
            descending=[True, True, False, False],
        ).row(0, named=True)
        findings.append(
            f"The highest raw-impact weak prerequisite outcome is {_module_activity_label(raw)}: before students succeed at {_format_pct(_safe_float(raw['before_success_rate']))} across {_format_num(raw['before_event_count'])} events."
        )
        normalized = supported_before.sort(
            ["before_success_rate", "before_event_count", "module_code", "activity_label"],
            descending=[False, True, False, False],
        ).row(0, named=True)
        findings.append(
            f"The strongest normalized severity signal is {_module_activity_label(normalized)} at {_format_pct(_safe_float(normalized['before_success_rate']))} before success, which keeps the ranking from being dominated by the busiest modules alone."
        )
        raw_text = _top_rank_text(
            supported_before.sort(
                ["before_failure_impact", "before_event_count", "module_code", "activity_label"],
                descending=[True, True, False, False],
            ),
            label_fn=_module_activity_label,
            metric_fn=lambda row: f"before success {_format_pct(_safe_float(row['before_success_rate']))}, {_format_num(row['before_event_count'])} events",
        )
        if raw_text:
            findings.append(f"Weakest activities by raw impact: {raw_text}.")
        normalized_text = _top_rank_text(
            supported_before.sort(
                ["before_success_rate", "before_event_count", "module_code", "activity_label"],
                descending=[False, True, False, False],
            ),
            label_fn=_module_activity_label,
            metric_fn=lambda row: f"before success {_format_pct(_safe_float(row['before_success_rate']))}",
        )
        if normalized_text:
            findings.append(f"Weakest activities by normalized severity: {normalized_text}.")
        top_modules = int(
            supported_before.sort(
                ["before_failure_impact", "before_event_count", "module_code", "activity_label"],
                descending=[True, True, False, False],
            )
            .head(min(5, supported_before.height))
            .select(pl.col("module_code").n_unique())
            .item()
            or 0
        )
        findings.append(
            f"The top 5 raw-impact weak activities span {_format_num(top_modules, digits=0)} module(s), which helps distinguish broad structural friction from a single high-volume module effect."
        )
    insufficiency = (
        table.filter(
            (pl.col("before_event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS)
            & (pl.col("before_unique_students").fill_null(0) >= COHORT_MIN_STUDENTS)
            & (pl.col("in_activity_event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS)
            & (pl.col("in_activity_unique_students").fill_null(0) >= COHORT_MIN_STUDENTS)
            & pl.col("before_success_rate").is_not_null()
            & pl.col("in_activity_success_rate").is_not_null()
        )
        .with_columns(
            (pl.col("in_activity_success_rate") - pl.col("before_success_rate")).alias("in_activity_gap"),
            (pl.col("before_event_count") * (1 - pl.col("before_success_rate"))).alias("before_failure_impact"),
        )
    )
    if insufficiency.height > 0:
        candidate = insufficiency.sort(
            ["in_activity_gap", "before_failure_impact", "module_code", "activity_label"],
            descending=[True, True, False, False],
        ).row(0, named=True)
        gap = _safe_float(candidate["in_activity_gap"]) or 0.0
        if gap >= 0.05:
            p_value = _two_proportion_p_value(
                candidate.get("before_success_count"),
                candidate.get("before_event_count"),
                candidate.get("in_activity_success_count"),
                candidate.get("in_activity_event_count"),
            )
            findings.append(
                f"The strongest structural-insufficiency candidate is {_module_activity_label(candidate)}: before success is {_format_pct(_safe_float(candidate['before_success_rate']))}, but in-activity success rises to {_format_pct(_safe_float(candidate['in_activity_success_rate']))} ({_format_p_value(p_value)} by two-proportion test)."
            )
        insufficiency_text = _top_rank_text(
            insufficiency.sort(
                ["in_activity_gap", "before_failure_impact", "module_code", "activity_label"],
                descending=[True, True, False, False],
            ),
            label_fn=_module_activity_label,
            metric_fn=lambda row: f"in-activity gain {_format_pct(_safe_float(row['in_activity_gap']))}",
        )
        if insufficiency_text:
            findings.append(f"Strongest recovery after same-activity exposure: {insufficiency_text}.")
    after_gap = (
        table.filter(
            (pl.col("before_event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS)
            & (pl.col("before_unique_students").fill_null(0) >= COHORT_MIN_STUDENTS)
            & (pl.col("after_event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS)
            & (pl.col("after_unique_students").fill_null(0) >= COHORT_MIN_STUDENTS)
            & pl.col("before_success_rate").is_not_null()
            & pl.col("after_success_rate").is_not_null()
        )
        .with_columns((pl.col("after_success_rate") - pl.col("before_success_rate")).alias("after_gap"))
    )
    if after_gap.height > 0:
        candidate = after_gap.sort(
            ["after_gap", "after_event_count", "module_code", "activity_label"],
            descending=[False, True, False, False],
        ).row(0, named=True)
        gap = _safe_float(candidate["after_gap"]) or 0.0
        if gap <= -0.05:
            p_value = _two_proportion_p_value(
                candidate.get("before_success_count"),
                candidate.get("before_event_count"),
                candidate.get("after_success_count"),
                candidate.get("after_event_count"),
            )
            findings.append(
                f"The strongest after-friction candidate is {_module_activity_label(candidate)}: after students with at least {_format_num(later_attempt_threshold, digits=0)} later attempts succeed at {_format_pct(_safe_float(candidate['after_success_rate']))}, below the {_format_pct(_safe_float(candidate['before_success_rate']))} seen for before students ({_format_p_value(p_value)} by two-proportion test)."
            )
        after_text = _top_rank_text(
            after_gap.sort(
                ["after_gap", "after_event_count", "module_code", "activity_label"],
                descending=[False, True, False, False],
            ),
            label_fn=_module_activity_label,
            metric_fn=lambda row: f"after-before gap {_format_pct(_safe_float(row['after_gap']))}",
        )
        if after_text:
            findings.append(f"Strongest after-friction ranking: {after_text}.")
    if not findings:
        return _insufficient(
            [
                "Global ZPDES analysis only promotes activities when rate estimates have at least 20 events and cohort comparisons include at least 5 unique students."
            ]
        )
    raw_mentions = "raw impact" if supported_before.height > 0 else "page-wide support"
    if insufficiency.height > 0 and after_gap.height > 0:
        interpretation = (
            "Across modules, the ZPDES graph does not look uniformly efficient. The broadest weak signals combine "
            f"{raw_mentions} with cohort gaps: some activities look under-prepared for before students, while some revisits from later content still do not recover performance."
        )
    elif insufficiency.height > 0:
        interpretation = (
            "Across modules, the clearest page-wide signal is structural insufficiency: some activities look substantially easier only after students stay inside the activity itself, which plausibly means the current prerequisite set is not always enough."
        )
    elif after_gap.height > 0:
        interpretation = (
            "Across modules, the clearest page-wide signal is after-friction: returning from later content does not always rescue success, which plausibly means some revisits happen after learners have already drifted past an unstable prerequisite bridge."
        )
    else:
        interpretation = (
            "Across modules, the page-wide ZPDES analysis mainly surfaces raw weak prerequisite outcomes rather than a single dominant recovery pattern."
        )
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_m1_individual_path(
    payload: dict[str, Any] | None,
    *,
    frame_idx: int,
) -> FigureAnalysis:
    if not payload:
        return _insufficient()
    student_ids = [str(user_id) for user_id in payload.get("student_ids") or [] if str(user_id).strip()]
    if not student_ids:
        return _insufficient()
    frame_cutoffs = [int(value) for value in payload.get("frame_cutoffs") or [0]]
    current_idx = min(max(0, int(frame_idx)), len(frame_cutoffs) - 1)
    cutoff = frame_cutoffs[current_idx]
    user_id = student_ids[0]
    series = ((payload.get("series") or {}).get(user_id)) or {}
    ordinals = [int(value) for value in series.get("attempt_ordinal") or []]
    if not ordinals:
        return _insufficient()

    visible_count = bisect_right(ordinals, cutoff)
    visible_activity_ids = [str(value or "") for value in (series.get("activity_id") or [])[:visible_count]]
    visible_labels = [str(value or "") for value in (series.get("activity_label") or [])[:visible_count]]
    visible_outcomes = [float(value or 0.0) for value in (series.get("outcome") or [])[:visible_count]]
    visible_mapped = [bool(value) for value in (series.get("is_mapped_activity") or [])[:visible_count]]

    mapped_attempts = sum(1 for value in visible_mapped if value)
    unmapped_attempts = visible_count - mapped_attempts
    mapped_successes = sum(
        outcome for outcome, is_mapped in zip(visible_outcomes, visible_mapped, strict=False) if is_mapped
    )
    visited_activities = sorted(
        {
            activity_id
            for activity_id, is_mapped in zip(visible_activity_ids, visible_mapped, strict=False)
            if is_mapped and activity_id
        }
    )

    compressed: list[tuple[str, str]] = []
    for activity_id, label, is_mapped in zip(
        visible_activity_ids,
        visible_labels,
        visible_mapped,
        strict=False,
    ):
        if not is_mapped or not activity_id:
            continue
        if not compressed or compressed[-1][0] != activity_id:
            compressed.append((activity_id, label or activity_id))
    recent_transitions = [
        f"{compressed[idx][1]} -> {compressed[idx + 1][1]}"
        for idx in range(max(0, len(compressed) - 4), max(0, len(compressed) - 1))
    ]

    findings = [
        f"{user_id} is currently at {_format_num(visible_count, digits=0)} visible M1 attempts and has touched {_format_num(len(visited_activities), digits=0)} mapped activities.",
    ]
    if mapped_attempts > 0:
        findings.append(
            f"Mapped activity success is {_format_pct(mapped_successes / mapped_attempts)} across {_format_num(mapped_attempts, digits=0)} mapped attempts."
        )
    if recent_transitions:
        findings.append(f"Recent distinct mapped moves: {' | '.join(recent_transitions[-3:])}.")

    caveats: list[str] = []
    if unmapped_attempts > 0:
        caveats.append(
            f"{_format_num(unmapped_attempts, digits=0)} visible attempt(s) are outside the M1 topology and are excluded from node coloring and arrows."
        )
    if visible_count <= 3:
        caveats.append("Very early replay frames are low-evidence and can change shape quickly.")

    interpretation = (
        f"The replay is currently showing {_format_num(visible_count, digits=0)} of {_format_num(len(ordinals), digits=0)} M1 attempts "
        f"for {user_id}; node color reflects cumulative activity success while size reflects cumulative activity exposure."
    )
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, caveats=tuple(caveats))


def analyze_classroom_progression_replay(payload: dict[str, Any] | None) -> FigureAnalysis:
    if not payload:
        return _insufficient()
    attempt_frames = payload.get('attempt_frames') or []
    success_frames = payload.get('success_frames') or []
    activity_labels = [str(value) for value in payload.get('activity_full_labels') or []]
    student_ids = [str(value) for value in payload.get('student_ids') or []]
    if not attempt_frames or not activity_labels or not student_ids:
        return _insufficient()
    attempts = attempt_frames[-1]
    successes = success_frames[-1] if success_frames else [[0 for _ in row] for row in attempts]
    student_totals = [sum(row[idx] for row in attempts) for idx in range(len(student_ids))]
    activity_totals = [sum(row) for row in attempts]
    findings = [f"By the final frame, {_format_num(sum(1 for total in student_totals if total > 0), digits=0)}/{_format_num(len(student_ids), digits=0)} students and {_format_num(sum(1 for total in activity_totals if total > 0), digits=0)}/{_format_num(len(activity_labels), digits=0)} activities are populated."]
    supported = []
    for idx, label in enumerate(activity_labels):
        total_attempts = int(activity_totals[idx])
        if total_attempts < RATE_MIN_OBSERVATIONS:
            continue
        success_total = int(sum(successes[idx])) if idx < len(successes) else 0
        supported.append({'label': label, 'attempts': total_attempts, 'success_rate': success_total / total_attempts})
    if supported:
        weakest = sorted(supported, key=lambda row: (row['success_rate'], -row['attempts'], row['label']))[0]
        strongest = sorted(supported, key=lambda row: (-row['success_rate'], -row['attempts'], row['label']))[0]
        findings.append(f"Among sufficiently observed activities, {weakest['label']} ends lowest at {_format_pct(weakest['success_rate'])}, while {strongest['label']} ends highest at {_format_pct(strongest['success_rate'])}.")
    total_attempts = sum(student_totals)
    top_two_share = sum(sorted(student_totals, reverse=True)[:2]) / total_attempts if total_attempts > 0 else 0.0
    findings.append(f"The two most active students account for {_format_pct(top_two_share)} of the final attempt volume.")
    avg_attempts = mean(student_totals) if student_totals else 0.0
    coeff_var = ((mean((value - avg_attempts) ** 2 for value in student_totals) ** 0.5) / avg_attempts) if avg_attempts > 0 else 0.0
    if coeff_var <= 0.35:
        interpretation = 'The classroom looks relatively synchronized in the final state: attempt volume is not concentrated in just a few students.'
    elif top_two_share >= 0.60:
        interpretation = 'The final replay state looks fragmented: a small subset of students carries most of the visible activity.'
    else:
        interpretation = 'The final replay state is mixed: participation is neither fully synchronized nor dominated by a single pair of students.'
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_classroom_progression_population(
    scoped_profiles: pl.DataFrame | pd.DataFrame | None,
    activity_summary: pl.DataFrame | pd.DataFrame | None,
    *,
    mode_scope_label: str,
) -> FigureAnalysis:
    profiles = _as_polars(scoped_profiles)
    activities = _as_polars(activity_summary)
    if profiles.height == 0:
        return _insufficient()
    findings: list[str] = []
    median_students = _safe_float(profiles["students"].median()) if "students" in profiles.columns else None
    median_activities = _safe_float(profiles["activities"].median()) if "activities" in profiles.columns else None
    findings.append(
        f"In {mode_scope_label}, classroom size ranges from {_format_num(_safe_float(profiles['students'].min()), digits=0)} to {_format_num(_safe_float(profiles['students'].max()), digits=0)} students, with a median of {_format_num(median_students, digits=0)}."
    )
    findings.append(
        f"Typical classroom coverage is {_format_num(median_activities, digits=0)} activities, while attempt volume ranges from {_format_num(_safe_float(profiles['attempts'].min()), digits=0)} to {_format_num(_safe_float(profiles['attempts'].max()), digits=0)} attempts."
    )
    total_attempts = float(profiles["attempts"].sum()) if "attempts" in profiles.columns else 0.0
    top_three_share = (
        float(profiles.sort("attempts", descending=True).head(min(3, profiles.height))["attempts"].sum()) / total_attempts
        if total_attempts > 0
        else 0.0
    )
    findings.append(
        f"The top 3 classrooms account for {_format_pct(top_three_share)} of visible attempt volume in this scope."
    )
    supported = activities.filter(
        (pl.col("attempts_total").fill_null(0) >= RATE_MIN_OBSERVATIONS)
        & (pl.col("classrooms_observed").fill_null(0) >= 3)
        & pl.col("mean_classroom_success_rate").is_not_null()
    )
    if supported.height > 0:
        raw = supported.with_columns(
            (pl.col("attempts_total") * (1 - pl.col("success_rate"))).alias("failure_impact")
        ).sort(
            ["failure_impact", "attempts_total", "activity_label"],
            descending=[True, True, False],
        ).row(0, named=True)
        findings.append(
            f"The highest raw-impact weak activity is {raw['activity_label']}: overall classroom success is {_format_pct(_safe_float(raw['success_rate']))} across {_format_num(raw['attempts_total'])} attempts in {_format_num(raw['classrooms_observed'], digits=0)} classrooms."
        )
        raw_text = _top_rank_text(
            supported.with_columns(
                (pl.col("attempts_total") * (1 - pl.col("success_rate"))).alias("failure_impact")
            ).sort(
                ["failure_impact", "attempts_total", "activity_label"],
                descending=[True, True, False],
            ),
            label_fn=lambda row: str(row["activity_label"]),
            metric_fn=lambda row: f"success {_format_pct(_safe_float(row['success_rate']))}, {_format_num(row['attempts_total'])} attempts",
        )
        if raw_text:
            findings.append(f"Weakest activities by raw classroom impact: {raw_text}.")
        broad = supported.filter(pl.col("classrooms_observed") >= 5).sort(
            ["weak_classroom_share", "classrooms_observed", "attempts_total", "activity_label"],
            descending=[True, True, True, False],
        )
        if broad.height > 0:
            broad_row = broad.row(0, named=True)
            findings.append(
                f"The broadest repeated weak outcome is {broad_row['activity_label']}: it finishes below 60% classroom-level success in {_format_num(broad_row['weak_classroom_count'], digits=0)} of {_format_num(broad_row['classrooms_observed'], digits=0)} classrooms."
            )
            broad_text = _top_rank_text(
                broad,
                label_fn=lambda row: str(row["activity_label"]),
                metric_fn=lambda row: f"weak in {_format_num(row['weak_classroom_count'], digits=0)}/{_format_num(row['classrooms_observed'], digits=0)} classrooms",
            )
            if broad_text:
                findings.append(f"Broadest weak-classroom ranking: {broad_text}.")
        raw_successes = (
            int(raw["successes_total"])
            if "successes_total" in supported.columns
            else int(_approx_successes(raw.get("success_rate"), raw.get("attempts_total")) or 0)
        )
        total_successes = (
            int(supported["successes_total"].sum())
            if "successes_total" in supported.columns
            else int(
                sum(
                    _approx_successes(row.get("success_rate"), row.get("attempts_total")) or 0
                    for row in supported.to_dicts()
                )
            )
        )
        rest_successes = total_successes - raw_successes
        rest_attempts = int(supported["attempts_total"].sum()) - int(raw["attempts_total"])
        p_value = _two_proportion_p_value(
            raw_successes,
            raw.get("attempts_total"),
            rest_successes,
            rest_attempts,
        )
        if p_value is not None and rest_attempts > 0:
            rest_rate = rest_successes / rest_attempts
            findings.append(
                f"The weakest raw-impact activity sits {_format_pct((_safe_float(raw['success_rate']) or 0.0) - rest_rate)} below the rest of the supported scope; a two-proportion test gives {_format_p_value(p_value)}."
            )
    if len(findings) <= 3 and activities.height == 0:
        return _insufficient(["Classroom population analysis needs activity-level rows in the selected work-mode scope."])
    if top_three_share >= 0.55:
        interpretation = (
            "This scope looks concentrated in a smaller set of classrooms, so the strongest weak signals are plausibly carried by a few heavy-usage groups rather than evenly across the population."
        )
    elif supported.height > 0:
        interpretation = (
            "This scope looks broad enough for classroom-level pattern reading: the main weak activities recur across multiple classrooms rather than coming only from a single selected replay."
        )
    else:
        interpretation = (
            "The selected work-mode scope has enough classroom coverage to describe size and volume, but not enough repeated activity support for stronger cross-classroom claims."
        )
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_classroom_progression_sankey(
    payload: dict[str, Any] | None,
    *,
    visible_steps: int,
    start_step: int = 1,
) -> FigureAnalysis:
    if not payload:
        return _insufficient()
    student_paths = payload.get("student_paths") or []
    student_count = int(payload.get("student_count") or len(student_paths) or 0)
    if student_count <= 0 or not student_paths:
        return _insufficient(["Classroom Sankey analysis requires at least one valid student path in the selected classroom."])

    start_step_value = max(1, int(start_step))
    start_index = start_step_value - 1
    window_end_step = start_step_value + max(1, int(visible_steps)) - 1
    visible_rows = [
        row
        for row in student_paths
        if len(row.get("activity_full_labels") or []) >= start_step_value
    ]
    visible_student_count = len(visible_rows)
    if visible_student_count <= 0:
        return _insufficient(
            [f"No student in the selected classroom reaches step {start_step_value} in the displayed Sankey window."]
        )

    findings: list[str] = [
        f"The selected classroom includes {_format_num(student_count, digits=0)} students overall; {_format_num(visible_student_count, digits=0)} reach step {_format_num(start_step_value, digits=0)} and contribute to the displayed Sankey window."
    ]

    entry_counter: dict[str, int] = {}
    first_transition_counter: dict[tuple[str, str], int] = {}
    second_transition_counter: dict[tuple[str, str], int] = {}
    third_transition_counter: dict[tuple[str, str], int] = {}
    visible_path_counter: dict[tuple[str, ...], int] = {}
    path_lengths: list[int] = []

    def stop_label(count: int) -> str:
        return f"Stopped after {count} {'activity' if count == 1 else 'activities'}"

    for row in visible_rows:
        full_labels = [str(value) for value in row.get("activity_full_labels") or [] if str(value).strip()]
        if not full_labels:
            continue
        path_length = len(full_labels)
        window_labels = full_labels[start_index : start_index + visible_steps]
        if not window_labels:
            continue
        path_lengths.append(path_length)
        entry_counter[window_labels[0]] = entry_counter.get(window_labels[0], 0) + 1
        if len(window_labels) >= 2:
            key = (window_labels[0], window_labels[1])
            first_transition_counter[key] = first_transition_counter.get(key, 0) + 1
        if len(window_labels) >= 3:
            key = (window_labels[1], window_labels[2])
            second_transition_counter[key] = second_transition_counter.get(key, 0) + 1
        if len(window_labels) >= 4:
            key = (window_labels[2], window_labels[3])
            third_transition_counter[key] = third_transition_counter.get(key, 0) + 1
        terminal = (
            f"More than {window_end_step} activities"
            if path_length > window_end_step
            else stop_label(path_length)
        )
        visible_path = tuple(window_labels + [terminal])
        visible_path_counter[visible_path] = visible_path_counter.get(visible_path, 0) + 1

    if not path_lengths or not entry_counter:
        return _insufficient(
            [f"Classroom Sankey analysis needs at least one non-empty first-time activity path from step {start_step_value} onward."]
        )

    entry_label, entry_count = sorted(entry_counter.items(), key=lambda item: (-item[1], item[0]))[0]
    findings.append(
        f"The most common visible entry activity is {entry_label}, reached at step {_format_num(start_step_value, digits=0)} by {_format_num(entry_count, digits=0)}/{_format_num(visible_student_count, digits=0)} students ({_format_pct(entry_count / visible_student_count)} of the displayed cohort)."
    )

    def transition_summary(counter: dict[tuple[str, str], int], ordinal: str) -> str | None:
        if not counter:
            return None
        ranked = sorted(counter.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
        if ranked[0][1] < 3:
            return None
        parts = []
        for rank, ((source, target), count) in enumerate(ranked[:TOP_LIST_LIMIT], start=1):
            parts.append(
                f"{rank}. {source} -> {target} ({_format_num(count, digits=0)} students, {_format_pct(count / visible_student_count)})"
            )
        return f"Most common {ordinal} transitions: {'; '.join(parts)}."

    for ordinal, counter in (
        ("first", first_transition_counter),
        ("second", second_transition_counter),
        ("third", third_transition_counter),
    ):
        sentence = transition_summary(counter, ordinal)
        if sentence:
            findings.append(sentence)

    for stop_count in range(start_step_value, min(window_end_step, start_step_value + 2) + 1):
        share = sum(1 for value in path_lengths if value == stop_count) / visible_student_count
        findings.append(
            f"{_format_pct(share)} of students stop after {stop_count} {'activity' if stop_count == 1 else 'activities'}."
        )
    overflow_share = sum(1 for value in path_lengths if value > window_end_step) / visible_student_count
    findings.append(
        f"{_format_pct(overflow_share)} of students reach `More than {window_end_step} activities` in the displayed Sankey."
    )

    ranked_paths = sorted(visible_path_counter.items(), key=lambda item: (-item[1], item[0]))
    if ranked_paths:
        parts = []
        for rank, (path, count) in enumerate(ranked_paths[:TOP_LIST_LIMIT], start=1):
            parts.append(
                f"{rank}. {' -> '.join(path)} ({_format_num(count, digits=0)} students, {_format_pct(count / visible_student_count)})"
            )
        findings.append(f"Most common visible paths: {'; '.join(parts)}.")
        top_path_share = ranked_paths[0][1] / visible_student_count
    else:
        top_path_share = 0.0

    unique_paths = len(visible_path_counter)
    if top_path_share >= 0.50:
        interpretation = (
            "The classroom flow is concentrated around one dominant visible pathway, so most students appear to share a common progression route through the selected scope."
        )
    elif unique_paths >= max(5, ceil(student_count / 2)):
        interpretation = (
            "The classroom flow looks fragmented: students spread across many distinct visible paths rather than aligning on one shared activity sequence."
        )
    else:
        interpretation = (
            "The classroom flow is mixed: there is some shared structure, but several alternative visible pathways remain active in the selected scope."
        )
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_student_elo_page(payload: dict[str, Any] | None) -> FigureAnalysis:
    if not payload:
        return _insufficient()
    series = payload.get('series') or {}
    student_ids = [str(user_id) for user_id in payload.get('student_ids') or [] if str(user_id).strip()]
    if not student_ids:
        return _insufficient()
    findings: list[str] = []
    negative_events: list[dict[str, Any]] = []
    volatilities: list[tuple[str, float]] = []
    total_events = 0
    for user_id in student_ids:
        bucket = series.get(user_id)
        if not isinstance(bucket, dict):
            continue
        post = [float(value) for value in bucket.get('student_elo_post') or []]
        pre = [float(value) for value in bucket.get('student_elo_pre') or []]
        activities = [str(value or 'Unknown') for value in bucket.get('activity_label') or []]
        modules = [str(value or 'Unknown') for value in bucket.get('module_label') or []]
        exercises = [str(value or 'Unknown') for value in bucket.get('exercise_id') or []]
        if not post or not pre:
            continue
        total_events += len(post)
        findings.append(f"{user_id} changes by {_format_num(post[-1] - pre[0], digits=1)} Elo across {_format_num(len(post), digits=0)} displayed attempts.")
        deltas = [post[idx] - pre[idx] for idx in range(min(len(post), len(pre)))]
        volatilities.append((user_id, mean(abs(delta) for delta in deltas) if deltas else 0.0))
        for idx, delta in enumerate(deltas):
            negative_events.append({'user_id': user_id, 'delta': float(delta), 'activity': activities[idx], 'module': modules[idx], 'exercise': exercises[idx]})
    if not findings:
        return _insufficient()
    if volatilities:
        most_volatile = sorted(volatilities, key=lambda item: (-item[1], item[0]))[0]
        findings.append(f"{most_volatile[0]} has the most volatile displayed trajectory, with an average absolute Elo move of {_format_num(most_volatile[1], digits=1)} points per attempt.")
    negative_rank = sorted(negative_events, key=lambda row: (row['delta'], row['activity'], row['exercise']))
    if negative_rank and negative_rank[0]['delta'] < 0:
        worst = negative_rank[0]
        findings.append(f"The largest negative Elo move in the displayed sample occurs on {worst['activity']} ({worst['exercise']}) with a drop of {_format_num(worst['delta'], digits=1)} points.")
        top_negative = [row for row in negative_rank if row['delta'] < 0][:5]
        activity_counts: dict[str, int] = {}
        module_counts: dict[str, int] = {}
        for row in top_negative:
            activity_counts[row['activity']] = activity_counts.get(row['activity'], 0) + 1
            module_counts[row['module']] = module_counts.get(row['module'], 0) + 1
        top_activity, top_activity_count = sorted(activity_counts.items(), key=lambda item: (-item[1], item[0]))[0]
        top_module, top_module_count = sorted(module_counts.items(), key=lambda item: (-item[1], item[0]))[0]
        if top_activity_count >= 2:
            findings.append(f"Negative Elo moves cluster most clearly around {top_activity} in the displayed sample.")
            interpretation = f"In this small displayed sample, the sharpest Elo drops cluster around {top_activity}. A plausible explanation is that this activity contains exercises that are harder than the current student estimate."
        elif top_module_count >= 2:
            findings.append(f"Negative Elo moves cluster most clearly inside {top_module} in the displayed sample.")
            interpretation = f"In this displayed sample, the sharpest Elo drops recur inside {top_module}. A plausible explanation is that this module region is more demanding than its neighbors for these students."
        else:
            interpretation = 'The displayed Elo drops are spread across different activities, so there is no single clear offending content area in this sample.'
    else:
        interpretation = 'The displayed sample does not contain a meaningful negative Elo move, so the main story is gradual progression rather than sharp setbacks.'
    caveats: list[str] = []
    if total_events < RATE_MIN_OBSERVATIONS:
        caveats.append('This analysis is sample-level and descriptive: the displayed students do not provide enough events for population-level claims.')
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation, caveats=tuple(caveats))


def analyze_student_elo_population(
    activity_summary: pl.DataFrame | pd.DataFrame | None,
    region_summary: pl.DataFrame | pd.DataFrame | None,
    eligible_profiles: pl.DataFrame | pd.DataFrame | None,
) -> FigureAnalysis:
    activities = _as_polars(activity_summary)
    regions = _as_polars(region_summary)
    profiles = _as_polars(eligible_profiles)
    if activities.height == 0 or profiles.height == 0:
        return _insufficient()
    supported = activities.filter(pl.col("event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS)
    if supported.height == 0:
        return _insufficient(["Population-level Elo analysis is only surfaced when an activity has at least 20 Elo events."])
    findings = [
        f"The eligible Elo population contains {_format_num(profiles.height, digits=0)} students across {_format_num(int(supported['event_count'].sum()), digits=0)} supported Elo events."
    ]
    raw_candidates = supported.filter(pl.col("total_negative_loss") > 0)
    raw = (raw_candidates if raw_candidates.height > 0 else supported).sort(
        ["total_negative_loss", "event_count", "module_code", "activity_label"],
        descending=[True, True, False, False],
    ).row(0, named=True)
    findings.append(
        f"The largest raw negative Elo impact is on {_module_activity_label(raw)}: mean Elo change is {_format_num(_safe_float(raw['mean_delta']), digits=1)} across {_format_num(raw['event_count'])} events, with {_format_num(_safe_float(raw['total_negative_loss']), digits=1)} total negative Elo points lost."
    )
    raw_p_value = _mean_p_value(raw.get("mean_delta"), raw.get("std_delta"), raw.get("event_count"))
    if raw_p_value is not None:
        findings.append(
            f"For that raw-impact activity, the mean Elo change is unlikely to be explained by random fluctuation alone ({_format_p_value(raw_p_value)} with a one-sample normal approximation)."
        )
    normalized_candidates = supported.filter(pl.col("mean_delta").is_not_null() & (pl.col("mean_delta") < 0))
    normalized = (normalized_candidates if normalized_candidates.height > 0 else supported.filter(pl.col("mean_delta").is_not_null())).sort(
        ["mean_delta", "event_count", "module_code", "activity_label"],
        descending=[False, True, False, False],
    ).row(0, named=True)
    findings.append(
        f"The strongest normalized negative effect is on {_module_activity_label(normalized)} at {_format_num(_safe_float(normalized['mean_delta']), digits=1)} Elo per event, which avoids letting the busiest modules dominate the ranking."
    )
    normalized_p_value = _mean_p_value(
        normalized.get("mean_delta"),
        normalized.get("std_delta"),
        normalized.get("event_count"),
    )
    if normalized_p_value is not None:
        findings.append(
            f"For that normalized-severity activity, the mean Elo drop is statistically clear relative to zero ({_format_p_value(normalized_p_value)})."
        )
    raw_text = _top_rank_text(
        (raw_candidates if raw_candidates.height > 0 else supported).sort(
            ["total_negative_loss", "event_count", "module_code", "activity_label"],
            descending=[True, True, False, False],
        ),
        label_fn=_module_activity_label,
        metric_fn=lambda row: f"total negative loss {_format_num(_safe_float(row['total_negative_loss']), digits=1)}",
    )
    if raw_text:
        findings.append(f"Highest raw negative-Elo impact: {raw_text}.")
    normalized_text = _top_rank_text(
        (normalized_candidates if normalized_candidates.height > 0 else supported.filter(pl.col("mean_delta").is_not_null())).sort(
            ["mean_delta", "event_count", "module_code", "activity_label"],
            descending=[False, True, False, False],
        ),
        label_fn=_module_activity_label,
        metric_fn=lambda row: f"mean Elo change {_format_num(_safe_float(row['mean_delta']), digits=1)}",
    )
    if normalized_text:
        findings.append(f"Strongest normalized Elo drops: {normalized_text}.")
    volatile = supported.sort(
        ["mean_abs_delta", "event_count", "module_code", "activity_label"],
        descending=[True, True, False, False],
    ).row(0, named=True)
    findings.append(
        f"The biggest volatility hotspot is {_module_activity_label(volatile)} with an average absolute Elo move of {_format_num(_safe_float(volatile['mean_abs_delta']), digits=1)} points per event."
    )
    volatility_text = _top_rank_text(
        supported.sort(
            ["mean_abs_delta", "event_count", "module_code", "activity_label"],
            descending=[True, True, False, False],
        ),
        label_fn=_module_activity_label,
        metric_fn=lambda row: f"mean absolute move {_format_num(_safe_float(row['mean_abs_delta']), digits=1)}",
    )
    if volatility_text:
        findings.append(f"Highest volatility hotspots: {volatility_text}.")
    supported_regions = regions.filter(pl.col("event_count").fill_null(0) >= RATE_MIN_OBSERVATIONS) if regions.height > 0 else pl.DataFrame()
    if supported_regions.height > 0:
        top_region = supported_regions.sort(
            ["total_negative_loss", "event_count", "module_code", "objective_label"],
            descending=[True, True, False, False],
        ).row(0, named=True)
        region_label = _label(top_region, objective_key="module_label", activity_key="objective_label")
        findings.append(
            f"Negative Elo impact clusters most strongly in {region_label}, which accumulates {_format_num(_safe_float(top_region['total_negative_loss']), digits=1)} total negative Elo points across {_format_num(top_region['event_count'])} events."
        )
        region_text = _top_rank_text(
            supported_regions.sort(
                ["total_negative_loss", "event_count", "module_code", "objective_label"],
                descending=[True, True, False, False],
            ),
            label_fn=lambda row: _label(row, objective_key="module_label", activity_key="objective_label"),
            metric_fn=lambda row: f"total negative loss {_format_num(_safe_float(row['total_negative_loss']), digits=1)}",
        )
        if region_text:
            findings.append(f"Objective regions with the largest negative Elo accumulation: {region_text}.")
    same_module = str(raw.get("module_code") or "") == str(normalized.get("module_code") or "")
    if same_module:
        interpretation = (
            f"Both the highest raw-impact and highest normalized Elo drops point toward {raw.get('module_label') or raw.get('module_code')}, which plausibly means this module region combines traffic with genuinely difficult content rather than only volume."
        )
    else:
        interpretation = (
            "The page-wide Elo story splits between impact and severity: one region carries the most negative volume, while another produces sharper per-event drops. That pattern plausibly means traffic alone is not the whole explanation."
        )
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_student_elo_comparison(
    comparison_payload: dict[str, Any] | None,
    exercise_comparison: pl.DataFrame | pd.DataFrame | None,
    eligible_profiles: pl.DataFrame | pd.DataFrame | None,
) -> FigureAnalysis:
    payload = comparison_payload or {}
    systems = list(payload.get("systems") or [])
    series = payload.get("series") or {}
    student_ids = [str(user_id) for user_id in payload.get("student_ids") or [] if str(user_id).strip()]
    exercise_frame = _as_polars(exercise_comparison)
    profiles = _as_polars(eligible_profiles)
    if len(systems) < 2 or not student_ids or exercise_frame.height == 0:
        return _insufficient()

    current_key, iterative_key = systems[:2]
    findings: list[str] = []

    gap_summaries: list[tuple[str, float, float, float]] = []
    for user_id in student_ids:
        current_series = ((series.get(current_key) or {}).get(user_id)) or {}
        iterative_series = ((series.get(iterative_key) or {}).get(user_id)) or {}
        current_post = [float(value) for value in current_series.get("student_elo_post") or []]
        iterative_post = [float(value) for value in iterative_series.get("student_elo_post") or []]
        if not current_post or len(current_post) != len(iterative_post):
            continue
        gaps = [iterative_post[idx] - current_post[idx] for idx in range(len(current_post))]
        mean_abs_gap = mean(abs(gap) for gap in gaps) if gaps else 0.0
        cut = max(1, len(gaps) // 3)
        early_abs_gap = mean(abs(gap) for gap in gaps[:cut]) if gaps[:cut] else 0.0
        late_abs_gap = mean(abs(gap) for gap in gaps[-cut:]) if gaps[-cut:] else 0.0
        final_gap = gaps[-1]
        gap_summaries.append((user_id, final_gap, mean_abs_gap, late_abs_gap - early_abs_gap))
        findings.append(
            f"For {user_id}, final Elo is {_format_num(current_post[-1], digits=1)} in the current system and {_format_num(iterative_post[-1], digits=1)} in the iterative system, a final gap of {_format_num(final_gap, digits=1)} points."
        )
        findings.append(
            f"For {user_id}, the two trajectories differ by {_format_num(mean_abs_gap, digits=1)} Elo points on average across the replayed history."
        )
        if late_abs_gap > early_abs_gap + 1.0:
            findings.append(
                f"For {user_id}, the old-vs-new gap widens later in the trajectory, which suggests the two calibrations diverge more after the early burn-in period."
            )
        elif early_abs_gap > late_abs_gap + 1.0:
            findings.append(
                f"For {user_id}, the old-vs-new gap is larger early than late, which suggests the two systems disagree most during the initial convergence phase."
            )

    if not findings:
        return _insufficient()

    supported_exercises = exercise_frame.filter(pl.col("calibrated").fill_null(False))
    if supported_exercises.height == 0:
        return FigureAnalysis(findings=tuple(findings), caveats=(INSUFFICIENT_EVIDENCE_MESSAGE,))

    correlation = (
        supported_exercises.select(
            pl.corr("current_exercise_elo", "iterative_exercise_elo").alias("elo_correlation")
        ).item()
        if supported_exercises.height > 1
        else None
    )
    median_abs_shift = supported_exercises.select(pl.col("abs_elo_diff").median().alias("median_abs_shift")).item()
    findings.append(
        f"Across {_format_num(supported_exercises.height, digits=0)} calibrated exercises, the current-versus-iterative difficulty correlation is {_format_num(_safe_float(correlation), digits=3)} and the median absolute item shift is {_format_num(_safe_float(median_abs_shift), digits=1)} Elo points."
    )

    shift_text = _top_rank_text(
        supported_exercises.sort(
            ["abs_elo_diff", "calibration_attempts", "module_code", "exercise_label"],
            descending=[True, True, False, False],
        ),
        label_fn=lambda row: f"{row.get('exercise_label') or row.get('exercise_id')} ({row.get('module_code') or 'unknown module'})",
        metric_fn=lambda row: f"shift {_format_num(_safe_float(row.get('elo_diff')), digits=1)}",
    )
    if shift_text:
        findings.append(f"Largest exercise-level shifts between the two systems: {shift_text}.")

    if supported_exercises.height >= RATE_MIN_OBSERVATIONS:
        quantiles = supported_exercises.select(
            [
                pl.col("calibration_attempts").quantile(0.25).alias("q25"),
                pl.col("calibration_attempts").quantile(0.75).alias("q75"),
            ]
        ).to_dicts()[0]
        q25 = max(1, int(_safe_float(quantiles.get("q25")) or 1))
        q75 = max(q25, int(_safe_float(quantiles.get("q75")) or q25))
        sparse = supported_exercises.filter(pl.col("calibration_attempts") <= q25)
        dense = supported_exercises.filter(pl.col("calibration_attempts") >= q75)
        if sparse.height > 0 and dense.height > 0:
            sparse_shift = _safe_float(
                sparse.select(pl.col("abs_elo_diff").median().alias("median_shift")).item()
            )
            dense_shift = _safe_float(
                dense.select(pl.col("abs_elo_diff").median().alias("median_shift")).item()
            )
            if sparse_shift is not None and dense_shift is not None:
                if sparse_shift > dense_shift + 1.0:
                    findings.append(
                        f"The bigger old-vs-new item shifts are concentrated in lower-exposure exercises: the sparse quartile has a median shift of {_format_num(sparse_shift, digits=1)} Elo versus {_format_num(dense_shift, digits=1)} for the high-exposure quartile."
                    )
                elif dense_shift > sparse_shift + 1.0:
                    findings.append(
                        f"The bigger old-vs-new item shifts are concentrated in high-exposure exercises: the high-exposure quartile has a median shift of {_format_num(dense_shift, digits=1)} Elo versus {_format_num(sparse_shift, digits=1)} for the sparse quartile."
                    )
                else:
                    findings.append(
                        f"Old-vs-new item shifts are similar in sparse and high-exposure exercises, with median absolute shifts of {_format_num(sparse_shift, digits=1)} and {_format_num(dense_shift, digits=1)} Elo respectively."
                    )

    if profiles.height > 0:
        findings.insert(
            0,
            f"The comparison page covers {_format_num(profiles.height, digits=0)} replay-eligible students that are available in both Elo systems.",
        )

    if gap_summaries:
        strongest_gap = sorted(gap_summaries, key=lambda item: (-abs(item[1]), item[0]))[0]
        interpretation = (
            f"The comparison mostly isolates how the item calibration changes the replay: {strongest_gap[0]} shows the largest final gap, while the page-wide item differences are summarized by a median absolute shift of {_format_num(_safe_float(median_abs_shift), digits=1)} Elo."
        )
    else:
        interpretation = (
            "The comparison isolates the item-calibration choice: both systems replay the same histories, so any trajectory difference comes from how exercise difficulty is estimated."
        )
    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)


def analyze_student_objective_spider(
    summary: pl.DataFrame | pd.DataFrame | None,
    *,
    student_id: str,
    module_code: str,
    module_label: str | None = None,
    total_attempts: int | None = None,
) -> FigureAnalysis:
    frame = _as_polars(summary)
    if frame.height == 0:
        return _insufficient(["Objective-level metrics are unavailable for the selected student/module."])

    module_display = str(module_label or module_code or "").strip() or str(module_code or "").strip()
    attempted = frame.filter(pl.col("has_attempts").fill_null(False))
    findings: list[str] = []
    if total_attempts is not None:
        findings.append(
            f"{student_id} has {_format_num(total_attempts, digits=0)} total attempts overall, and {_format_num(attempted.height, digits=0)} of {_format_num(frame.height, digits=0)} objectives are touched in {module_display}."
        )
    else:
        findings.append(
            f"{student_id} touches {_format_num(attempted.height, digits=0)} of {_format_num(frame.height, digits=0)} objectives in {module_display}."
        )

    untouched = frame.height - attempted.height
    if untouched > 0:
        findings.append(
            f"{_format_num(untouched, digits=0)} objectives remain untouched in the selected module, so the radar still shows visible gaps in breadth."
        )

    def _objective_name(row: dict[str, Any]) -> str:
        code = str(row.get("objective_code") or row.get("objective_id") or "").strip()
        label = str(row.get("objective_label") or code).strip() or code
        return f"{code} ({label})" if code and label and label != code else (code or label or "Unknown objective")

    attempted_success = attempted.filter(pl.col("success_rate_all_attempts").is_not_null())
    if attempted_success.height > 0:
        strongest_success = attempted_success.sort(
            ["success_rate_all_attempts", "attempts", "objective_order"],
            descending=[True, True, False],
        ).row(0, named=True)
        weakest_success = attempted_success.sort(
            ["success_rate_all_attempts", "attempts", "objective_order"],
            descending=[False, True, False],
        ).row(0, named=True)
        findings.append(
            f"Strongest success is {_objective_name(strongest_success)} at {_format_pct(_safe_float(strongest_success.get('success_rate_all_attempts')))} across {_format_num(strongest_success.get('attempts'), digits=0)} attempts."
        )
        findings.append(
            f"Weakest success is {_objective_name(weakest_success)} at {_format_pct(_safe_float(weakest_success.get('success_rate_all_attempts')))} across {_format_num(weakest_success.get('attempts'), digits=0)} attempts."
        )

    coverage_supported = frame.filter(pl.col("objective_exercise_total") > 0)
    if coverage_supported.height > 0:
        strongest_coverage = coverage_supported.sort(
            ["coverage_rate", "distinct_exercises_attempted", "objective_order"],
            descending=[True, True, False],
        ).row(0, named=True)
        weakest_coverage = coverage_supported.sort(
            ["coverage_rate", "objective_order"],
            descending=[False, False],
        ).row(0, named=True)
        findings.append(
            f"Broadest coverage is {_objective_name(strongest_coverage)} with {_format_num(strongest_coverage.get('distinct_exercises_attempted'), digits=0)}/{_format_num(strongest_coverage.get('objective_exercise_total'), digits=0)} exercises reached ({_format_pct(_safe_float(strongest_coverage.get('coverage_rate')))})."
        )
        findings.append(
            f"Thinnest coverage is {_objective_name(weakest_coverage)} with {_format_num(weakest_coverage.get('distinct_exercises_attempted'), digits=0)}/{_format_num(weakest_coverage.get('objective_exercise_total'), digits=0)} exercises reached ({_format_pct(_safe_float(weakest_coverage.get('coverage_rate')))})."
        )

    interpretation = "The selected module shows an uneven objective profile."
    if attempted_success.height > 1:
        median_success = _safe_float(
            attempted_success.select(pl.col("success_rate_all_attempts").median().alias("median_success")).item()
        )
        median_coverage = _safe_float(
            coverage_supported.select(pl.col("coverage_rate").median().alias("median_coverage")).item()
        )
        high_coverage_low_success = (
            attempted_success.filter(
                (pl.col("coverage_rate") >= (median_coverage if median_coverage is not None else 0.0))
                & (
                    pl.col("success_rate_all_attempts")
                    < (median_success if median_success is not None else 0.0)
                )
            )
            .sort(
                ["coverage_rate", "success_rate_all_attempts", "objective_order"],
                descending=[True, False, False],
            )
        )
        low_coverage_high_success = (
            attempted_success.filter(
                (pl.col("coverage_rate") < (median_coverage if median_coverage is not None else 0.0))
                & (
                    pl.col("success_rate_all_attempts")
                    >= (median_success if median_success is not None else 0.0)
                )
            )
            .sort(
                ["success_rate_all_attempts", "coverage_rate", "objective_order"],
                descending=[True, False, False],
            )
        )
        if high_coverage_low_success.height > 0:
            row = high_coverage_low_success.row(0, named=True)
            findings.append(
                f"{_objective_name(row)} stands out as high-coverage but below the student's module-median success, which can indicate persistence without equally strong mastery yet."
            )
            interpretation = (
                "Coverage is broader than performance in at least one visible objective, so the student appears to revisit some content without equally strong accuracy."
            )
        elif low_coverage_high_success.height > 0:
            row = low_coverage_high_success.row(0, named=True)
            findings.append(
                f"{_objective_name(row)} stands out as low-coverage but above the student's module-median success, which suggests good local performance in a still-narrow slice."
            )
            interpretation = (
                "The student performs well where exposed, but objective coverage remains selective rather than broad across the module."
            )
        elif untouched >= max(1, frame.height // 2):
            interpretation = (
                "The profile is dominated by breadth gaps: many objectives remain untouched, so the radar is more informative about coverage than mastery across the full module."
            )
        else:
            interpretation = (
                "Coverage and success look relatively aligned across the attempted objectives, with the main differences coming from how far the student has spread across the module."
            )
    elif untouched >= max(1, frame.height // 2):
        interpretation = (
            "Most of the visible signal comes from what the student has not touched yet, so this module view is primarily a breadth profile."
        )

    return FigureAnalysis(findings=tuple(findings), interpretation=interpretation)

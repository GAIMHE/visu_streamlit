"""Calibrate and compare exercise Elo separately by work mode.

The current exercise Elo is module-local and context-specific: an item is
identified by module, objective, activity, and raw exercise id.  This module
reuses that exact calibration method on playlist-only and ZPDES-only attempts,
then compares contexts calibrated in both modes.

Because every module-local calibration is centered on a mean item Elo of 1500,
the item sets available in each mode can create an arbitrary offset.  The
comparison therefore reports both raw Elo values and values re-centered on the
shared contexts within each module.  The latter is the primary invariance
diagnostic.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import numpy as np
import pandas as pd
import polars as pl

from visu2.config import Settings
from visu2.derive import build_agg_exercise_elo_from_fact
from visu2.derive_common import ELO_BASE_RATING

WORK_MODES = ("playlist", "zpdes")
CONTEXT_COLUMNS = ["module_code", "objective_id", "activity_id", "exercise_id"]
LABEL_COLUMNS = [
    "exercise_label",
    "exercise_type",
    "module_id",
    "module_label",
    "objective_label",
    "activity_label",
]


def _as_lazy(frame: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def _as_pandas(frame: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame.to_pandas()
    return frame.copy()


def calibrate_exercise_elo_by_work_mode(
    fact: pl.DataFrame | pl.LazyFrame,
    settings: Settings,
    *,
    work_modes: Iterable[str] = WORK_MODES,
) -> dict[str, pl.DataFrame]:
    """Run the current exercise-Elo calibration independently in each mode."""

    lazy_fact = _as_lazy(fact)
    if "work_mode" not in lazy_fact.collect_schema().names():
        raise ValueError("fact must contain a work_mode column")

    normalized_modes = tuple(dict.fromkeys(str(mode).strip().lower() for mode in work_modes))
    if not normalized_modes or any(not mode for mode in normalized_modes):
        raise ValueError("work_modes must contain at least one non-empty value")

    mode_expr = pl.col("work_mode").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return {
        mode: build_agg_exercise_elo_from_fact(
            lazy_fact.filter(mode_expr == mode),
            settings,
        )
        for mode in normalized_modes
    }


def _prepare_calibrated_table(
    frame: pd.DataFrame | pl.DataFrame,
    suffix: str,
    *,
    keep_labels: bool = False,
) -> pd.DataFrame:
    table = _as_pandas(frame)
    required = {*CONTEXT_COLUMNS, "exercise_elo", "calibration_attempts", "calibrated"}
    missing = required.difference(table.columns)
    if missing:
        raise ValueError(f"{suffix} Elo table is missing columns: {sorted(missing)}")

    table = table.loc[table["calibrated"].fillna(False).astype(bool)].copy()
    if table.duplicated(CONTEXT_COLUMNS).any():
        raise ValueError(f"{suffix} Elo table has duplicate exercise contexts")

    measurement_columns = [
        "exercise_elo",
        "calibration_attempts",
        "calibration_success_rate",
    ]
    available_measurements = [column for column in measurement_columns if column in table]
    columns = [*CONTEXT_COLUMNS, *available_measurements]
    if keep_labels:
        columns.extend(column for column in LABEL_COLUMNS if column in table)
    table = table.loc[:, columns]
    return table.rename(columns={column: f"{column}_{suffix}" for column in available_measurements})


def align_elo_columns_within_module(
    frame: pd.DataFrame,
    elo_columns: Iterable[str],
    *,
    output_suffix: str = "_aligned",
    target_mean: float = ELO_BASE_RATING,
) -> pd.DataFrame:
    """Re-center Elo columns on the contexts present in ``frame`` per module."""

    if "module_code" not in frame:
        raise ValueError("comparison frame must contain module_code")
    columns = tuple(elo_columns)
    missing = set(columns).difference(frame.columns)
    if missing:
        raise ValueError(f"comparison frame is missing Elo columns: {sorted(missing)}")

    aligned = frame.copy()
    for column in columns:
        values = pd.to_numeric(aligned[column], errors="coerce")
        module_mean = values.groupby(aligned["module_code"], dropna=False).transform("mean")
        aligned[f"{column}{output_suffix}"] = values - module_mean + float(target_mean)
    return aligned


def build_matched_work_mode_elo(
    all_mode_elo: pd.DataFrame | pl.DataFrame,
    mode_elos: Mapping[str, pd.DataFrame | pl.DataFrame],
    *,
    left_mode: str = "playlist",
    right_mode: str = "zpdes",
) -> pd.DataFrame:
    """Match contexts calibrated in both modes and add comparable Elo scales.

    ``elo_difference_raw`` and ``elo_difference_aligned`` are always right
    minus left.  Positive values therefore mean that the context was estimated
    as harder in ``right_mode``.
    """

    if left_mode not in mode_elos or right_mode not in mode_elos:
        raise ValueError(f"mode_elos must contain {left_mode!r} and {right_mode!r}")

    left = _prepare_calibrated_table(mode_elos[left_mode], left_mode)
    right = _prepare_calibrated_table(mode_elos[right_mode], right_mode)
    all_table = _prepare_calibrated_table(all_mode_elo, "all", keep_labels=True)

    matched = left.merge(
        right,
        on=CONTEXT_COLUMNS,
        how="inner",
        validate="one_to_one",
    ).merge(
        all_table,
        on=CONTEXT_COLUMNS,
        how="left",
        validate="one_to_one",
    )

    left_elo = f"exercise_elo_{left_mode}"
    right_elo = f"exercise_elo_{right_mode}"
    left_attempts = f"calibration_attempts_{left_mode}"
    right_attempts = f"calibration_attempts_{right_mode}"
    numeric_columns = [left_elo, right_elo, left_attempts, right_attempts]
    for column in numeric_columns:
        matched[column] = pd.to_numeric(matched[column], errors="coerce")
    matched = matched.dropna(subset=numeric_columns).copy()

    elo_columns = [
        f"exercise_elo_{suffix}"
        for suffix in (left_mode, right_mode, "all")
        if f"exercise_elo_{suffix}" in matched
    ]
    matched = align_elo_columns_within_module(matched, elo_columns)

    matched["elo_difference_raw"] = matched[right_elo] - matched[left_elo]
    matched["elo_difference_aligned"] = (
        matched[f"exercise_elo_{right_mode}_aligned"] - matched[f"exercise_elo_{left_mode}_aligned"]
    )
    matched["elo_mean_raw"] = 0.5 * (matched[right_elo] + matched[left_elo])
    matched["elo_mean_aligned"] = 0.5 * (
        matched[f"exercise_elo_{right_mode}_aligned"] + matched[f"exercise_elo_{left_mode}_aligned"]
    )
    matched["absolute_difference_raw"] = matched["elo_difference_raw"].abs()
    matched["absolute_difference_aligned"] = matched["elo_difference_aligned"].abs()
    matched["min_mode_attempts"] = matched[[left_attempts, right_attempts]].min(axis=1)
    matched["harmonic_mode_attempts"] = 2.0 / (
        (1.0 / matched[left_attempts]) + (1.0 / matched[right_attempts])
    )
    matched["attempt_log_ratio"] = np.log(
        (matched[right_attempts] + 1.0) / (matched[left_attempts] + 1.0)
    )

    sort_columns = ["module_code", "objective_id", "activity_id", "exercise_id"]
    return matched.sort_values(sort_columns, kind="stable", na_position="last").reset_index(
        drop=True
    )


def lin_concordance_correlation(left: pd.Series, right: pd.Series) -> float:
    """Return Lin's concordance correlation coefficient for paired values."""

    values = pd.concat(
        [pd.to_numeric(left, errors="coerce"), pd.to_numeric(right, errors="coerce")],
        axis=1,
    ).dropna()
    if len(values) < 2:
        return float("nan")
    x = values.iloc[:, 0].to_numpy(dtype=float)
    y = values.iloc[:, 1].to_numpy(dtype=float)
    denominator = np.var(x) + np.var(y) + (np.mean(x) - np.mean(y)) ** 2
    if denominator == 0.0:
        return 1.0 if np.array_equal(x, y) else float("nan")
    covariance = float(np.mean((x - np.mean(x)) * (y - np.mean(y))))
    return float(2.0 * covariance / denominator)


def _safe_correlation(left: pd.Series, right: pd.Series, method: str) -> float:
    if len(left) < 2 or left.nunique(dropna=True) < 2 or right.nunique(dropna=True) < 2:
        return float("nan")
    return float(left.corr(right, method=method))


def summarize_elo_pair(
    frame: pd.DataFrame,
    *,
    left_column: str,
    right_column: str,
    thresholds: Iterable[int] = (1, 10, 25, 50, 100),
    support_column: str = "min_mode_attempts",
    weight_column: str = "harmonic_mode_attempts",
    comparison: str,
    scale: str,
    align_within_module: bool = False,
) -> pd.DataFrame:
    """Summarize agreement across support thresholds for one Elo pair."""

    required = {left_column, right_column, support_column, weight_column}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"comparison frame is missing columns: {sorted(missing)}")

    rows: list[dict[str, float | int | str]] = []
    for raw_threshold in thresholds:
        threshold = int(raw_threshold)
        if threshold < 1:
            raise ValueError("thresholds must be positive integers")
        subset = frame.loc[
            pd.to_numeric(frame[support_column], errors="coerce") >= threshold
        ].copy()
        subset[left_column] = pd.to_numeric(subset[left_column], errors="coerce")
        subset[right_column] = pd.to_numeric(subset[right_column], errors="coerce")
        subset[weight_column] = pd.to_numeric(subset[weight_column], errors="coerce")
        subset = subset.dropna(subset=[left_column, right_column])

        if subset.empty:
            rows.append(
                {
                    "comparison": comparison,
                    "scale": scale,
                    "min_attempts_per_mode": threshold,
                    "contexts": 0,
                    "modules": 0,
                }
            )
            continue

        left = subset[left_column]
        right = subset[right_column]
        if align_within_module:
            left = left - left.groupby(subset["module_code"], dropna=False).transform("mean")
            right = right - right.groupby(subset["module_code"], dropna=False).transform("mean")
        difference = right - left
        absolute_difference = difference.abs()
        weights = subset[weight_column].where(subset[weight_column] > 0)
        valid_weights = weights.notna()
        if valid_weights.any():
            weighted_mean_difference = float(
                np.average(difference.loc[valid_weights], weights=weights.loc[valid_weights])
            )
            weighted_mae = float(
                np.average(
                    absolute_difference.loc[valid_weights],
                    weights=weights.loc[valid_weights],
                )
            )
        else:
            weighted_mean_difference = float("nan")
            weighted_mae = float("nan")

        rows.append(
            {
                "comparison": comparison,
                "scale": scale,
                "min_attempts_per_mode": threshold,
                "contexts": int(len(subset)),
                "modules": int(subset["module_code"].nunique(dropna=True)),
                "mean_difference": float(difference.mean()),
                "median_difference": float(difference.median()),
                "sd_difference": float(difference.std(ddof=1)),
                "mean_absolute_difference": float(absolute_difference.mean()),
                "median_absolute_difference": float(absolute_difference.median()),
                "rmse": float(np.sqrt(np.mean(np.square(difference)))),
                "p90_absolute_difference": float(absolute_difference.quantile(0.90)),
                "p95_absolute_difference": float(absolute_difference.quantile(0.95)),
                "within_25_elo_percent": float((absolute_difference <= 25.0).mean() * 100.0),
                "within_50_elo_percent": float((absolute_difference <= 50.0).mean() * 100.0),
                "within_100_elo_percent": float((absolute_difference <= 100.0).mean() * 100.0),
                "pearson_correlation": _safe_correlation(left, right, "pearson"),
                "spearman_correlation": _safe_correlation(left, right, "spearman"),
                "lin_concordance": lin_concordance_correlation(left, right),
                "weighted_mean_difference": weighted_mean_difference,
                "weighted_mean_absolute_difference": weighted_mae,
            }
        )
    return pd.DataFrame(rows)


def build_work_mode_agreement_metrics(
    matched: pd.DataFrame,
    *,
    thresholds: Iterable[int] = (1, 10, 25, 50, 100),
    left_mode: str = "playlist",
    right_mode: str = "zpdes",
) -> pd.DataFrame:
    """Return raw and shared-item-aligned agreement metrics by support level."""

    tables = []
    comparison = f"{right_mode}_minus_{left_mode}"
    for scale, align_within_module in (
        ("raw", False),
        ("shared_module_aligned", True),
    ):
        tables.append(
            summarize_elo_pair(
                matched,
                left_column=f"exercise_elo_{left_mode}",
                right_column=f"exercise_elo_{right_mode}",
                thresholds=thresholds,
                comparison=comparison,
                scale=scale,
                align_within_module=align_within_module,
            )
        )
    return pd.concat(tables, ignore_index=True)


def build_calibration_coverage(
    all_mode_elo: pd.DataFrame | pl.DataFrame,
    mode_elos: Mapping[str, pd.DataFrame | pl.DataFrame],
) -> pd.DataFrame:
    """Summarize calibrated contexts and first-attempt support in every fit."""

    rows: list[dict[str, float | int | str]] = []
    tables: dict[str, pd.DataFrame | pl.DataFrame] = {"all": all_mode_elo, **mode_elos}
    for label, frame in tables.items():
        table = _prepare_calibrated_table(frame, label)
        attempts = pd.to_numeric(table[f"calibration_attempts_{label}"], errors="coerce")
        rows.append(
            {
                "calibration": label,
                "calibrated_contexts": int(len(table)),
                "modules": int(table["module_code"].nunique(dropna=True)),
                "first_attempts": int(attempts.sum()),
                "median_attempts_per_context": float(attempts.median()),
                "p10_attempts_per_context": float(attempts.quantile(0.10)),
                "p90_attempts_per_context": float(attempts.quantile(0.90)),
            }
        )
    return pd.DataFrame(rows)


def build_module_agreement_summary(
    matched: pd.DataFrame,
    *,
    min_attempts_per_mode: int = 25,
) -> pd.DataFrame:
    """Summarize aligned work-mode divergence separately within each module."""

    eligible = matched.loc[matched["min_mode_attempts"] >= min_attempts_per_mode].copy()
    eligible = align_elo_columns_within_module(
        eligible,
        ["exercise_elo_playlist", "exercise_elo_zpdes"],
    )
    eligible["elo_difference_aligned"] = (
        eligible["exercise_elo_zpdes_aligned"] - eligible["exercise_elo_playlist_aligned"]
    )
    rows: list[dict[str, float | int | str]] = []
    for module_code, group in eligible.groupby("module_code", dropna=False, sort=True):
        difference = group["elo_difference_aligned"]
        left = group["exercise_elo_playlist_aligned"]
        right = group["exercise_elo_zpdes_aligned"]
        rows.append(
            {
                "module_code": module_code,
                "module_label": group.get("module_label", pd.Series(dtype="object")).iloc[0]
                if "module_label" in group
                else pd.NA,
                "contexts": int(len(group)),
                "mean_difference": float(difference.mean()),
                "mean_absolute_difference": float(difference.abs().mean()),
                "rmse": float(np.sqrt(np.mean(np.square(difference)))),
                "pearson_correlation": _safe_correlation(left, right, "pearson"),
                "spearman_correlation": _safe_correlation(left, right, "spearman"),
                "lin_concordance": lin_concordance_correlation(left, right),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "module_code",
                "module_label",
                "contexts",
                "mean_difference",
                "mean_absolute_difference",
                "rmse",
                "pearson_correlation",
                "spearman_correlation",
                "lin_concordance",
            ]
        )
    return pd.DataFrame(rows).sort_values(
        ["mean_absolute_difference", "contexts"], ascending=[False, False]
    )

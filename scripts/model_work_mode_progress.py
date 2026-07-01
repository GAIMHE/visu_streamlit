"""Fit work-mode progress models for two student populations.

This script builds a table of contiguous work-mode progress runs within
student-module-qualified-activity timelines from first retained exercise
attempts and fits the primary system-level model:

    mean_progress ~ work_mode
                    + (1 | classroom_id)
                    + (1 | student_id within classroom)

for two populations. Activity is deliberately not adjusted for because activity
selection is part of the ZPDES mechanism being evaluated. GPBoost represents
classroom and nested student identifiers as separate random-intercept grouping
columns. The script also writes a forest-style Plotly HTML chart for the top
modules by usage.

Population definitions:
- exclusive_modes: students observed in exactly one of playlist/zpdes.
- both_modes: students observed in both playlist and zpdes.

By default the script reads the local MIAAM parquet release from data_miaam/.
Pass --input-file to run it on another CSV or parquet source.

All playlist rows are included by default. Use
``--keep-only-single-module-playlists`` only for the legacy restricted analysis.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

WORK_MODES = ("playlist", "zpdes")
POP_EXCLUSIVE = "exclusive_modes"
POP_BOTH = "both_modes"


def _n_module_activities(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    return int(frame[["module", "activity_id"]].drop_duplicates().shape[0])


@dataclass(frozen=True)
class FitSummary:
    population: str
    status: str
    n_rows: int
    n_students: int
    n_classrooms: int
    n_modules: int
    n_activities: int
    model_specification: str | None = None
    optimizer: str | None = None
    intercept: float | None = None
    intercept_std_error: float | None = None
    intercept_p_value: float | None = None
    intercept_ci_low: float | None = None
    intercept_ci_high: float | None = None
    estimate_zpdes_vs_playlist: float | None = None
    std_error: float | None = None
    p_value: float | None = None
    ci_low: float | None = None
    ci_high: float | None = None
    playlist_adjusted_mean: float | None = None
    zpdes_adjusted_mean: float | None = None
    converged: bool | None = None
    scale: float | None = None
    log_likelihood: float | None = None
    random_student_var: float | None = None
    random_classroom_var: float | None = None
    variance_components: str | None = None
    warning_count: int = 0
    warning_messages: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class InteractionSummary:
    status: str
    n_rows: int
    n_students: int
    n_classrooms: int
    n_modules: int
    model_specification: str
    optimizer: str | None = None
    exclusive_playlist_change: float | None = None
    exclusive_zpdes_change: float | None = None
    exclusive_zpdes_vs_playlist: float | None = None
    exclusive_std_error: float | None = None
    exclusive_p_value: float | None = None
    exclusive_ci_low: float | None = None
    exclusive_ci_high: float | None = None
    both_playlist_change: float | None = None
    both_zpdes_change: float | None = None
    both_zpdes_vs_playlist: float | None = None
    both_std_error: float | None = None
    both_p_value: float | None = None
    both_ci_low: float | None = None
    both_ci_high: float | None = None
    interaction_both_minus_exclusive: float | None = None
    interaction_std_error: float | None = None
    interaction_p_value: float | None = None
    interaction_ci_low: float | None = None
    interaction_ci_high: float | None = None
    converged: bool | None = None
    random_student_var: float | None = None
    random_classroom_var: float | None = None
    scale: float | None = None
    warning_count: int = 0
    warning_messages: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class SensitivitySummary:
    population: str
    status: str
    n_rows: int
    n_students: int
    n_classrooms: int
    n_modules: int
    model_specification: str
    playlist_adjusted_mean: float | None = None
    zpdes_adjusted_mean: float | None = None
    estimate_zpdes_vs_playlist: float | None = None
    std_error: float | None = None
    p_value: float | None = None
    ci_low: float | None = None
    ci_high: float | None = None
    error: str | None = None


def _first_existing(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    present = set(columns)
    for candidate in candidates:
        if candidate in present:
            return candidate
    return None


def _table_columns(path: Path) -> list[str]:
    if path.suffix.lower() == ".parquet":
        import pyarrow.parquet as pq

        return pq.read_schema(path).names
    return list(pd.read_csv(path, nrows=0).columns)


def _read_table(path: Path, usecols: list[str]) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path, columns=usecols, engine="pyarrow")
    return pd.read_csv(path, usecols=usecols, low_memory=False)


def _clean_optional_text(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    return values.mask(values.isin(["", "None", "nan", "NaN", "<NA>"]), pd.NA)


def _read_table_input(path: Path) -> pd.DataFrame:
    columns = _table_columns(path)
    module_columns = [
        column
        for column in (
            "module_title",
            "module_name",
            "module_short_title",
            "module_long_title",
            "module_id",
        )
        if column in columns
    ]
    column_map = {
        "student_id": _first_existing(columns, ("student_id", "user_id")),
        "source": _first_existing(columns, ("source",)),
        "classroom_id": _first_existing(columns, ("classroom_id",)),
        "playlist_id": _first_existing(columns, ("playlist_or_module_id", "playlist_id")),
        "exercise_id": _first_existing(columns, ("exercise_id",)),
        "activity_id": _first_existing(columns, ("activity_id_y", "activity_id")),
        "created_at": _first_existing(columns, ("created_at",)),
        "data_correct": _first_existing(columns, ("data_correct", "success")),
        "work_mode": _first_existing(columns, ("work_mode",)),
    }
    missing = [name for name, column in column_map.items() if column is None and name != "source"]
    if not module_columns:
        missing.append("module")
    if missing:
        raise ValueError(f"Missing required input columns for {path}: {', '.join(missing)}")

    usecols = sorted(
        {column for column in [*column_map.values(), *module_columns] if column is not None}
    )
    frame = _read_table(path, usecols=usecols)
    frame = frame.rename(columns={column: name for name, column in column_map.items()})

    module = pd.Series(pd.NA, index=frame.index, dtype="object")
    for column in module_columns:
        module = module.fillna(_clean_optional_text(frame[column]))
    frame["module"] = module
    return frame


def _read_mia_exercise_catalog(exercise_catalog_json: Path, module_config_json: Path) -> pd.DataFrame:
    module_config = json.loads(module_config_json.read_text(encoding="utf-8"))
    modules = module_config["config"]["module"]
    module_names = {}
    for module_id, module in modules.items():
        title = module.get("title") or {}
        module_names[str(module_id)] = title.get("short") or title.get("long") or str(module_id)

    exercise_catalog = json.loads(exercise_catalog_json.read_text(encoding="utf-8"))
    rows = []
    for exercise in exercise_catalog.get("exercises", []):
        module_ids = [str(module_id) for module_id in exercise.get("modules", [])]
        if not module_ids:
            continue
        module_id = module_ids[0]
        rows.append(
            {
                "exercise_id": exercise.get("id"),
                "catalog_module": module_names.get(module_id, module_id),
                "catalog_activity_id": (
                    str(exercise.get("activities", [])[0]) if exercise.get("activities") else pd.NA
                ),
            }
        )
    return pd.DataFrame(rows).dropna(subset=["exercise_id"]).drop_duplicates("exercise_id")


def _with_catalog_module_fill(frame: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    exercise_catalog_json = getattr(args, "exercise_catalog_json", None)
    module_config_json = getattr(args, "module_config_json", None)
    if exercise_catalog_json is None or module_config_json is None:
        return frame

    catalog = _read_mia_exercise_catalog(exercise_catalog_json, module_config_json)
    frame = frame.copy()
    frame["module"] = _clean_optional_text(frame["module"])
    frame = frame.merge(catalog, on="exercise_id", how="left")
    frame["module"] = frame["module"].fillna(frame["catalog_module"])
    frame["activity_id"] = _clean_optional_text(frame["activity_id"]).fillna(
        frame["catalog_activity_id"]
    )
    return frame.drop(columns=["catalog_module", "catalog_activity_id"])


def _read_miaam_parquet(data_dir: Path) -> pd.DataFrame:
    attempts_path = data_dir / "maths_data.parquet"
    exercises_path = data_dir / "maths_exercises_table.parquet"
    if not attempts_path.exists() or not exercises_path.exists():
        raise FileNotFoundError(
            "Expected data_miaam/maths_data.parquet and "
            "data_miaam/maths_exercises_table.parquet. Pass --input-file otherwise."
        )

    attempts = pd.read_parquet(
        attempts_path,
        columns=[
            "user_id",
            "classroom_id",
            "playlist_or_module_id",
            "exercise_id",
            "created_at",
            "data_correct",
            "work_mode",
            "source",
        ],
    )
    exercises = pd.read_parquet(
        exercises_path,
        columns=["source", "exercise_id", "activity_id", "module_name"],
    ).drop_duplicates(["source", "exercise_id"])
    merged = attempts.merge(exercises, on=["source", "exercise_id"], how="inner")
    return merged.rename(
        columns={
            "user_id": "student_id",
            "playlist_or_module_id": "playlist_id",
            "module_name": "module",
        }
    )


def _keep_only_single_module_playlists(args: argparse.Namespace) -> bool:
    if hasattr(args, "keep_only_single_module_playlists"):
        return bool(args.keep_only_single_module_playlists)
    return False


def load_attempts(args: argparse.Namespace) -> pd.DataFrame:
    input_file = getattr(args, "input_file", None) or getattr(args, "input_csv", None)
    if input_file is not None:
        frame = _read_table_input(input_file)
    else:
        data_dir = getattr(args, "data_dir", None)
        if data_dir is None:
            raise ValueError(
                "No input data configured. Rerun the notebook parameter cell so "
                "`args.input_file` points to the raw MIA parquet, or pass a valid `data_dir`."
            )
        frame = _read_miaam_parquet(data_dir)
    frame = _with_catalog_module_fill(frame, args)

    standard_columns = [
        "student_id",
        "classroom_id",
        "exercise_id",
        "activity_id",
        "module",
        "created_at",
        "data_correct",
        "work_mode",
    ]
    if "playlist_id" in frame.columns:
        standard_columns.append("playlist_id")
    if "source" in frame.columns:
        standard_columns.append("source")
    frame = frame[standard_columns].copy()
    frame = frame.dropna(
        subset=["student_id", "exercise_id", "activity_id", "module", "data_correct", "work_mode"]
    )
    frame["student_id"] = frame["student_id"].astype(str)
    if "source" in frame.columns:
        frame["source"] = frame["source"].fillna("unknown_source").astype(str)
        frame["student_id"] = frame["source"] + "::" + frame["student_id"]
    frame["classroom_id"] = frame["classroom_id"].fillna("missing_classroom").astype(str)
    frame["exercise_id"] = frame["exercise_id"].astype(str)
    frame["activity_id"] = frame["activity_id"].astype(str)
    frame["module"] = frame["module"].astype(str)
    frame["work_mode"] = frame["work_mode"].astype(str).str.strip()
    frame = frame[frame["work_mode"].isin(WORK_MODES)].copy()
    if _keep_only_single_module_playlists(args):
        if "playlist_id" not in frame.columns:
            raise ValueError(
                "Single-module playlist filtering requires a playlist id column. "
                "Expected `playlist_or_module_id` or `playlist_id`."
            )
        frame = filter_single_module_playlists(frame)
    frame["created_at"] = pd.to_datetime(frame["created_at"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["created_at"])
    frame["success"] = _to_success_numeric(frame["data_correct"])
    frame = frame.dropna(subset=["success"])
    frame["success"] = frame["success"].astype(float)
    return frame


def filter_single_module_playlists(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["playlist_id"] = frame["playlist_id"].astype("string").str.strip()
    frame.loc[frame["playlist_id"].fillna("").eq(""), "playlist_id"] = pd.NA

    if "source" in frame.columns:
        has_playlist_id = frame["playlist_id"].notna()
        frame.loc[has_playlist_id, "playlist_id"] = (
            frame.loc[has_playlist_id, "source"].astype(str)
            + "::"
            + frame.loc[has_playlist_id, "playlist_id"].astype(str)
        )

    playlist_rows = frame["work_mode"] == "playlist"
    playlist_with_id = frame[playlist_rows & frame["playlist_id"].notna()]
    module_counts = playlist_with_id.groupby("playlist_id")["module"].nunique()
    single_module_playlist_ids = set(module_counts[module_counts == 1].index)

    keep_rows = ~playlist_rows | frame["playlist_id"].isin(single_module_playlist_ids)
    return frame[keep_rows].copy()


def _to_success_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(int)
    if pd.api.types.is_numeric_dtype(series):
        return series.astype(float)

    normalized = series.astype(str).str.strip().str.lower()
    truthy = {"true", "1", "yes", "y", "vrai"}
    falsy = {"false", "0", "no", "n", "faux"}
    return normalized.map(lambda value: 1 if value in truthy else 0 if value in falsy else np.nan)


def split_populations(
    attempts: pd.DataFrame,
    min_unique_exercises: int | None = None,
) -> dict[str, pd.DataFrame]:
    mode_sets = attempts.groupby("student_id")["work_mode"].agg(lambda values: set(values))
    both_students = set(mode_sets[mode_sets.map(lambda modes: set(WORK_MODES).issubset(modes))].index)

    populations = {
        POP_EXCLUSIVE: attempts[~attempts["student_id"].isin(both_students)].copy(),
        POP_BOTH: attempts[attempts["student_id"].isin(both_students)].copy(),
    }

    if min_unique_exercises is None:
        return populations
    if min_unique_exercises < 1:
        raise ValueError("min_unique_exercises must be at least 1 when provided")

    filtered: dict[str, pd.DataFrame] = {}
    for name, frame in populations.items():
        unique_counts = frame.groupby("student_id")["exercise_id"].nunique()
        eligible_students = set(unique_counts[unique_counts >= min_unique_exercises].index)
        filtered[name] = frame[frame["student_id"].isin(eligible_students)].copy()
    return filtered


def build_activity_level(
    attempts: pd.DataFrame,
    min_activity_exercises: int = 4,
) -> pd.DataFrame:
    """Build first-versus-later progress from first retained exercise attempts.

    Activity ids are module-local in MIA, so module is part of the sequence key.
    The first retained student-exercise row is selected before each activity
    timeline is split into contiguous work-mode runs.
    """

    from scripts.model_work_mode_first_attempt_trajectory import (
        SEGMENT_KEYS,
        build_first_attempt_trajectory,
    )

    frame = build_first_attempt_trajectory(
        attempts,
        min_activity_exercises=min_activity_exercises,
    )
    group_keys = SEGMENT_KEYS
    rank = frame["attempt_position"] + 1
    n_attempts = frame["segment_exercises"]

    first_half = rank <= (n_attempts // 2)
    second_half = rank > (n_attempts - n_attempts // 2)

    success_first = (
        frame[first_half].groupby(group_keys)["success"].mean().rename("success_rate_first")
    )
    success_later = (
        frame[second_half].groupby(group_keys)["success"].mean().rename("success_rate_later")
    )
    success_all = frame.groupby(group_keys)["success"].mean().rename("success_rate_all")
    n_rows = frame.groupby(group_keys)["success"].size().rename("n_first_attempts")
    unique_exercises = frame.groupby(group_keys)["exercise_id"].nunique().rename("unique_exercises")
    classroom = frame.groupby(group_keys)["classroom_id"].first()

    activity_level = pd.concat(
        [success_first, success_later, success_all, n_rows, unique_exercises, classroom],
        axis=1,
    ).reset_index()
    activity_level = activity_level.dropna(subset=["success_rate_first", "success_rate_later"])
    activity_level["mean_progress"] = (
        activity_level["success_rate_later"] - activity_level["success_rate_first"]
    ) * 100.0

    for column in [
        "student_id",
        "classroom_id",
        "module",
        "activity_id",
        "work_mode",
    ]:
        activity_level[column] = activity_level[column].astype(str)
    return activity_level


PRIMARY_MODEL_FORMULA = "mean_progress ~ C(work_mode, Treatment('playlist'))"
PRIMARY_MODEL_SPECIFICATION = (
    "Gaussian GPBoost model; random intercepts for classroom and student "
    "within classroom"
)
INTERACTION_MODEL_FORMULA = (
    "mean_progress ~ C(work_mode, Treatment('playlist')) "
    "* C(source_population, Treatment('exclusive_modes')) + C(module)"
)
INTERACTION_MODEL_SPECIFICATION = (
    "classroom random intercept; student random intercept nested within classroom; "
    "module fixed effects"
)
SENSITIVITY_MODEL_FORMULA = (
    "mean_progress ~ C(work_mode, Treatment('playlist')) + C(module)"
)


def _student_classroom_error(model_df: pd.DataFrame) -> str | None:
    classrooms_per_student = model_df.groupby("student_id")["classroom_id"].nunique()
    if not classrooms_per_student.empty and classrooms_per_student.max() > 1:
        return (
            "The primary model assumes each student is nested in one classroom, but "
            f"{int((classrooms_per_student > 1).sum())} students span multiple classrooms."
        )
    return None


def _fit_with_optimizer_fallback(model, maxiter: int):
    warning_messages: list[str] = []
    optimizer_errors: list[str] = []
    last_result = None
    last_optimizer = None
    for optimizer in ("lbfgs", "powell", "bfgs"):
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = model.fit(
                    reml=True,
                    method=optimizer,
                    maxiter=maxiter,
                    full_output=True,
                )
            warning_messages.extend(str(item.message) for item in caught)
            last_result = result
            last_optimizer = optimizer
            if bool(getattr(result, "converged", False)):
                break
        except Exception as exc:  # pragma: no cover - optimizer/data dependent
            optimizer_errors.append(f"{optimizer}: {exc}")
    return last_result, last_optimizer, warning_messages, optimizer_errors


def _unique_warning_text(messages: list[str]) -> str | None:
    unique_messages = list(dict.fromkeys(message for message in messages if message))
    return " | ".join(unique_messages) if unique_messages else None


def _variance_component_map(result) -> dict[str, float]:
    names = list(getattr(result.model.exog_vc, "names", []))
    values = list(getattr(result, "vcomp", []))
    return {
        name: float(value)
        for name, value in zip(names, values, strict=False)
    }


def _random_classroom_variance(result) -> float | None:
    if result.cov_re is None or result.cov_re.size == 0:
        return None
    return float(result.cov_re.iloc[0, 0])


def _prepare_gpboost_progress_inputs(
    model_df: pd.DataFrame,
) -> tuple[np.ndarray, pd.DataFrame, pd.DataFrame]:
    """Build the Gaussian response, fixed effects, and nested group identifiers."""

    fixed_effects = pd.DataFrame(
        {
            "Intercept": np.ones(len(model_df), dtype=np.float64),
            "zpdes": model_df["work_mode"].eq("zpdes").to_numpy(dtype=np.float64),
        },
        index=model_df.index,
    )
    student_in_classroom = pd.MultiIndex.from_frame(
        model_df[["classroom_id", "student_id"]]
    )
    grouping_values = {
        "classroom_id": model_df["classroom_id"],
        "student_in_classroom": student_in_classroom,
    }
    group_data = pd.DataFrame(index=model_df.index)
    for name, values in grouping_values.items():
        codes, _ = pd.factorize(values, sort=True)
        if np.any(codes < 0):
            raise ValueError(f"Could not encode grouping column {name}")
        group_data[name] = codes.astype(np.int32)

    response = model_df["mean_progress"].to_numpy(dtype=np.float64)
    return response, fixed_effects, group_data


def _gpboost_parameter_map(table: pd.DataFrame) -> dict[str, float]:
    return {
        str(name): float(value)
        for name, value in table.loc["Param."].items()
    }


def _adjusted_mode_means(result, model_df: pd.DataFrame) -> dict[str, float]:
    module_weights = model_df["module"].value_counts(normalize=True).sort_index()
    reference = pd.DataFrame({"module": module_weights.index})
    adjusted_means = {}
    for work_mode in WORK_MODES:
        prediction_frame = reference.assign(work_mode=work_mode)
        predictions = np.asarray(result.predict(prediction_frame), dtype=float)
        adjusted_means[work_mode] = float(np.dot(predictions, module_weights.to_numpy()))
    return adjusted_means


def _normal_p_value(estimate: float, std_error: float) -> float:
    if std_error <= 0 or not np.isfinite(std_error):
        return math.nan
    return math.erfc(abs(estimate / std_error) / math.sqrt(2.0))


def fit_mixed_model(activity_level: pd.DataFrame, population: str, maxiter: int) -> FitSummary:
    try:
        import gpboost as gpb
    except ImportError:  # pragma: no cover - exercised only without optional dep
        return FitSummary(
            population=population,
            status="failed",
            n_rows=len(activity_level),
            n_students=activity_level["student_id"].nunique(),
            n_classrooms=activity_level["classroom_id"].nunique(),
            n_modules=activity_level["module"].nunique(),
            n_activities=_n_module_activities(activity_level),
            model_specification=PRIMARY_MODEL_SPECIFICATION,
            error=(
                "gpboost is required for crossed mixed-model fitting. "
                "Install project dependencies with `uv sync`."
            ),
        )

    model_df = activity_level.dropna(
        subset=[
            "mean_progress",
            "work_mode",
            "student_id",
            "classroom_id",
        ]
    ).copy()
    base = {
        "population": population,
        "n_rows": len(model_df),
        "n_students": model_df["student_id"].nunique(),
        "n_classrooms": model_df["classroom_id"].nunique(),
        "n_modules": model_df["module"].nunique(),
        "n_activities": _n_module_activities(model_df),
        "model_specification": PRIMARY_MODEL_SPECIFICATION,
    }
    if len(model_df) == 0 or model_df["work_mode"].nunique() < 2:
        return FitSummary(**base, status="skipped", error="Need both work modes and non-empty data.")
    nesting_error = _student_classroom_error(model_df)
    if nesting_error:
        return FitSummary(**base, status="failed", error=nesting_error)

    try:
        response, fixed_effects, group_data = _prepare_gpboost_progress_inputs(model_df)
        model = gpb.GPModel(
            likelihood="gaussian",
            group_data=group_data,
        )
        model.fit(
            y=response,
            X=fixed_effects,
            params={
                "optimizer_cov": "lbfgs",
                "optimizer_coef": "lbfgs",
                "maxit": maxiter,
                "trace": False,
            },
        )
        coefficient_table = model.get_coef(std_err=True, format_pandas=True)
        covariance_table = model.get_cov_pars(std_err=False, format_pandas=True)
        coefficients = _gpboost_parameter_map(coefficient_table)
        standard_errors = {
            str(name): float(value)
            for name, value in coefficient_table.loc["Std. err."].items()
        }
        variance_components = _gpboost_parameter_map(covariance_table)
        iterations = int(model._get_num_optim_iter())
        converged = iterations < maxiter
    except Exception as exc:  # pragma: no cover - model construction/data dependent
        return FitSummary(**base, status="failed", error=str(exc))

    intercept = coefficients["Intercept"]
    intercept_se = standard_errors["Intercept"]
    mode_estimate = coefficients["zpdes"]
    mode_se = standard_errors["zpdes"]
    intercept_p_value = _normal_p_value(intercept, intercept_se)
    mode_p_value = _normal_p_value(mode_estimate, mode_se)
    return FitSummary(
        **base,
        status="ok" if converged else "not_converged",
        optimizer="lbfgs",
        intercept=intercept,
        intercept_std_error=intercept_se,
        intercept_p_value=intercept_p_value,
        intercept_ci_low=intercept - 1.96 * intercept_se,
        intercept_ci_high=intercept + 1.96 * intercept_se,
        estimate_zpdes_vs_playlist=mode_estimate,
        std_error=mode_se,
        p_value=mode_p_value,
        ci_low=mode_estimate - 1.96 * mode_se,
        ci_high=mode_estimate + 1.96 * mode_se,
        playlist_adjusted_mean=intercept,
        zpdes_adjusted_mean=intercept + mode_estimate,
        converged=converged,
        scale=variance_components.get("Error_var"),
        log_likelihood=-float(model.get_current_neg_log_likelihood()),
        random_student_var=variance_components.get("student_in_classroom"),
        random_classroom_var=variance_components.get("classroom_id"),
        variance_components="; ".join(
            f"{name}={value:.6g}"
            for name, value in variance_components.items()
            if name != "Error_var"
        ),
        error=None if converged else "Optimizer reached the maximum iteration count.",
    )


def fit_population_interaction_model(
    activity_level: pd.DataFrame,
    maxiter: int,
) -> InteractionSummary:
    import statsmodels.formula.api as smf

    required = [
        "mean_progress",
        "work_mode",
        "source_population",
        "student_id",
        "classroom_id",
        "module",
    ]
    model_df = activity_level.dropna(subset=required).copy()
    base = {
        "n_rows": len(model_df),
        "n_students": model_df["student_id"].nunique(),
        "n_classrooms": model_df["classroom_id"].nunique(),
        "n_modules": model_df["module"].nunique(),
        "model_specification": INTERACTION_MODEL_SPECIFICATION,
    }
    required_populations = {POP_EXCLUSIVE, POP_BOTH}
    if not required_populations.issubset(set(model_df["source_population"])):
        return InteractionSummary(
            **base,
            status="skipped",
            error="Interaction model requires exclusive_modes and both_modes.",
        )
    nesting_error = _student_classroom_error(model_df)
    if nesting_error:
        return InteractionSummary(**base, status="failed", error=nesting_error)

    try:
        model = smf.mixedlm(
            INTERACTION_MODEL_FORMULA,
            data=model_df,
            groups=model_df["classroom_id"],
            re_formula="1",
            vc_formula={"student_id": "0 + C(student_id)"},
            use_sparse=False,
        )
        result, optimizer, warning_messages, optimizer_errors = _fit_with_optimizer_fallback(
            model, maxiter=maxiter
        )
    except Exception as exc:  # pragma: no cover - model construction/data dependent
        return InteractionSummary(**base, status="failed", error=str(exc))
    if result is None:
        return InteractionSummary(
            **base,
            status="failed",
            warning_count=len(warning_messages),
            warning_messages=_unique_warning_text(warning_messages),
            error=" | ".join(optimizer_errors) or "No optimizer returned a result.",
        )

    fe = result.fe_params
    mode_key = next(
        key for key in fe.index if "C(work_mode" in key and "[T.zpdes]" in key and ":" not in key
    )
    interaction_key = next(
        key
        for key in fe.index
        if ":" in key and "C(work_mode" in key and "C(source_population" in key
    )
    interaction_estimate = float(fe[interaction_key])
    interaction_se = float(result.bse[interaction_key])
    interaction_ci = result.conf_int().loc[interaction_key]
    exclusive_difference = float(fe[mode_key])
    exclusive_se = float(result.bse[mode_key])
    exclusive_ci = result.conf_int().loc[mode_key]
    both_difference = float(fe[mode_key] + fe[interaction_key])
    fixed_covariance = result.cov_params().loc[
        [mode_key, interaction_key],
        [mode_key, interaction_key],
    ]
    both_variance = float(fixed_covariance.to_numpy().sum())
    both_se = math.sqrt(max(both_variance, 0.0))

    module_weights = model_df["module"].value_counts(normalize=True).sort_index()
    reference = pd.DataFrame({"module": module_weights.index})
    adjusted_means: dict[tuple[str, str], float] = {}
    for population in (POP_EXCLUSIVE, POP_BOTH):
        for work_mode in WORK_MODES:
            prediction_frame = reference.assign(
                source_population=population,
                work_mode=work_mode,
            )
            predictions = np.asarray(result.predict(prediction_frame), dtype=float)
            adjusted_means[(population, work_mode)] = float(
                np.dot(predictions, module_weights.to_numpy())
            )

    variance_components = _variance_component_map(result)
    converged = bool(getattr(result, "converged", False))
    error_parts = optimizer_errors.copy()
    if not converged:
        error_parts.append("Optimizer did not converge; estimates are provisional.")
    return InteractionSummary(
        **base,
        status="ok" if converged else "not_converged",
        optimizer=optimizer,
        exclusive_playlist_change=adjusted_means[(POP_EXCLUSIVE, "playlist")],
        exclusive_zpdes_change=adjusted_means[(POP_EXCLUSIVE, "zpdes")],
        exclusive_zpdes_vs_playlist=exclusive_difference,
        exclusive_std_error=exclusive_se,
        exclusive_p_value=_normal_p_value(exclusive_difference, exclusive_se),
        exclusive_ci_low=float(exclusive_ci.iloc[0]),
        exclusive_ci_high=float(exclusive_ci.iloc[1]),
        both_playlist_change=adjusted_means[(POP_BOTH, "playlist")],
        both_zpdes_change=adjusted_means[(POP_BOTH, "zpdes")],
        both_zpdes_vs_playlist=both_difference,
        both_std_error=both_se,
        both_p_value=_normal_p_value(both_difference, both_se),
        both_ci_low=both_difference - 1.96 * both_se,
        both_ci_high=both_difference + 1.96 * both_se,
        interaction_both_minus_exclusive=interaction_estimate,
        interaction_std_error=interaction_se,
        interaction_p_value=_normal_p_value(interaction_estimate, interaction_se),
        interaction_ci_low=float(interaction_ci.iloc[0]),
        interaction_ci_high=float(interaction_ci.iloc[1]),
        converged=converged,
        random_student_var=variance_components.get("student_id"),
        random_classroom_var=_random_classroom_variance(result),
        scale=float(result.scale),
        warning_count=len(warning_messages),
        warning_messages=_unique_warning_text(warning_messages),
        error=" | ".join(error_parts) or None,
    )


def fit_clustered_ols_sensitivity(
    activity_level: pd.DataFrame,
    population: str,
) -> SensitivitySummary:
    import statsmodels.formula.api as smf

    model_df = activity_level.dropna(
        subset=["mean_progress", "work_mode", "student_id", "classroom_id", "module"]
    ).copy()
    base = {
        "population": population,
        "n_rows": len(model_df),
        "n_students": model_df["student_id"].nunique(),
        "n_classrooms": model_df["classroom_id"].nunique(),
        "n_modules": model_df["module"].nunique(),
        "model_specification": "OLS with module fixed effects and classroom-clustered standard errors",
    }
    if model_df["work_mode"].nunique() < 2 or model_df["classroom_id"].nunique() < 2:
        return SensitivitySummary(
            **base,
            status="skipped",
            error="Sensitivity model requires both work modes and at least two classrooms.",
        )
    try:
        result = smf.ols(SENSITIVITY_MODEL_FORMULA, data=model_df).fit(
            cov_type="cluster",
            cov_kwds={"groups": model_df["classroom_id"], "use_correction": True},
        )
    except Exception as exc:  # pragma: no cover - model/data dependent
        return SensitivitySummary(**base, status="failed", error=str(exc))

    mode_key = next(
        key for key in result.params.index if "C(work_mode" in key and "[T.zpdes]" in key
    )
    adjusted_means = _adjusted_mode_means(result, model_df)
    confint = result.conf_int().loc[mode_key]
    return SensitivitySummary(
        **base,
        status="ok",
        playlist_adjusted_mean=adjusted_means["playlist"],
        zpdes_adjusted_mean=adjusted_means["zpdes"],
        estimate_zpdes_vs_playlist=float(result.params[mode_key]),
        std_error=float(result.bse[mode_key]),
        p_value=float(result.pvalues[mode_key]),
        ci_low=float(confint.iloc[0]),
        ci_high=float(confint.iloc[1]),
    )


def build_top_module_plot_table(
    population_attempts: dict[str, pd.DataFrame],
    population_activity: dict[str, pd.DataFrame],
    top_n_modules: int,
    require_both_work_modes: bool,
    plot_by_population: bool = False,
    forest_population: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if forest_population is not None:
        if forest_population not in population_attempts or forest_population not in population_activity:
            raise ValueError(f"Unknown forest-plot population: {forest_population}")
        population_attempts = {
            forest_population: population_attempts[forest_population],
        }
        population_activity = {
            forest_population: population_activity[forest_population],
        }
        plot_by_population = True

    usage_frames = []
    for population, frame in population_attempts.items():
        if frame.empty:
            continue
        tagged = frame.assign(population=population)
        usage = tagged.groupby(["population", "module"], as_index=False).agg(
            attempt_rows=("exercise_id", "size"),
            students=("student_id", "nunique"),
            unique_exercises=("exercise_id", "nunique"),
        )
        for work_mode in WORK_MODES:
            mode_usage = (
                tagged[tagged["work_mode"] == work_mode]
                .groupby(["population", "module"], as_index=False)
                .agg(
                    **{
                        f"{work_mode}_attempt_rows": ("exercise_id", "size"),
                        f"{work_mode}_students": ("student_id", "nunique"),
                        f"{work_mode}_unique_exercises": ("exercise_id", "nunique"),
                    }
                )
            )
            usage = usage.merge(mode_usage, on=["population", "module"], how="left")
        count_columns = [
            column
            for column in usage.columns
            if column.endswith(("_attempt_rows", "_students", "_unique_exercises"))
        ]
        usage[count_columns] = usage[count_columns].fillna(0).astype(int)
        usage_frames.append(usage)
    usage_by_population = pd.concat(usage_frames, ignore_index=True) if usage_frames else pd.DataFrame()
    if usage_by_population.empty:
        return usage_by_population, pd.DataFrame()

    activity_frames = [frame for frame in population_activity.values() if not frame.empty]
    usage_by_module = usage_by_population.groupby("module", as_index=False)["attempt_rows"].sum()
    eligible_modules = set(usage_by_module["module"])
    if require_both_work_modes:
        if activity_frames:
            if plot_by_population:
                required_populations = {
                    population for population, frame in population_activity.items() if not frame.empty
                }
                mode_counts = (
                    pd.concat(
                        [
                            frame.assign(population=population)
                            for population, frame in population_activity.items()
                            if not frame.empty
                        ],
                        ignore_index=True,
                    )
                    .groupby(["population", "module"])["work_mode"]
                    .nunique()
                    .reset_index(name="n_work_modes")
                )
                modules_by_population = (
                    mode_counts[mode_counts["n_work_modes"] >= len(WORK_MODES)]
                    .groupby("module")["population"]
                    .agg(lambda values: set(values))
                )
                eligible_modules = {
                    module
                    for module, populations in modules_by_population.items()
                    if required_populations.issubset(populations)
                }
            else:
                mode_counts = (
                    pd.concat(activity_frames, ignore_index=True)
                    .groupby("module")["work_mode"]
                    .nunique()
                )
                eligible_modules = set(mode_counts[mode_counts >= len(WORK_MODES)].index)
        else:
            eligible_modules = set()
        usage_by_module = usage_by_module[usage_by_module["module"].isin(eligible_modules)]

    ranked_usage = usage_by_module.sort_values(
        ["attempt_rows", "module"], ascending=[False, True]
    ).reset_index(drop=True)
    ranked_usage["usage_rank"] = ranked_usage.index + 1
    top_modules = ranked_usage.head(top_n_modules)["module"].tolist()
    rank_by_module = ranked_usage.set_index("module")["usage_rank"]
    usage_by_population["eligible_for_plot"] = usage_by_population["module"].isin(
        eligible_modules
    )
    usage_by_population["usage_rank"] = usage_by_population["module"].map(rank_by_module).astype(
        "Int64"
    )
    usage_by_population["selected_for_plot"] = usage_by_population["module"].isin(top_modules)

    plot_frames = []
    plot_sources = (
        population_activity
        if plot_by_population
        else {
            "combined": (
                pd.concat(activity_frames, ignore_index=True).assign(population="combined")
                if activity_frames
                else pd.DataFrame()
            )
        }
    )
    for population, frame in plot_sources.items():
        if frame.empty:
            continue
        sub = frame[frame["module"].isin(top_modules)].copy()
        if sub.empty:
            continue
        grouped = (
            sub.groupby(["population", "module", "work_mode"], as_index=False)
            if "population" in sub.columns
            else sub.assign(population=population).groupby(
                ["population", "module", "work_mode"], as_index=False
            )
        )
        stats = grouped.agg(
            estimate=("mean_progress", "mean"),
            std=("mean_progress", "std"),
            n_rows=("mean_progress", "size"),
            students=("student_id", "nunique"),
            activities=("activity_id", "nunique"),
        )
        stats["std"] = stats["std"].fillna(0.0)
        stats["se"] = stats["std"] / np.sqrt(stats["n_rows"].clip(lower=1))
        stats["ci_low"] = stats["estimate"] - 1.96 * stats["se"]
        stats["ci_high"] = stats["estimate"] + 1.96 * stats["se"]
        plot_frames.append(stats)

    plot_table = pd.concat(plot_frames, ignore_index=True) if plot_frames else pd.DataFrame()
    order = {module: idx for idx, module in enumerate(top_modules)}
    if not plot_table.empty:
        plot_table["module_order"] = plot_table["module"].map(order)
        plot_table = plot_table.sort_values(["module_order", "population", "work_mode"])
    return usage_by_population, plot_table


def build_forest_figure(plot_table: pd.DataFrame) -> go.Figure | None:
    if plot_table.empty:
        return None

    modules = (
        plot_table[["module", "module_order"]]
        .drop_duplicates()
        .sort_values("module_order")["module"]
        .tolist()
    )
    y_base = {module: idx for idx, module in enumerate(modules)}
    populations = plot_table["population"].drop_duplicates().tolist()
    combined_plot = populations == ["combined"]
    single_population_plot = len(populations) == 1
    simple_work_mode_plot = combined_plot or single_population_plot
    single_population = populations[0] if single_population_plot else None
    offsets = (
        {
            (single_population, "playlist"): -0.12,
            (single_population, "zpdes"): 0.12,
        }
        if simple_work_mode_plot
        else {
            (POP_EXCLUSIVE, "playlist"): -0.27,
            (POP_EXCLUSIVE, "zpdes"): -0.09,
            (POP_BOTH, "playlist"): 0.09,
            (POP_BOTH, "zpdes"): 0.27,
        }
    )
    colors = {"playlist": "#E68613", "zpdes": "#2E86AB"}
    symbols = {"combined": "circle", POP_EXCLUSIVE: "circle", POP_BOTH: "diamond"}
    names = (
        {
            (single_population, "playlist"): "playlist",
            (single_population, "zpdes"): "zpdes",
        }
        if simple_work_mode_plot
        else {
            (POP_EXCLUSIVE, "playlist"): "exclusive: playlist",
            (POP_EXCLUSIVE, "zpdes"): "exclusive: zpdes",
            (POP_BOTH, "playlist"): "both-modes: playlist",
            (POP_BOTH, "zpdes"): "both-modes: zpdes",
        }
    )

    fig = go.Figure()
    for population in populations:
        for work_mode in WORK_MODES:
            sub = plot_table[
                (plot_table["population"] == population) & (plot_table["work_mode"] == work_mode)
            ].copy()
            if sub.empty:
                continue
            offset = offsets.get((population, work_mode), 0.0)
            y_values = [y_base[module] + offset for module in sub["module"]]
            fig.add_trace(
                go.Scatter(
                    x=sub["estimate"],
                    y=y_values,
                    mode="markers",
                    name=names.get((population, work_mode), f"{population}: {work_mode}"),
                    marker={
                        "color": colors[work_mode],
                        "symbol": symbols[population],
                        "size": 10,
                        "line": {"width": 1, "color": "white"},
                    },
                    error_x={
                        "type": "data",
                        "symmetric": False,
                        "array": sub["ci_high"] - sub["estimate"],
                        "arrayminus": sub["estimate"] - sub["ci_low"],
                        "thickness": 1.4,
                    },
                    customdata=np.stack(
                        [sub["n_rows"], sub["students"], sub["activities"]], axis=-1
                    ),
                    hovertemplate=(
                        "%{fullData.name}<br>"
                        "module=%{text}<br>"
                        "mean_progress=%{x:.2f}<br>"
                        "rows=%{customdata[0]}<br>"
                        "students=%{customdata[1]}<br>"
                        "activities=%{customdata[2]}<extra></extra>"
                    ),
                    text=sub["module"],
                )
            )

    fig.add_vline(x=0, line_dash="dash", line_color="#555555")
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(len(modules))),
        ticktext=modules,
        autorange="reversed",
    )
    fig.update_layout(
        title=(
            "Top modules by usage: mean_progress by work mode"
            if combined_plot
            else (
                f"Top modules by usage: mean_progress by work mode ({single_population})"
                if single_population_plot
                else "Top modules by usage: mean_progress by work mode and population"
            )
        ),
        xaxis_title=(
            "Mean progress points (later first-attempt success rate "
            "- early first-attempt success rate)"
        ),
        yaxis_title="Module",
        template="plotly_white",
        height=max(420, 120 * len(modules)),
        legend_title="Work mode" if simple_work_mode_plot else "Population / work mode",
        margin={"l": 260, "r": 40, "t": 70, "b": 60},
    )
    return fig


def write_forest_plot(plot_table: pd.DataFrame, output_path: Path) -> None:
    fig = build_forest_figure(plot_table)
    if fig is None:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(output_path, include_plotlyjs="cdn")


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def _summary_to_frame(summaries: list[FitSummary]) -> pd.DataFrame:
    return pd.DataFrame([summary.__dict__ for summary in summaries])


def run(args: argparse.Namespace) -> int:
    attempts = load_attempts(args)
    populations = split_populations(attempts, min_unique_exercises=args.min_unique_exercises)

    activity_by_population: dict[str, pd.DataFrame] = {}
    fit_summaries: list[FitSummary] = []
    for population, frame in populations.items():
        activity_level = build_activity_level(
            frame,
            min_activity_exercises=args.min_activity_exercises,
        )
        activity_level["population"] = population
        activity_by_population[population] = activity_level
        if args.skip_model:
            fit_summaries.append(
                FitSummary(
                    population=population,
                    status="skipped",
                    n_rows=len(activity_level),
                    n_students=activity_level["student_id"].nunique(),
                    n_classrooms=activity_level["classroom_id"].nunique(),
                    n_modules=activity_level["module"].nunique(),
                    n_activities=_n_module_activities(activity_level),
                    error="Skipped by --skip-model.",
                )
            )
        else:
            fit_summaries.append(fit_mixed_model(activity_level, population, maxiter=args.maxiter))

    usage, plot_table = build_top_module_plot_table(
        populations,
        activity_by_population,
        top_n_modules=args.top_n_modules,
        require_both_work_modes=not args.allow_single_mode_top_modules,
        plot_by_population=args.plot_by_population,
        forest_population=getattr(args, "forest_population", None),
    )

    output_dir = args.output_dir
    _write_csv(_summary_to_frame(fit_summaries), output_dir / "work_mode_progress_model_summary.csv")
    _write_csv(usage, output_dir / "work_mode_progress_module_usage.csv")
    _write_csv(plot_table, output_dir / "work_mode_progress_top_modules_forest_data.csv")
    write_forest_plot(plot_table, output_dir / "work_mode_progress_top_modules_forest.html")

    print(f"Attempts after work-mode and playlist filtering: {len(attempts):,}")
    for population, frame in populations.items():
        print(
            f"{population}: {len(frame):,} attempts, "
            f"{frame['student_id'].nunique():,} students, "
            f"{frame['exercise_id'].nunique():,} unique exercises"
        )
    print(f"Wrote outputs to: {output_dir}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Optional CSV or parquet input file. Preferred for MIA-only analyses.",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="Compatibility alias for --input-file when using a CSV input.",
    )
    parser.add_argument(
        "--exercise-catalog-json",
        type=Path,
        default=None,
        help="Optional MIA exo_mia.json used to fill missing module labels from exercise ids.",
    )
    parser.add_argument(
        "--module-config-json",
        type=Path,
        default=None,
        help="Optional MIA config_mia.json used with --exercise-catalog-json.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data_miaam"),
        help="Directory containing maths_data.parquet and maths_exercises_table.parquet.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts") / "regression",
        help="Directory for CSV summaries and the forest plot HTML.",
    )
    parser.add_argument(
        "--min-unique-exercises",
        type=int,
        default=None,
        help="Optional legacy minimum distinct exercises per student; disabled by default.",
    )
    parser.add_argument(
        "--min-activity-exercises",
        "--min-activity-attempts",
        dest="min_activity_exercises",
        type=int,
        default=4,
        help=(
            "Minimum unique first-attempt exercises per contiguous work-mode run "
            "within a student-module-activity timeline."
        ),
    )
    parser.add_argument("--top-n-modules", type=int, default=5)
    parser.add_argument("--maxiter", type=int, default=200)
    parser.add_argument(
        "--allow-single-mode-top-modules",
        action="store_true",
        help="Allow top modules that only have one of playlist/zpdes in the filtered data.",
    )
    parser.add_argument(
        "--keep-only-single-module-playlists",
        action="store_true",
        help="Apply the legacy filter that drops playlists spanning multiple modules.",
    )
    parser.add_argument(
        "--include-multi-module-playlists",
        action="store_true",
        help="Deprecated compatibility flag; all playlist rows are already included by default.",
    )
    parser.add_argument(
        "--plot-by-population",
        action="store_true",
        help=(
            "Show separate forest-plot traces for exclusive_modes and both_modes. "
            "By default the forest plot combines populations and shows only playlist/zpdes."
        ),
    )
    parser.add_argument(
        "--forest-population",
        choices=(POP_EXCLUSIVE, POP_BOTH),
        default=None,
        help=(
            "Restrict forest-plot module ranking and estimates to one population. "
            "For example, use exclusive_modes to exclude students observed in both modes."
        ),
    )
    parser.add_argument(
        "--skip-model",
        action="store_true",
        help="Build populations and plot data without fitting the mixed model.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run(parse_args(sys.argv[1:])))

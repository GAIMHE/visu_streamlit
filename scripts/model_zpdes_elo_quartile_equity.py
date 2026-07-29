"""Prepare and fit the ZPDES module-specific Elo-quartile equity model.

The analysis unit is one student-module pair. Progress is averaged across that
pair's eligible ZPDES activity sequences before fitting the model, and initial
Elo quartiles are calculated separately within every module.

The fitted GPBoost model is the scalable independent-random-coefficient analogue
of the following lme4 specification:

    mean_progress ~ elo_quartile
      + (1 + elo_quartile || module_title)
      + (1 | user_id)
      + (1 | classroom_id)

The independent module random coefficients are deliberate: they allow the
Q2-Q1, Q3-Q1, and Q4-Q1 contrasts to vary between modules without estimating a
large random-effect correlation matrix from a modest number of modules.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

QUARTILE_LEVELS = ("Q1", "Q2", "Q3", "Q4")
FIXED_EFFECT_COLUMNS = ("Intercept", "Q2", "Q3", "Q4")


@dataclass
class QuartileEquityFit:
    """Outputs and diagnostics from the crossed mixed model."""

    model: object | None = None
    fixed_effects: pd.DataFrame = field(default_factory=pd.DataFrame)
    adjusted_quartile_means: pd.DataFrame = field(default_factory=pd.DataFrame)
    variance_components: pd.DataFrame = field(default_factory=pd.DataFrame)
    module_heterogeneity: pd.DataFrame = field(default_factory=pd.DataFrame)
    diagnostics: pd.DataFrame = field(default_factory=pd.DataFrame)


def load_module_lookup(module_config_path: Path) -> pd.DataFrame:
    """Return unique MIA module titles and codes from ``config_mia.json``."""

    config = json.loads(module_config_path.read_text(encoding="utf-8"))["config"]["module"]
    rows = []
    for module_key, metadata in config.items():
        title = metadata.get("title") or {}
        module_title = (
            title.get("short")
            or title.get("long")
            or str(metadata.get("code") or module_key)
        )
        rows.append(
            {
                "module_title": str(module_title),
                "module_code": str(metadata.get("code") or f"M{module_key}"),
            }
        )

    lookup = pd.DataFrame(rows).drop_duplicates()
    duplicate_titles = lookup.duplicated("module_title", keep=False)
    duplicate_codes = lookup.duplicated("module_code", keep=False)
    if duplicate_titles.any() or duplicate_codes.any():
        raise ValueError("Module titles and codes must form a one-to-one lookup")
    return lookup


def _as_boolean(series: pd.Series) -> pd.Series:
    """Normalize CSV boolean values without treating non-empty strings as true."""

    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y", "vrai"})


def load_adaptive_test_elo(
    adaptive_elo_path: Path,
    *,
    source_id: str = "mia",
) -> pd.DataFrame:
    """Load one valid pre-practice adaptive-test Elo per student and module."""

    columns = [
        "source_id",
        "user_id",
        "module_code",
        "adaptive_test_attempts",
        "adaptive_test_elo",
        "has_adaptive_test_elo",
    ]
    elo = pd.read_csv(adaptive_elo_path, usecols=columns, low_memory=False)
    elo = elo.loc[elo["source_id"].astype(str).eq(source_id)].copy()
    elo = elo.loc[
        _as_boolean(elo["has_adaptive_test_elo"])
        & elo["adaptive_test_elo"].notna()
        & elo["user_id"].notna()
        & elo["module_code"].notna()
    ].copy()
    elo = elo.rename(columns={"user_id": "student_id"})
    elo["student_id"] = elo["student_id"].astype(str)
    elo["module_code"] = elo["module_code"].astype(str)

    duplicate_pairs = elo.duplicated(["student_id", "module_code"], keep=False)
    if duplicate_pairs.any():
        raise ValueError(
            "Adaptive-test Elo must contain one row per student-module pair; "
            f"found {int(duplicate_pairs.sum())} duplicate rows"
        )
    return elo[
        [
            "student_id",
            "module_code",
            "adaptive_test_attempts",
            "adaptive_test_elo",
        ]
    ]


def _within_module_quartile(elo: pd.Series) -> pd.Categorical:
    """Assign tied Elo values to the same within-module rank quartile."""

    percentile_rank = elo.rank(method="average", pct=True)
    return pd.cut(
        percentile_rank,
        bins=[0.0, 0.25, 0.50, 0.75, 1.0],
        labels=QUARTILE_LEVELS,
        include_lowest=True,
        ordered=True,
    )


def build_student_module_analysis(
    activity_level: pd.DataFrame,
    adaptive_elo: pd.DataFrame,
    module_lookup: pd.DataFrame,
    *,
    min_adaptive_test_attempts: int = 1,
    min_pairs_per_quartile: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build one equally weighted ZPDES progression row per student-module pair.

    Returns
    -------
    analysis
        Retained student-module rows with within-module Elo quartiles.
    audit
        One-row audit of the join and filters.
    module_coverage
        Student-pair counts by module and Elo quartile.
    """

    required_activity = {
        "student_id",
        "classroom_id",
        "module",
        "activity_id",
        "work_mode",
        "mean_progress",
    }
    missing_activity = sorted(required_activity.difference(activity_level.columns))
    if missing_activity:
        raise ValueError(
            "Activity-level data are missing columns: " + ", ".join(missing_activity)
        )
    required_elo = {
        "student_id",
        "module_code",
        "adaptive_test_attempts",
        "adaptive_test_elo",
    }
    missing_elo = sorted(required_elo.difference(adaptive_elo.columns))
    if missing_elo:
        raise ValueError("Adaptive Elo data are missing columns: " + ", ".join(missing_elo))
    if min_adaptive_test_attempts < 1:
        raise ValueError("min_adaptive_test_attempts must be at least 1")
    if min_pairs_per_quartile < 1:
        raise ValueError("min_pairs_per_quartile must be at least 1")

    zpdes = activity_level.loc[activity_level["work_mode"].eq("zpdes")].copy()
    zpdes = zpdes.rename(columns={"module": "module_title"})
    zpdes = zpdes.merge(
        module_lookup,
        on="module_title",
        how="left",
        validate="many_to_one",
    )
    unmapped_modules = sorted(
        zpdes.loc[zpdes["module_code"].isna(), "module_title"].dropna().unique()
    )
    if unmapped_modules:
        raise ValueError("Unmapped ZPDES modules: " + ", ".join(unmapped_modules))

    student_classrooms = zpdes.groupby("student_id")["classroom_id"].nunique()
    multi_classroom_students = student_classrooms[student_classrooms > 1]
    if not multi_classroom_students.empty:
        raise ValueError(
            "Each student must map to one classroom for this model; "
            f"{len(multi_classroom_students)} students map to multiple classrooms"
        )

    student_module = (
        zpdes.groupby(
            ["student_id", "classroom_id", "module_code", "module_title"],
            as_index=False,
            observed=True,
        )
        .agg(
            mean_progress=("mean_progress", "mean"),
            n_sequences=("mean_progress", "size"),
            n_activities=("activity_id", "nunique"),
        )
    )
    joined = student_module.merge(
        adaptive_elo,
        on=["student_id", "module_code"],
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    with_elo = joined.loc[
        joined["_merge"].eq("both")
        & joined["adaptive_test_elo"].notna()
        & joined["adaptive_test_attempts"].ge(min_adaptive_test_attempts)
    ].drop(columns="_merge")

    with_elo = with_elo.copy()
    with_elo["elo_quartile"] = (
        with_elo.groupby("module_code", observed=True)["adaptive_test_elo"]
        .transform(_within_module_quartile)
        .astype(pd.CategoricalDtype(categories=QUARTILE_LEVELS, ordered=True))
    )
    with_elo = with_elo.dropna(subset=["elo_quartile"])

    module_coverage = (
        with_elo.groupby(
            ["module_code", "module_title", "elo_quartile"],
            observed=True,
        )
        .size()
        .unstack("elo_quartile", fill_value=0)
        .reset_index()
    )
    for quartile in QUARTILE_LEVELS:
        if quartile not in module_coverage:
            module_coverage[quartile] = 0
    module_coverage["minimum_quartile_pairs"] = module_coverage[
        list(QUARTILE_LEVELS)
    ].min(axis=1)
    module_coverage["eligible"] = module_coverage["minimum_quartile_pairs"].ge(
        min_pairs_per_quartile
    )
    eligible_modules = set(
        module_coverage.loc[module_coverage["eligible"], "module_code"]
    )
    analysis = with_elo.loc[with_elo["module_code"].isin(eligible_modules)].copy()
    analysis["elo_quartile"] = analysis["elo_quartile"].cat.remove_unused_categories()

    audit = pd.DataFrame(
        [
            {
                "zpdes_sequence_rows": len(zpdes),
                "zpdes_students": zpdes["student_id"].nunique(),
                "zpdes_modules": zpdes["module_code"].nunique(),
                "student_module_pairs_before_elo": len(student_module),
                "pairs_with_eligible_elo": len(with_elo),
                "pairs_retained_for_model": len(analysis),
                "students_retained": analysis["student_id"].nunique(),
                "classrooms_retained": analysis["classroom_id"].nunique(),
                "modules_retained": analysis["module_code"].nunique(),
                "min_adaptive_test_attempts": min_adaptive_test_attempts,
                "min_pairs_per_quartile": min_pairs_per_quartile,
            }
        ]
    )
    return analysis.reset_index(drop=True), audit, module_coverage


def _normal_p_value(estimate: float, standard_error: float) -> float:
    if not np.isfinite(standard_error) or standard_error <= 0:
        return math.nan
    return math.erfc(abs(estimate / standard_error) / math.sqrt(2.0))


def _fixed_effect_table(model) -> pd.DataFrame:
    raw = model.get_coef(std_err=True, format_pandas=True)
    rows = []
    for term in FIXED_EFFECT_COLUMNS:
        estimate = float(raw.loc["Param.", term])
        standard_error = float(raw.loc["Std. err.", term])
        rows.append(
            {
                "term": term,
                "estimate": estimate,
                "std_error": standard_error,
                "z_value": (
                    estimate / standard_error
                    if np.isfinite(standard_error) and standard_error > 0
                    else math.nan
                ),
                "p_value": _normal_p_value(estimate, standard_error),
                "ci_low": estimate - 1.96 * standard_error,
                "ci_high": estimate + 1.96 * standard_error,
            }
        )
    table = pd.DataFrame(rows)
    table["p_value_holm"] = math.nan
    contrast_mask = table["term"].isin(QUARTILE_LEVELS[1:])
    contrast_indices = table.index[contrast_mask].tolist()
    ordered_indices = sorted(contrast_indices, key=lambda index: table.loc[index, "p_value"])
    running_adjusted = 0.0
    n_tests = len(ordered_indices)
    for rank, index in enumerate(ordered_indices):
        adjusted = min(1.0, (n_tests - rank) * float(table.loc[index, "p_value"]))
        running_adjusted = max(running_adjusted, adjusted)
        table.loc[index, "p_value_holm"] = running_adjusted
    return table


def _variance_component_table(model) -> pd.DataFrame:
    raw = model.get_cov_pars(std_err=False, format_pandas=True)
    rows = []
    for original_name, value in raw.loc["Param."].items():
        name = str(original_name)
        for quartile in QUARTILE_LEVELS[1:]:
            if name.endswith(f"_rand_coef_{quartile}"):
                # GPBoost 1.6.8 labels Gaussian random coefficients with the
                # following group name. The C++ model still uses the requested
                # one-indexed group, which is module_title here.
                name = f"module_title_rand_coef_{quartile}"
        variance = float(value)
        rows.append(
            {
                "component": name,
                "variance": variance,
                "std_deviation": math.sqrt(max(variance, 0.0)),
            }
        )
    return pd.DataFrame(rows)


def fit_quartile_equity_model(
    analysis: pd.DataFrame,
    *,
    equivalence_margin: float = 3.0,
    maxiter: int = 300,
    trace: bool = False,
) -> QuartileEquityFit:
    """Fit pooled quartile effects with independent module-specific deviations."""

    required = {
        "mean_progress",
        "elo_quartile",
        "module_title",
        "student_id",
        "classroom_id",
    }
    missing = sorted(required.difference(analysis.columns))
    if missing:
        raise ValueError("Analysis data are missing columns: " + ", ".join(missing))
    if equivalence_margin <= 0:
        raise ValueError("equivalence_margin must be positive")
    if analysis["module_title"].nunique() < 8:
        raise ValueError(
            "At least eight modules are required for the module random-slope model"
        )
    if set(analysis["elo_quartile"].dropna().astype(str).unique()) != set(
        QUARTILE_LEVELS
    ):
        raise ValueError("All four Elo quartiles must be represented")

    try:
        import gpboost as gpb
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError("gpboost is required; install project dependencies") from exc

    quartile = analysis["elo_quartile"].astype(str)
    fixed_effects = pd.DataFrame(
        {
            "Intercept": np.ones(len(analysis), dtype=np.float64),
            "Q2": quartile.eq("Q2").to_numpy(dtype=np.float64),
            "Q3": quartile.eq("Q3").to_numpy(dtype=np.float64),
            "Q4": quartile.eq("Q4").to_numpy(dtype=np.float64),
        },
        index=analysis.index,
    )
    random_quartile_effects = fixed_effects[["Q2", "Q3", "Q4"]].copy()

    # Keep module_title as the second grouping column. GPBoost's one-indexed
    # random-coefficient mapping is therefore [2, 2, 2].
    group_data = pd.DataFrame(index=analysis.index)
    for column in ("classroom_id", "module_title", "student_id"):
        codes, _ = pd.factorize(analysis[column], sort=True)
        if np.any(codes < 0):
            raise ValueError(f"Could not encode grouping column {column}")
        group_data[column] = codes.astype(np.int32)

    response = analysis["mean_progress"].to_numpy(dtype=np.float64)
    model = gpb.GPModel(
        likelihood="gaussian",
        group_data=group_data,
        group_rand_coef_data=random_quartile_effects,
        ind_effect_group_rand_coef=[2, 2, 2],
    )
    model.fit(
        y=response,
        X=fixed_effects,
        params={
            "optimizer_cov": "lbfgs",
            "optimizer_coef": "lbfgs",
            "maxit": maxiter,
            "trace": trace,
        },
    )

    fixed = _fixed_effect_table(model)
    variance = _variance_component_table(model)
    coefficient_map = fixed.set_index("term")["estimate"].to_dict()
    adjusted_rows = []
    for quartile_name in QUARTILE_LEVELS:
        contrast_term = None if quartile_name == "Q1" else quartile_name
        difference = 0.0 if contrast_term is None else coefficient_map[contrast_term]
        contrast = (
            None
            if contrast_term is None
            else fixed.loc[fixed["term"].eq(contrast_term)].iloc[0]
        )
        adjusted_rows.append(
            {
                "elo_quartile": quartile_name,
                "adjusted_mean_progress": coefficient_map["Intercept"] + difference,
                "difference_from_q1": difference,
                "difference_ci_low": (
                    math.nan if contrast is None else float(contrast["ci_low"])
                ),
                "difference_ci_high": (
                    math.nan if contrast is None else float(contrast["ci_high"])
                ),
                "difference_p_value": (
                    math.nan if contrast is None else float(contrast["p_value"])
                ),
                "difference_p_value_holm": (
                    math.nan if contrast is None else float(contrast["p_value_holm"])
                ),
                "difference_95ci_inside_margin": (
                    pd.NA
                    if contrast is None
                    else bool(
                        contrast["ci_low"] > -equivalence_margin
                        and contrast["ci_high"] < equivalence_margin
                    )
                ),
                "equivalence_margin_points": equivalence_margin,
            }
        )
    adjusted = pd.DataFrame(adjusted_rows)

    heterogeneity_rows = []
    for quartile_name in QUARTILE_LEVELS[1:]:
        component = f"module_title_rand_coef_{quartile_name}"
        match = variance.loc[variance["component"].eq(component)]
        random_sd = math.nan if match.empty else float(match["std_deviation"].iloc[0])
        average_difference = coefficient_map[quartile_name]
        heterogeneity_rows.append(
            {
                "contrast": f"{quartile_name} - Q1",
                "average_difference": average_difference,
                "module_random_slope_sd": random_sd,
                "approx_module_difference_low": average_difference - 1.96 * random_sd,
                "approx_module_difference_high": average_difference + 1.96 * random_sd,
            }
        )
    heterogeneity = pd.DataFrame(heterogeneity_rows)

    iterations = int(model._get_num_optim_iter())
    finite_standard_errors = bool(
        np.isfinite(fixed["std_error"]).all() and fixed["std_error"].gt(0).all()
    )
    converged = iterations < maxiter
    diagnostics = pd.DataFrame(
        [
            {
                "status": "ok" if converged and finite_standard_errors else "check",
                "converged": converged,
                "finite_fixed_effect_standard_errors": finite_standard_errors,
                "iterations": iterations,
                "maxiter": maxiter,
                "log_likelihood": -float(model.get_current_neg_log_likelihood()),
                "n_student_module_pairs": len(analysis),
                "n_students": analysis["student_id"].nunique(),
                "n_classrooms": analysis["classroom_id"].nunique(),
                "n_modules": analysis["module_title"].nunique(),
                "model_specification": (
                    "Gaussian mixed model: mean_progress ~ elo_quartile; "
                    "random intercepts for classroom, student, and module; "
                    "independent module random deviations for Q2, Q3, and Q4"
                ),
            }
        ]
    )
    return QuartileEquityFit(
        model=model,
        fixed_effects=fixed,
        adjusted_quartile_means=adjusted,
        variance_components=variance,
        module_heterogeneity=heterogeneity,
        diagnostics=diagnostics,
    )

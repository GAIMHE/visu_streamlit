"""Fit an Elo-adjusted work-mode trajectory model within eligible sequences.

The analysis uses every retained first attempt in an eligible activity sequence.
Position is normalized to the interval [0, 1], so the fixed position effects
describe a start-to-end trajectory rather than a change per raw attempt number.
The primary model is a Bernoulli-logit mixed model. Work mode and position are
centered to improve numerical conditioning of the fixed-effect Hessian:

    success ~ centered_work_mode * centered_position + centered_exercise_elo
              + (1 | classroom_id)
              + (1 | student_id)
              + (1 + centered_position | sequence_id)

GPBoost represents the sequence intercept and slope as independent variance
components. The fixed work-mode-by-position interaction is the principal
adjusted comparison of the playlist and ZPDES trajectories.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from scripts.model_work_mode_first_attempt_trajectory import (
    SEGMENT_KEYS,
    build_first_attempt_trajectory,
)
from scripts.model_work_mode_progress import (
    WORK_MODES,
    _attach_exercise_elo,
    _qualify_playlist_activity_sequences,
)

FIXED_EFFECT_COLUMNS = [
    "Intercept",
    "zpdes",
    "centered_position",
    "zpdes_x_centered_position",
    "exercise_elo_centered_100",
]
GROUP_COLUMNS = ["classroom_id", "student_id", "sequence_id"]
MODEL_SPECIFICATION = (
    "Bernoulli-logit: centered work_mode * centered_position + exercise Elo; "
    "random intercepts for classroom, student, and sequence; "
    "independent random centered-position slope for sequence"
)
EMPTY_FIT_VALUES: dict[str, object] = {
    "iterations": None,
    "elo_reference": None,
    "zpdes_reference": None,
    "playlist_log_odds_slope": None,
    "zpdes_log_odds_slope": None,
    "slope_difference_log_odds": None,
    "slope_difference_std_error": None,
    "slope_difference_ci_low": None,
    "slope_difference_ci_high": None,
    "slope_difference_p_value": None,
    "playlist_adjusted_change_points": None,
    "zpdes_adjusted_change_points": None,
    "adjusted_difference_in_change_points": None,
    "slope_comparison_reportable": False,
    "probability_levels_reportable": False,
    "invalid_standard_error_terms": None,
}

SLOPE_INFERENCE_TERMS = [
    "centered_position",
    "zpdes_x_centered_position",
    "exercise_elo_centered_100",
]


@dataclass
class PreparedSequenceSlopeInputs:
    """Complete model frame and GPBoost inputs."""

    frame: pd.DataFrame
    response: np.ndarray
    fixed_effects: pd.DataFrame
    group_data: pd.DataFrame
    sequence_random_slope: pd.DataFrame
    elo_reference: float
    zpdes_reference: float


@dataclass
class SequenceSlopeFitResult:
    """Serializable model diagnostics and reporting tables."""

    summary: dict[str, object]
    fixed_effects: pd.DataFrame = field(default_factory=pd.DataFrame)
    variance_components: pd.DataFrame = field(default_factory=pd.DataFrame)
    adjusted_changes: pd.DataFrame = field(default_factory=pd.DataFrame)


def build_sequence_slope_trajectory(
    attempts: pd.DataFrame,
    exercise_elo: pd.DataFrame,
    min_sequence_exercises: int = 4,
) -> pd.DataFrame:
    """Build activity-sequence first-attempt rows for the slope model.

    This deliberately reuses the progress notebook's sequence construction:
    playlist activities are qualified by playlist id, the earliest retained
    student-exercise attempt is kept, and short sequences are removed.
    """

    if min_sequence_exercises < 2:
        raise ValueError("min_sequence_exercises must be at least 2")

    sequence_attempts = _qualify_playlist_activity_sequences(attempts)
    trajectory = build_first_attempt_trajectory(
        sequence_attempts,
        min_activity_exercises=min_sequence_exercises,
        sequence_scope="activity",
    )
    trajectory = _attach_exercise_elo(trajectory, exercise_elo)
    if trajectory.empty:
        trajectory["normalized_position"] = pd.Series(dtype="float32")
        trajectory["sequence_id"] = pd.Series(dtype="int32")
        return trajectory

    denominators = trajectory["segment_exercises"].astype(float) - 1.0
    trajectory["normalized_position"] = (
        trajectory["attempt_position"].astype(float) / denominators
    ).astype("float32")
    sequence_codes = trajectory.groupby(
        SEGMENT_KEYS,
        observed=True,
        sort=False,
    ).ngroup()
    if sequence_codes.isna().any():
        raise ValueError("Could not create a sequence id for every trajectory row")
    trajectory["sequence_id"] = sequence_codes.astype("int32")

    if not trajectory["normalized_position"].between(0.0, 1.0).all():
        raise ValueError("normalized_position must lie between 0 and 1")
    return trajectory


def prepare_sequence_slope_inputs(
    trajectory: pd.DataFrame,
) -> PreparedSequenceSlopeInputs:
    """Validate trajectory rows and construct GPBoost model matrices."""

    required = {
        "success",
        "work_mode",
        "normalized_position",
        "exercise_elo",
        *GROUP_COLUMNS,
    }
    missing = sorted(required.difference(trajectory.columns))
    if missing:
        raise ValueError("Missing sequence-slope columns: " + ", ".join(missing))

    model_df = trajectory.dropna(subset=list(required)).copy()
    if model_df.empty:
        raise ValueError("No complete sequence-slope rows are available")
    observed_modes = set(model_df["work_mode"].astype(str).unique())
    if observed_modes != set(WORK_MODES):
        raise ValueError("Sequence-slope modeling requires playlist and zpdes rows")
    success_values = set(model_df["success"].astype(float).unique())
    if not success_values.issubset({0.0, 1.0}):
        raise ValueError("success must be binary (0/1)")
    if not model_df["normalized_position"].between(0.0, 1.0).all():
        raise ValueError("normalized_position must lie between 0 and 1")
    modes_per_sequence = model_df.groupby("sequence_id", observed=True)["work_mode"].nunique()
    if not modes_per_sequence.eq(1).all():
        raise ValueError("Every sequence_id must contain exactly one work mode")
    classrooms_per_student = model_df.groupby("student_id", observed=True)["classroom_id"].nunique()
    if not classrooms_per_student.empty and classrooms_per_student.max() > 1:
        count = int((classrooms_per_student > 1).sum())
        raise ValueError(f"Student random effects invalid: {count} students span classrooms")

    elo_reference = float(model_df["exercise_elo"].mean())
    centered_elo = (model_df["exercise_elo"].astype(float) - elo_reference) / 100.0
    if not np.isfinite(centered_elo).all() or centered_elo.nunique() < 2:
        raise ValueError("exercise_elo must contain at least two finite values")

    zpdes = model_df["work_mode"].eq("zpdes").astype(float)
    zpdes_reference = float(zpdes.mean())
    centered_zpdes = zpdes - zpdes_reference
    centered_position = model_df["normalized_position"].astype(float) - 0.5
    model_df["centered_zpdes"] = centered_zpdes
    model_df["centered_position"] = centered_position
    fixed_effects = pd.DataFrame(
        {
            "Intercept": np.ones(len(model_df), dtype=np.float64),
            "zpdes": centered_zpdes.to_numpy(dtype=np.float64),
            "centered_position": centered_position.to_numpy(dtype=np.float64),
            "zpdes_x_centered_position": (centered_zpdes * centered_position).to_numpy(
                dtype=np.float64
            ),
            "exercise_elo_centered_100": centered_elo.to_numpy(dtype=np.float64),
        },
        index=model_df.index,
    )

    group_data = pd.DataFrame(index=model_df.index)
    for column in GROUP_COLUMNS:
        codes, _ = pd.factorize(model_df[column], sort=True)
        if np.any(codes < 0):
            raise ValueError(f"Could not encode grouping column {column}")
        group_data[column] = codes.astype(np.int32)

    sequence_random_slope = pd.DataFrame(
        {"centered_position": centered_position.to_numpy(dtype=np.float64)},
        index=model_df.index,
    )
    return PreparedSequenceSlopeInputs(
        frame=model_df,
        response=model_df["success"].to_numpy(dtype=np.float64),
        fixed_effects=fixed_effects,
        group_data=group_data,
        sequence_random_slope=sequence_random_slope,
        elo_reference=elo_reference,
        zpdes_reference=zpdes_reference,
    )


def fit_sequence_slope_model(
    trajectory: pd.DataFrame,
    population: str,
    maxiter: int = 200,
    trace: bool = False,
) -> SequenceSlopeFitResult:
    """Fit the adjusted Bernoulli-logit sequence-slope model."""

    base_summary = _base_summary(trajectory, population)
    try:
        prepared = prepare_sequence_slope_inputs(trajectory)
        base_summary = _base_summary(prepared.frame, population)
        base_summary["trajectory_rows"] = len(trajectory)
        import gpboost as gpb

        model = gpb.GPModel(
            likelihood="bernoulli_logit",
            group_data=prepared.group_data,
            group_rand_coef_data=prepared.sequence_random_slope,
            ind_effect_group_rand_coef=[3],
        )
        model.fit(
            y=prepared.response,
            X=prepared.fixed_effects,
            params={
                "optimizer_cov": "lbfgs",
                "optimizer_coef": "lbfgs",
                "maxit": maxiter,
                "trace": trace,
            },
        )
        coefficient_table = _coefficient_table(model, population)
        variance_table = _variance_component_table(model, population)
        iterations = int(model._get_num_optim_iter())
        converged = iterations < maxiter
    except Exception as exc:
        return SequenceSlopeFitResult(
            summary={
                **base_summary,
                **EMPTY_FIT_VALUES,
                "status": "failed",
                "converged": False,
                "reportable": False,
                "error": str(exc),
            }
        )

    adjusted_changes = build_adjusted_probability_summary(
        coefficient_table,
        population=population,
        elo_reference=prepared.elo_reference,
        zpdes_reference=prepared.zpdes_reference,
    )
    coefficient_map = coefficient_table.set_index("term")["estimate"].to_dict()
    interaction = coefficient_table.loc[
        coefficient_table["term"].eq("zpdes_x_centered_position")
    ].iloc[0]
    playlist_change = float(
        adjusted_changes.loc[
            adjusted_changes["work_mode"].eq("playlist"), "adjusted_change_points"
        ].iloc[0]
    )
    zpdes_change = float(
        adjusted_changes.loc[
            adjusted_changes["work_mode"].eq("zpdes"), "adjusted_change_points"
        ].iloc[0]
    )
    interaction_se = float(interaction["std_error"])
    standard_error_diagnostics = _standard_error_diagnostics(
        coefficient_table,
        converged=converged,
    )
    invalid_standard_error_terms = standard_error_diagnostics["invalid_standard_error_terms"]
    playlist_contrast = -prepared.zpdes_reference
    zpdes_contrast = 1.0 - prepared.zpdes_reference
    return SequenceSlopeFitResult(
        summary={
            **base_summary,
            "status": "ok" if converged else "not_converged",
            "converged": converged,
            "reportable": standard_error_diagnostics["slope_comparison_reportable"],
            "slope_comparison_reportable": standard_error_diagnostics[
                "slope_comparison_reportable"
            ],
            "probability_levels_reportable": standard_error_diagnostics[
                "probability_levels_reportable"
            ],
            "iterations": iterations,
            "elo_reference": prepared.elo_reference,
            "zpdes_reference": prepared.zpdes_reference,
            "playlist_log_odds_slope": (
                coefficient_map["centered_position"]
                + coefficient_map["zpdes_x_centered_position"] * playlist_contrast
            ),
            "zpdes_log_odds_slope": (
                coefficient_map["centered_position"]
                + coefficient_map["zpdes_x_centered_position"] * zpdes_contrast
            ),
            "slope_difference_log_odds": float(interaction["estimate"]),
            "slope_difference_std_error": interaction_se,
            "slope_difference_ci_low": float(interaction["ci_low"]),
            "slope_difference_ci_high": float(interaction["ci_high"]),
            "slope_difference_p_value": float(interaction["p_value"]),
            "playlist_adjusted_change_points": playlist_change,
            "zpdes_adjusted_change_points": zpdes_change,
            "adjusted_difference_in_change_points": zpdes_change - playlist_change,
            "invalid_standard_error_terms": (
                ", ".join(invalid_standard_error_terms) if invalid_standard_error_terms else None
            ),
            "error": None if converged else "Optimizer reached the maximum iteration count.",
        },
        fixed_effects=coefficient_table,
        variance_components=variance_table,
        adjusted_changes=adjusted_changes,
    )


def build_adjusted_probability_summary(
    fixed_effects: pd.DataFrame,
    population: str,
    elo_reference: float,
    zpdes_reference: float,
) -> pd.DataFrame:
    """Convert fixed log-odds coefficients into mean-Elo probability changes."""

    coefficient_map = fixed_effects.set_index("term")["estimate"].to_dict()
    missing = [column for column in FIXED_EFFECT_COLUMNS if column not in coefficient_map]
    if missing:
        raise ValueError("Missing fixed-effect coefficients: " + ", ".join(missing))

    rows = []
    for work_mode, zpdes in (("playlist", 0.0), ("zpdes", 1.0)):
        centered_zpdes = zpdes - zpdes_reference
        midpoint_linear_predictor = (
            coefficient_map["Intercept"] + coefficient_map["zpdes"] * centered_zpdes
        )
        mode_slope = (
            coefficient_map["centered_position"]
            + coefficient_map["zpdes_x_centered_position"] * centered_zpdes
        )
        start_linear_predictor = midpoint_linear_predictor - 0.5 * mode_slope
        end_linear_predictor = midpoint_linear_predictor + 0.5 * mode_slope
        start_probability = _expit_scalar(start_linear_predictor)
        end_probability = _expit_scalar(end_linear_predictor)
        rows.append(
            {
                "population": population,
                "work_mode": work_mode,
                "elo_reference": elo_reference,
                "zpdes_reference": zpdes_reference,
                "adjusted_start_probability": start_probability,
                "adjusted_end_probability": end_probability,
                "adjusted_change_points": (end_probability - start_probability) * 100.0,
            }
        )
    return pd.DataFrame(rows)


def _standard_error_diagnostics(
    coefficient_table: pd.DataFrame,
    *,
    converged: bool,
) -> dict[str, object]:
    """Separate primary slope inference from absolute probability-level inference."""

    standard_errors = coefficient_table.set_index("term")["std_error"]
    invalid_mask = ~np.isfinite(standard_errors) | standard_errors.le(0.0)
    invalid_terms = standard_errors.index[invalid_mask].tolist()
    missing_slope_terms = [
        term for term in SLOPE_INFERENCE_TERMS if term not in standard_errors.index
    ]
    valid_slope_terms = not missing_slope_terms and all(
        np.isfinite(float(standard_errors.loc[term])) and float(standard_errors.loc[term]) > 0.0
        for term in SLOPE_INFERENCE_TERMS
    )
    valid_all_terms = bool(
        len(standard_errors) > 0
        and np.isfinite(standard_errors.to_numpy(dtype=float)).all()
        and (standard_errors.to_numpy(dtype=float) > 0.0).all()
    )
    return {
        "slope_comparison_reportable": bool(converged and valid_slope_terms),
        "probability_levels_reportable": bool(converged and valid_all_terms),
        "invalid_standard_error_terms": invalid_terms,
        "missing_slope_standard_error_terms": missing_slope_terms,
    }


def _base_summary(trajectory: pd.DataFrame, population: str) -> dict[str, object]:
    return {
        "population": population,
        "trajectory_rows": len(trajectory),
        "n_rows": len(trajectory),
        "n_sequences": trajectory["sequence_id"].nunique() if "sequence_id" in trajectory else 0,
        "n_students": trajectory["student_id"].nunique() if "student_id" in trajectory else 0,
        "n_classrooms": (
            trajectory["classroom_id"].nunique() if "classroom_id" in trajectory else 0
        ),
        "n_modules": trajectory["module"].nunique() if "module" in trajectory else 0,
        "model_specification": MODEL_SPECIFICATION,
    }


def _coefficient_table(model, population: str) -> pd.DataFrame:
    coefficients = model.get_coef(std_err=True, format_pandas=True).transpose().reset_index()
    coefficients = coefficients.rename(
        columns={"index": "term", "Param.": "estimate", "Std. err.": "std_error"}
    )
    coefficients.insert(0, "population", population)
    coefficients["z_value"] = coefficients["estimate"] / coefficients["std_error"]
    coefficients["p_value"] = coefficients["z_value"].map(
        lambda value: math.erfc(abs(value) / math.sqrt(2.0)) if np.isfinite(value) else math.nan
    )
    coefficients["ci_low"] = coefficients["estimate"] - 1.96 * coefficients["std_error"]
    coefficients["ci_high"] = coefficients["estimate"] + 1.96 * coefficients["std_error"]
    coefficients["odds_ratio"] = np.exp(coefficients["estimate"])
    return coefficients


def _variance_component_table(model, population: str) -> pd.DataFrame:
    variance = model.get_cov_pars(std_err=False, format_pandas=True).transpose().reset_index()
    variance = variance.rename(columns={"index": "group", "Param.": "variance"})
    variance.insert(0, "population", population)
    return variance


def _expit_scalar(value: float) -> float:
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-value))
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)

"""Compare continuous and binary-half work-mode trajectories on identical rows.

Both models use the same Bernoulli-logit likelihood, fixed-effect structure,
grouping factors, optimizer, and sequence random-coefficient structure. The
only intended difference is the position encoding:

* ``continuous`` uses normalized sequence position from 0 to 1;
* ``binary_half`` uses 0 for the first half and 1 for the later half.

For odd-length sequences, the exact midpoint is assigned to the first half.
This retains every eligible attempt row in both fits.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from scripts.model_work_mode_first_attempt_trajectory import (
    SEGMENT_KEYS,
    build_first_attempt_trajectory,
)
from scripts.model_work_mode_progress import WORK_MODES, _qualify_playlist_activity_sequences
from scripts.model_work_mode_sequence_slope import (
    GROUP_COLUMNS,
    _coefficient_table,
    _expit_scalar,
    _variance_component_table,
)

CONTINUOUS = "continuous"
BINARY_HALF = "binary_half"
POSITION_ENCODINGS = (CONTINUOUS, BINARY_HALF)
FIXED_EFFECT_COLUMNS = [
    "Intercept",
    "zpdes",
    "position",
    "zpdes_x_position",
]
INTERACTION_INFERENCE_TERMS = [
    "position",
    "zpdes_x_position",
]


@dataclass
class PreparedPositionInputs:
    """Complete GPBoost inputs for one position encoding."""

    frame: pd.DataFrame
    response: np.ndarray
    fixed_effects: pd.DataFrame
    group_data: pd.DataFrame
    sequence_random_coefficient: pd.DataFrame
    position_encoding: str
    zpdes_reference: float
    position_reference: float


@dataclass
class PositionFitResult:
    """Serializable result for one matched position model."""

    summary: dict[str, object]
    fixed_effects: pd.DataFrame = field(default_factory=pd.DataFrame)
    variance_components: pd.DataFrame = field(default_factory=pd.DataFrame)
    adjusted_changes: pd.DataFrame = field(default_factory=pd.DataFrame)


def build_position_comparison_trajectory(
    attempts: pd.DataFrame,
    min_sequence_exercises: int = 4,
) -> pd.DataFrame:
    """Build one eligible trajectory without requiring or attaching exercise Elo."""

    if min_sequence_exercises < 2:
        raise ValueError("min_sequence_exercises must be at least 2")

    sequence_attempts = _qualify_playlist_activity_sequences(attempts)
    trajectory = build_first_attempt_trajectory(
        sequence_attempts,
        min_activity_exercises=min_sequence_exercises,
        sequence_scope="activity",
    )
    if trajectory.empty:
        trajectory["normalized_position"] = pd.Series(dtype="float32")
        trajectory["sequence_id"] = pd.Series(dtype="int32")
        return add_binary_half_indicator(trajectory)

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
    return add_binary_half_indicator(trajectory)


def add_binary_half_indicator(trajectory: pd.DataFrame) -> pd.DataFrame:
    """Assign positions above 0.5 to the later half without dropping midpoints."""

    if "normalized_position" not in trajectory:
        raise ValueError("trajectory must contain normalized_position")
    positions = pd.to_numeric(trajectory["normalized_position"], errors="coerce")
    if positions.isna().any() or not positions.between(0.0, 1.0).all():
        raise ValueError("normalized_position must contain finite values between 0 and 1")

    output = trajectory.copy()
    output["later_half"] = positions.gt(0.5).astype("int8")
    output["is_exact_midpoint"] = positions.eq(0.5)
    return output


def build_half_assignment_audit(trajectory: pd.DataFrame) -> pd.DataFrame:
    """Summarize the row-preserving binary-half assignment by work mode."""

    required = {"work_mode", "later_half", "is_exact_midpoint", "sequence_id", "student_id"}
    missing = sorted(required.difference(trajectory.columns))
    if missing:
        raise ValueError("Missing half-audit columns: " + ", ".join(missing))

    audit = (
        trajectory.groupby(["work_mode", "later_half"], as_index=False, observed=True)
        .agg(
            attempt_rows=("sequence_id", "size"),
            sequences=("sequence_id", "nunique"),
            students=("student_id", "nunique"),
            exact_midpoint_rows=("is_exact_midpoint", "sum"),
        )
        .sort_values(["work_mode", "later_half"])
        .reset_index(drop=True)
    )
    audit["half"] = audit["later_half"].map({0: "first", 1: "later"})
    return audit[
        [
            "work_mode",
            "half",
            "attempt_rows",
            "sequences",
            "students",
            "exact_midpoint_rows",
        ]
    ]


def prepare_position_inputs(
    trajectory: pd.DataFrame,
    position_encoding: str,
) -> PreparedPositionInputs:
    """Construct matched model matrices for one position representation."""

    if position_encoding not in POSITION_ENCODINGS:
        raise ValueError(f"position_encoding must be one of {POSITION_ENCODINGS}")
    if "later_half" not in trajectory or "is_exact_midpoint" not in trajectory:
        trajectory = add_binary_half_indicator(trajectory)

    required = {
        "success",
        "work_mode",
        "normalized_position",
        "later_half",
        *GROUP_COLUMNS,
    }
    missing = sorted(required.difference(trajectory.columns))
    if missing:
        raise ValueError("Missing position-model columns: " + ", ".join(missing))

    model_df = trajectory.dropna(subset=list(required)).copy()
    if model_df.empty:
        raise ValueError("No complete position-model rows are available")
    if set(model_df["work_mode"].astype(str).unique()) != set(WORK_MODES):
        raise ValueError("Position modeling requires playlist and zpdes rows")
    success_values = set(model_df["success"].astype(float).unique())
    if not success_values.issubset({0.0, 1.0}):
        raise ValueError("success must be binary (0/1)")
    if not model_df["normalized_position"].astype(float).between(0.0, 1.0).all():
        raise ValueError("normalized_position must lie between 0 and 1")
    if not set(model_df["later_half"].astype(float).unique()).issubset({0.0, 1.0}):
        raise ValueError("later_half must be binary (0/1)")

    modes_per_sequence = model_df.groupby("sequence_id", observed=True)["work_mode"].nunique()
    if not modes_per_sequence.eq(1).all():
        raise ValueError("Every sequence_id must contain exactly one work mode")
    classrooms_per_student = model_df.groupby("student_id", observed=True)["classroom_id"].nunique()
    if not classrooms_per_student.empty and classrooms_per_student.max() > 1:
        count = int((classrooms_per_student > 1).sum())
        raise ValueError(f"Student random effects invalid: {count} students span classrooms")

    zpdes = model_df["work_mode"].eq("zpdes").astype(float)
    zpdes_reference = float(zpdes.mean())
    centered_zpdes = zpdes - zpdes_reference
    raw_position = (
        model_df["normalized_position"].astype(float)
        if position_encoding == CONTINUOUS
        else model_df["later_half"].astype(float)
    )
    position_reference = 0.5
    centered_position = raw_position - position_reference

    model_df["centered_zpdes"] = centered_zpdes
    model_df["model_position"] = raw_position
    model_df["centered_model_position"] = centered_position
    fixed_effects = pd.DataFrame(
        {
            "Intercept": np.ones(len(model_df), dtype=np.float64),
            "zpdes": centered_zpdes.to_numpy(dtype=np.float64),
            "position": centered_position.to_numpy(dtype=np.float64),
            "zpdes_x_position": (centered_zpdes * centered_position).to_numpy(dtype=np.float64),
        },
        index=model_df.index,
    )

    group_data = pd.DataFrame(index=model_df.index)
    for column in GROUP_COLUMNS:
        codes, _ = pd.factorize(model_df[column], sort=True)
        if np.any(codes < 0):
            raise ValueError(f"Could not encode grouping column {column}")
        group_data[column] = codes.astype(np.int32)

    sequence_random_coefficient = pd.DataFrame(
        {"position": centered_position.to_numpy(dtype=np.float64)},
        index=model_df.index,
    )
    return PreparedPositionInputs(
        frame=model_df,
        response=model_df["success"].to_numpy(dtype=np.float64),
        fixed_effects=fixed_effects,
        group_data=group_data,
        sequence_random_coefficient=sequence_random_coefficient,
        position_encoding=position_encoding,
        zpdes_reference=zpdes_reference,
        position_reference=position_reference,
    )


def fit_position_model(
    trajectory: pd.DataFrame,
    position_encoding: str,
    population: str,
    maxiter: int = 200,
    trace: bool = False,
) -> PositionFitResult:
    """Fit one of the two otherwise-matched Bernoulli-logit models."""

    base_summary = _base_summary(trajectory, population, position_encoding)
    try:
        prepared = prepare_position_inputs(trajectory, position_encoding)
        base_summary = _base_summary(prepared.frame, population, position_encoding)
        base_summary["trajectory_rows"] = len(trajectory)

        import gpboost as gpb

        model = gpb.GPModel(
            likelihood="bernoulli_logit",
            group_data=prepared.group_data,
            group_rand_coef_data=prepared.sequence_random_coefficient,
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
        coefficient_table.insert(1, "position_encoding", position_encoding)
        variance_table = _variance_component_table(model, population)
        variance_table.insert(1, "position_encoding", position_encoding)
        iterations = int(model._get_num_optim_iter())
        converged = iterations < maxiter
    except Exception as exc:
        return PositionFitResult(
            summary={
                **base_summary,
                **_empty_fit_values(),
                "status": "failed",
                "converged": False,
                "reportable": False,
                "error": str(exc),
            }
        )

    adjusted_changes = build_adjusted_change_summary(
        coefficient_table,
        position_encoding=position_encoding,
        population=population,
        zpdes_reference=prepared.zpdes_reference,
        position_reference=prepared.position_reference,
    )
    coefficient_map = coefficient_table.set_index("term")["estimate"].to_dict()
    interaction = coefficient_table.loc[coefficient_table["term"].eq("zpdes_x_position")].iloc[0]
    diagnostics = _standard_error_diagnostics(coefficient_table, converged=converged)
    playlist_contrast = -prepared.zpdes_reference
    zpdes_contrast = 1.0 - prepared.zpdes_reference
    playlist_change = _mode_change(adjusted_changes, "playlist")
    zpdes_change = _mode_change(adjusted_changes, "zpdes")

    return PositionFitResult(
        summary={
            **base_summary,
            "status": "ok" if converged else "not_converged",
            "converged": converged,
            "reportable": diagnostics["interaction_reportable"],
            "interaction_reportable": diagnostics["interaction_reportable"],
            "probability_levels_reportable": diagnostics["probability_levels_reportable"],
            "iterations": iterations,
            "zpdes_reference": prepared.zpdes_reference,
            "position_reference": prepared.position_reference,
            "playlist_log_odds_change": (
                coefficient_map["position"]
                + coefficient_map["zpdes_x_position"] * playlist_contrast
            ),
            "zpdes_log_odds_change": (
                coefficient_map["position"] + coefficient_map["zpdes_x_position"] * zpdes_contrast
            ),
            "interaction_log_odds": float(interaction["estimate"]),
            "interaction_std_error": float(interaction["std_error"]),
            "interaction_ci_low": float(interaction["ci_low"]),
            "interaction_ci_high": float(interaction["ci_high"]),
            "interaction_p_value": float(interaction["p_value"]),
            "playlist_adjusted_change_points": playlist_change,
            "zpdes_adjusted_change_points": zpdes_change,
            "adjusted_difference_in_change_points": zpdes_change - playlist_change,
            "invalid_standard_error_terms": (
                ", ".join(diagnostics["invalid_standard_error_terms"])
                if diagnostics["invalid_standard_error_terms"]
                else None
            ),
            "error": None if converged else "Optimizer reached the maximum iteration count.",
        },
        fixed_effects=coefficient_table,
        variance_components=variance_table,
        adjusted_changes=adjusted_changes,
    )


def build_adjusted_change_summary(
    fixed_effects: pd.DataFrame,
    *,
    position_encoding: str,
    population: str,
    zpdes_reference: float,
    position_reference: float = 0.5,
) -> pd.DataFrame:
    """Convert fixed log-odds coefficients to matched endpoint/half changes."""

    if position_encoding not in POSITION_ENCODINGS:
        raise ValueError(f"position_encoding must be one of {POSITION_ENCODINGS}")
    coefficient_map = fixed_effects.set_index("term")["estimate"].to_dict()
    missing = [column for column in FIXED_EFFECT_COLUMNS if column not in coefficient_map]
    if missing:
        raise ValueError("Missing fixed-effect coefficients: " + ", ".join(missing))

    period_0_label, period_1_label = (
        ("start", "end") if position_encoding == CONTINUOUS else ("first_half", "later_half")
    )
    rows = []
    for work_mode, zpdes in (("playlist", 0.0), ("zpdes", 1.0)):
        centered_zpdes = zpdes - zpdes_reference
        mode_intercept = coefficient_map["Intercept"] + coefficient_map["zpdes"] * centered_zpdes
        mode_change = (
            coefficient_map["position"] + coefficient_map["zpdes_x_position"] * centered_zpdes
        )
        period_0_log_odds = mode_intercept + (0.0 - position_reference) * mode_change
        period_1_log_odds = mode_intercept + (1.0 - position_reference) * mode_change
        period_0_probability = _expit_scalar(period_0_log_odds)
        period_1_probability = _expit_scalar(period_1_log_odds)
        rows.append(
            {
                "population": population,
                "position_encoding": position_encoding,
                "work_mode": work_mode,
                "period_0_label": period_0_label,
                "period_1_label": period_1_label,
                "adjusted_period_0_probability": period_0_probability,
                "adjusted_period_1_probability": period_1_probability,
                "adjusted_change_points": (period_1_probability - period_0_probability) * 100.0,
            }
        )
    return pd.DataFrame(rows)


def build_adjusted_probability_curve(
    fixed_effects: pd.DataFrame,
    *,
    position_encoding: str,
    population: str,
    zpdes_reference: float,
    grid_points: int = 101,
    position_reference: float = 0.5,
) -> pd.DataFrame:
    """Return fixed-effect predictions over normalized position."""

    if position_encoding not in POSITION_ENCODINGS:
        raise ValueError(f"position_encoding must be one of {POSITION_ENCODINGS}")
    if grid_points < 2:
        raise ValueError("grid_points must be at least 2")
    coefficient_map = fixed_effects.set_index("term")["estimate"].to_dict()
    missing = [column for column in FIXED_EFFECT_COLUMNS if column not in coefficient_map]
    if missing:
        raise ValueError("Missing fixed-effect coefficients: " + ", ".join(missing))

    normalized_positions = np.linspace(0.0, 1.0, grid_points)
    model_positions = (
        normalized_positions
        if position_encoding == CONTINUOUS
        else (normalized_positions > 0.5).astype(float)
    )
    rows = []
    for work_mode, zpdes in (("playlist", 0.0), ("zpdes", 1.0)):
        centered_zpdes = zpdes - zpdes_reference
        for normalized_position, model_position in zip(
            normalized_positions, model_positions, strict=True
        ):
            centered_position = model_position - position_reference
            linear_predictor = (
                coefficient_map["Intercept"]
                + coefficient_map["zpdes"] * centered_zpdes
                + coefficient_map["position"] * centered_position
                + coefficient_map["zpdes_x_position"] * centered_zpdes * centered_position
            )
            rows.append(
                {
                    "population": population,
                    "position_encoding": position_encoding,
                    "work_mode": work_mode,
                    "normalized_position": normalized_position,
                    "model_position": model_position,
                    "adjusted_probability": _expit_scalar(linear_predictor),
                }
            )
    return pd.DataFrame(rows)


def _standard_error_diagnostics(
    coefficient_table: pd.DataFrame,
    *,
    converged: bool,
) -> dict[str, object]:
    standard_errors = coefficient_table.set_index("term")["std_error"]
    invalid_mask = ~np.isfinite(standard_errors) | standard_errors.le(0.0)
    invalid_terms = standard_errors.index[invalid_mask].tolist()
    missing_terms = [
        term for term in INTERACTION_INFERENCE_TERMS if term not in standard_errors.index
    ]
    valid_interaction_terms = not missing_terms and all(
        np.isfinite(float(standard_errors.loc[term])) and float(standard_errors.loc[term]) > 0.0
        for term in INTERACTION_INFERENCE_TERMS
    )
    valid_all_terms = bool(
        len(standard_errors) > 0
        and np.isfinite(standard_errors.to_numpy(dtype=float)).all()
        and (standard_errors.to_numpy(dtype=float) > 0.0).all()
    )
    return {
        "interaction_reportable": bool(converged and valid_interaction_terms),
        "probability_levels_reportable": bool(converged and valid_all_terms),
        "invalid_standard_error_terms": invalid_terms,
        "missing_interaction_standard_error_terms": missing_terms,
    }


def _base_summary(
    trajectory: pd.DataFrame,
    population: str,
    position_encoding: str,
) -> dict[str, object]:
    position_definition = (
        "normalized_position from 0 to 1"
        if position_encoding == CONTINUOUS
        else "later_half = 1 when normalized_position > 0.5; midpoint retained in first half"
    )
    return {
        "population": population,
        "position_encoding": position_encoding,
        "position_definition": position_definition,
        "trajectory_rows": len(trajectory),
        "n_rows": len(trajectory),
        "n_sequences": trajectory["sequence_id"].nunique() if "sequence_id" in trajectory else 0,
        "n_students": trajectory["student_id"].nunique() if "student_id" in trajectory else 0,
        "n_classrooms": (
            trajectory["classroom_id"].nunique() if "classroom_id" in trajectory else 0
        ),
        "n_modules": trajectory["module"].nunique() if "module" in trajectory else 0,
        "model_specification": (
            "Bernoulli-logit: centered work mode * centered model position; "
            "random intercepts for classroom, student, and sequence; independent random "
            f"{position_encoding} coefficient for sequence"
        ),
    }


def _mode_change(adjusted_changes: pd.DataFrame, work_mode: str) -> float:
    return float(
        adjusted_changes.loc[
            adjusted_changes["work_mode"].eq(work_mode), "adjusted_change_points"
        ].iloc[0]
    )


def _empty_fit_values() -> dict[str, object]:
    return {
        "iterations": None,
        "zpdes_reference": None,
        "position_reference": None,
        "playlist_log_odds_change": None,
        "zpdes_log_odds_change": None,
        "interaction_log_odds": None,
        "interaction_std_error": None,
        "interaction_ci_low": None,
        "interaction_ci_high": None,
        "interaction_p_value": None,
        "playlist_adjusted_change_points": None,
        "zpdes_adjusted_change_points": None,
        "adjusted_difference_in_change_points": None,
        "interaction_reportable": False,
        "probability_levels_reportable": False,
        "invalid_standard_error_terms": None,
    }

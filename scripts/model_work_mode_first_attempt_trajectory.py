"""Fit the combined first-attempt work-mode trajectory model.

The analysis unit is one first retained attempt of an exercise. Trajectories can
be built within student x activity or student x module timelines and are split
into contiguous work-mode runs. The combined notebook uses module-scoped runs.
The zero-based run position is used in a Bernoulli-logit mixed model:

    success ~ work_mode * attempt_position
              + (1 | classroom_id)
              + (1 | student_id within classroom_id)

Activity, module, and exercise are deliberately not adjusted for because their
selection is part of the work-mode mechanism being evaluated. GPBoost represents
classroom and nested student identifiers as separate ``group_data`` columns.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

WORK_MODES = ("playlist", "zpdes")
SEGMENT_KEYS = [
    "student_id",
    "module",
    "activity_id",
    "activity_sequence_id",
    "work_mode",
]
MODULE_SEGMENT_KEYS = [
    "student_id",
    "module",
    "module_sequence_id",
    "work_mode",
]
FIXED_EFFECT_COLUMNS = [
    "Intercept",
    "zpdes",
    "attempt_position",
    "zpdes_x_attempt_position",
]
GROUP_COLUMNS = [
    "classroom_id",
    "student_in_classroom",
]


@dataclass
class TrajectoryFitResult:
    """Serializable model diagnostics plus fixed and random-effect tables."""

    summary: dict[str, object]
    fixed_effects: pd.DataFrame = field(default_factory=pd.DataFrame)
    variance_components: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class TrajectoryDataset:
    """One memory-bounded population extract and its filtering audit."""

    frame: pd.DataFrame
    audit: dict[str, object]
    population_audit: pd.DataFrame = field(default_factory=pd.DataFrame)


def _validate_sequence_scope(sequence_scope: str) -> None:
    if sequence_scope not in {"activity", "module"}:
        raise ValueError("sequence_scope must be 'activity' or 'module'")


def _sequence_sql_parts(sequence_scope: str) -> tuple[str, str, str, str]:
    _validate_sequence_scope(sequence_scope)
    if sequence_scope == "module":
        timeline_partition = "student_id"
        sequence_id_column = "module_sequence_id"
        boundary_condition = """
            lag(work_mode) OVER (
                PARTITION BY student_id
                ORDER BY created_at, source_order, exercise_id
            ) IS DISTINCT FROM work_mode
            OR lag(module) OVER (
                PARTITION BY student_id
                ORDER BY created_at, source_order, exercise_id
            ) IS DISTINCT FROM module
        """
        position_partition = "student_id, module, module_sequence_id"
        return (
            timeline_partition,
            sequence_id_column,
            boundary_condition,
            position_partition,
        )

    timeline_partition = "student_id, module, activity_id"
    sequence_id_column = "activity_sequence_id"
    boundary_condition = """
        lag(work_mode) OVER (
            PARTITION BY student_id, module, activity_id
            ORDER BY created_at, source_order, exercise_id
        ) IS DISTINCT FROM work_mode
    """
    position_partition = "student_id, module, activity_id, activity_sequence_id"
    return (
        timeline_partition,
        sequence_id_column,
        boundary_condition,
        position_partition,
    )


def _trajectory_segment_keys(frame: pd.DataFrame) -> list[str]:
    if "module_sequence_id" in frame.columns:
        return MODULE_SEGMENT_KEYS
    return SEGMENT_KEYS


def _load_mia_first_attempt_trajectory_in_process(
    input_file: Path,
    exercise_catalog_json: Path,
    module_config_json: Path,
    population: str,
    min_unique_exercises: int | None = None,
    min_activity_exercises: int = 4,
    keep_only_single_module_playlists: bool = False,
    sequence_scope: str = "activity",
) -> TrajectoryDataset:
    """Build one trajectory population directly from the raw MIA parquet.

    This SQL path reproduces the existing raw-MIA loader, population split, and
    student eligibility rules while avoiding a 5M-row pandas attempt table.
    DuckDB performs the combined extraction without building the raw attempt
    table in pandas.
    """

    allowed_populations = {"exclusive_modes", "both_modes", "combined"}
    if population not in allowed_populations:
        raise ValueError(f"population must be one of {sorted(allowed_populations)}")
    if min_unique_exercises is not None and min_unique_exercises < 1:
        raise ValueError("min_unique_exercises must be at least 1 when provided")
    if min_activity_exercises < 1:
        raise ValueError("min_activity_exercises must be at least 1")
    (
        sequence_partition,
        sequence_id_column,
        boundary_condition,
        position_partition,
    ) = _sequence_sql_parts(sequence_scope)
    if not input_file.exists():
        raise FileNotFoundError(input_file)

    from scripts.model_work_mode_progress import _read_mia_exercise_catalog

    catalog = _read_mia_exercise_catalog(exercise_catalog_json, module_config_json)
    catalog = catalog.rename(
        columns={
            "catalog_module": "catalog_module_name",
            "catalog_activity_id": "catalog_activity",
        }
    )

    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit = '4GB'")
        connection.execute("SET threads = 2")
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("SET preserve_insertion_order = false")
        connection.register("exercise_catalog", catalog)
        playlist_condition = (
            "work_mode <> 'playlist' OR playlist_id IN "
            "(SELECT playlist_id FROM single_module_playlists)"
            if keep_only_single_module_playlists
            else "true"
        )
        student_eligibility_having = (
            ""
            if min_unique_exercises is None
            else (
                "HAVING count(DISTINCT exercise_id) >= "
                f"{int(min_unique_exercises)}"
            )
        )
        population_condition = (
            "true" if population == "combined" else "population = ?"
        )
        parameters: list[object] = [str(input_file)]
        if population != "combined":
            parameters.append(population)

        connection.execute(
            f"""
            CREATE TEMP TABLE eligible_attempts AS
            WITH standardized AS (
                SELECT
                    CAST(raw.user_id AS VARCHAR) AS student_id,
                    COALESCE(CAST(raw.classroom_id AS VARCHAR), 'missing_classroom')
                        AS classroom_id,
                    CAST(raw.exercise_id AS VARCHAR) AS exercise_id,
                    COALESCE(
                        NULLIF(TRIM(CAST(raw.activity_id AS VARCHAR)), ''),
                        catalog.catalog_activity
                    ) AS activity_id,
                    COALESCE(
                        NULLIF(TRIM(CAST(raw.module_short_title AS VARCHAR)), ''),
                        NULLIF(TRIM(CAST(raw.module_long_title AS VARCHAR)), ''),
                        catalog.catalog_module_name
                    ) AS module,
                    NULLIF(TRIM(CAST(raw.playlist_or_module_id AS VARCHAR)), '')
                        AS playlist_id,
                    TRIM(CAST(raw.work_mode AS VARCHAR)) AS work_mode,
                    TRY_CAST(raw.created_at AS TIMESTAMPTZ) AS created_at,
                    CAST(raw.data_correct AS DOUBLE) AS success,
                    raw.file_row_number AS source_order
                FROM read_parquet(?, file_row_number = true) AS raw
                LEFT JOIN exercise_catalog AS catalog
                  ON CAST(raw.exercise_id AS VARCHAR) = catalog.exercise_id
                WHERE raw.user_id IS NOT NULL
                  AND raw.exercise_id IS NOT NULL
                  AND raw.data_correct IS NOT NULL
                  AND TRIM(CAST(raw.work_mode AS VARCHAR)) IN ('playlist', 'zpdes')
            ), complete_rows AS (
                SELECT *
                FROM standardized
                WHERE activity_id IS NOT NULL
                  AND module IS NOT NULL
                  AND created_at IS NOT NULL
            ), single_module_playlists AS (
                SELECT playlist_id
                FROM complete_rows
                WHERE work_mode = 'playlist' AND playlist_id IS NOT NULL
                GROUP BY playlist_id
                HAVING count(DISTINCT module) = 1
            ), filtered_modes AS (
                SELECT *
                FROM complete_rows
                WHERE {playlist_condition}
            ), student_mode_counts AS (
                SELECT student_id, count(DISTINCT work_mode) AS mode_count
                FROM filtered_modes
                GROUP BY student_id
            ), population_rows AS (
                SELECT
                    filtered_modes.*,
                    CASE
                        WHEN student_mode_counts.mode_count = 2 THEN 'both_modes'
                        ELSE 'exclusive_modes'
                    END AS population
                FROM filtered_modes
                INNER JOIN student_mode_counts USING (student_id)
            ), eligible_students AS (
                SELECT population, student_id
                FROM population_rows
                GROUP BY population, student_id
                {student_eligibility_having}
            )
            SELECT population_rows.*
            FROM population_rows
            INNER JOIN eligible_students USING (population, student_id)
            WHERE {population_condition}
            """,
            parameters,
        )

        eligible_summary = connection.execute(
            """
            SELECT
                count(*) AS eligible_attempt_rows,
                count(DISTINCT student_id) AS students,
                count(DISTINCT classroom_id) AS classrooms,
                count(DISTINCT module) AS modules,
                count(DISTINCT exercise_id) AS exercises,
                count(DISTINCT (student_id, exercise_id)) AS first_attempt_rows,
                count(*) FILTER (WHERE work_mode = 'playlist') AS eligible_playlist_rows,
                count(*) FILTER (WHERE work_mode = 'zpdes') AS eligible_zpdes_rows
            FROM eligible_attempts
            """
        ).fetchone()
        description = connection.description
        if eligible_summary is None or description is None:
            raise RuntimeError("Could not summarize eligible attempts")
        summary_names = [column[0] for column in description]
        audit = dict(zip(summary_names, eligible_summary, strict=True))
        population_audit = connection.execute(
            """
            SELECT
                population,
                count(*) AS eligible_attempt_rows,
                count(DISTINCT student_id) AS students,
                count(DISTINCT classroom_id) AS classrooms,
                count(DISTINCT module) AS modules,
                count(DISTINCT exercise_id) AS exercises,
                count(DISTINCT (student_id, exercise_id)) AS first_attempt_rows,
                count(*) FILTER (WHERE work_mode = 'playlist') AS eligible_playlist_rows,
                count(*) FILTER (WHERE work_mode = 'zpdes') AS eligible_zpdes_rows
            FROM eligible_attempts
            GROUP BY population
            ORDER BY population
            """
        ).fetch_df()

        trajectory = connection.execute(
            f"""
            WITH ranked_exercises AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY student_id, exercise_id
                        ORDER BY created_at, source_order
                    ) AS exercise_attempt_rank
                FROM eligible_attempts
            ), first_attempts AS (
                SELECT * EXCLUDE (exercise_attempt_rank)
                FROM ranked_exercises
                WHERE exercise_attempt_rank = 1
            ), ordered_attempts AS (
                SELECT
                    *,
                    CASE
                        WHEN {boundary_condition}
                        THEN 1 ELSE 0
                    END AS starts_new_sequence
                FROM first_attempts
            ), sequenced_attempts AS (
                SELECT
                    *,
                    sum(starts_new_sequence) OVER (
                        PARTITION BY {sequence_partition}
                        ORDER BY created_at, source_order, exercise_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS {sequence_id_column}
                FROM ordered_attempts
            ), positioned AS (
                SELECT
                    * EXCLUDE (source_order, starts_new_sequence),
                    row_number() OVER (
                        PARTITION BY {position_partition}
                        ORDER BY created_at, source_order, exercise_id
                    ) - 1 AS attempt_position,
                    count(*) OVER (
                        PARTITION BY {position_partition}
                    ) AS segment_exercises
                FROM sequenced_attempts
            )
            SELECT
                population,
                student_id,
                classroom_id,
                module,
                activity_id,
                CAST({sequence_id_column} AS INTEGER) AS {sequence_id_column},
                work_mode,
                exercise_id,
                created_at,
                success,
                CAST(attempt_position AS INTEGER) AS attempt_position,
                CAST(segment_exercises AS INTEGER) AS segment_exercises,
                classroom_id || chr(31) || student_id AS student_in_classroom,
                true AS is_first_retained_attempt
            FROM positioned
            WHERE segment_exercises >= ?
            ORDER BY student_id, module, {sequence_id_column}, attempt_position
            """,
            [min_activity_exercises],
        ).fetch_df()
    finally:
        connection.close()

    trajectory = _compact_trajectory_dtypes(trajectory)
    audit.update(
        {
            "population": population,
            "trajectory_rows": len(trajectory),
            "removed_repeat_rows": int(
                audit["eligible_attempt_rows"] - audit["first_attempt_rows"]
            ),
            "removed_short_segment_rows": int(
                audit["first_attempt_rows"] - len(trajectory)
            ),
            "segments": (
                trajectory.groupby(_trajectory_segment_keys(trajectory), observed=True).ngroups
                if not trajectory.empty
                else 0
            ),
            "max_attempt_position": (
                int(trajectory["attempt_position"].max()) if not trajectory.empty else None
            ),
        }
    )
    population_trajectory_audit = []
    for source_population, source_frame in trajectory.groupby("population", observed=True):
        population_trajectory_audit.append(
            {
                "population": str(source_population),
                "trajectory_rows": len(source_frame),
                "segments": source_frame.groupby(
                    _trajectory_segment_keys(source_frame), observed=True
                ).ngroups,
                "max_attempt_position": int(source_frame["attempt_position"].max()),
            }
        )
    population_audit = population_audit.merge(
        pd.DataFrame(population_trajectory_audit),
        on="population",
        how="left",
        validate="one_to_one",
    )
    population_audit["trajectory_rows"] = population_audit["trajectory_rows"].fillna(0).astype(int)
    population_audit["removed_repeat_rows"] = (
        population_audit["eligible_attempt_rows"] - population_audit["first_attempt_rows"]
    )
    population_audit["removed_short_segment_rows"] = (
        population_audit["first_attempt_rows"] - population_audit["trajectory_rows"]
    )
    return TrajectoryDataset(
        frame=trajectory,
        audit=audit,
        population_audit=population_audit,
    )


def load_mia_first_attempt_trajectory(
    input_file: Path,
    exercise_catalog_json: Path,
    module_config_json: Path,
    population: str,
    min_unique_exercises: int | None = None,
    min_activity_exercises: int = 4,
    keep_only_single_module_playlists: bool = False,
    isolate_process: bool = False,
    sequence_scope: str = "activity",
) -> TrajectoryDataset:
    """Materialize raw-MIA trajectories for one population.

    In-process execution is the default because it provides reliable exceptions
    in notebook kernels. ``isolate_process=True`` remains available when
    releasing DuckDB allocator memory before model fitting is more important.
    """

    if not isolate_process:
        return _load_mia_first_attempt_trajectory_in_process(
            input_file=input_file,
            exercise_catalog_json=exercise_catalog_json,
            module_config_json=module_config_json,
            population=population,
            min_unique_exercises=min_unique_exercises,
            min_activity_exercises=min_activity_exercises,
            keep_only_single_module_playlists=keep_only_single_module_playlists,
            sequence_scope=sequence_scope,
        )

    with tempfile.TemporaryDirectory(prefix="visu2_first_attempt_") as temp_directory:
        temp_path = Path(temp_directory)
        trajectory_path = temp_path / "trajectory.parquet"
        audit_path = temp_path / "audit.json"
        population_audit_path = temp_path / "population_audit.parquet"
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "materialize-mia",
            "--input-file",
            str(input_file),
            "--exercise-catalog-json",
            str(exercise_catalog_json),
            "--module-config-json",
            str(module_config_json),
            "--population",
            population,
            "--min-activity-exercises",
            str(min_activity_exercises),
            "--sequence-scope",
            sequence_scope,
            "--trajectory-output",
            str(trajectory_path),
            "--audit-output",
            str(audit_path),
            "--population-audit-output",
            str(population_audit_path),
        ]
        if min_unique_exercises is not None:
            command.extend(["--min-unique-exercises", str(min_unique_exercises)])
        if keep_only_single_module_playlists:
            command.append("--keep-only-single-module-playlists")
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            diagnostic = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(
                "First-attempt materialization failed "
                f"(exit code {completed.returncode}): {diagnostic or 'no worker output'}"
            )

        trajectory = pd.read_parquet(trajectory_path)
        trajectory = _compact_trajectory_dtypes(trajectory)
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        population_audit = pd.read_parquet(population_audit_path)
    return TrajectoryDataset(
        frame=trajectory,
        audit=audit,
        population_audit=population_audit,
    )


def _compact_trajectory_dtypes(frame: pd.DataFrame) -> pd.DataFrame:
    for column in [
        "population",
        "student_id",
        "classroom_id",
        "module",
        "activity_id",
        "work_mode",
        "exercise_id",
        "student_in_classroom",
    ]:
        frame[column] = frame[column].astype("category")
    frame["success"] = frame["success"].astype("int8")
    frame["attempt_position"] = frame["attempt_position"].astype("int32")
    frame["segment_exercises"] = frame["segment_exercises"].astype("int32")
    for column in ("activity_sequence_id", "module_sequence_id"):
        if column in frame:
            frame[column] = frame[column].astype("int32")
    return frame


def combine_trajectory_populations(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate compact trajectories without expanding categories to strings."""

    nonempty = [frame for frame in frames if not frame.empty]
    if not nonempty:
        return _empty_trajectory(pd.DataFrame())
    columns = nonempty[0].columns.tolist()
    if any(frame.columns.tolist() != columns for frame in nonempty[1:]):
        raise ValueError("Trajectory frames must have identical columns")

    categorical_columns = [
        column for column in columns if isinstance(nonempty[0][column].dtype, pd.CategoricalDtype)
    ]
    combined_data: dict[str, object] = {}
    for column in columns:
        if column in categorical_columns:
            combined_data[column] = pd.api.types.union_categoricals(
                [frame[column].array for frame in nonempty],
                sort_categories=True,
            )
        else:
            combined_data[column] = np.concatenate(
                [frame[column].to_numpy(copy=False) for frame in nonempty]
            )
    return pd.DataFrame(combined_data)


def build_first_attempt_trajectory(
    attempts: pd.DataFrame,
    min_activity_exercises: int = 4,
    sequence_scope: str = "activity",
) -> pd.DataFrame:
    """Build zero-based trajectories from first retained student-exercise rows.

    The caller supplies attempts after the existing playlist and population
    filters. Consequently, "first" means the earliest retained playlist/zpdes
    row, not necessarily the first row in an excluded work mode such as
    adaptive-test or revision.
    """

    if min_activity_exercises < 1:
        raise ValueError("min_activity_exercises must be at least 1")
    (
        sequence_partition,
        sequence_id_column,
        boundary_condition,
        position_partition,
    ) = _sequence_sql_parts(sequence_scope)

    required = {
        "student_id",
        "classroom_id",
        "module",
        "activity_id",
        "work_mode",
        "exercise_id",
        "created_at",
        "success",
    }
    missing = sorted(required.difference(attempts.columns))
    if missing:
        raise ValueError(f"Missing trajectory columns: {', '.join(missing)}")

    if attempts.empty:
        return _empty_trajectory(attempts, sequence_scope=sequence_scope)

    # DuckDB performs both window operations without making multiple pandas
    # copies of the multi-million-row attempt table and can spill sorts to disk.
    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit = '4GB'")
        connection.execute("SET threads = 2")
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("SET preserve_insertion_order = false")
        connection.register("attempt_rows", attempts)
        query = f"""
            WITH complete_rows AS (
                SELECT
                    CAST(student_id AS VARCHAR) AS student_id,
                    CAST(classroom_id AS VARCHAR) AS classroom_id,
                    CAST(module AS VARCHAR) AS module,
                    CAST(activity_id AS VARCHAR) AS activity_id,
                    CAST(work_mode AS VARCHAR) AS work_mode,
                    CAST(exercise_id AS VARCHAR) AS exercise_id,
                    created_at,
                    CAST(success AS DOUBLE) AS success,
                    row_number() OVER () AS source_order
                FROM attempt_rows
                WHERE student_id IS NOT NULL
                  AND classroom_id IS NOT NULL
                  AND module IS NOT NULL
                  AND work_mode IN ('playlist', 'zpdes')
                  AND exercise_id IS NOT NULL
                  AND created_at IS NOT NULL
                  AND success IS NOT NULL
            ), ranked_exercises AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY student_id, exercise_id
                        ORDER BY created_at, source_order
                    ) AS exercise_attempt_rank
                FROM complete_rows
            ), first_attempts AS (
                SELECT * EXCLUDE (exercise_attempt_rank)
                FROM ranked_exercises
                WHERE exercise_attempt_rank = 1
            ), ordered_attempts AS (
                SELECT
                    *,
                    CASE
                        WHEN {boundary_condition}
                        THEN 1 ELSE 0
                    END AS starts_new_sequence
                FROM first_attempts
            ), sequenced_attempts AS (
                SELECT
                    *,
                    sum(starts_new_sequence) OVER (
                        PARTITION BY {sequence_partition}
                        ORDER BY created_at, source_order, exercise_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS {sequence_id_column}
                FROM ordered_attempts
            ), positioned AS (
                SELECT
                    * EXCLUDE (source_order, starts_new_sequence),
                    row_number() OVER (
                        PARTITION BY {position_partition}
                        ORDER BY created_at, source_order, exercise_id
                    ) - 1 AS attempt_position,
                    count(*) OVER (
                        PARTITION BY {position_partition}
                    ) AS segment_exercises
                FROM sequenced_attempts
            )
            SELECT
                student_id,
                classroom_id,
                module,
                activity_id,
                CAST({sequence_id_column} AS BIGINT) AS {sequence_id_column},
                work_mode,
                exercise_id,
                created_at,
                success,
                CAST(attempt_position AS BIGINT) AS attempt_position,
                CAST(segment_exercises AS BIGINT) AS segment_exercises,
                classroom_id || chr(31) || student_id AS student_in_classroom,
                true AS is_first_retained_attempt
            FROM positioned
            WHERE segment_exercises >= ?
            ORDER BY student_id, module, {sequence_id_column}, attempt_position
        """
        return connection.execute(query, [min_activity_exercises]).fetch_df()
    finally:
        connection.close()


def _empty_trajectory(
    frame: pd.DataFrame,
    sequence_scope: str = "activity",
) -> pd.DataFrame:
    _, sequence_id_column, _, _ = _sequence_sql_parts(sequence_scope)
    columns = [
        "student_id",
        "classroom_id",
        "module",
        "activity_id",
        "work_mode",
        "exercise_id",
        "created_at",
        "success",
    ]
    result = frame[[column for column in columns if column in frame]].iloc[0:0].copy()
    result["attempt_position"] = pd.Series(dtype="int64")
    result["segment_exercises"] = pd.Series(dtype="int64")
    result[sequence_id_column] = pd.Series(dtype="int64")
    result["student_in_classroom"] = pd.Series(dtype="object")
    result["is_first_retained_attempt"] = pd.Series(dtype="bool")
    return result


def prepare_gpboost_inputs(
    trajectory: pd.DataFrame,
) -> tuple[np.ndarray, pd.DataFrame, pd.DataFrame]:
    """Return response, fixed-effect design, and integer-coded grouping data."""

    required = {
        "success",
        "work_mode",
        "attempt_position",
        *GROUP_COLUMNS,
    }
    missing = sorted(required.difference(trajectory.columns))
    if missing:
        raise ValueError(f"Missing model columns: {', '.join(missing)}")

    complete = trajectory[list(required)].notna().all(axis=1)
    model_df = trajectory if complete.all() else trajectory.loc[complete].copy()
    if model_df.empty:
        raise ValueError("No complete trajectory rows are available for modeling")
    observed_modes = set(model_df["work_mode"].dropna().unique().astype(str))
    if not observed_modes.issubset(WORK_MODES):
        raise ValueError("work_mode must contain only playlist and zpdes")

    success_values = set(model_df["success"].astype(float).unique())
    if not success_values.issubset({0.0, 1.0}):
        raise ValueError("success must be binary (0/1)")

    zpdes = model_df["work_mode"].eq("zpdes").astype(float)
    position = model_df["attempt_position"].astype(float)
    fixed_effects = pd.DataFrame(
        {
            "Intercept": np.ones(len(model_df), dtype=np.float64),
            "zpdes": zpdes.to_numpy(dtype=np.float64),
            "attempt_position": position.to_numpy(dtype=np.float64),
            "zpdes_x_attempt_position": (zpdes * position).to_numpy(dtype=np.float64),
        },
        index=model_df.index,
    )

    group_data = pd.DataFrame(index=model_df.index)
    for column in GROUP_COLUMNS:
        codes, _ = pd.factorize(model_df[column], sort=True)
        if np.any(codes < 0):
            raise ValueError(f"Could not encode grouping column {column}")
        group_data[column] = codes.astype(np.int32)

    response = model_df["success"].to_numpy(dtype=np.float64)
    return response, fixed_effects, group_data


def build_empirical_trajectory_summary(
    trajectory: pd.DataFrame,
    exercise_elo: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate observed success and calibrated exercise Elo by mode and position."""

    sequence_id_column = (
        "module_sequence_id"
        if "module_sequence_id" in trajectory.columns
        else "activity_sequence_id"
    )
    required_trajectory = {
        "student_id",
        "module",
        sequence_id_column,
        "work_mode",
        "attempt_position",
        "success",
        "exercise_id",
        "activity_id",
    }
    missing_trajectory = sorted(required_trajectory.difference(trajectory.columns))
    if missing_trajectory:
        raise ValueError(
            "Missing empirical trajectory columns: " + ", ".join(missing_trajectory)
        )

    required_elo = {"exercise_id", "activity_id", "exercise_elo"}
    missing_elo = sorted(required_elo.difference(exercise_elo.columns))
    if missing_elo:
        raise ValueError("Missing exercise Elo columns: " + ", ".join(missing_elo))

    elo_context = exercise_elo.copy()
    if "calibrated" in elo_context:
        elo_context = elo_context.loc[elo_context["calibrated"].fillna(False)]
    elo_context = elo_context.dropna(
        subset=["exercise_id", "activity_id", "exercise_elo"]
    )[["exercise_id", "activity_id", "exercise_elo"]]
    duplicate_contexts = elo_context.duplicated(
        subset=["exercise_id", "activity_id"], keep=False
    )
    if duplicate_contexts.any():
        raise ValueError(
            "Exercise Elo must be unique for each exercise_id and activity_id context"
        )
    elo_by_exercise = (
        elo_context.groupby("exercise_id", as_index=False, observed=True)["exercise_elo"]
        .mean()
        .rename(columns={"exercise_elo": "fallback_exercise_elo"})
    )

    import duckdb

    connection = duckdb.connect()
    try:
        connection.register("trajectory_attempts", trajectory)
        connection.register("exercise_elo_context", elo_context)
        connection.register("exercise_elo_by_exercise", elo_by_exercise)
        summary = connection.execute(
            f"""
            SELECT
                CAST(t.work_mode AS VARCHAR) AS work_mode,
                CAST(t.attempt_position AS INTEGER) AS attempt_position,
                CAST(t.attempt_position AS INTEGER) + 1 AS attempt_number,
                count(*) AS attempt_rows,
                count(DISTINCT (
                    CAST(t.student_id AS VARCHAR),
                    CAST(t.module AS VARCHAR),
                    CAST(t.{sequence_id_column} AS INTEGER),
                    CAST(t.work_mode AS VARCHAR)
                )) AS sequences,
                count(DISTINCT CAST(t.student_id AS VARCHAR)) AS students,
                count(DISTINCT CAST(t.module AS VARCHAR)) AS modules,
                avg(CAST(t.success AS DOUBLE)) AS success_rate,
                avg(COALESCE(context_elo.exercise_elo, exercise_elo.fallback_exercise_elo))
                    AS mean_exercise_elo,
                count(COALESCE(
                    context_elo.exercise_elo,
                    exercise_elo.fallback_exercise_elo
                )) AS elo_attempt_rows,
                count(context_elo.exercise_elo) AS exact_context_elo_attempt_rows,
                count(*) FILTER (
                    WHERE context_elo.exercise_elo IS NULL
                      AND exercise_elo.fallback_exercise_elo IS NOT NULL
                ) AS exercise_id_fallback_elo_attempt_rows,
                count(COALESCE(
                    context_elo.exercise_elo,
                    exercise_elo.fallback_exercise_elo
                ))::DOUBLE / count(*) AS elo_coverage
            FROM trajectory_attempts AS t
            LEFT JOIN exercise_elo_context AS context_elo
              ON CAST(t.exercise_id AS VARCHAR) = CAST(context_elo.exercise_id AS VARCHAR)
             AND CAST(t.activity_id AS VARCHAR) = CAST(context_elo.activity_id AS VARCHAR)
            LEFT JOIN exercise_elo_by_exercise AS exercise_elo
              ON CAST(t.exercise_id AS VARCHAR) = CAST(exercise_elo.exercise_id AS VARCHAR)
            GROUP BY t.work_mode, t.attempt_position
            ORDER BY t.work_mode, t.attempt_position
            """
        ).fetch_df()
    finally:
        connection.close()

    if not summary["attempt_rows"].eq(summary["sequences"]).all():
        raise ValueError("Each sequence must contribute at most one row per attempt position")
    if int(summary["elo_attempt_rows"].sum()) == 0:
        raise ValueError("No trajectory attempts matched calibrated exercise Elo contexts")
    return summary


def fit_gpboost_trajectory(
    trajectory: pd.DataFrame,
    population: str,
    maxiter: int = 200,
    trace: bool = False,
) -> TrajectoryFitResult:
    """Fit the Bernoulli-logit trajectory model for one population."""

    complete_columns = ["success", "work_mode", "attempt_position", *GROUP_COLUMNS]
    complete = trajectory[complete_columns].notna().all(axis=1)
    model_df = trajectory if complete.all() else trajectory.loc[complete].copy()
    base_summary: dict[str, object] = {
        "population": population,
        "n_rows": len(model_df),
        "n_students": model_df["student_id"].nunique() if "student_id" in model_df else 0,
        "n_classrooms": model_df["classroom_id"].nunique() if not model_df.empty else 0,
        "n_modules": model_df["module"].nunique() if not model_df.empty else 0,
        "n_exercises": model_df["exercise_id"].nunique() if not model_df.empty else 0,
        "n_segments": (
            model_df.groupby(_trajectory_segment_keys(model_df), observed=True).ngroups
            if not model_df.empty
            else 0
        ),
        "max_attempt_position": (
            int(model_df["attempt_position"].max()) if not model_df.empty else None
        ),
        "model_specification": (
            "Bernoulli-logit: work_mode * attempt_position; random intercepts "
            "for classroom and student within classroom"
        ),
    }

    if model_df.empty or model_df["work_mode"].nunique() < 2:
        return TrajectoryFitResult(
            summary={
                **base_summary,
                "status": "skipped",
                "converged": False,
                "reportable": False,
                "error": "Need non-empty data containing both work modes.",
            }
        )
    if model_df["attempt_position"].nunique() < 2:
        return TrajectoryFitResult(
            summary={
                **base_summary,
                "status": "skipped",
                "converged": False,
                "reportable": False,
                "error": "Need at least two distinct attempt positions.",
            }
        )

    classrooms_per_student = model_df.groupby("student_id", observed=True)[
        "classroom_id"
    ].nunique()
    if not classrooms_per_student.empty and classrooms_per_student.max() > 1:
        count = int((classrooms_per_student > 1).sum())
        return TrajectoryFitResult(
            summary={
                **base_summary,
                "status": "failed",
                "converged": False,
                "reportable": False,
                "error": f"Nested-student model invalid: {count} students span multiple classrooms.",
            }
        )

    try:
        import gpboost as gpb

        response, fixed_effects, group_data = prepare_gpboost_inputs(model_df)
        model = gpb.GPModel(
            likelihood="bernoulli_logit",
            group_data=group_data,
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
        coefficient_table = _coefficient_table(model, population)
        variance_table = _variance_component_table(model, population)
        iterations = int(model._get_num_optim_iter())
        converged = iterations < maxiter
    except Exception as exc:
        return TrajectoryFitResult(
            summary={
                **base_summary,
                "status": "failed",
                "converged": False,
                "reportable": False,
                "error": str(exc),
            }
        )

    interaction = coefficient_table.loc[
        coefficient_table["term"].eq("zpdes_x_attempt_position")
    ].iloc[0]
    playlist_slope = _coefficient_value(coefficient_table, "attempt_position")
    interaction_estimate = float(interaction["estimate"])
    invalid_standard_errors = coefficient_table.loc[
        ~np.isfinite(coefficient_table["std_error"]), "term"
    ].tolist()
    warning = None
    if invalid_standard_errors:
        warning = "Unavailable standard errors: " + ", ".join(invalid_standard_errors)

    reportable = bool(converged and np.isfinite(float(interaction["std_error"])))
    return TrajectoryFitResult(
        summary={
            **base_summary,
            "status": "ok" if converged else "not_converged",
            "converged": converged,
            "reportable": reportable,
            "iterations": iterations,
            "playlist_log_odds_slope": playlist_slope,
            "zpdes_log_odds_slope": playlist_slope + interaction_estimate,
            "slope_difference": interaction_estimate,
            "slope_difference_std_error": float(interaction["std_error"]),
            "slope_difference_ci_low": float(interaction["ci_low"]),
            "slope_difference_ci_high": float(interaction["ci_high"]),
            "slope_difference_p_value": float(interaction["p_value"]),
            "warning": warning,
            "error": None if converged else "Optimizer reached the maximum iteration count.",
        },
        fixed_effects=coefficient_table,
        variance_components=variance_table,
    )


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


def _coefficient_value(table: pd.DataFrame, term: str) -> float:
    row = table.loc[table["term"].eq(term), "estimate"]
    if row.empty:
        raise ValueError(f"Missing fixed-effect coefficient: {term}")
    return float(row.iloc[0])


def build_fixed_effect_trajectory(
    fixed_effects: pd.DataFrame,
    positions: np.ndarray | list[float],
) -> pd.DataFrame:
    """Calculate zero-random-effect probability curves from a fitted coefficient table."""

    coefficient_map = fixed_effects.set_index("term")["estimate"].to_dict()
    missing = [name for name in FIXED_EFFECT_COLUMNS if name not in coefficient_map]
    if missing:
        raise ValueError(f"Missing curve coefficients: {', '.join(missing)}")

    position_values = np.asarray(positions, dtype=float)
    rows = []
    for work_mode, zpdes in (("playlist", 0.0), ("zpdes", 1.0)):
        linear_predictor = (
            coefficient_map["Intercept"]
            + coefficient_map["zpdes"] * zpdes
            + coefficient_map["attempt_position"] * position_values
            + coefficient_map["zpdes_x_attempt_position"] * zpdes * position_values
        )
        probability = _expit(linear_predictor)
        rows.append(
            pd.DataFrame(
                {
                    "work_mode": work_mode,
                    "attempt_position": position_values,
                    "attempt_number": position_values + 1,
                    "linear_predictor": linear_predictor,
                    "predicted_probability": probability,
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def _expit(values: np.ndarray) -> np.ndarray:
    result = np.empty_like(values, dtype=float)
    positive = values >= 0
    result[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    result[~positive] = exp_values / (1.0 + exp_values)
    return result


def _json_scalar(value: object) -> object:
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _materialize_cli(arguments: argparse.Namespace) -> None:
    dataset = _load_mia_first_attempt_trajectory_in_process(
        input_file=arguments.input_file,
        exercise_catalog_json=arguments.exercise_catalog_json,
        module_config_json=arguments.module_config_json,
        population=arguments.population,
        min_unique_exercises=arguments.min_unique_exercises,
        min_activity_exercises=arguments.min_activity_exercises,
        keep_only_single_module_playlists=arguments.keep_only_single_module_playlists,
        sequence_scope=arguments.sequence_scope,
    )
    dataset.frame.to_parquet(arguments.trajectory_output, index=False)
    arguments.audit_output.write_text(
        json.dumps(dataset.audit, ensure_ascii=False, default=_json_scalar),
        encoding="utf-8",
    )
    dataset.population_audit.to_parquet(arguments.population_audit_output, index=False)


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    materialize = subparsers.add_parser("materialize-mia")
    materialize.add_argument("--input-file", type=Path, required=True)
    materialize.add_argument("--exercise-catalog-json", type=Path, required=True)
    materialize.add_argument("--module-config-json", type=Path, required=True)
    materialize.add_argument(
        "--population",
        choices=("exclusive_modes", "both_modes", "combined"),
        required=True,
    )
    materialize.add_argument("--min-unique-exercises", type=int, default=None)
    materialize.add_argument("--min-activity-exercises", type=int, default=4)
    materialize.add_argument(
        "--sequence-scope",
        choices=("activity", "module"),
        default="activity",
    )
    materialize.add_argument("--keep-only-single-module-playlists", action="store_true")
    materialize.add_argument("--trajectory-output", type=Path, required=True)
    materialize.add_argument("--audit-output", type=Path, required=True)
    materialize.add_argument("--population-audit-output", type=Path, required=True)
    return parser


def main() -> None:
    arguments = _build_cli_parser().parse_args()
    if arguments.command == "materialize-mia":
        _materialize_cli(arguments)


if __name__ == "__main__":
    main()

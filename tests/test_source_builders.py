"""Maureen adapter regressions for the source-local runtime builder."""

from __future__ import annotations

from pathlib import Path

from visu2.source_builders import _build_maureen_catalog_and_raw


def test_build_maureen_catalog_and_raw_adds_synthetic_activity_and_attempt_ordinals(tmp_path: Path) -> None:
    config_path = tmp_path / "module_config.csv"
    config_path.write_text(
        "\n".join(
            [
                "type;shell;id;short_title;long_title;group_title;description;description_student;targeted_difficulties;status;available_in_specimen;code;prerequisites;init_open;stay_consec_min;nbMinStep;nbMaxComputeThreshold;activating_Threshold;deact_Threshold;deact_prerequisites;window_progression;prom_coeff;student_path;student_path_alt;mapping_nodes;states_definition;learning_items",
                "module;witch;module-1;Module short;Module long;;;;;visible;FAUX;M16;;;;;;;;;;;;;;;",
                "objective;witch;objective-1;Objective short;Objective long;;;;;visible;FAUX;M16O1;;Y;;;;;;;;;;;;;;;;",
                "activity;witch;activity-1;Activity short;Activity long;;;;;visible;FAUX;M16O1A1;;;;;;;;;;;;;;;11111111-1111-1111-1111-111111111111",
            ]
        ),
        encoding="utf-8",
    )
    attempts_path = tmp_path / "attempts.csv"
    attempts_path.write_text(
        "\n".join(
            [
                "user_id,variation,module_id,objective_id,activity_id,exercise_id,created_at,data_score,data_correct,data_test_context,progression_score,initial_test_max_success,initial_test_weighted_max_success,initial_test_success_rate,finished_module_mean_score,finished_module_graphe_coverage_rate,is_gar",
                "user-1,var,module-1,objective-1,activity-1,11111111-1111-1111-1111-111111111111,2024-03-19 12:00:00+00:00,1.0,True,zpdes,1.0,,,,,,,True",
                "user-1,var,module-1,objective-1,activity-extra,22222222-2222-2222-2222-222222222222,2024-03-19 12:01:00+00:00,0.0,False,adaptive-test,2.0,,,,,,,True",
                "user-1,var,module-1,objective-1,activity-extra,22222222-2222-2222-2222-222222222222,2024-03-19 12:02:00+00:00,1.0,True,adaptive-test,3.0,,,,,,,True",
            ]
        ),
        encoding="utf-8",
    )

    raw_attempts, learning_catalog, zpdes_rules, exercises_json, warnings = _build_maureen_catalog_and_raw(
        attempts_path,
        config_path,
    )

    assert raw_attempts.height == 3
    assert set(raw_attempts["work_mode"].to_list()) == {"zpdes", "adaptive-test"}
    extra_rows = raw_attempts.filter(raw_attempts["activity_id"] == "activity-extra")
    assert extra_rows["attempt_number"].to_list() == [1, 2]
    assert raw_attempts["student_attempt_index"].to_list() == [1, 2, 3]

    module = learning_catalog["modules"][0]
    objective = module["objectives"][0]
    assert len(objective["activities"]) == 2
    synthetic_activity = [row for row in objective["activities"] if row["id"] == "activity-extra"][0]
    assert synthetic_activity["code"] == "M16O1A2"
    assert "Added synthetic activity" in warnings[0]

    assert learning_catalog["exercise_to_hierarchy"]["22222222-2222-2222-2222-222222222222"]["activity_id"] == "activity-extra"
    assert zpdes_rules["map_id_code"]["code_to_id"]["M16O1A2"] == "activity-extra"
    exercise_ids = [row["id"] for row in exercises_json["exercises"]]
    assert "22222222-2222-2222-2222-222222222222" in exercise_ids

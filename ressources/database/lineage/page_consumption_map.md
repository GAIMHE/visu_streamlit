# Page Consumption Map

## Active Pages

- `apps/streamlit_app.py`
  - `fact_attempt_core`, `agg_activity_daily`, `agg_transition_edges`, reports/manifest
- `apps/pages/2_objective_activity_matrix.py`
  - `agg_activity_daily`, `agg_exercise_daily`, `agg_activity_elo`, `agg_exercise_elo`, `fact_attempt_core`, `learning_catalog.json`
- `apps/pages/3_zpdes_dependencies.py`
  - `zpdes_rules.json`, `learning_catalog.json`, optional `agg_activity_daily`
- `apps/pages/4_classroom_progression_replay.py`
  - `fact_attempt_core`
- `apps/pages/5_student_elo_evolution.py`
  - `student_elo_events`, `student_elo_profiles`

## Hidden Page (Not in current navigation)

- `apps/disabled_pages/1_usage_playlist_engagement.py`
  - `fact_attempt_core`, `agg_module_usage_daily`, `agg_playlist_module_usage`, `agg_module_activity_usage`

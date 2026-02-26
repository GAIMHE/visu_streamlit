---
license: other
license_name: am-license
license_link: LICENSE
language:
- en
---
# Adaptiv'Math dataset

## Context

The Adaptiv’Math dataset contains interaction traces from a large-scale adaptive digital math learning environment used in real classrooms.
It includes learning trajectories from 29,000+ students, capturing how learners navigate structured math content under both adaptive and teacher-defined sequencing.

Adaptiv’Math is a digital learning platform where students solve math exercises within a structured curriculum.
The pedagogical content follows a hierarchical structure:
- a module covers a broad math theme,
- each module contains several objectives (targeted skills),
- each objective contains activities (learning steps),
- each activity is composed of exercises (concrete tasks answered by students).

This hierarchy enables analysis of learning across multiple levels (exercise → curriculum).

Students interact with the content through different sequencing modes. 
The primary mode is an adaptive system based on a Zone of Proximal Development with Estimated Success (ZPDES) approach. In this setting, students initially encounter activities with minimal prerequisites, and the system continuously updates estimates of mastery based on observed success rates. As mastery is inferred, new activities may unlock dynamically, sometimes across different objectives. As a result, learning trajectories are not strictly linear but evolve in a personalized manner over time.
In addition to adaptive sequencing, the platform also supports teacher-defined playlists. In this mode, teachers specify a fixed sequence of activities in advance, and students follow a shared, ordered curriculum. 
This dual structure provides a natural contrast between adaptive and fixed sequencing, enabling the study of how personalized progression compares with more structured learning pathways.

This dataset supports diverse research directions in learning sciences and AI for education, enabling the analysis of learning dynamics, adaptive sequencing, and large-scale modeling of student progression.

## Overview

This package is organized around 4 main files designed to be used together:

1. `adaptiv_math_history.parquet`  
2. `learning_catalog.json`  
3. `zpdes_rules.json`
4. `exercises.json`

Summary:
- `adaptiv_math_history.parquet`: 6,264,394 attempt rows, 18 columns
- `learning_catalog.json`: 7 modules, 95 objectives, 454 activities, 8,862 exercise mappings
- `zpdes_rules.json`: dependencies rules for 11 modules
- `exercises.json`: `8,862` exercise definitions

## Pedagogical hierarchy

`module -> objective -> activity -> exercise`

Where this hierarchy lives:
- Structure and mapping are in `learning_catalog.json`
- Attempt events of exercise are in `adaptiv_math_history.parquet` and should be linked through `exercise_id`
- Rule dependencies: `zpdes_rules.json`
- Exercise content/details: `exercises.json`

## Pedagogical mode

The `work_mode` field indicates which pedagogical sequencing context generated each attempt.

Observed values in `adaptiv_math_history.parquet`:

| work_mode | Count | Share of attempts | Likely interpretation |
|---|---:|---:|---|
| `zpdes` | 5,217,996 | 83.2961% | Main adaptive learning mode (dynamic unlocking based on estimated mastery and dependency rules). |
| `initial-test` | 883,608 | 14.1052% | Initial diagnostic/placement phase used before regular progression. |
| `adaptive-test` | 161,320 | 2.5752% | Adaptive evaluation/test phase used to refine proficiency estimates. |
| `playlist` | 1,470 | 0.0235% | Teacher-defined fixed sequence of content (non-adaptive ordering). |

Notes:
- Most attempts come from adaptive progression (`zpdes`).
- `playlist` is present but very small in this extract.
- Mode semantics are inferred from naming conventions and observed behavior in the data contracts.

## Files and roles

### 1) `adaptiv_math_history.parquet`

- Table at the exercise-attempt level (one row = one attempt).
- Current size: 6,264,394 rows** and 18 columns.
- Main join keys: `exercise_id`, `activity_id`, `objective_id`, `playlist_or_module_id`, `work_mode`.

Important:
- In `playlist` mode, raw `activity_id`/`objective_id` can be unusable placeholders.
- Reliable hierarchy mapping should be done via `exercise_id` + `learning_catalog.json`.

Main columns:
- Identity/context: `user_id`, `teacher_id`, `classroom_id`, `playlist_or_module_id`
- Hierarchy IDs in raw events: `objective_id`, `activity_id`, `exercise_id`
- Time/session: `created_at`, `login_time`, `data_duration`, `session_duration`
- Performance/attempt: `data_correct`, `attempt_number`, `student_attempt_index`, `first_attempt_success_rate`
- Mode: `work_mode`

### 2) `learning_catalog.json`

Pedagogical structure:
- nested hierarchy: `modules -> objectives -> activities -> exercise_ids`,

This is the reference file to link attempts (`parquet`) to the pedagogical hierarchy.

Main sections:
- `modules`: canonical tree used for pedagogical structure
- `exercise_to_hierarchy`: canonical join bridge from `exercise_id` to activity/objective/module IDs
- `id_label_index`: ID -> label/code dictionary for readable names
- `conflicts`: cross-source diagnostics
- `orphans`: exercise IDs not canonically mappable
- `meta`: build provenance and counts

### 3) `zpdes_rules.json`

Source for ZPDES rules:
- `module_rules` (rules per module),
- `map_id_code` (`code_to_id`, `id_to_codes`),
- `links_to_catalog`, `unresolved_links`,
- `dependency_topology` (compatibility snapshot for graph topology, when present).

Main sections:
- `module_rules`: per-module rule payloads (thresholds, requirements, adaptive behavior)
- `map_id_code`: global code <-> ID mapping
- `dependency_topology`: ready-to-use nodes/edges for graph rendering (if generated)
- `links_to_catalog`: link audit between rule IDs and catalog IDs
- `unresolved_links`: known mapping ambiguities/missing links

### 4) `exercises.json`

Exercise content/configuration:
- exercise type
- instruction payload
- interaction configuration

Join key: `exercise_id` (`adaptiv_math_history.parquet`) -> `id` (`exercises.json`)

## File dependencies

Recommended flow:

1. Start from `adaptiv_math_history.parquet`.
2. Map `exercise_id` through `learning_catalog.exercise_to_hierarchy`.
3. Resolve labels/codes through `learning_catalog.id_label_index`.
4. Use `zpdes_rules.json` for dependency and activation/deactivation rules.
5. Add exercise content from `exercises.json` if needed.

## How the deployed Streamlit app consumes these files

The deployed app uses local file paths internally (`data/...` and `artifacts/...`), but those files can be synchronized at startup from this Hugging Face dataset repository.

Expected deployment pattern:
1. Keep code in GitHub.
2. Keep runtime files in this HF dataset repo with the same relative paths.
3. Configure app secrets/env:
   - `VISU2_HF_REPO_ID`
   - `VISU2_HF_REVISION` (pin to a tag/commit)
   - `HF_TOKEN` (private access)
4. On startup, the app downloads required runtime files and then runs normally with local paths.

Runtime subset usually needed by the app:
- `data/learning_catalog.json`
- `data/zpdes_rules.json`
- `data/exercises.json`
- `artifacts/reports/consistency_report.json`
- `artifacts/reports/derived_manifest.json`
- `artifacts/derived/*.parquet`

Current lean runtime artifact set excludes:
- `artifacts/derived/agg_student_module_exposure.parquet` (deprecated/removed from build contract).

Note:
- `adaptiv_math_history.parquet` is not required at deployed runtime unless you rebuild derived artifacts inside the deployment environment.

## ZPDES mini glossary

- `ZPDES`: adaptive progression mode where content availability depends on mastery rules.
- `activation`: rule that unlocks a target unit when prerequisite performance reaches a threshold.
- `deactivation`: rule that can close/de-prioritize a unit when conditions are met.
- `requirements`: prerequisite dependency definitions used for activation logic.
- `deact_requirements`: dependency definitions used for deactivation logic.
- `sr`: success-rate condition in a rule (typically a value in [0,1]).
- `lvl`: additional level/step condition used by some rule payloads.
- `init_open` or equivalent: unit available at start before additional unlock conditions.


## Minimal Example (Python)

```python
import json
import polars as pl

fact = pl.read_parquet("adaptiv_math_history.parquet")
catalog = json.load(open("learning_catalog.json", "r", encoding="utf-8"))
rules = json.load(open("zpdes_rules.json", "r", encoding="utf-8"))
exercises = json.load(open("exercises.json", "r", encoding="utf-8"))

exercise_map = catalog["exercise_to_hierarchy"]
print(exercise_map.get("some-exercise-id"))
```

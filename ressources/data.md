---
license: other
license_name: am-license
license_link: LICENSE
language:
- en
---
# AdaptivMath Data Reference

## 1. Purpose and Scope

This document is the canonical data reference for the current visualization and analytics scope.

Scope of this document:
- Describe all datasets present in `data/`.
- Define the canonical join model and source precedence.
- Document known cross-source inconsistencies and required handling rules.
- Provide a design blueprint for next-phase derived datasets (design only, no generation yet).

## 1.1 Migration Update (2026-02-23)

Runtime and artifact generation rely on this standalone package:
- `data/adaptiv_math_history.parquet`
- `data/learning_catalog.json`
- `data/zpdes_rules.json`
- `data/exercises.json`

Legacy files may still exist in the repository for historical traceability, but they are deprecated and not required by runtime/build flows:
- `data/legacy/summary.json`
- `data/legacy/admathGraphe.json`
- `data/legacy/modules_configGraphe.xlsx`

Derived runtime update (2026-02-26):
- Lean derived schema `phase0_thin_slice_v3_lean` is now active.
- `agg_student_module_exposure.parquet` was removed from runtime/build contracts.
- Page-2 exposure and diligent analytics are computed from filtered `fact_attempt_core` directly.

## 2. Dataset Inventory

| File | Format | Size (bytes) | Role |
|---|---:|---:|---|
| `data/adaptiv_math_history.parquet` | Parquet | 195390166 | Attempt-level event fact table (primary analytical source). |
| `data/learning_catalog.json` | JSON | 5519826 | Canonical runtime hierarchy and labels: module -> objective -> activity -> exercise IDs + `exercise_to_hierarchy`. |
| `data/zpdes_rules.json` | JSON | 1134761 | Canonical runtime dependency/rules payload and global `map_id_code` mapping. |
| `data/exercises.json` | JSON | 79082948 | Exercise content and interaction configuration for 8862 exercises. |
| `data/legacy/summary.json` | JSON | 1435454 | Deprecated legacy hierarchy snapshot (historical reference only). |
| `data/legacy/admathGraphe.json` | JSON | 478427 | Deprecated legacy adaptive graph snapshot (historical reference only). |
| `data/legacy/modules_configGraphe.xlsx` | XLSX | 348521 | Deprecated legacy authoring workbook (historical reference only). |

## 3. Per-File Schema and Semantics

### 3.1 `adaptiv_math_history.parquet`

#### Metadata
- Rows: `6264394`
- Columns: `18`
- Row groups: `23`
- Created by: `Polars`
- Time span from column statistics:
  - `created_at`: `2022-08-05T08:17:34.374000+00:00` to `2025-11-20T20:10:14.624000+00:00`
  - `login_time`: `2022-08-05T08:15:22.598000+00:00` to `2025-11-20T20:09:04.559000+00:00`

#### Grain and key concept
- Grain: one row = one exercise attempt event.
- Primary key concept: no explicit single ID column; treat each row as an immutable attempt fact.

#### Schema (18 columns)
- `classroom_id` (string, nullable)
- `teacher_id` (string, nullable)
- `user_id` (string, non-null)
- `playlist_or_module_id` (string, non-null)
- `objective_id` (string, non-null)
- `activity_id` (string, non-null)
- `exercise_id` (string, non-null)
- `module_long_title` (string, nullable)
- `created_at` (timestamp, UTC, non-null)
- `login_time` (timestamp, UTC, nullable)
- `data_correct` (boolean, non-null)
- `work_mode` (string, non-null)
- `data_answer` (string, non-null; can contain plain values or serialized structured payloads)
- `data_duration` (float64, non-null)
- `session_duration` (float64, nullable)
- `student_attempt_index` (uint32 semantics, non-null)
- `attempt_number` (uint32 semantics, non-null)
- `first_attempt_success_rate` (float64, non-null)

#### Null-rate details (sparse columns)
- `teacher_id`: `135217` nulls (`2.1585%`)
- `classroom_id`: `2093` nulls (`0.0334%`)
- `login_time`: `158029` nulls (`2.5227%`)
- `session_duration`: `158029` nulls (`2.5227%`)
- `module_long_title`: `149` nulls (`0.0024%`)

#### Join keys for analytics
- Main foreign keys for joins:
  - `playlist_or_module_id`
  - `objective_id`
  - `activity_id`
  - `exercise_id`

#### Playlist-mode caveat in raw parquet
- In `work_mode = playlist`, raw `objective_id` and `activity_id` are placeholder `"None"` values (not canonical IDs).
- Raw playlist slice facts:
  - rows: `1470`
  - distinct `exercise_id`: `812`
  - distinct `activity_id` excluding placeholder: `0`
- Implication: hierarchy for playlist rows must be recovered through `exercise_id` mapping, not raw objective/activity fields.

### 3.2 `learning_catalog.json` (canonical runtime hierarchy)

#### Top-level structure
- `meta`
- `id_label_index`
- `modules` (nested hierarchy)
- `exercise_to_hierarchy`
- `conflicts`
- `orphans`

#### Active module scope
- `M1`, `M31`, `M32`, `M33`, `M41`, `M42`, `M43`

#### Canonical model
- Nested hierarchy:
  - `module -> objectives -> activities -> exercise_ids`
- Flat hierarchy map:
  - `exercise_to_hierarchy[exercise_id] -> {activity_id, objective_id, module_id}`
- Label/code lookup:
  - `id_label_index[id] -> {type, code, short_title, long_title, sources}`

Integrity checks on this file:
- modules: `7`
- objectives: `95`
- activities: `454`
- exercise refs: `8862`
- `exercise_to_hierarchy`: `8862`

This file is the canonical hierarchy source for runtime joins.

### 3.3 `zpdes_rules.json` (canonical runtime dependency/rules payload)

Top-level structure:
- `meta`
- `module_rules` (`11` module entries)
- `map_id_code`:
  - `code_to_id`
  - `id_to_codes`
- `links_to_catalog`
- `unresolved_links`

Role:
- Primary runtime source for rule extraction and dependency graph fallback logic.
- Optional compatibility topology can be stored under:
  - `dependency_topology[module_code].nodes`
  - `dependency_topology[module_code].edges`

Use in this project:
- Runtime source for ZPDES page/module rule parsing.
- `map_id_code` fallback for module/objective/activity code enrichment in derivation.

### 3.4 `exercises.json`

#### Top-level structure
- Root key: `exercises`
- Exercise records: `8862`
- Unique `id`: `8862`
- Exercise types: `12`

#### Type distribution
- `MULTI_GAMEPLAY`: `4009`
- `INPUT_LINE_GLOBAL`: `1480`
- `MULTIPLE_CHOICE`: `1021`
- `GRADUATED_RULER`: `665`
- `DROP_COMPARE`: `540`
- `ALTER_SUM`: `308`
- `INTERVAL_COLORING`: `240`
- `DROP_QUANTITY`: `173`
- `ARC_ON_RULER`: `162`
- `NUMBER_BOX`: `147`
- `RECTANGLE_NUMBER`: `105`
- `MEMORY`: `12`

#### Important caveat (must follow)
Fields `modules`, `objectives`, `activities`, and `index` in this file are placeholders in this extract:
- `modules = ['1']` for all rows
- `objectives = ['1']` for all rows
- `activities = ['1']` for all rows
- `index = 1` for all rows

Therefore:
- Do not use `exercises.json` hierarchy fields for joins.
- Canonical hierarchy join must use `learning_catalog.exercise_to_hierarchy`.

### 3.5 `summary.json` (deprecated legacy file)

Role:
- Historical hierarchy reference only.
- Not required by runtime, checks, or artifact generation.

### 3.6 `admathGraphe.json` and `modules_configGraphe.xlsx` (deprecated legacy files)

Role:
- Historical references only.
- Not required by runtime, checks, or artifact generation.

## 4. Cross-Dataset Relationship Model

Canonical hierarchy contract:
- `module_id -> objective_id -> activity_id -> exercise_id`
- Hierarchy source: `learning_catalog.json` (not `exercises.json` hierarchy fields).

Attempt fact contract:
- Attempt fact source: `adaptiv_math_history.parquet`
- Main join keys: `playlist_or_module_id`, `objective_id`, `activity_id`, `exercise_id`

Recommended join flow for current scope:
1. Start from attempts in Parquet.
2. Join hierarchy labels and structure using `learning_catalog.exercise_to_hierarchy` + `id_label_index`.
3. Join exercise content/config using `exercise_id -> exercises.json.id`.
4. Use `zpdes_rules.json` for rule payloads and code/id fallback.
5. Do not use deprecated legacy files as dependencies for runtime or build.

## 5. Source Precedence (Current Visualization Scope)

1. Hierarchy and exercise membership truth:
- `learning_catalog.json` (primary)

2. Attempt-level behavioral truth:
- `adaptiv_math_history.parquet` (primary)

3. Exercise content/config truth:
- `exercises.json` (primary for content payloads, not for hierarchy joins)

4. Rule payload and code mapping truth:
- `zpdes_rules.json` (primary for runtime dependency and code fallback)

5. Legacy historical context:
- `data/legacy/summary.json`, `data/legacy/admathGraphe.json`, `data/legacy/modules_configGraphe.xlsx` (deprecated, non-runtime)

## 6. Known Inconsistencies and Handling Rules

### 6.1 Requested transparency facts
- `summary` vs `modules_configGraphe.xlsx` modules:
  - overlap `7`, summary-only `0`, xlsx-only `13`
- `summary` vs `modules_configGraphe.xlsx` activities:
  - overlap `407`, summary-only `47`, xlsx-only `178`
- Of the `407` overlapping activities, `75` have differing exercise membership sets.
- `47` summary activities are absent from both:
  - `modules_configGraphe.xlsx`
  - `admathGraphe.json` mappings

### 6.2 Practical handling rules
- Rule A: For active scope visuals and metrics, trust `learning_catalog.json` for activity -> exercise membership.
- Rule B: Treat `data/legacy/summary.json`, `data/legacy/modules_configGraphe.xlsx`, and `data/legacy/admathGraphe.json` as deprecated historical references, not runtime truth.
- Rule C: Ignore `exercises.json` hierarchy fields (`modules/objectives/activities/index`) for joins.
- Rule D: If scope extends beyond active summary modules, define an explicit contract migration before blending sources.
- Rule E: For playlist attempts with placeholder IDs, backfill `activity_id/objective_id/module_*` from `exercise_id -> learning_catalog.exercise_to_hierarchy` when mappable.

### 6.3 Playlist exercise backfill coverage (current snapshot)
- Distinct playlist `exercise_id` in raw parquet: `812`
- Mappable via canonical `learning_catalog` exercise hierarchy: `756` (`93.1%`)
- Unmapped in summary scope: `56` (`6.9%`)
- For mapped playlist exercises, coverage spans:
  - `56` activities
  - `24` objectives
  - `7` modules

## 7. Derived Dataset Blueprint (Next Phase, Design Only)

No derived dataset is generated in this phase. The following are design targets.

### 7.1 `fact_attempt_core`
- Grain: one row per attempt.
- Source: `adaptiv_math_history.parquet`.
- Suggested columns:
  - `created_at`, `user_id`, `teacher_id`, `classroom_id`
  - `playlist_or_module_id`, `objective_id`, `activity_id`, `exercise_id`
  - `data_correct`, `data_duration`, `session_duration`, `attempt_number`
  - `student_attempt_index`, `work_mode`
- Purpose: fast UI-ready attempt fact for filtering and drill-down.

### 7.2 `agg_activity_daily`
- Grain: one row per (`date`, `activity_id`) with optional classroom/teacher partitions.
- Metrics:
  - attempts, unique students, correctness rate, median duration
- Purpose: activity trend charts and bottleneck detection.

### 7.3 `agg_objective_daily`
- Grain: one row per (`date`, `objective_id`) with optional group partitions.
- Metrics:
  - attempts, unique students, correctness rate, progression proxies
- Purpose: objective-level dashboards and cohort comparisons.

### 7.4 `agg_student_module_progress`
- Grain: one row per (`user_id`, `module_id`, `date` snapshot or week bucket).
- Metrics:
  - attempts completed, unique activities touched, success trend, last activity timestamp
- Purpose: student trajectory summaries and class monitoring.

### 7.5 `agg_transition_edges`
- Grain: directed edge between consecutive learning states for Sankey/path views.
- Suggested state:
  - `activity_id` or `exercise_id` (configurable)
- Metrics:
  - transition count, success-conditioned transition count
- Purpose: trajectory explorer and path bottleneck visualization.

## 8. Validation Checklist

1. Parquet metadata validation:
- Confirm `18` columns and `6264394` rows.
- Confirm sparse-column null rates are documented.

2. Catalog hierarchy integrity:
- Confirm no missing module -> objective and objective -> activity references.
- Confirm catalog exercise memberships are complete and unique (`8862`).

3. Exercise mapping integrity:
- Confirm all `8862` catalog exercise IDs exist in `exercises.json`.

4. Cross-source consistency report:
- Recompute overlaps/mismatches for modules, activities, and overlapping activity membership differences.

5. Documentation quality checks:
- No mojibake artifacts.
- All files in `data/` documented.
- Join caveats and source precedence explicitly stated.

# Maureen Data Files Summary

## Scope
Comparison of:
- `data_maureen/score-progression-f253161a-d2d6-11ed-afa1-0242ac120002 1(in).csv`
- `data_maureen/researcher_data_Comprendre les mots pour mieux les lire(in).csv`

The newer `researcher_data_...csv` file was first repaired in-memory for malformed CSV rows caused by `data_answer` quoting.

## Date Coverage

| File | Rows | Unique students | Date range |
|---|---:|---:|---|
| Old `score-progression...csv` | 57,164 | 937 | 2024-03-19 to 2026-01-23 |
| Repaired new `researcher_data...csv` | 71,589 | 1,024 | 2024-11-06 to 2026-03-18 |

Repair note:
- native well-formed rows in new file: 16,203
- repaired malformed rows: 55,386
- remaining problematic rows: 1

## Student Overlap

| Cohort | Students |
|---|---:|
| Present in both files | 845 |
| Only in old file | 92 |
| Only in new file | 179 |

Date-range interpretation:
- all 92 students only in the old file are entirely **before** the new file's date range
- all 179 students only in the new file are entirely **after** the old file's date range
- this suggests the two files are largely complementary in time

Additional mapping result from the repaired new file:
- old users with recoverable `classroom_id`: 845 / 937 (90.18%)
- `user_id -> classroom_id` mapping is stable for recovered users
- `user_id -> teacher_id` and `user_id -> UAI` are also stable for recovered users

## Column Summary

### Present in old, missing in new
- `variation`
- `module_id`
- `progression_score`
- `initial_test_max_success`
- `initial_test_weighted_max_success`
- `initial_test_success_rate`
- `finished_module_mean_score`
- `finished_module_graphe_coverage_rate`
- `is_gar`

### Present in new, missing in old
- `UAI`
- `classroom_id`
- `teacher_id`
- `playlist_or_module_id`
- `module_short_title`
- `module_long_title`
- `login_time`
- `is_initial_test`
- `data_nb_tries`
- `data_answer`
- `data_duration`
- `session_duration`
- `work_mode`

## Practical Conclusion
- The new `researcher_data...csv` file is richer and useful.
- It is **not** a clean drop-in replacement for the old file because the schema differs and some old fields are missing.
- The safest near-term strategy is:
  - keep the old file as the base attempt history
  - use the repaired new file to backfill `classroom_id`, `teacher_id`, and `UAI` by `user_id`

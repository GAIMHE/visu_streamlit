# MIA interaction overview notebook

## Context and scope

Create a reusable notebook for the MIA interaction parquet that reports overall unique students, exercises, classrooms, and attempts, plus ZPDES-versus-playlist attempt and student percentages overall and by module.

## Main changes

- Added `notebooks/mia_interaction_overview.ipynb`.
- Added narrow `.gitignore` exceptions so this notebook and its iteration log are commit-ready while other local notebooks and iteration files remain ignored.
- Added configurable parquet discovery and optional CSV exports.
- Added overall counts, work-mode summaries, exclusive student participation groups, module totals, and work-mode summaries by module.
- Recovered module identity for playlist attempts through the exercise hierarchy in `config_mia.json` and exposed mapping coverage.

## Important decisions and rationale

- Defined one parquet row as one attempt.
- Included both target-mode percentages and percentages against all work modes to make denominators explicit.
- Kept student-mode percentages non-exclusive and added an exclusive `zpdes only` / `playlist only` / `both` table because students can occur in both modes.
- Used module codes and short titles because displayed module numbers are reused across French and mathematics.
- Preserved unresolved module mappings as `Unmapped` instead of silently dropping them.
- Used DuckDB to scan the 6.4-million-row parquet efficiently without loading the full interaction table into pandas.

## Follow-up actions

- Re-run the notebook after replacing or adding a MIA parquet; set `PARQUET_PATH` explicitly if multiple parquet snapshots are present.
- Enable `EXPORT_RESULTS` when CSV versions of the result tables are needed.

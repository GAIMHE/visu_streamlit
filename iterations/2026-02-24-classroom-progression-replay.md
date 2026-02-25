# 2026-02-24 - Classroom Progression Replay Page

## Context / Scope

Goal of this iteration:
- add a new Streamlit page to replay classroom learning progression as a dynamic student x activity matrix
- keep implementation within existing artifact contracts (no derived schema changes)
- support replay in `zpdes`, `playlist`, and `all` scopes with locked default behavior

Out of scope:
- backend/API refactor
- new derived parquet generation
- changes to existing pages behavior

## Main Changes Made

1. New replay helper module:
- `src/visu2/classroom_progression.py`
- added public helpers:
  - `build_classroom_mode_profiles(...)`
  - `select_default_classroom(...)`
  - `build_replay_payload(...)`
  - `build_heatmap_figure(...)`

2. New Streamlit page:
- `apps/pages/4_classroom_progression_replay.py`
- features implemented:
  - mode scope toggle (`zpdes`, `playlist`, `all`) with default `zpdes`
  - classroom selection with automatic default ranking
  - date range filtering
  - replay controls (play/pause, reset, step, speed, frame slider, max frame cap)
  - mastery threshold slider and optional cell-value annotations
  - anonymized student axis labels with full IDs in hover
  - cap-awareness banner when effective replay step is increased

3. Test coverage:
- added `tests/test_classroom_progression.py`:
  - profile generation and classroom validity filtering
  - default-classroom selection rule behavior
  - replay payload cumulative correctness and empty initial frame
  - frame cap / effective step behavior

4. Documentation:
- updated `README.md` with a dedicated section for the new replay page

## Important Decisions and Rationale

1. Default ZPDES classroom rule:
- enforce eligibility (`15-20` students and `10-49` activities) before ranking
- avoids trivial matrices while preserving larger-cohort preference

2. Replay semantics:
- attempt-by-attempt chronological replay
- frame 0 empty matrix
- cumulative success rate by `(student, activity)` cell

3. Scalability policy:
- retain chronology while capping frame count
- compute `effective_step = max(step_size, ceil(total_events / max_frames))`

4. Privacy/readability:
- anonymized student axis labels (`Student 1..N`)
- full technical identifiers available only in hover metadata

## Validation / Checks Run

Executed:
- `uv run pytest -q` -> PASS
- `uv run python scripts/run_slice.py --smoke` -> PASS
- code-reviewer pass:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose` -> completed
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> completed, no critical findings

Notes:
- Streamlit bare-mode `ScriptRunContext` warnings appear during smoke run and are expected in this execution mode.
- `review_report_generator.py` emits its own `datetime.utcnow()` deprecation warning; this is inside the skill script, not app runtime code.

## Follow-up Actions

1. Optional UX enhancement:
- add a compact timeline scrubber with labels at key checkpoints (start, 25%, 50%, 75%, end)

2. Optional analytics enhancement:
- add a second color mode for cumulative attempt count (same replay engine, alternate metric view)

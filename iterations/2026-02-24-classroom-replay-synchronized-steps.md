# 2026-02-24 - Classroom replay synchronized steps

## Context / scope
- Requested behavior change for the classroom replay page:
  - Keep chronology **inside each student**.
  - Do not enforce chronology **between students**.
  - One replay step should advance all students at once.
- Scope covered:
  - replay payload builder
  - replay page labels/captions
  - unit tests
  - README section for this page

## Main changes made
- Refactored `src/visu2/classroom_progression.py` replay engine:
  - Replaced global-event stepping with synchronized per-student stepping.
  - New frame progression key: `frame_step_counts` (local step index per frame).
  - Kept `frame_event_counts` as cumulative integrated attempts for progress display.
  - Added `total_sync_steps` in payload metadata.
  - Updated cap logic to compute effective step from synchronized steps (not total events).
- Updated `apps/pages/4_classroom_progression_replay.py`:
  - Replay control label now states `attempts per student/frame`.
  - Frame-cap info message updated to synchronized semantics.
  - Caption now shows synchronized step progress + integrated attempts.
  - Intro caption now explains synchronized stepping behavior.
- Updated `tests/test_classroom_progression.py`:
  - Added assertions for synchronized step counts.
  - Added explicit check that frame 1 advances each student by one local attempt.
  - Added uneven-student test to validate step and cumulative event progression.
- Updated `README.md` classroom replay section to match new replay semantics.

## Important decisions and rationale
- Preserved existing matrix metric and rendering model (cumulative success rate by student/activity).
- Added, rather than replaced, progress metadata:
  - `frame_step_counts` for synchronized step logic.
  - `frame_event_counts` retained for cumulative attempt visibility.
- Kept API shape backward-compatible for existing page usage while clarifying semantics in UI text.

## Validation results
- `uv run pytest tests/test_classroom_progression.py -q` -> pass
- `uv run python scripts/run_slice.py --smoke` -> pass
- `uv run pytest -q` -> pass
- Code reviewer workflow:
  - `uv run python .codex/skills/code-reviewer/scripts/code_quality_checker.py . --verbose` -> executed
  - `uv run python .codex/skills/code-reviewer/scripts/review_report_generator.py --analyze` -> executed

## Follow-up actions
- Optional UX refinement: expose a small legend line that explicitly defines
  `synchronized step` vs `integrated attempts` for non-technical users.

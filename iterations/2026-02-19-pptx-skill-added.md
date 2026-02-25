# Iteration: 2026-02-19 - PPTX Skill Added

## Context/Scope
- Add a reusable `pptx` skill for PowerPoint creation, editing, and analysis workflows.
- Register the skill so future turns can trigger it automatically or by explicit name.

## Main Changes
- Added `.codex/skills/pptx/SKILL.md` with the provided PPTX workflow guidance.
- Registered `pptx` in `AGENTS.md` skill list.
- Updated `.codex/skills/README.md` current skills list to include `pptx`.

## Decisions and Rationale
- Kept the provided instructions intact as the main operational source for this skill.
- Registered the skill at repo level to align with existing discovery and trigger rules.

## Follow-ups
- Optional: add bundled `scripts/` and reference docs under `.codex/skills/pptx/` if you want fully local, self-contained tooling.

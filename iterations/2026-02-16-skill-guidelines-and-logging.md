# Iteration: 2026-02-16 - Skill Guidelines and Logging

## Context/Scope
- Establish repo-level guidance for coding practices and AI skill usage.
- Add a durable iteration logging convention for future changes.

## Main Changes
- Updated `AGENTS.md` with mandatory skill usage requirements:
  - `code-reviewer` for non-trivial changes
  - `vercel-react-best-practices` for React/Next.js changes
  - `frontend-design` for UI/frontend design work
- Added `CONTRIBUTING.md` with coding, module, quality, and AI-assisted workflow guidance.
- Created `iterations/README.md` with naming conventions and a reusable template.

## Decisions and Rationale
- Split responsibilities:
  - `CONTRIBUTING.md` for human contributors
  - `AGENTS.md` for AI-agent behavior
- Enforced dated iteration logs to improve traceability and maintain historical context.

## Follow-ups
- Add language/framework-specific lint/test commands once project tooling is finalized.
- Consider adding a PR template that links to `CONTRIBUTING.md` and requires a new iteration log entry when applicable.

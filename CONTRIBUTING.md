# Contributing Guidelines

## Goals
- Keep changes small, testable, and easy to review.
- Prefer consistency with existing patterns over novelty.
- Document decisions when they affect architecture or long-term maintenance.

## Coding Standards
- Use clear names and small focused functions/modules.
- Avoid duplicated logic; extract shared utilities when duplication appears.
- Keep comments concise and only where code intent is not obvious.
- Follow language/framework conventions already used in the repo.

## Modules and Structure
- Add new modules in the most relevant existing domain folder.
- Keep public interfaces explicit (clear inputs/outputs).
- Avoid tight coupling across unrelated modules.
- If introducing a cross-cutting module, include a short rationale in the PR/iteration log.

## Quality Gates
- Run relevant linting/testing before finalizing changes.
- Validate edge cases for new behavior.
- Confirm no sensitive data is introduced.

## Skill Usage (for AI-assisted changes)
- Use `code-reviewer` for non-trivial feature/refactor/bug-fix changes.
- Use `vercel-react-best-practices` when editing React/Next.js code.
- Use `frontend-design` for UI/frontend creation or redesign work.

## Iteration Logs
- For each substantial iteration, add one markdown file in `iterations/`.
- Filename format: `YYYY-MM-DD-short-title.md`.
- Include:
  - Context/scope
  - Main changes
  - Decisions and rationale
  - Follow-ups

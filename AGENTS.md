## Skills
A skill is a set of local instructions to follow that is stored in a `SKILL.md` file. Below is the list of skills available in this repo.

### Available skills
- vercel-react-best-practices: React and Next.js performance optimization guidelines from Vercel Engineering. Use for writing, reviewing, or refactoring React/Next.js code with performance goals. (file: .codex/skills/vercel-react-best-practices/SKILL.md)
- file-organizer: Intelligently organizes files and folders by understanding context, finding duplicates, and suggesting better organizational structures. Use when user wants cleanup, duplicate removal, or project/file restructuring. (file: .codex/skills/file-organizer/SKILL.md)
- frontend-design: Create distinctive, production-grade frontend interfaces with high design quality. Use for building or styling web pages, components, dashboards, and UI artifacts with strong visual direction. (file: .codex/skills/frontend-design/SKILL.md)
- canvas-design: Create original visual art in `.png` and `.pdf` driven by a design philosophy, with minimal text and high craftsmanship. Use for posters, static art pieces, and design artifacts. (file: .codex/skills/canvas-design/SKILL.md)
- pptx: Presentation creation, editing, and analysis workflows for `.pptx` files, including text extraction, OOXML edits, template-based generation, and thumbnail-based visual validation. (file: .codex/skills/pptx/SKILL.md)
- code-reviewer: Comprehensive code review skill for TypeScript, JavaScript, Python, Swift, Kotlin, and Go. Use for PR reviews, issue finding, quality checks, and review report generation. (file: .codex/skills/code-reviewer/SKILL.md)

### How to use skills
- Trigger rules: If a skill name is mentioned explicitly (for example `$vercel-react-best-practices`) or the task clearly matches its description, use that skill for the turn.
- Loading: Open the relevant `SKILL.md` and load only the sections/files needed for the current task.
- Scope: Do not carry skills across turns unless re-mentioned.
- Fallback: If a skill file is missing or unclear, state the issue briefly and continue with best-effort guidance.

## Engineering Workflow Requirements
- For any non-trivial feature, refactor, or bug fix, run a review pass using `code-reviewer` before finalizing.
- For React/Next.js code changes, apply `vercel-react-best-practices`.
- For frontend/UI creation or redesign tasks, apply `frontend-design`.
- If one or more skills are used, state which skill(s) were applied in a short line in the final response.
- If a required skill cannot be applied (missing files, missing tooling), state the blocker and proceed with best-effort fallback.

## Iteration Log Requirement
- Keep a dated markdown log in `iterations/` for each substantial iteration.
- Filename format: `YYYY-MM-DD-short-title.md` (example: `2026-02-16-skill-guidelines-and-logging.md`).
- Each log must include:
  - Context/scope of the iteration
  - Main changes made
  - Important decisions and rationale
  - Follow-up actions (if any)

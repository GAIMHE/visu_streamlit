# 6-Month Roadmap Reset (Intern Arrival)

1. Planning horizon: **6 months from the intern's arrival**
2. Main objective: **turn the current analytics work into a coherent research program, a validated figure set, a paper draft, and a stronger app**
3. Working reference for project status:
   - the **broader multi-page analytics app** is the project baseline
   - the current `inspection` branch is a **focused experimental branch** and should not be treated as the canonical product state for onboarding

---

## Current Baseline

- The core runtime data contract is in place:
  - `data/adaptiv_math_history.parquet`
  - `data/learning_catalog.json`
  - `data/zpdes_rules.json`
  - `data/exercises.json`
- The derived-artifact pipeline exists and is documented.
- The broader analytics app already has documented page families and figure descriptions.
- A focused cohort-inspection tool exists on the current branch for internal filtering and debugging.
- Figure documentation already exists in:
  - `README.md`
  - `ressources/STREAMLIT_FIGURES_GUIDE.md`
  - `ressources/figures/README.md`
- A dataset paper outline already exists in:
  - `ressources/NEURIPS_2026_DATASET_PAPER_OUTLINE.md`
- What is still missing is not the basic infrastructure, but:
  - clearer research prioritization
  - stronger figure validation
  - clearer distinction between stable views and exploratory/internal ones
  - a practical onboarding path for the intern

---

## Scope for the Next 6 Months

- Select a realistic, publication-relevant set of research questions.
- Match those questions to figures and analyses that are already useful, and identify what still needs validation or redesign.
- Consolidate the app so it supports both:
  - internal research exploration
  - a more stable external-facing visualization story
- Prepare the paper and figure package in parallel with app/documentation maturation.

---

## Timeline and Deliverables

## Pre-arrival / This Week
### Goals
- Remove ambiguity before the intern starts.
- Make the current project state legible.
- Give the intern one clear entry point for week 1.

### Tasks
- Refresh this roadmap using the current repo state.
- Prepare an intern onboarding file with:
  - project summary
  - current app/figure state
  - essential docs and commands
  - first-week checklist
- Clarify what counts as:
  - stable baseline
  - useful but still needing validation
  - exploratory/internal

### Deliverables
- Updated roadmap
- Intern onboarding pack
- Short list of known documentation or figure ambiguities to watch from week 1

## Intern Week 1
### Goals
- Understand the project, the current app, and the figure landscape.
- Reframe the existing work in terms of research questions and evidence quality.

### Tasks
- Run the app locally and browse the main pages.
- Read the figure guide and page documentation.
- Match each major page to one or more candidate research questions.
- Identify:
  - which figures already look useful for research
  - which metrics or visual choices are unclear
  - which pages feel exploratory/internal rather than publication-ready
- Keep a short structured note of:
  - confusing metrics
  - undocumented assumptions
  - missing validation checks
  - likely paper-useful views

### Expected Outputs
- App and figure audit note
- Prioritized shortlist of research questions and supporting figures
- List of terminology/documentation gaps
- Recommended first implementation or analysis tasks for week 2

## Intern Month 1
### Goals
- Lock the first research direction and stabilize the most relevant figures.

### Tasks
- Select a primary research-question set and a smaller backup set.
- Build an analysis-to-visualization mapping for the selected questions.
- Review which existing pages are:
  - directly reusable
  - usable with validation/refinement
  - out of scope for the first paper pass
- Define what must be measured or checked before claiming a figure is paper-ready.

### Deliverables
- Final selected question set
- Figure/question mapping table
- First validated shortlist of figures to carry into the paper workflow

## Months 2-3
### Goals
- Produce robust analysis outputs and make the selected figures defensible.

### Tasks
- Run the main analyses tied to the selected questions.
- Check metric definitions and figure consistency across pages.
- Refine labels, interpretations, and caveats.
- Build reusable tables or exports that support both the app and the paper.

### Deliverables
- Reproducible analysis outputs
- Refined figure set for the primary questions
- Interpretation notes and caveat log

## Months 4-5
### Goals
- Turn the selected work into a coherent publication and product package.

### Tasks
- Draft methods, dataset, and results sections of the paper.
- Improve the app around the selected narrative:
  - cleaner labels
  - clearer page roles
  - reduced ambiguity between exploratory and stable views
- Prepare a more stable presentation/deployment package.

### Deliverables
- Paper draft with aligned figure set
- Improved app/documentation package
- Prioritized list of remaining fixes before submission or demo handoff

## Month 6
### Goals
- Finalize the submission-ready package.

### Tasks
- Polish the paper, captions, and key figures.
- Remove remaining inconsistencies between text, metrics, and visuals.
- Freeze the release/demo story.

### Deliverables
- Submission-ready paper draft
- Final figure pack
- Stable internal release snapshot

---

## Research Question Portfolio

Current candidate questions:
1. Are adaptive paths (`zpdes`) associated with faster or cleaner progression than other work modes?
2. Which activities or transitions look like the strongest bottlenecks?
3. Which trajectory patterns are associated with stronger or weaker outcomes?
4. Which learner profiles emerge from retries, pacing, persistence, or classroom context?
5. Which content areas combine high effort with low success, and are they product or pedagogical priorities?
6. How do placement modes (`initial-test`, `adaptive-test`) shape later trajectories?
7. Which current figures are descriptive only, and which can support stronger research claims?

Selection rule:
- prioritize questions that score well on:
  - feasibility with the current data and app
  - publication relevance
  - interpretability of the resulting figures
  - product or teacher usefulness
  - ability to validate the claim with reasonable effort

---

## Working Principles for the Intern Phase

- Use the **broader app documentation** as the reference map of the project.
- Treat the `inspection` branch as an internal focused tool, not as the main product definition.
- Prefer a small number of well-understood, validated figures over a large set of loosely interpreted views.
- Keep a running distinction between:
  - descriptive views
  - validated analytical views
  - exploratory/internal tools
- Keep paper direction, figure usefulness, and documentation clarity aligned from the start.

---

## Validation Expectations

## Research validity
- Re-running the analysis should reproduce the main result tables and figures.
- Definitions of key metrics should be stable and documented.
- Figure interpretations should include caveats when a chart is descriptive rather than causal.

## App/documentation validity
- A new contributor should be able to understand the page landscape from the docs without guesswork.
- The same metric should not appear with conflicting meanings across pages.
- Stable and exploratory views should be clearly distinguished in internal documentation.

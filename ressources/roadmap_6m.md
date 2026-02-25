# 6-Month Roadmap (Month 1 Solo + 5 Months With Intern)

1. Planning horizon: **6 months** (M1 solo, M2-M6 with intern).
2. Goal: publication + web-site
3. Baseline status:
  - cleaning/preprocessing complete
  - exploratory prototype started
4. Data:
  - `adaptiv_math_history.parquet`
  - `learning_catalog.json`
  - `zpdes_rules.json`
  - `exercises.json`

---

## Scope

- Select and answer a set of feasible research questions.
- Turn results into interpretable visualizations.
- Build a production-ready visualization platform (beyond Streamlit).
- Prepare  a paper draft and release web-site.

---

## Month-by-Month Plan (Concrete Tasks)

## M1 (solo)
### Goals
- Consolidate what is already done and remove ambiguity before intern arrival.
### Tasks
- Freeze baseline data contract and metric definitions.
- Define candidate research question list.
- Write onboarding package for intern (repo map, scripts, conventions, key scripts and what they do, glossary).
### Deliverables
- Baseline status note (done/in-progress inventory).
- Question longlist + scoring sheet
- Onboarding pack + templates
- Prepare weekly meeting rhythm

## M2 (intern month 1)
### Goals
- Onboard intern and select what will be analyzed and visualized.
### Tasks
- Intern onboarding (codebase, data flow, visualization goals).
- Score and select primary & secondary questions
- Define analysis plans and expected visual outputs per selected question.
### Deliverables
- Final selected question set
- Completed analysis-to-visualization mapping table

## M3 (intern month 2)
### Goals
- Produce analysis outputs for primary questions.
### Tasks
- Descriptive/trajectory/segmentation analyses (as selected).
- Build reusable result tables for visualization consumption.
- Create figures for each primary question.
### Deliverables
- Reproducible analysis scripts
- Draft result tables + figures
- Interpretation notes

## M4 (intern month 3)
### Goals
- Move from exploratory prototype to production architecture.
### Tasks
- Implement production stack skeleton (front-end + data serving).
- Implement priority views.
- Add filter model and shared metric dictionary in UI.
- Write UI specification page by page:
  - purpose
  - chart type
  - filters
- Draft methods/results section of paper.
### Deliverables
- Internal web app
- Visualization specification
- Paper draft (methods + preliminary results)

## M5 (intern month 4)
### Goals
- Deliver a platform with core pages working end-to-end.
### Tasks
- Feedback sessions.
- Refine app.
### Deliverables
- Functionnal app
- Feedback summary with prioritized fixes.

## M6 (intern month 5)
### Goals
- Finalize publication package and deployment-ready website.
### Tasks
- Complete paper draft and figure set.
### Deliverables
- Submission-ready paper draft.

---

## Research Question Portfolio

Potential questions:
1. Are adaptive paths (`zpdes`) associated with faster mastery signals than fixed playlists?
2. Which trajectory patterns are linked to strong vs weak progression?
3. Which activity transitions are most associated with later success/failure?
4. Which learner profiles emerge from retries, pacing, and persistence?
5. Where are the strongest bottlenecks in module/objective/activity pathways?
6. How do diagnostic modes (`initial-test`, `adaptive-test`) relate to later progression?
7. Which parts of content show high effort but low success (product improvement targets)?

Selection rule:
- Keep only questions with strong combined score on:
  - feasibility now
  - publication relevance
  - teacher usefulness
  - product value for EvidenceB
  - visualization clarity

---

## Analysis-to-Visualization Mapping
For each selected question, include a 1-row mapping table with:
- Question
- Method (plain language)
- Main metrics
- Visualization type
- Expected insight
- Publication contribution statement

---

## Production Visualization Platform Plan

## Target architecture
- Front-end: production web app (not Streamlit)
- Back-end: API serving precomputed analytics
- Data layer: derived/aggregated tables for interactive queries

## UX principles
- Plain labels, minimal jargon
- One clear takeaway per panel

---

## Time and Deliverables (At a Glance)

| Month | Primary output |
|---|---|
| M1 (March) | Intern-ready execution package |
| M2 (April) | Locked questions |
| M3 (May) | Draft results + draft figures |
| M4 (June) | Final analysis |
| M5 (July) | Web-app |
| M6 (August) | Paper draft |

---

## Test Cases and Validation Scenarios

## Research validity
- Re-run analysis scripts and reproduce all primary figures/tables.
- Sensitivity checks do not reverse main conclusions.
- Segment/trajectory definitions are stable across reruns.

## Product validity
- Each v1 chart can be interpreted by non-technical users in usability sessions.
- Filters update consistently across pages.
- No contradictory numbers between views.

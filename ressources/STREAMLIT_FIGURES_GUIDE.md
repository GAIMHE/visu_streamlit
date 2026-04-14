# Streamlit Figures Guide (Plain Language)

This document explains what each figure in the app shows, and the practical question it can answer.

Important note:
- These visuals are descriptive.
- They help spot patterns and compare groups.
- They do **not** prove cause-and-effect by themselves.
- This guide describes the broader multipage app as the reference landscape.
- On the current `inspection` branch, the visible UI may be reduced to the `Cohort Filter Viewer` plus source selection.

---

## Page: Learning Analytics Overview

### 1) Overview KPIs (metric cards)
**What it shows**
- A compact snapshot of the currently filtered attempt history:
  - total attempts
  - unique students
  - unique exercises

**Question it can answer**
- How large is the current slice?
- How many learners and exercises are represented in this slice?

---

### 2) Work Mode Summary Table
**What it shows**
- One row per work mode with:
  - attempts
  - attempt-weighted success rate
  - exercise-balanced success rate
  - unique students
  - curriculum width metrics
  - median attempts per activity
  - repeat-attempt rate

**Question it can answer**
- Which work modes combine broad exploration with strong success?
- Which work modes rely more on repetition than on breadth?

---

### 3) Attempt Concentration
**What it shows**
- A bar chart of attempt share after the current overview filters are applied.
- You can switch between:
  - content concentration
  - student concentration
- In content concentration, you can look at exercises, activities, objectives, or modules.
- In student concentration, students are ranked globally across the visible slice.
- The bars are rank buckets such as `Top 10%`, `10-20%`, and so on, except for module-level content concentration where each bar is one module.
- Clicking a bar opens the relevant drilldown table below the chart.

**Question it can answer**
- How much of the total attempt volume is covered by the most-used exercises, activities, or objectives?
- How much of the visible work is done by the top 10% students?
- Which exact entities or students sit inside a dominant bucket?

---

### 4) Work Mode Transitions
**What it shows**
- A global Sankey diagram of student work-mode histories.
- Each student starts in their first observed mode, then follows up to the first three mode changes seen in the raw attempt history.
- Students who never change mode end in `No transition`.
- Students who continue changing after the third visible change end in `More than 3 transitions`.
- This figure ignores the page filters above so it can preserve full student histories.
- Very small links are hidden when they involve fewer than 10 students.

**Question it can answer**
- What is the most common first work mode?
- How often do students stay in one mode versus move across modes?
- Which work-mode changes are the most common first, second, or third transitions?

---

## Page: Bottlenecks and Transitions

### 5) Bottleneck Candidates (horizontal bar chart)
**What it shows**
- Ranks modules/objectives/activities (you choose the level) where students struggle the most.
- Bar length shows failure rate directly.
- Bar color shows repeat attempt rate.
- The combined score is still kept in analysis/hover as a secondary summary signal.

**Question it can answer**
- Where are students getting stuck the most?
- Which activities/objectives should teachers review first?

---

### 6) Path Transitions (horizontal bar chart)
**What it shows**
- Most frequent transitions from one activity to the next (focused on cross-objective transitions).
- Also shows how often the destination attempt is successful (as a count).

**Question it can answer**
- What are the most common learning paths?
- Which transitions seem common but lead to weaker outcomes?

---

## Page: Objective-Activity Matrix Heatmap

### 7) Objective-Activity Matrix (heatmap)
**What it shows**
- Rows = objectives in the selected module.
- Columns = local activity positions (`A1..An`) within each objective.
- Cell color = selected metric:
  - attempts
  - success rate
  - exercise-balanced success rate
  - activity mean exercise Elo
  - repeat attempt rate
  - first-attempt success rate (if available)
  - playlist unique exercises
- A `Cohort population` control can restrict the non-Elo metrics to:
  - all modes
  - `zpdes`
  - `playlist`
- Interaction: click populated squares directly to open exercise drilldown.

**Question it can answer**
- Inside a module, where are strong/weak zones across objectives and activity positions?
- Do early vs late activities in objectives show different performance?
- Which activities look intrinsically harder when difficulty is calibrated from exercise history?

---

## Page: ZPDES Transition Efficiency

### 8) Transition-Efficiency Graph (network/lanes graph with cohort hover metrics)
**What it shows**
- The structural ZPDES lane layout for one module.
- Activity circles are colored either by:
  - first-attempt success rate in `zpdes`, or
  - activity mean exercise Elo
- Hover always shows ZPDES first-attempt success and event counts for the activity.
- A typed threshold input defines how many prior later-activity attempts are required for the `after` cohort
- Hover on one activity shows:
  - `before` cohort success rate, eligible event count, unique-student count, and total previous attempts
  - `after` cohort success rate, eligible event count, unique-student count, and total previous attempts
  - `in-activity` cohort success rate, eligible event count, unique-student count, and total previous attempts

**Question it can answer**
- Do students attempting a new exercise from strictly earlier content perform differently from students who already explored later content?
- Which activities attract many event-level `after` or `in-activity` attempts?
- Are weak outcomes concentrated among these `after` or `in-activity` cohorts?

---

## Page: Module 1 Individual Path

### 9) Module 1 Path Replay (dependency replay graph)
**What it shows**
- One selected student's full attempt path inside Module 1.
- The attempts are replayed on top of the fixed M1 dependency layout.
- The page keeps the structural topology fixed and lets you step through the student's local progression over time.
- It mixes all visible work modes inside M1 for that student.

**Question it can answer**
- In what order did this student reach activities inside M1?
- Where do retries or pauses accumulate on the dependency layout?
- Does the student follow a clean path or bounce across different parts of the module?

---

## Page: Classroom Progression Replay

### 10) Student x Activity Replay Matrix (animated heatmap)
**What it shows**
- A classroom matrix:
  - X-axis = students (anonymized)
  - Y-axis = activities
- The page first narrows classrooms by target student count inside the selected work-mode scope.
- Matching classrooms are ordered by activity coverage first, then attempts, then students.
- Replay starts empty, then fills in synchronized steps.
- Each cell color = cumulative success rate for that student on that activity.
- The color scale uses a fixed `75%` reference threshold.

**Question it can answer**
- How does progression spread across a class over time?
- Are students progressing together or diverging?
- Which activities become class-wide bottlenecks during the sequence?

---

## Page: Classroom Progression Sankey

### 11) Classroom Activity Sankey (static Sankey)
**What it shows**
- A static Sankey diagram for one selected classroom in the chosen work-mode scope.
- Each student contributes the ordered sequence of activities they reach for the first time.
- Revisits to already-seen activities are intentionally ignored so the diagram focuses on progression breadth rather than loops.
- A `Visible activity steps` control keeps only the first few reached activities visible before sending longer paths to `More than N activities`.
- The page keeps the current size-based matching workflow and also allows a direct classroom-ID override inside the selected scope.

**Question it can answer**
- What is the dominant classroom entry activity?
- Do students mostly share one progression route, or does the classroom split across many paths?
- How many students stop early versus continue beyond the visible path depth?

---

## Page: Student Elo Evolution

### 12) Student Elo Evolution (replay line chart)
**What it shows**
- One student's Elo trajectory over that student's own attempt sequence under two systems:
  - the current retrospective item-Elo calibration
  - the new iterative offline calibration
- Both systems replay the same student histories and use the same student update rule.
- The line replay advances in synchronized local steps (for example every 10 attempts).
- Optional dotted markers can flag large timestamp gaps without changing the attempt-ordinal x-axis.

**Question it can answer**
- How much does the student trajectory depend on the item calibration method?
- Does the iterative system mostly change early burn-in behavior, or does it also change later progression?
- Where are the long inactivity gaps in the selected student's history?

---

## Page: Student Objective Spider

### 13) Student Objective Spider (radar chart)
**What it shows**
- One selected student inside one selected module.
- One spoke per catalog objective in that module.
- Two overlaid traces:
  - all-attempt success rate
  - coverage percentage based on distinct attempted exercises
- Objectives the student never touched remain visible so breadth gaps are explicit.

**Question it can answer**
- Which objectives has this student explored broadly versus only lightly?
- Are there objectives where coverage is high but success is still weak?
- Which parts of the selected module remain untouched for this student?

---

## Internal Page: Cohort Filter Viewer

### 14) Cohort Filter Viewer (funnel + final slice summary)
**What it shows**
- A cohort-definition tool rather than a teaching-facing figure.
- You can combine filters such as:
  - modules to keep
  - minimum student history
  - retry caps
  - transition counts
  - exact work-mode schemas
- The page then shows:
  - how many students remain
  - how many attempts remain
  - how attempts are distributed across modules
  - which cleaned schemas remain in the final slice

**Question it can answer**
- How much of the dataset remains after a given cohort definition?
- Does a filtered cohort still represent a large enough population?
- Which work-mode patterns dominate inside the filtered slice?

---

## Quick reading tips

- Each active analytical block now pairs the visualization with two collapsed helpers:
  - `Info`: what is shown, the metrics, and the main controls.
  - `Analysis`: deterministic findings tied to the broader analytical scope of the page, with caveats shown when the evidence is too thin.
- `Analysis` panels are intentionally cautious:
  - they reuse the page's existing synced data,
  - they may ignore narrow example selectors when the page is meant to surface a broader population pattern,
  - they only promote stronger comparisons when the scope is large enough,
  - they can surface ranked lists rather than a single top item,
  - and, when clean counts or Elo deltas are available, they may add lightweight statistical checks to distinguish strong signals from noise,
  - and any explanation is phrased as a plausible interpretation rather than a proven cause.
- Use filters first (date/module/objective/activity/classroom) to avoid mixed signals.
- Start with the broad overview, then move to bottlenecks/transitions or drill down into the matrix and graph pages.
- For intervention decisions:
  1. Find weak points (overview, bottlenecks, matrix),
  2. Check path context (transitions/dependencies),
  3. Compare individual trajectories if needed (Student Elo),
  4. Check class-level dynamics (replay page).

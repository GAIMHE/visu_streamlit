# Streamlit Figures Guide (Plain Language)

This document explains what each figure in the app shows, and the practical question it can answer.

Important note:
- These visuals are descriptive.
- They help spot patterns and compare groups.
- They do **not** prove cause-and-effect by themselves.

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

## Page: Bottlenecks and Transitions

### 3) Bottleneck Candidates (horizontal bar chart)
**What it shows**
- Ranks modules/objectives/activities (you choose the level) where students struggle the most.
- Score combines:
  - low success
  - many repeated attempts

**Question it can answer**
- Where are students getting stuck the most?
- Which activities/objectives should teachers review first?

---

### 4) Path Transitions (horizontal bar chart)
**What it shows**
- Most frequent transitions from one activity to the next (focused on cross-objective transitions).
- Also shows how often the destination attempt is successful (as a count).

**Question it can answer**
- What are the most common learning paths?
- Which transitions seem common but lead to weaker outcomes?

---

## Page: Objective-Activity Matrix Heatmap

### 5) Objective-Activity Matrix (heatmap)
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

### 6) Transition-Efficiency Graph (network/lanes graph with cohort hover metrics)
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

## Page: Classroom Progression Replay

### 7) Student x Activity Replay Matrix (animated heatmap)
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

## Page: Student Elo Evolution

### 8) Student Elo Evolution (replay line chart)
**What it shows**
- One or two students' Elo trajectories over their own attempt sequence.
- Exercise difficulty is fixed from the historical calibration.
- The line replay advances in synchronized local steps (for example every 10 attempts).

**Question it can answer**
- Is a student improving steadily, plateauing, or oscillating?
- Do two students with similar attempt volume progress at the same pace?
- Do failures cause short dips followed by recovery, or longer stagnation?

---

## Quick reading tips

- Use filters first (date/module/objective/activity/classroom) to avoid mixed signals.
- Start with the broad overview, then move to bottlenecks/transitions or drill down into the matrix and graph pages.
- For intervention decisions:
  1. Find weak points (overview, bottlenecks, matrix),
  2. Check path context (transitions/dependencies),
  3. Compare individual trajectories if needed (Student Elo),
  4. Check class-level dynamics (replay page).

# Streamlit Figures Guide (Plain Language)

This document explains what each figure in the app shows, and the practical question it can answer.

Important note:
- These visuals are descriptive.  
- They help spot patterns and compare groups.  
- They do **not** prove cause-and-effect by themselves.

---

## Page: Learning Analytics Overview

### 1) Work Mode Footprint and Depth (grouped bar chart)
**What it shows**
- For each work mode (`zpdes`, `playlist`, etc.), how much content was explored:
  - number of modules
  - number of objectives
  - number of activities
- The page also shows two success-rate views by work mode:
  - attempt-weighted success rate
  - exercise-balanced success rate (each exercise weighted equally)

**Question it can answer**
- Which work mode leads to broader exploration of the curriculum?
- Do students in one mode visit more content than in another mode?

---

### 2) Bottleneck Candidates (horizontal bar chart)
**What it shows**
- Ranks modules/objectives/activities (you choose the level) where students struggle the most.
- Score combines:
  - low success
  - many repeated attempts

**Question it can answer**
- Where are students getting stuck the most?
- Which activities/objectives should teachers review first?

---

### 3) Path Transitions (horizontal bar chart)
**What it shows**
- Most frequent transitions from one activity to the next (focused on cross-objective transitions).
- Also shows how often the destination attempt is successful (as a count).

**Question it can answer**
- What are the most common learning paths?
- Which transitions seem common but lead to weaker outcomes?

---

## Page: Usage, Playlist and Engagement

### 4) Exposure Bucket Distribution by Module (stacked bar chart)
**What it shows**
- For each module, student-module pairs are split into exposure buckets:
  - low (`<=10` attempts)
  - medium (`11-50`)
  - high (`>50`)

**Question it can answer**
- Are students lightly exposed or deeply exposed in each module?
- Which modules are mostly used for quick practice vs intensive work?

---

### 5) Attempts Over Time by Module (line chart)
**What it shows**
- Daily number of attempts per module.

**Question it can answer**
- When does usage increase or decrease?
- Which modules are most used during a given period?

---

### 6) Unique Students Over Time by Module (area chart)
**What it shows**
- Daily number of distinct students active in each module.

**Question it can answer**
- Is module usage broad (many students) or concentrated (few students)?
- Are changes in attempts caused by more students or just heavier use by the same students?

---

### 7) Average Activities per Playlist by Module (bar chart)
**What it shows**
- Average number of distinct activities included in playlists, by module.

**Question it can answer**
- In playlists, which modules are used in a broader vs narrower way?
- Are playlists in some modules very focused (few activities) or diversified?

---

### 8) Activity Usage Within Module (horizontal bar chart)
**What it shows**
- Top activities in the selected module, ranked by:
  - attempts, or
  - unique students (toggle)

**Question it can answer**
- Which activities receive most practice?
- Which activities are central for many learners, and which are underused?

---

## Page: Objective-Activity Matrix Heatmap

### 9) Objective-Activity Matrix (heatmap)
**What it shows**
- Rows = objectives in the selected module.
- Columns = local activity positions (`A1..An`) within each objective.
- Cell color = selected metric:
  - attempts
  - success rate
  - exercise-balanced success rate
  - repeat attempt rate
  - first-attempt success rate (if available)
- Interaction: click populated squares directly to open exercise drilldown.

**Question it can answer**
- Inside a module, where are strong/weak zones across objectives and activity positions?
- Do early vs late activities in objectives show different performance?

---

## Page: ZPDES Dependency Graph

### 10) Dependency Graph (network/lanes graph)
**What it shows**
- Objective lanes and activity nodes.
- Edges represent dependency rules:
  - activation rules
  - deactivation rules
- Optional node overlay for attempts/success/repeat rate.

**Question it can answer**
- Which prerequisites unlock which parts of a module?
- Where do dependency chains look fragile (low performance on key prerequisite nodes)?
- Which nodes are likely critical for progression flow?

---

## Page: Classroom Progression Replay

### 11) Student × Activity Replay Matrix (animated heatmap)
**What it shows**
- A classroom matrix:
  - X-axis = students (anonymized)
  - Y-axis = activities
- Replay starts empty, then fills in synchronized steps.
- Each cell color = cumulative success rate for that student on that activity.

**Question it can answer**
- How does progression spread across a class over time?
- Are students progressing together or diverging?
- Which activities become class-wide bottlenecks during the sequence?

---

## Quick reading tips

- Use filters first (date/module/objective/activity/classroom) to avoid mixed signals.
- Start with broad figures (trends, footprint), then drill down (matrix, bottlenecks, dependency graph).
- For intervention decisions:
  1. Find weak points (bottlenecks/matrix),
  2. Check path context (transitions/dependencies),
  3. Check class-level dynamics (replay page).

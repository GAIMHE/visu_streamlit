# Elo in VISU2

This document is the single overview for Elo in this project.

It explains:
- why Elo exists in the repo
- what the Elo files mean
- what the student Elo page is comparing
- how to interpret the results without over-claiming

---

## 1. Why there is Elo in this project

Elo is used here as a descriptive difficulty-and-trajectory layer.

The goal is not to produce a production mastery engine.
The goal is to build useful retrospective signals such as:
- relative exercise difficulty
- average activity difficulty
- one student's trajectory over time

So Elo in VISU2 should be read as:
- a convenient modeling layer for comparison
- not a causal claim

---

## 2. The two main Elo stages

The Elo pipeline has two main stages.

### Stage A: fixed exercise difficulty

We first estimate one fixed difficulty per exercise context.

This produces:
- `agg_exercise_elo.parquet`

Then activity-level values are derived from exercise-level values:
- `agg_activity_elo.parquet`

Simple interpretation:
- higher exercise Elo = harder exercise
- higher activity Elo = harder activity on average

### Stage B: student replay

Once exercise difficulty is fixed, we replay student trajectories over time.

This produces:
- `student_elo_events.parquet`
- `student_elo_profiles.parquet`

These are used by the Student Elo page.

---

## 3. What counts as an item

An item is not just `exercise_id`.

The project uses the full pedagogical context:
- `module_code`
- `objective_id`
- `activity_id`
- `exercise_id`

This matters because the same exercise can appear in different contexts.

---

## 4. Sequential replay Elo

This is the simpler replay.

Idea:
- keep exercise difficulty fixed
- replay one student inside one module
- update the student score attempt by attempt

So the question is:

- after this attempt, how should the student's score move?

What it is good for:
- seeing short-term movement
- understanding how one attempt changes the curve

What it is not:
- a global lifelong student score
- a claim that the score is a direct measure of learning in a strong causal sense

---

## 5. Batch / iterative replay Elo

This is the smoother replay.

Idea:
- keep the same fixed exercise difficulty
- at each step, refit the student score from the full history seen so far in that module

So the question is:

- given everything the student has done so far in this module, what is the best current estimate of the student's level?

What it is good for:
- smoother trajectories
- less sensitivity to one noisy attempt
- comparison against the simpler sequential replay

---

## 6. What the Student Elo page compares

The Student Elo page compares two ways of replaying the same student:

- the current sequential replay
- the smoother batch / iterative replay

So the comparison is mostly about:
- replay logic
- not about a completely different exercise difficulty calibration

This means:
- if the curves differ, the difference comes mainly from how the student is updated over time

---

## 7. Main Elo files in the repo

### Difficulty side

- `artifacts/derived/agg_exercise_elo.parquet`
  - fixed exercise difficulty
- `artifacts/derived/agg_activity_elo.parquet`
  - average activity difficulty

### Student replay side

- `artifacts/derived/student_elo_events.parquet`
  - per-attempt sequential replay events
- `artifacts/derived/student_elo_profiles.parquet`
  - one-row-per-student summary for the sequential replay
- `artifacts/derived/student_elo_events_batch_replay.parquet`
  - per-attempt batch replay events
- `artifacts/derived/student_elo_profiles_batch_replay.parquet`
  - one-row-per-student summary for the batch replay

Depending on the current app branch or page state, not all of these may be used equally.

---

## 8. Important interpretation limits

Keep these limits in mind:

- Elo is retrospective here
- scores are usually module-local, not one universal scale across the whole curriculum
- the curves are useful for comparison and description
- they should not be read as a direct proof of causal learning gains

Good phrasing:
- "this student's replayed score rises steadily in this module"
- "this activity has higher estimated difficulty than others in the same scope"

Less safe phrasing:
- "the model proves the student mastered the concept"
- "a 50-point difference means an exact amount of learning"

---

## 9. If you want more detail

This file is the main Elo overview.

Older, more detailed Elo notes were moved to:

- `ressources/archive/elo/`

Use them only if you need the historical step-by-step explanation of the older writeups.

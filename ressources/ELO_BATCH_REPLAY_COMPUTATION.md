# How Batch Replay Elo Is Computed

This note explains the **Batch Replay Elo** used by the app.
It describes the logic implemented in:

- `src/visu2/derive_common.py`
- `src/visu2/derive_elo.py`
- `apps/page_modules/5_student_elo_evolution.py`

**Overview:**
1. We first reuse the fixed exercise difficulties calibrated for **Sequential Replay Elo**.
2. We keep the same module-local exercise-context definition.
3. We replay one student and one module at a time.
4. At each attempt, we compute the expected success from the student's current prefix estimate.
5. We then refit the student's level from the whole history seen so far in that module.
6. The process repeats at every attempt, so each point is a fresh batch estimate on the visible prefix.

## Short Summary

Batch Replay Elo is computed in **two layers**:

1. **Reuse Sequential Replay Elo fixed exercise difficulty**
   - module by module
   - same calibrated `exercise_elo` as Sequential Replay Elo
   - no new item calibration is done here

2. **Refit student ability on each module-local prefix**
   - uses all attempts seen so far in that module
   - student starts at `1500` at the beginning of each module
   - each new point is a new batch fit of student ability against fixed item difficulty

So the displayed curve is still **module-local**, but it is **prefix-batch**, not sequential.

## Core Idea

The goal is to answer a different question from Sequential Replay Elo.

Sequential Replay Elo asks:

- "After this attempt, how should the Elo move if we apply one standard Elo update?"

Batch Replay Elo asks:

- "Given everything the student has done so far in this module, what is the best current estimate of the student's level?"

So Batch Replay Elo is meant to be:

- smoother
- less sensitive to one single noisy attempt
- closer to a running latent-ability estimate

The neutral Elo value is:

- `1500`

The main constants are:

- `ELO_BASE_RATING = 1500`
- `ELO_SCALE = 400`

These are defined in `src/visu2/derive_common.py`.

## What Counts as an "Exercise"

Batch Replay Elo uses the same item definition as Sequential Replay Elo.

An item is not identified by `exercise_id` alone.

It is identified by the raw context:

- `module_code`
- `objective_id`
- `activity_id`
- `exercise_id`

This matters because the same exercise can appear in several pedagogical contexts.
Using the full context avoids mixing those cases together.

## Step 1: Reuse Sequential Replay Elo Fixed Exercise Difficulty

Batch Replay Elo does **not** calibrate a new difficulty table.

It reuses the fixed difficulty already produced by Sequential Replay Elo:

- `agg_exercise_elo.parquet`

So the item side stays fixed.
Only the student side is re-estimated over time.

This means:

- Sequential Replay Elo and Batch Replay Elo are comparable against the same item scale
- the difference comes from the student replay logic, not from a different exercise calibration

## Step 2: Split the Replay by Student and Module

Replay is done separately for each:

- `(user_id, module_code)`

So:

- a student is reset when entering a new module
- one student can have several independent Batch Replay Elo trajectories

This is the same module-local scope as Sequential Replay Elo.

## Step 3: Order Attempts Chronologically

Inside each student-module slice, attempts are sorted by:

- `created_at`
- with the existing module-local replay ordering used by the Elo pipeline

The replay then proceeds one visible attempt at a time:

- attempt 1
- attempt 2
- attempt 3
- etc.

## Step 4: Start the Module at 1500

At the beginning of each student-module trajectory:

- `student_elo_pre = 1500`

So for the very first attempt of a module:

- the student starts from the neutral prior
- the first update comes entirely from that first observed result

This keeps the replay causal:

- the first point does not use future information

## Step 5: Compute Expected Success Before the Current Attempt

For attempt `t`, the code first takes:

- the student's current prefix estimate before the attempt
- the fixed `exercise_elo` of the current exercise-context item

Then it computes the expected success with the usual Elo probability:

`P(success) = 1 / (1 + 10^((exercise_elo - student_elo_pre) / 400))`

This value is stored as:

- `expected_success`

Interpretation:

- if the student is already estimated above the item difficulty, success is expected
- if the item is harder than the student estimate, failure is more expected

## Step 6: Add the Current Attempt to the Prefix History

After computing `student_elo_pre` and `expected_success`, the current attempt is added to the prefix history.

The code stores that prefix history in an aggregated form:

- one entry per exercise-context item already seen in the module
- with:
  - number of attempts on that item so far
  - number of successes on that item so far

So the prefix is not kept as a long raw list only for fitting.
It is summarized as weighted item counts:

- `(exercise_elo, attempts_so_far, successes_so_far)`

This is more efficient and mathematically equivalent for the student fit used here.

## Step 7: Refit the Student on the Whole Prefix

This is the key Batch Replay step.

At attempt `t`, the code asks:

- "Given all attempts from `1..t` in this module, what student rating best explains them together?"

The item difficulties stay fixed.
Only the student rating is optimized.

The fit uses:

- the same Elo success model
- a penalty that keeps the rating from drifting too far from `1500`

So the objective is a penalized likelihood over the whole prefix:

- observed successes should have high predicted probability
- observed failures should have low predicted probability
- sparse evidence should stay closer to `1500`

This student fit is implemented in:

- `_fit_batch_student_rating_weighted(...)`

in `src/visu2/derive_elo.py`.

## Step 8: Use Weighted Prefix Counts Instead of Replaying Every Row in the Fit

Suppose the student has already seen the same exercise-context item several times.

Instead of refitting with a raw list like:

- success
- failure
- success

the code aggregates that to:

- `attempts = 3`
- `successes = 2`

and fits the student against:

- the fixed item difficulty
- the total number of trials on that item
- the total number of successes on that item

So the batch fit uses weighted evidence by item, not one separate optimization term per stored row.

## Step 9: Save Two Ratings for Each Attempt

For every attempt, the event table stores:

- `student_elo_pre`
- `student_elo_post`

where:

- `student_elo_pre` = batch estimate using attempts `1..t-1`
- `student_elo_post` = batch estimate using attempts `1..t`

So for the first attempt:

- `student_elo_pre = 1500`
- `student_elo_post` = fitted rating after only one attempt

For the second attempt:

- `student_elo_pre` already reflects attempt 1
- `student_elo_post` reflects attempts 1 and 2 together

And so on.

## Step 10: Repeat at Every Attempt

This process is repeated for every visible attempt in the module:

1. read the current prefix estimate
2. compute expected success for the current item
3. add the new attempt to the prefix history
4. refit the student on the whole prefix
5. save the updated rating

So the curve is a sequence of:

- prefix-based student estimates

not a sequence of:

- one-step Elo increments only

## Step 11: Build Per-Student-Per-Module Summaries

After the event table is built, the code aggregates:

- one profile row per `(user_id, module_code)`

This profile stores:

- total attempts in the module
- first and last attempt timestamps
- number of distinct objectives and activities seen
- final student Elo in that module

The profile artifact is:

- `student_elo_profiles_batch_replay.parquet`

The event artifact is:

- `student_elo_events_batch_replay.parquet`

## The Main Mathematical Difference from Sequential Replay Elo

Sequential Replay Elo replay is:

- sequential
- one update per attempt

Batch Replay Elo is:

- prefix-batch
- one fresh refit per attempt

That means:

- Sequential Replay Elo at attempt `t` is updated directly from attempt `t`
- Batch Replay Elo at attempt `t` is the best fit using attempts `1..t` together

So Batch Replay Elo is usually:

- smoother
- less jumpy
- more conservative after isolated surprising attempts

## Important Consequence

Batch Replay Elo is still chronological, because each point uses only the prefix seen so far.

But it is **not** a classic online Elo trajectory.

It is better understood as:

- a running batch estimate of student ability inside the selected module

## In One Sentence

Batch Replay Elo reuses the fixed module-local Sequential-Replay-Elo exercise difficulties and, at every attempt, recomputes the student's module-local rating from the whole history seen so far in that module, starting from `1500`.

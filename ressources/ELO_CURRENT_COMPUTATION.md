# How Sequential Replay Elo Is Computed

This note explains the **Sequential Replay Elo** used by the app.
It describes the logic implemented in:

- `src/visu2/derive_common.py`
- `src/visu2/derive_elo.py`
- `apps/page_modules/5_student_elo_evolution.py`

**Overview:**
1. We split the data by module and keep only first attempts for calibration.

2. For each exercise-context item, we compute an initial difficulty from the smoothed first-attempt success rate, then convert it into an Elo-like difficulty.

3. All students in the module start at 1500.

4. Keeping current item difficulties fixed, we refit each student ability using all of that student’s first-attempt successes and failures in the module.

5. Keeping those new student abilities fixed, we refit each item difficulty using all first-attempt successes and failures observed on that item.

6. We alternate steps 4 and 5, recentering the module after each round, until the ratings become stable.

7. We then keep the fitted exercise difficulties fixed.

8. Finally, we replay each student’s Elo inside each module, starting again at 1500, using all attempts and the fixed exercise difficulties.

## Short Summary

Sequential Replay Elo is computed in **two stages**:

1. **Calibrate exercise difficulty offline**
   - done **module by module**
   - uses **first attempts only**
   - estimates one fixed difficulty per exercise-context item

2. **Replay student Elo inside each module**
   - uses **all attempts**
   - student starts at **1500** at the beginning of each module
   - exercise difficulty stays fixed during replay

So the displayed student curve is **module-local**, not global across all modules.

## Core Idea

The system tries to model a simple intuition:

- strong students should succeed more often
- hard exercises should be failed more often
- success probability depends mainly on:
  - student ability
  - exercise difficulty

The neutral Elo value is:

- `1500`

The main constants are:

- `ELO_BASE_RATING = 1500`
- `ELO_SCALE = 400`
- `ELO_K = 24`

These are defined in `src/visu2/derive_common.py`.

## What Counts as an "Exercise"

For Sequential Replay Elo, an item is **not** identified by `exercise_id` alone.

It is identified by the raw context:

- `module_code`
- `objective_id`
- `activity_id`
- `exercise_id`

This matters because the same exercise can appear in several pedagogical contexts.  
Using the full context avoids mixing those cases together.

## Step 1: Split the Data by Module

Calibration is done **independently for each module**.

That means:

- module `M1` gets its own Elo calibration
- module `M41` gets its own Elo calibration
- scores are not fitted on one single global cross-module scale

This is why the Elo page now asks for:

1. a student
2. then one of that student's modules

## Step 2: Keep Only First Attempts for Calibration

To estimate exercise difficulty, the code keeps only rows where:

- `attempt_number == 1`

Why?

Because we want exercise difficulty to reflect the **first encounter** with the exercise, not learning after retries.

So if a student fails, learns, and later succeeds, only the first attempt is used when calibrating the difficulty of that exercise-context item.

## Step 3: Build an Initial Difficulty for Each Item

Within one module, the code computes for each item:

- how many first attempts it has
- how many of those were successful
- its raw success rate

Then it applies **smoothing**:

`smoothed_rate = (successes + 20 * module_average_rate) / (attempts + 20)`

Why smooth?

Because small samples are unstable.

Example:

- 1 success out of 1 attempt gives a raw success rate of `100%`
- that does not mean the item is certainly very easy

Smoothing pulls low-data items toward the module average.

Then the smoothed success rate is converted into an initial Elo-like difficulty:

`difficulty = 1500 - 400 * log10(p / (1 - p))`

where `p` is the smoothed success probability.

Interpretation:

- high success rate -> easier item -> lower difficulty
- low success rate -> harder item -> higher difficulty

## Step 4: Start All Students at 1500

For the calibration stage, every student in the module initially starts at:

- `1500`

At this point, we have:

- an initial difficulty for each item
- an initial ability for each student

These are only starting values.

## Step 5: Fit Students and Items Jointly

This is the most important part.

The code then alternates between two fitting steps.

### 5a. Fit Student Ability While Keeping Item Difficulty Fixed

For one student, the code looks at **all** their first-attempt results in the module.

For each attempt, it uses the standard Elo success probability:

`P(success) = 1 / (1 + 10^((exercise_rating - student_rating) / 400))`

Then it asks:

- what single student rating best explains all these observed successes and failures together?

This is done by minimizing a loss:

- successes are rewarded when predicted probability is high
- failures are rewarded when predicted probability is low
- ratings too far from `1500` get a small penalty

So for one student, the fit is based on the **whole set** of first attempts, not on a chronological one-by-one update.

### 5b. Fit Item Difficulty While Keeping Student Ability Fixed

Then the code switches perspective.

For one item, it looks at **all** students who attempted it on their first try.

It asks:

- what single item difficulty best explains who succeeded and who failed on it?

Again, it uses:

- the Elo probability formula
- a penalty that keeps values from drifting too far from `1500`

### 5c. Repeat Until Stable

The code alternates:

1. fit students using current item difficulties
2. fit items using current student abilities

and repeats this several times until the ratings stop moving much.

This is why it is called a **batch** fit:

- it uses the whole first-attempt dataset of the module together
- it does not define student ability by a permanent chronological update after each attempt

## Step 6: Recenter the Module Around 1500

There is a technical issue in this kind of model:

- if you add the same constant to all student ratings and all item ratings,
- predicted probabilities do not change

So the absolute level is not naturally fixed.

To stabilize the scale, after each outer iteration the code recenters the module so that:

- the **mean item difficulty** is close to `1500`

This does **not** change who is stronger or weaker.  
It only fixes the origin of the scale.

## Step 7: Save Fixed Exercise Difficulty

After calibration, each exercise-context item gets a fixed value:

- `exercise_elo`

This value is stored in:

- `agg_exercise_elo.parquet`

This artifact is then used as the fixed difficulty table for replay.

## Step 8: Replay Student Elo Inside Each Module

Once exercise difficulty is fixed, the code replays student Elo trajectories.

This replay is simpler than calibration:

- it uses **all attempts**
- it works **inside one module at a time**
- student rating resets to `1500` at the start of each `(student, module)` trajectory

So if the same student works in 3 modules:

- they have 3 separate Sequential Replay Elo trajectories

## Step 9: Update Elo Attempt by Attempt During Replay

During replay, attempts are processed in chronological order within each student-module slice.

For each attempt:

1. read the student's current Elo before the attempt
2. read the fixed exercise difficulty
3. compute expected success:

`expected = 1 / (1 + 10^((exercise_elo - student_elo_pre) / 400))`

4. convert the result to:
   - `1` if correct
   - `0` if incorrect

5. compute the Elo update:

`delta = 24 * (outcome - expected)`

6. update student Elo:

`student_elo_post = student_elo_pre + delta`

Interpretation:

- if the student does better than expected, Elo goes up
- if the student does worse than expected, Elo goes down
- a surprise success against a hard item gives a larger increase
- a failure on an easy item gives a larger decrease

## Step 10: Build the App Outputs

The replay produces:

- `student_elo_events.parquet`
  - one row per replayed attempt
  - includes:
    - module
    - attempt order within the module
    - exercise difficulty
    - expected success
    - Elo before
    - Elo after

- `student_elo_profiles.parquet`
  - one row per `(student, module)`
  - used to populate the page selectors and summary metrics

## What the Elo Page Shows

The active Student Elo page now works like this:

1. choose a student
2. choose one available module for that student
3. display the replayed Elo trajectory for that student in that module

So the page is showing:

- **Sequential Replay Elo only**
- **one module-local trajectory at a time**

## What This Means in Practice

### Good consequences

- the same reused `exercise_id` can be treated differently in different contexts
- module-specific learning paths are easier to interpret
- exercise difficulty is based on first exposure, which is usually more meaningful pedagogically

### Important limitation

Because calibration is module-local:

- a student's Elo in module `M1` and in module `M41` are not meant to be interpreted as one single continuous global skill scale

They are two module-specific scales, both centered around `1500`.

## One-Sentence Summary

Sequential Replay Elo is computed by:

- calibrating **fixed exercise difficulty per module from first attempts only**, then
- replaying **student Elo inside each module from 1500 using all attempts**.


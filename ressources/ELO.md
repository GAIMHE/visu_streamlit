# Elo in VISU2

Elo is used here as a retrospective modeling layer for:

- estimating relative exercise difficulty;
- summarizing average activity difficulty;
- replaying or estimating student level inside a module;
- producing descriptive signals for analysis.

---

## 1. Shared Idea

The pipeline separates two problems:

1. estimate fixed exercise difficulties;
2. estimate or replay student level against those fixed difficulties.

The important distinction is:

- exercises are treated as fixed objects: an exercise does not improve or
  regress over time;
- students are allowed to change over time, or to have an estimated level for a
  specific block such as an adaptive test.

All Elo variants use the same probability model:

```text
EloProb(student_elo, exercise_elo)
    = 1 / (1 + 10^((exercise_elo - student_elo) / 400))
```

Interpretation:

- if `student_elo` is much higher than `exercise_elo`, expected success is high;
- if `student_elo` is much lower than `exercise_elo`, expected success is low;
- if both are equal, expected success is 50%.

Useful constants:

```text
base Elo = 1500
scale = 400
lambda = ln(10) / 400
```

`lambda` is the derivative scaling factor of the base-10 Elo logistic function.

---

## 2. What Counts As An Item

An item is not only `exercise_id`.

The current exercise-difficulty calibration uses the full pedagogical context:

```text
(module_code, objective_id, activity_id, exercise_id)
```

This matters because the same exercise identifier can appear in different
pedagogical contexts. In that case, the contexts are treated as distinct item
instances for Elo calibration.

---

## 3. Step 1: Calibrate Fixed Exercise Difficulty

This stage is shared by the student replay methods and the adaptive-test
placement estimate.

Goal:

```text
estimate one fixed exercise_elo per exercise context
```

This is an iterative parameter-estimation procedure, similar in spirit to
fitting a simple Rasch / 1PL item-response model in Elo scale:

- each student has a temporary ability parameter within a module;
- each exercise context has a difficulty parameter within the same module;
- student ability and exercise difficulty are fitted together;
- the fitted parameters are chosen to make observed first-attempt outcomes
  likely under `EloProb(student_elo, exercise_elo)`.

This is not chronological replay. It uses first-attempt observations together to
produce one fixed difficulty estimate per exercise context.

### Why First Attempts Only?

The calibration uses first attempts only:

```text
attempt_number == 1
```

The reason is to reduce bias from retries and learning effects. If a student
fails an exercise and later succeeds after feedback or repeated practice, that
later attempt is informative about the student trajectory, but it is less clean
for estimating the fixed difficulty of the exercise.

### Initialization

For each module, each exercise context starts from a smoothed first-attempt
success rate:

```text
smoothed_rate =
    (successes + 20 * module_average_rate) / (attempts + 20)
```

This avoids extreme initial ratings for sparse exercises.

The smoothed rate is converted into an Elo-like difficulty:

```text
exercise_elo_init = 1500 - 400 * log10(p / (1 - p))
```

So:

- low success rate -> high difficulty Elo;
- high success rate -> low difficulty Elo.

### Iterative Fitting

For each module:

```text
keep only first attempts

initialize each exercise-context difficulty
    count first-attempt successes and failures
    compute smoothed_rate
    convert smoothed_rate into exercise_elo_init

initialize each student around 1500

repeat until stable:
    fit all student ratings using fixed item difficulties
    fit all item difficulties using fixed student ratings

    for one student:
        collect all first-attempt outcomes in the module
        choose theta that best explains them under:
            EloProb(theta, item_rating)

    for one item:
        collect all first-attempt outcomes on that item
        choose b that best explains them under:
            EloProb(student_rating, b)

    use:
        Gaussian prior centered at 1500
        Newton updates
        clipping to [600, 2400]

    recenter the module so mean(item_rating) = 1500
        because the model is invariant to shifting all ratings

    stop when item changes are small enough

save fixed exercise_elo
```

Outputs:

- `agg_exercise_elo.parquet`
- `agg_activity_elo.parquet`

Simple interpretation:

- higher `exercise_elo` = harder exercise context;
- higher `activity_mean_exercise_elo` = harder activity on average.

---

## 4. Step 2A: Sequential Replay Elo

Sequential Replay Elo updates the student score one attempt at a time.

Shared replay scope:

- replay is done separately for each `(student, module)`;
- the student starts at `1500` at the beginning of each module;
- attempts are processed in chronological order;
- item difficulty stays fixed during replay.

At attempt `t`:

```text
student_elo_pre = current student Elo
expected = EloProb(student_elo_pre, exercise_elo)
delta = K * (outcome - expected)
student_elo_post = student_elo_pre + delta
```

The implementation uses:

```text
K = 24
```

So it answers:

```text
How should the score move after this exact attempt?
```

Pseudocode:

```text
load fixed exercise_elo from calibration

for each (student, module):
    student_elo = 1500

    for each attempt in time order:
        expected = EloProb(student_elo, exercise_elo)
        delta = 24 * (outcome - expected)
        student_elo = student_elo + delta

        save:
            student_elo_pre
            expected_success
            student_elo_post
```

Good for:

- seeing short-term movement;
- understanding how each attempt affects the curve.

Limit:

- it is reactive to noisy single attempts.

Outputs:

- `student_elo_events.parquet`
- `student_elo_profiles.parquet`

---

## 5. Step 2B: Batch Replay Elo

Batch Replay Elo uses the same fixed `exercise_elo`, but it does not update the
student only from the current attempt.

At attempt `t`:

- take all attempts `1..t` seen so far in that module;
- keep item difficulties fixed;
- refit one student Elo from the full prefix history.

So it answers:

```text
Given everything the student has done so far in this module,
what is the best current estimate of the student's level?
```

In practice:

```text
student_elo_pre  = estimate from attempts 1..t-1
student_elo_post = estimate from attempts 1..t
```

Repeated work on the same exercise context is aggregated as:

```text
attempts_so_far
successes_so_far
```

Pseudocode:

```text
load fixed exercise_elo from calibration

for each (student, module):
    current_rating = 1500
    prefix_history = {}

    for each attempt in time order:
        student_elo_pre = current_rating
        expected = EloProb(student_elo_pre, exercise_elo_of_current_item)

        update prefix_history for the current exercise-context:
            attempts_so_far += 1
            successes_so_far += outcome

        refit the student on the whole prefix:
            for each seen item j:
                n_j = attempts_so_far on item j
                s_j = successes_so_far on item j
                p_j = EloProb(theta, exercise_elo_j)

            choose theta that best explains all prefix counts together
            using:
                Gaussian prior centered at 1500
                Newton updates
                fixed item difficulties

        student_elo_post = fitted theta
        current_rating = student_elo_post

        save:
            student_elo_pre
            expected_success
            student_elo_post
```

Good for:

- smoother trajectories;
- lower sensitivity to one noisy attempt;
- comparing with Sequential Replay Elo.

Limit:

- it is computationally heavier;
- it is still retrospective and descriptive.

Outputs:

- `student_elo_events_batch_replay.parquet`
- `student_elo_profiles_batch_replay.parquet`

---

## 6. Step 2C: Batch Elo On Adaptive Tests

It uses the same fixed exercise difficulties from Step 1, but it fits one
student Elo from the adaptive-test block as a whole.

This is the right approach for adaptive tests because we generally assume the
adaptive test is measuring entry level, not teaching the student over the test.
Therefore we do not want:

```text
attempt 1 -> update -> attempt 2 -> update -> ...
```

Instead we want:

```text
all adaptive-test outcomes for one student-module block
    -> one fitted student Elo
```

So it answers:

```text
Given the adaptive-test exercises shown to this student,
and the student's successes and failures on them,
what single Elo level best explains this adaptive-test block?
```

### Why Not Use Raw Adaptive-Test Success Rate?

The adaptive test is adaptive: students do not receive the same exercises.

Two students can have the same success rate but very different levels if one
student was shown harder exercises. Raw success rate ignores that difference.

Adaptive-test batch Elo uses:

- the difficulty of each exercise context shown;
- the student's outcome on each exercise;
- one batch fit across the whole adaptive-test block.

### Adaptive-Test Batch Elo Pseudocode

```text
load fixed exercise_elo from calibration

for each (student, module):
    collect adaptive-test attempts for this module

    optionally keep only adaptive-test attempts before first same-module practice

    aggregate by exercise context:
        n_j = number of adaptive-test attempts on item j
        s_j = number of successes on item j

    fit one theta:
        for each item j:
            p_j = EloProb(theta, exercise_elo_j)

        choose theta that best explains all adaptive-test counts together
        using:
            Gaussian prior centered at 1500
            Newton updates
            fixed item difficulties

    save theta as adaptive_test_elo
```

### Export Script

The corresponding export script is:

```bash
python scripts/export_adaptive_test_elo.py --source mia
```

Strict initial-placement version:

```bash
python scripts/export_adaptive_test_elo.py --source mia --only-before-practice
```

The strict version uses only adaptive-test attempts that occur before the first
`zpdes` or `playlist` practice attempt in the same module.

Output files:

- `artifacts/reports/mia_adaptive_test_elo_all.csv`
- `artifacts/reports/mia_adaptive_test_elo_before_practice.csv`

Main columns:

- `user_id`
- `module_id`
- `module_code`
- `module_label`
- `adaptive_test_attempts`
- `adaptive_test_elo_attempts`
- `adaptive_test_success_rate`
- `adaptive_test_elo`
- `has_adaptive_test_elo`
- `first_practice_at`
- `practice_attempts`
- `has_same_module_practice`
- `all_adaptive_test_attempts_before_first_practice`
- `any_adaptive_test_attempt_after_first_practice`

Recommended use:

- use `mia_adaptive_test_elo_before_practice.csv`;
- optionally filter to `has_same_module_practice = true`;
- optionally require a minimum number of adaptive-test Elo attempts, for example
  `adaptive_test_elo_attempts >= 5`.

---

## 7. Student Elo Page

The Student Elo page compares two replay curves for the same student:

- Sequential Replay Elo;
- Batch Replay Elo.

The comparison is mostly about replay logic. Both curves use the same fixed
exercise-difficulty calibration.

So if the curves differ, the difference comes mainly from how the student score
is updated over time, not from a different item calibration.

Adaptive-test Elo is not currently a replay curve on this page. It is better
understood as an initial placement estimate that can be joined to later student
trajectories.

---

## 8. Main Files

### Difficulty Side

- `artifacts/derived/agg_exercise_elo.parquet`
  - fixed exercise difficulty per exercise context
- `artifacts/derived/agg_activity_elo.parquet`
  - average activity difficulty

### Student Replay Side

- `artifacts/derived/student_elo_events.parquet`
  - per-attempt Sequential Replay Elo events
- `artifacts/derived/student_elo_profiles.parquet`
  - one-row-per-student summary for Sequential Replay Elo
- `artifacts/derived/student_elo_events_batch_replay.parquet`
  - per-attempt Batch Replay Elo events
- `artifacts/derived/student_elo_profiles_batch_replay.parquet`
  - one-row-per-student summary for Batch Replay Elo

### Analysis Exports

- `scripts/export_student_convergence_elo.py`
  - exports early-practice / convergence Elo at a fixed attempt threshold
- `scripts/export_adaptive_test_elo.py`
  - exports batch-fitted adaptive-test initial Elo

Depending on the current app branch or page state, not all files may be used
equally.

---

## 9. Interpretation Limits

Keep these limits in mind:

- Elo is retrospective here.
- Scores are usually module-local, not one universal scale across the whole
  curriculum.
- Exercise difficulty is estimated from observed platform data, not from an
  external gold standard.
- Student replay curves are useful for comparison and description.
- Adaptive-test Elo is a placement estimate, not proof of stable ability.
- None of these scores should be read as direct proof of causal learning gains.

Good phrasing:

- "this student's replayed score rises steadily in this module"
- "this activity has higher estimated difficulty than others in the same scope"
- "this student's adaptive-test Elo suggests a higher entry level in this module"

Less safe phrasing:

- "the model proves the student mastered the concept"
- "a 50-point difference means an exact amount of learning"
- "ZPDES caused the Elo gain"

---

## 10. If You Need More Detail

Older, more detailed Elo notes were moved to:

- `ressources/archive/elo/`

Use them only if you need the historical step-by-step explanation of older
writeups.

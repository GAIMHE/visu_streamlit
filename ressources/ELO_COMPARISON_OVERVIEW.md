# Sequential Replay Elo and Batch Replay Elo

## Shared Overview

Both methods use the same two-stage pipeline:

1. **Calibrate fixed exercise difficulty**
2. **Replay the student inside each module**

So the two methods do not differ on item calibration.
They differ only on how the student score is replayed once item difficulty has been fixed.

## Step 1: Calibrate Fixed Exercise Difficulty

This stage is shared by both methods.

- done module by module
- uses first attempts only, to avoid bias from learning effects across retries
- one item is one unique exercise context:
  - `(module_code, objective_id, activity_id, exercise_id)`
- initial item difficulty comes from a smoothed first-attempt success rate:
  - `smoothed_rate = (successes + 20 * module_average_rate) / (attempts + 20)`
- this smoothed rate is converted into an Elo-like starting difficulty:
  - `exercise_elo_init = 1500 - 400 * log10(p / (1 - p))`

Statistically, this calibration stage is a Rasch-style / 1PL logistic model in Elo scale:

- student ability and item difficulty are fitted together
- both use Gaussian priors centered at `1500`
  - `theta ~ N(1500, 250^2)`
  - `b ~ N(1500, 250^2)`
- optimization is done by Newton updates

Useful shared definitions:

- `EloProb(a, b) = 1 / (1 + 10^((b - a) / 400))`
- `lambda = ln(10) / 400`
  - this is the derivative scaling factor from the base-10 Elo logistic function

### Shared Calibration Pseudocode

```text
for each module:
    keep only first attempts

    initialize each exercise-context difficulty
        count first-attempt successes and failures
        compute smoothed_rate =
            (successes + 20 * module_average_rate) / (attempts + 20)
        convert smoothed_rate into exercise_elo_init

    initialize each student at 1500

    repeat until stable:
        fit all student ratings using fixed item difficulties
        fit all item difficulties using fixed student ratings

        more explicitly:
            for one student:
                collect all first-attempt outcomes in the module
                choose theta that best explains them under EloProb(theta, item_rating)

            for one item:
                collect all first-attempt outcomes on that item
                choose b that best explains them under EloProb(student_rating, b)

        both student and item updates use:
            - a Gaussian prior centered at 1500
            - Newton steps
            - clipping to [600, 2400]

        recenter the module so mean(item_rating) = 1500
            because the model is invariant to a constant shift of all ratings

        stop when item changes are small enough

save fixed exercise_elo
```


## Step 2: Replay the Student Inside Each Module

This is where the two methods differ.

Shared replay scope:

- replay is done separately for each `(student, module)`
- the student starts at `1500` at the beginning of each module
- all attempts are processed in chronological order
- item difficulty stays fixed during replay

### Sequential Replay Elo

Sequential Replay Elo updates the student one attempt at a time.

At attempt `t`:

- take the student score just before the attempt
- compute expected success against the fixed item difficulty
- apply one standard Elo update from this one outcome only

So it answers:

- "How should the score move after this exact attempt?"

### Sequential Replay Pseudocode

```text
for each (student, module):
    student_elo = 1500

    for each attempt in time order:
        expected = EloProb(student_elo, exercise_elo)
        K = 24
        delta = K * (outcome - expected)
        student_elo = student_elo + delta

        save:
            student_elo_pre
            expected
            student_elo_post
```

### Batch Replay Elo

Batch Replay Elo uses the same fixed `exercise_elo`, but it does not update the student from only the current attempt.

At attempt `t`:

- take all attempts `1..t` seen so far in that module
- keep item difficulties fixed
- refit the student level from the whole history seen so far

So it answers:

- "Given everything the student has done so far in this module, what is the best current estimate of the student's level?"

In practice:

- `student_elo_pre` = estimate from attempts `1..t-1`
- `student_elo_post` = estimate from attempts `1..t`

Repeated work on the same item is aggregated as:

- `attempts_so_far`
- `successes_so_far`

### Batch Replay Pseudocode

```text
load fixed exercise_elo from the shared calibration stage

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
                - the same Gaussian prior centered at 1500
                - Newton steps
                - fixed item difficulties

        student_elo_post = fitted theta
        current_rating = student_elo_post

        save:
            student_elo_pre
            expected
            student_elo_post
```

## Main Difference During Replay

The key difference is:

- **Sequential Replay Elo** updates from the current attempt only
- **Batch Replay Elo** refits from all attempts seen so far

So in practice:

- Sequential Replay Elo is more reactive to the latest attempt
- Batch Replay Elo is smoother but computationally heavier

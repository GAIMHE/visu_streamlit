# Learning progress in playlist and ZPDES sequences

Concerning the analysis comparing success progression within eligible activity sequences in playlist and ZPDES, I tried compare two **matched attempt-level models**. The first divides normalized sequence position into a first and later half. The second uses the exact continuous position of every first attempt.

Both models were fitted on exactly the same population and rows:

```text
34,224 students
468,013 eligible sequences
3,779,314 retained first attempts
```
Both models point in the same direction: success increases more strongly over ZPDES sequences than over playlist sequences.

### About weighting

Each attempt is one observation in the models, while the sequence tells the models which attempts belong to the same trajectory. For example, a sequence containing 20 exercises contributes 20 observations and a sequence containing 4 exercises contributes 4. The sequence random effects prevent attempts from the same sequence from being treated as unrelated, but they do not make the two sequences contribute equally.

The results therefore describe progression across the **attempts observed in eligible sequences**. They do not estimate the progression of an equally weighted “typical sequence.” That would require first calculating one change score or slope per sequence and then comparing those sequence-level values.

## 1. Matched binary-half model vs. continuous model

### Binary first-half vs. later-half model

```text
success ~ work_mode * later_half
          + classroom random intercept
          + student random intercept
          + sequence random intercept
          + sequence random half coefficient
```

Results:

```text
ZPDES-by-half interaction = 0.8166 log-odds
SE = 0.0058
95% CI = [0.8053, 0.8278]
p < .001
```

The interaction is the difference between the ZPDES and playlist first-to-later-half changes. In odds terms, `exp(0.8166) = 2.26`: the modeled multiplication of success odds between halves is approximately 2.26 times greater in ZPDES than in playlist.

The model-implied overall trajectories are easier to interpret as percentages::

| Work mode            | First half | Later half | Change            |
| -------------------- | ---------- | ---------- | ----------------- |
| Playlist             | 74.09%     | 76.41%     | +2.32 points      |
| ZPDES                | 56.01%     | 76.54%     | +20.53 points     |
| Difference in change |            |            | **+18.22 points** |

The main limitation of the binary representation is that it discards exact position. For example, the first exercise and the exercise at the midpoint receive the same value, while two exercises immediately on opposite sides of the threshold receive different values.

### Continuous progression model

The continuous analysis keeps every retained first attempt and represents its position within the sequence on a normalized scale:

```text
first exercise = 0
last exercise = 1
```

We then fit an attempt-level Bernoulli-logit mixed model:

```text
success ~ work_mode * normalized_position
          + classroom random intercept
          + student random intercept
          + sequence random intercept
          + sequence random position slope
```

The matched continuous-model result is:

```text
ZPDES-by-position interaction = 1.3162 log-odds
SE = 0.0089
95% CI = [1.2988, 1.3335]
p < .001
```

In odds terms, `exp(1.3162) = 3.73`: the modeled start-to-end multiplication of success odds is approximately 3.73 times greater in ZPDES than in playlist.

In percentages:

| Work mode            | Start  | End    | Change            |
| -------------------- | ------ | ------ | ----------------- |
| Playlist             | 75.28% | 79.84% | +4.56 points      |
| ZPDES                | 50.67% | 83.28% | +32.61 points     |
| Difference in change |        |        | **+28.05 points** |

## 2. Exercise Elo across work modes

The table provides context about exercise difficulty but the ELO is not included into the models analysis.

| Work mode | First-half Elo | Later-half Elo | Change |
| --------- | -------------- | -------------- | ------ |
| Playlist  | 1475.38        | 1481.38        | +6.00  |
| ZPDES     | 1521.96        | 1508.56        | −13.40 |

ZPDES exercises are slightly harder in the first half than in the later half. Playlist shows the opposite pattern, although the difference is small.

ZPDES exercises also have higher pooled Elo than playlist exercises in both halves:

```text
first-half ZPDES − playlist = +46.58 Elo
later-half ZPDES − playlist = +27.18 Elo
```

Therefore, ZPDES exercises do not begin easier than playlist exercises. The difference in difficulty between the two modes simply becomes smaller during the later half.

### Is Elo an adequate difficulty control?

The decline in pooled Elo within ZPDES is interesting: according to this calibration, students encounter slightly harder exercises at the beginning and easier exercises later.

Exercise Elo is estimated from observed student responses, and ZPDES adaptively selects exercises according to the learner’s estimated state. The assignment mechanism and the population of students encountering each exercise could therefore influence its estimated difficulty.

For example, an intrinsically difficult exercise may be presented mainly to stronger students in ZPDES. Its observed success rate—and potentially its estimated Elo—could then differ from when the same exercise is encountered in playlist.

To check whether exercise difficulty was independent of work mode, I recalibrated Elo separately using:

```text
playlist attempts only
ZPDES attempts only
```

I then compared the same exercise ELO across the two calibrations. An exercise was retained only if it had at least 25 first attempts in each mode, resulting in 7,978 shared exercise.

Results:

```text
Pearson correlation = .843
Spearman correlation = .830
median absolute difference = 73.4 Elo
mean signed difference ≈ 0
```

The results suggest that relative exercise difficulty is strongly, but not perfectly, stable across work modes. Exercises that are relatively difficult in playlist are generally also relatively difficult in ZPDES. However, for an individual exercise context, the two calibrated values typically differ by around 73 Elo points.

The near-zero mean that positive and negative context-level differences approximately cancel out, with no strong overall tendency for one mode to produce higher Elo values.
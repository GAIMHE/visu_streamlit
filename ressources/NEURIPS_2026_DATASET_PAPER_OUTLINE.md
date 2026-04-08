# NeurIPS 2026 Dataset Paper Outline

Working draft structured from the template discussed for a NeurIPS 2026 Datasets / Evaluations style submission.

Goal of this document:
- keep a paper-shaped skeleton early,
- fill what is already grounded in the current repo and dataset state,
- leave explicit TODO bullets for analysis, writing, and positioning work still needed.

---

## Title Block

`Formatting Instructions For NeurIPS 2026`

`Anonymous Author(s)`

`Affiliation`

`Address`

`email`

TODO:
- decide final paper title
- decide whether to pitch the paper as:
  - a KT benchmark dataset,
  - an educational modeling benchmark,
  - or a structured/intervention-aware learning dataset

Working title ideas:
- `Adaptiv'Math and MIA: A Classroom-Grounded Benchmark for Structured Learning Trajectories in Mathematics`
- `A Benchmark for Knowledge Tracing and Progression Modeling with Pedagogical Modes and Curriculum Structure`
- `Main and MIA: A Multi-Source Dataset for Classroom-Aware and Intervention-Aware Student Modeling`

---

## Abstract

Current draft direction:

- present the dataset family as interaction traces from a real adaptive math learning environment used in classrooms
- emphasize that the contribution is **not only** next-response KT
- highlight the extra structure compared with classic KT benchmarks:
  - work modes / pedagogical sequencing modes
  - curriculum hierarchy: module -> objective -> activity -> exercise
  - duration per interaction
  - classroom / teacher context
  - pedagogical dependency topology
  - aligned multi-source setup (`main` and `mia`)
- state that the benchmark supports both standard KT and richer educational modeling tasks

What can already be stated safely:
- `main` contains `6,264,394` interactions from `29,226` students
- `mia` contains `1,114,822` interactions from `11,500` students
- both are math datasets collected from real usage
- both have timestamps and duration
- both can be normalized into a common schema

TODO:
- decide the exact benchmark scope described in the abstract
- decide whether to mention one benchmark task only, or a task family
- write final abstract once positioning and benchmark tasks are frozen

---

## 1 Outline

### 1.1 Introduction

### Core story to aim for

The introduction should probably open with a gap like:

- existing public KT datasets are strong for sequence modeling,
- but they often flatten away the instructional context,
- while real educational products involve:
  - diagnostic phases,
  - adaptive progression,
  - teacher-defined or alternative sequencing modes,
  - structured curriculum hierarchies,
  - classroom-level deployment context.

### Draft contribution framing

Proposed framing:

- present a dataset family for mathematics learning trajectories in real classrooms
- include exercises, learning pathways, and evaluation benchmark(s)
- support both student-level and classroom-level analyses
- support both standard KT tasks and richer progression tasks

Potential contribution bullets:
- a large-scale real-world interaction dataset in mathematics
- explicit curriculum structure:
  - modules
  - objectives
  - activities
  - exercises
- pedagogical mode information:
  - `main`: `zpdes`, `initial-test`, `adaptive-test`, `playlist`
  - `mia`: `zpdes`, `adaptive-test`, `duo`, `revision`
- duration on every interaction
- classroom / teacher context
- pedagogical dependency graph / progression topology
- two aligned sources under one schema

### What can already be inserted

`main`:
- `6,264,394` interactions
- `29,226` students
- `9,009` exercises
- `543` activities
- `126` objectives
- `12` resolved modules in `fact_attempt_core`
- `4,788` classrooms
- `3,630` teachers in raw data
- time range: `2022-08-05` to `2025-11-20`

`mia`:
- `1,114,822` interactions
- `11,500` students
- `1,062` exercises
- `70` activities
- `10` objectives
- `1` module
- `1,575` classrooms
- `895` teachers in raw data
- time range: `2024-11-02` to `2026-04-02`

### TODO

- decide whether the introduction should foreground:
  - KT,
  - progression modeling,
  - or evaluation of educational AI systems in realistic contexts
- decide whether to position `main` and `mia` as:
  - one benchmark with two sources,
  - or one primary dataset with one transfer/generalization companion source
- decide which benchmark tasks are central enough to mention in the introduction

---

### 1.2 Related Work

### Anchor reference

The most important close reference at the moment is:

- `XES3G5M: A Knowledge Tracing Benchmark Dataset with Auxiliary Information`
  - NeurIPS 2023 Datasets and Benchmarks
  - useful both as:
    - a structural reference for the paper
    - a comparison point for dataset contribution

### Current comparison intuition

`XES3G5M` is strong on:
- question-side auxiliary information
- KC routes / KC structure
- question analysis
- standard KT benchmark packaging

Our dataset family is stronger on:
- pedagogical work modes
- classroom / teacher context
- curriculum hierarchy grounded in the product
- duration per interaction
- progression topology tied to instructional rules
- multi-source alignment

### Draft comparative claim

Do **not** position the paper as:
- “another math KT dataset with rich metadata”

Better position:
- a benchmark for structured, intervention-aware, classroom-grounded educational modeling

### Related work subsections to include

1. KT benchmark datasets
- ASSISTments
- Junyi
- KDD Cup algebra datasets
- EdNet
- Eedi / NeurIPS Education Challenge style datasets
- XES3G5M

2. AI for Education datasets with real learner traces
- focus on what is publicly available
- highlight what is usually missing:
  - real classroom context
  - pedagogical modes
  - explicit progression structure
  - durations

3. Product-grounded structured learning datasets
- if enough close references exist
- otherwise merge into the KT datasets section

4. ZPDES / adaptive progression context and MIA product context
- explain the instructional modes and product setting
- distinguish this from generic KC graphs

### TODO

- build a related-work comparison table
- verify exactly which datasets have:
  - timestamps
  - duration
  - classroom context
  - question content
  - curricular topology
  - multiple instructional modes
- write one paragraph explicitly explaining how our contribution differs from XES3G5M
- decide how much product-specific detail about ZPDES / MIA belongs in related work versus data description

---

### 1.3 Dataset Collection and Processing

### Dataset family overview

This section can describe the dataset family as two aligned sources:

- `main`: Adaptiv'Math main source
- `mia`: MIA source for a single module currently available in the repo

Possible framing:
- interaction data from a deployed digital math learning environment
- content authored within a structured pedagogical framework
- traces collected from real student usage in classrooms

### Current source files

For `main`, the core source files are:
- `data/adaptiv_math_history.parquet`
- `data/learning_catalog.json`
- `data/zpdes_rules.json`
- `data/exercises.json`

For `mia`, the current source materials include:
- `data_MIA/researcher_data-053df3ec-5501-4ad8-9917-a935bcf76740.csv`
- `data_MIA/config_mia.json`

### What the raw rows represent

`main` and `mia` raw interaction data:
- one row = one attempt on one exercise by one student at one time

Available interaction information includes:
- student id
- classroom id
- teacher id
- module/objective/activity/exercise ids
- correctness
- answer payload
- timestamp
- duration
- work mode

### Processing pipeline already available in the repo

We already have a normalized view through `fact_attempt_core.parquet`, which:
- standardizes the hierarchy
- resolves readable labels
- aligns both sources to a common schema

That makes it possible to describe both:
- the raw interaction logs
- the normalized benchmark-ready interaction table

### Data quality and known caveats

For `main`, we already know several irregularities exist:
- suspiciously high work-mode transitions for a minority of students
- some orphan playlist exercises
- some initial-test exercises with raw objective/activity ids not present in canonical metadata
- retry-heavy students
- classroom heterogeneity

For `mia`, the audit suggests the data is cleaner on several of those dimensions:
- no unmapped exercise hierarchy holes
- much lower transition extremes
- less extreme retry patterns

This could become a useful subsection or appendix:
- either as known limitations,
- or as a strength of releasing both the raw and normalized forms

### Human-readable content generation

Already possible to describe:
- `main` uses:
  - `learning_catalog.json`
  - `zpdes_rules.json`
  - `exercises.json`
- `mia` uses:
  - `config_mia.json`
  - generated `learning_catalog.json`
  - generated `zpdes_rules.json`

This is important because it shows that:
- the benchmark is not just anonymous ids,
- it preserves or reconstructs meaningful pedagogical labels and structures.

### Data statistics subsection

Likely statistics to include:
- number of students
- number of interactions
- number of classrooms
- number of teachers
- number of modules/objectives/activities/exercises
- work-mode distribution
- time span covered
- retry statistics
- interaction duration statistics
- sequence length statistics after benchmark preprocessing

### Splits

This needs to be specified depending on the benchmark tasks.

Possible split strategies:
- student-level train/dev/test
- classroom-level split
- source transfer:
  - train on `main`, evaluate on `mia`
- time-based split

### Visualization examples

Potential examples already available from the repo:
- work-mode transition examples
- ZPDES dependency graph examples
- classroom progression replay
- student Elo trajectory examples

These could be:
- paper figures
- appendix figures
- or supported by the public visualization tool/demo

### Bias / noise / ethics

This section should probably discuss:
- anonymization
- real-world logging noise
- unresolved metadata irregularities in `main`
- source imbalance
- work-mode imbalance
- classroom heterogeneity
- one-source-vs-two-source representativeness

### TODO

- confirm deployment geography if we want to mention number of countries
- confirm authorship / curation process of the exercises
- add anonymization details
- decide whether to include both raw and normalized schema diagrams
- add descriptive statistics tables
- define benchmark splits
- decide whether the paper should include screenshots of the visualization tool
- add a dataset limitations subsection

---

### 1.4 Evaluation Benchmark

### Main point

This section should define what the dataset is meant to evaluate.

Important recommendation:
- do **not** limit the benchmark to one classical KT task only
- if possible, define a small family of tasks that showcases the dataset’s distinctive value

### Candidate benchmark tasks

#### Task A. Standard next-response prediction

Description:
- predict whether the next interaction will be correct from the student’s prior history

Why include it:
- directly comparable to the KT literature
- necessary for comparison with XES3G5M and pyKT-style baselines

Potential baselines:
- DKT
- SAKT
- AKT
- simpleKT
- qDKT

#### Task B. Time-aware next-response prediction

Description:
- same as Task A, but explicitly using duration and/or temporal gaps

Why it is valuable:
- your data has duration per interaction
- XES3G5M explicitly lacks duration

#### Task C. Mode-aware prediction

Description:
- predict performance while conditioning on or transferring across work modes

Possible settings:
- train pooled, evaluate by mode
- train on `zpdes`, test on `adaptive-test`
- estimate whether mode context improves prediction

#### Task D. Topology-aware progression modeling

Description:
- use dependency graph / curriculum structure to predict progression or mastery transitions

Why it is distinctive:
- uses the explicit pedagogical graph rather than only KC adjacency

#### Task E. Cross-source generalization

Description:
- train on `main`, evaluate on `mia`, or the reverse

Why it is valuable:
- tests robustness under distribution shift
- leverages the aligned schema

#### Task F. Classroom-level forecasting / evaluation

Description:
- forecast class-level progression statistics or identify bottleneck structure

Why it is valuable:
- difficult to study in typical KT benchmark datasets

### Minimal benchmark if scope must stay narrow

If the paper needs a narrower scope, the minimum viable benchmark could be:

1. student-level next-response prediction
2. duration-aware ablation
3. cross-source evaluation

This would already differentiate the work from a plain KT dataset release.

### Metrics to consider

For student-level prediction:
- AUC
- accuracy
- log loss / Brier score
- calibration

For cross-source evaluation:
- in-domain vs out-of-domain performance gap

For classroom/topology tasks:
- task-specific metrics still need to be defined

### Benchmark preprocessing section

This section should explain:
- starting point:
  - raw attempt rows or `fact_attempt_core`
- how sequences are built
- how `question_id` and `skill_id` are defined
- how mode/context is used
- how train/dev/test splits are constructed

### TODO

- freeze the benchmark task list
- decide whether the benchmark uses:
  - raw data,
  - normalized data,
  - or both
- define train/dev/test split protocol
- pick baseline models
- pick evaluation metrics
- decide whether to include ablations:
  - without duration
  - without mode
  - without topology
  - without classroom context

---

## Appendix Planning

Potential appendix content:
- example exercises from `main`
- example exercises from `mia`
- schema tables
- dataset processing pipeline diagram
- benchmark preprocessing details
- additional dataset quality analyses
- comparison table against XES3G5M and other KT datasets
- examples of work-mode trajectories
- examples of topology graphs

TODO:
- decide which appendix items are essential for reproducibility
- decide what belongs in a supplementary zip/repo instead of the PDF

---

## Positioning Notes Against XES3G5M

This should become either:
- a subsection in the introduction,
- a paragraph in related work,
- or a summary box for internal drafting.

Current draft:

- XES3G5M is a strong benchmark for KT with rich question/KC auxiliary information.
- Our dataset family should be positioned less as “another KT benchmark” and more as a benchmark for **structured, intervention-aware, classroom-grounded educational modeling**.

Key differences to emphasize:
- work modes and instructional phases
- classroom / teacher context
- explicit curriculum hierarchy grounded in the product
- duration per interaction
- pedagogical topology
- aligned multi-source benchmark design

Avoid overclaiming:
- XES is stronger on question-side auxiliary info packaging and benchmark polish
- our contribution is stronger on educational context and structure

---

## Immediate Next Writing Tasks

1. Build a dataset comparison table:
- XES3G5M
- ASSISTments
- EdNet
- Junyi
- our `main`
- our `mia`

2. Draft a 1-paragraph positioning statement against XES3G5M.

3. Decide the benchmark scope:
- KT-only
- or KT + progression + transfer

4. Freeze the exact names of the sources in the paper:
- “Adaptiv'Math main”
- “MIA”
- or one family name plus subsets

5. Build the first statistics table directly from the repo.


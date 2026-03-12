# 2026-03-12 Student Elo Single Info Panel

## Context
The Student Elo Evolution page had separate in-page section headers and figure-level info boxes for the student summary cards and the replay chart. The page needed a simpler layout with one page-level info block under the title.

## Main changes made
- Added a single `student_elo_page` info entry in `apps/figure_info.py`.
- Removed the `Student Summary Cards` heading from `apps/pages/5_student_elo_evolution.py`.
- Removed the `Elo Replay Chart` heading from `apps/pages/5_student_elo_evolution.py`.
- Removed the two local figure info boxes and replaced them with one page-level info panel under the main title.

## Important decisions and rationale
- Kept the student summary cards themselves because they remain useful context for the chart.
- Kept the replay controls and chart behavior unchanged.
- Structured the new info block into `What it shows`, `Metrics`, and `How to use` to match the requested reading order.

## Follow-up actions
- If needed, align the deeper docs in `ressources/figures/` with the simplified page layout.

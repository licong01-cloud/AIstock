# Paper v2 Selection Center L3 regression - superseded failed attempt

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-30T01:21:03
- Git commit before run: 009859c
- Operator: lc999

## Scope

- Objective: rerun Paper v2 + Selection Center L3 after adding watchlist automation coverage.
- Result: superseded by `20260430_012735_l3_paper-v2-selection-center-l3-regression.md`.

## Failure

`paper_v2_data_quality` failed because the smoke resolver treated 2026-04-30 as a completed trading day at approximately 01:21 local time and demanded same-day daily datasets (`stk_limit`) plus 2026-04-29 daily feature datasets that were not yet expected to be available before the post-close data-ready window.

## Fix

`scripts/aistock_data_quality_smoke.py` now resolves the latest completed market date with a local post-close cutoff: before 18:00 Asia/Shanghai, today's trading day is not considered completed for daily dataset readiness.

## Rerun

The final rerun `20260430_012735_l3_paper-v2-selection-center-l3-regression.md` passed `l0`, `paper_v2_backend`, `paper_v2_data_quality`, and `paper_v2_ui`.

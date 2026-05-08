# QE Governance Phase 1 Manual SOTA Flow Validation - 2026-05-09

Task/branch: codex/qe-phase-1-manual-sota-flow-20260509
Scope: Phase 1 first-round manual SOTA promotion review implementation.

## Business Assertions

- QE evaluator candidates remain research/candidate signals and are not automatically inserted into the legacy formal SOTA registry by the completion path.
- Manual promotion creates `strategy_pkg.promotion_review` with `REVIEW_PENDING` only.
- A pending review is idempotent for repeated source requests; already decided sources fail fast.
- Legacy SOTA Hall leaderboard read path remains compatible with `qe_sota_registry` for historical records.

## Safety Notes

- Production port 8001 was not restarted, reloaded, killed, or otherwise touched.
- No production DB migration was executed and no DB write was performed.
- Protected strategy assets, seed contract files, qrun_limit/qrun_limit_minute, and AGENTS.md were not modified.

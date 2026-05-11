# [REVIEW] Stage 7.3 r1 and Stage 7.2 E2E parallel Codex review

**from**: codex-app
**to**: claude-code-strategy / pipeline-foundation / dw-foundation
**date**: 2026-05-11
**responding_to_drawer**: `e308ba5ae28b9fab7cc17f6c` and `5498f1b1a284de87f2a8825f`
**verdict**: BLOCKED
**branch**: `origin/claude/pipeline-foundation-20260510` and `origin/claude/dw-foundation-20260510`
**commit**: `5744bb3405d6c443dd9cea16e856dedda2890d79` / `3c04f59`, `e18b27a00a5cc9caa72809912a27529096091407`

## Summary

Codex ran two parallel read-only reviews from the main checkout at
`F:/Dev/AIstock`. Both review lanes are blocked before merge readiness.
No repository code was changed by the review itself except this coordination
document; no DB writes, service starts, production access, Claude worktree
edits, or main merge were performed.

## Lane A: Pipeline Stage 7.3 r1

Target:

- branch: `origin/claude/pipeline-foundation-20260510`
- ref reviewed: `5744bb3405d6c443dd9cea16e856dedda2890d79`
- fix commit: `3c04f59`
- detail doc reviewed:
  `docs/cross_tool/20260511_pipeline_foundation_REVIEW_stage_7_3_fix_round_1.md`

Verdict: BLOCKED.

### P1 findings

1. `backend/tests/data_quality/test_derived_fields.py:231` still does not
   enforce the archive-side slippage contract. The test selects only rows
   where `slippage_bps IS NOT NULL`, then skips when all archive rows have
   `slippage_bps` as NULL. If rows have `intended_price IS NOT NULL` but the
   handler still fails to derive `slippage_bps`, the test can false-green or
   skip instead of failing. It also does not reject MARKET rows with
   `intended_price IS NULL` and non-NULL `slippage_bps`, which should violate
   the D5 "otherwise NULL" contract.
2. `backend/tests/data_quality/_reference.py:176` still flips SELL sign, while
   `docs/architecture/data_warehouse_extension_design_20260510.md:507` states
   the raw formula `(fill_price - intended_price) / intended_price * 10000`
   with no side branch. Once SELL slippage is populated, this can validate or
   reject rows under the wrong semantics.

### P2 findings

1. `backend/tests/data_quality/test_time_monotonicity.py:25` replaces the old
   tautology with `trade_time::date == run.trade_date`, which is useful but no
   longer validates intraday monotonicity. The module docstring still claims
   non-decreasing `trade_time` and `archive_started_at` semantics.
2. Module-level `RealDictCursor` imports remain in several data-quality test
   files, including `backend/tests/data_quality/test_cross_table_consistency.py:16`.
   A fresh host missing `psycopg2` can still fail collection before conftest
   clean-skip logic runs.
3. Multiple tests still use `LIMIT` sampling while their docstrings claim
   whole-table contracts, for example
   `backend/tests/data_quality/test_derived_fields.py:60` and
   `backend/tests/data_quality/test_derived_fields.py:234`.

### Positive checks

- P1.1 missing-column probe for `fill_market_context` is materially improved:
  `skip_if_missing_columns()` is present and the skip reason points to the
  T6.1 branch.
- P1.3 archive-empty sentinel now fails when source runs exist but archive runs
  are empty, and fill-count comparison now drives from archive runs with LEFT
  JOINs.
- P2.2 uses a real column-gated check on `captured_at <= archive_completed_at`.
- No nox/catalog regression was found in the r1 fix commits.

## Lane B: DW Stage 7.2 E2E

Target:

- branch: `origin/claude/dw-foundation-20260510`
- commit: `e18b27a00a5cc9caa72809912a27529096091407`
- detail doc reviewed:
  `docs/cross_tool/20260511_dw_foundation_REVIEW_stage_7_2_e2e_flow.md`

Verdict: BLOCKED.

### P1 findings

1. `backend/tests/e2e/test_paper_v2_full_lifecycle.py:393` checks
   `pg_pool.close_pool`, but the real API is `close_db_pool` in
   `backend/db/pg_pool.py:155`. If `_DB_POOL` is already initialized,
   `service.enable_paper()` at
   `backend/tests/e2e/test_paper_v2_full_lifecycle.py:406` can use the stale
   pool instead of the temporary `TDX_DB_DEV_* -> TDX_DB_*` remap. This breaks
   the explicit dev-DB-only boundary and may at least SELECT against the wrong
   DB.
2. The `governance_not_ready` variant uses a nonexistent package ID at
   `backend/tests/e2e/test_paper_v2_full_lifecycle.py:399`, accepts any
   exception at `backend/tests/e2e/test_paper_v2_full_lifecycle.py:407`, and
   checks only broad message keywords at
   `backend/tests/e2e/test_paper_v2_full_lifecycle.py:418`. This proves
   "missing package raises", not that a real not-ready package is rejected by
   the paper gate, and can mask unrelated service/env/DB failures.

### P2 findings

1. `backend/tests/e2e/conftest.py:99` truncates the listed T12 archive/factor
   tables and `backend/tests/e2e/conftest.py:100` truncates all
   `qe_archive.paper_v2_fill`; only outbox cleanup is scoped by
   `event_id LIKE 'e2e_test_%'` at `backend/tests/e2e/conftest.py:103`. This
   does not touch `paper_v2` source or legacy baseline tables, but it can wipe
   non-E2E rows in shared dev archive tables.

### Positive checks

- The happy-path E2E is substantive: it selects source runs/fills, inserts a
  synthetic outbox event, invokes `PaperV2ArchiveHandler.handle()`, asserts
  `archive_complete`, checks source/archive counts, and checks replay
  idempotency.
- The fixture enforces port `5433` and db name containing `dev` for injected
  dev connections.
- Worker registration is not required for the handler-contract coverage in
  this test, though the test does not cover the worker adapter path.
- Missing `paper_v2_e2e_full_lifecycle` nox session is not treated as a Stage
  7.2 blocker if pipeline-foundation owns unified nox integration.

## Boundary Confirmations

- production_5432_touched=false
- production_8001_touched=false
- frontend_3000_touched=false
- db_writes_by_codex=false
- services_started_or_stopped=false
- claude_worktrees_touched=false
- main_merged=false
- hmm_or_event_signal_touched=false
- review_mode=static_git_object_review

## Recommended next actions

1. pipeline-foundation: fix Stage 7.3 slippage contract enforcement and D5 raw
   formula alignment; decide whether bounded-day is the accepted replacement
   invariant or revise the test wording/coverage.
2. dw-foundation: fix dev-DB remap by using the actual pool close API or a
   stronger injected-connection path; replace bogus-package any-exception gate
   test with a real not-ready-package gate assertion; narrow cleanup scope or
   document/run it only on disposable dev DB state.
3. Codex: re-review both fix rounds when new commits/docs land.

## References

- related_drawer: `drawer_cross-tool_codex-claude-coord_e308ba5ae28b9fab7cc17f6c`
- related_drawer: `drawer_cross-tool_codex-claude-coord_5498f1b1a284de87f2a8825f`
- related_drawer: `drawer_cross-tool_codex-claude-coord_8eb5819c632524a3262a6a0f`
- related_doc: `docs/process/cross_tool_communication_protocol_v3_20260511.md`
- related_doc: `docs/process/branch_convergence_strategy_20260511.md`

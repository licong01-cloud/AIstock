# [BASELINE] Stage 6 RERUN on main@4a3fa60 (post-R4 + hotfix)

> **from**: paper-v2 team Lead (cross-test teammate)
> **date**: 2026-05-11
> **target**: origin/main @ `4a3fa60` "docs(handoff): update Codex takeover doc with 7bf840d delivery + R6 prep progress"
> **prior baseline**: `a31365f` against `3973e7d` (YELLOW: 5G/2F/5SKIP/1NOX-SKIP)
> **reviewer branch**: claude/paper-v2-vnpy-mvp-20260508 (HEAD ee2e56f, unchanged)
> **purpose**: R5 merge readiness; replace YELLOW baseline; verify hotfix Fix A + Fix B + R4 (T12/T14a/b/c)

## §0 Verdict

**GREEN** — all 10 targeted backend/data-quality sessions PASS; both hotfixes verified; R4 content (T12 + T14a/b/c handlers) verified GREEN; UI sessions skipped per service-start policy; model_registry_backend NOX-internal SKIP (pending separate merge, not a paper-v2 R5 blocker).

R5 merge readiness: **GO**

Delta vs prior baseline (a31365f):
- 2 sessions flipped GREEN (`validation_module_registry_l0`: FAIL→GREEN; `data_quality_deep`: FAIL→GREEN)
- 1 new session covered for R4 content (`qe_archive_backend` now includes T14a/b/c handler contract tests — 70 passed)
- 0 remaining FAILED sessions

Hotfix verification:
- Fix A (validation_module_registry_l0 rl_execution duplicate): **verified ✓** — 8 module-ownership tests pass, module-ownership scan 12/12 mapped, no "Duplicate module_id: rl_execution" error
- Fix B (data_quality_deep archive-empty skip per D5 Q2.c): **verified ✓** — 10 passed, 21 skipped; archive-empty test correctly enters SKIP path (not FAIL) per D5 Q2.c policy

R4 verification:
- T12 22 qe_archive paper_v2_* tables: **verified ✓** — qe_archive_data_quality smoke shows `existing_table_count=27` (T12 22 paper_v2_* + 5 others), `missing_tables=[]`, schema_version=`qe_archive_v1_20260502`
- T14a/b/c archive handlers: **verified ✓** — qe_archive_backend collects/passes `backend/tests/qe_archive/test_handler_contract.py` against `backend/services/qe_archive/handlers/{contract,paper_v2_archive_handler,factor_value_archive_handler}.py` (70 passed total)

## §1 Session run matrix

| # | Session | Status | Pytest / Smoke summary | Duration | Notes |
|---|---|---|---|---|---|
| 1 | `l0` | GREEN | guardrail_scan: 28 MEDIUM (baseline), 6 baseline findings, 0 blocking | ~10s | sanity OK |
| 2 | `paper_v2_backend` | GREEN | 154 passed in 11.14s | 14s | core paper-v2 |
| 3 | `paper_v2_data_quality` | GREEN | 9 PASS + 1 WARN (legacy ledger order_fill_quantity_mismatches=3 — pre-existing, baseline) | ~25s | DB read-only |
| 4 | `paper_v2_l3` | SKIPPED (policy) | not run | — | UI; needs 8012/3012 |
| 5 | `validation_module_registry_l0` | GREEN | 8 passed in 1.26s; scan 12 mapped / 0 unmapped / 0 ambiguous | 2s | **Fix A ✓** |
| 6 | `qe_archive_ui` | SKIPPED (policy) | not run | — | UI |
| 7 | `strategy_package_governance_ui` | SKIPPED (policy) | not run | — | UI |
| 8 | `market_regime_ui` | SKIPPED (policy) | not run | — | UI |
| 9 | `rl_execution_ui` | SKIPPED (policy) | not run | — | UI |
| 10 | `data_quality_deep` | GREEN | 10 passed, 21 skipped in 1.47s | 2s | **Fix B ✓** |
| 11 | `dr_validate` | GREEN | 9 passed, 2 skipped in 1.34s | 2s | R1 sanity |
| 12 | `qe_archive_backend` | GREEN | 70 passed in 9.46s (schema + repo + completion contract + handler contract) | 13s | **R4 ✓** |
| 13 | `model_registry_backend` | NOX-INTERNAL SKIP | "Model Registry module not yet merged to main. Skipped pending origin/codex/qe-governance-integration-20260509 merge." | <1s | gated by `nox.session.skip()` — NOT a paper-v2 R5 blocker |
| 14 | `qe_archive_data_quality` | GREEN | 27/27 tables, 0 missing, 458 columns commented, 0 failures, 1 informational warning (450 pending outbox) | ~3s | D5 Q2.c — runs as smoke (no archive-emptiness assertion) |
| 15 | `guardrail_changed_files` | GREEN | files=0 (no staged changes); 0 findings; module-ownership 0/0 mapped | <1s | — |
| 16 | `market_regime_label` | GREEN | 19 passed in 0.38s | 2s | — |
| 17 | `rl_execution_smoke` | GREEN | 3 passed in 10.78s | 15s | module visibility OK; verifies rl_execution importable post-Fix A |

## §2 Skipped sessions (service-start policy)

Per `feedback_no_service_start` and Lead pre-verified port state (8012/3012 NOT listening):

| Session | Reason |
|---|---|
| `paper_v2_l3` | UI session, needs dev backend 8012 + frontend 3012 |
| `paper_v2_ui` | UI session (not in this batch but same class) |
| `qe_archive_ui` | UI session |
| `strategy_package_governance_ui` | UI session |
| `market_regime_ui` | UI session |
| `rl_execution_ui` | UI session |

## §3 Failed sessions

**None.** No FAILED sessions in this run.

Closest-to-failure observations:
- `paper_v2_data_quality` emits 1 WARN: `paper_v2_ledger_consistency: order_fill_quantity_mismatches=3` — this is a pre-existing legacy ledger inconsistency surfaced as WARN (not FAIL), present in prior baseline a31365f as well. The smoke script intentionally requires `--strict-history` or `--portfolio-name-prefix` to escalate to FAIL.
- `paper_v2_data_quality` calendar shows `latest_trading_day=2026-05-08` against today=2026-05-10 (smoke baseline date). Audit rows up-to-date.
- `qe_archive_data_quality` reports 450 pending outbox events — informational only, not a smoke failure.

## §4 Aggregate stats

- Sessions targeted (run set): **17** (13 from matrix + 4 plan-key sessions)
- Sessions actually executed: **11**
- GREEN: **11**
- FAILED: **0**
- SKIPPED (service policy, not executed): **5** UI sessions
- NOX-INTERNAL SKIP (gated by `nox.session.skip()`): **1** (`model_registry_backend`)
- MISSING (not in noxfile): **0**
- Total wall time across executed sessions: ~75 seconds nox-reported (excludes Python startup overhead in shell)

## §5 Delta vs prior baseline a31365f

| Session | a31365f result | 4a3fa60 result | Delta |
|---|---|---|---|
| `l0` | GREEN | GREEN | unchanged |
| `paper_v2_backend` | GREEN | GREEN | unchanged |
| `paper_v2_data_quality` | GREEN (with same WARN) | GREEN (with same WARN) | unchanged |
| `validation_module_registry_l0` | FAILED (rl_execution duplicate) | GREEN | **flipped ✓ Fix A** |
| `data_quality_deep` | FAILED (archive-empty assert) | GREEN (archive-empty SKIP) | **flipped ✓ Fix B** |
| `dr_validate` | GREEN | GREEN | unchanged |
| `qe_archive_backend` | GREEN (pre-handler) | GREEN (now +handler contract tests) | enhanced coverage |
| `model_registry_backend` | NOX-INTERNAL SKIP | NOX-INTERNAL SKIP | unchanged (still pending merge) |
| `qe_archive_data_quality` | (not in prior batch / equivalent) | GREEN | new coverage |
| `market_regime_label` | (n/a) | GREEN | new |
| `rl_execution_smoke` | (n/a) | GREEN | new |
| `guardrail_changed_files` | (n/a) | GREEN | new |
| UI sessions (×5) | SKIPPED (policy) | SKIPPED (policy) | unchanged |

Net: **2 FAIL→GREEN flips, 0 regressions, 4 new GREEN sessions added.**

## §6 R5 readiness assessment

- All backend sessions GREEN? **YES** (10/10 executed backend/data-quality sessions GREEN)
- Both hotfixes verified? **YES** (Fix A + Fix B both ✓)
- R4 content covered? **YES** (T12 22 tables present + T14a/b/c handler contract tests pass)
- Any new regression introduced by hotfix or R4? **NO**
- UI sessions skipped legitimately per service-start policy? **YES** (Lead pre-verified 8012/3012 not listening)
- `model_registry_backend` NOX-internal SKIP: not a paper-v2 R5 blocker — gated awaiting independent merge of `origin/codex/qe-governance-integration-20260509`

Recommendation: **GO for R5 merge.** This rerun supersedes prior YELLOW baseline (a31365f). Both hotfixes verified, R4 archive infrastructure (schema T12 + handler contracts T14a/b/c) green, no regressions detected.

## §7 Boundary confirmations

- `main_HEAD_verified`: `4a3fa60` (`docs(handoff): update Codex takeover doc with 7bf840d delivery + R6 prep progress`)
- `prod_db_writes`: false (only nox-managed dev DB 5433 used by data-quality smokes)
- `prod_8001_touched`: false (no read; no write)
- `no_service_started`: true (all UI sessions skipped; backend nox sessions run pytest in-process only)
- `code_modified`: false (verification only, no edits anywhere)
- paper-v2 branch HEAD unchanged: `ee2e56f` (only baseline doc added in this commit)
- `frontend/tsconfig.tsbuildinfo` NOT staged (confirmed via explicit-stage of baseline doc only)

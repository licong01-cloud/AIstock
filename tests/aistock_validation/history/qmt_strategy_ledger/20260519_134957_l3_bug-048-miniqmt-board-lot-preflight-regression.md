# BUG-048 MiniQMT board-lot preflight regression

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-19T13:49:57+08:00
- Git base: ea1efcd origin/main after BUG-060 merge
- Fix commit: dff0002
- Branch: bug/BUG-048-miniqmt-board-lot
- Worktree: F:\Dev\AIstock_worktrees\bug-048-miniqmt-board-lot
- Operator: codex-app
- Linked bug: BUG-048 / GitHub #46

## Scope

- Changed files: backend/services/qmt_strategy_ledger/order_service.py; backend/tests/qmt_strategy_ledger/test_order_service_preflight.py; backend/tests/qmt_strategy_ledger/test_selection_order_builder.py; tests/aistock_validation/bugs/20260519_BUG-048-miniqmt-managed-order-preflight-incorrectly-rejects-legal-st.json; this validation record.
- Impacted flows: MiniQMT managed-order BUY preflight; StrategyPackage selection-to-managed-order builder board-lot sizing; qmt_strategy_ledger regression tests.
- Business goal: legal STAR-market BUY quantities such as 688379.SH 2706 shares pass AIstock preflight and reach MiniQMT broker validation, while illegal board-lot quantities remain blocked with actionable context.
- Out of scope: broker connectivity, live/SIM order placement, sellability/T+1, cash settlement, SELL fill settlement, rebalance sell generation, batch atomicity, raw QMT endpoint restrictions.
- Protected assets reviewed: no StrategyPackage manifest/model/factor artifact, HMM snapshot, production DB data, or production port 8001/3000 touched.

## Environment

- Backend port: not started; no production 8001 access used.
- Frontend port: not started.
- TDX port: not used.
- Conda/env: AIstock, Python via `conda run -n AIstock`.
- Database: not used by these unit tests.
- Browser/headless: not used; backend-only bug.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| STAR legal quantity | 688/689 BUY quantity >=200 with 1-share increment is accepted by managed preflight | `test_preview_accepts_star_market_buy_quantity_after_minimum` covers 201 and 2706 | PASS |
| STAR below minimum | 688 BUY 199 is rejected as `BUY_BOARD_LOT` with min/increment/canonical context | `test_preview_rejects_star_market_buy_quantity_below_minimum` | PASS |
| Main/ChiNext board lot | 600/000/300 BUY 101 is rejected while canonical helper reports 100-share increment | `test_preview_rejects_main_board_and_chinext_non_100_share_buys` | PASS |
| Builder/service consistency | SelectionOrderBuilder preserves 688379.SH target-weight quantity 2706, and QmtManagedOrderService accepts the same canonical quantity | `test_selection_order_builder_preserves_star_market_increment_after_minimum`; targeted preflight tests | PASS |
| qmt_strategy_ledger regression | Full module unit suite remains green | `conda run -n AIstock python -m pytest backend/tests/qmt_strategy_ledger -q` -> 39 passed | PASS |
| L0/static gate | Repository static guardrail gate passes; no blocking new guardrail finding | `conda run -n AIstock python -m nox -s l0` -> successful | PASS |
| CI static companion | Module registry L0 passes, matching GitHub CI static-gate second session | `conda run -n AIstock python -m nox -s validation_module_registry_l0` -> successful, 8 passed | PASS |
| Compile check | Touched runtime modules compile | `conda run -n AIstock python -m compileall ...` -> exit 0 | PASS |
| Diff hygiene | No whitespace errors | `git diff --check` -> exit 0 | PASS |

## Commands

```bash
conda run -n AIstock python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py -q
conda run -n AIstock python -m pytest backend/tests/qmt_strategy_ledger -q
conda run -n AIstock python -m nox -s l0
conda run -n AIstock python -m nox -s validation_module_registry_l0
conda run -n AIstock python -m compileall backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/execution_algos/board_lot.py
git diff --check
```

## Evidence

- Targeted backend tests: 14 passed in 0.37s.
- Full qmt_strategy_ledger tests: 39 passed in 20.17s.
- L0 guardrails: successful; existing baseline/medium raw JSON findings were reported by the guardrail scanner, with `blocking=0`.
- Module registry L0: 8 passed in 0.70s; ownership scan mapped 12/12 files.
- Guardrail artifacts: tmp/validation/guardrails/l0_paths.json; tmp/validation/guardrails/l0_paths.md; tmp/validation/module_ownership/l0_paths.json; tmp/validation/module_ownership/l0_paths.md.
- API calls: none; this is a pure service/unit regression.
- DB checks: none; no database mutation involved.
- Logs: command output in Codex session; no backend service logs generated.
- Business output summary: managed preflight now reuses `backend.execution_algos.board_lot` instead of hard-coded 100-share BUY multiple.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| STAR-market legal quantities were rejected as `BUY_BOARD_LOT` | `QmtManagedOrderService.preview_order()` duplicated a simplified `quantity % 100` BUY rule | Replaced the hard-coded check with canonical `board_lot_rule()` / `round_to_board_lot()` validation and actionable error context | Targeted tests 14 passed; full qmt_strategy_ledger tests 39 passed |

## Result

- Final status: PASS for BUG-048 code-level and local pipeline validation.
- Remaining risks: GitHub PR CI still needs to run after branch push/PR; no live MiniQMT order was placed as part of this fix.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need dev service restart: not required for tests; required only if manually testing the HTTP endpoint in a running backend.
- Production impact: no production `8001/3000`, broker order placement, or DB writes used during validation.

## Main Sync Rerun - 2026-05-19 18:52 CST

- Reason: PR #62 previously failed `validation_center_backend` because BUG-060 was not yet merged into `main`.
- Action: rebased the BUG-048 branch onto `origin/main` at `ea1efcd` and dropped the duplicate BUG-060 reporting commit from this PR scope.
- Backup: preserved the old remote branch head as local `backup/bug-048-before-main-sync-20260519-185244`.
- Rerun result:
  - `conda run -n AIstock python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py -q` -> 14 passed.
  - `conda run -n AIstock python -m pytest backend/tests/qmt_strategy_ledger -q` -> 39 passed.
  - `conda run -n AIstock python -m compileall backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/execution_algos/board_lot.py` -> passed.
  - `git diff --check` -> passed.
  - `C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0` -> 8 passed.
  - `C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files` -> passed with no staged-file findings at rerun time.
  - `C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0` -> passed; existing baseline/raw-JSON warnings remain non-blocking.
  - `C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_center_backend` -> 101 passed, coverage line 80.19%, branch 60.52%.


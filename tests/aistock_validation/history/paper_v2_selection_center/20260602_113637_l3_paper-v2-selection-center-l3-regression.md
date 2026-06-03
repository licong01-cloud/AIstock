# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-02T11:36:37
- Git commit: ef356a86
- Operator: lc999

## Scope

- Changed files: backend/infra/qmt_client.py; backend/services/paper_trading_v2/broker/minqmtsim.py; backend/services/paper_trading_v2/live_session.py; backend/services/paper_trading_v2/scheduler.py; backend/tests/paper_trading_v2/test_live_session.py; backend/tests/paper_trading_v2/test_minqmtsim_backend.py; backend/tests/paper_trading_v2/test_session.py; tests/aistock_validation/bugs/20260602_BUG-198-paper-v2-miniqmt-order-submit-timeout-blocks-unattended-rebalance.json; tests/aistock_validation/history/paper_trading_v2/20260602_110041_l0_bug-198-miniqmt-submit-timeout-scheduler-recovery.md.
- Impacted flows: Paper v2 backend regression, Selection Center/StrategyPackage integration tests, Paper v2 data-quality smoke, validation module registry, validation center backend gate.
- Business goal: BUG-198 MiniQMT submit-timeout/scheduler fix must not regress Paper v2 backend, Selection Center traceability, StrategyPackage policy readiness, or validation center workflow gates.
- Out of scope: UI E2E on test port 8012; production backend restart; production DB writes; live MiniQMT client-login remediation or live order submission.
- Protected assets reviewed: no StrategyPackage frozen manifest, QE artifact, HMM snapshot, model weight, paper ledger, or production runtime asset was modified by this change.

## Environment

- Backend port: production/runtime 8001 was not restarted. Test port 8012 was not used and must not be treated as production evidence.
- Frontend port: not started or touched.
- TDX port: not restarted.
- Conda/env: repository default Python/nox/pytest environment.
- Database: read-only smoke checks only; no DB/DDL write by Codex.
- Browser/headless: UI E2E skipped intentionally via `PAPER_V2_L3_SKIP_UI=1` because this BUG changes backend timeout/retry/scheduler code and user controls service restarts.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking P0/P1 guardrail for this scope | `python -m nox -s l0` as child of `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` -> success, guardrail blocking=0 | PASS |
| Backend tests | Paper v2 + Selection Center + StrategyPackage backend regression passes | `python -m nox -s paper_v2_backend` -> 566 passed, 1 skipped, 2 xfailed | PASS |
| Validation module registry | Module ownership/catalog gate remains valid | `python -m nox -s validation_module_registry_l0` -> 8 passed and ownership scan mapped=12/unmapped=0/ambiguous=0 | PASS |
| Validation center backend | Issue workflow and validation center backend gates remain healthy | `python -m nox -s validation_center_backend` -> 320 passed; coverage line=79.91 branch=62.07 status=passed | PASS |
| Data quality smoke | Paper v2/Selection Center data-quality gates remain usable | `paper_v2_data_quality` child session -> required tables, audit rows, traceability checks PASS; legacy ledger mismatch WARN only | PASS |
| Deep data-quality tests | Data-quality unit suite passes | `data_quality_deep` child session -> 10 passed, 21 skipped | PASS |
| UI E2E | No UI regression claim is made in this backend-only BUG | `PAPER_V2_L3_SKIP_UI=1` explicitly skips UI; no 8012 startup/restart attempted | DEFERRED |
| Asset safety | No protected asset modified silently | Git diff limited to backend timeout/retry/scheduler/test/evidence files | PASS |

## Commands

```powershell
python -m nox -s validation_module_registry_l0
python -m nox -s paper_v2_backend
python -m nox -s validation_center_backend
$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
```

## Evidence

- API calls: Production 8001 read-only checks are recorded in `tests/aistock_validation/history/paper_trading_v2/20260602_110041_l0_bug-198-miniqmt-submit-timeout-scheduler-recovery.md`; no 8012 production check was used.
- DB checks: `paper_v2_data_quality` read-only smoke passed required schema/audit/traceability checks; it reported a legacy ledger mismatch warning only, not a blocker for BUG-198.
- Log files: command stdout captured in Codex session; generated coverage artifacts under `tmp/validation/coverage/` and data-quality smoke output under `tmp/paper_v2_data_quality_smoke.json`.
- Playwright report/trace: not generated; UI E2E skipped by explicit environment gate.
- Screenshots: not applicable.
- Business output summary: MiniQMT timeout/scheduler backend fix remains compatible with Paper v2 backend, Selection Center traceability, StrategyPackage readiness, validation center backend, and data-quality gates. Live MiniQMT L5 remains deferred because the MiniQMT client login issue is external to AIstock.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Full UI L3 would require test backend 8012 | 8012 is test-only and user controls service restarts | Used `PAPER_V2_L3_SKIP_UI=1` for backend/data L3 and explicitly avoided production interpretation of 8012 | `paper_v2_l3` child sessions passed without UI |
| Legacy ledger mismatch warning in data smoke | Historical rows outside BUG-198 scope | Not changed in this backend timeout/scheduler fix; warning is non-blocking without `--strict-history` | `paper_v2_data_quality` session successful |
| Live MiniQMT L5 unavailable | MiniQMT client cannot log in externally | Deferred live client validation; AIstock offline/8001 read-only evidence retained | BUG-198 evidence and 8001 read-only record |

## Result

- Final status: PASS for backend/data L3 regression with UI explicitly deferred; not a live MiniQMT L5 pass.
- Remaining risks: production 8001 must be restarted by the user after merge to load this fix; live MiniQMT validation can resume only after the MiniQMT client can log in.
- Need production backend restart: yes after merge/activation; Codex did not restart it.
- Need dev service restart: only for optional UI E2E on test ports. Test port 8012 must never be used as production Paper v2/MiniQMT evidence.

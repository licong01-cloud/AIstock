# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-01T11:43:15+08:00
- Git commit: 73a7411d (base before BUG-182 commit)
- Operator: lc999

## Scope

- Changed files: same BUG-182 backend/QMT scheduler timeout paths recorded in `tests/aistock_validation/history/paper_trading_v2/20260601_111826_l2_bug-182-miniqmt-scheduler-timeout-bounded-diagnostics.md`.
- Impacted flows: Paper v2 + Selection Center backend regression and data-quality chain.
- Business goal: ensure BUG-182 backend timeout changes do not break Paper v2/Selection Center core regression, data readiness, traceability, or deep data-quality assertions.
- Out of scope: frontend UI E2E and production restarts; `PAPER_V2_L3_SKIP_UI=1` was used because this issue does not change UI and user controls service restarts.
- Protected assets reviewed: no StrategyPackage manifest/model/HMM/QE artifact/paper ledger asset changed.

## Environment

- Backend port: no dev/prod service started by Codex for this nox chain.
- Frontend port: UI skipped.
- TDX port: existing configured data source only via read-only data-quality smoke.
- Conda/env: local repository nox/pytest environment.
- Database: read-only data-quality smoke and data-quality tests.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 | No blocking P0/P1 guardrail regressions in standard path set | `l0` session inside `paper_v2_l3` passed; broad scan had only non-blocking/baseline findings | Passed |
| Paper v2 backend | Paper v2 + Selection Center + StrategyPackage backend suite stays green | `paper_v2_backend` inside L3 -> 554 passed, 1 skipped, 2 xfailed | Passed |
| Data quality smoke | Required Paper v2/Selection schema, dataset audit freshness, package readiness, traceability pass | `paper_v2_data_quality` -> all required checks PASS; one existing legacy ledger consistency WARN only | Passed with non-blocking legacy warning |
| Deep data quality | DB data-quality assertion suite remains green | `data_quality_deep` -> 10 passed, 21 skipped | Passed |
| UI E2E | Not required for BUG-182 backend/QMT timeout change | `PAPER_V2_L3_SKIP_UI=1` documented | Skipped by scope |

## Commands

```powershell
$env:PAPER_V2_L3_SKIP_UI='1'
python -m nox -s paper_v2_l3
```

## Evidence

- API calls: none in L3 chain.
- DB checks: `paper_v2_data_quality` read-only checks PASS; latest completed trading day resolved to 2026-05-29 for data-quality context.
- Log files: no separate log file; nox stdout captured in terminal.
- Playwright report/trace: not applicable, UI skipped.
- Screenshots: not applicable.
- Business output summary: backend/data validation chain is green; existing legacy ledger consistency warning is not caused by BUG-182.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in final L3 backend/data chain | N/A | N/A | `paper_v2_l3` ran 5 sessions successfully with UI skipped |

## Result

- Final status: Passed for backend/data L3 slice.
- Remaining risks: UI E2E not rerun because no UI change and service restarts are user-controlled; final production runtime verification still required after merge and user backend restart.
- Need production backend restart: yes after merge/deploy; user must perform it.
- Need dev service restart: no.

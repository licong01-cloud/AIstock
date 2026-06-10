# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-10T16:24:09
- Git commit: e3c4a4b2
- Operator: Codex / lc999
- Related issue: BUG-210 / GitHub #567

## Scope

- Changed files: Paper v2 readiness/defaults, Selection Center UI fallback, MiniQMT unified runtime bridge, qmt_strategy_ledger retry logic, and tests.
- Impacted flows: Paper v2 Selection Center, Paper v2 run console readiness, simulation runtime MiniQMT bridge, MiniQMT operator UI path, L2/L4 scheduler retry.
- Business goal: prove Paper v2 L3 still works after Phase 7 canonical MiniQMT runtime migration and PIT/default-date fixes.
- Out of scope: real MiniQMT SIM L5 trading-window validation and production service restart.
- Protected assets reviewed: no model weights, strategy assets, production DB DDL, or production runtime services modified.

## Environment

- Backend port: temporary validation backend 8012, started by nox.
- Frontend port: temporary validation frontend 3012, started by nox and reclaimed after the run.
- TDX port: 19080, health checked by `scripts/aistock_validate.py services`.
- Conda/env: repository default nox/Python environment.
- Database: configured AIstock DB, used by data-quality/readiness checks; no production DDL executed.
- Browser/headless: Playwright Chromium via `npm run test:e2e -- tests/paper-v2`.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking high-risk path/secret/fallback/asset finding | `python -m nox -s l0` completed; guardrail findings existing/baseline or P2 non-blocking, `blocking=0` | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `paper_v2_backend: 612 passed, 1 skipped, 2 xfailed` | PASS |
| Data quality | Required Paper v2/Selection tables and audit data are fresh enough | `paper_v2_data_quality` success; known legacy ledger warning non-blocking | PASS |
| Deep data quality | Data quality regression tests pass or skip by design | `data_quality_deep: 10 passed, 21 skipped` | PASS |
| UI E2E | User-visible Paper v2 flow works with no unexpected page/request errors | `paper_v2_ui: 20 passed, 1 skipped` | PASS |
| Run console readiness | Readiness card appears and structured readiness flow works | `paper-v2-real-flow.spec.ts:1018` passed after PIT cutoff and minute-data default fixes | PASS |
| Operator UI | MiniQMT operator command remains explicit and controlled | `simulation-runtime-ops.spec.ts` two tests passed inside paper_v2_ui | PASS |
| Asset safety | No protected asset modified silently | Git diff limited to code/tests/validation records/BUG JSON; production gates noop | PASS |

## Commands

```bash
python -m nox -s paper_v2_l3
```

Expanded sessions from the same run:

```bash
python -m nox -s l0
python -m nox -s paper_v2_backend
python scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center --output tmp/paper_v2_data_quality_smoke.json
python -m pytest backend/tests/data_quality -q -p no:cacheprovider
npm run test:e2e -- tests/paper-v2
```

## Evidence

- API calls: `scripts/aistock_validate.py services --backend-port 8012 --tdx-port 19080` returned FastAPI `/openapi.json` HTTP 200 and TDX minute endpoint HTTP 200.
- DB checks: `paper_v2_data_quality` passed required schema/audit/readiness checks; known legacy ledger consistency warning remains non-blocking.
- Log files: `tmp/validation/services/paper_v2_backend_8012.log`, `tmp/validation/services/paper_v2_frontend_3012.log`.
- Playwright report/trace: Playwright list run showed 20 passed, 1 skipped.
- Business output summary: Selection Center live-data inference/history/import flow, run-console readiness/replay reset/live waiting controls, and simulation runtime MiniQMT operator panel all passed.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Run-console readiness previously hung/missed `.pv2-readiness-card` | Auto-generated selection artifact used same-day DB_HISTORICAL without previous-trading-day PIT cutoff | Added `selection_cutoff.py`; integrated readiness/day_runner/live_session PIT cutoff injection and runtime profile binding refresh | `paper_v2_l3` passed; `paper-v2-real-flow.spec.ts:1018` passed |
| Trading-day defaults previously selected current day without DB minute bars | `/paper-v2/trading-days/defaults` only gated on `stk_limit`, not required minute/day datasets | Defaults now use min ready date across `kline_minute_raw`, `stk_limit`, and `suspend_d`; fallback checks actual minute table max date | `test_trading_day_defaults.py`; `paper_v2_l3` passed |
| Selection Center history aggregation could show 0/0 when selectable-packages request failed | UI used `Promise.all`, so an auxiliary request abort hid real history aggregation | Changed package loading to `Promise.allSettled` and preserved history rendering when package list fails | `paper_v2_l3` Selection Center tests passed |

## Result

- Final status: PASS for L3 regression in BUG-210 Phase 7 non-live validation.
- Remaining risks: L5 real MiniQMT SIM trading-window validation still pending; legacy MiniQMT adapter file deletion deferred until L5 + user confirmation.
- Need production backend restart: yes after merge for activation, but Codex did not restart it.
- Need dev service restart: no; nox temporary services were cleaned by validation flow.

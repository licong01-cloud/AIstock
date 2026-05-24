# Paper v2 gate decoupling implementation

- Module: paper_trading_v2 / selection_center / strategy_package
- Level: L3
- Date: 2026-05-25T04:21:20+08:00
- Worktree: `F:\Dev\AIstock_worktrees\paper-v2-gate-decoupling-impl-20260525`
- Branch: `feature/paper-v2-gate-decoupling-impl-20260525`
- Design baseline: `docs/architecture/paper_v2_strategy_package_gate_decoupling_design_20260525.md`
- Git commit at record creation: pending implementation commit; previous HEAD `a85f5c3`
- Operator: Codex / lc999

## Scope

- Changed files: StrategyPackage admission and routers; Selection Center HMM runtime, result enrichment, industry tree, repository/models/service; Paper v2 trading calendar/session/service/UI; Paper v2 packages/selection/portfolio/run-console UI; frontend Paper v2 API/types; backend and Playwright regression tests.
- Impacted flows: QE StrategyPackage -> Selection Center -> watchlist import -> Paper v2 portfolio/runtime profile/session, HMM runtime profile, official trading-day status, Paper v2 industry blacklist selector.
- Business goal: remove Paper/Selection hard gates unrelated to alpha-core package identity; make HMM coefficients automatic/cache-backed; display actionable selection result fields; use official trading calendar service without weekday fallback.
- Out of scope: real live order submission, MiniQMT real order validation, production port restart, existing GitHub issue closure.
- Protected assets reviewed: no frozen StrategyPackage manifest, model weights, HMM snapshots, QE/RD-Agent artifacts, Paper ledger, or production data were edited by this implementation/validation.

## Environment

- Backend port: not started; production `8001` not touched.
- Frontend port: Playwright used dev `3011` through `playwright.paper-v2.config.ts`; production `3000` not touched.
- TDX port: not contacted by tests; result enrichment uses mocked quote fetchers in unit tests.
- Conda/env: current local Python environment through `python -m pytest`; frontend Node via existing `frontend/node_modules` junction.
- Database: backend tests use in-memory repositories or fake connections except existing tests that already use their configured dev fixtures; no DDL executed.
- Browser/headless: Playwright Chromium headless, mocked API routes.
- Branch state: branch is intentionally not merged to `main`; status showed branch behind `origin/main` by 1 before final commit.

## Design Compliance Matrix

| ID | Design item | Implementation refs | Evidence | Status | Gap / exception |
|---|---|---|---|---|---|
| V-01 | StrategyPackage vs runtime profile boundary | `backend/services/strategy_package/service.py`, `backend/services/selection_center/package_health.py` | Static scans: no `_require_governance_paper_ready`; no weekday fallback in Paper-sensitive paths | PASS | N/A |
| V-02 | `BACKTEST_APPROVED` admission | `PAPER_SIMULATION_ALLOWED_STATUSES`, `paper_simulation_admission` | `pytest backend/tests/strategy_package` and full targeted suite | PASS | N/A |
| V-03 | `paper_ready` is governance info only | `governance_eligibility`, `paper_ready_semantics`, `does_not_block_paper_simulation` | `test_enable_paper_endpoint_allows_simulation_despite_governance_blockers` | PASS | N/A |
| V-04 | Seed / rolling retrain warning only | `paper_simulation_admission.admission_policy.non_blocking_governance` | StrategyPackage service tests | PASS | Live approval remains stricter and out of scope |
| V-05 | Manifest drift remains fail-fast | StrategyPackage validator/repository integrity paths | StrategyPackage regression suite | PASS | Data repair for existing drift is separate from code implementation |
| V-06 | Selectable packages/admission API | StrategyPackage router + Selection Center selectable statuses | Backend API/router tests in targeted suite | PASS | No live server API smoke in this record |
| V-07 | Create portfolio without `enable-paper` as hard prerequisite | Paper v2 service/status allowance | Paper v2 backend regression suite | PASS | Real operator flow still requires post-merge dev/prod smoke |
| V-08 | Packages page can create from `BACKTEST_APPROVED` | `frontend/src/app/paper-v2/packages/page.tsx` | TypeScript/build validation | PARTIAL | No dedicated packages-page browser click in this record |
| V-09 | HMM compute-on-miss | `backend/services/selection_center/hmm_runtime.py` | HMM runtime unit tests | PASS | N/A |
| V-10 | HMM cache hit | `SectorHMMRuntime._load_existing_coefficients` and lock recheck | HMM runtime unit tests assert generator called once | PASS | N/A |
| V-11 | HMM idempotency/concurrency | Per-key generation lock in `SectorHMMRuntime` | Code review + HMM cache-hit regression | PARTIAL | No explicit multi-thread stress test yet |
| V-12 | HMM fail-fast, no neutral fallback | `hmm_runtime.py` validation | Static scan only finds docstring saying neutral fallback never fabricated; selection tests | PASS | N/A |
| V-13 | Paper/QE-style industry selector | `PaperIndustryBlacklistSelector.tsx`, Paper v2 selection/portfolio/run-console pages | TypeScript/build validation | PARTIAL | No dedicated selector Playwright test yet |
| V-14 | Runtime-scoped industry blacklist | Paper runtime config writes selected codes/trace | Backend config paths + TypeScript | PARTIAL | Persisted profile hash/evidence covered generally, not per-industry API assertion |
| V-15 | Remove failing Selection data-source options | `SelectionDataSource = "DB_HISTORICAL"`, selection page dropdown | Static scan: Selection UI no `TDX_REALTIME`/`MINIQMT_REALTIME` options | PASS | Global Paper `DataSource` still contains broker/quote values and is not Selection dropdown |
| V-16 | Source semantics split | Selection artifact uses DB historical; result enrichment uses TDX quote only for prices | Result enrichment tests + static scan | PASS | N/A |
| V-17 | Current-date entry price from TDX | `SelectionResultEnrichmentService` | `test_current_day_selection_entry_price_uses_tdx_quote_and_display_fields_persist` | PASS | TDX missing fails fast |
| V-18 | Historical PIT entry price | `SelectionResultEnrichmentService._load_daily_rows` | `test_historical_selection_entry_price_uses_pit_reference_close_not_current_quote` | PASS | Requires PIT runtime config to provide reference date |
| V-19 | Current price display only; watchlist uses entry price | Selection repository/service + UI table | Result enrichment tests + new watchlist entry-price regression | PASS | N/A |
| V-20 | Selection result stock name | Paper v2 symbol name resolver reused in enrichment | Result enrichment unit test + UI table columns | PASS | No screenshot; Playwright covers build/runtime only |
| V-21 | Trading-day cache normal reads | `TradingCalendarStatusService` | `test_trading_calendar_status.py` | PASS | N/A |
| V-22 | Trading-day cache auto rebuild | `TradingCalendarStatusService._load_or_refresh_cache` | `test_trading_calendar_status.py` | PASS | N/A |
| V-23 | Next-month coverage warning | `TradingCalendarStatusService._coverage_warnings` | `test_trading_calendar_status.py` + overview Playwright warning | PASS | N/A |
| V-24 | No weekday fallback | Paper v2 market/session/coldstart/scheduler paths | Static grep no weekday fallback in `backend/services`, `backend/routers`, `backend/schedulers`, `frontend/src/app/paper-v2`; scheduler unit test | PASS | Non-runtime scripts may still use weekday for safety windows/weekend jobs |
| V-25 | Paper home trading-day display | `frontend/src/app/paper-v2/page.tsx` | Playwright mocked overview test | PASS | Real backend/UI smoke pending user-approved environment |
| V-26 | Broker/source mismatch as preflight, not package gate | StrategyPackage admission excludes broker/source | Static review + Paper backend tests | PASS | MiniQMT dry-run not executed |
| V-27 | Execution profile as platform/preflight | Portfolio UI allows manifest default; backend fail-fast stays runtime-scoped | Paper v2 backend suite + TypeScript | PASS | N/A |
| V-28 | Selection -> Paper portfolio -> one-day LocalSim | Backend services covered by regression suite | `paper_trading_v2` tests | PARTIAL | No real DB LocalSim E2E run in this record |
| V-29 | Historical replay with official trading-day service | Paper v2 session/defaults uses trading calendar service | Paper v2 session tests | PARTIAL | No multi-day real replay smoke in this record |
| V-30 | MiniQMT sim dry-run | Scope intentionally decoupled from package gates | Static review | NOT RUN | Requires MiniQMT/runtime environment; not required for code merge gate here |
| V-31 | QE/Paper score equivalence | No scoring algorithm rewrite beyond result enrichment | Existing selection regression suite | NOT RUN | Business oracle needs real QE artifact comparison after merge/dev deployment |
| V-32 | Run evidence trace | Runtime config, display fields, trading-day cache info persisted/exposed | Backend tests + API/type coverage | PARTIAL | Full DB evidence audit for every runtime field pending dev API smoke |
| V-33 | Regression protection | Paper v2 + Selection + StrategyPackage full targeted suite | `481 passed, 1 skipped, 2 xfailed` | PASS | N/A |
| V-34 | Production boundary/gates | This record | Production gates below | PASS | No production activation performed |

## Validation Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No whitespace, obvious stale hard gate, weekday fallback, or Selection data-source regression | `git diff --check`; static `rg` scans | PASS |
| Backend syntax | Changed Python modules compile | `python -m py_compile ...` | PASS |
| Backend regression | Paper v2 + Selection Center + StrategyPackage behavior passes | `481 passed, 1 skipped, 2 xfailed in 16.64s` | PASS |
| Frontend type check | Paper v2 UI/types compile | `npx tsc --noEmit --pretty false --incremental false` -> no errors | PASS |
| Frontend build | Next.js production build succeeds | `npm run build` succeeded; existing unrelated hook warnings only | PASS |
| UI E2E | HMM auto-cache UI payloads and Paper overview trading-day display work with no page failures | Playwright `PASS (4) FAIL (0)` | PASS |
| Asset safety | No protected assets or production data edited | Git status/diff review | PASS |

## Commands

```bash
rtk python -m py_compile backend/services/selection_center/hmm_runtime.py backend/services/selection_center/result_enrichment.py backend/services/selection_center/industry_tree.py backend/services/trading_calendar_status.py backend/services/paper_trading_v2/session.py backend/services/paper_trading_v2/coldstart_sentinel.py backend/services/paper_trading_v2/poc/step4_intraday_revalidate.py backend/services/strategy_package/service.py backend/schedulers/strategy_scheduler.py backend/routers/trading_calendar.py backend/routers/paper_trading_v2.py backend/routers/selection_center.py backend/routers/strategy_packages.py backend/main.py
rtk python -m pytest backend/tests/strategy_package -q -p no:cacheprovider
rtk python -m pytest backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package backend/tests/test_strategy_scheduler_calendar.py -q -p no:cacheprovider
rtk npx tsc --noEmit --pretty false --incremental false
rtk npm run build
rtk npx playwright test tests/paper-v2/paper-v2-hmm-runtime-coefficients.spec.ts --config=playwright.paper-v2.config.ts
rtk git diff --check
rtk rg -n "weekday\\(|isoweekday|weekday fallback" backend/services backend/routers backend/schedulers frontend/src/app/paper-v2 -g "!*__pycache__*"
rtk rg -n "_require_governance_paper_ready|governance eligibility must be paper_ready|original fixed-weight validation must pass before enabling Paper" backend/services/strategy_package backend/tests/strategy_package
rtk rg -n "TDX_REALTIME|MINIQMT_REALTIME" frontend/src/app/paper-v2/selection frontend/src/lib/paper-v2/types.ts backend/services/selection_center -g "!*__pycache__*"
```

## Evidence

- Backend syntax: `py_compile` exited 0.
- Backend regression: targeted full suite exited 0 with `481 passed, 1 skipped, 2 xfailed`.
- StrategyPackage full suite before final aggregation: `182 passed`.
- Frontend type check: `TypeScript: No errors found`.
- Frontend build: Next.js build completed; warnings were pre-existing hook dependency warnings outside this change set or unrelated pages.
- Playwright: `tests/paper-v2/paper-v2-hmm-runtime-coefficients.spec.ts` passed 4/4 on Chromium using dev port `3011` and mocked API.
- Static scan: no weekday fallback matches in Paper/trading-sensitive backend services/routers/schedulers or Paper v2 UI; Selection UI no failing realtime data-source options.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| StrategyPackage tests still expected `paper_ready` blockers to return 400 | Tests encoded old governance gate semantics | Updated tests to assert Paper simulation admission is allowed and governance is warning/read-only | StrategyPackage suite `182 passed`; targeted suite `481 passed` |
| Runtime profile tests missed `model_config_id` | HMM profile now supports config-based latest snapshot resolution | Updated expected normalized HMM profile | Targeted suite `481 passed` |
| Watchlist tests used real enrichment/DB prices unintentionally | Result enrichment now correctly replaces reference price with entry price | Injected no-op/fixed enrichment for isolated watchlist tests and added entry-price-not-current-price regression | Targeted suite `481 passed` |
| Playwright HMM spec still required `coefficients_path` | UI behavior changed to automatic model-config based cache | Updated spec to assert no manual `coefficients_path` and added Paper overview trading-day assertion | Playwright `PASS (4) FAIL (0)` |

## Production Gates

- `production_ddl_gate=noop`: no migration/DDL/schema file changed; runtime reads existing `market.trading_calendar`.
- `production_frontend_dependency_gate=noop`: no frontend dependency file changed.
- `production_backend_dependency_gate=noop`: no backend dependency file changed.
- Production backend `8001`: not restarted or touched.
- Production frontend `3000`: not restarted or touched.
- Post-merge runtime activation: backend/frontend restart will be needed after user-approved merge for the new router/UI code to become active.

## Result

- Final status: implementation branch validated and ready for user review; not merged to `main`.
- Remaining risks: real DB/API LocalSim, MiniQMT sim dry-run, QE/Paper score equivalence, and production runtime smoke are intentionally deferred until user-approved deployment/test window.
- Need production backend restart: no action performed now; yes after approved merge/activation.
- Need dev service restart: only Playwright-managed dev frontend `3011` was used and closed by Playwright.

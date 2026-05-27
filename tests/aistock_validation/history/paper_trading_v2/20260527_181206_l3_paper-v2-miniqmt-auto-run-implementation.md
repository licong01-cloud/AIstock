# Paper v2 MiniQMT auto-run implementation

- Module: paper_trading_v2
- Level: L3
- Date: 2026-05-27T18:12:06+08:00
- Git commit: pre-commit branch feature/paper-v2-miniqmt-auto-run-20260527, base f8f4c9f0
- Operator: lc999 / Codex

## Scope

- Changed files: Paper v2 backend models/repository/service/session/scheduler/live_session/router, MiniQMT auto-run UI/API types, HMM runtime profile defaults, DB schema/migration, regression tests.
- Impacted flows: StrategyPackage -> MiniQMT SIM auto-run portfolio creation, portfolio auto-run enable/disable/status/config, scheduler restart recovery, MiniQMT live-session waiting/retry/cutoff, HMM daily auto-compute config acceptance.
- Business goal: 后端重启和盘中恢复后，auto_run_enabled 的 MiniQMT SIM 组合自动补齐 LIVE_ONLY session，并按交易窗口/数据/broker/HMM 状态等待或失败；不恢复 StrategyPackage 门禁，不用 TDX/LocalSim 伪造成交。
- Out of scope: 生产 DDL 应用、生产后端重启、MiniQMT 实盘 live、Phase 2 多策略虚拟分仓、真实交易时段 broker-authoritative 完整成交验证。
- Protected assets reviewed: 未修改 StrategyPackage manifest、QE/RD-Agent artifact、HMM snapshot、validated execution policy、paper ledger 历史资产；新增 migration 仅为显式 DDL，未应用生产库。

## Environment

- Backend port: 未启动开发后端；未触碰生产 8001。
- Frontend port: 未启动开发前端；执行静态 type/build。
- TDX port: 未调用。
- Conda/env: AIstock, plus direct `C:\Users\lc999\miniconda3\envs\AIstock\python.exe` for nox to avoid conda GBK output bug.
- Database: 未连接/未迁移生产 DB；repository 使用单元测试 in-memory 与静态 migration 检查。
- Browser/headless: 未执行 Playwright；UI 改动通过 TypeScript 与 Next build 验证。

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `python -m nox -s l0` PASS; guardrail scan reports blocking=0; existing P2/baseline findings outside touched scope | PASS with known nonblocking baseline warnings |
| Backend tests | Paper v2 + Selection Center backend tests pass | `python -m nox -s paper_v2_backend` -> 511 passed, 1 skipped, 2 xfailed | PASS |
| Focused Paper v2 tests | Auto-run/session/MiniQMT regressions pass | targeted pytest -> 103 passed; focused auto/session/live/miniqmt subset -> 84 passed | PASS |
| Static backend | Modified backend modules compile | `python -m compileall ...` PASS | PASS |
| Frontend type/build | MiniQMT UI API/types compile and production build succeeds | `frontend/node_modules/.bin/tsc.cmd --noEmit` PASS; `npm --prefix frontend run build` PASS with pre-existing react-hooks warnings in unrelated files | PASS |
| API flow | API contracts exist for create/status/enable/disable/config/bootstrap/recover | Router/service tests cover service behavior; no live dev backend API smoke executed | PARTIAL |
| UI E2E | User-visible MiniQMT auto-run flow works | Static build only; no dev-port Playwright in this run | NOT RUN |
| Asset safety | No protected asset modified silently | `git diff --stat`, changed-file review; no protected artifact paths modified | PASS |

## Design Compliance Matrix

| Design item | Implementation refs | Evidence | Status |
|---|---|---|---|
| Phase 1 scheduler bootstrap status/log | `backend/main.py`, `backend/services/paper_trading_v2/scheduler.py`, router bootstrap endpoint | unit test `test_scheduler_status_exposes_auto_run_bootstrap_state`, nox backend | PASS static/unit |
| Phase 2 portfolio auto-run columns and broker binding table | `backend/db/init_trading_core_v2_schema.py`, `backend/migrations/paper_v2_miniqmt_auto_run_20260527.sql`, `repository.py`, `models.py` | `test_auto_run_migration_declares_required_comments`, compileall | PASS; production DDL not applied |
| Phase 2 config normalize/hash | `backend/services/paper_trading_v2/auto_run.py` | `test_auto_run_config_hash_is_stable_and_uses_platform_defaults` | PASS |
| Phase 3 recover enabled portfolios and auto-create LIVE_ONLY | `AutoRunCoordinator`, `scheduler.run_once` | `test_auto_run_recovery_creates_missing_live_session_without_ticking_orders` | PASS |
| Phase 4 MiniQMT window/wait/retry/cutoff | `live_session.py` | tests for waiting market window, broker wait, cutoff failure; no fake success checks | PASS unit |
| Phase 5 HMM auto daily config acceptance | `runtime_profile.py`, `hmm_runtime.py` | nox backend tests; backtest/runtime profile tests updated for `auto_compute=true`, `manual_snapshot_required=false` | PASS config path; cache compute not live-verified |
| Phase 6 API and UI | `backend/routers/paper_trading_v2.py`, `frontend/src/app/paper-v2/miniqmt-sim/page.tsx`, `frontend/src/lib/paper-v2/*` | tsc/build; backend service tests | PASS static; UI E2E not run |
| Phase 7 production activation | migration and runtime code ready | production DDL/restart/MiniQMT trading-hours verification not executed | PENDING by design |

## V-01..V-20 Design Matrix

| ID | Result | Evidence / gap |
|---|---|---|
| V-01 | PARTIAL | bootstrap status exposes env=false/true semantics; no process-start integration test. |
| V-02 | PARTIAL | scheduler bootstrap API implemented; no production/dev backend restart test. |
| V-03 | PASS | auto-run recovery creates missing LIVE_ONLY session without run/order. |
| V-04 | COVERED BY EXISTING | live session uses official `calendar_provider.ensure_trading_day`; non-trading day waits; no weekday fallback added. |
| V-05 | PASS | 08:00/09:00 window test waits and does not call day runner/order. |
| V-06 | PASS | BrokerConnectivityError before cutoff -> LIVE_WAITING_BROKER, no final failure. |
| V-07 | NOT RUN | Afternoon broker-recovery success path requires fake state-switch or live-time test; not covered in this run. |
| V-08 | PASS | after cutoff creates failed run/session, no orders/fake success. |
| V-09 | PARTIAL | retryable data/HMM exceptions are handled as waiting before cutoff; refresh_audit mock case not separately added. |
| V-10 | PARTIAL | HMM runtime accepts auto_compute and existing auto-generation path remains; compute-once/cache reuse not live-verified. |
| V-11 | PARTIAL | HMMRuntimeUnavailableError is retryable before cutoff and fails after cutoff via existing fail-fast; dedicated HMM failure test not added. |
| V-12 | COVERED BY EXISTING | existing SUCCEEDED run returns already reconciled and no duplicate submit in live_session path. |
| V-13 | PARTIAL | existing active/failed run handling prevents naive duplicate submit; crash-after-partial broker order reconciliation remains live/broker validation gap. |
| V-14 | PASS | same MiniQMT account second active auto-run binding rejected as account resource conflict, not StrategyPackage gate. |
| V-15 | COVERED BY EXISTING | create portfolio still uses `asset_eligibility_service.require_eligible`; only asset eligibility remains a package admission check. |
| V-16 | PASS | auto-run config patch/TopK/HMM/blacklist stored as portfolio runtime config; no runtime-profile trading gate reintroduced. |
| V-17 | PARTIAL | UI calls real create/status/recover APIs and build passes; Playwright one-click flow not run. |
| V-18 | PARTIAL | MiniQMT page uses `ErrorPanel`/Chinese status panels; no Playwright error-diagnostics capture. |
| V-19 | PASS | migration comment test covers new columns/table required comments. |
| V-20 | PASS | creatable broker backends remain `local_sim|minqmt_sim`; `minqmt_live` excluded. |

## Commands

```powershell
# Targeted backend regressions
conda run -n AIstock python -m pytest backend/tests/paper_trading_v2/test_auto_run.py backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_portfolio_broker_backend.py -q
conda run -n AIstock python -m pytest backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py backend/tests/paper_trading_v2/test_auto_run.py backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_portfolio_broker_backend.py -q

# Static backend
conda run -n AIstock python -m compileall backend/services/paper_trading_v2 backend/services/selection_center backend/routers/paper_trading_v2.py backend/db/init_trading_core_v2_schema.py

# Frontend type/build
cd frontend
.\node_modules\.bin\tsc.cmd --noEmit
cd ..
npm --prefix frontend run build
python -c "import subprocess,pathlib; pathlib.Path('frontend/tsconfig.tsbuildinfo').write_bytes(subprocess.check_output(['git','show','HEAD:frontend/tsconfig.tsbuildinfo']))"

# Project gates
conda run -n AIstock python -m nox -s l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_backend

git diff --check
```

## Evidence

- Backend targeted: 84 passed; broader Paper v2/coldstart subset: 103 passed.
- Backend nox: 511 passed, 1 skipped, 2 xfailed.
- Static: compileall passed; tsc passed; Next build passed with unrelated existing react-hooks warnings.
- DB checks: migration comment fragments tested; production DB migration not applied.
- Log files: command output captured in Codex session; no service logs generated.
- Playwright report/trace: not generated.
- Screenshots: not generated.
- Business output summary: portfolio auto-run config persists with stable hash; enabled portfolio recovery creates missing LIVE_ONLY session without order; MiniQMT waits before window/broker and fails after cutoff without fake success; broker-authoritative MiniQMT data source preserved.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `paper_v2_backend` first run failed 2 tests | HMM runtime profile now intentionally includes `auto_compute` and `manual_snapshot_required`, tests still expected old manual snapshot schema | Updated runtime/backtest contract test expectations | targeted 2 passed; full `paper_v2_backend` 511 passed |
| `conda run ... nox -s paper_v2_backend` wrapper printed GBK `UnicodeEncodeError` despite command intent | conda base wrapper cannot encode nox/pytest Unicode output on Windows console | Re-ran with env python directly | direct env python nox PASS |

## Result

- Final status: PASS for code-level implementation and static/unit/backend gates; production activation gates remain pending.
- Remaining risks: UI E2E not run; production DDL not applied; backend restart/autostart not verified on port 8001; MiniQMT trading-hours full broker-authoritative submit/reconcile not verified; Phase 2 multi-strategy shared account not in scope.
- Need production backend restart: yes after merge/code deployment, but not performed by Codex.
- Need dev service restart: no dev service was started.
- production_ddl_gate: pending, because new `paper_v2.portfolio` columns and `paper_v2.broker_account_binding` table require applying `backend/migrations/paper_v2_miniqmt_auto_run_20260527.sql` before production auto-run activation.
- production_backend_dependency_gate: noop, no new Python dependency.
- production_frontend_dependency_gate: noop, no package dependency change.

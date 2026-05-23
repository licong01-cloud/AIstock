# BUG-104 SimulationRuntime Production Provider Validation - 2026-05-23

## Scope

- BUG: `BUG-104` / GitHub `#173`.
- Branch: `bug/BUG-104-simulation-runtime-provider-20260523`.
- Worktree: `F:\Dev\AIstock_worktrees\bug-104-simulation-runtime-provider-20260523`.
- Task tier: T2 / P0 simulation runtime readiness slice.
- Production runtime impact: backend `8001` not started or restarted; frontend `3000` not stopped; no production DB writes; no real MiniQMT submit.

## Implemented Scope

- Added production `SimulationRunContextProvider` mode that loads LocalSim state from persisted Paper v2 portfolio state and MiniQMT state from `qmt_strategy` virtual ledger state.
- Added fail-fast provider diagnostics and env-gated production provider bootstrap via `SIMULATION_RUNTIME_CONTEXT_PROVIDER=production` or `ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER=1`.
- Added MiniQMT preview-only managed order facade for production provider default submit-disabled mode; preview may read broker positions for sellability/reconciliation but never calls `place_order`/`cancel_order`.
- Added controlled scheduler status/tick/start/stop capability reporting; plain lifecycle tick remains `submit=false` by default.
- Updated LocalSim broker construction so production context can seed latest persisted cash and positions.
- Updated simulation runtime UI and Playwright mock to surface controlled ops, provider mode, and MiniQMT preview/dry-run semantics.

## Design Compliance Matrix

| Requirement | Implementation refs | Evidence | Status | Gap / exception |
| --- | --- | --- | --- | --- |
| Production provider builds real contexts from persisted runtime release/binding state | `backend/services/simulation_runtime/scheduler.py` (`ProductionSimulationRunContextProvider`) | `test_production_context_provider_builds_localsim_broker_from_persisted_paper_state`, `test_production_context_provider_loads_miniqmt_positions_from_virtual_ledger_without_submit_broker` | PASS | Uses in-memory fakes for unit isolation; default factories point to real repos. |
| LocalSim dry-run generates `SimulationDailyRun`, `DailySelectionEvidence`, and `ExecutionPlan` | existing lifecycle path plus provider/local broker seeding | `python -m pytest backend/tests/simulation_runtime -q -p no:cacheprovider` | PASS | Production `8001` smoke not run by user boundary. |
| MiniQMT dry-run/preview generates shared plan plus qmt strategy ledger preview without unsafe submit | `PreviewOnlyMiniQMTManagedOrderService`; `MiniQMTPreviewBatchSubmitResult`; ledger batch/intent persistence | `test_production_context_provider_miniqmt_submit_defaults_to_preview_only_and_persists_ledger_evidence` | PASS | Real trading-hours MiniQMT L5 remains pending; this is safe dry-run/preview only. |
| Preview-only MiniQMT does not bypass sellability checks | preview facade delegates broker can-sell checks without calling order submit | `test_production_context_provider_miniqmt_preview_checks_broker_can_sell_without_submit` | PASS | Reads broker positions only; no broker mutation. |
| Controlled manual tick/run endpoint exists and defaults to dry-run | `backend/routers/simulation_runtime.py`; `backend/services/simulation_runtime/ops.py` | `test_scheduler_tick_api_is_controlled_dry_run_by_default` | PASS | `start/stop` require background scheduler instance; no production start in validation. |
| Scheduler status reports provider, submit mode, and recovery mode | `SimulationLifecycleScheduler.status`, `SimulationLifecycleBackgroundScheduler.status`, ops projection | `test_scheduler_status_reports_provider_and_controlled_tick_capability`, ops API test, Playwright UI test | PASS | None. |
| Restart/repeated tick does not duplicate orders | persisted run/plan reuse and preview batch idempotency | MiniQMT preview test reruns `scheduler.run_once(... submit=True)` and asserts no duplicate preview intents / no broker submits | PASS | Process restart simulated by repeated tick over persisted repo state, not live backend restart. |
| No stale selection binding is used for a new trading day; fresh daily evidence is required | `SimulationLifecycleScheduler._validate_fresh_selection_evidence` | `test_scheduler_rejects_stale_selection_evidence_for_new_trade_date` | PASS | Rejects mismatched trade date/release/manifest evidence before plan build. |
| API/UI smoke covers status/runs/detail/execution plan/manual tick/provider display | ops API pytest and frontend Playwright | `test_ops_api.py`; `simulation-runtime-ops.spec.ts` | PASS | Live `8001` smoke intentionally not run. |
| BUG registry and GitHub remain synchronized | BUG JSON already links GitHub #173; MCP context was queried | `get_bug_agent_context(BUG-104)` returned GitHub #173; GitHub issue view returned OPEN with labels `bug`, `P0`; BUG JSON stays open until the fix commit exists. | PARTIAL | MCP lifecycle write is deferred until after commit hash exists; live GitHub issue was readable. |

## Validation Commands

- `gh.exe issue view 173 --repo licong01-cloud/AIstock --json number,title,state,labels,url`
  - Result: GitHub #173 is OPEN with labels `bug`, `P0` and URL `https://github.com/licong01-cloud/AIstock/issues/173`.
- `python -m pytest backend/tests/simulation_runtime -q -p no:cacheprovider`
  - Result: `62 passed in 11.69s`.
- `python -m compileall backend/services/simulation_runtime backend/routers/simulation_runtime.py backend/services/paper_trading_v2/broker/localsim.py`
  - Result: passed; output listed `backend/services/simulation_runtime`.
- `git diff --check`
  - Result: passed; no whitespace errors.
- `python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_ops_api.py -q -p no:cacheprovider`
  - Result: `33 passed in 16.43s`.
- From `frontend/`: `FRONTEND_PORT=3012 BACKEND_PORT=8012 npx playwright test tests/paper-v2/simulation-runtime-ops.spec.ts --project=chromium --reporter=line`
  - Result: `1 passed (6.7s)`.

## Business Outcomes Verified

- Production provider no longer silently converts loader failures into empty-position/empty-price success.
- LocalSim provider path seeds latest persisted cash and positions into the broker instead of starting from default cash/empty lots.
- MiniQMT default production path is preview-only unless explicit env gates enable managed submit.
- MiniQMT preview writes durable qmt strategy ledger batch/intent evidence with `preview_only=true` and `broker_called=false`.
- MiniQMT preview enforces strategy lot availability, batch aggregate cash/sell checks, and broker can-sell constraints before ledger preview persistence.
- Controlled tick endpoint defaults to `submit=false` and reports per-binding run/plan status.
- UI no longer describes the page as read-only-only; it shows controlled ops, manual tick/start-stop capability, provider mode, and default submit safety.

## Gates

- `production_ddl_gate=noop` - no migration or schema DDL changed.
- `production_backend_dependency_gate=noop` - no backend dependency manifest changed.
- `production_frontend_dependency_gate=noop` - no `package.json` or lockfile changed; `frontend/node_modules` is ignored local install state only.
- `production_backend_8001_touched=false` - no backend start/restart performed.
- `production_frontend_3000_touched=false` - existing `3000` listener was not stopped.
- `dev_frontend_3012_touched=true` - Playwright started/stopped its dev server on `3012`.
- `production_db_write=false` - no production DB writes.
- `miniqmt_real_submit=false` - no real MiniQMT submit/order placement.

## Residual Risks / Follow-up

- Live production API smoke against `http://127.0.0.1:8001` was not executed because the user retained backend runtime ownership and explicitly disallowed backend start/restart in this task.
- MiniQMT real SIM L5 trading-hours validation remains separate and must be explicitly authorized; this validation proves safe preview/dry-run behavior, not live trading readiness.
- BUG/GitHub lifecycle should be moved to `fixed` only after the commit hash exists; live GitHub issue #173 was readable during validation.

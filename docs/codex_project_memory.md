# AIstock Codex Project Memory

## Purpose

AIstock is an A-share quantitative research and experiment platform, with future live-trading capabilities. The platform combines local market data, strategy research, AI-assisted factor or strategy generation, paper trading, QMT integration, and frontend dashboards.

This memory is maintained for Codex. Do not use AGENTS.md for Codex-specific notes unless the user explicitly asks for that file to be changed.

## Service Architecture

### 1. TDX Go Backend

- Path: tdx-api-main/web/server.go
- Typical port: 19080
- Role: TDX data service and market data bridge.

### 2. FastAPI Backend

- Path: backend/main.py
- Typical port: 8001
- Role: main application API, orchestration layer, quant workflow backend, local data access, AI workflow backend, and trading adapter gateway.

Important directories:

- backend/routers: FastAPI router layer and API endpoints.
- backend/services: business services and workflow logic.
- backend/data_service: unified data access layer.
- backend/db/pg_pool.py: PostgreSQL / TimescaleDB connection pool.
- backend/infra: external infrastructure clients, including QMT, DeepSeek, WSL Qlib, and compute node clients.
- backend/qlib_exporter: Qlib snapshot and bin export tools.

### 3. Next.js Frontend

- Path: frontend
- Typical port: 3000
- Main routes: frontend/src/app
- Major sections include quantevolver, rdagent, local-data, qmt, paper-trading, watchlist, and analysis.

## Core Subsystems

### QuantEvolver / QE

- backend/services/quantevolver
- backend/routers/quantevolver.py
- backend/routers/quantevolver_evolution.py
- Purpose: AI-assisted strategy or factor evolution workflows.

### RD-Agent Integration

- backend/services/rdagent_*
- backend/routers/rdagent*.py
- rdagent_assets
- Purpose: integration with RD-Agent workflows and generated research assets.

### Paper Trading

- backend/services/paper_trading
- Purpose: simulated trading and strategy validation before live execution.

### QMT / xtquant

- backend/infra/qmt_client.py
- backend/routers/qmt.py
- Purpose: QMT client integration for future live or semi-live trading workflows.

### RL Execution

- rl_execution
- Purpose: reinforcement-learning related execution research.

### Monitoring

- monitoring/docker-compose.yml
- Purpose: Prometheus / Grafana monitoring stack.

## Engineering Rules for Codex

- Do not modify AGENTS.md unless explicitly requested.
- Prefer AGENTS.override.md and docs/codex_project_memory.md for Codex-specific project notes.
- Before making structural changes, inspect the relevant router, service, data access, and frontend route together.
- Treat trading-related code as high-risk: preserve existing behavior unless the user explicitly requests a behavioral change.
- Distinguish research, paper trading, and real execution paths. Do not assume live-trading safety.
- Prefer small, reviewable changes with tests or clear manual verification steps.
- For project-wide searches, prefer rg / rg --files.

## Known Current Workspace Notes

- The existing AGENTS.md in the project root may belong to another programming tool. Avoid editing it.
- If Codex is launched from F:\Dev\AIstock, this AGENTS.override.md file should be read as the Codex-specific project instruction file.
- If Codex is launched from another directory, use --add-dir F:\Dev\AIstock for write access, but project instructions may not be loaded automatically unless the project directory is the working directory.
- Trading Core v2 / Paper Trading v2 restart-safe continuation plan is recorded in `docs/architecture/paper_trading_v2_remaining_execution_plan.md`. Before continuing this work after a restart, read that document. Do not install Torch yet; V24 is deprecated and V25 execution strategy is still under development.

## Trading Core v2 / Paper Trading v2 Update - 2026-04-25

- Added explicit Strategy Package persistence/service/API around `strategy_pkg` schema with frozen manifest JSON, `manifest_sha256`, mutable status transitions, and paper-usage tracking.
- Added package-based Selection Center runtime/repository/service/API under `backend/services/selection_center`, supporting single package, intersection, union, and weighted rank fusion.
- Added Paper Trading v2 portfolio/run persistence and `PaperTradingDayRunner` under `backend/services/paper_trading_v2`, using frozen StrategyPackage manifests, explicit minute data sources, trading-calendar checks, suspend-status requirements, OMS/minute execution, fill-driven ledger, persisted cash/positions/snapshots/errors.
- Added schema bootstrap/migration files: `backend/db/init_trading_core_v2_schema.py` and `backend/migrations/trading_core_v2_schema.sql`. Business services do not run DDL implicitly.
- New routers are registered in `backend/main.py`: `/api/v1/selection-center` and `/api/v1/paper-v2`; Strategy Package API was expanded under `/api/v1/strategy-packages`.

## Tushare suspend_d Integration - 2026-04-26

- Added `market.suspend_d` ingestion support for Tushare `suspend_d` daily suspension/resumption rows, including table bootstrap, DatasetSpec registration, ingestion API/frontend dataset registration, and default daily schedule at 09:05.
- Paper Trading v2 minute market inputs now use an explicit `DbSuspendStatusProvider` backed by `market.suspend_d` when authoritative runs require suspension status; raw minute-volume heuristics are not used as the authoritative suspend source.
- Updated suspend_d scheduling to use explicit current/next trading-day refreshes: a base hourly `suspend_d` schedule plus fixed internal refresh windows at 17:30 previous-day, 07:30, 08:50, 09:05, 12:40, and 16:10. These jobs bypass normal incremental auto-range because future-dated suspend rows may already exist before same-day pre-open refreshes.
- Backfilled `market.suspend_d` from 2018-08-01 through 2026-04-27 with 2,827 successful daily batches and zero failed batches, then refreshed 2026-04-27 through 2026-04-28. Future `suspend_d` date-window refreshes are incremental but replace each fetched trade_date before insert, so upstream corrections or removed rows do not leave stale local suspension flags.
- Added `market.dataset_date_refresh_audit` for dataset/trade_date refresh status. `suspend_d` Tushare BY_DATE syncs now write success/failure audit rows, and historical audit rows were seeded for 2018-08-01 through 2026-04-28. Selection Center and Paper Trading v2 use this audit as a fail-fast data readiness gate before applying confirmed suspension filtering.
- Selection Center and Paper Trading v2 now treat runtime score output as raw ranking, remove confirmed `suspend_d` suspensions from final tradable candidates, compactly re-rank selected candidates, and keep excluded rows with `suspended_by_suspend_d` trace context. Paper Trading v2 also exposes historical DB minute replay via `/api/v1/paper-v2/portfolios/{portfolio_id}/replay`, reusing the normal day runner instead of the legacy paper/backtest path.

## Paper Trading v2 Readiness Update - 2026-04-26

- Paper Trading v2 day runs now require both `suspend_d` and `stk_limit` dataset-date audit success before execution when the frozen strategy package declares those minute data requirements. Execution still re-checks per-symbol suspension, limit price, `pre_close`, and minute bars through the strict market data provider.
- Added `PaperTradingReadinessService` and `/api/v1/paper-v2/portfolios/{portfolio_id}/readiness` to preflight the authoritative day path without persisting a run. The check validates package manifest, trading calendar, duplicate run absence, dataset refresh audit, portfolio state, selection runtime, tradability filter, target positions, rebalance intents, and minute market data.
- Historical Paper v2 replay now has an explicit rerun policy. The only supported policy is `reject_existing`; replay preflights the whole date range and fails before partial execution if any portfolio/date already has a run.
- Added `scripts/seed_dataset_refresh_audit.py` to seed audit rows from existing local market tables. `stk_limit` audit rows were seeded for 2018-08-01 through 2026-04-24 from existing `market.stk_limit` rows.
- Paper Trading v2 day runs now persist detailed run events for data readiness, signal generation, tradability filtering, target generation, order-intent generation, per-symbol market data loading, and order execution. The API exposes runs, run events, and persisted errors through `/api/v1/paper-v2/portfolios/{portfolio_id}/runs`, `/run-events`, and `/errors`.

## Package Selection Paper v2 Closure Update - 2026-04-26

- Strategy Package API now exposes status events and a generic validated status transition endpoint, while retaining dedicated enable-selection, enable-paper, and retire endpoints. Manifest hash remains independent from mutable package status.
- Selection Center now stores `package_ids` on `selection.run`, supports listing selection runs, and can create a Paper v2 portfolio from a successful single-package selection run. The created portfolio is linked back through `selection.paper_portfolio_link` with selection runtime config containing the exact aggregate `selection_scores` and source trace.
- Multi-package selection-to-paper remains fail-fast with `UnsupportedFeatureError` until a combined StrategyPackage contract exists; union/intersection results are not silently converted into a single-package portfolio.
- Paper v2 portfolio lifecycle now supports pause, resume, complete, and retire transitions. Day runs/readiness checks require the portfolio to be `READY`.
- Paper v2 exposes a basic persisted-snapshot performance report endpoint at `/api/v1/paper-v2/portfolios/{portfolio_id}/performance-report`; it fails if no daily snapshot exists.

## Paper v2 Runtime Config / Execution Policy Update - 2026-04-26

- Added detailed design doc `docs/architecture/paper_trading_v2_runtime_profile_execution_policy_design.md` for runtime profiles, backtest-validated execution policies, multi-package weighted fusion, model freshness, and replay reset.
- Selection Center now implements `weighted_fusion` using weighted rank-normalized scores with source package/rank/weight trace metadata; raw-score fusion is not used silently.
- Selection Center API now has dependency-injected service construction for tests and exposes weighted-fusion traces through run and aggregate-result responses. Multi-package selection-to-paper still fails fast with `UNSUPPORTED_FEATURE`; only single-package selection runs can create Paper v2 portfolios until a combined package or SelectionBundle contract exists.
- Strategy Package Center now has `strategy_pkg.validated_execution_policy` and service/API support for backtest-validated minute execution policies. Paper Trading v2 rejects raw runtime execution overrides and stores the validated policy snapshot/hash in each run.
- Paper v2 replay now supports explicit `reset_portfolio` with confirmation text matching `portfolio_id`, reset audit in `paper_v2.reset_audit`, and dependency-safe deletion before replay. Default remains `reject_existing`.
- Added `strategy_pkg.model_state` and Strategy Package model-state/rolling-retrain-preview APIs. Initial backtest-origin models are surfaced as stale-warning state; retrain remains manual-confirmation driven.

## Dynamic Multi-Package Selection Update - 2026-04-26

- Added design doc `docs/architecture/dynamic_multi_package_selection_design.md` for dynamic multi-package package picking, metrics display, existing-run aggregation, and the explicit no-Paper-v2-execution boundary.
- Strategy Package API payloads now include display-only `metrics_summary`, and `/api/v1/strategy-packages/{package_id}/metrics-summary` exposes IC, Rank IC, ICIR, Sharpe, annual return, max drawdown, NAV, turnover, sample dates, missing metrics, and raw metric keys without changing the frozen manifest hash.
- Selection Center now exposes `/api/v1/selection-center/selectable-packages` with eligible StrategyPackages, metrics summaries, model staleness state, and latest selection run summary for the aggregation UI.
- Selection Center now exposes `/api/v1/selection-center/aggregate-runs` to aggregate already-completed single-package selection runs when trade date/data source match; it persists a new aggregate `selection.run` with source run trace.
- Added frontend route `frontend/src/app/paper-trading/package-selection/page.tsx` and a Paper Trading tab for dynamic package selection, weight editing, direct multi-package runs, existing-run aggregation, fail-fast error display, and aggregate result trace. Multi-package Paper v2 portfolio creation remains unavailable by design.

## Paper v2 Backend Hardening Update - 2026-04-26

- Added normalized Selection/Paper `runtime_profile` parsing with industry blacklist, HMM settings, tradability settings, and top-k settings outside the frozen StrategyPackage manifest. Unknown keys inside `runtime_profile` fail validation.
- Shared tradability filtering now removes industry-blacklisted candidates with lower-rank backfill and stores explicit `industry_blacklisted` exclusions. HMM `enabled=true` now uses the precomputed-artifact runtime documented below.
- Strategy Package model retrain now has manual-confirmation job tracking through `strategy_pkg.model_retrain_job`, `/model-retrain/start`, and `/model-retrain/jobs`. Starting a job marks model state `RETRAINING`; it never marks the model current until a future training executor records success.
- Paper v2 supports per-trade-date activation of backtest-validated execution policies through `paper_v2.execution_policy_activation` and `/execution-policy-activations`. Day runner/readiness use the active date policy first and the portfolio default policy otherwise.
- Selection Center exposes `/runs/{run_id}/excluded-results` for suspended/blacklisted trace review.
- Paper v2 performance reports now include annualized return, annualized volatility, Sharpe, average daily return, win-day ratio, and explicit insufficient-data reasons rather than fabricating unavailable metrics.
- UI/manual page联调 remains deferred per user direction; use a non-8001 temporary backend port for API validation.

## Selection Runtime HMM / Industry Data Update - 2026-04-26

- Confirmed the required Shenwan sector data chain already exists in the local Tushare ingestion layer and DB: `market.sw_index_classify`, `market.sw_index_member`, `market.sw_daily`, and post-processed `market.sector_data`.
- `backend/data_service` exposes `sector_data` for QE/factor feature loading, but StrategyPackage/Paper v2 needs PIT stock-to-industry metadata. To avoid changing legacy data-service semantics, Selection Center now has `DbSwIndustryLookupProvider` backed directly by `market.sw_index_member`.
- Industry blacklist filtering now matches exact L1/L2/L3 industry code or name, persists rich exclusion context, backfills from lower-ranked candidates, and fails fast if the blacklist is enabled but no PIT/candidate industry metadata exists.
- HMM runtime is now implemented for precomputed artifacts only: `runtime_profile.hmm.enabled=true` requires `model_snapshot_id` and `signal_preset`; the snapshot/model/coefficients file must exist; every candidate must have a stock-sector map entry and a trade-date coefficient; adjusted scores are re-ranked and traced in `component_scores.hmm`.
- HMM training/retraining is still not run from Paper v2 or Selection Center. Missing HMM data never falls back to neutral coefficients.

## HMM Rolling Training Update - 2026-04-26

- HMM rolling training is implemented as a manual HMM Training Center flow, still executed through WSL `rdagent-gpu`; Selection Center and Paper v2 only consume completed snapshots plus coefficient artifacts.
- Added rolling preview/trigger APIs under `/api/v1/hmm-training/configs/{config_id}/rolling-training/*`. Trigger requires `confirm_text == config_id` and persists the computed split into `model_train_configs.config_json` before creating a pending job.
- The recommended validation choice among 1-3 months is the latest 3 calendar months. One- or two-month validation windows remain available for diagnostics but return warnings; default training window is 3 years ending before validation starts.
- Rolling preview uses the common latest completed date across `market.sector_data`, `market.sw_daily`, and `market.index_daily` (`000300.SH`), then derives trading-day-aware `train_start`, `train_end`, `val_start`, `val_end`, `coefficient_start`, and `coefficient_end`.
- `scripts/hmm_train_script.py` now passes rolling split dates into RD-Agent `HMMTrainConfig`. HMM coefficient precompute is mandatory before a snapshot is marked completed; precompute failure fails the job and avoids inserting a ready snapshot.

## QE Status Sync / Enhanced Metrics Update - 2026-04-26

- One-off QE experiments now have a backend reconciliation scanner (`QEExperimentStatusScanner`) started by `backend/main.py`; it scans running `qe_experiments` and reuses run-status synchronization so single-alpha and Multi-Alpha rows converge without relying on an open UI/SSE session.
- RD-Agent callback URLs are now expanded to concrete AIstock webhook endpoints instead of passing raw `infra.compute_nodes.callback_url` base URLs. Single QE uses `/api/v1/quantevolver/webhook/loop-completed`; evolution uses `/api/v1/quantevolver/evolution/webhook/loop-completed`.
- `get_experiment_enhanced_metrics` can read local `qlib_results_enhanced.json` from `workspace_path`/`QE_WORKSPACE_WIN` before falling back to RDAgent, fixing completed experiments whose enhanced artifacts exist locally while RDAgent 9000 is stopped.
- Experiment log streaming now appends `AIstock authoritative final status` and can fall back to local `run.log`, so preserved old failure stack traces do not hide a later successful recovery.

## QE Strategy Config Display Update - 2026-04-26

- `StrategyConfigCard` now reads effective runtime settings from `custom_params`, `model_params`, loop config and task overrides instead of only experiment top-level columns. This fixes QE detail pages showing execution algo, tail unfilled handling, stock-pool blacklist filtering and HMM as default/disabled even though the submitted experiment persisted them in `custom_params`.
- Compose submission now fails fast when industry blacklist is enabled but no generated stock pool exists, or HMM is enabled without a completed snapshot selection. These selections are no longer silently omitted from the generated QE payload.
- QE stock-pool blacklist experiments now carry/display an industry blacklist snapshot. Future generated experiments persist `sector_blacklist_snapshot` into `custom_params`; old experiments are display-enriched from `filtered_pool_YYYYMMDD` plus current `sw2_pool_config` with an explicit warning that it is reconstructed, not a historical persisted snapshot.
- Enhanced metrics loading was hardened so partial cached `result_metrics` summaries no longer mask full local `qlib_results_enhanced.json` artifacts, WSL-style workspace paths are converted when searching local artifacts, and the frontend accepts Multi-Alpha/absolute-return/trade-diagnostic payloads as valid enhanced data.

## Paper Trading v2 UI Implementation Update - 2026-04-26

- Added detailed design doc `docs/architecture/paper_trading_v2_ui_design.md` and a new standalone frontend route tree under `/paper-v2`; legacy `/paper-trading/*` remains separate and is not the v2 authority.
- The new UI covers StrategyPackage creation/promotion from QE experiment or QE evolution loop, selectable package metrics/model state, package-based selection, dynamic multi-package aggregation, single-package selection-to-portfolio creation, portfolio lifecycle, readiness, run-day, replay/reset, execution policy activations, ledger review, performance, and manual model/HMM maintenance.
- Added shared Paper v2 frontend API/types/components in `frontend/src/lib/paper-v2` and `frontend/src/components/paper-v2`, including backend fail-fast error parsing that surfaces `detail.error_code`, `detail.message`, and `detail.context`.
- Added Paper Trading v2 Sidebar entries under `/paper-v2`, `/paper-v2/packages`, `/paper-v2/selection`, `/paper-v2/portfolios`, and `/paper-v2/model-hmm`.
- Added `/api/v1/paper-v2/portfolios/{portfolio_id}/cash-ledger` and repository support so the v2 Ledger UI can show orders, fills, cash ledger, positions, snapshots, run events, and persisted errors.
- Existing 8001 backend should not be restarted for UI work; use a temporary backend port for API smoke or browser validation.
- Verification completed: frontend `npm run build` passed, relevant backend pytest suite passed with 94 tests, temporary backend smoke on port 8011 returned 200 for core v2 endpoints, and temporary frontend production-start route smoke on port 3011 returned 200 for all `/paper-v2` routes after a clean rebuild. `next lint` remains interactive due missing project ESLint config, and browser click automation was not run because Playwright is not installed.

## Paper Trading v2 UI Chinese Localization / E2E Plan Update - 2026-04-26

- Localized the `/paper-v2` route tree and shared Paper v2 UI components into Chinese while keeping backend enum/API field names such as `DB_HISTORICAL`, `TDX_REALTIME`, `manifest_sha256`, and status codes visible where traceability requires them.
- Added `docs/architecture/paper_trading_v2_ui_e2e_validation_plan.md` for UI-based validation of the first three QE experiment sources, recent-10-trading-day DB replay, TDX realtime minute smoke, business-value metrics, fail-fast negative tests, and next-trading-day realtime validation.
- Frontend `npm run build` now completes successfully after the current workspace fixes; Paper v2 routes compile and type-check in the full Next.js production build.

## Paper Trading v2 UI E2E Validation Update - 2026-04-26

- Added a Paper-v2-specific Playwright config at `frontend/playwright.paper-v2.config.ts` plus `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`; this avoids overwriting the shared `frontend/playwright.config.ts` that may be used by other active test work.
- The E2E config starts a temporary frontend on port 3011 and proxies `/api/v1/*` to `PAPER_V2_API_BASE` through `PAPER_V2_API_PROXY_TARGET`, so browser validation does not require restarting the existing 8001 backend and does not hit CORS preflight failures on temp ports.
- Paper v2 UI validation now passes with a temporary backend on 8011: StrategyPackage display/readiness fail-fast, Selection Center fail-fast for missing `selection_runtime`, multi-package research boundary, portfolio creation fail-fast for unavailable V24 runtime/model path, model/HMM page loading, structured negative API errors, and TDX minute HTTP endpoint smoke.
- The first three QE experiment packages (`qe_20260416_002701`, `qe_20260413_084216`, `qe_20260416_082012`) can be created and enabled for selection, and their IC/RankIC/annual-return/max-drawdown summaries display in the UI.
- Full value validation (successful selection, portfolio creation, 10-trading-day replay, ledger/performance profit/loss) remains blocked because these packages currently lack authoritative `selection_runtime` score artifacts and their paper execution policy references V24 assets/torch, which are intentionally not enabled or installed.

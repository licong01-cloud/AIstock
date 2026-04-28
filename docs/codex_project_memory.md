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
- Trading Core v2 / Paper Trading v2 restart-safe continuation plan is recorded in `docs/architecture/paper_trading_v2_remaining_execution_plan.md`. Before continuing this work after a restart, read that document. The older "do not install Torch / V25 under development" note has been superseded for this workstation by the 2026-04-27 local PyTorch verification below; still do not install or change runtime dependencies without user direction.

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
- Selection Center now stores `package_ids` on `selection.run`, supports listing selection runs, and can create a Paper v2 portfolio from a successful single-package selection run. The created portfolio is linked back through `selection.paper_portfolio_link` with source trace only; Paper v2 must regenerate/load authoritative live selection artifacts per trading day and must not reuse raw `selection_scores` as signal input.
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
- UI/manual page鑱旇皟 remains deferred per user direction; use a non-8001 temporary backend port for API validation.

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

## Paper Trading v2 Realtime/Replay Session Update - 2026-04-27

- Trading Core v2 schema initialization is applied through `backend/db/init_trading_core_v2_schema.py`; business services still do not run DDL implicitly.
- Paper v2 now has durable trade sessions for `REPLAY_ONLY`, `LIVE_ONLY`, and `CATCHUP_THEN_LIVE`, including an `auto_switch_to_live` option that normalizes replay requests to catch-up-then-live with explicit `DB_HISTORICAL` and `TDX_REALTIME` source roles.
- Live sessions process observed TDX minute bars incrementally with persisted `order_execution_state`, intraday snapshots, idempotent fill/event IDs, and session locks. A tick with no new bar records `LIVE_WAITING_FOR_BAR`, not success.
- `V25_TWO_STAGE` is declared live-capable only through the explicit streaming adapter: historical replay still requires 240 bars, while live mode may start with one observed bar and must persist the generated 240-step plan. Missing Torch/model/context still fails fast; no TWAP/daily fallback is allowed.
- Paper v2 has an opt-in session scheduler exposed under `/api/v1/paper-v2/session-scheduler/*`. It is not auto-started on development ports unless explicitly enabled, to avoid a temporary 8011/8012 backend advancing durable sessions visible on production 8001.

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

## Selection Score Artifact Runtime Update - 2026-04-26

- Added `strategy_pkg.selection_score_artifact` and StrategyPackage selection artifact service/repository so Selection Center can load persisted ranked model scores without depending on V24/V25 minute execution adapters or Torch.
- Correction: QE backtest `pred.pkl` artifacts are not authoritative current selection data. They are diagnostic/backtest-only (`metadata.source_type=qe_mlruns_pred_pkl_v1`, `authority_scope=diagnostic_backtest_only`) and `StrategyPackageRuntime` rejects them for Selection Center/Paper v2.
- Authoritative `/selection-artifacts/generate` now runs live/latest-data QE model inference: it reconstructs a temporary StrategyPackage inference workspace, recomputes factors from DB-backed current data, applies the saved QE LGB model in WSL/Qlib, persists `source_type=live_qe_model_inference_v1`, and records model/factor/runtime trace metadata.
- Strategy Package API keeps `/selection-artifacts/generate-diagnostic-backtest` for explicit diagnostics from QE `pred.pkl`; this endpoint is intentionally separate and its output is not accepted by authoritative runtime.

## HMM Three-Version Comparison Update - 2026-04-27

- The active HMM comparison set is intentionally limited to three DB-visible versions: original baseline `HMM_BASELINE_ORIGINAL_w3_raw_unfixed__n3_diag_rw3_nozscore`, same-parameter repaired `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`, and repaired w5/zscore `HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore`.
- The obsolete pre-fix w5/zscore config/snapshot was removed after confirming no QE, Selection Center, or Paper v2 runtime references. HMM runtime assets under `backend/data/hmm_models` remain ignored by Git.
- HMM snapshot display labels are stored in `model_train_snapshots.metrics_json.snapshot_display_name` and exposed by `/api/v1/hmm-training` as `display_name`; Paper v2 selectors render the readable label instead of UUID/date pairs.
- QE unified config builders now treat HMM as a task-level runtime policy for auto evolution: the selected snapshot/preset is carried through every loop, reviewer output is not allowed to change it, and the Multi-Alpha auto-evolution branch also builds and passes the same HMM config to `MultiAlphaEngine`.
- QE HMM adapter support is covered for single experiments, standard auto-evolution loop config, strategy/custom evolution, Multi-Alpha single experiments, and Multi-Alpha custom/auto loop config. Auto-evolution runtime execution was intentionally not smoke-tested in this change.
- `StrategyPackageRuntime` also rejects raw `runtime_config.selection_scores` and manifest embedded `strategy_config.selection_runtime.scores/scores_path`; unit tests seed authoritative artifacts instead of bypassing live/latest-data inference.
- Strict StrategyPackage live inference now enables `AISTOCK_STRICT_INFERENCE=1` and fails rather than padding/truncating features, filling missing features with zero, using earlier factor dates, tolerating insufficient data windows, or accepting missing fundamental/moneyflow/sector DB data.
- Verified successful DB_HISTORICAL live inference selection artifacts and single-package Selection Center runs for the first three QE packages on 2026-04-24: `qe_20260416_002701`, `qe_20260413_084216`, and `qe_20260416_082012`. Paper execution remains separately gated by execution-policy/runtime readiness.
- V25 execution remains a separate minute-execution concern. The current V25 two-stage executor imports PyTorch directly, so Windows-side V25 execution requires Torch unless V25 is exported to a non-PyTorch inference format or executed out-of-process in the QE/WSL environment.
- V25 execution remains a separate minute-execution concern. The current V25 two-stage executor imports PyTorch directly, so Windows-side V25 execution requires Torch unless V25 is exported to a non-PyTorch inference format or executed out-of-process in the QE/WSL environment.

## QE Blacklist Metadata Parameter Filtering Update - 2026-04-26

- Fixed QE config generation so industry blacklist display metadata (`sector_blacklist`, `sector_blacklist_enabled`, `sector_blacklist_snapshot`, `blacklist_enabled`) is filtered out before Qlib strategy kwargs are validated or serialized. The actual executable blacklist restriction continues to be represented by the generated `stock_pool`; snapshot fields remain persisted only for UI/detail traceability.
- Added a unified-engine regression test covering `ScoreWeightedTopkStrategyV2` with blacklist metadata in `custom_params`, ensuring these fields do not enter generated strategy YAML and do not trigger unsupported-parameter failures.

## QE Compose Correlation Removal UI Update - 2026-04-26

- QE compose correlation analysis now lets users remove either factor directly from each high/medium-correlation pair row. The action updates the selected factor set and prunes correlation pairs immediately, so users no longer need to return to the factor-selection table to deselect redundant factors.

## Paper v2 UI/Selection Portfolio Validation Update - 2026-04-27

- Paper v2 frontend now has Chinese StrategyPackage source dropdowns, single-package selection with explicit live-inference artifact generation, HMM config/snapshot selectors, TopK 20 default/50 max, historical run detail display, multi-run aggregation, watchlist import, and single-package Paper v2 portfolio startup controls.
- Added `/api/v1/paper-v2/trading-days/defaults` so non-trading/pre-open UI defaults use the latest data-ready trading day from `stk_limit` refresh audit instead of blindly using wall-clock today; this prevents weekend/pre-open defaults like 2026-04-27 when DB historical data is only ready through 2026-04-24.
- Playwright Paper v2 E2E now validates the UI through temporary backend/frontend ports: QE package display, successful live-data selection for the first three packages, watchlist import with reference prices, historical multi-run union aggregation, portfolio creation fail-fast for unavailable V24/V25 execution assets, structured negative errors, and TDX minute endpoint reachability.
- Paper portfolio replay remains blocked for the current QE packages because their minute execution policy references unavailable V24 assets; V25 adapter/QMT/Shadow/live trading were intentionally not implemented in this phase.

## Local GPU / PyTorch Runtime Note - 2026-04-27

- Current workstation GPU for AIstock V25 execution validation: NVIDIA GeForce RTX 5080, NVIDIA Blackwell generation.
- RTX 5080 requires a PyTorch build with CUDA 12.8 or newer for GPU execution; CUDA 12.6/12.4-era PyTorch wheels are not suitable for authoritative V25 GPU validation.
- Preferred local installation target for Paper Trading v2 V25 Windows backend execution is PyTorch CUDA 12.8 (`cu128`) in the same Python environment used by `backend.main`.
- After installing torch, verify with `python -c "import torch; print(torch.__version__); print(torch.version.cuda); print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'NO_CUDA')"` before running Paper v2 V25 replay.
- Verified user AIstock conda environment on 2026-04-27: `torch 2.11.0+cu128`, CUDA runtime `12.8`, `torch.cuda.is_available() == True`, device `NVIDIA GeForce RTX 5080`.

## Paper v2 Session Implementation Update - 2026-04-27

- Added detailed design doc `docs/architecture/paper_trading_v2_realtime_replay_session_design.md` and started backend implementation of durable Paper v2 trade sessions.
- Implemented session models, schema DDL, repository persistence, session APIs, and `REPLAY_ONLY` session ticking through the existing strict historical replay path.
- Split execution algorithm capabilities into historical/live semantics. V25 remains historical/full-day capable with 240-bar requirements but is explicitly not declared real-time safe; live V25 now fails with `ALGO_REALTIME_UNSUPPORTED` instead of requiring 240 bars at open or falling back to another algorithm.
- `LIVE_ONLY` and `CATCHUP_THEN_LIVE` sessions are intentionally fail-fast until incremental per-minute execution state and source-role split are implemented; they do not use the closed-day runner as a fake live path.
- Added source-role minute-feed primitives on `PaperV2MinuteMarketDataProvider`: completed-day DB loading, observed intraday TDX bars, cursor-based new-bar loading, and latest common live bar time. These methods reject DB/TDX role misuse rather than falling back.
- Paper v2 exposes session capability diagnostics so the UI can tell which modes are genuinely startable. `REPLAY_ONLY` can be startable when portfolio/source/policy match; `LIVE_ONLY` and `CATCHUP_THEN_LIVE` remain not startable until the incremental executor and source-role split are fully implemented.

## Production Port Safety - 2026-04-27

- Treat FastAPI backend port `8001` as the user's production backend. Codex must not stop, restart, or otherwise manage the `8001` service during development/testing unless the user explicitly asks in that turn.
- Use development backend ports `8011` or `8012` for validation. Before starting a dev backend, check whether the target port is already occupied.
- Use development frontend ports `3011` or `3012` for UI validation. Before starting a dev frontend, check whether the target port is already occupied.
- After code changes are complete, notify the user that production services may need a user-managed restart; do not restart production services directly.

## Backend Runtime Logs - 2026-04-27

- `backend/main.py` installs rotating FastAPI/Python file logging under `backend/logs/`: `aistock.log` receives INFO+ root/application logs and `errors.log` receives ERROR+ logs; `/api/v1/quantevolver/evolution/system/logs` tails these files for UI/API diagnostics.
- Use `backend/logs/aistock.log` and `backend/logs/errors.log` first when diagnosing AIstock FastAPI backend runtime issues, scheduler startup, QE router/service errors, ingestion warnings, and propagated `aistock.*` logger output.
- `backend/logs/` is not the complete log universe for AIstock: TDX Go, Next.js, RD-Agent workspaces, dispatch task logs, QE workspace `run.log`, Qlib subprocess output, and remote compute nodes can have separate logs/artifacts. Check those source-specific logs when backend logs do not contain the failure.
- `backend/logs/` is ignored by Git and should stay local runtime state only; old rotated logs can be deleted when they grow large, while active current logs should not be removed from a running backend process.

## Asset / Program Framework Separation Rule - 2026-04-27

- Program framework changes and persisted/trained asset changes must be treated as separate change classes. During framework development, Codex must not silently modify saved assets.
- Protected assets include StrategyPackage frozen manifests and manifest hashes, validated execution policies, model asset paths, model weight files, QE experiment artifacts, selection score artifacts, HMM snapshots/coefficients, and any DB row or filesystem file that represents a persisted strategy/model/data asset.
- Any asset modification requires explicit user confirmation in the current task, an impact analysis listing affected asset IDs/paths/hashes, a backup or reproducible rollback path, and targeted validation. Do not bundle asset edits into infrastructure/API/UI refactors.
- Framework code changes that alter how existing assets are interpreted or executed are high-risk global behavior changes even if the assets themselves are untouched. Execution adapters, selection runtime, risk/ledger/fee logic, factor inference, and model-loading behavior must be called out explicitly.
- V25-specific note: changing `V25_TWO_STAGE` adapter/capability code changes the global interpreter for existing V25 policies, while changing V25 `.pt` weights, catalog asset paths, or validated policy JSON changes assets. Keep those paths separate and require stricter validation for either kind of change.

## GitHub Traceability Rule - 2026-04-27

- After user confirmation on 2026-04-27, Codex development changes in this repository must be committed and pushed to GitHub so every modified file is traceable.
- Do not include unrelated pre-existing dirty worktree files in a commit. Stage only the files modified for the current task unless the user explicitly asks to commit all workspace changes.
- If a follow-up memory/process note is added because of a user instruction, commit and push that note as well.

## Codex Skill Notes - 2026-04-27

- Created local Codex skill `develop-minute-execution-algo` at `C:\Users\lc999\.codex\skills\develop-minute-execution-algo` and a Git-tracked mirror at `.codex/skills/develop-minute-execution-algo` so future intraday minute execution algorithms follow the V25-derived standard contract.
- The skill requires asset/program separation, core/adapter layering, explicit historical/realtime capabilities, market-state vs data-error taxonomy, no silent fallback, QE/Paper v2 consistency, and targeted tests before reporting completion.
- Use this skill for future V24/V25/V26-style execution strategy work, QE Qlib minute execution helpers, Paper Trading v2 execution adapters, and any suspend/limit/pre_close/minute-bar execution-policy wiring.


## Minute Execution Standard / V25 Core Update - 2026-04-27

- Added `docs/architecture/minute_execution_algo_standard_contract.md` as the standard contract for all intraday minute execution algorithms. It requires one logical core with QE/Paper adapters, split historical/live capabilities, explicit market-state taxonomy, no silent fallback, plan/state persistence, and a V25 regression matrix.
- Added shared `backend/execution_algos/v25_core.py` for V25 market-state classification and 240-bar two-stage plan generation with 88.79% / 11.21% early/late weights. The core is independent from Paper v2, Qlib, DB, and API objects.
- Updated Paper v2 `V25_TWO_STAGE` adapter to use the shared core, distinguish suspension/limit business states from data/config/model errors, support realtime one-observed-bar streaming with persisted plan metadata, keep historical 240-bar requirements, and require explicit `day_features` unless an audited diagnostic flag allows zero defaults.
- Updated the execution algo adapter so V25 can handle suspended bars itself and advance its execution step; non-market-aware algorithms retain the existing suspended-bar skip behavior.
- Verified with small scenario tests covering V25 plan weights, suspend-driven missing prev_close, data-error missing prev_close, limit-up buy block, P0 buy-at-down-limit, one-bar realtime streaming, historical 240-bar requirement, and day_features fail-fast.
- QE V25 workspace generation now passes the generated suspend artifact into `TailTWAPWithV25TwoStageStrategy` when signal suspension filtering is enabled; the template skips artifact-confirmed suspended orders with explicit `suspended_by_suspend_d` instead of entering V25 plan generation and failing on missing `prev_close`.
- Synchronized the AIstock V25 QE helper template to `F:\Dev\RD-Agent-main\rdagent\scenarios\qlib\experiment\factor_template\tail_twap_v25_strategy.py` because ConfigComposer treats that RD-Agent template path as the authoritative QE helper source when present.
- Trading Core now emits `OrderEventType.NO_FILL` for market-aware V25 no-fill minutes, preserving explicit reasons such as limit-up buy blocks without converting normal market constraints into execution failures.
- Verified and hardened QE V25 suspend filtering so `filter_suspended_on_signal`, `suspend_filter_file`, and `suspend_filter_strict` are emitted both on the outer signal strategy and the V25 `inner_strategy`. A real 2024-11-01..2024-11-05 `suspend_d` artifact confirmed `688143.SH` is filtered before selection and recognized by `TailTWAPWithV25TwoStageStrategy._is_artifact_suspended()` in WSL; a tiny WSL `qrun_limit_minute.py --pred-backtest` V25 smoke completed without the prior `prev_close=NaN` failure.

## Paper v2 Gap Closure / Runtime Profile Audit Design - 2026-04-27

- Added `docs/architecture/paper_trading_v2_gap_closure_detailed_design_20260427.md` as the next-phase detailed design that consolidates existing Paper v2 plans and the latest StrategyPackage semantic correction.
- StrategyPackage should be treated as a frozen research alpha asset for factor set, model family/assets, QE lineage, and metrics; Paper v2 runtime choices such as selection TopK, suspend filtering, industry blacklist, HMM snapshot/preset, daily/rebalance policy, minute execution policy activation, data-source roles, replay/live mode, and reset behavior must be mutable only through versioned/audited runtime configuration.
- Runtime changes must create traceable versions/hashes/activations and be copied into selection runs, paper runs, and trade sessions; they must not mutate frozen manifests, validated policies, model/HMM assets, QE artifacts, or selection artifacts.
- The next implementation must include code-audit/static-scan gates and full backend-driven UI E2E validation on development ports 8011/8012 and 3011/3012, without restarting production backend port 8001.

## Paper v2 Baseline Alignment - 2026-04-27

- Added `docs/architecture/paper_trading_v2_baseline_alignment_20260427.md` after checking current code, historical Paper v2 docs, frontend routes, schemas, and tests against the new gap-closure design.
- Alignment conclusion: the gap-closure design matches the current code when read as implemented baseline plus next-phase gaps. Implemented pieces include StrategyPackage persistence/status/metrics/policies/model state/artifacts, Selection Center runtime filters/HMM/multi-package aggregation/watchlist, Paper v2 ledgers/replay/reset/session framework, V25 core/adapter/capabilities, and the Paper v2 UI route tree.
- Explicit remaining gaps are still runtime profile version/audit tables and APIs, UI use of session capability diagnostics, V25 day_features provider for real Paper v2 V25 runs, and full backend-driven UI value validation on dev ports. Current V25 code correctly fails without `day_features`; no default day-feature fallback is allowed in authoritative Paper v2.
- Baseline tests passed: `pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider` -> 125 passed.

## Paper v2 WSL UNC / V25 Day Features Update - 2026-04-27

- `develop-minute-execution-algo` is for creating or changing intraday minute execution strategy implementations and their standard contract; do not use it as a generic Paper v2 bug-fix or architecture-refactor skill.
- Removed Windows-side runtime probing/reading of WSL UNC paths from StrategyPackage model asset resolution, manual factor validation result handling, and stock-pool generation. WSL-origin outputs must be explicitly copied by a WSL subprocess into Windows-visible temp/cache paths, or accessed inside WSL through `wsl -d <configured distro>`.
- WSL distro selection for stock-pool WSL commands is explicit through `AISTOCK_WSL_DISTRO` or `QLIB_WSL_DISTRO`; no hard-coded `Ubuntu` distro is used in the updated stock-pool sync/generation paths.
- Added `DbV25DayFeatureProvider` for Paper v2 V25 execution context. It builds `paper_v2_v25_day_features_v2` from audited previous-trading-day DB data and injects `market_context.day_features` only when the active policy is `V25_TWO_STAGE`.
- Seeded local `market.dataset_date_refresh_audit` rows from existing tables for `kline_daily_raw`, `daily_basic`, `stock_moneyflow_ts`, `sector_data`, and `index_daily`, so V25 day-feature readiness can be checked against explicit audit rows instead of assuming table presence.
- `allow_default_day_features` remains diagnostic-only inside the V25 algorithm implementation; StrategyPackage/Paper v2 validation rejects it for authoritative Paper Trading v2 policies.

## HMM Daily Coefficient Generation Update - 2026-04-28

- Added `docs/architecture/hmm_daily_coefficient_generation_design_20260428.md` defining the Paper v2 / Selection Center HMM daily prediction contract: HMM training remains separate from daily coefficient generation; each `effective_trade_date` coefficient must be generated from a strictly earlier completed `as_of_trade_date`.
- Added HMM Training API endpoints under `/api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/{preview,generate}`. Generation requires `confirm_text == snapshot_id`, writes additive coefficient artifacts beside `models.json`, and refuses to overwrite mismatched existing artifacts.
- Extended `scripts/precompute_hmm_coefficients.py` with `output_trade_date` remapping so forward-filtered as-of coefficients can be emitted for the next trading day without reading future data.
- Updated `/paper-v2/model-hmm` with a Chinese daily coefficient generation card and UI E2E coverage. Selection Center/Paper v2 still consume only generated artifacts and fail fast when HMM coefficients do not cover the requested trade date.
- Validation completed on development ports 8012/3012 without restarting production port 8001: backend Paper v2/Selection/HMM tests passed and Paper v2 Playwright UI suite passed.

## HMM Daily Coefficient Async Job Update - 2026-04-28

- Replaced the Paper v2 HMM page's long synchronous daily coefficient generation call with a durable async job flow to avoid Next.js dev proxy `socket hang up` on WSL/conda generation.
- Added `model_train_daily_coefficient_jobs` via `backend/migrations/hmm_daily_coefficient_jobs_20260428.sql` and `backend/db/init_quant_schema.py`; each job records PIT dates, preset, plan, status, result artifact hash, and fail-fast error context.
- Added `/api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/jobs`, `/api/v1/hmm-training/daily-coefficients/jobs/{job_id}`, and snapshot job-list APIs. The old synchronous `/generate` endpoint remains for direct diagnostics, but the UI uses job creation plus polling.
- Updated `/paper-v2/model-hmm` to show daily coefficient job status, artifact result, SHA256, and job audit table without exposing raw JSON content; generated HMM model/coefficients assets remain runtime assets and are not committed.
- Applied the new schema on the local dev database and validated with backend tests, frontend type/build checks, and full Paper v2 Playwright UI E2E on 8012/3012. Production port 8001 was not restarted.

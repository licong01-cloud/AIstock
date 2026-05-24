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

## Current Development Standard - 2026-05-23

- Canonical project development standard is `docs/standards/aistock_development_standard_v1.5_20260523.md`, with machine catalog `docs/standards/aistock_development_standard_v1.5_20260523.yaml`; v1.4 is archived under `docs/standards/archive`.
- New efficiency governance: classify each task as T0/T1/T2/T3 before loading context; do not inject full standards, full designs, full memory, or unrelated module prompts by default. Use compact context packs, design acceptance indexes, RTK/log summaries, and MCP summary mode unless full evidence is required.
- Same-module related issues should default to a batch worktree / batch branch when safe. Keep per-issue GitHub links, commits, closure evidence, and verification mapping.
- Feature/fix handoffs must now report `production_ddl_gate`, `production_frontend_dependency_gate`, and `production_backend_dependency_gate`; dependency-file changes require production runtime dependency sync and build/import verification before restart-readiness claims.
- Issue-fix automation entrypoint: when a user asks to fix, process, triage, batch, finish, close, or sync an AIstock BUG/GitHub Issue, trigger `.codex/skills/fix-aistock-issue` and run `scripts/aistock_issue_workflow.py` first; `scripts/issue_flow.py` remains the lower-level helper and should not replace the high-level workflow entrypoint.

## Engineering Rules for Codex

- Do not modify AGENTS.md unless explicitly requested.
- Prefer AGENTS.override.md and docs/codex_project_memory.md for Codex-specific project notes.
- Before making structural changes, inspect the relevant router, service, data access, and frontend route together.
- Treat trading-related code as high-risk: preserve existing behavior unless the user explicitly requests a behavioral change.
- Distinguish research, paper trading, and real execution paths. Do not assume live-trading safety.
- Prefer small, reviewable changes with tests or clear manual verification steps.
- For project-wide searches, prefer rg / rg --files.
- Database schema standard: every new DB table and every new DB column created or modified by Codex must have explicit PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN` metadata in the reviewed DDL or migration. Comments should be program-readable, describe business semantics, and mention units/source/quality semantics when relevant. Add tests or a review check to prevent uncommented fields.
- QE production isolation standard: while building the QE archive and future QE automation, new ingestion, archive, optimizer, or agent code must not affect the current QE production runtime by default. Runtime hooks must be disabled by default or explicitly gated, no production backend `8001` restart unless the user asks, and validation should use dev ports only.
- Testing standard: future high-risk AIstock features must define tests at design time, include appropriate L0-L5 validation, produce run records, and enforce coverage gates for new/changed code. Backend Python coverage should include line and branch coverage; QE data completeness, warehouse/archive, trading execution, cost/ledger, HMM, and cleanup gates are high-risk modules that need explicit unit/integration/business-oracle tests in addition to UI E2E.
- Standards document location: project development standards must live under `docs/standards`, not `docs/architecture`. The active human-readable standard and same-version machine-readable YAML stay in `docs/standards`; older versions move to `docs/standards/archive`. Architecture documents may explain implementation of guardrails but must not become a competing standards source.
- Issue-fix / parallel-work standard: `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` is the active workflow reference for bug triage, fix ownership, `allowed_write_scope`, one-issue/one-branch/one-worktree isolation, and integrator-controlled shared files. For bug-fix work in multi-window AIstock sessions, read this standard before editing; future automation can mirror its fields in BUG JSON, MCP, GitHub Issues, and CI guardrails.
- Codex issue-fix skill standard: short prompts such as "按规范修复 BUG-XXX" must use `.codex/skills/fix-aistock-issue` and `scripts/aistock_issue_workflow.py start/finish/triage-p0` before manual exploration, so later Codex windows get compact context packs, explicit scope, selected validation, and production gates automatically.
- Design implementation compliance standard: any implementation based on a design, issue closure criteria, or explicit user requirements must perform a pre-merge item-by-item design compliance review. Do not deliver or report unapproved simplified, subset, POC, placeholder, mock-only, backend-only, or partial-loop implementations as complete; stop and ask for user approval when scope must change.

## Multi-Codex Parallel Development Guardrails - 2026-05-06

These rules are mandatory for all future Codex windows to avoid losing work when multiple agents develop AIstock in parallel. 2026-05-19 user reaffirmation: every future new feature, bugfix, or non-trivial documentation task must default to a new branch plus a separate worktree created from latest `origin/main`; do not develop in a physical directory that another window is using, and do not switch the branch of a shared worktree to start unrelated work.

- Treat the root worktree `F:\Dev\AIstock` / branch `main` as a sync and rescue baseline, not a normal development workspace. Do not start new feature work on a dirty `main`.
- Each Codex or Claude Code window must use its own branch and its own worktree under a path such as `F:\Dev\AIstock_worktrees\<task-name>`, created from the latest `origin/main`; use `feature/<short-task>-YYYYMMDD` for features, `bug/BUG-XXX-<short-slug>` for bugfixes, and a short `docs/<short-task>-YYYYMMDD` or temporary main worktree for documentation-only updates.
- At task start, run a Git preflight: `git status --short --branch`, `git branch --show-current`, and `git log --oneline -5`. If the branch is `main`, has `M`/`MM`/`??` changes, or is ahead/behind unexpectedly, stop and create a clean worktree or make a rescue backup before editing.
- Never run `git pull --rebase`, `git merge origin/main`, `git reset --hard`, `git checkout -- .`, or `git clean -fd` in a dirty shared worktree unless the user explicitly approves that exact destructive/synchronizing action after a backup.
- Keep one task per branch and one feature per commit series. Do not mix QE runtime changes, Paper v2 changes, HMM experiments, validation-center work, and local-data UI changes in the same commit unless the user explicitly requests a single integrated change.
- Commit or export patches before handing work to another Codex window. Handoff notes must include branch name, commit hash, changed file list, tests run, untracked files, push status, and whether production port `8001` was touched.
- Use clean integration branches from `origin/main` for combining parallel work. Cherry-pick feature commits in logical order, validate after each functional group, and prefer the already-pushed/validated `origin/main` version over older local duplicate commits.
- Before any risky synchronization or conflict resolution, create a rescue snapshot: backup branch at current `HEAD`, `git bundle create ... --all`, binary patches for staged/unstaged changes, `git status --porcelain=v1 -uall`, and a copy/list of untracked files outside the repo.
- Keep temporary outputs and large experiment artifacts out of the repo by default (`.codex_tmp/`, `.coverage`, `catboost_info/`, Qlib validation CSV/Bin/PKL artifacts, one-off diagnostics). Store them under `F:\Dev\AIstock_backups` or `F:\Dev\AIstock_artifacts` unless they are deliberately curated validation records.
- When a local branch and `origin/main` both contain similar features, do not blindly preserve the local commit. Compare final file contents and validation records; adopt the most complete validated version, then re-apply only true missing functionality as a small patch.


## Production Root Sync Rule - 2026-05-06

The 2026-05-06 production/GitHub reconciliation is documented in `docs/operations/prod_github_sync_conflict_resolution_20260506.md` and the detailed audit in `docs/operations/prod_reconcile_audit_20260506.md`.

Mandatory future rules:

- `F:\Dev\AIstock` is the production runtime/sync target, not a normal development worktree. Do not perform feature development there.
- Start every new coding task, feature, bugfix, or substantial documentation update from a clean worktree created from latest `origin/main`, for example `F:\Dev\AIstock_worktrees\<task-name>` on branch `feature/<short-task>-YYYYMMDD`, `bug/BUG-XXX-<short-slug>`, or another task-scoped branch.
- Before editing, always run `git status --short --branch`, `git branch --show-current`, and `git log --oneline -5`. If the worktree is dirty, on `main`, or diverged, create a backup or clean worktree before editing.
- Never leave developed functionality as untracked files. New frontend pages, backend modules, scripts, configs, migrations, tests, docs, and validation records must be committed, or explicitly classified as temporary artifacts with a backup path.
- Before synchronizing `F:\Dev\AIstock` to GitHub, classify local-only files first. Functional local-only files must be copied into a clean reconcile branch and committed before `F:\Dev\AIstock` is reset or cleaned.
- Temporary artifacts must be quarantined under `F:\Dev\AIstock_backups` or `F:\Dev\AIstock_artifacts`, not kept in the production root and not silently deleted.
- Do not run `git pull`, `git merge`, `git reset --hard`, `git checkout -- .`, or `git clean -fd` in `F:\Dev\AIstock` unless all of the following are true: a backup exists, GitHub already contains all preserved functionality, local-only files are classified, and the operation is an explicit production sync procedure.
- Code synchronization and runtime activation are separate. Do not restart or reload production port `8001` unless the user explicitly requests it; use non-production ports for validation.
- Every handoff after code changes must include branch, commit hash, changed files, tests/validation results, untracked file status, push status, and whether production `8001` was touched.
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
- UI/manual page閼辨棁鐨?remains deferred per user direction; use a non-8001 temporary backend port for API validation.

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

## QE Analysis Documentation Update - 2026-05-02

- Store all future QE/quant analysis documents under `docs/analysis`; use fixed-width aligned text tables for user-facing table output.
- For `qe_20260501_011054_c90a`, Loop19+ current-scope audits exclude capital-size/capacity/impact assumptions until a dedicated capital experiment exists.
- Added read-only P0 audit tooling at `scripts/qe_loop_p0_audit.py` and mirrored it into the `qe-evolution-diagnostics` skill for future reuse. It recomputes IC/RankIC from `pred.pkl`/`label.pkl`, validates Qlib signal artifacts, report/account return consistency, label-horizon date gaps, top-bucket conversion, yearly segments, and static future-leakage risk.

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

## AIstock Testing / Version Management System Design - 2026-04-29

- Added `docs/architecture/aistock_testing_version_management_system_design_20260429.md` to define the target result-oriented automated testing and version-management system for AIstock.
- The design standardizes L0-L5 validation levels, reusable test matrices, test run records, UI/API/DB/log cross-validation, protected-asset safety, no-silent-fallback guardrails, development-port isolation, and phased rollout from local validation to release candidate gates.
- Recommended the first stable internal release process: complete the testing baseline first, then use SemVer `0.x.y`, Conventional Commits, `VERSION`, `CHANGELOG.md`, and release candidate reports before tagging a future `v0.1.0`.
- Updated the testing design to make local execution authoritative: AIstock should not rely on cloud CI/deployment for business validation because the environment depends on local DB, TDX, WSL/Qlib/RD-Agent, GPU/Torch, data, and strategy/model assets. The automation pipeline should live in the AIstock repository itself (`noxfile.py`, validation scripts, Semgrep rules, Playwright tests, `tests/aistock_validation`, and Codex skills) so code, tests, business oracles, and release gates version together. Cloud checks, if ever added, are optional lightweight static checks only.

## Paper v2 + Selection Center Validation Bootstrap - 2026-04-29

- Selection Center is included in the first Paper Trading v2 validation rollout because the backend and UI flows are strongly coupled.
- Added local-only validation entry points with `noxfile.py`, `scripts/aistock_validate.py`, `.pre-commit-config.yaml`, `.semgrep/aistock/guardrails.yml`, `requirements-dev.txt`, and `tests/aistock_validation`.
- Installed `nox` and `pytest-html` into the local `AIstock` conda environment; no external tool source trees were cloned into the AIstock repository.
- First-stage commands validated locally without starting or restarting any backend/frontend service: `conda run -n AIstock python -m nox -s l0` passed, and `conda run -n AIstock python -m nox -s paper_v2_backend` passed with 103 tests.

## Paper v2 + Selection Center Validation L3 Closure - 2026-04-29

- Completed the first full local L3 validation of Paper v2 + Selection Center with UI included by default in `nox -s paper_v2_l3`; the run used backend port 8012, frontend port 3011, and TDX port 19080 without touching production backend port 8001.
- Hardened the local validation runner: `scripts/aistock_validate.py services` now fail-fast probes the FastAPI `/openapi.json` endpoint and the TDX realtime minute endpoint before Playwright starts, and `paper_v2_ui` wires this check into the UI E2E flow.
- Fixed validation-tool robustness: Nox no longer passes unsupported `cwd` to `Session.run`, the Codex skill validator path is resolved from `CODEX_HOME` or the user home directory instead of a workstation hardcode, and Playwright can explicitly skip its web server only when reusing an already-running frontend.
- Verified commands: `nox -s l0` passed with 0 HIGH findings and 13 existing MEDIUM review findings; `nox -s paper_v2_backend` passed with 103 tests; `nox -s paper_v2_ui` passed with 12 Playwright tests; `nox -s paper_v2_l3` passed L0 + backend + UI in one run.
- Evidence record: `tests/aistock_validation/history/paper_v2_selection_center/20260429_015310_l3_paper-v2-selection-center-l3-regression.md`. No StrategyPackage manifests, model weights, HMM snapshots, validated execution policies, QE/RD-Agent artifacts, or source strategy assets were modified by this framework-validation change.

## Paper v2 Trading-Hours Catchup-To-Live Validation - 2026-04-29

- Added and validated the `paper_v2_live` local validation path for trading-hours Paper v2 sessions. It creates an isolated Paper v2 portfolio, replays completed `DB_HISTORICAL` minutes, switches to `TDX_REALTIME`, and accepts live waiting states such as `LIVE_WAITING_FOR_BAR` as a bounded successful tick state instead of waiting for a terminal session.
- Paper v2 historical replay now derives existing-position equity from the first observed DB minute close and derives held-position snapshots from the latest available minute close when the held symbol had no same-day order market-data load. These are explicit real-data mark-to-market paths with run events, not default-price fallbacks.
- Live Paper v2 sessions now inject the previous trading day as `selection_artifact_config.cutoff_date` for current-day signal generation. Live selection artifacts keep the target `trade_date` while recording `cutoff_date`, `score_trade_date`, and `reference_price_trade_date`, so current-day live selection uses latest completed DB data without pretending same-day daily factors exist.
- Synced real Tushare `stk_limit` data for 2026-04-29 after the local DB had an audit row with row_count 0; the final row count was 7,568 and Paper v2 remained fail-fast until the real limit rows existed.
- Verified on development ports only: `pytest backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/selection_center/test_runtime_selection.py -q -p no:cacheprovider` -> 42 passed; `nox -s l0` -> passed; `nox -s paper_v2_backend` -> 107 passed; `npm exec tsc -- --noEmit` -> passed; `nox -s paper_v2_ui` -> 12 passed; `nox -s paper_v2_live -- --require-live-bars` -> passed with `qe_20260416_002701`, V25, replay_start 2026-04-28, live_date 2026-04-29, order_count 21, fill_count 74, error_count 0.
- Evidence record: `tests/aistock_validation/history/paper_v2_selection_center/20260429_103547_l4_paper-v2-catchup-to-live-trading-hours-validation.md`. Production backend 8001 was not restarted; dev backend 8012 was restarted for validation.

## Paper v2 Live/HMM Validation Follow-up - 2026-04-29

- Fixed HMM daily coefficient generation for dynamic PUP snapshots whose `models.json` contains `state_validation_stats` instead of legacy `state_labels`. `scripts/precompute_hmm_coefficients.py` now computes PIT dynamic daily coefficients from forward-filtered posterior probabilities plus the registered HMM config (`method`, `horizon_weights`, confidence scale, coefficient lambda/bounds) instead of failing on missing labels or falling back to neutral coefficients.
- `HMMTrainingService` passes the registered `config_json` into the WSL coefficient-generation script so live/Paper v2 HMM prediction can use the same dynamic coefficient semantics that produced the validated precomputed artifacts.
- Paper v2 model/HMM UI now hides raw stderr/traceback details and local file paths from ordinary operator panels while keeping backend job error details persisted for audit; successful daily coefficient jobs show status, result, dates, and artifact hash.
- Revalidated on development ports only (`8012` backend, `3012` frontend, TDX `19080`): targeted Paper v2/Selection tests 42 passed, `backend/tests/test_hmm_daily_coefficients.py` 8 passed, `nox -s paper_v2_backend` 107 passed, `nox -s paper_v2_ui` 12 passed, and `nox -s paper_v2_live -- --require-live-bars` passed at live bar `2026-04-29T11:19:00+08:00` with replay_start `2026-04-28`, order_count 21, fill_count 74, error_count 0.

## V25 Minute Execution Optimization Strategy - 2026-04-29

- Added `docs/architecture/v25_minute_execution_optimization_strategy_20260429.md` as the follow-up design entry point for V25/V25.1 execution optimization.
- The document records that current V25 is trained toward a 240-minute Oracle weight curve rather than a one-shot Oracle minute, and that the next framework change should discretize continuous weights through cumulative target curves instead of independent per-minute round-lot truncation.
- The next V25 framework work should add board-aware lot rules, explicit fee/minimum-commission semantics, cost-aware child-order batching, residual/tail handling, and full QE/Paper v2 consistency checks before any Paper v2 admission.
- Historical tick data may be used to generate tick-informed minute-level Oracle labels and historical aggregate features for a future V25.1, but runtime policies must fail fast if they require realtime tick data that Paper v2/live execution cannot provide; no default tick features or silent fallback are allowed.
- No V25 model weights, StrategyPackage manifests, validated execution policies, QE/RD-Agent workspaces, DB asset rows, or runtime assets were modified by this documentation/index update.

## Paper v2 Live Dashboard / PIT Selection Validation - 2026-04-29

- Added the Paper v2 live observation dashboard contract and implementation for `/paper-v2/portfolios/{portfolio_id}/live-dashboard`, backed by read-only APIs for live dashboard, intraday snapshots, and minute execution timelines. The page shows signal, target/rebalance, minute execution, NAV snapshots, positions, errors, and warnings in Chinese operator-readable UI without exposing raw JSON.
- Selection Center now supports point-in-time selection mode `PREVIOUS_TRADING_DAY_CLOSE`: selecting target trade date D resolves and records the previous trading day cutoff, passes `cutoff_date` into live StrategyPackage inference, and stores the PIT context in the selection run/runtime artifact hash. Historical selection for D therefore uses data no later than D-1 trading day.
- Paper v2 live/replay session ticks now use a PostgreSQL advisory lock in addition to the in-process lock, preventing multiple backend processes or schedulers (for example production 8001 plus dev 8011/8012) from processing the same session concurrently. This avoids duplicate replay attempts and duplicate ledger side effects while preserving fail-fast lock errors.
- The UI E2E suite now pre-generates PIT-aligned selection artifacts, waits for long-running selection requests to settle instead of reading placeholder rows, validates multi-package weighted/union/intersection paths with real package overlaps, treats incomplete HMM sector mappings as explicit fail-fast UI errors, and keeps V25 replay UI validation short enough to avoid production scheduler interference on shared local DBs.
- Validation completed on development ports only (`8011` backend, `3011` frontend, TDX `19080`) while production backend `8001` stayed running: `pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider` -> 146 passed; `npm exec tsc -- --noEmit` -> passed; `nox -s l0` -> passed with existing MEDIUM review findings only; `nox -s paper_v2_backend` -> 109 passed; `nox -s paper_v2_ui` -> 12 passed; `nox -s paper_v2_l3` -> passed L0 + backend + UI.
- Evidence record: `tests/aistock_validation/history/paper_v2_selection_center/20260429_174029_l3_paper-v2-selection-center-l3-regression.md`. No StrategyPackage manifests, model weights, HMM snapshots, validated execution policies, QE/RD-Agent assets, or strategy source assets were modified.

## Paper v2 Session Isolation / No-Rebalance Validation - 2026-04-29

- Added explicit `manual_tick_only` handling for Paper v2 UI/validation-created sessions. Such sessions are created in `PAUSED` status and require an explicit tick request with `allow_paused=true`, so another backend process or the production 8001 scheduler cannot race a just-created replay/catch-up session before the UI-owned tick starts.
- During manual replay/catch-up processing, the session remains non-tickable until the explicit tick finishes; this is a concurrency isolation mechanism, not a silent fallback. Scheduler-driven API sessions remain unchanged unless `manual_tick_only` is requested.
- Rebalance no-op is now an explicit business state: if target positions exactly match current positions, `RebalanceEngine` returns an empty diff instead of treating it as missing data. Paper v2 day/live runners persist `NO_REBALANCE_REQUIRED` events and real minute-price mark-to-market snapshots; missing targets still fail fast.
- Local validation on dev ports 8011/3011 with TDX 19080 passed: full Trading Core/StrategyPackage/Paper v2/Selection pytest `149 passed`, TypeScript `tsc --noEmit` passed, `nox -s paper_v2_l3` passed with Playwright `12 passed`, and `nox -s paper_v2_live` passed after-hours with `qe_20260416_002701`, V25, replay_start 2026-04-28, live_date 2026-04-29, `order_count=20`, `fill_count=74`, `error_count=0`.
- Production backend 8001 was not restarted by Codex. During validation it still returned 404 for the new Selection PIT cutoff endpoint, so production 8001 must be restarted by the user to load the latest Paper v2/Selection Center code and avoid old scheduler behavior.

## Paper v2 / Selection Data Quality Gate - 2026-04-29

- Added `scripts/aistock_data_quality_smoke.py` as a read-only DB smoke for the Paper v2 + Selection Center validation pipeline. It checks required schema tables, trading calendar resolution, dataset refresh audit freshness, StrategyPackage catalog readiness, selection result traceability, Paper v2 run snapshot/event traceability, and ledger consistency.
- The gate now recognizes successful live/replay completion events from both `paper_v2.run_events` and `paper_v2.session_events`, because live session finalization can be recorded at session level by `run_id`.
- Historical local Paper v2 rows with known order/fill mismatches are reported as `WARN` in default baseline mode so legacy polluted development data does not block new validation work. Scoped checks using `--portfolio-name-prefix`, `--portfolio-id`, or `--strict-history` remain fail-fast.
- Added `paper_v2_data_quality` to `noxfile.py` and included it in `paper_v2_l3`. Current user decision: permissions/auth/security/Web security tests are deferred for this internal single-operator phase; current gates focus on business logic, data quality, asset safety, no silent fallback, and UI/backend alignment.
- Verified without starting/restarting backend 8001 or frontend services: `nox -s paper_v2_data_quality` passed with one legacy WARN, `nox -s l0` passed with existing MEDIUM review findings only, `nox -s paper_v2_backend` passed with 112 tests, and `PAPER_V2_L3_SKIP_UI=1 nox -s paper_v2_l3` passed.

## Paper v2 / Selection Non-Realtime Full Validation - 2026-04-29

- Added explicit non-realtime validation support for Paper v2 + Selection Center. `scripts/aistock_validate.py services --skip-tdx` and `PAPER_V2_SKIP_REALTIME=1` skip only TDX realtime probing and live-session trading assertions; DB historical replay, PIT selection, aggregation, watchlist import, V25 replay, ledger, performance, runtime/policy audit, HMM UI, and data-quality checks remain mandatory.
- Hardened Playwright UI E2E against two validation issues found during full non-realtime testing: multi-package aggregation now selects the known compatible setup selection runs by run_id instead of arbitrary historical rows, and the Model/HMM maintenance test reloads once if Next dev serves an unhydrated placeholder-only page before requiring real package/config options from the backend.
- Full non-realtime validation ran on development ports only (`8011` backend, `3011` frontend) without touching production 8001: TypeScript `tsc --noEmit` passed, `nox -s paper_v2_backend` passed with 112 tests, `nox -s paper_v2_data_quality` passed with one legacy WARN, `nox -s l0` passed with existing MEDIUM findings only, `nox -s paper_v2_ui` passed with 12 Playwright tests, final `nox -s paper_v2_l3` passed, and full targeted pytest `backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center` passed with 149 tests.
- Scoped E2E data-quality smoke `--portfolio-name-prefix E2E --since-hours 2` passed with 0 warnings/failures, confirming the new validation-created Paper v2 portfolios/runs have consistent order/fill/cash/position/snapshot persistence. The default historical baseline still reports 3 legacy order/fill mismatches as WARN only.

## Paper v2 / Selection Watchlist Validation - 2026-04-30

- Extended the Paper v2 + Selection Center automated validation flow to prove watchlist import persistence instead of only checking the UI success toast. The Playwright E2E now imports a real Selection Center result into a unique watchlist category, verifies `/selection-center/runs/{run_id}/aggregate-results`, `/watchlist/categories`, `/watchlist/items`, and then opens `/watchlist` to confirm category/source filtering, StrategyPackage source name, selection run id, rank, entry price, entry_as_of date, gain-tracking columns, and join time are visible to the operator.
- Added Selection Center backend regression coverage for `add_run_to_watchlist`: imported payloads must carry source StrategyPackage name, selection run id, rank, reference price and as_of date; missing reference prices fail fast and are not silently fetched or defaulted.
- Hardened watchlist router fail-fast behavior by making `/watchlist/all` and `/watchlist/items/source-tasks` raise clear HTTP 500 errors on query failure instead of returning fake empty success.
- Fixed the Paper v2 data-quality smoke readiness gate for pre-close runs: before 18:00 Asia/Shanghai, the current trading day is not treated as completed for daily dataset freshness, preventing false failures that demand same-day daily datasets before they can exist.
- Validated on development ports only (`8011` backend, `3011` frontend) with realtime checks skipped intentionally: targeted selection test 26 passed, TypeScript `tsc --noEmit` passed, `paper_v2_backend` 114 passed, `paper_v2_ui` 12 passed, final `paper_v2_l3` passed, full targeted pytest `backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center` passed with 151 tests, and scoped E2E data-quality smoke passed with 0 warnings/failures. Evidence records: `tests/aistock_validation/history/paper_v2_selection_center/20260430_012103_l3_paper-v2-selection-center-l3-regression.md` and `tests/aistock_validation/history/paper_v2_selection_center/20260430_012735_l3_paper-v2-selection-center-l3-regression.md`.

## Paper v2 Running Summary / Package Enable Fix - 2026-04-30

- Fixed `/paper-v2/running` browser `Failed to fetch` caused by fan-out requests over many accumulated active Paper v2 portfolios. The UI now reads `/api/v1/paper-v2/running-summary`, which aggregates persisted portfolio/run/session/order/fill/position/snapshot/error data server-side without trading side effects.
- StrategyPackage `enable-paper` now validates package identity/hash/status only. It no longer validates the manifest-embedded historical minute execution runtime asset because Paper v2 execution is selected through separate backtest-validated execution policy rows. Explicitly selected/default execution policies still fail fast on missing runtime assets at portfolio/session entry.
- Validated on dev ports 8011/3011 only: targeted backend pytest 33 passed, full Trading Core/StrategyPackage/Paper v2/Selection pytest 152 passed, TypeScript passed, `nox -s paper_v2_ui` passed with 12 UI tests, and `nox -s l0` passed with no HIGH findings. Production 8001 was not restarted by Codex.

## Codex Response Formatting Preference - 2026-04-30

- User requires all future tables in Codex responses to have visibly aligned column titles and content widths. Prefer fixed-width tables in fenced code blocks or otherwise ensure column widths align before responding.

## Analysis Documentation Directory - 2026-05-02

- User requires all future analysis-class documents produced by Codex in this repository to be stored under `docs/analysis` (`F:\Dev\AIstock\docs\analysis`). Do not place new analysis reports elsewhere unless the user explicitly requests a different path.
- The QE no-alpha label-horizon root-cause analysis for `qe_20260501_201036_b699` is recorded at `docs/analysis/qe_20260501_201036_b699_no_alpha_label_horizon_root_cause_20260502.md`.

## Architecture Design Documentation Directory - 2026-05-02

- User requires all future design鏂规 / architecture design / implementation design documents produced by Codex in this repository to be stored under `docs/architecture` (`F:\Dev\AIstock\docs\architecture`). Do not place new design documents elsewhere unless the user explicitly requests a different path.
- QE real-time experiment warehouse top-level design is recorded at `docs/architecture/qe_realtime_experiment_warehouse_top_level_design_20260502.md`.
- QE experiment data completeness pre-warehouse design is recorded at `docs/architecture/qe_experiment_data_completeness_prewarehouse_plan_20260503.md`; it establishes that QE DB may duplicate non-large operational data for generation/retry/recovery/UI, while the future warehouse must keep independent long-term storage because QE workspace and QE DB records may be cleaned after verified archival.
- AIstock automated testing, coverage, observability, and version-gate design is recorded at `docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`; v1.1 explicitly builds on existing `noxfile.py`, `scripts/aistock_validate.py`, `tests/aistock_validation`, Playwright tests, data-quality smoke scripts, and run records rather than creating a separate framework from zero. Future test UI should schedule allowlisted nox/aistock_validate plans and add JSON metadata, evidence manifests, coverage gates, and L0-L5 quality pipeline requirements.

## QE Worker Workspace Access Red Line - 2026-05-02

- Treat WSL QE/RD-Agent runtime as an independent Linux compute node, equivalent to a remote machine. Windows-side AIstock services must not assume the Linux worker workspace is locally reachable through `F:\...`, `/mnt/f/...` path conversion, `\\wsl$`, or any other filesystem shortcut.
- `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, and DB `workspace_path` are legacy/local metadata only. In Windows FastAPI request paths, they must not be used to directly read, scan, copy, mutate, or delete QE/RD-Agent worker workspace files, including `mlruns`, `run.log`, `qlib_results_enhanced.json`, `pred.pkl`, `params.pkl`, `conf.yaml`, factor files, or position pickle artifacts.
- QE artifact access from AIstock must go through node APIs such as `QEWorkspaceClient`, Results API endpoints, explicit SSH/node cleanup commands, DB-cached summaries, or a controlled AIstock-local artifact store populated by an explicit sync/download step. Local cache reads are allowed only when the cache is clearly owned by AIstock and is not the worker workspace path.
- User-facing APIs and pages must never return 500 because optional QE workspace artifacts are inaccessible from Windows. Optional artifact enrichment must be best-effort, catch filesystem/network errors, and report artifact unavailable instead of failing the main task/detail response.
- Future QE/backend/StrategyPackage/Paper changes must audit direct `Path.exists/glob/rglob/open/read_pickle/shutil` access to `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, normalized `/mnt/... -> drive:` paths, and DB `workspace_path`. If the access targets worker artifacts, replace it with node API or explicit artifact sync; do not add new exceptions.

## QE Archive Design Confirmations - 2026-05-02

- QE archive artifacts should use the AIstock repo-root path `qe_archive/artifacts` (`F:\Dev\AIstock\qe_archive\artifacts`) as the long-term artifact store entry, not `rdagent_assets/qe_archive/artifacts`.
- QE daily-frequency backtests without authoritative limit-up/limit-down and suspension handling must be archived as `research_valid=false` and excluded from default leaderboards, optimizer warm-start samples, and effective research rankings.
- QE archive model/factor importance analysis must cover all model families used in experiments. LSTM/deep models are in scope from the first implementation phase via model-agnostic attribution and deep-model attribution; tree-model native importance is not sufficient by itself.
- LLM agents may read controlled, audited, read-only QE archive aggregate views/tools to propose candidates in future phases; implementation details remain a later design task.

## QE Realtime Experiment Warehouse Detailed Design - 2026-05-02

- QE realtime experiment warehouse detailed design is recorded at `docs/architecture/qe_realtime_experiment_warehouse_detailed_design_20260502.md`.
- The detailed design preserves the confirmed decisions: new `qe_archive` schema, artifact root `qe_archive/artifacts`, default exclusion of daily backtests without authoritative limit/suspend handling, single first-version `score_total` with sub-scores retained, all-model/LSTM attribution support, Optuna/custom-evolution near-term tuning, and future audited read-only LLM agent interfaces.

## QE Loop19+ Backtest Truth Audit Update - 2026-05-02

- Added read-only QE audit tooling for Loop19+ under `scripts/`: `qe_execution_truth_audit.py`, `qe_price_tradability_audit.py`, `qe_strategy_code_evidence_audit.py`, and `qe_factor_dynamic_truncation_audit.py`; copied the same tools into the `qe-evolution-diagnostics` skill.
- Current synthesis for `qe_20260501_011054_c90a` Loop19-28 is recorded at `docs/analysis/P0_P1_qe_20260501_011054_c90a_loop19_28_backtest_truth_synthesis_20260502.md`.
- Cost interpretation for nested minute QE: report `cost`/`total_cost` can be zero because inner executor `generate_portfolio_metrics=false` prevents cost metric accumulation, while Qlib source shows `Exchange.deal_order` computes cost and `Position.update_order` subtracts cost from cash. Treat this as a metric-recording gap unless a dedicated no-cost rerun proves NAV ignored costs; do not double-subtract post-hoc cost overlays from NAV.
- V25/tail substitute traceability: aggregate day/minute indicator consistency is verifiable from current artifacts, but exact per-order V25 plan/no-fill/tail-substitute branch execution is not reconstructable until plan and branch events are persisted.

## QE Archive Phase 1 Implementation - 2026-05-02

- Started the QE realtime experiment warehouse implementation with explicit schema bootstrap `backend/db/init_qe_archive_schema.py`, service package `backend/services/qe_archive`, and targeted tests `backend/tests/test_qe_archive_schema.py` plus `backend/tests/test_qe_archive_repository_static.py`.
- Applied local DB bootstrap for `qe_archive_v1_20260502`; verification showed 27 `qe_archive` tables and the expected schema version row.
- The first schema contract records reproducibility-critical experiment/loop configuration via `run_config` (`canonical_config`, `raw_config`, `config_sha256`, factor list/hash, config provenance, capture-complete flag, missing config items) and `run_reproducibility_manifest` (hashes, environment/package versions, source config paths, artifact manifest, missing items, reproducibility level).
- Phase 1 repository methods support idempotent run/config/repro manifest writes, raw payload inserts, outbox event inserts, metric batch replacement, and artifact manifest writes. Runtime webhook hooks, artifact parsers, historical backfill, aggregate views, and UI are still pending later phases.
- Validation record: `tests/aistock_validation/history/qe/20260502_153630_l1_qe-archive-phase1-schema-repository.md`. Production backend 8001 was not restarted; no QE/RD-Agent worker workspace assets were modified.

## QE Archive Comments / Production-Safe Next Step - 2026-05-02

- `backend/db/init_qe_archive_schema.py` now generates PostgreSQL `COMMENT ON SCHEMA`, `COMMENT ON TABLE`, and `COMMENT ON COLUMN` statements for the managed `qe_archive` schema. Verification showed 27/27 tables and 458/458 columns have non-empty comments in the local database.
- The QE archive schema test now enforces comment coverage for every managed table and column, matching the new DB schema standard in the engineering rules.
- Added `backend/services/qe_archive/event_capture.py` as a disabled-by-default foundation for future outbox ingestion. It is not wired into QE routers/webhooks yet and writes only when `QE_ARCHIVE_EVENT_CAPTURE_ENABLED` or an explicit test constructor enables it.
- Validation record: `tests/aistock_validation/history/qe/20260502_155009_l1_qe-archive-comments-and-disabled-event-capture.md`. Production backend 8001 was not restarted; current QE production runtime behavior was not changed.

## QE Archive Validation Pipeline - 2026-05-02

- Introduced Paper v2-style validation entry points for QE archive development in `noxfile.py`: `qe_archive_backend`, `qe_archive_data_quality`, `qe_archive_ui`, and `qe_archive_l3`.
- Added read-only DB smoke `scripts/qe_archive_data_quality_smoke.py`, which checks managed `qe_archive` table existence, schema version, table comments, column comments, run count, and pending outbox count without starting services or mutating QE state.
- Added test matrix `tests/aistock_validation/modules/qe_archive.md`. Future QE archive backend workflow, artifact/parser, API, and UI phases must add/extend tests in this matrix and preserve default production isolation.
- `qe_archive_l3` now runs guardrail scan + backend tests + DB smoke, with UI explicitly skipped by `QE_ARCHIVE_L3_SKIP_UI=1` until QE archive UI exists. Validation passed with 15 backend tests and 27 table / 458 column comment coverage.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_160746_l3_qe-archive-realtime-warehouse-validation.md`. Production backend 8001 was not restarted and no current QE production runtime hook was enabled.

## QE Archive Outbox Worker Foundation - 2026-05-02

- Added disabled-by-default QE archive outbox worker foundation in `backend/services/qe_archive/worker.py`, exported from `backend/services/qe_archive/__init__.py`. It only processes events when explicitly enabled through `QE_ARCHIVE_WORKER_ENABLED` or a test constructor.
- Repository support now includes outbox claim/complete/fail transitions and archive job create/complete/fail transitions. The worker records archive jobs, completes outbox events on handler success, and marks jobs/outbox retry state on handler failure.
- The worker is intentionally not wired into FastAPI startup, schedulers, or QE webhook paths. Current QE production runtime behavior remains unchanged unless a future explicit integration phase enables event capture/worker flags.
- Validation expanded to 20 backend tests plus a real PostgreSQL synthetic outbox/archive_job state-machine smoke. Synthetic validation rows were cleaned up, and final data-quality smoke confirmed 27/27 managed tables, 458/458 commented columns, `pending_outbox_count=0`, and empty `archive_job_status_counts`.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_162657_l3_qe-archive-realtime-warehouse-validation.md`. Production backend 8001 was not restarted; no QE/RD-Agent worker workspace assets were accessed or modified.

## QE Archive Manual Payload Service Foundation - 2026-05-02

- Added manual/dry-run archive payload processing via `backend/services/qe_archive/payload_extractor.py` and `backend/services/qe_archive/archive_service.py`. This phase accepts already-collected QE loop/experiment payloads only; it is not wired into QE webhooks, FastAPI startup, or any scheduler.
- The extractor normalizes reproducibility-critical config, ordered factor list/hash, data context, daily invalidity (`research_valid=false` when daily without authoritative limit/suspend), account absolute return fields, scalar metrics, IC/RankIC/return/drawdown/training curves, factor rows, reproducibility manifest, and raw payload snapshots.
- Repository support now includes `run_source`, `run_data_context`, `run_account_summary`, `run_curve`, and `run_factor` writes in addition to the existing run/config/repro/metric/raw/artifact/outbox/job writes.
- Validation expanded to 23 backend tests plus a real PostgreSQL synthetic archive-service write/cleanup smoke. The synthetic payload wrote run/config/repro/data_context/account/metric/curve/factor/raw rows, then deleting the synthetic `qe_archive.run` row cascaded cleanup. Final data-quality smoke showed `run_count=0`, `pending_outbox_count=0`, and no archive jobs.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_164341_l3_qe-archive-realtime-warehouse-validation.md`. Production backend 8001 was not restarted; no QE/RD-Agent worker workspace assets were accessed or modified.

## QE Archive Source Assembler / Backfill Dry-Run - 2026-05-02

- Added `backend/services/qe_archive/source_assembler.py` to assemble archive payloads from existing public DB rows in `qe_experiments`, `qe_evolution_loops`, and `qe_evolution_tasks`. It is read-only against source QE tables and intentionally omits legacy worker artifact path metadata.
- Added manual CLI `scripts/qe_archive_backfill.py`. Default mode is dry-run; write mode requires both `--write` and `--confirm-write QE_ARCHIVE_WRITE`. This CLI is not scheduled and is not imported by FastAPI runtime.
- Archive service writes now replace raw payload rows for the same run/payload types via `replace_raw_payloads`, avoiding duplicate raw payload rows during repeated confirmed backfill runs.
- Validation expanded to 25 backend tests plus real DB dry-run preview: `python scripts/qe_archive_backfill.py --source all --limit 1` processed one completed experiment and one completed loop with `written=false`. Final data-quality smoke still showed `run_count=0`, `pending_outbox_count=0`, and empty `archive_job_status_counts`.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_171206_l3_qe-archive-realtime-warehouse-validation.md`. Production backend 8001 was not restarted; no QE/RD-Agent worker workspace assets were accessed or modified.

## QE Archive First Confirmed Backfill Write - 2026-05-02

- Extended `scripts/qe_archive_data_quality_smoke.py` with optional run-level validation (`--run-id`, minimum metric/curve/factor thresholds, and account-summary requirement) while preserving read-only behavior.
- Improved `backend/services/qe_archive/payload_extractor.py` to infer `backtest_start` and `backtest_end` from enhanced return-curve dates when a source QE loop config does not explicitly store the backtest window. This allows reproducibility completeness for cached enhanced-metrics loops without reading worker files.
- Confirmed-wrote the first real loop through the manual CLI only: `qe_20260501_011054_c90a_Loop11` archived as `qear_run_6aad101d9e6e31f629230a4c` using `--write --confirm-write QE_ARCHIVE_WRITE`. Re-running the same command kept counts stable, proving idempotent replacement semantics for metrics/curves/factors/raw payloads.
- Run-level smoke confirmed the archived loop has `config_capture_complete=true`, `reproducibility_level=full`, 1 source row, 1 data context, 1 account summary, 81 metrics, 3,489 curves, 57 factor rows, 3 raw payload rows, and zero failures/warnings.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_174513_l3_qe-archive-realtime-warehouse-validation.md`. Production backend 8001 was not restarted; no QE/RD-Agent worker workspace assets were accessed or modified. Broad historical backfill, artifact manifests/parsers, API/UI consumers, and webhook/worker integration remain future phases.


## QE P0/P1 Existing Artifact Audit Extension - 2026-05-02

- Added read-only QE audit tools `scripts/qe_v25_existing_artifact_audit.py` and `scripts/qe_factor_importance_selector.py`; copied both into the `qe-evolution-diagnostics` skill and documented the P0/P1 command sequence.
- Ran Loop19-28 existing-artifact audits for `qe_20260501_011054_c90a` without rerunning QE: IC/RankIC/label horizon/top bucket/year segments, execution truth, strategy-code evidence, full-minute price/tradability, V25 minute distribution/replay readiness, factor-importance selection, and targeted dynamic truncation on top factors.
- New key evidence: full warning minute audit classified 1,642 Qlib `$close=None` warnings as 1,123 DB daily+minute+limit present/not suspended, 484 suspend/no DB price, and 35 suspend_d + daily present + minute missing; 300 actual-trade price samples had max DB-vs-Qlib close/limit diff <= 0.000007.
- V25 aggregate evidence remains strong: 1min/1day `value` and `deal_amount` aggregate with zero diff across Loop19-28, active dates have 240/241 minute rows and no bad minute dates. Exact V25 child-order branch replay is still not provable from current artifacts because plan/no-fill/tail-substitute event rows are not persisted.
- Dynamic truncation expanded from the initial two-factor sample to top feature-importance factors on representative Loop19/22/26; all audited factor/date rows matched after PIT truncation with zero mismatches.

## QE P0/P1 Existing Artifact Follow-up Audit - 2026-05-02

- Added read-only QE audit tools `scripts/qe_close_none_root_cause_audit.py` and `scripts/qe_tail_window_risk_audit.py`; copied both into the `qe-evolution-diagnostics` skill and documented their usage. These tools only read existing artifacts/DB/Qlib bin and do not rerun QE or add strategy logging.
- Follow-up synthesis for `qe_20260501_011054_c90a` Loop19-28 is recorded at `docs/analysis/P0_P1_qe_20260501_011054_c90a_loop19_28_existing_artifact_followup_20260502.md`; the earlier full synthesis was updated with the follow-up summary.
- Close-none root-cause audit showed the 35 daily-present/minute-missing rows are confirmed suspension/no-trade rows: all have `suspend_d` `suspend_type=S`, daily `volume_hand=0`, and DB minute count 0.
- Close-none root-cause audit showed the 1,123 DB-present/not-suspended warnings are Qlib minute feature coverage gaps: DB daily/minute/limit rows exist, Qlib day close and instrument membership exist, Qlib 1min calendar rows exist, but Qlib 1min `$close` is null for every minute.
- Tail-window audit for Loop24/25/27 found high tail activity on some days, but same-day tail-ratio/return Spearman correlation is weak (0.0171 to 0.0712), so existing artifacts do not support tail activity as a mechanical return driver.
- Dynamic PIT truncation now covers top-12 feature-importance factors on Loop19/22/26: 36/36 factor-loop checks, 501,495 compared rows, zero mismatches. This lowers leakage risk for top-priority factors but is not a full proof for every generated factor.

## QE Backtest Data Accuracy Materiality Audit - 2026-05-02

- Added read-only materiality audit tool `scripts/qe_backtest_accuracy_materiality_audit.py`; copied it into the `qe-evolution-diagnostics` skill. It consumes existing P0/P1 JSON artifacts, persisted reports, and run logs only; it does not rerun QE, mutate workspaces, or add strategy logging.
- Materiality report for `qe_20260501_011054_c90a` Loop19-28 is recorded at `docs/analysis/P0_qe_20260501_011054_c90a_loop19_28_backtest_data_accuracy_materiality_20260502.md`.
- Current answer for Loop19-28 data accuracy: no NAV/account/position/IC/RankIC/V25 aggregate calculation-chain error has been found. Numerical gates passed: IC/RankIC max diff 0, report return/account max diff 1.11e-16, position/report account/cash diff 0, stock-value diff 1.19e-7, V25 1day/1min value max diff 6.71e-8, deal_amount max diff 4.66e-8, report NaN/Inf count 0.
- The remaining proven warning is Qlib 1min minute coverage: 1,123 DB-present/not-suspended warnings all had Qlib 1min `$close` all-null. Materiality from run logs: 326 total ScoreWeighted invalid-price skips, 107 DB-present coverage-gap skips, 21,842 derived buy rows, total invalid skips 1.49% of buy rows, DB-present coverage-gap skips 0.49% of buy rows.
- Until Loop1-18 full_train reruns complete, continue data-accuracy validation only; do not start model/factor optimization synthesis.


## Codex Git Commit Requirement - 2026-05-02

- User requires Codex to commit every future code/documentation modification to GitHub after completing and validating the work. Commit only the files changed for the current task and do not include unrelated dirty-worktree changes.

## QE Archive Small-Batch Historical Backfill - 2026-05-02

- Expanded the QE archive historical backfill from the first single-loop confirmed write to a small trusted batch using only the manual CLI and local PostgreSQL `qe_archive` schema; no production FastAPI `8001` restart and no QE runtime hook/worker integration were performed.
- Dry-run previewed 20 recent completed QE evolution loops: 12 were `research_valid=true`, 8 were excluded from the confirmed write batch, and no missing items were reported by the extractor dry-run stats.
- Confirmed-wrote 10 additional valid 1min evolution loops: `qe_20260501_011054_c90a` Loop7/8/9/10/12/13 and `qe_20260502_131502_9b54` Loop1/2/3/4. Together with the earlier Loop11 write, local archive `run_count=11`.
- Each newly written run passed run-level data-quality smoke with `config_capture_complete=true`, `reproducibility_level=full`, `research_valid=true`, account summary present, at least 60 scalar metrics, at least 3,000 curve rows, and 57 factor rows. Label horizon coverage in the batch includes 5 and 10.
- Re-running the same confirmed write batch proved idempotency: final data-quality smoke still showed `run_count=11`, `pending_outbox_count=0`, and empty `archive_job_status_counts`.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_182707_l3_qe-archive-realtime-warehouse-validation.md`. `qe_archive_backend`, `qe_archive_data_quality`, and `qe_archive_l3` passed; QE archive UI remains skipped through `QE_ARCHIVE_L3_SKIP_UI=1` until implemented.

## QE Archive API Backfill / Realtime Hook Foundation - 2026-05-02

- Added backend API router `backend/routers/qe_archive.py` under `/api/v1/qe-archive`, registered in `backend/main.py`. The API exposes `/health`, `/backfill`, and `/runs/{run_id}/quality`.
- Added `backend/services/qe_archive/backfill_service.py` so historical experiment/loop琛ュ綍 can be triggered through API instead of hand-running `scripts/qe_archive_backfill.py`. API write mode requires `confirm_write=QE_ARCHIVE_WRITE` and can validate minimum metric/curve/factor/account row counts after writing.
- Added `backend/services/qe_archive/realtime_ingestion.py` and best-effort QE completion hooks in `backend/services/quantevolver/qe_evolution_service.py` plus single-experiment status sync in `backend/routers/quantevolver.py`. Realtime archive ingestion is disabled by default through `QE_ARCHIVE_REALTIME_ENABLED`; when enabled it writes after source QE DB completion succeeds and catches/logs archive failures without changing QE loop/experiment status.
- API smoke with FastAPI `TestClient` proved dry-run, confirmed write, run quality, and warehouse health endpoints against local DB using existing run `qear_run_c2b3a64b30929794faf91e65`; local archive remains `run_count=11`, `pending_outbox_count=0`.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_195907_l3_qe-archive-realtime-warehouse-validation.md`. `qe_archive_backend`, `qe_archive_data_quality`, and `qe_archive_l3` passed with 31 backend tests; production backend `8001` was not restarted.

## QE Qlib Minute OHLCV/Factor Gap Diagnosis - 2026-05-02

- Added read-only Qlib minute gap diagnosis script `scripts/qe_qlib_minute_gap_diagnosis.py` and copied it into the `qe-evolution-diagnostics` skill as Additional Tool L. It consumes existing close-none and price/tradability audit JSON, inspects `/home/lc999/data/qlib_minute_bin` 1min bin files directly, and compares affected dates to current DB minute coverage without rerunning QE or mutating data.
- For `qe_20260501_011054_c90a` Loop19-28, exact QE-warning affected stock-date pairs are recorded at `docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_gap_stock_dates_20260502.csv`: 486 stock-date pairs, 157 stocks, 7 trading dates from 2025-07-08 through 2025-07-16.
- Full current-DB universe gaps are recorded at `docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_gap_all_db_present_stock_dates_20260502.csv`: 9,655 DB-present stock-date pairs have Qlib 1min `$close` all-null on those 7 dates.
- Proven local root cause: not DB minute absence, not suspension, not calendar/instrument membership, and not limit-price precision. Affected Qlib `open/high/low/close/volume/amount/factor` 1min bins are all NaN for those offsets, while current DB minute rows and Qlib `prev_close/up_limit_price/down_limit_price` bins are present. The exact historical operational cause beyond this file-level proof requires export job logs or preserved CSV snapshots.

## QE Qlib Minute Export Lineage Root Cause - 2026-05-02

- Export lineage/root-cause document is recorded at `docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_export_lineage_root_cause_20260502.md`.
- Historical production record shows `/home/lc999/data/qlib_minute_bin` was an old full-market minute base (`2024-01-02~2026-03-19`) plus incremental append to `2026-04-28`; the proven gaps are in `2025-07-08~2025-07-16`, so they were inherited from the old historical base snapshot rather than produced by the 2026-03-20+ append.
- Retained 10-stock candidate CSV/bin under `qlib_minute_validation/full_factor_minute_chain_20260428_candidate` proves current DB plus export logic can produce valid 2025-07 rows for affected sample stocks such as `000063.SZ` and `000651.SZ`, while the official production minute bin has all-null `close/factor` for the same stock-date offsets.
- Official full-market production CSV/log/backup (`qlib_minute_prod/csv`, `qlib_minute_full/csv`, `/home/lc999/data/qlib_minute_bin_backup_20260429_205315`) was not found locally, so do not claim an exact operational cause such as interrupted export/copy; the proven cause is an incomplete production OHLCV/factor minute bin snapshot.

## QE Qlib Minute Bin Direct Repair Plan - 2026-05-02

- Direct-repair implementation plan is recorded at `docs/architecture/qlib_minute_bin_direct_repair_plan_20260502.md`.
- Important distinction: prior Codex work only diagnosed/documented the gap and did not modify Qlib bin files; the historical `dump_limit_price_minute_bins.py` overlay filled only `prev_close/up_limit_price/down_limit_price`, leaving `open/high/low/close/volume/amount/factor` missing in the affected official minute bin offsets.
- Any future repair must be dry-run first, then backed up, then patch all required fields consistently; do not patch only `close.1min.bin`, do not fill zeros/defaults, and fail fast on DB row, factor, offset, field-file, or checksum mismatch.

## QE Qlib Minute Bin Repair Dry-Run Verification - 2026-05-02

- Added dry-run repair planner `scripts/qe_qlib_minute_bin_repair.py` in the repo. It was not kept in the QE experiment-analysis skill per user direction.
- Scan confirmed 9,655 DB-present stock-date gaps across 2,696 stocks and 7 dates (`2025-07-08` through `2025-07-16`), with `open/high/low/close/volume/amount/factor/limit_up/limit_down` all missing for the affected Qlib offsets. Dry-run patch plan contains 9,655 records, 24,264 unique field files, zero skipped records, and file hashes.
- Initial DB-max-adj factor validation failed for 33 stocks because current DB max `adj_factor` differs from the denominator embedded in the official Qlib minute bin. The verifier now infers each stock's official factor denominator from adjacent non-null Qlib `$factor` plus DB `adj_factor`, reports DB/Qlib denominator drift explicitly, and never silently falls back to current DB max.
- Final read-only verify-plan passed: 9,655 records, 2,696 stocks, 84,628 adjacent factor samples, 0 failures, 0 warnings. It found 33 stocks where current DB max adj differs from the official inferred denominator, planned missing-date factor range `0.6711399555~1.0`, and stable denominators for every checked stock.
- Evidence files: `docs/analysis/P0_qlib_minute_bin_direct_repair_dry_run_summary_20260502.md`, `docs/analysis/P0_qlib_minute_bin_gap_scan_20260502.md`, `docs/analysis/P0_qlib_minute_bin_patch_plan_dry_run_20260502.md`, `docs/analysis/P0_qlib_minute_bin_patch_plan_verify_20260502.md`, and `docs/analysis/P0_qlib_minute_bin_factor_basis_20260502.csv`. JSON plan/verify artifacts are local dry-run outputs and are ignored by git by default.

## QE Qlib Minute Bin Direct Repair Applied - 2026-05-02

- User clarified that minute-bin repair should not be added to the QE experiment-analysis skill, so the temporary skill additions were removed. The repo script remains `scripts/qe_qlib_minute_bin_repair.py`.
- Added and executed the explicit write command `apply-plan`, guarded by confirmation text `APPLY_Q_LIB_MINUTE_BIN_REPAIR`. It validates pre-apply calendar/file SHA256 hashes, uses inferred official factor denominators, validates DB minute timestamps against the Qlib 1min calendar, applies atomic field-file rewrites, and readbacks every patched offset.
- Applied the repair to `/home/lc999/data/qlib_minute_bin`: 9,655 stock-date records, 2,696 stocks, 24,264 field files, 20,854,800 float32 values. Readback max absolute difference was 0.0.
- Before writing `limit_up/limit_down`, adjacent official Qlib minute bins were compared against DB raw minute prices and `market.stk_limit` on 75,431 stock-date samples. Existing official bins matched `raw close_li / 1000` versus raw `up_limit/down_limit`; they did not use adjusted prices or one-word limit logic.
- Backup was created before writes at `/home/lc999/data/qlib_minute_bin_backup_direct_repair_20260502_` with 24,264 files and 12,962,997,064 bytes; manifest is `/home/lc999/data/qlib_minute_bin_backup_direct_repair_20260502_/backup_manifest.json`.
- Post-repair scan confirmed `patchable_candidates=0` and no missing patch fields for the same 9,655 DB-present stock-date pairs. Post-repair verify-plan passed with 0 failures / 0 warnings and 94,283 adjacent factor samples.
- Repair evidence files: `docs/analysis/P0_qlib_minute_bin_repair_apply_20260502.md`, `docs/analysis/P0_qlib_minute_bin_post_repair_scan_20260502.md`, `docs/analysis/P0_qlib_minute_bin_post_repair_verify_20260502.md`, and `docs/analysis/P0_qlib_minute_bin_direct_repair_dry_run_summary_20260502.md`.

## QE Archive Durable Outbox / UI Update - 2026-05-02

- QE archive realtime ingestion now defaults to durable outbox mode when `QE_ARCHIVE_REALTIME_ENABLED` is explicitly enabled. Disabled mode still performs no writes; `QE_ARCHIVE_REALTIME_MODE=direct` is retained only for diagnostic/rollback direct archive writes.
- Added API-facing one-shot worker service and endpoint `/api/v1/qe-archive/worker/run-once`, requiring `confirm_run=QE_ARCHIVE_WORKER_RUN`; no scheduler/startup worker is registered. Added `/outbox` and `/jobs` read APIs for queue/job monitoring.
- Added first QE Archive frontend route `/qe-archive` plus API client, sidebar entry, dry-run/write backfill panel, worker run-once panel, warehouse health, recent outbox/job tables, and run quality lookup. UI uses readable Chinese business labels and not raw JSON as the primary operator view.
- Added mocked Playwright E2E for QE Archive UI and `QE_ARCHIVE_UI_MOCK_API=1` support in `qe_archive_ui`, allowing UI route/workflow validation without restarting or depending on production backend `8001`.
- Validation record: `tests/aistock_validation/history/qe_archive/20260502_231049_l3_qe-archive-realtime-warehouse-validation.md`. `qe_archive_backend`, `qe_archive_data_quality`, `qe_archive_ui` with mocked API, and full `qe_archive_l3` passed; local DB still has 27/27 managed tables, 458/458 commented columns, `run_count=11`, and `pending_outbox_count=0`.

## QE Archive Candidate-List Backfill UI - 2026-05-03

- Added API/UI support for historical backfill candidate lists: `/api/v1/qe-archive/backfill-candidates` lists QE evolution tasks and single experiments with type, description, loop counts, archived/pending counts, model/label/factor metadata, status, and execution timestamps.
- Backfill now accepts `task_ids`; selecting one QE evolution task expands to all matching loops and archives every loop through the existing archive service. This preserves idempotent archive writes and avoids manual shell scripts for historical backfill.
- The `/qe-archive` UI now shows selectable pending candidates, "select all pending", dry-run preview, confirmed "鍐欏叆鏁颁粨", and a fixed quality gate explanation. Minimum metrics/curves/factors are post-write completeness checks, not data collection filters.
- Updated QE archive validation matrix and detailed design to make candidate-list backfill part of the first-stage workflow. Production backend `8001` was not restarted; realtime ingestion remains disabled unless explicitly enabled.
- Validation record: `tests/aistock_validation/history/qe_archive/20260503_000218_l3_qe-archive-realtime-warehouse-validation.md`. `qe_archive_backend`, `qe_archive_data_quality`, `qe_archive_ui`, and full `qe_archive_l3` passed with mocked UI APIs; DB smoke still shows 27/27 managed tables, 458/458 commented columns, `run_count=11`, and `pending_outbox_count=0`.


## QE Archive Symbol/Trade Structured Extraction - 2026-05-03

- QE archive Phase 2 now extracts structured stock-level and trade-level data from already collected DB/API payloads only; no direct QE/RD-Agent worker workspace file access is introduced.
- `QEArchivePayloadExtractor` maps enhanced `all_stocks`/`top_stocks`/`bottom_stocks` into `qe_archive.run_symbol_summary`, `stock_trades` into `qe_archive.run_trade`, and parser/trade/execution diagnostics into `qe_archive.run_execution_event`.
- Repository quality summaries, data-quality smoke, API responses, and `/qe-archive` UI quality panel now expose `symbol_summary_count`, `trade_count`, and `execution_event_count`.
- Live dev validation on ports `8011`/`3011` re-ran API confirmed backfill for task `qe_20260502_131502_9b54`; 4 loops passed quality gates and sample run `qear_run_61fe6f6dccabca49b1228033` stored 792 symbol summaries, 4,322 trades, and 3 execution events.
- Production backend `8001` was not restarted; realtime ingestion remains feature-flagged/disabled by default unless explicitly enabled.

## Qlib Authoritative Minute Bin Export Update - 2026-05-03

- Added authoritative DB -> per-stock CSV -> `dump_bin.py` tooling for Qlib stock daily and 1min minute bins with QE/V25 fields: OHLCV, volume, amount, factor, prev_close, up/down limit prices, and limit flags.
- Full authoritative minute snapshot `qlib_minute_authoritative_full_20260428` was generated under `/home/lc999/data/qlib_minute_authoritative_full_20260428`: 5,515 instruments, 700,457,459 CSV rows, 66,180 feature bin files, 134,807 1min calendar rows, and 0 CSV-vs-bin coverage errors.
- Exporter now fails fast on missing factor/limit fields, records explicit prev_close repair counts, filters pre-listing minute rows by `stock_basic.list_date`, uses `1min` as the Qlib frequency, and prevents incremental stock exports from extending qfq `basis_end` because that would diverge from a full rebuild.
- `688766.SH` exposed a valid edge: zero-volume minute dates had `stk_limit.up_limit/down_limit` but null `pre_close`; missing `prev_close` is now filled only from the previous valid daily close and counted in `previous_daily_prev_close_filled_rows`.
- UI `frontend/src/app/qlib/page.tsx` supports selecting `stock_minute`; validation evidence is documented in `docs/analysis/P0_qlib_authoritative_bin_export_tool_and_validation_20260503.md` and `tests/aistock_validation/history/qlib_data/20260503_052234_l4_qlib-authoritative-minute-bin-export-validation-20260503.md`.

## QE/V25 Price Basis Contract - 2026-05-03

- Locked the execution price basis: `market.stk_limit.pre_close`, `up_limit`, and `down_limit` are raw/unadjusted RMB prices; V25/Paper v2 limit checks, P0 checks, and open-gap/gap-ratio calculations must also use raw prices.
- Qlib stock `open/high/low/close` remain adjusted for Qlib compatibility, so any Qlib execution adapter must compute `raw_price = adjusted_price / $factor` before comparing with raw `prev_close/up_limit/down_limit`; missing or invalid `$factor` on an otherwise valid bar is fail-fast, never `factor=1` fallback.
- Real-data verification for `/home/lc999/data/qlib_minute_authoritative_full_20260428` is documented in `docs/analysis/P0_price_basis_alignment_up_limit_prev_close_open_gap_20260503.md`: 5 stocks, 15 stock-dates, 3,600 DB minute rows, 43,200 checked field values, and zero DB-vs-Qlib errors when using the full snapshot `basis_end=2026-04-28`.

## Qlib Stock Universe Export Rule Update - 2026-05-03

- AIstock Qlib stock exports for QE must use SH/SZ only; BJ/BSE stocks are fixed excluded and should fail fast if requested.
- Authoritative bin stock export defaults to exclude ST and delisted/paused listings, always requires `stock_basic.list_status = 'L'`, and enforces `IPO_FILTER_DAYS=365` via `list_date + 365 days <= end`.
- The Qlib UI keeps BJ disabled and no longer sends `bj` in H5/bin/data-check payloads.
- Existing `/home/lc999/data/qlib_minute_authoritative_full_20260428` is invalid under this rule: meta has `exchanges=["bj","sh","sz"]`, `exclude_st=false`, `exclude_delisted_or_paused=false`, and instruments contain 310 BJ entries. Regenerate it with the fixed exporter rather than editing metadata only.

## HMM Training Current Status - 2026-05-03

- Latest HMM training/registry continuation document: `docs/analysis/hmm_training_current_status_20260503.md`. Read this first before updating HMM models, HMM QE selectable versions, or HMM normalization/preprocessing.
- Current retained effective baseline is `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore` with config `b99c907b-873a-4173-a4ee-5eab266f8c49` and snapshot `bbec3863-fb67-445f-938e-66f092d18696`.
- Dynamic PUP strict 0.10 (`5a3183b6-39bc-45dd-8b3d-d2027c476e62`) and 0.075 (`8ef81e6b-263d-4acd-93ff-4a20526b2d13`) are soft-disabled under `sector_hmm_disabled_ineffective_20260502`; keep them for traceability but do not re-add to QE selection without a new validation reason.
- After `qe_20260504_014618_a9ec`, the QE HMM selector is intentionally narrowed to two active `sector_hmm` configs only: Loop2 old covfix baseline `b99c907b-873a-4173-a4ee-5eab266f8c49` and Loop10 penalty-only best `ce4952c1-4b0d-46a7-81f2-ae1d4a249555`. The other seven 2026-05-02/2026-05-04 test candidates are soft-disabled under `sector_hmm_disabled_superseded_by_loop2_loop10_20260504` for traceability.
- Current old-covfix baseline does not use z-score normalization (`zscore=false`, no `zscore_mean/std` in `models.json`), but uses relative observation features rather than raw prices/index levels. Next HMM optimization should compare train-only zscore, winsor+zscore, robust zscore, and sector cross-sectional rank variants without modifying raw daily/minute data.

## Stock Pool / Universe Export Design Note - 2026-05-04

- Overview document: `docs/analysis/stock_pool_universe_export_overview_20260504.md`.
- User explicitly excluded all historical legacy selection paths and old paper-trading paths from future stock-pool retrofits; only QE main paths and Paper Trading v2/Selection Center should be adapted.
- Future target rule: feature data and stock-pool eligibility must be separated. Qlib Bin and H5/parquet should preserve real SH/SZ historical facts and must not delete an entire stock history because of future ST, pause, or delisting events. BJ/BSE remains excluded from QE stock exports.
- Buy/sell eligibility should be represented by PIT universe spans/masks. Qlib uses `instruments/*.txt` multi-segment start/end ranges; H5/parquet must use a companion `universe_spans` / `tradable_mask` or a loader that enforces instruments. Do not create multiple H5 copies per stock pool as the long-term architecture.
- Until the unified `StockPoolResolver` and H5/no-alpha loader filtering exist, closest-to-target QE backtests should prefer Alpha158 or NestedDataLoader through Qlib Bin plus explicit `stock_pool`. Do not treat no-alpha/H5-direct results as authoritative unless the parquet input is explicitly prefiltered to the same PIT universe.

## QE Data Completeness Phase 1 Foundation - 2026-05-04

- Added first-phase implementation plan at `docs/architecture/qe_data_completeness_phase1_development_plan_20260504.md`; this phase explicitly excludes full warehouse UI, production realtime archive hooks, workspace/DB cleanup, LLM auto-evolution, all-model attribution rollout, and new NoSQL storage.
- `scripts/aistock_validate.py` now keeps the existing Markdown run-record workflow while also supporting JSON run metadata sidecars and an `evidence` manifest command. This extends the existing validation framework instead of replacing `noxfile.py` / `tests/aistock_validation`.
- Added QE completion payload and artifact manifest contract at `backend/services/quantevolver/completion_contract.py`. It validates complete vs partial collection status, required sections, artifact manifest trace fields, sha256 format, and rejects raw WSL/remote worker workspace artifact URIs without wiring any production QE hook.
- Added `qe_data_contract_backend` nox session plus tests `backend/tests/test_aistock_validate_metadata.py` and `backend/tests/unified_engine/test_qe_completion_contract.py`; validation passed with targeted pytest 11 tests, `qe_data_contract_backend` 11 tests, targeted L0 0 findings, `qe_archive_backend` 46 tests, and read-only `qe_archive_data_quality` smoke with 27/27 tables and 458/458 commented columns.
- Validation evidence is recorded at `tests/aistock_validation/history/qe_data_completeness/20260504_015036_l3_qe-data-completeness-contract-and-validation-metadata.md` with JSON metadata and evidence manifest. Production backend `8001` and remote worker APIs were not restarted.

## Qlib H5 + Daily Bin Candidate Export - 2026-05-04

- Candidate export document: `docs/analysis/qlib_h5_daily_bin_current_candidate_export_20260504.md`.
- Generated non-production H5 snapshot `F:/Dev/AIstock/qlib_snapshots/qlib_20260430_shsz_current_candidate` and daily Qlib Bin snapshot `F:/Dev/AIstock/qlib_bin/qlib_bin_20260430_shsz_current_candidate`; production WSL `/home/lc999/data/qlib_bin` was not replaced.
- Current transition rules: SH/SZ only, ST `ann_date <= 2026-04-30` excluded, stock_basic D/P excluded, IPO 365D enforced via `instruments/all.txt`, feature rows kept as full post-listing history.
- Export counts: H5 feature universe 4,664, official all.txt 4,583, daily rows 7,264,909, daily bin CSV rows 7,264,909, static_factors rows 7,264,601 with 112 columns.
- Validation report: `reports/qlib_candidate_export/qlib_20260430_shsz_current_candidate_daily_validate_20260504.json`. H5/all.txt policy passed, Qlib `D.features` daily Bin values matched H5+DB `stk_limit` expectations on 2026-04-30 and 2025-07-10 under float32 storage tolerance; limit prices and limit flags matched exactly. Minute Bin was not regenerated in this candidate export.

## QE Enhanced Metrics / Data Completeness Progress - 2026-05-04

- User priority is to first make QE experiment creation/runtime/completion capture all valuable experiment data before relying on the QE warehouse. Key gaps already analyzed include incomplete loop enhanced metrics, sparse `position_summary` / `holding_audit`, missing full effective strategy-parameter snapshots, missing training-source explanation for backtest-only loops, missing artifact manifest, cost/report reconciliation gaps, and missing event-level execution logs such as order intent, child order, unfilled reason, tail-substitute candidate, final fill direction, and fill amount.
- Main planning documents are `docs/architecture/qe_experiment_data_completeness_prewarehouse_plan_20260503.md` and `docs/architecture/qe_data_completeness_phase1_development_plan_20260504.md`. The future warehouse design must assume QE source DB rows and worker workspace files may be cleaned after archive, so durable analysis must not depend on QE runtime DB or worker workspace paths.
- First implementation foundation is complete in commit `9c9073c feat: add QE data completeness contracts`: `scripts/aistock_validate.py` writes JSON run metadata and evidence manifests; `backend/services/quantevolver/completion_contract.py` defines QE completion payload and artifact-manifest contracts; tests live in `backend/tests/test_aistock_validate_metadata.py` and `backend/tests/unified_engine/test_qe_completion_contract.py`; validation matrix is `tests/aistock_validation/modules/qe_data_completeness.md`.
- Validation evidence is `tests/aistock_validation/history/qe_data_completeness/20260504_015036_l3_qe-data-completeness-contract-and-validation-metadata.md` plus the forced JSON metadata/evidence sidecars in the same directory. Verified commands included targeted compileall/pytest, `qe_data_contract_backend`, targeted L0 guardrail, `qe_archive_backend`, and `qe_archive_data_quality`; production backend `8001` and remote worker APIs were not restarted.
- Current status: contract/schema/validation metadata foundation only. No QE production completion hook is enabled, no QE runtime behavior is changed, no warehouse UI cleanup dependency is assumed, and Python coverage is not yet actually enforced; coverage fields in run metadata are placeholders until `pytest-cov` / coverage parser / gate work is implemented.
- Next continuation should first add the internal validation pipeline coverage implementation plan and then implement coverage parsing/gates, before wiring QE completion payload collection into runtime or archive paths. All future related design docs stay under `docs/architecture`, and all analysis evidence stays under `docs/analysis`.

## Validation Center Bug Workflow Update - 2026-05-04

- Internal validation-center implementation plan is `docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`; v1.1 adds the required development submission flow, nightly/long-run validation policy, Bug registry, and Codex/Claude repair loop.
- Recommended workflow: non-long-running and non-market-dependent work must pass the relevant L0/L1/L2 and, when applicable, L3 UI/API/DB pipeline before GitHub submission. Long-running backtest/QE/market-dependent work must pass fast gates first, then run nightly/L4/L5 in the background; the feature remains `L4 pending` and cannot be marked complete or enabled in production until the scheduled evidence passes.
- Bug record source-of-truth should be GitHub Issues, using issue forms/labels/assignees/milestones and commit/PR links. The AIstock `validation` DB or JSON history should be a local UI/index/cache linked to run metadata, evidence, coverage, failing plan/case, fingerprint, fix commit, verification run, fixed/submitted/closed timestamps, and agent events.
- Effective 2026-05-19, any newly created BUG / Issue must be synchronized to GitHub in the same workflow before commit or merge. BUG JSON records under `tests/aistock_validation/bugs/` must contain `github_issue_number` and `github_issue_url`; local-only records are allowed only as uncommitted triage drafts or dedicated historical cleanup/backfill work.
- Validation Center UI should primarily display Bug description, trigger condition, severity, status, first/last seen, failure count, evidence, assigned agent, fix commit, verification run, fixed/submitted status, and GitHub links. UI must not execute arbitrary shell or mark a Bug fixed without a verification run.
- Codex/Claude repair support should expose machine-readable bug agent-context: reproduce command, failing run/evidence, allowed write scope, suspected files/modules, safety constraints, and required verification commands. Agents should update Bug status through GitHub CLI/API or Validation API after reproducing, fixing, testing, committing, and linking evidence.

## Development Standards / Guardrails Framework - 2026-05-04

- Current canonical project development standard as of 2026-05-23: `docs/standards/aistock_development_standard_v1.5_20260523.md`. This is the human-readable authority for Python engineering, quant/trading engineering, QE/warehouse completeness, UI, testing, agent workflow, documentation placement, and detailed-design delivery governance.
- Same-version machine-readable standard: `docs/standards/aistock_development_standard_v1.5_20260523.yaml`. Every enabled rule and manual review control must reference the human standard with `standard_ref`; old machine rules are archived under `docs/standards/archive`.
- Guardrail framework design document: `docs/architecture/aistock_development_standards_and_guardrails_20260504.md`. It is implementation design only, not a competing standards source; if it conflicts with `docs/standards`, `docs/standards` wins.
- Required implementation path: update the human standard first, update the same-version YAML in the same change, run a read-only full-repo baseline scan, save the human summary to `docs/analysis/aistock_guardrail_baseline_YYYYMMDD.md`, then make `nox -s l0` block new/changed P0/P1 violations after rule calibration.
- Historical AIstock code is expected to contain many legacy violations from exploratory multi-tool development. Do not try to fix all historical findings in one large change; treat them as baseline/backlog, prioritize P0 workspace/production/fail-fast risks first, and burn down by module with regression tests.
- Violating development standards is a quality or architecture defect even if the feature appears to work. New code must not directly read WSL/remote worker workspaces, restart production `8001`, silently fallback or fake success, create DB schema without comments, store architecture/analysis docs outside approved directories, or bypass run evidence.

## Design Compliance Update - 2026-05-20

- Canonical project development standard is now `docs/standards/aistock_development_standard_v1.5_20260523.md`, with machine catalog `docs/standards/aistock_development_standard_v1.5_20260523.yaml`; v1.4 is archived under `docs/standards/archive`.
- New P0 rule DESIGN-COMPLIANCE-001 requires a design acceptance matrix before reporting completion, requesting Main merge, closing an issue, or marking a feature verified. The matrix must map each design/user requirement item to implementation references, real validation evidence, status, and any approved exception.
- Pipeline success is not sufficient when the pipeline does not cover the design: UI/API/DB/MCP/QE/RP/Paper/HMM requirements need real route, DB side-effect, run record, E2E, screenshot, or controlled smoke evidence.
- Future Codex App, Codex CLI, and Claude Code sessions must not silently downgrade a design into a simplified version or subset; if full implementation is blocked, stop and ask the user to approve the changed scope.

- Validation Center implementation plan now depends on the guardrail framework: `docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md` should surface guardrail quality gates, bug records, agent-context, and eventual UI visibility for baseline/new violations.

## Development Standards Phase 1 Execution - 2026-05-04

- Canonical project development standard is now `docs/standards/aistock_development_standard_v1.5_20260523.md`. It intentionally combines Python engineering rules, quant/trading engineering rules, documentation delivery governance, and detailed-design Main-submission rules into one implementation-facing document. Future standard revisions must create new versioned files and move the previous version to `docs/standards/archive`.
- The current machine-readable guardrail catalog is `docs/standards/aistock_development_standard_v1.5_20260523.yaml`; `scripts/aistock_guardrail_scan.py` defaults to it. The scanner supports regex, path_regex, loop-window DataFrame checks, tracked baseline scan, changed-files scan, JSON output, Markdown summary, and severity fail thresholds.
- Added scanner tests in `backend/tests/test_aistock_guardrail_scan.py`. Verified catalog loading, regex compilation, silent-fallback detection, test-path exclusions, JSON/Markdown output, and compileall.
- First read-only tracked-files baseline report is `docs/analysis/aistock_guardrail_baseline_20260504.md`; full local JSON is `tmp/validation/guardrails/baseline_20260504.json` and should not be committed. After v1.1 standards update, the current baseline scans 1,039 tracked files and finds 1,632 review findings: P0=341, P1=109, P2=1,182, P3=0. Treat this as historical baseline, not immediate full-repo blocking.
- Do not wire changed-files blocking into `nox -s l0` until the baseline and first false positives are reviewed. Next recommended step is rule calibration, then changed-files P0/P1 gate with baseline suppression.

## Development Standards Phase 1.1 Update - 2026-05-04

- Historical note: v1.1 previously lived at `docs/standards/aistock_development_standard_v1.1_20260504.md` with machine-readable YAML `docs/standards/aistock_development_standard_v1.1_20260504.yaml`; as of 2026-05-19 both are archived under `docs/standards/archive` and v1.2 is active.
- v1.1 adds project directory/root-pollution rules, `debug_tools/` as the required location for one-off test/diagnostic scripts, document location rules, Python fail-fast expectations for scripts, DataFrame/big-file memory bounds, resource cleanup, and algorithm complexity review.
- `scripts/aistock_guardrail_scan.py` now defaults to the v1.2 YAML and supports `path_regex` rules for path/location checks such as root pollution and one-off script placement, plus a bounded loop-window checker for DataFrame concat patterns without catastrophic regex backtracking.

## Data Sync Autonomy Design and Development Standard v1.2 - 2026-05-19

- Detailed design document: `docs/architecture/data_sync_autonomous_control_plane_design_20260519.md`. It records the local data-management root cause for `cyq_perf`, the autonomous daily data-sync control plane, release/deadline policy, persistent retry queue, reconciliation, alert gate, dashboard cache semantics, strict L0-L5 test plan, result-data validation SQL, and Main acceptance criteria.
- `cyq_perf` root cause: the scheduler still routes `cyq_perf` / `cyq_chips` through `scripts/ingest_tushare_cyq.py`; that legacy path writes the physical table and job/log/progress rows but does not write `market.dataset_date_refresh_audit`, while the dashboard reads cached `market.data_stats`. Job `success` therefore does not equal audit-backed readiness.
- `market.dataset_date_refresh_audit` is the readiness authority for daily data; `market.data_stats` is a dashboard/gap-query cache that may be stale and must be rebuilt or marked stale from audit/physical reconciliation. Do not treat `data_stats.max_date` or `ingestion_jobs.success` as the final business-ready signal.
- Development standard upgraded to `docs/standards/aistock_development_standard_v1.5_20260523.md` with machine catalog `docs/standards/aistock_development_standard_v1.5_20260523.yaml`; v1.4 is archived under `docs/standards/archive`. The guardrail scanner default catalog and tests now point to v1.5.
- New governance rule: completed detailed design deliverables must include strict test cases, test plan, result-data validation method, and Main acceptance criteria, then be validated, committed, and pushed to `origin/main` unless the user explicitly exempts them. This rule covers documentation/design delivery only; runtime code, DB migrations, schedulers, production data repair, and strategy assets still require an independent development branch, automated pipeline validation, and user confirmation before Main merge.

## Tushare ST Events Local Dataset - 2026-05-04

- Added first-phase local ST event ingestion dataset `stock_st_events` backed by Tushare `st`, stored in `market.stock_st_events`; it is separate from the existing daily ST snapshot table `market.stock_st`.
- The table uses `pub_date` as the announcement publication date, `imp_date` as the formal implementation date, maps upstream `st_tpye` to local `st_type`, preserves `st_reason/st_explain`, and registers `market.data_stats_config` with `date_sequence=calendar`, `cursor_source=refresh_audit`, `source_api=tushare.st`, and `updated_column=ingested_at` for dashboard stats, sparse-date refresh tracking, last-update tracking, and auto-range.
- `DatasetSpec` now supports `date_param_name` for BY_DATE APIs that do not use `trade_date`; `stock_st_events` sets `date_param_name=pub_date`, `row_limit=1000`, `replace_existing_dates=True`, and an audit-backed incremental cursor so successful empty announcement dates do not cause repeated catch-up from the last non-empty pub_date.
- Ingestion auto-range and the background scheduler now distinguish calendar-date datasets from trading-date datasets through `data_stats_config.extra_info.date_sequence`; calendar datasets advance by one natural day and catch up to `CURRENT_DATE`.
- Local-data dashboard integration includes init/incremental dropdowns, truncation support, data-stats categorization, row-level fill-to-latest, and daily schedule quick-create at 20:40. A small smoke sync for 2026-04-24..2026-04-30 inserted 119 rows, then a 2026-05-01..2026-05-04 catch-up inserted 0 rows but recorded successful audit dates through 2026-05-04; `market.data_stats` was refreshed. No historical full backfill and no H5/Bin/PIT universe export has been run yet.
- The `add-tushare-dataset` skill was amended to require data dashboard/statistics/one-click update integration and special handling for non-trading-date BY_DATE datasets.

## PIT Stock Universe Foundation - 2026-05-04

- Backfilled `market.stock_st_events` from 2018-08-01 through 2026-05-04 via Tushare `st`: 2,834 successful daily batches, 0 failed batches, 1,825 rows, 754 distinct stocks, `pub_date` range 2018-08-08 to 2026-04-30; audit rows now cover every calendar day in that range.
- Updated `stock_basic` ingestion to fetch list statuses L/D/P through single-call parameter sets; local `market.stock_basic` now includes 5,512 listed rows and 325 delisted rows. Tushare returned 0 paused-listing (`P`) rows at this time, but the PIT generator handles P as terminal when present.
- Added `scripts/build_stock_universe_pit_spans.py`, which creates commented DB tables `market.stock_universe_pit_spans` and `market.stock_universe_pit_events`, registers `stock_universe_pit_spans` in `market.data_stats_config`, and writes JSON + all.txt-style reports under `reports/stock_universe_pit`.
- Current first-stage rule version is `st_pub_next_trade_delist_pause_pit_v1`: SH/SZ only, IPO warm-up 365 days, seed already-ST names at universe start from `market.stock_st`, negative ST exits from first trading day after `pub_date`, true ST removals re-enter from `max(next_trading_day(pub_date), imp_date)`, delisting-board/stock_basic-D exits are terminal, and paused-listing P rows would be terminal.
- Final generated universe `shsz_pit_v1` for 2018-08-01 through 2026-05-04 has 5,577 spans across 5,318 eligible instruments, 2,233 classified events (`st_negative=1408`, `st_restore=343`, `delisted=321`, `delist_event=161`), 249 multi-span instruments, and zero validation errors for invalid spans, overlapping spans, negative-event action-date coverage, or terminal re-entry.
- Additional DB smoke checks passed: no uncommented columns in the new PIT/ST tables, no overlapping PIT spans, no delisted stock eligible on 2026-05-04, and 0 of 235 `market.stock_st` current ST names on 2026-04-30 were still eligible in `shsz_pit_v1`.
- `market.data_stats_config` for `stock_universe_pit_spans` now uses `eligible_end` plus `updated_column=generated_at` and `is_timeseries=false`, so the local-data dashboard shows row count and latest PIT coverage through 2026-05-04 rather than treating span rows as daily bars.
- `scripts/qlib_authoritative_bin_export.py` now supports `--stock-universe-mode pit_spans --universe-key shsz_pit_v1`; in this mode CSV export resolves codes from `market.stock_universe_pit_spans`, Qlib `instruments/all.txt` is rewritten with multi-segment PIT spans, and validation uses the same PIT universe key. Legacy static export remains the default for compatibility.
- H5/Bin data was not regenerated or replaced in production. Next work should expose PIT mode in the Qlib UI/API export path and add H5/no-alpha loader filtering before promoting any new dataset as authoritative.

## PIT Qlib Export API/UI and LGB Smoke - 2026-05-04

- Qlib Bin export API/UI now exposes `stock_universe_mode` with `pit_spans` / `legacy_static` options and `universe_key` (default UI value `shsz_pit_v1`). PIT mode rewrites stock `instruments/all.txt` from `market.stock_universe_pit_spans`; incremental V2 fails fast if the requested stock-universe mode/key differs from full-export metadata.
- `scripts/qlib_authoritative_bin_export.py` PIT mode now writes future metadata with `ipo_filter_mode=pit_universe_spans` plus `stock_universe_mode`, `universe_key`, and `all_txt_rewrite`; CLI default remains `legacy_static` for compatibility, UI default is PIT.
- Small non-production daily Bin smoke candidate `qlib_bin_pit_smoke_lgb_202001_202112_220` was generated for 2020-01-01..2021-12-31 using `shsz_pit_v1`, 220 explicit stocks, 47 multi-span instruments, and no production WSL data replacement. Report: `reports/qlib_authoritative_export/qlib_bin_pit_smoke_lgb_202001_202112_220_stock_daily_all.json`.
- Candidate export validation passed: 220 CSV/feature dirs, `all.txt` 267 rows across 220 instruments, `all_pit_universe_summary.json` `mode=pit_universe_spans`, 106,557 DB-vs-Bin checked rows, 1,278,684 checked values, 0 errors, max abs diff 0.0 for all checked daily/limit fields.
- WSL `rdagent-gpu` Qlib Alpha158 + LGBModel smoke passed against `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220`: train `(44988,159)`, valid `(21549,159)`, test `(26509,159)`, prediction 26,509 rows / 125 days / 214 instruments, and TopK daily backtest completed for 2021-07-01..2021-12-30 with benchmark disabled for the stock-only candidate. Evidence: `tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-validation.md` and result JSON in the same directory.
- Decision: the PIT data path is usable for small LGB/Qlib smoke, but this is not enough to replace production data. Next recommended step is a full-size non-production PIT daily Bin candidate plus broader QE/Qlib validation; production WSL `/home/lc999/data/qlib_bin` replacement still requires explicit user approval.


## QE Minute Runtime Contract Write/Backfill - 2026-05-04

- QE experiment generation and loop-completion write paths now persist an explicit minute runtime contract in `qe_experiments.custom_params`: `runtime_mode=minute`, `bar_freq=1m`, `backtest_freq=1min`, `execution_algo`, `execution_algo_params`, `runtime_contract_version=qe_minute_runtime_contract_v1`, and `runtime_contract_source`.
- `backtest_freq`/`bar_freq` are derived compatibility and audit fields; `execution_algo` and `execution_algo_params` remain the variable runtime policy because V25 is not fixed and future minute execution algorithms may be added.
- StrategyPackage QE source resolution and QE archive source assembly can enrich older experiment rows from explicit `qe_evolution_loops.config_json` or task execution settings, but they do not silently convert daily `CLOSE_PRICE` or no-evidence historical rows into minute runs.
- Historical backfill entry point: `scripts/backfill_qe_minute_runtime_contract.py`. It defaults to dry-run and requires `--write --confirm-write QE_MINUTE_RUNTIME_CONTRACT_BACKFILL` for writes.
- Local DB backfill on 2026-05-04 updated 122 minute-evidence QE rows; follow-up dry-run scanned 455 rows and found 0 remaining updatable rows, with 289 missing rows intentionally skipped for no minute runtime evidence. Target `qe_20260502_231229_0565_L1` now resolves to StrategyPackage with `backtest_freq=1min` and `execution_algo=V25_TWO_STAGE`.
- New generation smoke used `ConfigComposer.compose_experiment_in_memory(skip_db_save=False)` without running a full QE job; the created row had `backtest_freq=1min`, `runtime_mode=minute`, `bar_freq=1m`, `execution_algo=TWAP`, and a minute runner command, then the synthetic row was deleted after verification.

## Full PIT Daily Bin Candidate Validation - 2026-05-04

- Full non-production daily Qlib Bin candidate `qlib_bin_pit_full_20180801_20260430` was generated under `F:/Dev/AIstock/qlib_bin`, with CSV source under `F:/Dev/AIstock/qlib_csv/qlib_bin_pit_full_20180801_20260430/stock_daily`; production WSL `/home/lc999/data/qlib_bin` was not modified.
- Export scope: SH/SZ PIT universe key `shsz_pit_v1`, daily range `2018-08-01` to `2026-04-30`, basis range equal to data range, 5,318 PIT instruments requested, 5,121 instruments written with feature data, 197 PIT instruments skipped because they had no raw price rows.
- Before export, local authoritative repairs were applied for strict limit validation: `001914.SZ` historical `stk_limit` rows from old code `000043.SZ`, `688033.SH` first STAR listing day no-limit sentinel row, and `689009.SH` first two days `pre_close` from Tushare daily. Post-repair PIT kline rows had zero missing `stk_limit` matches.
- Dump succeeded through WSL `dump_bin.py dump_all`; candidate has 1,878 calendar rows, 61,452 feature bin files, 5,368 `instruments/all.txt` PIT span rows across 5,121 feature instruments, 237 multi-span instruments, and zero overlapping spans.
- Full DB-vs-Bin value validation was run as 8 shards and aggregated: 5,318 universe stocks, 8,237,832 checked rows, 98,853,984 checked field values, 0 errors, max abs diff 0.0 for daily OHLCV, factor, limit price, prev_close, limit_up, and limit_down fields. Aggregate report: `reports/qlib_authoritative_export/qlib_bin_pit_full_20180801_20260430_stock_daily_validate_aggregate.json`.
- WSL Qlib read smoke passed against `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_pit_full_20180801_20260430`: calendar 2018-08-01..2026-04-30, `D.list_instruments("all")` count 3,259 on 2020-01-02 and 4,871 on 2026-04-30, sample reads had 0 NaNs, and patched `688033.SH` 2019-07-22 limit fields were readable.
- Short-window Alpha158 + LGBModel + TopK daily backtest smoke passed on the full candidate: train `(615525,159)`, valid `(166898,159)`, test `(211445,159)`, prediction 211,445 rows / 43 days / 4,942 instruments, test IC mean about 0.01796, RankIC mean about 0.02019, and a 42-row daily backtest completed. This validates data usability, not alpha quality.
- Evidence: `tests/aistock_validation/history/qlib_data/20260504_l4_pit-full-daily-bin-validation.md`, Qlib read JSON, LGB result JSON, and validation reports under `reports/qlib_authoritative_export/validate_shards/`. Guardrails passed: targeted `py_compile`, 6 targeted pytest tests, frontend `npm run build`, and targeted `git diff --check` with CRLF warnings only.
- Decision: daily Bin candidate is eligible for production replacement review from a data-correctness and Qlib-usability perspective, but replacement was not performed. Any production promotion must be explicitly approved, back up `/home/lc999/data/qlib_bin`, copy the candidate to WSL ext4 storage, and rerun Qlib/QE smoke against the production path.

## Docker Desktop Disk Cleanup Progress - 2026-05-04

- Current continuation document for disk cleanup is `docs/operations/docker_desktop_disk_cleanup_progress_20260504.md`; read it before continuing Docker Desktop, Prometheus, WSL VHDX, or host disk cleanup work.
- Prometheus cleanup was completed on 2026-05-03: `monitoring_prometheus_data` is now about 28GB and Prometheus retention is `2w` / `30GiB` with admin API enabled.
- Active TimescaleDB volume `10f3de2305f0333f10f4ceaeecf00e99491dc45a6ac960effcc66616de93e1b4` is about 113.8GB and must not be pruned as a Docker volume.
- Docker internal disk used is about 145.7G, while `F:\DockerData\wsl\disk\docker_data.vhdx` is about 303.55GB; VHDX compaction may recover roughly 150GB but requires stopping Docker/WSL and will interrupt database containers.
- Other major host-side files noted for later analysis: `F:\WSL\Ubuntu\ext4.vhdx` about 927.82GB and `F:\wsl-swap.vhdx` about 256.01GB.

## ST-only PIT Active Universe Implementation - 2026-05-04

- Current first-stage authoritative ST-only PIT universe key is `shsz_st_pit_active_v1`, rule version `st_pub_next_trade_restore_active_l_v1`, scope `st_only_active`. It intentionally implements only ST exits/restores: negative ST exits from the first trading day after `pub_date`, true ST removals re-enter from `max(next_trading_day(pub_date), imp_date)`, and current stock_basic D/P stocks are excluded by active-list scope rather than delisting/pause PIT.
- Source tables remain raw/authoritative (`market.stock_basic`, `market.stock_st`, `market.stock_st_events`). Successful Tushare sync of any of these source datasets marks `market.stock_universe_pit_state` dirty and attempts a non-strict rebuild; H5/Bin PIT export paths strict-ensure the derived spans before use.
- Derived/dashboard tables are `market.stock_universe_pit_events`, `market.stock_universe_pit_spans`, and `market.stock_universe_pit_state`; all are registered in `market.data_stats_config` and surfaced in the local-data dashboard with a manual rebuild button.
- Current local state after validation: ready, dirty=false, 2018-08-01..2026-04-30, source SHA `d8a6bd97a42d1ff1537990f7e8c3b955b85638c850c0196cfe602f78a50cdbba`, 1,886 ST events (`st_negative=1543`, `st_restore=343`), 5,372 spans, 5,117 eligible instruments, 4,880 eligible on 2026-04-30, 245 multi-span instruments, and zero validation errors.
- Qlib H5/Bin export UI/API now support `stock_universe_mode=pit_spans` with default key `shsz_st_pit_active_v1`; PIT metadata records `st_pit=true`, `delist_pit=false`, `pause_pit=false`. Daily H5 PIT smoke passed; minute H5 direct smoke exceeded the interactive validation window, so candidate minute export validation remains required before any minute dataset promotion.
- Validation evidence is `tests/aistock_validation/history/qlib_data/20260504_l3_st-pit-active-derived-universe-implementation.md`. Production Qlib data was not replaced.

## ST-only PIT Full Candidate Overnight Export Validation - 2026-05-05

- Completed non-production ST-only PIT candidate validation for universe `shsz_st_pit_active_v1` / rule `st_pub_next_trade_restore_active_l_v1`. Production WSL datasets `/home/lc999/data/qlib_bin` and `/home/lc999/data/qlib_minute_bin` were not replaced, and full minute/5min/10min H5 export was intentionally skipped.
- Candidate artifacts: daily/aux H5 snapshot `qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260430`, daily Bin `qlib_bin/qlib_bin_st_pit_active_daily_candidate_20180801_20260430`, and 1min Bin `qlib_bin/qlib_bin_st_pit_active_minute_candidate_20240102_20260430` with source CSVs under `qlib_csv/qlib_bin_st_pit_active_minute_candidate_20240102_20260430/stock_minute_1min`.
- H5 daily/aux validation passed for `daily_pv.h5`, `daily_basic.h5`, `moneyflow.h5`, `bak_basic.h5`, `margin_detail.h5`, `cyq_perf.h5`, `sector_data.h5`, and `static_factors.parquet`; H5 `all.txt` has 5,372 rows across 5,117 instruments, 245 multi-span instruments, zero BJ rows, and zero overlaps.
- Daily Bin candidate validation passed after repairing sparse calendar alignment for `600610.SH`: aggregate report checked 18 representative stocks, 32,618 rows, 391,416 values, error_count=0, max diff=0.0. Daily all.txt has 5,372 PIT rows across 5,117 instruments.
- Full 1min Bin export and validation passed: 5,083 CSV files, 72.18GB CSV, 61,000 bin files, 30.54GB bin, 5,130 PIT all.txt rows, 47 multi-span instruments, 0 zero-byte CSVs. Full validation checked 5,083 stocks, 2,835,050 stock-dates, 682,238,616 DB rows, 8,189,648,292 field values, error_count=0; targeted validation checked 15 stocks, 2,028,660 rows, 24,343,920 values, error_count=0.
- QE-like usability validation passed: daily Alpha158 LGB smoke trained/backtested on the daily candidate (`train=(616533,159)`, `valid=(167170,159)`, `test=(211811,159)`, prediction 211,811 rows, test IC mean about 0.01534 and RankIC mean about 0.02385); daily Bin + H5 multi-dataset smoke passed across daily_basic/moneyflow/bak_basic/cyq_perf/sector_data; day+1min NestedExecutor minute smoke passed with 13 stocks, 8 trade days, all 104 stock-days having 240 non-null 1min bars, and portfolio report returned.
- Validation record: `tests/aistock_validation/history/qlib_data/20260505_l5_st-pit-active-full-candidate-overnight-validation.md`. Main reports include `reports/qlib_st_pit_active_h5_daily_candidate_validate.json`, `reports/qlib_authoritative_export/st_pit_all_dataset_integrity_summary.json`, `reports/qlib_authoritative_export/st_pit_active_daily_lgb_smoke_result.json`, `reports/qlib_multi_dataset_smoke_st_pit_active_20180801_20260430/report.json`, and `reports/qlib_authoritative_export/st_pit_active_minute_chain_smoke/report.json`.
- Residual decision: candidate is ready for production replacement review from the validated data-correctness/Qlib-usability perspective, but no production replacement should occur without explicit user approval, backup of current WSL datasets, copy to WSL ext4 paths, and final smoke against the production paths.

## ST-only PIT Manual QE Candidate Validation - 2026-05-05

- Ran a manual QE-style validation against the non-production candidate paths only: H5 daily/aux snapshot `/mnt/f/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260430`, daily Bin `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_daily_candidate_20180801_20260430`, and 1min Bin `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_minute_candidate_20240102_20260430`; production WSL datasets were not replaced.
- Workspace: `rdagent_assets/qe_workspace_st_pit_validation/qe_manual_st_pit_candidate_20260505`. The run used LGBModel, `m_intraday_range_ratio_5d` custom H5 factor plus Alpha158, `topk_dropout_conservative`, and TWAP nested 1min execution over a short 2026-04-20..2026-04-28 test window. Return code was 0.
- Manual QE outputs passed: factor preparation wrote 8,219,924 custom-factor rows, Qlib initialized with the candidate day/1min providers, LGB training produced `pred.pkl`, and 1min nested backtest wrote `port_analysis_1day.pkl`, `indicator_analysis_1day.pkl`, `qlib_results_enhanced.json`, and `qlib_results_llm.json`.
- Key smoke metrics: IC `0.00766`, ICIR `0.15866`, RankIC `-0.00906`, final NAV `1.007224` over 7 trading days, annualized return with cost `0.25823`, max drawdown `-0.01795`, FFR `0.97561`, final stock count 30. These validate runtime/data usability, not alpha quality.
- Post-run PIT membership audit passed: `pred.pkl` had 39,425 prediction rows across 8 dates / 4,939 instruments, with 0 rows outside candidate daily `all.txt` and 0 rows outside candidate minute `all.txt`; 35 buy orders across 35 instruments also had 0 daily/minute PIT violations.
- Evidence: `tests/aistock_validation/history/qlib_data/20260505_l4_st-pit-active-manual-qe-validation.md`, `reports/qlib_authoritative_export/manual_qe_st_pit_validation/manual_qe_candidate_summary.json`, `manual_qe_candidate_result.json`, `manual_qe_candidate_run.log`, and `manual_qe_candidate_pit_membership_check.json`. Residual risk: generated benchmark parquet ended at 2026-03-10, so benchmark-relative metrics are not authoritative for the April 2026 test window.

## ST-only PIT Local Production Replacement - 2026-05-05

- Per explicit user approval, replaced local production data only and did not create backups; remote machine datasets were not touched. Replaced targets: `/home/lc999/data/qlib_bin`, `/home/lc999/data/qlib_minute_bin`, `/home/lc999/data/factor_data`, `F:/Dev/AIstock/qlib_snapshots/qlib_test`, and `F:/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data`.
- Replacement sources were the validated ST-only PIT candidates: H5 daily/aux `qlib_st_pit_active_h5_daily_candidate_20180801_20260430`, daily Bin `qlib_bin_st_pit_active_daily_candidate_20180801_20260430`, and 1min Bin `qlib_bin_st_pit_active_minute_candidate_20240102_20260430` for universe `shsz_st_pit_active_v1` / rule `st_pub_next_trade_restore_active_l_v1`.
- Post-replacement integrity checks passed: daily target 470M with 5,372 all.txt rows, minute target 31G with 5,130 all.txt rows, factor/H5 targets 6.2G with 5,372 all.txt rows, and rsync dry-run comparisons from candidate to production targets showed no differences for daily Bin, 1min Bin, and `qlib_test`.
- Production Qlib/H5 smoke passed and wrote `reports/qlib_authoritative_export/st_pit_production_replacement_smoke.json`: day calendar 2018-08-01..2026-04-30, 1min calendar 2024-01-02 09:30..2026-04-30 15:00, daily/minute sample reads worked, and all required daily/aux H5 plus `static_factors.parquet` loaded.
- Data Doctor after metadata schema sync passed static factor, H5 freshness, sector coverage, WSL path, and metadata checks. The remaining `qlib_bin` FAIL is a known stale-folder-selection heuristic that prefers legacy `qlib_bin_20260430_shsz_current_candidate`; do not run `rdagent-data-doctor --fix` until that heuristic recognizes ST PIT candidate names.
- Production-path manual QE validation passed using `/home/lc999/data/qlib_bin`, `/home/lc999/data/qlib_minute_bin`, and `/home/lc999/data/factor_data`. Run evidence: `reports/qlib_authoritative_export/manual_qe_st_pit_production_validation/manual_qe_production_result.json` and workspace `rdagent_assets/qe_workspace_st_pit_production_validation/qe_manual_st_pit_production_20260505`.
- Manual QE PIT audit passed: `pred.pkl` had 39,417 prediction rows across 4,938 instruments with 0 daily/minute PIT membership violations, and 36 buy orders had 0 daily/minute PIT membership violations. Validation record: `tests/aistock_validation/history/qlib_data/20260505_l4_st-pit-production-replacement-validation.md`.

## RDAgent Data Doctor Skill + UI Export Coverage - 2026-05-05

- Updated local skill `C:/Users/lc999/.codex/skills/rdagent-data-doctor` for the current ST-only PIT production dataset: daily Bin and 1min Bin metadata validation, ST PIT authority fields, `margin_detail.h5`, 122 parquet fields, 120 schema fields, and safe `--fix` that only syncs verified ST PIT candidates.
- Data Doctor revalidation passed all checks and saved JSON evidence at `reports/qlib_authoritative_export/rdagent_data_doctor_st_pit_skill_update_20260505_rerun.json`: daily Bin WSL target has 5,372 PIT `all.txt` rows, 1min Bin WSL target has 5,130 PIT `all.txt` rows, all tracked H5 files are fresh through `2026-04-30`, and metadata sync is complete.
- Qlib export UI coverage review: local-data dashboard covers raw-data init/incremental updates, schedules, ST PIT status, and manual ST PIT rebuild; Qlib page covers H5 full/per-dataset incremental export, `static_factors.parquet`, field map, and Qlib Bin full/incremental export for stock daily and stock 1min with default `pit_spans` key `shsz_st_pit_active_v1`.
- Known UI lifecycle gaps: H5 one-click `incremental_all` updates daily/auxiliary H5 files but not `minute_1min.h5`; stock Bin incremental may fail fast if qfq basis extension requires full rebuild; candidate-to-production promotion/replacement for `/home/lc999/data/qlib_bin`, `/home/lc999/data/qlib_minute_bin`, and `/home/lc999/data/factor_data` is still manual/scripted, not a normal UI operation.
- Validation record: `tests/aistock_validation/history/qlib_data/20260505_l2_rdagent-data-doctor-skill-ui-export-coverage.md`; DB snapshot evidence: `reports/qlib_authoritative_export/ui_data_export_coverage_db_snapshot_20260505.json`.

## Unified Event Risk Policy Runtime - 2026-05-05

- Added a unified Selection/Paper runtime risk-policy contract under `backend/services/selection_center/risk_policy.py`. The implemented provider is `st_pit`, backed by `market.stock_universe_pit_spans`; the reserved `announcement_risk` provider intentionally fails fast until announcement risk events are implemented.
- `runtime_profile.risk_policy` now normalizes through `backend/services/selection_center/runtime_profile.py` with fields for `enabled`, `policy_version=stock_event_risk_policy_v1`, providers, ST universe key, hard actions (`block_buy`, `force_exit`), visible-time mode, strict data readiness, and future score overlay caps. Unknown runtime-profile keys still fail validation.
- Selection Center now applies the unified risk policy before suspend/industry tradability filtering. Hard blocks create `SelectionExclusion(reason=risk_policy_block_buy)`, while future score/weight overlays are traced in `component_scores.event_risk` without changing the Selection/Paper adapter contract.
- Paper v2 day-runner, readiness, and live-session preparation now use the same risk decision contract. If an existing holding is outside the active risk universe, the target layer adds an explicit zero-quantity `risk_policy_forced_exit` target; execution still handles suspend/limit/no-bar as market states, not silent deletion.
- QE config generation can now emit a wider Qlib Exchange quote/sell universe through `custom_params.quote_universe_codes` while keeping `market/all.txt` as the buy/selection universe, and records a commented `risk_policy` contract trace. The next section records the follow-up closure that adds QE strategy-template forced-exit runtime support.
- Validation passed: `pytest backend/tests/selection_center backend/tests/paper_trading_v2 backend/tests/unified_engine/test_qe_config_truth.py backend/tests/strategy_package/test_rebalance_runtime.py -q -p no:cacheprovider` (142 passed), and `pytest backend/tests/trading_core backend/tests/strategy_package -q -p no:cacheprovider` (91 passed).

## QE Event Risk Policy Runtime Closure - 2026-05-05

- Added frozen QE runtime artifact `qe_event_risk_policy.json` generated by `ConfigComposer` when `custom_params.risk_policy.enabled=true`. The artifact contains the `stock_event_risk_policy_v1` contract, ST PIT spans from `market.stock_universe_pit_spans`, hard actions, coverage dates, and source state metadata; `announcement_risk` still fails fast until the announcement event table/provider is implemented.
- Added `scripts/qe_event_risk_policy.py` and wired the existing QE suspend-filter strategy wrappers to consume it. Qlib backtests now block new buys outside ST PIT eligibility and add explicit forced-exit sell orders for existing holdings that have left the active PIT universe; if a sell is blocked by suspension/limit/no-bar, it remains a market no-fill state rather than being silently deleted.
- QE config generation now auto-populates `exchange_kwargs.codes` from the risk artifact's PIT span symbols when the caller did not provide `quote_universe_codes`, so the quote/sell universe is wider than the buy universe by default. This is required to price and sell positions after they leave `all.txt`.
- ScoreWeighted V1/V2 wrappers now filter buy scores through the same risk artifact and append missing forced-exit orders beyond normal `max_n_drop` turnover limits. TopkDropout wrapper integrates forced exits before buy-slot calculation so forced sells do not double-count buy slots.
- Validation passed after the QE closure: targeted `py_compile`; `pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider` (41 passed); `pytest backend/tests/selection_center backend/tests/paper_trading_v2 backend/tests/unified_engine/test_qe_config_truth.py backend/tests/strategy_package/test_rebalance_runtime.py -q -p no:cacheprovider` (146 passed); and `pytest backend/tests/trading_core backend/tests/strategy_package -q -p no:cacheprovider` (91 passed).

## Mandatory QE ST PIT Risk Policy For New Runs - 2026-05-05

- New runnable QE configs now force the ST PIT event-risk policy by default. `ExperimentConfig.build_custom_params()` injects `risk_policy.enabled=true`, `providers=['st_pit']`, `hard_actions=['block_buy','force_exit']`, universe key `shsz_st_pit_active_v1`, and `strict_data_ready=true`; explicit `risk_policy.enabled=false` fails fast.
- `ConfigComposer._prepare_risk_policy_runtime()` also injects/validates the default for direct composer callers, rebuilds `qe_event_risk_policy.json`, and recomputes `quote_universe_codes` from the current PIT artifact instead of preserving stale inherited quote universes.
- `/api/v1/quantevolver/config/generate` injects the same policy into new single- and Multi-Alpha experiment custom params before persistence. Custom-evo create/clone/rerun/append route preparation also stores the policy in each loop's `strategy_params`, while completed historical experiments are not migrated in place.
- Derived runs from old experiments still get the new logic because single-experiment rerun, standard evolution retry, strategy-evo backtest-only, custom-evo selected-loop execution, and Multi-Alpha root backtest all flow through `ExperimentConfig.build_custom_params()` or `ConfigComposer._prepare_risk_policy_runtime()`.
- Follow-up closure after real QE backtest warnings: the mandatory risk policy now also forces signal-side `suspend_d` filtering, prepares the risk artifact before suspend-filter runtime, and prevents inherited `filter_suspended_on_signal=false` from bypassing the ST PIT/suspend contract.
- `ScoreWeightedTopkStrategy` wrappers now guard suspend/missing-close symbols before current-price reads, filter score/order universes through risk/suspend/missing-close checks, and only append forced-exit orders when orderable; suspension/limit/no-close remains an explicit market no-fill/wait state instead of a default-price fallback.
- Backtest-only WSL validation used source task `qe_20260505_153534_388f` loop 1 with existing recorder `8fdfd04a826a40f89838a9cc26b4a0de`; final task `qe_20260505_200632_a357` completed with 0 Qlib `$close None!!!`, 0 `WARNING - qlib.online operator`, 0 `涔板叆浠锋牸鏃犳晥`, and 0 Traceback entries. Production backend `8001` was not restarted; temporary backend `8012` and WSL API `9000` were used.
- Validation record: `tests/aistock_validation/history/qe/20260505_182545_l3_qe-mandatory-st-pit-risk-policy-for-new-and-derived-runs.md`. Validation passed: `py_compile`; final unified QE suite (108 passed); Selection/Paper/StrategyPackage regression (151 passed); TradingCore/StrategyPackage regression (91 passed); ST PIT targeted tests (10 passed); frontend build passed. No production datasets, model weights, HMM snapshots, manifests, or validated policies were modified by the code validation pass.

## Selection/Paper v2 ST PIT Alignment Audit - 2026-05-06

- Detailed note: `docs/analysis/selection_paper_st_pit_alignment_and_blockers_20260506.md`. Evidence artifacts are under `F:/Dev/AIstock_artifacts/selection_blocker_scan_20260506`.
- Current direct Selection Center runs have a mismatch risk: they apply `normalize_selection_runtime_config` but do not automatically inherit the frozen QE backtest contract. The current UI omits `runtime_profile.risk_policy`, so the parsed default is `enabled=false`; new ST PIT QE packages need Selection Center to inherit the same `risk_policy` contract as Paper v2.
- Paper v2 day-runner/readiness/live-session paths already normalize runtime config through `normalize_runtime_config_with_backtest_contract`, so they are closer to QE contract parity, but target generation is still an independent Python port of QE ScoreWeighted V1/V2 and needs parity tests against Qlib strategy decisions.
- The four currently selectable packages scanned on 2026-05-06 all lack manifest `custom_params.risk_policy`, so they are legacy/non-ST-PIT-contract packages. Do not mutate their frozen manifests; either keep them clearly marked as legacy or recreate StrategyPackages from new ST PIT QE backtests.
- Recorded blockers: production `8001` needed restart for the pushed `qe_evolution_loop` source-resolution fix; `pkg_1de...` resolves source but strict live inference keeps 0/4636 fully scored rows; `pkg_006...` has model feature mismatch expected 63 vs actual 52; `pkg_991...` and `pkg_b668...` run from current local cache but fail cold-cache materialization because node `mlruns-params` returns 404; HMM-on selection still has coefficient sector-mapping artifact risk; Paper v2 current-day readiness can be blocked by failed `stk_limit` audit.
- Implementation-order decision: first add package health/preflight and UI run gating so the operator stops finding blockers by clicking manually; then align Selection Center to the frozen QE backtest contract; then repair/rebuild/retire package-specific blockers; then add Paper v2/QE decision parity tests and production activation only after dev-port validation. Details are appended to `docs/analysis/selection_paper_st_pit_alignment_and_blockers_20260506.md`.

## Selection Center ST PIT Health Gate Implementation - 2026-05-06

- Worktree `F:/Dev/AIstock_worktrees/selection-st-pit-health-20260506` on branch `codex/selection-st-pit-health-20260506` implements the first two phases of the ST PIT alignment plan; production backend `8001` was not restarted.
- Added `SelectionPackageHealthService` and included `selection_health` in selectable-package responses. The Paper v2 Selection UI now shows health/preflight status, disables packages whose health is not runnable, and sends `st_pit_authoritative=true`.
- Direct Selection Center ST PIT authoritative runs now call `normalize_runtime_config_with_backtest_contract(...)`, persist per-package effective runtime configs plus `qe_backtest_runtime_contract`, and reject legacy non-ST-PIT packages instead of silently upgrading frozen manifests.
- UI TopN is now `display_top_n`; engine TopK is inherited from the frozen QE contract. This prevents the Selection UI from changing backtested portfolio behavior.
- Validation record: `tests/aistock_validation/history/paper_v2_selection_center/20260506_l3_selection-center-st-pit-health-contract-alignment.md`. Passed: `py_compile`; Selection/StrategyPackage/Paper regression (`142 passed`); QE config/rebalance regression (`49 passed`); frontend `tsc`; frontend `npm run build`; `git diff --check` with line-ending warnings only. No protected assets were modified.

## Paper v2 ST PIT Risk/Readiness Closure - 2026-05-06

- Follow-up in the same branch stayed within `backend/services/paper_trading_v2` plus Paper v2 tests/docs; no QE shared implementation files were modified and production backend `8001` was not restarted.
- Added a Paper v2 forced-exit target overlay so ST PIT/risk-policy forced exits replace same-symbol QE-style ghost/sell targets instead of appending duplicate target rows. Day-runner, readiness, and live-session paths now share this helper.
- Paper v2 readiness now loads DB historical first-observed minute close for existing positions when `current_prices` are absent, matching the day-runner equity path before target/rebalance checks.
- Validation record: `tests/aistock_validation/history/paper_v2_selection_center/20260506_l3_paper-v2-st-pit-risk-readiness-closure.md`. Passed: Paper v2 focused tests (`4 passed`), full Paper v2 suite (`69 passed`), Selection/StrategyPackage/Paper regression (`145 passed`), QE config/rebalance regression (`49 passed`), frontend `tsc`, frontend `npm run build`, and guardrail scans. No protected assets or production data were modified.

## HMM Regime Redefinition Handoff - 2026-05-10

- Current HMM continuation document: `docs/analysis/hmm_regime_redefinition_qe_handoff_20260510.md`; read it after this memory file when resuming HMM/QE work.
- Completed comparison task `qe_20260510_010004_8c2d` showed no-HMM still best versus the tested old/new HMM overlays; do not promote a new HMM candidate from that round.
- Completed task `qe_20260510_102726_4fd3` was a four-loop backtest-only HMM regime-redefinition validation on `rdagent-node1` with parallelism 4 and unchanged non-HMM settings from `qe_20260502_131502_9b54` Loop1. It again showed no-HMM as best: Loop1 annual return 38.18%, Sharpe/IR 1.690, max drawdown -15.50%; best HMM Loop2 annual return 37.75%, Sharpe/IR 1.653, max drawdown -17.21%.
- Cleanup note: `rdagent_assets/qe_experiments` is not the local disk-space bottleneck; keep `backend/data/hmm_models` and current task assets, and only delete old completed QE artifacts after a keep/delete classification.


## Production DDL Activation Rule - 2026-05-21

- Current canonical project development standard is `docs/standards/aistock_development_standard_v1.5_20260523.md`, with machine catalog `docs/standards/aistock_development_standard_v1.5_20260523.yaml`; v1.4 is archived under `docs/standards/archive`.
- Mandatory rule: when a merged `main` change includes production DB DDL or runtime code depends on new DB objects, apply the committed production migration to the production DB immediately after `main` merge and before production runtime activation/restart. If production DDL cannot be executed or verified, stop and report `production_ddl_pending`; do not report the feature as production-ready.
- Every feature/fix handoff must include `production_ddl_gate`: `applied` with migration file and validation evidence, `noop` when no DB DDL exists, or `pending/blocking` when production DDL could not be safely applied. Never again leave production code running against a schema missing required runtime tables/columns.
- Required evidence after production DDL: target DB preflight without secrets, before/after `to_regclass` or catalog checks, table/column/index/constraint/comment verification, API/scheduler/log smoke, and a validation record under `tests/aistock_validation/history/`.

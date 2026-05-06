# Progress Log

## Session: 2026-04-16

### Phase 7: 评级v1与管理工具栏设计
- **Status:** complete
- Actions taken:
  - 将日频低换手与选股稳定性要求纳入正式评级v1设计。
  - 明确唯一正式评级应锚定到当前 AIstock 日频多Alpha生产场景，而不是泛化学术评级。
  - 设计了版本化规则管理、UI单一入口执行流、数据库分表方案与工具栏能力。
  - 明确 v1 使用数据库现有 `turnover` 作为选股稳定性 proxy，后续可在 v2 增加更精确的篮子重合率指标。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

- **Status:** complete
- Actions taken:
  - 基于现有因子评级冲突，设计了唯一权威的统一规则评级方案。
  - 明确正式评级必须由单一规则引擎产出，LLM 只做补充审核与说明。
  - 明确所有评级输入必须统一从数据库读取，禁止任何文件侧读数参与正式评级。
  - 评估了方案合理性与局限性：方案方向正确，但规则必须版本化并定期校准。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)


### Phase 1: Requirements & Discovery
- **Status:** complete
- **Started:** 2026-04-16
- Actions taken:
  - 读取记忆与当前代码，确认用户关注的因子值缓存缺口。
  - 检查后端 router、前端入口、backfill 脚本、pipeline、执行层 prepare_factors 注入逻辑。
  - 确认执行层已具备按需自动写缓存与下次复用能力。
  - 精确识别到正在运行的 `backfill_factor_cache.py` 任务并按用户授权终止。
  - 建立 planning-with-files 所需文件。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (created)
  - `F:\Dev\AIstock\findings.md` (created)
  - `F:\Dev\AIstock\progress.md` (created)

### Phase 2: Current-State Code Analysis
- **Status:** complete
- Actions taken:
  - 确认 `start_date/end_date` 后端已支持但前端未传。
  - 确认 `--incremental` 仅停留在参数层，没有形成真实的批任务增量语义。
  - 确认存在 `extend_single_factor_cache()` 可作为真正增量方案基础。
  - 确认 pipeline 失败信息被截断，缺少结构化失败日志与前端诊断展示。
  - 确认 `source_hash_raw` 与 `source_hash` 并存，存在对齐需求。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

### Phase 3: Alignment Design
- **Status:** complete
- Actions taken:
  - 明确以执行层当前缓存协议作为对齐基准，不先改 execution-layer 主逻辑。
  - 明确 `source_hash_raw` 统一策略：迁移期读兼容、写统一。
  - 明确低风险 rollout：先对齐 factor value compute/backfill/router/front-end，再增强增量与诊断。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

### Phase 4: Feature Design
- **Status:** complete
- Actions taken:
  - 设计了日期范围选择的前端/API 流程。
  - 设计了真正 incremental + resume/retry_failed_only 的批任务语义。
  - 设计了结构化失败日志与任务诊断链路。
  - 设计了 execution-layer 自动缓存与 backfill 缓存的元数据一致性方案。
- Files created/modified:
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

### Phase 5: Delivery
- **Status:** in_progress
- Actions taken:
  - 整理了交付给用户的方案摘要与实施顺序。
  - 分析了 planning-with-files stop hook 报错，确认问题属于本机 Claude skill/plugin hook 路径解析，而非 AIstock 项目代码。
  - 已落地第一轮开发：统一 `source_hash_raw` 写入、增强 backfill 批任务编排（incremental / task checkpoint / failed.ndjson / resume_task_id / retry_failed_only）、增强后端任务状态接口、增强前端缓存管理入口（日期区间 / incremental / 最近任务 / 日志 / 失败因子）。
  - 对修改后的 Python 文件执行了 `py_compile` 静态编译通过。
  - 前端 TypeScript 未能真实 type-check：当前环境里 `tsc` 仅返回“typescript 未安装”的提示，不是项目代码错误。
- Files created/modified:
  - `F:\Dev\AIstock\scripts\backfill_factor_cache.py` (rewritten)
  - `F:\Dev\AIstock\backend\services\quantevolver\factor_value_pipeline.py` (updated)
  - `F:\Dev\AIstock\backend\routers\quantevolver.py` (updated)
  - `F:\Dev\AIstock\frontend\src\app\quantevolver\components\FactorList.tsx` (updated)
  - `F:\Dev\AIstock\task_plan.md` (updated)
  - `F:\Dev\AIstock\findings.md` (updated)
  - `F:\Dev\AIstock\progress.md` (updated)

## Test Results
| Test | Input | Expected | Actual | Status |
|------|-------|----------|--------|--------|
| Find running factor cache task | Process query for `backfill_factor_cache.py` | Only real factor cache task found | Confirmed WSL PIDs 48420/66384 | ✓ |
| Stop confirmed factor cache task | Stop-Process on confirmed PIDs | Task terminated | PID 48420 stopped, 66384 already exited; no remaining process on recheck | ✓ |

## Error Log
| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| 2026-04-16 | Stop hook requested MemPalace autosave but MCP tool returned internal error | 1 | Ignored for current analysis work |
| 2026-04-16 | PowerShell variable name `$pid` conflicted with built-in `$PID` during process termination | 1 | Renamed loop variable to `$procId` |

## 5-Question Reboot Check
| Question | Answer |
|----------|--------|
| Where am I? | Phase 3: Alignment Design |
| Where am I going? | Phase 4 feature design, then delivery |
| What's the goal? | Design factor cache alignment and enhancements around `source_hash_raw`, dates, incremental resume, logging, and execution-layer reuse |
| What have I learned? | See findings.md |
| What have I done? | See above |

---
*Update after completing each phase or encountering errors*

## Session: 2026-04-26 Paper Trading v2 UI
- **Status:** started
- Actions taken:
  - User confirmed new Paper v2 UI direction.
  - Started detailed design + implementation flow.
  - Will avoid restarting production backend on port 8001; temporary ports only for verification.
- Added `docs/architecture/paper_trading_v2_ui_design.md` with route map, page wireframes, backend API mapping, fail-fast UI contract, and verification plan.

### Paper v2 UI implementation pass
- **Status:** implementation and verification complete
- Actions taken:
  - Added new `/paper-v2` route tree with Overview, Packages, Selection, Portfolios, Portfolio Detail, Run Console, Ledger, Performance, Model & HMM, and Settings pages.
  - Added shared Paper v2 frontend API wrappers, types, formatting helpers, status/error/notice/JSON/table/card/confirmation components, and visual CSS.
  - Added StrategyPackage creation controls for QE experiment and QE evolution loop sources.
  - Added dynamic multi-package Selection Center controls plus existing-run aggregation UI while keeping multi-package Paper execution blocked.
  - Added portfolio readiness/run-day/replay/reset UI, dated validated-execution-policy activation UI, full ledger UI, and performance report UI.
  - Added `/api/v1/paper-v2/portfolios/{portfolio_id}/cash-ledger` repository/router/API client support for cash-ledger traceability.
  - Added Sidebar links for Paper Trading v2 without modifying legacy `/paper-trading/*`.
  - Ran frontend build and route smoke checks on temporary port 3011.
  - Ran backend import/API smoke checks on temporary port 8011 without touching port 8001.
  - Ran relevant backend pytest suite.
- Files created/modified:
  - `docs/architecture/paper_trading_v2_ui_design.md`
  - `docs/codex_project_memory.md`
  - `backend/routers/paper_trading_v2.py`
  - `backend/services/paper_trading_v2/repository.py`
  - `frontend/src/app/Sidebar.tsx`
  - `frontend/src/app/paper-v2/**`
  - `frontend/src/components/paper-v2/**`
  - `frontend/src/lib/paper-v2/**`

## Paper v2 UI Verification Results
| Test | Result |
|------|--------|
| `npm run lint` in `frontend` | Not usable non-interactively; `next lint` prompted to create ESLint config |
| `npm run build` in `frontend` | Passed; all `/paper-v2` routes compiled. After a dev smoke dirtied `.next`, the generated `.next` directory was safely removed and a clean build also passed |
| Backend import smoke | Passed; `backend.main` imported and cash-ledger route was present |
| Temporary backend on port 8011 | Started successfully; `/openapi.json`, `/api/v1/paper-v2/portfolios`, `/api/v1/selection-center/selectable-packages`, and `/api/v1/strategy-packages` returned 200 |
| OpenAPI path check | Passed for cash-ledger, execution-policy activations, selection aggregate-runs, and strategy package creation |
| `pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/test_tushare_sync_engine.py backend/tests/test_hmm_rolling_training.py -q -p no:cacheprovider` | Passed: 94 passed |
| `npm run start -- -p 3011` route smoke | Passed after clean rebuild: `/paper-v2`, packages, selection, portfolios, detail, run-console, ledger, performance, model-hmm, settings returned 200 |
| Browser click automation | Not run; Playwright is not installed in the frontend project |

## Session: 2026-04-26 Paper v2 completion continuation
- **Status:** design completed, backend implementation starting
- Actions taken:
  - Re-read Codex project memory and Paper v2 architecture context.
  - Re-read existing planning files and confirmed previous Paper v2 UI pass left several garbled/unfinished UI paths.
  - Added detailed design document: docs/architecture/paper_v2_ui_selection_portfolio_completion_plan.md.
  - Confirmed DB has target QE experiments and existing packaged StrategyPackages for qe_20260416_002701, qe_20260413_084216, qe_20260416_082012.
- Timestamp: 2026-04-26T23:59:23

## Session: 2026-04-27 QE Config Truthfulness / No-Silent-Override Hardening
- **Status:** started
- Actions taken:
  - Activated planning-with-files for persistent tracking.
  - Re-read docs/codex_project_memory.md.
  - Captured dirty git baseline; many pre-existing modified/untracked files are present.
  - Added new task plan section for this work.


## Session: 2026-04-28 HMM Horizon-Aware v2 Training
- **Status:** started
- Actions taken:
  - Activated `planning-with-files` workflow for persistent tracking.
  - Re-read `docs/codex_project_memory.md` and HMM analysis reports from current context.
  - Captured dirty git baseline; many unrelated untracked files pre-exist.
  - Verified WSL `rdagent-gpu` dependency availability via `.codex_tmp/probe_env.sh`.
  - Added this HMM-specific task plan section.
- Timestamp: 2026-04-28T01:51:39

- Wrote detailed HMM horizon-aware optimization/training plan: `docs/analysis/hmm_horizon_optimization_training_plan_20260428.md`.
- Confirmed local DB has data through 2026-04-27 for sector, sw_daily, index_daily, and kline_daily_raw.
- Timestamp: 2026-04-28T01:55:02

- Created additive scripts: `scripts/hmm_horizon_v2_train.py` and `scripts/hmm_horizon_v2_compare.py`.
- Verified both scripts compile in Windows Python and WSL `rdagent-gpu`; help output works.
- Fixed a Python 3.10 f-string path-conversion syntax issue before training.
- Timestamp: 2026-04-28T02:00:04

- Trained new HMM horizon v2 in WSL `rdagent-gpu`.
- New config: `f1da5529-0109-495f-a2b8-a2033cc31ee8`; snapshot: `77113d1b-1225-4cb2-9d1c-9d0c24f1d130`.
- New model: `backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/models.json`.
- New coefficients: `backend/data/hmm_models/f1da5529-0109-495f-a2b8-a2033cc31ee8/2026-04-28/coefficients_preset_horizon_v2_2025-09-01_2026-03-03.json`.
- Validation calibration boosted the validation-best label (`fading` coefficient 1.020983) and downweighted negative labels (`neutral` 0.992210, `trending` 0.986807).
- Timestamp: 2026-04-28T02:01:33

### HMM Horizon v2 final validation pass
- **Status:** complete
- Actions taken:
  - Re-ran `scripts/hmm_horizon_v2_compare.py` after adding contribution and execution-proxy diagnostics.
  - Confirmed updated outputs at `.codex_tmp/hmm_horizon_v2_backtest_20260428.{json,md}` plus summary/monthly CSVs.
  - Wrote final detailed report: `docs/analysis/hmm_horizon_v2_training_backtest_report_20260428.md`.
  - Updated HMM task plan phases 4 and 5 to complete.
- Key result:
  - New HMM Horizon v2 is a valid isolated training artifact but should not be promoted; it underperformed Raw/no-HMM and old w3 preset_B in PIT-compatible script validation.
- Files created/modified:
  - `docs/analysis/hmm_horizon_v2_training_backtest_report_20260428.md`
  - `scripts/hmm_horizon_v2_compare.py` (added contribution/proxy diagnostics)
  - `.codex_tmp/hmm_horizon_v2_backtest_20260428.json`
  - `.codex_tmp/hmm_horizon_v2_backtest_20260428_summary.csv`
  - `.codex_tmp/hmm_horizon_v2_backtest_20260428_monthly.csv`
  - `.codex_tmp/hmm_horizon_v2_backtest_20260428.md`
- Timestamp: 2026-04-28T02:12:00

## Session: 2026-04-28 HMM w5 zscore PIT Retrain Check
- **Status:** started
- Actions taken:
  - Re-read `docs/codex_project_memory.md` and current HMM planning/findings/progress.
  - Confirmed existing diagnostic w5 zscore snapshot used Train 2023-01-30 ~ 2026-01-23 and Validation 2026-01-26 ~ 2026-04-24, which overlaps the 2025-09-01 ~ 2026-03-03 script backtest.
  - Added PIT retrain task plan using Train 2022-09-01 ~ 2025-05-30 and Validation 2025-06-02 ~ 2025-08-29.
- Timestamp: 2026-04-28T02:20:00

### HMM w5 zscore PIT retrain completion
- **Status:** complete
- Actions taken:
  - Trained new PIT-compatible w5/zscore HMM with non-overlapping train/validation windows.
  - Generated `preset_A` and `preset_B` coefficient artifacts for the six-month script backtest window.
  - Registered the new config/snapshot/job in `model_train_configs`, `model_train_snapshots`, and `model_train_jobs`.
  - Re-ran script-only comparison across 9 HMM coefficient artifacts plus Raw/no-HMM.
  - Wrote incremental report: `docs/analysis/hmm_w5_zscore_pit_retrain_report_20260428.md`.
- Key result:
  - Old diagnostic-only w5/zscore +6.46% did not survive PIT retrain; new PIT w5 preset_A = -16.38%, preset_B = -14.98%.
  - Best formal PIT-compatible version remains w3 raw same-params preset_B = -9.48%.
- Files created/modified:
  - `backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/models.json`
  - `backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/coefficients_preset_A_2025-09-01_2026-03-03.json`
  - `backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/coefficients_preset_B_2025-09-01_2026-03-03.json`
  - `backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/training_result.json`
  - `.codex_tmp/hmm_w5_zscore_pit_backtest_20260428.json`
  - `.codex_tmp/hmm_w5_zscore_pit_backtest_20260428_summary.csv`
  - `.codex_tmp/hmm_w5_zscore_pit_backtest_20260428_monthly.csv`
  - `.codex_tmp/hmm_w5_zscore_pit_backtest_20260428.md`
  - `docs/analysis/hmm_w5_zscore_pit_retrain_report_20260428.md`
- Timestamp: 2026-04-28T08:55:00

### HMM leaky w5/zscore hard deletion
- **Status:** complete
- Actions taken:
  - Removed leaking diagnostic `HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore` from `model_train_jobs`, `model_train_snapshots`, and `model_train_configs`.
  - Removed model asset directory `backend/data/hmm_models/be681443-fe5d-4641-b55f-5f889e6af8e1` after verifying the resolved path is inside `backend/data/hmm_models`.
  - Verified DB no longer has config/snapshot/job rows for `be681443-fe5d-4641-b55f-5f889e6af8e1` and the filesystem path is absent.
- Timestamp: 2026-04-28T09:10:00

## Session: 2026-04-28 HMM Daily Coefficient Generation
- **Status:** complete
- Actions taken:
  - Added detailed design doc `docs/architecture/hmm_daily_coefficient_generation_design_20260428.md`.
  - Implemented HMM daily coefficient preview/generate service methods and API routes.
  - Extended `scripts/precompute_hmm_coefficients.py` to remap as-of coefficients to a separate effective trade date.
  - Added `/paper-v2/model-hmm` UI controls for daily HMM coefficient preview/generation and fixed the confirmation component Chinese text.
  - Added backend unit tests and extended Paper v2 Playwright coverage.
- Validation:
  - `pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/test_hmm_daily_coefficients.py backend/tests/test_hmm_rolling_training.py -q -p no:cacheprovider` -> 149 passed.
  - `cd frontend && npx tsc --noEmit --pretty false` -> passed.
  - `cd frontend && npm run build` -> passed.
  - Dev backend 8012 and frontend 3012 UI E2E `npx playwright test --config=playwright.paper-v2.config.ts tests/paper-v2 --reporter=line` -> 12 passed.
- Notes:
  - Production backend 8001 was not restarted.
  - Generated local runtime artifact `backend/data/hmm_models/c095ab83-48f4-453d-9eb9-c1987b6bd7fe/2026-04-28/coefficients_preset_A_2026-04-28_2026-04-28.json` during validation; it is an auditable HMM daily coefficient artifact and should not be committed as source code unless asset versioning is explicitly requested.


## Session: 2026-04-29T01:11:15 HMM Dynamic Coefficient Offline Experiments
- **Status:** started
- Actions taken:
  - Activated planning-with-files for long offline HMM experiment run.
  - Scope locked to HMM scripts/model artifacts/qlib-style validation only; no AIstock backend/frontend code or existing DB HMM versions will be modified.

- 2026-04-29T01:20:42: Created and smoke-tested `scripts/hmm_dynamic_offline_experiments.py`; smoke wrote only `.codex_tmp/hmm_dynamic_offline_smoke` and did not write DB. Starting full 1-year run next.

- 2026-04-29T01:24:34: First full dynamic HMM run completed but was invalidated by NaN posterior/signal handling; patched `forward_posteriors` and coefficient sanitization, rerunning to `.codex_tmp/hmm_dynamic_offline_20260429_v2`.

- 2026-04-29T01:29:00: Full run v2 showed zero confidence/effect. Debug found hmmlearn `_hmmc.forward_log` expects probability start/trans matrices, not log matrices. Patched and rerunning to `.codex_tmp/hmm_dynamic_offline_20260429_v3`.

- 2026-04-29T01:33:31: Full valid v3 run completed. Best QE-ready offline candidate is `dyncoef_pup_blend_k3_clip_0p98_1p02` with total -13.39% vs baseline -21.00%, Sharpe -0.292 vs -0.628, MaxDD -34.13% vs -37.34%. Report written to `docs/analysis/hmm_dynamic_offline_experiment_report_20260429.md`. Verified existing DB HMM config count remained 4.

- 2026-04-29T01:34:11: Removed self-created invalid/smoke temp roots (`hmm_dynamic_offline_smoke`, `hmm_dynamic_offline_20260429`, `hmm_dynamic_offline_20260429_v2`) to avoid confusion; kept only valid `.codex_tmp/hmm_dynamic_offline_20260429_v3`.

## Session: 2026-04-29 HMM Dynamic Micro-Tuning Loop Completion
- **Status:** complete
- Actions taken:
  - Continued from the first dynamic HMM experiment and ran second-pass through eighth-pass offline tuning grids.
  - Added `pup_z` and `pup_rank` support to the offline experiment script for diagnostic relative-PUP tests.
  - Added pass3-pass8 tuning scripts under `scripts/` without modifying AIstock backend/frontend/QE runtime code.
  - Ran all grids in WSL `Ubuntu` conda env `rdagent-gpu` against qlib daily data and DB sector data.
  - Wrote final report `docs/analysis/hmm_dynamic_tuning_final_report_20260429.md` and combined summaries under `.codex_tmp/`.
- Key result:
  - Best: `p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`, total -0.81%, Sharpe 0.142, MaxDD -30.91%, monthly win rate 54.55%.
  - Baseline No-HMM: total -21.00%, Sharpe -0.628, MaxDD -37.34%.
- Validation:
  - Windows and WSL `py_compile` passed for updated/offline HMM scripts.
  - Full pass8 command completed successfully.
  - DB HMM config count check returned 4 unchanged configs.
- Files created/modified:
  - `scripts/hmm_dynamic_offline_experiments.py`
  - `scripts/hmm_dynamic_tuning_experiments.py`
  - `scripts/hmm_dynamic_tuning_pass3_experiments.py` through `scripts/hmm_dynamic_tuning_pass8_experiments.py`
  - `.codex_tmp/hmm_dynamic_tuning_*_20260429/`
  - `.codex_tmp/hmm_dynamic_tuning_combined_summary_20260429.csv`
  - `docs/analysis/hmm_dynamic_tuning_final_report_20260429.md`

## Session: 2026-04-29 HMM DB vs Dynamic 1Y Script Comparison
- **Status:** complete
- Actions taken:
  - Added standalone comparison script `scripts/hmm_db_vs_dynamic_1y_compare.py`.
  - Compared existing DB HMM coefficient artifacts that cover 2025-03-11 ~ 2026-03-03 with the two recommended offline dynamic candidates.
  - Used qlib daily Top50 equal-weight 5D rebalance validation; no QE experiment and no DB write.
  - Wrote outputs under `.codex_tmp/hmm_db_vs_dynamic_1y_20260429/`.
  - Wrote analysis report `docs/analysis/hmm_db_vs_dynamic_1y_comparison_report_20260429.md`.
- Key result:
  - Best PIT-compatible candidate: `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`, total -0.81%, Sharpe 0.142, MaxDD -30.91%.
  - No-HMM baseline: total -21.00%, Sharpe -0.628, MaxDD -37.34%.
  - All full-window DB coefficient artifacts are diagnostic-only for this 1-year window due train/validation overlap.
- Validation:
  - WSL `py_compile scripts/hmm_db_vs_dynamic_1y_compare.py` passed.
  - Full WSL comparison command completed successfully.
  - DB HMM config/snapshot counts remained 4/4 after validation.

## Session: 2026-04-29 HMM Dynamic Candidates DB Registration
- **Status:** complete
- Actions taken:
  - Added `scripts/register_dynamic_hmm_candidates.py`.
  - Kept only `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore` from previous DB HMM versions.
  - Deleted DB rows and filesystem assets for the old original baseline, Horizon v2, and w5/zscore PIT-6m HMM versions.
  - Registered both dynamic PUP candidates into DB:
    - `HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag`
    - `HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag`
  - Re-ran 1Y qlib script comparison after DB registration.
  - Wrote report `docs/analysis/hmm_dynamic_db_registration_report_20260429.md`.
- Key result:
  - DB now has 3 sector_hmm configs: 1 old baseline + 2 dynamic candidates.
  - NEW1 DB result: -0.81%, Sharpe 0.142, MaxDD -30.91%.
  - NEW2 DB result: -0.95%, Sharpe 0.138, MaxDD -30.91%.
- Validation:
  - `python -m py_compile scripts/register_dynamic_hmm_candidates.py` passed in WSL `rdagent-gpu`.
  - DB/file verification confirmed model and coefficient artifacts exist and coefficient JSON contains `preset_A`, full 1Y dates, and stock-sector map.
  - Post-registration script comparison discovered DB=4 coefficient artifacts and excluded DB=0.


---

## Session: 2026-05-06 ST PIT Official Factor Metrics and Cache
- Status: started implementation in isolated worktree `F:/Dev/AIstock_worktrees/factor-st-pit-metrics-20260506`.
- Branch: `codex/factor-st-pit-metrics-20260506`.
- Added design document `docs/architecture/factor_st_pit_official_metrics_cache_design_20260506.md`.
- Production port 8001 has not been touched.


### ST PIT factor metrics/cache implementation progress (2026-05-06T13:25:00+08:00)
- Restored accidental root-only edits into the isolated worktree and repaired the missing `factor_universe_mask_service.py` file.
- Implemented ST PIT universe metadata propagation for factor snapshots, single-factor parquet cache, official metrics, cache coverage checks, loader sidecar checks, and correlation persistence.
- Applied DB migration `factor_metrics_st_pit_universe_metadata_20260506.sql` after stripping BOM; new column comments verified in local DB.
- Validation so far: target `py_compile` passed; `pytest backend/tests/test_factor_st_pit_metrics_cache.py backend/tests/test_factor_metrics_authority_static.py -q -p no:cacheprovider` -> 20 passed, 1 expected numpy warning.
- Data validation: `shsz_st_pit_active_v1` ready/dirty=false, coverage 2018-08-01~2026-04-27; Jan 2025 service eligible index rows 88193 matched direct SQL join rows 88193.

- Follow-up hardening: `FactorValuePipeline` now recreates an existing snapshot when its universe key/fingerprint/index policy is stale relative to the current ST PIT state. Re-ran py_compile and targeted pytest after the change.

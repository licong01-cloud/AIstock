# Task Plan: 因子值缓存与执行层对齐设计

## Goal
在不影响当前实验主链的前提下，设计因子值缓存体系的对齐与增强方案：统一 `source_hash_raw`，并覆盖日期范围选择、按成功进度增量补算、失败日志诊断、执行层自动缓存复用与 backfill/管理端一致性。

## Current Phase
Phase 7

## Phases

### Phase 1: Requirements & Discovery
- [x] Understand user intent
- [x] Identify constraints and requirements
- [x] Document findings in findings.md
- **Status:** complete

### Phase 2: Current-State Code Analysis
- [x] Inspect cache management router, backfill script, execution-layer cache hooks, and pipeline internals
- [x] Identify current support vs missing pieces
- [x] Document code locations and mismatches
- **Status:** complete

### Phase 3: Alignment Design
- [x] Define how to unify `source_hash_raw` across execution layer, backfill script, router, and meta consumers
- [x] Define compatibility strategy that avoids breaking running experiments
- [x] Define required meta schema adjustments
- **Status:** complete

### Phase 4: Feature Design
- [x] Design UI + API flow for selectable start/end dates
- [x] Design true incremental/resume semantics for batch cache compute
- [x] Design structured failure logging and task diagnostics flow
- [x] Design consistency between execution-layer auto-write cache and manual backfill cache
- **Status:** complete

### Phase 5: Delivery
- [x] Summarize architecture decisions and implementation order
- [x] Identify minimal-risk rollout sequence
- [x] Deliver design to user
- **Status:** complete

### Phase 6: 统一评级标准设计
- [x] 梳理规则评级 / LLM评级 / 批量脚本重算之间的冲突点
- [x] 明确唯一权威评级标准必须由单一规则引擎产出
- [x] 设计适配 Multi-Alpha 的多指标综合评分框架
- [x] 明确 LLM 仅作为补充审核与文字说明，不得单独改写正式评级
- [x] 明确所有评级输入统一从数据库读取，禁止从文件侧读取
- **Status:** complete

### Phase 7: 评级v1与管理工具栏设计
- [x] 将日频低换手与选股稳定性要求纳入评级v1
- [x] 设计唯一 UI 入口触发的评级执行流
- [x] 设计版本化规则管理、批量重评与规则说明展示
- [x] 设计数据库结构与页面展示改造方案
- **Status:** complete

## Key Questions
1. How can `source_hash_raw` be unified without invalidating currently written cache metadata or disturbing running experiments?
2. What is the minimal set of changes needed to make factor cache management usable for long-running incremental backfill and diagnostics?
3. Which current capabilities already exist in execution layer and should be preserved rather than rebuilt?

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Focus on design first, no code changes yet | User asked to start designing and current experiment is running in execution layer |
| Treat execution-layer cache path as authoritative behavior baseline | It already provides on-demand cache hit/write during experiments |
| Avoid any design that requires starting/stopping services | User preference and current session constraints |
| Do not modify execution-layer cache protocol first; instead align factor value compute/backfill chain to it | Execution layer is actively running experiments, so lower-risk direction is to converge other writers/readers to `source_hash_raw` |
| Use compatibility read fallback during migration, but only write `source_hash_raw` going forward | Prevents breaking old cache meta while converging schema |
| Add task-level checkpoint files for batch cache jobs | Required to support resume, retry_failed_only, and structured diagnostics |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| Stop-hook requested MemPalace autosave but MCP mempalace tools returned internal error | 1 | Continued task work without relying on autosave |
| Initial broad PowerShell process query failed due to shell interpolation issue | 1 | Replaced with precise process query targeting `backfill_factor_cache.py` |
| planning-with-files stop hook reported `SD=.../scripts` / `/check-complete.sh` not found | 1 | Verified local skill path exists; root cause is hook command's plugin-root assumption and fragile shell fallback under current Windows installation layout |

## Notes
- Running experiment exists in execution layer, so alignment design should minimize blast radius.
- `source_hash_raw` is already used by execution-layer cache hit path; design should converge other writers/readers to that field.
- Planning files live in project root and should be updated as analysis progresses.

---

# Task Plan: Paper Trading v2 UI Implementation

## Goal
Create a new `/paper-v2` UI that is independent from legacy `/paper-trading`, aligns with StrategyPackage -> Selection Center -> Paper Trading v2 backend APIs, and exposes the full correct Paper v2 workflow with fail-fast error visibility.

## Current Phase
Paper v2 UI Phase 1: design document and implementation baseline

## Phases

### Phase 1: UI design document
- [x] Add detailed Paper v2 UI design document under `docs/architecture/`.
- [x] Document route map, API mapping, user flow, fail-fast behavior, and visual system.

### Phase 2: API/types/component foundation
- [x] Add Paper v2 frontend API client wrappers.
- [x] Add shared types and error handling.
- [x] Add common UI components for status badges, error panels, JSON traces, confirmations, cards, and tables.

### Phase 3: Core workflow pages
- [x] Add `/paper-v2` shell and Overview.
- [x] Add Packages page.
- [x] Add Selection Center page.
- [x] Add Portfolio Center and create wizard.

### Phase 4: Trading operations pages
- [x] Add portfolio detail.
- [x] Add Run Console with readiness/run-day/replay/reset.
- [x] Add Ledger views.
- [x] Add Performance report.
- [x] Add Model & HMM center.

### Phase 5: Navigation and verification
- [x] Add global Sidebar links.
- [x] Run frontend lint/type/build checks where available.
- [x] Run backend import/API smoke checks on non-8001 port if needed.
- [x] Document UI-based validation capability and limitations.

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Create new `/paper-v2` UI instead of refactoring legacy `/paper-trading` | Avoid mixing legacy paper_trading APIs with Paper v2 fail-fast workflow |
| Keep multi-package Paper execution disabled in UI | Backend intentionally requires combined package/SelectionBundle before trading aggregate selections |
| Use validated execution policy selector only | Paper-only execution config is prohibited |
| Surface backend errors with full context | Paper v2 must not hide fail-fast backend errors |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| None yet | - | - |

| PowerShell New-Item multiple path invocation failed | 1 | Re-ran directory creation with a foreach loop and -LiteralPath |

| New-Item -LiteralPath unsupported in current shell | 2 | Used System.IO.Directory.CreateDirectory for exact path creation including [portfolioId] |
| `npm run lint` entered interactive ESLint setup because the project has no ESLint config | 1 | Used `npm run build`, which completed Next compilation plus type/lint validity checks successfully |
| `npm run start -- -p 3011` initially used stale dev `.next` artifacts after a dev smoke and returned 500 | 1 | Removed generated `.next`, rebuilt cleanly, then `npm run start -- -p 3011` route smoke returned 200 for all Paper v2 routes |

---

# Task Plan: Paper v2 Selection/Package/Portfolio UI Completion

## Goal
补齐 Paper Trading v2 新 UI 与后端主链路，使 StrategyPackage 创建、权威选股、自选股票池加入、历史选股记录聚合、单策略包模拟盘启动和运行组合列表都能在 `/paper-v2` 中文页面中完成，并保持 fail-fast 与可追溯。

## Current Phase
Phase 2: 后端能力补齐

## Phases

### Phase 1: 设计文档
- [x] 落地 `docs/architecture/paper_v2_ui_selection_portfolio_completion_plan.md`
- [x] 明确 artifact 自动生成、自选加入、QE source 下拉、单包模拟盘启动边界
- **Status:** complete

### Phase 2: 后端能力补齐
- [ ] 新增 QE 未打包来源查询接口
- [ ] Selection Center 支持显式 auto_generate artifact
- [ ] Selection Center 支持选股结果加入自选股票池
- [ ] TopK 后端限制到 50
- **Status:** in_progress

### Phase 3: 前端页面补齐与中文化
- [ ] `/paper-v2/packages` 使用 QE source 下拉并展示指标
- [ ] `/paper-v2/selection` 补齐 Top20、HMM 下拉、历史记录详情、聚合按钮、自选按钮
- [ ] `/paper-v2/portfolios` 补齐单包启动、运行配置、HMM/黑名单、回放/实时模式、运行列表
- **Status:** pending

### Phase 4: 测试与 UI 验证
- [ ] 后端 pytest
- [ ] 前端 build
- [ ] 临时端口 API/UI smoke
- [ ] Playwright 或等价后台 UI 流程验证
- **Status:** pending

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| UI 自动生成 selection artifact 通过 `selection_artifact_config.auto_generate=true` 显式启用 | 避免用户手动生成 artifact，同时不让后端无条件 silent fallback |
| `auto_generate` 不进入 artifact hash | 这是编排开关，不改变模型推理结果 |
| 自选加入使用 selection result 的 `reference_price` | 加入价必须与选股时点可追溯，缺价格直接失败 |
| 多策略包聚合只用于选股研究 | 当前不创建多策略包模拟盘执行 |

---

# Task Plan: QE Config Truthfulness / No-Silent-Override Hardening (2026-04-27)

## Goal
Ensure every UI-visible QE/RDAgent configuration is either executed exactly as submitted or fails fast with an explicit error. Remove silent config mutation, fix V25 and hold-threshold execution mismatch, address remote execution audit issues, add optional suspend_d-based daily selection filtering, and verify via backend/UI flows on ports 8011/3011.

## Current Phase
Phase 1: Baseline documents and code scan

## Phases

### Phase 1: Baseline Docs & Workspace Safety
- [ ] Read docs/architecture/qe_remote_execution_capability_audit_20260427.md
- [ ] Read Desktop v25 fix documents
- [ ] Capture dirty worktree baseline and avoid staging unrelated pre-existing changes
- **Status:** in_progress

### Phase 2: Config/Execution Scan
- [ ] Scan QE compose API payload, persistence, config composer, WSL/remote command generation, result collector, UI display
- [ ] Scan auto-evolution and Multi-Alpha branches for silent defaults/fallbacks/config rewrites
- [ ] Document silent-mutation findings in indings.md
- **Status:** pending

### Phase 3: Fail-Fast Contract & V25/Hold Fixes
- [ ] Add strict execution policy resolution for V25 rather than default TailTWAP fallback
- [ ] Enforce hold_thresh in ScoreWeightedTopkStrategyV2 generated/runtime code
- [ ] Add visible trace/assertions so generated config matches UI/custom params
- **Status:** pending

### Phase 4: suspend_d Selection Filtering
- [ ] Design and implement UI/API/runtime config switch for filtering suspended stocks during daily signal generation
- [ ] Use local market.suspend_d data with audit/readiness; decide cache/export artifact for backtest runtime
- [ ] Add fail-fast behavior for incomplete data when filter enabled
- **Status:** pending

### Phase 5: Remote Execution Audit Remediation
- [ ] Implement issues from qe_remote_execution_capability_audit_20260427.md
- [ ] Add tests/smokes for remote/compute-node command generation and artifact retrieval
- **Status:** pending

### Phase 6: Verification
- [ ] Backend unit tests for config composer, execution algo resolution, hold threshold, suspend filtering, remote execution
- [ ] Frontend build/type checks and Playwright/UI tests on backend 8011 + frontend 3011
- [ ] Validate with real QE experiment branch and V25 repaired strategy
- **Status:** pending

### Phase 7: Git Traceability
- [ ] Commit only files modified in this task, grouped by concern
- [ ] Include clear commit messages and report hashes; push if remote auth works
- **Status:** pending

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Fail-fast beats fallback for any UI-visible trading/execution config | Silent defaults caused the V25/TailTWAP mismatch and must be prevented |
| Preserve existing dirty worktree baseline | Many files are already modified before this task; do not revert or accidentally commit unrelated work |
| Use temporary ports 8011/3011 only | Matches project memory and avoids disrupting existing services |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| None yet | - | - |



---

# Task Plan: HMM Horizon-Aware v2 Training and Script Backtest (2026-04-28)

## Goal
Create a non-overwriting HMM horizon-aware training workflow that matches future QE emphasis on 5D/10D/20D returns, train a new HMM version in the existing WSL/RD-Agent environment, and run script-only 6-month comparison backtests across old/new HMM versions without launching QE experiments.

## Current Phase
Complete: HMM Horizon-Aware v2 training, script backtest, and final report

## Constraints
- Do not modify or overwrite old HMM model assets, coefficient files, or existing training scripts.
- New training code must be additive: new filenames and unique output directories/config display names.
- Use WSL `rdagent-gpu` environment and existing local DB/Qlib data paths.
- Do not use QE experiment execution for validation; all validation must be script-only.
- Registering a new HMM snapshot/asset is allowed because the user explicitly requested new-version training, but old rows/assets must remain untouched.

## Phases

### Phase 1: Persistent Plan and Design Doc
- [x] Read project memory and HMM reference reports.
- [x] Capture workspace/env baseline.
- [x] Write detailed optimization/training plan MD under `docs/analysis/`.
- **Status:** complete

### Phase 2: Additive Script Creation
- [x] Create new horizon-aware HMM training script without changing old scripts.
- [x] Create new coefficient precompute/calibration script if needed.
- [x] Create script-only 6-month comparison backtest script.
- **Status:** complete

### Phase 3: Train New HMM Version
- [x] Run new training in WSL `rdagent-gpu`.
- [x] Save new model under unique `backend/data/hmm_models/<config_id>/<date>/` directory.
- [x] Register a new DB snapshot/config with unique display name, if DB connection permits.
- **Status:** complete

### Phase 4: Six-Month Script Backtest
- [x] Select latest valid six-month data window from available local data.
- [x] Compare Raw/no-HMM, old baseline, covfix same params, covfix w5 zscore, and new horizon v2 HMM.
- [x] Report HMM Top50 replacement, HMM-only label/forward returns, monthly returns, contribution, utilization proxies, and final holdings.
- **Status:** complete

### Phase 5: Final Report
- [x] Write final detailed results MD.
- [x] Summarize whether new HMM improves over prior versions.
- [x] List files/assets created and validation limitations.
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Build horizon-aware HMM as additive scripts/assets only | User explicitly forbids overwriting old model files and training scripts |
| Remove `limit_up_ratio` from v2 observation features unless consistently PIT available | Prior report identified it as covariance anomaly source and some runtime precompute paths do not consistently use it |
| Label states by 5D/10D/20D train-window utility rather than 1D daily return | Future QE emphasis and prior evidence show fixed 1D-style trending is mismatched |
| Calibrate coefficients per snapshot from validation 5D/10D/20D utility | Prevents fixed `trending=1.05` from rewarding a negative multi-day state |
| Use script-only validation, no QE runs | User requirement |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| WSL inline PowerShell quoting failed for dependency probe | 1 | Wrote a temporary `.codex_tmp/probe_env.sh` and executed it with WSL bash |
| Conda activation failed under `set -u` because `LD_LIBRARY_PATH` was unbound | 1 | Re-ran the training shell wrapper without `set -u` |

## Additional HMM Error Log
| Error | Attempt | Resolution |
|-------|---------|------------|
| Temporary WSL wrapper inherited CRLF line continuation and generated output filenames with a private-use CR glyph | 1 | Replaced those generated temp outputs with sanitized `.codex_tmp/hmm_horizon_v2_backtest_20260428*` filenames |

---

# Task Plan: HMM w5 zscore PIT Retrain Check (2026-04-28)

## Goal
Retrain the previously best diagnostic-only `w5 zscore` HMM with Train/Validation ending before the six-month script backtest window, generate new isolated coefficient artifacts, and compare it against all prior HMM versions using the same script-only validation.

## Current Phase
Complete: w5 zscore PIT retrain and script comparison

## Constraints
- Do not overwrite prior HMM scripts, models, or coefficient artifacts.
- Do not run QE experiments; use script-only comparison.
- Use Train/Validation that end before 2025-09-01 to remove overlap with the 2025-09-01 ~ 2026-03-03 six-month validation window.
- Preserve old diagnostic `w5 zscore` snapshot for comparison, but mark it diagnostic-only.

## Phases

### Phase 1: PIT Split and Training
- [x] Use w5/zscore/n3/diag parameters with Train 2022-09-01 ~ 2025-05-30 and Validation 2025-06-02 ~ 2025-08-29.
- [x] Train into a new unique config/model directory.
- [x] Save training stdout/stderr and metrics.
- **Status:** complete

### Phase 2: Coefficient Precompute
- [x] Precompute `preset_A` for 2025-09-01 ~ 2026-03-03.
- [x] Precompute `preset_B` for 2025-09-01 ~ 2026-03-03 if training succeeds.
- [x] Register a new DB config/snapshot or provide equivalent result JSON for script comparison.
- **Status:** complete

### Phase 3: Script Backtest and Report
- [x] Re-run six-month script comparison including the new PIT w5 zscore artifacts.
- [x] Compare new PIT w5 zscore versus old diagnostic w5, w3 preset_B, Raw, and Horizon v2.
- [x] Write a short incremental report under `docs/analysis/`.
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Reuse w5/zscore/n3/diag hyperparameters but move train/val entirely before 2025-09-01 | Directly tests whether the old +6.46% result survives without overlap leakage |
| Generate both preset_A and preset_B | preset_A was old diagnostic winner, while preset_B was best PIT-compatible in w3; both are cheap to precompute once the model exists |

---

# Task Plan: HMM Daily Coefficient Generation (2026-04-28)

## Goal
补齐 Paper v2 / Selection Center 所需的 HMM 每日实盘预测能力：使用最新已完成交易日数据生成下一交易日 HMM 系数产物，并通过 UI 完成预览和生成验证。

## Current Phase
Complete: design, backend/API, frontend UI, backend tests, UI E2E validation.

## Phases

### Phase 1: Design
- [x] 写入 `docs/architecture/hmm_daily_coefficient_generation_design_20260428.md`
- [x] 明确 PIT 规则、产物元数据、API、UI、fail-fast 边界

### Phase 2: Backend/API
- [x] `HMMTrainingService.preview_daily_coefficients`
- [x] `HMMTrainingService.generate_daily_coefficients`
- [x] `/hmm-training/snapshots/{snapshot_id}/daily-coefficients/preview`
- [x] `/hmm-training/snapshots/{snapshot_id}/daily-coefficients/generate`
- [x] `scripts/precompute_hmm_coefficients.py` 支持 `output_trade_date`

### Phase 3: Frontend/UI
- [x] `hmmTrainingApi.previewDailyCoefficients/generateDailyCoefficients`
- [x] `/paper-v2/model-hmm` 新增每日系数生成卡片
- [x] Playwright 覆盖每日系数预览和确认生成

### Phase 4: Validation
- [x] Backend pytest: 149 passed
- [x] Frontend typecheck/build passed
- [x] Paper v2 UI E2E on 8012/3012: 12 passed

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| 每日系数生成独立于 HMM 训练 | 真实模拟盘需要每日预测，不需要每日重训 |
| `effective_trade_date` 必须晚于 `as_of_trade_date` | 防止未来函数和当天收盘数据泄漏 |
| 生成文件为 additive artifact | 不修改模型权重、快照或历史系数文件 |
| 同名已存在文件只允许完全匹配时幂等返回 `EXISTS` | 防止静默覆盖或篡改历史产物 |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| Next dev proxy 对较长 HMM generate 请求出现 socket hang up | 1 | UI E2E 使用 3012 前端 + 8012 绝对 API base，避免经过 Next proxy；生成 API 本身直接验证成功 |


---

# Task Plan: HMM Dynamic Coefficient Offline Experiments (2026-04-29T01:11:15)

## Goal
???? AIstock ??/???????????????? HMM ????????????? HMM ?????????????? 6 ??? HMM ?????? 1 ? qlib/??????? No-HMM ?????????????? QE ??????

## Scope Guardrails
- ????? HMM ???????????????? coefficient artifact?qlib/????????????
- ????? AIstock ??/??????????????????/??????? HMM config/snapshot/job??? QE ???
- ??????????????????? QE ranking/TopK ?????

## Phases

### Phase 1: Scope & Discovery
- [ ] ???? HMM ???DB ?????????????
- [ ] ?? 1 ???????? PIT train/validation/test ??
- **Status:** in_progress

### Phase 2: Offline Experiment Script
- [ ] ???????????????
- [ ] ?? 6 ????dynamic expected-return?dynamic probability-up?10/20 blend?confidence shrink?K4 dynamic?additive overlay ????
- [ ] ???? DB
- **Status:** pending

### Phase 3: Training & Artifact Generation
- [ ] ????????
- [ ] ?? models.json?coefficients/signals?metadata?logs ?????
- **Status:** pending

### Phase 4: One-Year Validation
- [ ] ?? No-HMM baseline
- [ ] ?? 6 ????? 1 ???/qlib-style ??
- [ ] ?????Sharpe?MaxDD????HMM-only/Raw-only????????
- **Status:** pending

### Phase 5: Report & Recommendation
- [ ] ??????
- [ ] ????????????? QE ????
- **Status:** pending

## Decisions Made
| Decision | Rationale |
|---|---|
| ??? DB | ????????????? HMM ???????????? |
| ???? coefficient ???????? | ?? QE runtime ????????????????? |
| ?? overlay ?????? | ?? QE ???? runtime ????????? QE-ready ?? |

## Errors Encountered
| Error | Attempt | Resolution |
|---|---|---|
| None yet | - | - |

| NaN posterior/signal caused identical invalid full-run rankings | Full run v1 | Patched forward posterior normalization and sanitized non-finite signals/coefficients; rerun under a new output root |

---

# Task Plan: HMM Dynamic Coefficient Micro-Tuning Loop (2026-04-29)

## Goal
Iteratively tune valuable HMM dynamic-coefficient directions using script-only qlib validation, without modifying AIstock application code, existing DB HMM versions, or QE experiments.

## Status
Complete.

## Completed Phases
- [x] Confirmed scope: HMM offline scripts/model artifacts/qlib outputs only.
- [x] Ran second-pass tuning grid after the first dynamic experiment.
- [x] Added relative PUP helpers and pass3-pass8 narrow grids around the best direction.
- [x] Trained and validated 112 offline HMM variants plus repeated No-HMM baselines.
- [x] Verified HMM DB config count remains 4; no new DB HMM config/snapshot/job was inserted.
- [x] Wrote final report: `docs/analysis/hmm_dynamic_tuning_final_report_20260429.md`.

## Final Decision
Best script-level candidate is `p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`: 3-state PUP, 5D/10D/20D weights 0.20/0.30/0.50, lambda 0.06, coefficient clip 0.98~1.015, confidence_scale 0.075.

## Errors Encountered
| Error | Resolution |
|---|---|
| PowerShell heredoc quoting broke a WSL DB query | Wrote a temporary `.codex_tmp` Python query and removed it after execution |
| A first DB query referenced nonexistent `config_name` | Inspected table columns and re-ran using `model_type/display_name` only |

---

# Task Plan: HMM DB vs Dynamic 1Y Script Comparison (2026-04-29)

## Goal
Run a script-only one-year qlib comparison between current DB HMM coefficient artifacts and the two recommended offline dynamic HMM candidates, without inserting any new DB HMM version.

## Scope Guardrails
- No DB writes and no QE experiment submission.
- No AIstock backend/frontend/runtime code changes.
- Only additive HMM offline script, qlib comparison outputs, and analysis report are created.

## Completed Phases
- [x] Discovered current DB HMM snapshots and coefficient coverage.
- [x] Added `scripts/hmm_db_vs_dynamic_1y_compare.py` as a standalone comparison script.
- [x] Ran qlib Top50/5D rebalance validation for 2025-03-11 ~ 2026-03-03.
- [x] Wrote report: `docs/analysis/hmm_db_vs_dynamic_1y_comparison_report_20260429.md`.
- [x] Verified DB HMM config/snapshot counts remain 4/4.

## Final Decision
Do not insert into DB automatically. The best PIT-compatible result is the offline dynamic `p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`, so it should be treated as a pending DB-registration candidate after user confirmation.

---

# Task Plan: Register Dynamic HMM Candidates Into DB (2026-04-29)

## Goal
Register both dynamic PUP HMM candidates into the HMM DB version list, and keep only one existing DB HMM version as the baseline.

## Completed Phases
- [x] Kept baseline: `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`.
- [x] Deleted old DB configs/assets for original baseline, Horizon v2, and w5/zscore PIT-6m.
- [x] Registered `HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag`.
- [x] Registered `HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag`.
- [x] Verified DB now has exactly 3 sector_hmm configs.
- [x] Re-ran script comparison after DB registration and confirmed DB artifacts reproduce the offline results.

## Usage Note
The two dynamic DB versions currently have `preset_A` coefficient artifacts for `2025-03-11 ~ 2026-03-03`. QE windows outside this range need matching coefficient artifacts before validation.


---

# Task Plan: ST PIT Official Factor Metrics and Cache (2026-05-06)

## Goal
Make the official factor independent metrics, single-factor cache, factor backtest cache, and correlation calculation use the same ST PIT universe semantics: `shsz_st_pit_active_v1`, daily buy-eligible mask, and universe fingerprint metadata.

## Phases
- [x] Create isolated worktree/branch and detailed design document.
- [ ] Implement shared factor universe mask service.
- [ ] Wire ST PIT universe into snapshots, factor value pipeline, and metric engine.
- [ ] Wire universe metadata into backtest cache and correlation cache.
- [ ] Add DB migration and targeted tests.
- [ ] Run data accuracy checks and test-port backend validation.
- [ ] Commit, push, and confirm clean worktree.

## Constraints
- Do not modify `AGENTS.md`.
- Do not restart production backend port 8001.
- Use test port 8012 for backend validation.
- Commit all files changed for this task; leave no uncommitted files.


---

# Task Plan: ST PIT Official Factor Metrics and Cache (2026-05-06)

## Goal
Make official factor independent metrics, single-factor backtest cache, and factor correlation all use one ST PIT universe contract: `shsz_st_pit_active_v1`, daily buy-eligible mask/index, and universe fingerprint metadata.

## Current Phase
Phase 4: validation and API smoke

## Phases
- [x] Phase 1: verify worktree safety and recover misplaced root edits into isolated task worktree.
- [x] Phase 2: implement `FactorUniverseMaskService` and wire DataSnapshotManager / FactorValuePipeline / metric engine.
- [x] Phase 3: wire cache coverage, loader sidecar checks, official metrics DB writes, correlation metadata, backfill script, migration, and tests.
- [ ] Phase 4: run py_compile, pytest, DB data checks, and test-port API smoke; save validation run record.
- [ ] Phase 5: commit and push task branch; leave no uncommitted files in the task worktree.

## Decisions Made
| Decision | Rationale |
| --- | --- |
| Official metrics use ST PIT buy-eligible samples, not all raw rows | Aligns independent metrics with QE experiment buy universe and avoids future-ST leakage. |
| Snapshots use window union, caches use daily eligible index | Preserves enough raw data while metrics/cache samples are PIT-correct per date. |
| Correlation validates universe metadata | Prevents merged cache or old single cache from being silently mixed into current official correlation. |
| Root worktree edits are not reverted in this task | They may be user/parallel-session state; only the isolated task worktree is committed. |

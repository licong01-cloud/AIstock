# Findings & Decisions

## Requirements
- 统一因子值缓存元数据字段到 `source_hash_raw`
- 修改因子值计算功能以与执行层缓存逻辑对齐
- 设计开始/结束日期可选能力
- 设计基于上次成功进度的增量补算/续跑能力
- 设计因子计算失败日志输出与诊断链路
- 设计执行层自动缓存复用与 backfill/管理端的一致性方案
- 当前执行层正在做实验，设计需优先考虑低风险对齐

## Research Findings
- 执行层 `prepare_factors` 已内置 `_try_cache_hit()` 与 `_write_cache()` 逻辑，能对实验中实际使用到的因子做按需缓存与下次复用。位置：`backend/services/quantevolver/config_composer.py:2056-2127`, 调用点约在 `2173`, `2223`。
- 执行层缓存命中依赖 `source_hash_raw` 与 `date_range` 覆盖 `TRAIN_START~TEST_END`。
- backfill 后端 API 已支持 `start_date/end_date`，请求模型位置 `backend/routers/quantevolver.py:2408-2415`，WSL 命令透传位置 `2441-2447`。
- 前端缓存计算入口未暴露日期输入，提交 body 仅含 `workers/timeout_per_factor/force`。位置：`frontend/src/app/quantevolver/components/FactorList.tsx:236-245`。
- backfill 脚本支持 `--start/--end/--incremental` 参数，但 `--incremental` 目前没有真正改变主流程编排语义；当前仅做 coverage-based skip。位置：`scripts/backfill_factor_cache.py:64-87`, `129-131`, `166-170`。
- `FactorValuePipeline` 已存在单因子向后扩展能力 `extend_single_factor_cache()`，可用于真正 incremental 方案的一部分。位置：`backend/services/quantevolver/factor_value_pipeline.py:1033-1163`。
- 失败异常在 pipeline 内部能获取 traceback，但返回给结果对象时被截断为前 500 字符，导致诊断信息不足。位置：`factor_value_pipeline.py:728-753`。
- 当前管理后端已有任务状态接口 `/factor-cache/compute-status/{task_id}` 和 `/factor-cache/active-tasks`，但前端未做任务日志展示。位置：`backend/routers/quantevolver.py:2487-2519`。
- hash 字段目前不一致：执行层写 `source_hash_raw`，backfill 脚本写 `source_hash`，管理端统计两者都兼容但前端 hash 匹配判断读取 `source_hash_raw`。这会导致命中/展示可能不一致。

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| 以执行层缓存协议为基准统一其他链路到 `source_hash_raw` | 执行层正在运行实验，现有逻辑已在线使用，应让 backfill/管理端向其收敛 |
| 将改造拆成“元数据对齐、任务编排增强、诊断增强、前端入口增强”四条线 | 降低风险，便于分阶段实施 |
| 增量补算设计应优先复用 `extend_single_factor_cache()` | 现有单因子后向扩展逻辑已经存在，避免重复设计 |
| 诊断链路应采用结构化失败日志 + 任务总日志双轨 | 兼顾人读和程序化恢复/重试 |
| 迁移期读取兼容 `source_hash_raw/source_hash`，写入统一为 `source_hash_raw` | 保证旧缓存元数据仍可识别，同时新写入不再分叉 |
| 先改 factor value compute/backfill/pipeline/router/front-end，不动 execution-layer 命中/写回主逻辑 | 当前实验正在运行，优先降低 blast radius |
| 批量缓存任务需要独立 `_tasks/{task_id}.json` 状态文件 | 才能支撑 resume、retry_failed_only、失败诊断与进度增量 |

## Design Summary
- `source_hash_raw` 对齐方案：
  - execution layer 保持不动，继续作为缓存协议基准。
  - `scripts/backfill_factor_cache.py` 写 meta 时改为写 `source_hash_raw`，不再写 `source_hash`。
  - `backend/services/quantevolver/factor_value_pipeline.py` 未来若直接写单因子 meta，也统一写 `source_hash_raw`。
  - 所有读取端在迁移期使用 `entry.get("source_hash_raw") or entry.get("source_hash")` 兼容旧数据；待缓存逐步重写后再删除 fallback。
- 日期范围能力：
  - 前端 `FactorList.tsx` 增加 `start_date/end_date` 输入；提交到现有 `/factor-cache/compute` 请求体。
  - stats 区增加“目标区间 vs dominant cached range”展示，减少误判。
- 增量补算/续跑：
  - 真正的 `incremental` 语义定义为：优先跳过 covered；仅缺后段时走 `extend_single_factor_cache()`；其余缺口走 full rebuild。
  - 新增任务级 checkpoint：`rdagent_assets/factor_values/_tasks/{task_id}.json`，记录 success/failed/skipped/pending。
  - 后端请求体新增 `resume_task_id`、`retry_failed_only`；脚本可按任务状态构造本轮待跑集合。
- 失败日志诊断：
  - pipeline 结果对象保留 `error_short + traceback_full + error_type`。
  - 每个 batch task 输出 `_tasks/{task_id}.failed.ndjson`，每行一条失败因子记录。
  - 保留现有总日志 `${task_id}.log`，前端增加最近任务/状态/日志尾部/失败因子查看。
- 执行层自动缓存与回填一致性：
  - 执行层继续按需 `CACHE HIT / cache WRITTEN`。
  - backfill 侧写入的 meta 结构与 execution layer 完全一致：`computed_at/rows/date_range/as_of_date/source_hash_raw`。
  - 后续若需要统一 hash 算法，也应以 execution layer 当前 raw hash 算法为准，避免同代码不同 hash。
- 实施顺序（低风险）：
  1. 统一 meta writer/reader 到 `source_hash_raw` + 读取 fallback。
  2. 前端接出日期输入 + 任务状态/日志展示。
  3. 让 `--incremental` 真正生效并接入 `extend_single_factor_cache()`。
  4. 增加 `_tasks` checkpoint/resume/retry_failed_only/failed.ndjson。

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| 无现成 planning files | 已在 `F:\Dev\AIstock` 下新建 `task_plan.md/findings.md/progress.md` |
| PowerShell 广义进程查询噪音过大且包含 shell 自身 | 改用精确匹配 `backfill_factor_cache.py` 的进程查询 |
| MemPalace autosave MCP 内部报错 | 不阻塞当前设计分析 |
| planning-with-files stop hook 报 `SD=.../scripts` / `/check-complete.sh` 路径错误 | 已定位为本机 skill/plugin 安装布局与 hook 命令中的 root 假设不一致，且 shell 回退链路在 Windows/Git Bash 下较脆弱 |
| TypeScript 环境缺少可执行 `tsc` | 当前无法做真实前端类型检查；日志显示未安装 TypeScript，而不是项目源码报错 |

## Resources
- `F:\Dev\AIstock\backend\services\quantevolver\config_composer.py`
- `F:\Dev\AIstock\backend\services\quantevolver\factor_value_pipeline.py`
- `F:\Dev\AIstock\backend\routers\quantevolver.py`
- `F:\Dev\AIstock\frontend\src\app\quantevolver\components\FactorList.tsx`
- `F:\Dev\AIstock\scripts\backfill_factor_cache.py`

## Unified Factor Grading Standard Proposal (2026-04-16)
- 当前系统不存在唯一权威评级标准：规则函数、LLM 提示词、以及 `batch_fill_multi_alpha_dimensions.py` 的 grade 重算三者口径不一致。
- 正式评级必须收敛为“单一规则 + 单一代码路径 + 单一数据库读源”；否则前端展示、自动筛选、Multi-Alpha 选因子都会继续漂移。
- 最合理职责边界：规则引擎负责 `official_grade` / `official_score` / `grade_reason_structured`；LLM 只读取数据库中的同一批指标与规则结果，输出解释、风险提示、冲突审阅，不得写正式 grade。
- 规则引擎不应继续用当前 `_grade_by_metrics()` 的简化二元阈值；需要升级为多维综合评分 + 少量硬门槛的统一 rubric。
- 统一评级输入应只来自数据库，建议唯一来源限定为：
  - `aistock_factor_metrics`（full + recent_6m + recent_3m + out_sample 最新快照）
  - `qe_factor_classification` 中仅保留规则产出的正式评级结果与 LLM 审阅结果；不再允许脚本绕过统一服务直接改 `grade`
  - 必要的元信息仅来自 `aistock_factor_catalog`
- 规则设计上，Multi-Alpha 需要的不只是“强因子”，还要可分组、可稳定、可互补。因此评级应覆盖：强度、稳定性、持有期一致性、收益质量、可交易性、可分组性。
- 建议统一核心评级维度：
  1. Predictive Strength：核心IC强度（按持有期选 1d/5d/20d core_ic）
  2. Stability：年化ICIR/RankICIR + recent_6m/recent_3m/out_sample 稳定性
  3. Economic Quality：超额年化、超额Sharpe、多头表现
  4. Monotonicity & Direction Quality：分组单调性、方向一致性
  5. Tradability：换手率、覆盖率
  6. Multi-Alpha Fitness：data_source_group、linearity、holding_period_class、与组内重复风险（后续可扩展）
- 评分形式应采用：基础分 0-100 + 等级映射 S/A/B/C/D；并设置少量 hard gates 防止“局部一项很强但整体极差”仍拿高等级。
- 建议 hard gates：
  - S/A 不允许 recent_3m 与 recent_6m 核心IC同时显著为负
  - S 不允许单调性 <= 0
  - S/A 不允许 coverage 过低
  - S 不允许超额年化显著为负
- 这个方案是合理的，但必须诚实指出：
  - “唯一标准”是完全合理且必要的；
  - “所有因子只靠静态规则就能永远准确评级”并不现实，规则仍需版本化和定期校准；
  - 因此应建立 `grading_version`，允许未来升级规则，但任一时刻线上只能有一个生效版本。

## Rating v1 + UI Toolbar Design (2026-04-16)
- 用户确认采用“统一规则为权威、LLM仅补充审核”的方案，并要求先完成 v1 设计，不处理实时方案。
- 对当前产品目标必须说实话：如果只允许一个唯一评级，那么这个评级必须明确锚定到 **AIstock 日频、多Alpha、低换手生产场景**，否则单一等级无法同时公平评价高换手短周期 alpha 与低换手生产 alpha。
- 因此建议把正式评级命名为：`AIstock Multi-Alpha Daily Production Grade v1`。这不是“普适学术评级”，而是“面向当前生产目标的唯一权威评级”。
- 日频策略下，因子导致股票池每天剧烈变化会显著增加交易成本并降低选股可执行性，必须进入正式评级要求中。
- 但必须诚实指出：当前数据库里能直接表示“股票每天是否大幅更换”的现成指标主要是 `turnover`，它是有效 proxy，但不是最精确的“Top篮子日重合率”。因此：
  - v1 可以把 `turnover` 作为正式评分与高等级 hard gate；
  - 若以后要更精准衡量选股稳定性，应把 `top_bucket_overlap_1d/5d`、`avg_holding_days` 等指标先写入数据库，再纳入 v2。
- 正式评级入口应严格单一：只能通过 UI 的“因子评级”工具栏触发后端统一评级服务；其他脚本、分析器、批处理不再直接写 `official_grade`。
- 现有 `qe_factor_classification` 不适合继续承载版本化正式评级，建议拆分：
  - `qe_factor_classification`：保留 category / factor_dimension / LLM说明等分类信息
  - `qe_factor_grading_versions`：保存规则版本、文字描述、权重、阈值、是否生效
  - `qe_factor_grading_runs`：保存一次执行任务（单因子/多因子/全量）的状态和范围
  - `qe_factor_grades`：保存某因子在某版本下的正式评分结果、子分数、hard gates、指标快照
- 前端列表页正式等级应改为只读 `qe_factor_grades` 的 active version 最新结果，绝不再从 `qe_factor_classification.grade` 读取。
- LLM 的职责边界：读取数据库中的同一组指标 + 规则结果，输出解释、风险提示、人工复核意见；不得写正式 grade。
- v1 评分建议采用“多维总分 0-100 + 高等级 hard gates”结构：
  1. Predictive Strength（25）
  2. Stability（25）
  3. Economic Quality（15）
  4. Selection Stability & Cost（15）
  5. Monotonicity & Reliability（10）
  6. Multi-Alpha Fitness（10）
- v1 正式输入统一只从数据库读取：
  - `aistock_factor_metrics`：full / out_sample / recent_6m / recent_3m 最新记录
  - `aistock_factor_catalog`：元信息
  - 分类类元数据可从 `qe_factor_classification` 读，但不得反向决定 grade
- 需要停用/去权的现有写 grade 路径：
  - `backend/services/quantevolver/factor_analyst.py` 中正式 grade 写入
  - `scripts/batch_fill_multi_alpha_dimensions.py` 中对 `qe_factor_classification.grade` 的覆盖
- UI 管理卡片工具栏应支持：
  - 版本选择（active/default，可切换历史版本重评）
  - 范围选择：选中因子 / 当前筛选结果 / 全量因子
  - 操作按钮：执行评级、全量重评、查看规则说明、查看运行历史
  - 显示当前规则版本、文字版评分说明、等级门槛、hard gates 摘要
  - 批任务进度、成功/失败/跳过统计、最近 run 列表
- 如果多个版本并存，页面必须明确区分：
  - 当前展示版本
  - 当前激活版本
  - 本次执行版本
  防止“页面看的是 v1，执行跑的是 v2”。

## Visual/Browser Findings
- 无

## Paper Trading v2 UI Findings (2026-04-26)
- Legacy `/paper-trading/*` pages mostly call `/api/v1/paper-trading/*`, not `/api/v1/paper-v2/*`.
- Existing `frontend/src/app/paper-trading/package-selection/page.tsx` is the only page already aligned with new Selection Center APIs.
- New UI should live under `/paper-v2` to avoid legacy API confusion.
- Required backend API groups: `/strategy-packages`, `/selection-center`, `/paper-v2`, `/hmm-training`.
- New UI implementation must expose StrategyPackage creation from QE experiment/evolution loop, otherwise the visible workflow would start after the authoritative source step.
- Cash ledger was persisted in `paper_v2.cash_ledger` but not exposed through the Paper v2 router; a `/cash-ledger` endpoint was needed for the Ledger page to cover cash traceability.
- Portfolio Run Console should own dated execution policy activations because the user wants minute execution policy to be configurable before each trading day while still restricted to backtest-validated policies.
- Direct browser UI verification is only fully automatable if an existing browser automation runner is available; otherwise validation can cover Next build/static route compilation plus HTTP/API smoke on a non-8001 backend port.

## Paper v2 Completion Findings (2026-04-26)
- Current `/paper-v2` frontend files include mojibake Chinese and some previously reported corruption symptoms; pages need clean UTF-8 Chinese rewrites before build validation.
- Existing packages in DB already use QE experiment names as package names for the three requested sources:
  - `pkg_b668f8a633c44b72a5d557a2cb8970e3` -> `qe_20260416_002701`
  - `pkg_006a42323f7c4e81a468fdaad2cb16a3` -> `qe_20260413_084216`
  - `pkg_99142cb1440c40a7824e83902f4e7da9` -> `qe_20260416_082012`
- `StrategyPackageRuntime` correctly rejects direct `selection_scores` and requires persisted authoritative artifacts; UI must request explicit auto generation before running selection.
- HMM runtime already requires a completed snapshot and coefficient artifact; UI can safely expose config/snapshot dropdowns without adding runtime fallback.
- Existing watchlist service supports `entry_price`, `entry_rank`, `entry_source`, `entry_task_id`, `entry_loop_id`, `entry_as_of`, which is sufficient for Selection Center one-click watchlist import when the backend prevalidates prices.

## QE Config Truthfulness Findings (2026-04-27)
- Session started at 2026-04-27T00:36:40.7611171+08:00.
- Existing worktree is already dirty before this task; commits must stage only task-owned changes.
- User-reported root issue confirmed from prior analysis: UI/custom params persisted V25_TWO_STAGE, while generated Qlib config defaulted to TailTWAPWithLimitStrategy; hold_thresh=5 was serialized but not enforced by custom strategy override.


## HMM Horizon-Aware v2 Findings (2026-04-28T01:51:39)
- Workspace already contains many unrelated untracked files; this task will create only additive HMM plan/script/report/model artifacts and will not stage/revert unrelated work.
- WSL `rdagent-gpu` environment is available with Python 3.10.19 and required packages (`hmmlearn`, `psycopg2`, `numpy`, `pandas`).
- `TDX_DB_PASSWORD` is empty in the Windows environment; scripts should support empty password and/or WSL local DB defaults, then log DB connection failures explicitly.
- Authoritative HMM issue from reports: covfix w5 zscore fixed technical instability but `preset_A` rewards `trending` despite negative 3D/5D/10D/20D validation returns.
- New HMM training should align observation features, state labels, and coefficient calibration with 5D/10D/20D horizon utility.

## HMM Data/Snapshot Discovery (2026-04-28T01:55:02)
- Local DB coverage: `market.sector_data`, `market.sw_daily`, `market.index_daily`, and `market.kline_daily_raw` are available through 2026-04-27.
- Existing active HMM snapshots found in DB: baseline original w3 raw, covfix same-params w3 raw, and covfix w5 zscore candidate.
- The covfix same-params w3 raw snapshot has positive validation `trending` returns through 20D; this should be included in six-month script comparison because it may be a stronger prior candidate than the w5/zscore version.

## HMM v2 Script Implementation Notes (2026-04-28T02:00:04)
- New training script is additive and includes integrated validation/coefficient precompute; no existing HMM trainer was modified.
- New comparison script is script-only and uses a causal trailing 5D/10D/20D raw score with Top50 5D rebalancing; it intentionally does not use QE experiment execution.
- Python 3.10 disallows backslashes inside f-string expressions, so WSL compilation caught and fixed a path conversion issue that Windows py_compile did not flag.

## HMM v2 Training Result (2026-04-28T02:01:33)
- New HMM v2 trained 131/131 sectors with zero skipped sectors.
- Coefficient calibration result: `fading=1.020983`, `neutral=0.992210`, `trending=0.986807`.
- Validation weighted 5D/10D/20D utilities show that the train-labeled `fading` bucket performed best in validation (+0.034876 pct), while `neutral` and `trending` were negative. This confirms why fixed state-name semantics are unsafe and why snapshot-specific calibration is required.
- The first training attempt failed because `set -u` conflicted with conda activation; rerunning without nounset completed successfully.

## HMM v2 Script Backtest Result (2026-04-28T02:07:01)
- Six-month script validation window: 2025-09-01 to 2026-03-03, Top50 equal-weight, 5D rebalance, trailing 5D/10D/20D raw score.
- Best PIT-compatible result is `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B`: total -9.48%, annualized -18.87%, Sharpe -0.703, max drawdown -12.99%.
- `HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore::preset_A` is best overall in the proxy (+6.46%) but flagged diagnostic-only because train/validation overlaps the backtest window.
- New Horizon v2 variants did not improve: main -23.87%, conservative -22.28%, risk_only -15.58%, all worse than Raw/no-HMM (-13.98%).
- New v2 failure mode: HMM-only replacements were not better than raw-only candidates; v2 main HMM-only 5D averaged -0.72% while raw-only averaged +0.64%.
- The comparison JSON now includes per-version stock contribution summaries under `contributions`; summary CSV includes capital utilization and buy-unfilled close-to-close proxies.
- Final detailed report written to `docs/analysis/hmm_horizon_v2_training_backtest_report_20260428.md`.

## HMM w5 zscore PIT Retrain Check (2026-04-28)
- User asked whether the diagnostic-only w5/zscore result can be retrained with non-overlapping train/validation windows.
- Planned split: Train 2022-09-01 ~ 2025-05-30, Validation 2025-06-02 ~ 2025-08-29, Backtest 2025-09-01 ~ 2026-03-03.
- This directly removes the overlap that made `HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore::preset_A` diagnostic-only.

## HMM w5 zscore PIT Retrain Result (2026-04-28T08:51:43)
- New PIT w5/zscore version trained and registered: `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore`, config `c095ab83-48f4-453d-9eb9-c1987b6bd7fe`, snapshot `b6e18fc0-2b58-4f8b-a27b-353bdf203c6f`.
- Non-overlap split: Train 2022-09-01 ~ 2025-05-30, Validation 2025-06-02 ~ 2025-08-29, script backtest 2025-09-01 ~ 2026-03-03.
- Training succeeded for 131/131 sectors; rolling_window=5, zscore=true, covariance_type=diag; covariance clipping fixed 121 sectors / 248 anomalous covariance values.
- Precomputed both `preset_A` and `preset_B` for 2025-09-01 ~ 2026-03-03.
- Script backtest results: new PIT w5 preset_A total -16.38%, Sharpe -1.134; new PIT w5 preset_B total -14.98%, Sharpe -1.025.
- The old diagnostic w5 preset_A +6.46% did not reproduce under non-overlap PIT training; current best PIT-compatible HMM remains `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B` at -9.48%.
- Incremental report: `docs/analysis/hmm_w5_zscore_pit_retrain_report_20260428.md`.

## HMM Leaky w5/zscore Deletion (2026-04-28)
- User requested hard deletion of the old leaking diagnostic version to avoid QE selection confusion.
- Deleted DB records for config `be681443-fe5d-4641-b55f-5f889e6af8e1`, snapshot `4c9b5f7b-8e59-44a6-b580-e7186b9283df`, and job `b9263abe-bdf2-411d-a3e3-cb142360a72a`.
- Deleted filesystem directory `backend/data/hmm_models/be681443-fe5d-4641-b55f-5f889e6af8e1`.
- Verification: DB query for that config now returns no configs/snapshots/jobs; filesystem path no longer exists.
- Remaining completed HMM DB configs: baseline original w3, covfix w3 raw same-params, new PIT w5 zscore, and Horizon v2.

## HMM Daily Coefficient Generation Findings (2026-04-28)
- Existing HMM runtime behavior was correct but incomplete: it consumed completed snapshots and coefficient artifacts, then fail-fasted when no artifact covered `trade_date`.
- The missing production capability was daily artifact generation, not rolling HMM retraining.
- The implemented PIT rule is: `effective_trade_date` must be later than `as_of_trade_date`; generation reads DB data only through `as_of_trade_date`, then remaps that day's forward-filtered coefficients to `effective_trade_date`.
- Existing artifacts remain immutable. A deterministic daily filename may be reused only if metadata exactly matches; otherwise generation refuses to overwrite.
- UI E2E initially exposed a proxy timeout risk for long generation calls; using absolute dev API base on port 8012 avoids the Next dev proxy during validation.


## HMM Dynamic Coefficient Offline Experiment Findings (2026-04-29T01:11:15)
- User requested six offline HMM directions with ~1-year qlib/script validation before QE.
- Guardrail: do not modify existing HMM DB versions; do not modify AIstock application code; only HMM experiment scripts, model files, and validation artifacts are in scope.

- 2026-04-29T01:20:42: WSL distro is `Ubuntu`; qlib 0.9.6.99 is available in conda env `rdagent-gpu`; `/home/lc999/data/qlib_bin` calendar currently ends at 2026-03-10, so the 1-year test window is set to 2025-03-11 ~ 2026-03-03 to leave 5D forward-return room.

- 2026-04-29T01:29:00: Important implementation finding: `hmmlearn._hmmc.forward_log` in local 0.3.3 expects `startprob_` and `transmat_` probabilities plus log frame likelihoods. Passing log start/trans generated NaN forward lattices and invalid coefficients.

## HMM Dynamic Offline Valid Result (2026-04-29)
- Valid output root: `.codex_tmp/hmm_dynamic_offline_20260429_v3`. Earlier v1/v2 roots are diagnostic invalid because forward posterior normalization was wrong or zero-confidence.
- Best QE-ready direction: `dyncoef_pup_blend_k3_clip_0p98_1p02`, total -13.39% vs No-HMM -21.00%, Sharpe -0.292 vs -0.628, MaxDD -34.13% vs -37.34%.
- Additive overlay also improved vs baseline but is not QE-ready without runtime score-adjustment changes.
- K4 probability-up underperformed materially and should be rejected or redesigned.

## HMM Dynamic Coefficient Micro-Tuning Findings (2026-04-29)
- Completed 8 offline tuning passes totaling 112 HMM variants; no QE experiments and no DB writes were performed.
- Final best candidate: `p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`, total return -0.81%, Sharpe 0.142, MaxDD -30.91% vs No-HMM -21.00%, Sharpe -0.628, MaxDD -37.34%.
- Best direction is 3-state probability-up (PUP), 5D/10D/20D weights 0.20/0.30/0.50, lambda 0.06, asymmetric clip 0.98~1.015, confidence_scale 0.075.
- The best candidate improves because HMM-only replacements outperform raw-only candidates on all tested horizons: 5D +1.74pp, 10D +1.59pp, 20D +1.39pp.
- ER/winsor/median, K4, neutral-band/confidence-floor, cross-sectional PUP rank/z, and 20D weights above 55% are not recommended for near-term QE validation.
- The remaining negative total return is not caused only by final days; monthly drag is concentrated in 2025-03, 2025-04, and 2025-11, while HMM materially improves 2025-04, 2025-06, 2025-08, 2025-09, 2025-10, and 2026-01.
- Verified DB HMM config count remained 4 after the full offline loop.

## HMM DB vs Dynamic 1Y Script Comparison Findings (2026-04-29)
- Ran `scripts/hmm_db_vs_dynamic_1y_compare.py` against qlib daily data for 2025-03-11 ~ 2026-03-03; no DB writes and no QE experiments were performed.
- Included 5 full-window DB coefficient artifacts and 2 offline dynamic candidates; excluded 7 DB artifacts because their coefficient files only covered 2025-09-01 ~ 2026-03-03 or single dates.
- Full-window DB artifacts are diagnostic-only for the 1-year window because their train/validation periods overlap the test start; this includes the w5/zscore PIT-6m version, which is PIT-compatible for the 2025-09-01 six-month window but not for 2025-03-11 one-year validation.
- Best PIT-compatible result is still `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075`: total -0.81%, Sharpe 0.142, MaxDD -30.91%, versus No-HMM -21.00%, Sharpe -0.628, MaxDD -37.34%.
- The robust alternate `OFFLINE_DYNAMIC::p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10` is very close: total -0.95%, Sharpe 0.138, MaxDD -30.91%.
- Best DB diagnostic full-window artifact was `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_A`: total -8.74%, Sharpe -0.182, MaxDD -27.27%; it cannot be considered a formal 1-year winner due split overlap.
- DB HMM config/snapshot counts remained 4/4 after validation.

## HMM Dynamic DB Registration Findings (2026-04-29)
- User requested both dynamic candidates be added to DB and all old DB HMM versions except the recommended baseline be removed.
- Kept existing baseline: `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore`, config `b99c907b-873a-4173-a4ee-5eab266f8c49`, snapshot `bbec3863-fb67-445f-938e-66f092d18696`.
- Deleted DB configs and filesystem model directories for `HMM_BASELINE_ORIGINAL_w3_raw_unfixed__n3_diag_rw3_nozscore`, `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore`, and `HMM_HORIZON_V2_w5w10w20_oos6m__n3_diag_ms75_no_limitup`.
- Registered NEW1: `HMM_DYNAMIC_PUP_w20_50_conf_0p075_PIT1Y__n3_diag`, config `442fd70a-47b5-41ca-b4f5-96f52b81742e`, snapshot `ecd2bc1f-5b1b-4057-8815-c5590ab26804`.
- Registered NEW2: `HMM_DYNAMIC_PUP_w20_50_conf_0p10_PIT1Y__n3_diag`, config `f3fe9433-ea86-4a16-a44b-989e1398c1b2`, snapshot `daddcd16-a618-4d5b-8919-dd61fd4e5eca`.
- Both new DB versions use runtime preset `preset_A` and have coefficient artifacts covering `2025-03-11 ~ 2026-03-03`.
- Post-registration script comparison confirmed the DB-registered NEW1/NEW2 reproduce the offline results exactly: NEW1 total -0.81%, Sharpe 0.142; NEW2 total -0.95%, Sharpe 0.138.
- Different train/validation windows can materially change HMM results because hidden states, state posterior confidence, sector regimes, and validation-calibrated coefficients are all window-dependent.
- PIT follow-up check: NEW1/NEW2 train/validation/coefficient periods are non-overlapping for the 2025-03-11 ~ 2026-03-03 validation window, and training code enforces `train_end < val_start` and `val_end < test_start`.
- Residual metadata-PIT caveat: registered coefficient JSON uses a static `stock_sector_map` for runtime compatibility. The qlib/script validation itself used PIT date-sector maps, but current QE runtime consumes static `stock_sector_map`; this is not price/return leakage, but it is not perfect point-in-time industry membership. Over the 1Y window, 30 stocks have multiple overlapping membership rows in the broad overlap query.
- Strict forward-label embargo caveat: because the dynamic PUP calibration uses 5D/10D/20D forward returns inside the validation period, a validation end of 2025-03-10 is adjacent to test start 2025-03-11 and therefore late-validation 20D labels look into the test window. For a strict 20D embargo, the latest validation date before 2025-03-11 should be 2025-02-11 or earlier.


---

# Findings: ST PIT Official Factor Metrics and Cache (2026-05-06)

- Current official metric universe still uses `EvaluationUniverseService` static end-date semantics in multiple places.
- Current metric coverage denominator excludes suspend/warmup but does not include ST PIT buy eligibility.
- Current single cache metadata only validates `as_of_date`; universe metadata is missing.
- Current correlation merged cache sidecar validates `as_of_date` but not universe key/fingerprint/index policy.
- Existing `market.stock_universe_pit_spans` and `StockUniversePitService` already provide the correct first-stage ST-only PIT source.


## ST PIT factor metrics/cache implementation findings (2026-05-06)
- Root worktree had accidental task edits in `data_snapshot_manager.py`, `factor_cache_coverage.py`, `factor_value_pipeline.py`, `qe_eval_v2_metric_engine.py`, and untracked `factor_universe_mask_service.py`; the isolated task worktree was missing the new service. Changes were migrated/repaired in the task worktree without reverting root files.
- `FactorValuePipeline` had ST PIT cache-index logic but missed `_UNIVERSE_META_KEYS` and `_do_compute_factor_values(..., cache_index, cache_metadata)` parameters; py_compile caught and repairs were applied.
- `qe_eval_v2_metric_engine` needed `FactorUniverseMaskService` imports and `_pit_coverage_stats_from_masks`; unit tests now verify denominator/numerator semantics.
- The migration file initially had a UTF-8 BOM that PostgreSQL rejected; it was rewritten as UTF-8 without BOM before applying.

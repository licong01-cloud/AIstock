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

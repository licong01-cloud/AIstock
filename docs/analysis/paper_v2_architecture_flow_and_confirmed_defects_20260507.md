# Paper v2 架构、业务流程与已确认缺陷清单

生成时间：2026-05-07  
范围：Paper Trading v2、StrategyPackage、Selection Center、HMM 运行时、ST PIT 股票池、Paper v2 前端页面  
状态：只读诊断记录；未启动、停止、重启任何生产或测试服务；未写入数据库

## 1. 结论摘要

Paper v2 的目标是让 QE 回测、选股中心、模拟盘使用同一套被冻结的 StrategyPackage 合约，并在运行时通过权威 live/latest-data 推理生成选股信号。当前设计方向正确，但“从 QE 单次实验或 QE 演进 loop 直接实现选股”的产品闭环尚未完全落地。

当前最关键的问题不是单个 HTTP 404，而是以下链路尚未形成可稳定通过的端到端保证：

1. QE source 或 loop 被用户选择后，系统需要自动创建或复用 StrategyPackage。
2. StrategyPackage 必须来自启用 ST PIT risk policy 的 QE 回测合约。
3. ST PIT universe 必须覆盖目标交易日。
4. live/latest-data inference 必须能从 AIstock 物化缓存或节点 API 获得 conf、factor、model 参数等资产。
5. 严格推理必须保留至少一批 fully-scored instruments，不能默认填充缺失因子。
6. Selection Center 必须基于权威 artifact 运行，并把 fail-fast 错误在 UI 上提前暴露。
7. Paper v2 模拟盘必须使用与 QE 回测一致的 runtime profile、ST PIT risk policy、停牌/涨跌停/分钟线执行合约。

目前已确认的阻断点包括：

- 当前可选的 4 个 StrategyPackage 都是旧版非 ST PIT 包，健康状态应为 `LEGACY_NON_ST_PIT`，不能作为新 ST PIT 权威选股的成功验收对象。
- `market.stock_universe_pit_state` 当前只覆盖到 `2026-04-30`，但 `2026-05-06`、`2026-05-07`、`2026-05-08` 都是交易日；新 ST PIT 选股在这些日期会被 readiness 阻断。
- 当前产品没有一个“直接从 QE 单次实验或 QE 演进 loop 到选股结果”的原子化入口；用户仍需先创建 StrategyPackage，再到选股页选择包。
- live inference 冷启动链路存在多类历史失败：WSL inference failed、节点 API 404、`mlruns-params` 缺失、`combined_factors_df.parquet` 缺失、严格因子覆盖为 0。
- 测试端口 `8011` 当前返回的 `/selection-center/selectable-packages` 缺少 `selection_health` 字段，说明该测试进程不是当前仓库最新代码；后续验证不能复用该旧进程作为最终结论。

## 2. 运行边界和安全约束

本文件记录的操作边界如下：

- 不重启生产 FastAPI 后端 `8001`。
- 不重启生产前端 `3000`。
- 不重启 WSL / RD-Agent API `9000`。
- 不停止正在运行的 QE / RD-Agent 任务。
- 后续验证只能使用测试端口，例如后端 `8011` / `8012`，前端 `3011` / `3012`。
- 生产环境的 API 和后端只能由用户手工重启。
- Paper v2 修复应限定在 Paper v2 / StrategyPackage / Selection Center 模块范围内；不得修改 QE 实验执行核心、RD-Agent worker 代码或 QE 共用执行代码，除非用户单独授权。

## 3. 主要代码模块

### 3.1 后端 API 层

| 文件 | 职责 |
| --- | --- |
| `backend/routers/strategy_packages.py` | StrategyPackage 创建、列表、状态流转、selection artifact 生成、execution policy、model state、QE source 列表 |
| `backend/routers/selection_center.py` | Selection Center run、selectable packages、aggregate runs、加入自选、从 selection run 创建 Paper v2 portfolio |
| `backend/routers/paper_trading_v2.py` | Paper v2 portfolio、readiness、run day、replay、session、ledger、performance 等 API |
| `backend/routers/hmm_training.py` | HMM 训练、快照、日度系数、rolling training 相关 API |

### 3.2 StrategyPackage 服务层

| 文件 | 职责 |
| --- | --- |
| `backend/services/strategy_package/service.py` | 创建/保存/查询 StrategyPackage、状态流转、执行策略、model state |
| `backend/services/strategy_package/qe_source_resolver.py` | 从 QE 单次实验或 QE 演进 loop 构建冻结 manifest |
| `backend/services/strategy_package/backtest_contract.py` | 从 manifest 中抽取 QE 回测 runtime 合约，并校验 Paper/Selection 与 QE 一致 |
| `backend/services/strategy_package/selection_artifact.py` | 生成/读取 authoritative selection score artifact |
| `backend/services/strategy_package/live_inference.py` | 物化 QE runtime 资产，构建临时推理 workspace，调用本地或 WSL 推理 |
| `backend/services/strategy_package/runtime.py` | 从 authoritative artifact 构建 Selection Center 信号快照和 Paper v2 target positions |
| `backend/services/strategy_package/workspace_policy.py` | 限制直接读取 worker workspace，仅允许 AIstock 已物化资产根目录 |

### 3.3 Selection Center 服务层

| 文件 | 职责 |
| --- | --- |
| `backend/services/selection_center/service.py` | 选股主编排、单包/交集/并集/加权融合、PIT cutoff、artifact auto-generate、watchlist、selection-to-paper |
| `backend/services/selection_center/package_health.py` | StrategyPackage 健康预检，识别 `RUNNABLE` / `LEGACY_NON_ST_PIT` / `BLOCKED` |
| `backend/services/selection_center/risk_policy.py` | ST PIT 风险策略，基于 `market.stock_universe_pit_spans` 阻断买入或强制退出 |
| `backend/services/selection_center/tradability.py` | 停牌、行业黑名单等 tradability 过滤 |
| `backend/services/selection_center/hmm_runtime.py` | HMM 预计算系数加载、预检、候选股打分调整 |
| `backend/services/selection_center/runtime_profile.py` | 运行时 profile 解析，兼容旧字段，禁止未知字段和 silent fallback |

### 3.4 Paper Trading v2 服务层

| 文件或目录 | 职责 |
| --- | --- |
| `backend/services/paper_trading_v2` | Paper v2 portfolio、day runner、readiness、session、ledger、execution、performance |
| `backend/services/paper_trading_v2/readiness.py` | Paper v2 执行前 readiness 预检 |
| `backend/services/paper_trading_v2/day_runner.py` | 单日执行编排，使用 StrategyPackage 信号、target、rebalance、minute execution |
| `backend/services/paper_trading_v2/repository.py` | Paper v2 DB 持久化 |

### 3.5 前端页面

| 路由 | 职责 |
| --- | --- |
| `frontend/src/app/paper-v2/packages/page.tsx` | QE source 列表、创建 StrategyPackage、状态流转、execution policy、model state |
| `frontend/src/app/paper-v2/selection/page.tsx` | 选择 StrategyPackage 后运行选股、历史选股、聚合、加入自选 |
| `frontend/src/app/paper-v2/portfolios/page.tsx` | 创建和管理 Paper v2 portfolio |
| `frontend/src/app/paper-v2/portfolios/[portfolioId]/run-console/page.tsx` | readiness、run day、replay |
| `frontend/src/app/paper-v2/portfolios/[portfolioId]/ledger/page.tsx` | ledger、orders、fills、positions、cash |
| `frontend/src/app/paper-v2/portfolios/[portfolioId]/performance/page.tsx` | performance report |
| `frontend/src/app/paper-v2/model-hmm/page.tsx` | HMM 快照、系数、训练中心入口 |

## 4. 数据库和资产边界

### 4.1 关键 DB schema / table

| 表 | 用途 |
| --- | --- |
| `qe_experiments` | QE 单次实验和 QE 演进 loop 的统一结果记录 |
| `qe_evolution_loops` | QE 演进 loop 原始配置和状态 |
| `strategy_pkg.package` | StrategyPackage manifest 和状态 |
| `strategy_pkg.selection_score_artifact` | authoritative 或 diagnostic selection artifact |
| `selection.run` | Selection Center run 主表 |
| `selection.run_result` / 相关结果表 | Selection Center 候选股、聚合结果、排除结果 |
| `selection.paper_portfolio_link` | selection run 与 Paper v2 portfolio 的追踪关系 |
| `paper_v2.*` | Paper v2 portfolio、run、event、order、fill、position、snapshot、ledger |
| `market.stock_universe_pit_state` | ST PIT universe readiness 状态 |
| `market.stock_universe_pit_spans` | 股票在某 universe 下的 PIT 可买区间 |
| `market.dataset_date_refresh_audit` | `suspend_d`、`stk_limit`、daily data 等刷新审计 |
| `market.suspend_d` | Tushare 停牌状态 |
| `market.stk_limit` | 涨跌停价 |
| `market.kline_daily_raw` | 日线行情和参考价 |
| `market.trading_calendar` | 交易日历 |

### 4.2 关键资产目录

| 目录 | 用途 |
| --- | --- |
| `rdagent_assets/strategy_package_runtime` | StrategyPackage live inference runtime cache |
| `rdagent_assets/selection_artifacts` | selection artifact 相关资产 |
| `rdagent_assets/model_cache` | 执行模型或 runtime model cache |
| `rdagent_assets/qe_experiments` | AIstock 侧已物化 QE 实验资产 |
| `backend/data/hmm_models` | HMM 快照和日度系数 |
| `qe_archive` | QE 归档资产 |

### 4.3 Workspace path policy

当前策略是：

- 允许访问 AIstock 已物化 runtime cache，例如 `F:\Dev\AIstock\rdagent_assets\strategy_package_runtime\...`。
- 在 WSL 进程中，如果 `/mnt/...` 路径指向 AIstock 允许的资产根目录，可以访问。
- 禁止直接读取 worker workspace：
  - `qe_workspace`
  - `rdagent_workspace`
  - `\\wsl$...`
  - `\\wsl.localhost...`
- 这样做的原因是 worker workspace 属于远端/隔离执行环境，生产代码不能绕过节点 API 或已物化缓存直接读 worker 内部路径，否则会出现本机可用、生产不可用、资产不可追溯的问题。

## 5. 端到端业务流程

### 5.1 QE source 到 StrategyPackage

当前流程：

1. 前端 `/paper-v2/packages` 调用：
   - `GET /api/v1/strategy-packages/qe-sources?source_kind=all&limit=...`
2. 后端从 `qe_experiments` 列出可打包 source：
   - 单次实验：`source_kind=qe_experiment`
   - 演进 loop：`source_kind=qe_evolution_loop`
3. 用户点击创建：
   - 单次实验：`POST /api/v1/strategy-packages/from-qe-experiment`
   - 演进 loop：`POST /api/v1/strategy-packages/from-qe-evolution-loop`
4. `QEExperimentSourceResolver` 构建 manifest：
   - source identity
   - alpha components
   - universe policy
   - portfolio policy
   - minute execution policy
   - strategy_config / custom_params
   - asset checks
5. `freeze_manifest` 生成 `manifest_sha256`。
6. manifest 和状态写入 `strategy_pkg.package`。

关键原则：

- manifest 冻结后不可被状态流转修改。
- 状态流转不进入 `manifest_sha256`。
- Paper v2 和 Selection Center 必须以 manifest 中的 QE 回测合约为准。

### 5.2 StrategyPackage 到 Selection Center

当前流程：

1. 前端 `/paper-v2/selection` 调用：
   - `GET /api/v1/selection-center/selectable-packages`
2. 后端返回可选包：
   - `BACKTEST_APPROVED`
   - `SELECTION_ENABLED`
   - `PAPER_ENABLED`
3. 前端选择一个或多个包，构造 runtime config：
   - `st_pit_authoritative=true`
   - `selection_artifact_config.auto_generate=true`
   - `selection_artifact_config.inference_backend=wsl`
   - `runtime_profile.selection.top_k`
   - `runtime_profile.tradability.exclude_suspended`
   - `runtime_profile.hmm`
   - `runtime_profile.industry_blacklist`
4. 调用：
   - `POST /api/v1/selection-center/runs`
5. 后端执行：
   - normalize runtime config
   - PIT cutoff 解析
   - package health preflight
   - data readiness preflight
   - authoritative artifact 存在性检查
   - 缺失时调用 live inference 生成 artifact
   - `StrategyPackageRuntime.build_signal_snapshot`
   - ST PIT risk policy
   - 停牌/行业黑名单过滤
   - 单包或多包聚合
   - 写入 selection run 和结果

### 5.3 Selection artifact 生成

当前 authoritative artifact 生成流程：

1. `StrategyPackageSelectionArtifactService.generate_from_live_inference`
2. 读取 StrategyPackage 和 frozen manifest。
3. 解析 runtime hash：
   - hash 只包含 score-production config。
   - `auto_generate`、`force_regenerate` 不参与 hash。
4. `QEExperimentRuntimeAssetResolver.load_source_for_strategy_package` 根据 source identity 解析 QE source。
5. `QEExperimentRuntimeAssetResolver._materialize_runtime_source_from_node` 通过节点 API 下载：
   - `conf.yaml`
   - factor files
   - optional `model.py`
   - static loader schema
   - `mlruns` model params archive
6. 构建临时推理 workspace：
   - `manifest.json`
   - `factor_order.json`
   - `strategy_package_factor_entry.py`
   - `model/params.pkl`
7. WSL provider 调用：
   - `scripts/strategy_package_live_inference.py`
8. `backend/inference_engine.py` 严格推理。
9. 生成 rows：
   - `symbol`
   - `score`
   - `rank`
   - `target_weight`
   - `reference_price`
   - `component_scores`
10. 保存到 `strategy_pkg.selection_score_artifact`。

关键原则：

- authoritative selection artifact 必须是 `live_qe_model_inference_v1`。
- `qe_mlruns_pred_pkl_v1` 只能用于 diagnostic backtest，不允许作为权威选股输入。
- strict inference 不能默认补 0、不能静默填充缺失因子、不能返回空成功。

### 5.4 Selection run 到自选股票池

当前流程：

1. 用户在 selection run 成功后点击加入自选。
2. 调用：
   - `POST /api/v1/selection-center/runs/{run_id}/add-to-watchlist`
3. 后端要求：
   - run 必须 `SUCCEEDED`
   - 候选股必须有 `reference_price`
   - 写入 watchlist category 和 items
   - 记录 source run、rank、entry price、entry as-of date

关键原则：

- 缺 entry price 必须失败。
- 不允许用实时价或默认价冒充成功。

### 5.5 Selection run 到 Paper v2 portfolio

当前流程：

1. 只允许单包 selection run 创建 portfolio。
2. 多包 union/intersection/weighted_fusion 只允许研究聚合，不允许直接创建模拟盘组合。
3. 调用：
   - `POST /api/v1/selection-center/runs/{run_id}/create-paper-portfolio`
4. 后端生成 Paper v2 portfolio。
5. Paper v2 日运行时不会复用 selection run 的 raw scores；必须按交易日重新生成或读取 authoritative live selection artifact。

关键原则：

- Selection run 到 Paper portfolio 只是 source trace。
- Paper v2 运行当天必须重新走权威 StrategyPackage runtime。

### 5.6 Paper v2 readiness / run / replay / live session

Paper v2 day run 的核心流程：

1. portfolio 状态必须 `READY`。
2. 验证交易日历。
3. 验证重复 run 策略。
4. 验证 StrategyPackage manifest。
5. 验证 execution policy 与 manifest minute execution policy 一致。
6. 验证数据审计：
   - `suspend_d`
   - `stk_limit`
   - minute bar
   - trade calendar
7. 运行 Selection runtime 生成候选股。
8. 生成 target positions。
9. 生成 rebalance intents。
10. 加载 minute market data。
11. 执行订单模拟。
12. 写入：
    - run events
    - orders
    - fills
    - positions
    - cash ledger
    - snapshots
    - errors

关键原则：

- 不能 daily fallback。
- 不能默认价格。
- 不能静默跳过缺失分钟线。
- 不能绕过 ST PIT 风险策略。

## 6. 当前已确认缺陷

### P0-1：当前 4 个可选 StrategyPackage 全部是旧版非 ST PIT 包

确认结果：

| package_id | source | loop | 状态 | 健康结论 |
| --- | --- | --- | --- | --- |
| `pkg_1de32357724a4c5b874f2abd90f22da5` | `qe_20260502_231229_0565` | `Loop1` | `BACKTEST_APPROVED` | `LEGACY_NON_ST_PIT` |
| `pkg_99142cb1440c40a7824e83902f4e7da9` | `qe_20260416_082012` | `Loop1` | `SELECTION_ENABLED` | `LEGACY_NON_ST_PIT` |
| `pkg_006a42323f7c4e81a468fdaad2cb16a3` | `qe_20260413_084216` | `Loop1` | `SELECTION_ENABLED` | `LEGACY_NON_ST_PIT` |
| `pkg_b668f8a633c44b72a5d557a2cb8970e3` | `qe_20260416_002701` | `Loop1` | `SELECTION_ENABLED` | `LEGACY_NON_ST_PIT` |

原因：

- 这些包的冻结 QE 回测合约没有启用 ST PIT risk policy。
- 新的 Selection Center / Paper v2 要求回测和模拟盘使用同一 ST PIT 引擎，因此这些包不能作为新权威流程的成功样例。

影响：

- 用户在选股中心看到这些包时会被健康预检阻断。
- 如果某个旧服务实例没有返回 `selection_health`，前端可能误导用户继续尝试。

处理建议：

- 不修旧包的 manifest，不伪造 ST PIT 合约。
- 用新的 ST PIT QE 实验或 loop 创建新的 StrategyPackage。
- UI 中应明确区分“历史旧包”和“可运行 ST PIT 包”。

### P0-2：ST PIT universe readiness 未覆盖当前交易日

确认结果：

`market.stock_universe_pit_state`：

| universe_key | status | dirty | start_date | end_date |
| --- | --- | --- | --- | --- |
| `shsz_st_pit_active_v1` | `ready` | `false` | `2018-08-01` | `2026-04-30` |

交易日历：

| 日期 | 是否交易日 |
| --- | --- |
| `2026-05-06` | 是 |
| `2026-05-07` | 是 |
| `2026-05-08` | 是 |

影响：

- 新 ST PIT 包在 `2026-05-06` 及之后做 selection 或 Paper v2 readiness，会被 `ST PIT risk policy universe does not cover trade_date` 阻断。
- 当前日线行情已到 `2026-05-06`，但 ST PIT spans 只到 `2026-04-30`，数据层存在时间不一致。

处理建议：

- 先由数据管线刷新或重建 `market.stock_universe_pit_spans` 和 `market.stock_universe_pit_state` 到最新交易日。
- Selection Center 在用户选择交易日时提前展示 ST PIT 覆盖范围，不等运行后失败。

### P0-3：缺少“从 QE 单次实验或演进 loop 直接选股”的原子化入口

现状：

- 用户需要先进入 `/paper-v2/packages` 创建 StrategyPackage。
- 再进入 `/paper-v2/selection` 选择 StrategyPackage。
- 选股页不能直接选择 QE 单次实验或 QE 演进 loop。

用户期望：

- 可以直接选择 QE 单次实验。
- 可以直接选择 QE 演进实验中的某个 loop。
- 系统直接完成选股。

当前缺口：

- 缺少类似以下 API：
  - `POST /api/v1/selection-center/runs/from-qe-source`
- 缺少后端原子编排：
  - source 解析
  - 创建或复用 StrategyPackage
  - ST PIT 合约校验
  - asset preflight
  - authoritative artifact 生成
  - selection run
  - 返回候选股
- 缺少前端 QE source/loop 直选 UI。

影响：

- 用户需要跨页面、跨概念操作。
- 每一步失败都需要用户重新尝试。
- 系统无法在开始前一次性展示所有阻断点。

处理建议：

- 新增 Paper v2 范围内的一键选股编排入口。
- 保留 StrategyPackage 作为内部持久化合约，不让用户必须手工理解和操作每个中间状态。

### P0-4：live inference 冷启动链路存在不稳定失败

历史失败分组：

| 错误 | 次数 | 最近样例 |
| --- | ---: | --- |
| `WSL live QE model inference failed` | 30 | `sel_554adb83a35244d2b73f47ded9b01afd` |
| `QE experiment does not exist for live inference` | 3 | `sel_036816fe1f6647248d879ed343f83a2a` |
| `failed to materialize QE runtime assets through the node API` | 3 | `sel_59cd5038940048649a75b385cb7b0d25` |
| `HMM coefficient artifact is missing stock sector mapping` | 20 | `sel_5693d0827804425c8e7241e155baff16` |
| `selection aggregation produced no candidates` | 3 | `sel_32100ac6ebe340d7bfc2d7c919b407af` |
| `no HMM coefficient artifact covers trade_date` | 2 | `sel_f96c4baf3f954b128a4cce6567303d54` |

已观察到的资产问题：

- `mlruns-params` 下载 `HTTP 404`。
- `combined_factors_df.parquet` 下载 `HTTP 404`。
- 某些本地已物化 QE 资产只有 `conf.yaml` 和 `factors`，没有 `params.pkl`。
- 某些远端节点 API 与 DB 中记录的 `execution_node_id` / `qe_task_id` / `qe_loop_id` 不完全一致。

影响：

- 用户点击选股后等待较长时间才失败。
- 错误被包装成 `DATA_UNAVAILABLE`，但 UI 缺少提前诊断。

处理建议：

- 在 selection 运行前做 asset preflight：
  - source record 存在。
  - node 可达。
  - `conf.yaml` 可读。
  - factor files 可读。
  - model params 可读。
  - factor order 可解析。
  - runtime workspace 可构建。
- 不允许直接读 worker workspace。
- 优先复用 AIstock 已物化 runtime cache。
- 若缺资产，应在按钮运行前返回明确缺失项。

### P0-5：严格因子覆盖可能为 0

最新确认样例：

- run：`sel_554adb83a35244d2b73f47ded9b01afd`
- package：`pkg_1de32357724a4c5b874f2abd90f22da5`
- trade_date：`2026-05-06`
- 错误：`WSL live QE model inference failed`
- 关键上下文：
  - `input_rows=4636`
  - `kept_rows=0`
  - `dropped_rows=4636`
  - `invalid_cell_count=13047`

原因：

- 严格 StrategyPackage inference 要求 fully-scored instruments。
- 多个因子列在目标日期或 cutoff 上存在缺失/非法值。
- 系统正确拒绝了默认填充，但没有在选股前暴露覆盖率预检。

影响：

- 这是当前用户最直接感知的“选股操作失败”之一。
- 即使资产能下载，推理仍可能因为数据/因子覆盖失败。

处理建议：

- 增加 feature coverage preflight。
- 预检输出：
  - input rows
  - kept rows
  - dropped rows
  - invalid columns top N
  - factor coverage by source
  - cutoff date
  - trade date
- 若 `kept_rows=0` 或低于阈值，直接阻断，不进入正式 selection run。

### P0-6：测试端口 `8011` 不是当前最新代码

确认现象：

- 当前仓库代码 `SelectionCenterService.list_selectable_packages` 会返回 `selection_health`。
- 测试端口 `8011` 的 `/api/v1/selection-center/selectable-packages` 返回中没有 `selection_health` 字段。

影响：

- 用该测试进程验证 Paper v2，会得到旧行为。
- 前端可能不能正确禁用 legacy / blocked 包。

处理建议：

- 后续测试必须启动新的测试端口服务，例如 `8012`。
- 不得重启生产 `8001`。
- 测试服务应在启动日志中记录 git commit 和端口。

### P1-1：HMM artifact 历史上存在 sector map 缺失

确认历史失败：

- 错误：`HMM coefficient artifact is missing stock sector mapping`
- 次数：20
- 典型上下文包含：
  - symbol
  - raw_rank
  - package_id
  - trade_date
  - snapshot_id
  - coefficients_path

当前代码状态：

- `hmm_runtime.py` 和 `package_health.py` 已加入 preflight，可以在 live inference 前阻断坏 HMM artifact。

剩余风险：

- 历史 HMM 系数文件本身仍可能缺 stock-sector map。
- UI 需要避免用户选择不覆盖目标日期或缺 sector map 的 snapshot。

处理建议：

- HMM snapshot 列表应显示覆盖日期、preset、stock_sector_map 完整性。
- 对缺 map 的 artifact 需要重新生成，而不是在 Selection Center 里兜底。

### P1-2：旧包已有 selection artifact，但不能证明新 ST PIT 权威流程可用

确认结果：

- 旧包已有多个 `live_qe_model_inference_v1` artifact。
- 例如：
  - `pkg_006a...` 在 `2026-04-29` 有 3661 条 score。
  - `pkg_b668...` 在 `2026-04-24` 有 786 条 score。
  - `pkg_99142...` 在 `2026-04-24` 有 785 条 score。

问题：

- 这些 artifact 属于旧非 ST PIT 包。
- 成功读取旧 artifact 不等于新 ST PIT 回测合约可在 Paper v2 中跑通。

处理建议：

- 历史 artifact 可保留用于诊断。
- 新验收必须使用 ST PIT QE source 重新打包并生成 artifact。

### P1-3：StrategyPackage model state 多为 stale 初始回测模型

确认结果：

- 当前可选包 model state 多为 `STALE_INITIAL_BACKTEST_MODEL`。

影响：

- 不直接阻断选股。
- 但对模拟盘长期运行有模型新鲜度风险。

处理建议：

- 短期不作为选股 P0。
- 中期在 Paper v2 readiness 中作为 warning。
- 长期接入手工确认的模型再训练流程。

### P1-4：`QE experiment does not exist for live inference` 曾由 source identity 解析不一致触发

历史现象：

- 新加入策略包 `qe_20260502_231229_0565` 选股时出现：
  - `DATA_UNAVAILABLE: QE experiment does not exist for live inference`
  - `HTTP 404`

原因类别：

- QE evolution loop package 不能只按 `source_id` 当作 `experiment_id` 查。
- 对演进 loop，应该使用：
  - `source_type=qe_evolution_loop`
  - `source_id=qe_task_id`
  - `loop_id=LoopN`
  - `run_id=qe_task_id_LN`

当前代码状态：

- `QEExperimentRuntimeAssetResolver.load_source_for_strategy_package` 已支持 `qe_evolution_loop` 使用 `qe_task_id + qe_loop_id` 查找。

剩余风险：

- 如果生产后端未重启到新代码，仍会出现旧行为。
- 如果测试端口使用旧进程，也会出现旧行为。

处理建议：

- 不重启生产，由用户手工重启。
- 后续验证使用新测试端口。
- API 响应里应明确 source lookup 使用的是 experiment id 还是 task+loop。

## 7. 当前可用的新 ST PIT QE source

只读检查显示，已有若干新 ST PIT QE source 可以构建 manifest，示例：

| source_kind | source | loop | ST PIT risk policy | manifest dry-run |
| --- | --- | --- | --- | --- |
| `qe_experiment` | `qe_20260506_182113` | `Loop1` | enabled | 成功 |
| `qe_evolution_loop` | `qe_20260506_220823_6489` | `Loop1` | enabled | 成功 |
| `qe_evolution_loop` | `qe_20260506_004257_b34a` | `Loop7` | enabled | 成功 |

这些 source 的共同点：

- `risk_policy.enabled=true`
- `providers=["st_pit"]`
- `hard_actions=["block_buy","force_exit"]`
- strategy family 为 `score_weighted_topk_v2`

但它们尚未完成以下验证：

- 创建或复用 StrategyPackage。
- live inference asset preflight。
- authoritative selection artifact 生成。
- strict feature coverage 非 0。
- Selection Center run 成功。
- Paper v2 readiness 成功。

因此，不能仅凭 manifest dry-run 成功宣称选股可用。

## 8. 推荐目标架构修正

### 8.1 新增一键 QE source 选股 API

建议新增 Paper v2 / Selection Center 范围内的后端入口：

```text
POST /api/v1/selection-center/runs/from-qe-source
```

建议 payload：

```json
{
  "source_kind": "qe_experiment",
  "experiment_id": "qe_20260506_182113",
  "qe_task_id": null,
  "qe_loop_id": null,
  "trade_date": "2026-04-30",
  "data_source": "DB_HISTORICAL",
  "mode": "single_package",
  "runtime_config": {
    "st_pit_authoritative": true,
    "display_top_n": 20,
    "selection_artifact_config": {
      "auto_generate": true,
      "inference_backend": "wsl",
      "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE"
    }
  }
}
```

演进 loop payload：

```json
{
  "source_kind": "qe_evolution_loop",
  "qe_task_id": "qe_20260506_004257_b34a",
  "qe_loop_id": "Loop7",
  "trade_date": "2026-04-30",
  "data_source": "DB_HISTORICAL",
  "mode": "single_package",
  "runtime_config": {
    "st_pit_authoritative": true,
    "display_top_n": 20,
    "selection_artifact_config": {
      "auto_generate": true,
      "inference_backend": "wsl",
      "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE"
    }
  }
}
```

后端内部流程：

1. 解析 QE source。
2. 查找是否已有相同 source identity 的 StrategyPackage。
3. 没有则创建 StrategyPackage。
4. 校验 frozen manifest 包含 ST PIT risk policy。
5. 校验 ST PIT universe 覆盖目标交易日或 PIT cutoff。
6. 校验数据刷新审计。
7. 校验 runtime assets。
8. 生成或复用 authoritative selection artifact。
9. 执行 Selection Center run。
10. 返回：
    - package
    - package_health
    - preflight report
    - selection run
    - candidates
    - excluded results

### 8.2 新增 preflight-only API

建议新增：

```text
POST /api/v1/selection-center/preflight/qe-source
```

用途：

- 用户选择 QE source/loop 后，先展示所有阻断点。
- 不创建 selection run。
- 可选择是否创建/复用 StrategyPackage，但不做正式选股。

建议返回：

```json
{
  "ok": false,
  "source": {},
  "package": {},
  "checks": [
    {
      "name": "st_pit_contract",
      "status": "PASS",
      "message": "frozen QE backtest contract contains ST PIT risk policy"
    },
    {
      "name": "st_pit_universe_coverage",
      "status": "BLOCKED",
      "message": "ST PIT universe does not cover trade_date",
      "context": {
        "trade_date": "2026-05-06",
        "coverage_end": "2026-04-30"
      }
    }
  ]
}
```

### 8.3 UI 改造

`/paper-v2/selection` 应增加两种运行模式：

1. `从 StrategyPackage 选股`
2. `从 QE 实验/Loop 直接选股`

第二种模式应提供：

- source_kind 下拉：
  - QE 单次实验
  - QE 演进 loop
- QE source 搜索和排序。
- loop 指标展示：
  - annual return
  - IC / Rank IC
  - max drawdown
  - Sharpe
  - ST PIT 合约状态
- 预检按钮。
- 运行选股按钮。
- 失败时展示结构化阻断点，不显示长 traceback 作为主要信息。

### 8.4 Health gate 行为

健康状态建议分层：

| 状态 | 含义 | UI 行为 |
| --- | --- | --- |
| `RUNNABLE` | 可运行 | 允许选股 |
| `WARN` | 可运行但有风险 | 允许，展示 warning |
| `BLOCKED` | 明确阻断 | 禁用运行 |
| `LEGACY_NON_ST_PIT` | 旧非 ST PIT 包 | 新权威模式禁用 |
| `UNKNOWN` | 尚未检查 | 要求先预检 |

## 9. 必须补齐的验证矩阵

### 9.1 后端单测

必须覆盖：

- QE 单次实验 source 创建/复用 StrategyPackage。
- QE evolution loop 创建/复用 StrategyPackage。
- 旧非 ST PIT 包被阻断。
- ST PIT universe 不覆盖交易日被阻断。
- node asset 缺失被 preflight 阻断。
- strict feature coverage 为 0 被 preflight 阻断。
- HMM artifact 缺 stock sector map 被 preflight 阻断。
- authoritative artifact 存在时跳过再生成。
- diagnostic `pred.pkl` artifact 不可进入权威 Selection Center。

### 9.2 后端 API 集成测试

必须覆盖：

1. `POST /selection-center/preflight/qe-source`
2. `POST /selection-center/runs/from-qe-source`
3. `POST /selection-center/runs`
4. `GET /selection-center/selectable-packages`
5. `GET /selection-center/runs/{run_id}`
6. `GET /selection-center/runs/{run_id}/excluded-results`
7. `POST /selection-center/runs/{run_id}/add-to-watchlist`

### 9.3 真实业务验证

至少需要两条真实成功路径：

| 路径 | 要求 |
| --- | --- |
| QE 单次实验 | 使用 `qe_20260506_182113` 或后续更新的 ST PIT 单次实验，完成 preflight、artifact、selection run |
| QE 演进 loop | 使用 `qe_20260506_004257_b34a / Loop7` 或后续更新的 ST PIT loop，完成 preflight、artifact、selection run |

每条路径都必须记录：

- source identity
- StrategyPackage id
- manifest sha256
- trade date
- cutoff date
- data source
- artifact id
- artifact runtime hash
- score count
- kept rows
- excluded count
- final candidate count
- top 20 symbols

### 9.4 UI E2E

必须覆盖：

- 从 QE 单次实验直接选股。
- 从 QE evolution loop 直接选股。
- 旧包显示 `LEGACY_NON_ST_PIT` 并禁用。
- ST PIT coverage 不足时按钮禁用或预检 BLOCKED。
- WSL/node asset 缺失时显示结构化错误。
- 成功 run 后展示候选股、排除股、artifact trace。
- 加入自选成功和缺 reference price fail-fast。

### 9.5 测试端口要求

所有验证必须在测试端口执行：

- 后端：`8011` 或 `8012`
- 前端：`3011` 或 `3012`

禁止：

- 重启生产 `8001`
- 重启生产 `3000`
- 重启 WSL API `9000`
- 停止正在运行的 QE 任务

## 10. 修复优先级建议

### 第一阶段：阻断点前置化

目标：用户不再通过反复点击选股发现基础错误。

任务：

1. 增加 QE source / StrategyPackage preflight 服务。
2. 输出 ST PIT 合约、ST PIT universe、asset、artifact、HMM、feature coverage 状态。
3. 前端在运行前展示阻断点。
4. 旧包默认禁用，明确标识 `LEGACY_NON_ST_PIT`。

### 第二阶段：一键从 QE source 选股

目标：满足用户“直接选择 QE 单次实验或演进 loop 实现选股”的要求。

任务：

1. 新增 `runs/from-qe-source` API。
2. 后端自动创建或复用 StrategyPackage。
3. 自动生成 authoritative artifact。
4. 自动执行 selection run。
5. 前端新增 QE source 直选模式。

### 第三阶段：真实 ST PIT 验收

目标：证明新回测、新选股、新模拟盘使用同一引擎。

任务：

1. 刷新 ST PIT universe 到最新交易日。
2. 选择一个 QE 单次实验和一个 QE evolution loop。
3. 跑通 artifact 和 selection。
4. 创建 Paper v2 portfolio 并 readiness。
5. 运行至少一个历史交易日 replay。
6. 保存验证记录到 `tests/aistock_validation/history/paper_v2_selection_center/`。

### 第四阶段：Paper v2 模拟盘一致性收口

目标：Paper v2 执行端完全继承 QE 回测合约。

任务：

1. Paper v2 readiness 复用同一 runtime contract。
2. Paper v2 run 强制使用 ST PIT risk policy。
3. Paper v2 不允许 UI runtime 覆盖 QE 回测中未启用的 HMM、行业黑名单、risk policy。
4. execution policy 必须与 manifest minute execution policy 一致。

## 11. 当前不可接受的做法

以下做法会制造假成功，必须禁止：

- 用 QE 回测 `pred.pkl` 作为权威当前选股输入。
- 旧包缺 ST PIT 合约时，在 Paper v2 runtime 中强行打开 ST PIT。
- ST PIT universe 不覆盖目标交易日时继续选股。
- live inference 因子缺失时默认填 0 或填均值。
- 参考价缺失时用实时价或默认价写入自选。
- WSL/node API 404 时直接扫描 worker workspace。
- Paper v2 缺分钟线时回退到日线执行。
- 测试端口验证时误操作生产 `8001` / `3000` / `9000`。

## 12. 当前状态表

| 项目 | 当前状态 | 是否阻断 |
| --- | --- | --- |
| Paper v2 基础路由和 UI | 已存在 | 否 |
| StrategyPackage manifest 冻结合约 | 已存在 | 否 |
| Selection Center 单包/多包聚合 | 已存在 | 否 |
| authoritative artifact 设计 | 已存在 | 否 |
| QE evolution loop source identity 修复 | 代码已在当前仓库 | 取决于服务是否重启 |
| 当前可选包 ST PIT 合约 | 全部旧非 ST PIT | 是 |
| ST PIT universe 覆盖当前交易日 | 只到 `2026-04-30` | 是 |
| QE source/loop 直接选股入口 | 不存在 | 是 |
| live inference 冷启动资产预检 | 不完整 | 是 |
| strict feature coverage 预检 | 不完整 | 是 |
| HMM artifact preflight | 已加强，但历史坏 artifact 仍存在 | 部分阻断 |
| 测试端口 `8011` 最新性 | 不是当前最新代码 | 是，不能作为验收 |

## 13. 下一步验收定义

只有满足以下条件，才能向用户报告“选股功能可用”：

1. 测试后端使用最新代码启动在测试端口。
2. 测试前端使用最新代码启动在测试端口。
3. 不触碰生产服务。
4. ST PIT universe 覆盖所选 trade date。
5. 至少一个 QE 单次实验从 source 直选到 selection 成功。
6. 至少一个 QE evolution loop 从 source 直选到 selection 成功。
7. 成功结果来自 `live_qe_model_inference_v1`。
8. strict inference kept rows 大于 0。
9. Selection run 候选股不为空。
10. 结果可查看 excluded rows 和 trace。
11. 加入自选成功路径和缺 reference price 失败路径都通过。
12. Paper v2 readiness 至少对一个新 ST PIT 包通过或给出非伪成功的明确阻断点。

## 14. 相关文件索引

设计和记录：

- `docs/architecture/paper_v2_ui_selection_portfolio_completion_plan.md`
- `docs/architecture/paper_v2_selection_business_flow.md`
- `docs/architecture/strategy_package_authoritative_selection_inference.md`
- `tests/aistock_validation/modules/paper_v2_selection_center.md`

后端：

- `backend/routers/strategy_packages.py`
- `backend/routers/selection_center.py`
- `backend/services/strategy_package/qe_source_resolver.py`
- `backend/services/strategy_package/live_inference.py`
- `backend/services/strategy_package/selection_artifact.py`
- `backend/services/strategy_package/backtest_contract.py`
- `backend/services/strategy_package/workspace_policy.py`
- `backend/services/selection_center/service.py`
- `backend/services/selection_center/package_health.py`
- `backend/services/selection_center/risk_policy.py`
- `backend/services/selection_center/hmm_runtime.py`

前端：

- `frontend/src/app/paper-v2/packages/page.tsx`
- `frontend/src/app/paper-v2/selection/page.tsx`
- `frontend/src/lib/paper-v2/api.ts`
- `frontend/src/lib/paper-v2/types.ts`

测试：

- `backend/tests/selection_center/test_runtime_selection.py`
- `backend/tests/selection_center/test_risk_policy.py`
- `backend/tests/strategy_package/test_qe_source_resolver.py`
- `backend/tests/strategy_package/test_backtest_contract.py`
- `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`


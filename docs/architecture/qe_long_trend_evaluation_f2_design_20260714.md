# QE 长期上涨趋势评价层 F2 设计

- 版本：v1.5
- 日期：2026-07-15
- 状态：`PHASE1_VERIFIED_SOURCE_DELIVERY_THIS_CHANGESET_PLATFORM_PENDING`
- 任务分级：`T3 / F2`
- 模块：`QuantEvolver / QE-only Evaluation Store / QE Archive Read Model / QE UI`
- 风险等级：高（跨计算节点、制品、数仓、API、MCP 与 UI）
- 当前阶段：Phase 1 / 工作流 A 的 QE-only 契约、严格数据读取、纯计算核、entry/exit evidence bridge 和 authoritative portfolio 已实现并通过定向 oracle；CAS、状态机、三表、API/MCP/UI、历史补算、真实 Qlib resolver 与 E2E 仍按 Phase 2–5 继续。本 changeset 不执行 DDL、不创建评价任务、不重启服务，也不预写 PR/merge 状态
- 上位蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` 第 9.6 节与 F-014
- 相关蓝图：`docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md` Phase 8

## 1. Background / 背景

QE 已支持 `1/3/5/10/20/30/40/60/120/180D` 训练标签，标签公式为：

```text
label_h = close[T+h+1] / close[T+1] - 1
```

该能力解决“模型能否使用更长训练目标”，但没有回答 Type B 长期上涨趋势策略的核心业务问题：

1. h20 RankIC 是否能转化为 60–180 个交易日的右尾收益；
2. 模型能否提前召回最终上涨 30%/50%/70% 的股票；
3. 策略是否在趋势继续时过早退出；
4. 板块轮动信号究竟改善板块选择、板块内排序，还是只增加行业集中；
5. 最新一年、最近六个月和板块集中行情中的结果是否仍成立。

当前 QE 的 `read_exp_res.py` 已从 Recorder 提取 IC、收益曲线、交易诊断、预测诊断、Top20/Top50 单一标签收益和账户收益；`PredictionArtifactStore` 已以 CAS 形式保存 `pred.pkl/label.pkl/params.pkl`；QE Archive 已有 run identity 和 reproducibility manifest；Loop UI 已能读取 `enhanced_metrics`。这些代码只作为现状与实现模式参考。本能力不得改变通用 Prediction Store 的 artifact 映射，也不得改变 QE Archive 已有 `run_metric/run_curve/run_artifact/raw_payload` 或 Paper v2 扩展表；长期评价使用 QE 专属 CAS namespace 和 additive 评价表。

长期评价是实验结果诊断，不是训练标签、因子独立指标、因子缓存、相关性计算、荐股模型或模拟盘运行逻辑。评价失败不能伪装成成功，也不能反向把已经成功的训练/回测标记为失败；它必须形成独立、可查询、可重试的任务状态、指标族状态和 reason code。任何输入缺失、证据不足、样本未成熟或平台功能未交付都只限定相应结论的可计算性、可验证性或展示能力，不构成研究许可条件，也不得淘汰、暂停或禁止某个研究方向。

## 2. Scope / 范围

### 2.1 目标

本设计覆盖：

1. 对单次实验、自动演进 Loop、自定义演进 Loop 和可复算组合腿的已归档预测执行统一长期趋势评价；
2. 计算 20/40/60/120/180D 收益、RankIC、Top-K 右尾召回、30%/50%/70% 目标触达、ordered stage、删失调整的 stage survival、time-to-hit、MFE/MAE、右删失和路径覆盖；
3. 从 Qlib position/trade artifact 重建持仓 episode，计算趋势捕获率、退出后机会和 false early-exit；
4. 使用信号日 PIT `l2_code_id` 输出板块分解、板块集中和板块内/板块间归因；
5. 支持实验完成时自动评价，以及对已完成 R6 等历史 Loop 做 `long_trend_only` 结果评价；两种入口复用同一引擎和身份契约；
6. 将小型汇总指标固化到 QE 专属 evaluation 表，将逐信号和逐 episode 明细固化为 QE 专属 CAS Parquet，不把百万级明细写入 PostgreSQL；
7. 在 QE Loop 详情、QE Archive 比较页和只读 QE MCP 中展示相同结果、成熟度和失败原因；
8. 保证后台重启不终止已提交到计算节点的评价任务，不重复启动，不重复归档。
9. 在不把评价器变成执行模拟器的前提下，将理论 `T+1 close_qfq` 机会与实际 signal→fill/exit 证据桥接，量化涨跌停、停牌和延迟成交造成的长期趋势捕获损失。

### 2.2 交付边界

实施完成时必须同时覆盖计算引擎、异步状态、制品、数仓、API、MCP、UI、历史结果评价和回归测试。只提供离线脚本、只写 JSON、不入数仓、只做后端不做 UI、只做新实验不支持已完成 Loop，均不构成本设计的完整实现。

为提高吞吐，实施允许三个工作流并行：`计算/统计/可成交性`、`CAS/状态/三表/幂等恢复`、`API/MCP/UI/历史补算`。状态分为三条互不替代、互不传播阻断的轴：

- `task_status`：queued/running/succeeded/partial/failed，描述本次计算任务生命周期；
- `family_status`：分别描述 `signal_path`、`position_episode`、`portfolio_result`、`order_fill`、`execution_cause`、`sector_regime` 是否 `COMPUTED`、`COMPUTED_WITH_LIMITATIONS`、`NOT_COMPUTABLE` 或 `NOT_VERIFIABLE`；
- `platform_delivery_status`：core_compute/cas/database/api/mcp/ui/backfill/e2e 各子项的实施进度，只描述平台是否完整交付。

六个指标族独立解析输入、独立计算、独立持久化状态。某族不可计算或不可验证时，其他族继续执行；receipt 必须生成对应的数据获取、补归档或补算计划。期限选择、R8B2、R8M、R8C 和后续研究可以使用当时已计算出的结果，并必须连同适用范围、缺失项和相互印证情况一起分析。平台最终交付仍覆盖计算、CAS、数仓、API、MCP、UI、历史补算和真实 E2E，但任何平台子项的完成度都不决定科研结果是否可分析。

### 2.3 科研分析原则

1. F-014 不定义研究通过/失败门禁，不输出“允许研究”或“禁止研究”的全局布尔值。
2. 数据不全首先形成可见的数据问题、影响范围和获取/补算方案；不得把“尚未获得”解释为方向无效。
3. 同一实验中可用的不同证据应分别计算，并允许做交叉印证、差异归因和潜在损失估计；不得因其中一族缺失而丢弃其余结果。
4. 统计显著性、覆盖率、成熟度、可复算性和工程测试均作为质量信息随结果展示，不是研究启动、期限选择或继续演进的许可条件。
5. 唯一硬边界是 QE-only 隔离：评价只能读取 QE 实验身份、QE 数据集和 QE 制品，且对 Selection、Advisory、Paper、模拟盘、荐股、QMT、StrategyPackage 等非 QE 模块保持零影响。

## 3. Non-Goals / 非目标

1. 不改变任何模型训练、因子选择、标签公式、回测策略、仓位、交易成本或执行算法。
2. 不自动启动新的 QE 实验，不在本能力内选择 R6/R7 冠军。
3. 不重新计算因子独立指标、官方因子缓存或因子相关性。
4. 不做行业中性化；板块轮动收益保持为目标信号的一部分。
5. 不读取或修改 Selection、Paper v2、模拟盘、荐股运行时的 PIT 数据；只消费 QE 数据集契约。
6. 不把日线 high/low 触达描述为订单成交或不成交；F-014 只基于已归档 QE order/trade/position 证据做结果分层，不模拟队列、撤单或 child-order。分钟级 executable MFE/MAE 仍属于 Advisory/执行专项能力。
7. 不接入概念板块；当前只使用申万 L2 PIT `l2_code_id`，未来概念数据沿独立 PIT 数据设计接入。
8. 不使用当前数据覆盖旧实验的原始特征快照；结果观察快照只允许是原快照本身或经过重叠区间一致性验证的严格扩展版本。
9. 不把旧 Type A 超跌反弹包作为 Type B 演进母体；旧包只可作为组合相关性或风险基线。
10. 不修改或复用 Selection、Advisory、Paper v2、模拟盘、QMT、StrategyPackage 的服务、路由、表、缓存、调度器、环境变量或 artifact namespace。
11. 不给共享 `qe_archive.run_metric/run_curve/run_artifact/raw_payload` 增加字段或改变 writer；不扩展通用 `/prediction-store` 上传协议。
12. 不在 FastAPI/Windows 后端进程中读取 H5 或执行指标计算；计算只能在 QE workspace 的 WSL/远端 QE compute node 内运行。

## 4. Current Code Baseline and Gaps / 现行代码与差距

| 现行能力 | 当前代码入口 | 复用方式 | 必须补齐的差距 |
|---|---|---|---|
| 训练标签期限 | `experiment_config.py`、`qe_custom_loaders.py`、`config_composer.py` | 沿用 20–180D 标签语义 | 训练标签不等于长期评价 |
| QE 数据集隔离 | `qe_dataset_contract.py`、`config_composer.py` | 复用 `QE_DATASET_CONTRACT_ID` 和 QE-only PIT | 当前 Archive 的 `dataset_snapshot_id` 字段未形成强制生产链 |
| 日线数据 | `daily_pv.h5`、`BacktestBaseDataMemoryCache` | Phase 1 已新增仅加载 `daily_pv/sector_data`、内容 hash 与 QE workspace 文件绑定的严格 reader | Phase 2 仍需接入 compute-node snapshot resolver；不得加载全部因子文件 |
| 结果提取 | `templates/read_exp_res.py` | 将长期评价 receipt 接入 `enhanced_metrics` | 现有 Top-K 只消费单一 `label.pkl`，无法给出多期限路径 |
| 结果补算 | `results_only_retry.py` | 增加 `long_trend_only` 模式 | 当前只补 IC/RankIC/portfolio summary，未读取结果观察快照 |
| 预测制品 | `model_store/artifact_store.py`、`routers/prediction_store.py` | 只参考 CAS、原子 manifest 和 SHA-256 模式 | 通用接口保持不变；新增 QE-only evaluation store/route |
| 数仓 | `qe_archive.run` 和 reproducibility manifest | 只复用 `run_id` 作为只读父身份 | 新增独立 evaluation/metric/artifact 表，不修改现有通用表 |
| 资源阶段 | `qe_resource_phase_service.py` | 增加 CPU 评价阶段和现有认证/sequence/outbox 语义 | 当前状态机只有 bootstrap/train/predict/backtest/finalize |
| Loop UI | `LoopDetailPanel.tsx`、`loopDiagnostics.ts` | 增加长期趋势页签和进度 | 当前没有长期成熟度、barrier、capture 展示 |
| Archive UI/API | `qe-archive/api.ts`、`qe_archive.py` | 增加有界对比查询 | 当前 Top-K API 无 horizon/evaluation vintage 维度 |

设计不得直接扩展 `qe_eval_v2_metric_engine.py`：该文件服务因子独立指标计算，其输入、缓存和生命周期与实验预测评价不同。长期评价属于实验结果层。

## 5. Design Acceptance Index / 设计验收索引

| ID | 验收项 |
|---|---|
| F-001 | 长期评价只挂接 QE Loop/Recorder/QE run identity，不建设平行实验系统，也不接入任何非 QE 运行链。 |
| F-002 | 评价配置使用版本化 profile，首版记录 20/40/60/120/180D、30%/50%/70% 和全期/126D/252D 切片；新口径发布新版本并与旧结果并存，支持追踪口径变化而不以 profile 作为研究许可。 |
| F-003 | 信号收益严格使用 `close[T+h+1]/close[T+1]-1`，与训练标签逐点一致；entry、terminal 和交易日历边界有直接 oracle。 |
| F-004 | close-path 与 qfq high/low path 分开；日线 high/low 只标记 path diagnostic，不冒充 executable outcome。 |
| F-005 | 固定期限未成熟样本和开放持仓使用右删失；position 首个快照已持仓时使用左删失；均不记为失败、0 收益、未命中或伪造入场。 |
| F-006 | 特征快照和结果观察快照双身份固化；同版本或重叠价格一致的 extension-only 版本提供可复算的信号路径。full-overlap receipt、内容 hash、lineage、日期窗和 QE workspace 文件绑定必须一致；快照缺失或冲突时仅相关指标族标记 `NOT_COMPUTABLE`/`COMPUTED_WITH_LIMITATIONS` 并生成数据补齐计划。 |
| F-007 | 评价只消费 QE PIT 预测池和信号日 `l2_code_id`；不读取 live Selection/Paper PIT，不做行业中性化。 |
| F-008 | 信号层输出多期限 return/RankIC、Top-K precision/recall、barrier、ordered stage、删失调整 stage survival、time-to-hit、MFE/MAE、成熟度和统计置信区间。 |
| F-009 | episode 层从 position/trade artifact 重建真实持仓周期，以 position 自身 as-of 描述持仓边界、以 outcome as-of 描述扩展价格路径；输出左/右删失、capture ratio、extended capture、post-exit opportunity 和 false early-exit。 |
| F-010 | 最近 126/252 个信号交易日和全期为冻结切片；所有申万 L2 分板块输出明细制品，页面不靠硬编码“科技”日期或名称。 |
| F-011 | 重叠 horizon 使用按信号日聚类的 block bootstrap/HAC；多 horizon/barrier 同时报 raw p 与 BH-FDR q，不用逐行独立样本显著性。 |
| F-012 | 百万级逐信号和逐 episode 明细写 QE 专属 CAS Parquet；PostgreSQL 只保存 QE 评价身份、状态、标量和制品指针。 |
| F-013 | 新增 `qe_archive.run_evaluation/run_evaluation_metric/run_evaluation_artifact`；不 ALTER 或改写既有 run_metric/run_curve/run_artifact/raw_payload/Paper 表。 |
| F-014 | 自动评价和 `long_trend_only` 历史补算使用同一 evaluator、profile、数据身份、制品 schema 和入仓 writer。 |
| F-015 | 评价任务使用 QE 专属持久化状态、fencing、认证 sequence 和原子制品发布；不注册全局 scheduler，后端重启不终止 worker、不重复运行、不重复归档。 |
| F-016 | 评价任务或任一指标族异常不回写训练/回测成功为失败；Loop、Archive 和 MCP 必须显示 task/family 状态、reason code、已完成结果和数据补齐计划，不存在全局研究门禁。 |
| F-017 | Loop UI 展示 horizon、barrier、成熟度、板块、episode capture 和错误；Archive UI 支持跨 run/seed/model 对比。 |
| F-018 | QE MCP 只提供有界只读查询；不新增通过 MCP 写生产 DB、启动交易或自动晋级的路径。 |
| F-019 | 默认对既有任务关闭；仅 QE task/Loop 的显式 profile 生效，无 startup side effect，不改变既有模型输出和回测结果。 |
| F-020 | DESIGN-COMPLIANCE-001、真实制品 E2E、DEV DB E2E、UI E2E、重启恢复和 extension snapshot oracle 作为平台交付质量记录；未完成项必须可见，但不限制已计算科研结果的分析和后续研究。 |
| F-021 | 代码所有权和运行依赖执行 QE-only allowlist；静态 import/route/schema 回归证明 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 和通用 Prediction Store 零变化。 |
| F-022 | 理论机会与实际可成交结果分层：Qlib `indicators_normal_{freq}_obj.pkl` 的 `amount/inner_amount/deal_amount/ffr` 与 reconciled trade/position 提供外层目标、内层执行目标和成交证据；entry/exit 使用同一 authority、逐信号 trade 一对一归属及真实数量/时点矛盾 fail-fast；Qlib 分层执行下 `deal_amount>amount`、`ffr>1` 是可记录的 overfill，不得误判为损坏，只有 `deal_amount>inner_amount`、`ffr` 与 `deal_amount/amount` 不一致等才属于数量冲突；日线触板不直接推断原因；缺队列或原因码时仅 `execution_cause` 为 `NOT_VERIFIABLE`。 |
| F-023 | 六个指标族独立计算和定级；缺 position/order/trade/maturity/价格/板块数据只影响依赖它的指标，不传播为全局失败，并输出结构化数据获取、补归档或补算计划。 |
| F-024 | CAS、DDL、API、MCP、UI、历史补算和 E2E 只形成 `platform_delivery_status`；不得解锁或阻断期限选择、R8B2、R8M、R8C、oracle、两层模型或任何研究方向。 |

## 6. Architecture / 架构

### 6.1 总体数据流

```text
QE Loop train/predict/backtest completed
  -> immutable pred.pkl / label.pkl / params.pkl
  -> resolve and hash all available QE position / report / indicator / trade / optional order artifacts
  -> resolve feature dataset identity from Loop execution manifest
  -> resolve outcome dataset: same snapshot or verified extension snapshot
  -> LongTrendEvaluationEngine on CPU compute node
       -> independently compute signal_path / position_episode / portfolio_result
       -> independently compute order_fill / execution_cause / sector_regime
       -> unavailable family -> NOT_COMPUTABLE or NOT_VERIFIABLE + data action plan
       -> available families continue and cross-check one another
       -> signal observation / holding episode / execution evidence Parquet
       -> compact receipt JSON
  -> QELongTrendArtifactStore QE-only CAS atomic publish
  -> qlib_results_enhanced.json.long_trend_diagnostics
  -> qe_archive.run_evaluation + run_evaluation_metric + run_evaluation_artifact
  -> Loop detail / QE Archive / read-only MCP
```

评价执行在 CPU 后处理阶段，不占 GPU phase lease。训练产生 `pred.pkl` 并真实释放 GPU 后，下一 Loop 的 GPU 训练可与上一 Loop 的 backtest/long-trend evaluation 重叠；CPU/内存/磁盘 I/O 仍受节点级 `cpu_postprocess` 单槽限制。

### 6.2 模块边界

| 模块 | 职责 |
|---|---|
| `long_trend_evaluation_contract.py` | profile、schema version、reason code、身份 hash 和 DTO |
| `long_trend_evaluation.py` | 纯计算引擎；无 DB、HTTP、LLM 和生产路径依赖 |
| `templates/qe_long_trend_eval.py` | 计算节点 wrapper；加载本地 Recorder/数据、写临时制品、上报阶段 |
| `config_composer.py` | 将 profile、dataset identity 和 evaluator 源码 hash 写入 workspace/execution manifest |
| `results_only_retry.py` | `long_trend_only` 历史评价入口；不训练、不回测 |
| `long_trend_artifact_store.py` | QE 专属 artifact namespace、manifest、SHA-256 和原子发布；不调用通用 Prediction Store writer |
| `long_trend_evaluation_repository.py` | 只写三张 evaluation 表；只读 `qe_archive.run` 父身份 |
| `qe_resource_phase_service.py` | `long_trend_eval` 阶段、资源、sequence、重启恢复 |
| API/MCP/UI | 创建/查询评价任务，展示同一权威结果 |

### 6.3 与运行中实验隔离

1. 本能力不修改已运行 R6 workspace、命令或进程。
2. 自动评价只对创建时显式带 profile 的新 Loop 生效。
3. 历史 R6 使用独立 `long_trend_only` 请求；只读预测和数据，生成新 evaluation identity。
4. 评价任务失败不触发 Loop 训练重试，不修改模型、预测、回测或 MLflow Recorder。
5. 任何代码发布、生产 DDL、后端重启和评价任务启动分别执行并分别汇报。

### 6.4 QE-only 硬隔离

运行时允许的调用链固定为：

```text
QE task/Loop or QE long_trend_only API
  -> backend.services.quantevolver.long_trend_*
  -> QE workspace worker on WSL/remote QE node
  -> QE_DATASET_CONTRACT_ID snapshot
  -> qe_archive.run_evaluation* + QE-only artifact namespace
  -> /quantevolver or /qe-archive read UI/MCP
```

允许修改的代码 ownership：

- `backend/services/quantevolver/long_trend_*` 和必要的 QE composer/result/resource glue；
- QE 专属 router 或现有 `quantevolver_evolution.py` 的 QE 路由；
- additive `qe_archive.run_evaluation*` migration/init 定义；
- QE Archive read-only MCP 的新查询工具；
- `/quantevolver/**` 与 `/qe-archive/**` 页面。

禁止 import、调用或写入：

- `backend/services/selection_center/**`、`backend/services/advisory*`；
- `backend/services/paper_trading/**`、Paper v2 表和页面；
- `backend/infra/qmt_client.py`、QMT/MiniQMT/实盘或模拟盘路由；
- `backend/services/strategy_package/**` 的运行、promotion 或资产 writer；
- live Selection/Paper PIT universe、持仓、订单、现金、荐股列表和运行缓存；
- 通用 `PredictionArtifactStore.write_artifacts()`、`/prediction-store` 上传协议和已有 manifest；
- 全局 startup scheduler、定时任务目录和非 QE 环境变量。

全部新环境变量使用 `QE_LONG_TREND_` 前缀；全部计算 worker 必须同时具有 QE task/Loop identity、`QE_DATASET_CONTRACT_ID` 和 QE workspace root。任一身份缺失以 `QELT_NON_QE_SOURCE_REJECTED` 拒绝。FastAPI 只做派发、callback、持久化和查询，不加载 H5、不创建进程内全量价格缓存。DDL 只 additive 新表/索引，不 ALTER 现有 Paper/Selection/Advisory/通用 Archive 表。

### 6.5 指标族独立执行契约

| 指标族 | 主要输入 | 可形成的结论 | 输入不足时的局部状态 |
|---|---|---|---|
| `signal_path` | prediction、交易日历、qfq close/high/low | 多期限收益、RankIC、barrier、time-to-hit、MFE/MAE、右删失 | 缺预测或所需价格路径为 `NOT_COMPUTABLE`；部分 horizon 未成熟时已成熟 horizon 继续，未成熟 horizon 单独记录 |
| `position_episode` | position snapshot、价格路径；trade 作交叉验证 | capture ratio、extended capture、post-exit opportunity、false early-exit | 缺 position 为 `NOT_COMPUTABLE`；trade 缺失只降低成交价/费用结论，不影响可由 position 重建的 episode |
| `portfolio_result` | Qlib portfolio report/indicator summary | 组合收益、成本后收益、风险和换手 | 缺 report 为 `NOT_COMPUTABLE`；不影响信号或持仓路径 |
| `order_fill` | `indicators_normal_{freq}_obj.pkl` 的 `amount/deal_amount/ffr`，以及可得 trade/position | 尝试量、成交量、填充率、零成交/部分成交/延迟成交 | 缺 indicator object 时为 `NOT_COMPUTABLE` 或由其他权威制品形成 `COMPUTED_WITH_LIMITATIONS` |
| `execution_cause` | 明确订单原因码、队列/撤单轨迹、可复核执行规则；日线市场状态只辅助 | 未成交或延迟是否由涨跌停、停牌、队列等造成 | 无直接原因证据时为 `NOT_VERIFIABLE`；不得把该状态传播给 `order_fill` 或其他指标族 |
| `sector_regime` | 信号日 PIT `l2_code_id`、可用的信号/持仓/组合指标 | 板块分解、集中度、板块内外归因、regime 切片 | 缺 PIT 板块数据为 `NOT_COMPUTABLE`；其他五族继续 |

每族状态都必须附带 `available_inputs`、`missing_inputs`、`coverage`、`limitations`、`supporting_artifacts`、`reason_codes` 和 `data_actions`。`data_actions` 至少给出数据源候选、需要补归档/补采集的字段与时间范围、可否对历史 run 补算、预期能够恢复的指标，不创建全局 `research_ready` 或 decision gate。

## 7. Contracts / 契约

### 7.1 不可变评价 profile

首版 profile：`qe_long_trend_v1`。

```json
{
  "profile_id": "qe_long_trend_v1",
  "schema_version": "qe_long_trend_eval_v1",
  "horizons": [20, 40, 60, 120, 180],
  "barriers": [0.30, 0.50, 0.70],
  "calendar_slices": ["all_oos", "last_252_signal_days", "last_126_signal_days"],
  "k_policy": {"fixed": [20, 50], "include_strategy_topk_up_to": 50},
  "entry_rule": "signal_T_entry_T_plus_1_close_qfq",
  "terminal_rule": "T_plus_h_plus_1_close_qfq",
  "barrier_primary_projection": "future_close_qfq",
  "path_projection": "future_high_low_qfq_diagnostic",
  "execution_bridge": "qe_archived_order_trade_position_reconciled_v1",
  "execution_authority_order": ["qlib_indicator_object_amount_deal_amount_ffr", "reconciled_order_and_trade", "reconciled_trade", "position_transition", "daily_market_state_diagnostic"],
  "unknown_execution_policy": "explicit_not_verifiable",
  "sector_projection": "signal_date_sw_l2_l2_code_id",
  "missing_input_policy": "family_local_status_and_data_action_plan",
  "coverage_reporting_reference": {"entry": 0.98, "path": 0.98, "sector": 0.98}
}
```

API 使用注册过的 `profile_id` 保证结果口径可追踪，不接受调用方无版本覆盖 horizons、barriers、切片或参考覆盖率。新研究口径发布新 profile/version，旧结果不覆盖；profile 是可复算元数据，不是科研门禁。

`qe_long_trend_v1` 首版实现日频公式，但任何具有合法 QE identity 的 run 都可以创建可用性分析：只有 prediction 的腿可计算 `signal_path`；具有 position 的 run 可增加 `position_episode`；具有 report/indicator object 的 run 可增加 `portfolio_result/order_fill`；具有信号日板块 PIT 的 run 可增加 `sector_regime`。非日频或缺少某类制品时，对应族记录 `NOT_COMPUTABLE` 和适配/补归档计划，不拒绝研究任务。多 Alpha/融合结果可先分析 combined prediction；完成 combine-backtest 后再补算持仓、组合与执行相关指标。

### 7.2 双数据快照身份

每次评价同时保存：

- `feature_dataset_snapshot_id`：产生因子、标签、预测和回测的原始 QE 数据集；
- `feature_dataset_manifest_sha256`：原始 `meta.json`、关键文件 hash 和 QE dataset contract 的组合 hash；
- `outcome_dataset_snapshot_id`：提供已实现未来路径的观察数据集；
- `outcome_dataset_manifest_sha256`：结果数据集组合 hash；
- `overlap_price_parity_sha256`：两个快照在原 feature snapshot 截止日之前 qfq OHLC 重叠区间的确定性校验 receipt；
- `evaluation_asof`：outcome snapshot 的最后交易日。

可复算的信号路径条件：

```text
outcome == feature
OR
outcome.start <= feature.start
AND outcome.end > feature.end
AND overlap qfq OHLC + calendar + instrument PIT semantics are identical
AND outcome declares feature as lineage parent/ancestor
```

若只是路径名称相同、当前默认常量相同或日期更晚，但重叠价格/hash 不一致，不得把它冒充原实验的 extension，也不得回退到当前生产数据。`signal_path` 及依赖该路径的 `sector_regime/position_episode` 指标按实际可用证据标记 `NOT_COMPUTABLE` 或 `COMPUTED_WITH_LIMITATIONS`，其他从 Recorder 自身可读取的 `portfolio_result/order_fill` 继续；receipt 同时输出定位原快照、重导 extension 或补归档价格路径的数据行动计划。

### 7.3 信号日、entry 和 horizon

设预测信号日为 `T`，全市场交易日历的下一个交易日为 `S=T+1`：

```text
entry_price = close_qfq[S]
terminal_date(h) = calendar[S_index + h]
return_h = close_qfq[terminal_date(h)] / entry_price - 1
```

`return_h` 与 Qlib `label_h` 逐点对比，目标浮点误差为 `1e-6`。若已有同 horizon `label.pkl`，同时保存抽样和全量 parity；不一致时保留双方数值、差异分布和公式身份，将受影响的 `signal_path` 指标标为 `COMPUTED_WITH_LIMITATIONS`，不得静默选边，也不得使整次评价或其他指标族失败。

上述 entry 是“若能在 S 日收盘建立仓位”的理论机会锚点，不表示策略实际成交。实际桥接优先解析 Qlib Recorder 的 `indicators_normal_{freq}_obj.pkl`：`amount` 是外层策略目标量，`inner_amount` 是内层执行策略累计目标量，`deal_amount` 是实际撮合量，`ffr=deal_amount/amount` 是相对外层目标的填充率。分层执行、合法整手处理或执行策略再分配可能使 `inner_amount>=deal_amount>amount`，此时 `ffr>1` 是可解释的 overfill，而不是数据损坏。评价器必须同时保存 target/inner-target/deal/fill-ratio/overfill，并与可得的 trade/position artifact 交叉核对，再按以下稳定状态输出：

| `entry_execution_status` | 判定 |
|---|---|
| `filled_t1` | S 日存在经 reconciliation 的首次买入成交；`deal_amount>=amount` 时为全额成交，若大于外层目标则另存 `overfill_amount` 和 `fill_ratio>1` |
| `partial_fill_t1` | S 日 `amount>deal_amount>0` 或 `0<ffr<1`，保存未成交量与填充率 |
| `delayed_fill` | S 日后才首次成交，并保存 `entry_delay_days` |
| `never_filled` | indicator object 或真实订单制品显示 `amount>0`，但评价窗口内 `deal_amount=0` 且无成交 |
| `not_attempted_by_strategy` | 权威订单/组合决策证明未尝试建仓，不归因于市场阻断 |
| `not_verifiable` | 缺下单意图、订单队列或足够 reconciliation 证据，禁止猜测 |

`amount/inner_amount/deal_amount/ffr` 足以支持 `order_fill` 的尝试、成交、部分成交、零成交和 overfill 统计，但通常不包含队列位置、撤单轨迹和稳定原因码。`entry_block_reason` 只在直接证据足够时取 `blocked_limit_up`、`blocked_suspension` 或其他注册原因；否则只将 `execution_cause` 标为 `NOT_VERIFIABLE`。日线涨停/停牌状态只能辅助解释，不能单独把无成交归因成阻断；派生的 stock-level trade 汇总可用于收益归因，但不能伪装为精确 child-order ledger。原因不可验证不影响 `order_fill`、信号路径、持仓或组合结果的分析。

因为 entry 使用 `S` 日收盘价，MFE/MAE 和 barrier 的未来路径从 `S+1` 开始，不能使用 `S` 日已经发生的 high/low：

```text
close_mfe_h = max(0, max(close_qfq[S+1:S+h] / entry_price - 1))
close_mae_h = min(0, min(close_qfq[S+1:S+h] / entry_price - 1))
path_mfe_h  = max(0, max(high_qfq [S+1:S+h] / entry_price - 1))
path_mae_h  = min(0, min(low_qfq  [S+1:S+h] / entry_price - 1))
```

主 barrier 使用收盘路径；high-path 触达只作为 diagnostic：

```text
close_hit_b = first u in [S+1, S+h] where close_qfq[u] / entry_price - 1 >= b
time_to_close_hit_b = trading_step(u) - trading_step(S)
```

逐信号 Parquet 只保存 180D 内首次触达步数；任意 horizon 的 hit 可由 `time_to_close_hit_b <= h` 确定，避免为五个 horizon 重复存储同一事件时间。

### 7.4 成熟、删失和数据缺口

每个 `(signal_date, instrument, horizon)` 只能处于：

- `matured`：entry、terminal 和要求的路径均可观测；
- `right_censored`：outcome snapshot 在 terminal 前结束；
- `open_event_censored`：持仓 episode 在评价 as-of 仍开放；
- `invalid_entry`：entry close 缺失或非正；
- `path_incomplete`：terminal 在快照内，但股票中间路径缺失超过 profile 阈值；
- `instrument_exit_unresolved`：退市/数据终止无法按权威语义解析。

只有 `matured` 进入固定 horizon 均值、RankIC 和 barrier miss 分母。删失样本进入成熟度、生存和 coverage 统计，但不得写 0、false 或亏损。任何 forward-fill 必须被拒绝。

### 7.5 信号层指标

对每个 horizon 和 slice 固化：

1. 全截面每日 RankIC 及其均值、标准差、ICIR、正向率、日期数；
2. Top-K `mean/median/p10/p50/p90 return`、正收益率、MFE/MAE；
3. `precision_at_k(barrier)`：Top-K 中最终命中 barrier 的比例；
4. `recall_at_k(barrier)`：当日全部可评价 barrier winner 中被 Top-K 召回的比例；
5. `time_to_hit` 的 p25/p50/p75 和命中样本数；
6. `highest_stage`：NONE/HIT30/HIT50/HIT70，概率必须满足 `P70 <= P50 <= P30`；
7. `mature_count/censored_count/invalid_count/coverage_ratio`；
8. 预测分数对 barrier event 的 AUCPR；只有模型制品明确声明概率语义时才计算 Brier，普通 score 不得伪装成概率。

排名固定为 `score DESC, instrument ASC`，相同 score 使用 instrument 稳定打破平局。K 集合为 20、50 以及不超过 50 的实际 strategy topk；不得由 test 结果临时选择 K。

### 7.6 持仓 episode 与趋势捕获

从 `positions_normal_1day.pkl` 为主、trade artifact 为交叉校验重建 episode：

```text
episode entry: instrument amount 由 0 变为 >0
episode exit:  instrument amount 由 >0 变为 0
open episode:  evaluation_asof 仍 >0
```

position artifact 的连续性只校验其自身首个快照至 `position_observation_end_date`，不得要求历史策略持仓快照延伸到更晚的 outcome snapshot。outcome 的更晚日期只用于成熟信号和 extended/post-exit 价格路径。若首个 position 快照已经 `amount>0`，该段必须标记 `left_censored=true`：`entry_date` 只表示最早观察边界，不得计算 entry return/capture，也不得与后续信号伪匹配成真实入场；同时生成补取更早 position history 的 data action。

每个 closed episode 保存：

```text
episode_close_return_qfq = exit_close_qfq / entry_close_qfq - 1
episode_mfe          = max qfq high from first post-entry day through exit / entry - 1
episode_capture_ratio = episode_close_return_qfq / max(episode_mfe, epsilon)
extended_mfe_180     = max qfq high from first post-entry day through min(entry+180, asof) / entry - 1
extended_capture_ratio = episode_close_return_qfq / max(extended_mfe_180, epsilon)
post_exit_mfe        = max qfq high after exit through entry+180 / exit_close_qfq - 1
```

episode 同时保存 `exit_execution_status`、`exit_delay_days` 和 `exit_block_reason`。`exit_execution_status` 至少区分 `filled_on_exit_signal_day`、`delayed_exit`、`never_exited` 与 `not_verifiable`；`exit_block_reason` 只在证据足够时取 `blocked_limit_down`、`blocked_suspension` 或其他注册原因。评价量化趋势反转或退出信号后无法减仓/清仓造成的额外 MAE、回撤和持仓天数。买入涨停与卖出跌停/停牌使用同一证据等级，不能只审计买入侧。

该 return 只评价退出时点对 qfq 收盘路径的捕获，不冒充真实成交收益。原始 ratio 不裁剪；UI 可另给可视化范围，但数仓保存原值和异常标记。只有 trade artifact 明确提供并通过价格基准、数量和费用 reconciliation 时，才另存 `execution_gross_return/execution_net_return`；缺费用时不得以 close return 或 gross 冒充 net。组合成本后收益继续以 Qlib portfolio report 为权威。reconciled trade/order artifact 对实际成交日期、价格、数量和费用具有最高权威；position transition 可作缺省持仓边界证据，日线市场状态只作 diagnostic。

历史结果的 episode source resolver 固定为：原始 Recorder position/trade artifact → 经完整性校验的 QE Archive position/trade 行 → QE-only CAS 中由 QE run 产生的 position/trade artifact。三者都不可用时，`position_episode=NOT_COMPUTABLE`，不从 Top-K 列表猜持仓，也不向通用 Prediction Store 增加 artifact；其他指标族继续。R6 创建评价时先输出各族输入可用性预览，缺失来源进入补归档/补算计划，不作为创建或分析阻断。

`false_early_exit=true` 的冻结定义：episode 在 180D 内已经退出，退出前最高 close stage 低于最终 180D 最高 close stage，且最终至少达到 HIT30。开放 episode、强制退市/数据缺口 episode 和未成熟 episode不进入该比例分母。

### 7.7 板块与 regime 切片

1. 板块身份只使用信号日 `sector_data.h5.l2_code_id`；不能使用当前行业回填历史。导出约定的 `-1` unknown sentinel 在 reader 中规范化为 null，不得作为真实板块参与集中度、切换率或逐板块收益。
2. 保存每个 L2 的样本数、return、barrier precision/recall、MFE/MAE 和 time-to-hit。
3. 保存 Top-K 的 `top1_sector_share`、sector HHI、有效板块数和换板块频率。
4. “科技抱团”不在代码中硬编码日期或中文名称；逐 L2 明细和集中度曲线进入 Parquet，UI 按当前稳定 code/name mapping 过滤。
5. 最近 252/126 个信号交易日从本次评价的 `evaluation_asof` 向前取交易日，不使用自然日。
6. slice 结果只作稳定性诊断，不允许反向修改全期 profile 或公式。

### 7.8 重叠标签与统计推断

1. 每日先做截面聚合，再以信号日为统计单位；不得把数百万股票行当独立样本。
2. 均值差使用按信号日 moving-block bootstrap，block length 固定为对应 horizon；RankIC 同时报 Newey-West/HAC，lag=`h-1`。
3. 5 个 horizon × 3 个 barrier 的检验族同时输出 raw p-value 与 Benjamini-Hochberg q-value。
4. primary 结论使用全 OOS；126/252D 和单板块切片不单独决定晋级。
5. 每个指标必须同时返回有效日期数、有效样本数、删失数和置信区间，样本不足时状态为 `insufficient_maturity`，不是 0 或失败。

## 8. Artifact and Persistence Design / 制品与持久化

### 8.1 逐信号 Parquet

`long_trend_signal_observations.parquet` 一行对应一个 `(signal_date, instrument)`，采用 wide schema 避免按 horizon 复制身份列：

```text
signal_date, instrument, score, stable_rank, l2_code_id,
entry_date, entry_close_qfq, entry_volume_qfq,
entry_suspension_diagnostic, entry_limit_state_diagnostic, entry_instrument_event,
signal_calendar_position, evaluation_calendar_position,
entry_execution_status, entry_execution_evidence_level,
actual_entry_date, actual_entry_price, entry_delay_days, entry_block_reason,
missed_mfe_due_to_entry_block, missed_barrier_winner_due_to_entry_block,
return_20/40/60/120/180,
close_mfe_20/.../180, close_mae_20/.../180,
path_mfe_20/.../180, path_mae_20/.../180,
maturity_20/.../180, observed_steps_20/.../180,
observed_prefix_steps_20/.../180, observed_high_low_steps_20/.../180,
path_quality_20/.../180,
close_hit_30/50/70, time_to_close_hit_30/50/70,
high_path_hit_30/50/70,
highest_close_stage_180,
row_quality_flags
```

### 8.2 逐 episode Parquet

`long_trend_holding_episodes.parquet` 一行对应一个持仓 episode：

```text
instrument, episode_seq, entry_date, exit_date, left_censored, open_censored,
position_observation_end_date, episode_maturity_state,
entry_close_qfq, exit_close_qfq,
entry_execution_status, entry_execution_evidence_level,
actual_entry_date, actual_entry_price, entry_delay_days, entry_block_reason,
exit_signal_date, actual_exit_date, actual_exit_price,
exit_execution_status, exit_execution_evidence_level, exit_delay_days, exit_block_reason,
post_exit_signal_mae, blocked_exit_extra_drawdown, blocked_exit_extra_holding_days,
episode_close_return_qfq,
execution_gross_return, execution_net_return,
episode_mfe, episode_mae, episode_capture_ratio,
extended_mfe_180, extended_capture_ratio, post_exit_mfe,
highest_stage_at_exit, highest_stage_180, false_early_exit,
cost_quality, episode_quality_flags
```

### 8.3 Compact receipt

`long_trend_evaluation_receipt.json` 包含：

- evaluation identity、profile/version、evaluator source SHA；
- prediction/label/position/report/`indicators_normal_{freq}_obj.pkl`/trade/order artifact SHA（不存在时以显式 null 进入 manifest）；
- feature/outcome dataset identity 和 overlap parity receipt；
- 六个指标族的状态、汇总指标、成熟度、coverage、limitations、resource statistics 和 reason code；
- execution evidence coverage、`not_verifiable` 比例、入场/退出阻断分层及其 MFE/barrier/回撤损失；
- 每个不可计算/不可验证族的 `data_actions`，以及可用指标之间的交叉印证或冲突摘要；
- 两个 Parquet 的 URI、SHA-256、行数、列 schema hash；
- `no_training/no_backtest/no_live_data_access` 三项真值。

`read_exp_res.py` 只校验并挂载 receipt 的 compact summary，不在该大文件中重复计算长期指标。

### 8.4 QE-only CAS namespace

新增 `backend/services/quantevolver/long_trend_artifact_store.py`，只服务 QE 长期趋势评价，并使用独立根目录 `QE_LONG_TREND_ARTIFACT_STORE_ROOT`、run key `qelt_<evaluation_id>` 和 manifest schema `qe_long_trend_artifact_manifest_v1`。允许复用安全 path component、SHA-256、临时文件加原子 rename、manifest compare-and-swap 等算法模式，但不得调用或修改通用 `PredictionArtifactStore` writer、`/prediction-store` 上传协议、既有 prediction manifest 或其 artifact 映射。

QE-only allowlist：

- `portfolio_positions` → 原 Recorder 中的 `positions_normal_1day.pkl`，存在时用于 `position_episode`；
- `portfolio_report` → 原 Recorder 中的 `report_normal_{freq}.pkl`，存在时用于 `portfolio_result`；
- `portfolio_indicator_object` → 原 Recorder 中的 `indicators_normal_{freq}_obj.pkl`，以 `amount/deal_amount/ffr` 支持 `order_fill`；
- `portfolio_trades` → 原 Recorder 中可得的 trade artifact，存在时用于费用和交易 reconciliation；
- `portfolio_orders` → QE run 已真实归档的订单/意图 artifact（可选）；不存在时不得伪造，相关非成交原因进入 `not_verifiable`；
- `long_trend_signal_observations` → Parquet；
- `long_trend_holding_episodes` → Parquet；
- `long_trend_evaluation_receipt` → JSON。

`pred.pkl/label.pkl` 仍按既有只读 pointer/download 契约读取，不修改其 manifest。新 full-backtest Loop 只在 QE workspace 内复制所有真实存在的 position/report/indicator/trade/order artifact；某制品上传失败只把依赖它的平台制品状态与指标族标为未完成，已计算结果仍保留并可分析。历史 run 不伪造回填：逐一检查原 Recorder、QE Archive rows 和 QE-only CAS，真实可得的输入计算对应指标族，缺失项以显式 null 固化并生成数据行动计划。不得生成替代订单，不得把多 evaluation version 写成同一 prediction run 的同名 artifact，也不得把未知 artifact 默认命名为 `params.pkl`。

### 8.5 DB schema

新增 additive migration：`backend/migrations/qe_long_trend_evaluation_f2_20260714.sql`，以及对应 rollback 文件。

`qe_archive.run_evaluation`：

| 字段 | 类型 | 约束/说明 |
|---|---|---|
| `evaluation_id` | TEXT | PK，`qelt_` + identity hash |
| `run_id` | TEXT | FK `qe_archive.run(run_id)`，ON DELETE CASCADE |
| `evaluation_type` | TEXT | 首版固定 `long_trend` |
| `profile_id` | TEXT | `qe_long_trend_v1` |
| `profile_sha256` | TEXT | 非空 |
| `evaluator_version` | TEXT | 非空 |
| `evaluator_source_sha256` | TEXT | 非空 |
| `qe_dataset_contract_id` | TEXT | 非空；证明来源属于 QE-only，是唯一硬边界身份 |
| `feature_dataset_snapshot_id` | TEXT | 可空；缺失时相关指标族记录数据行动计划 |
| `feature_dataset_manifest_sha256` | TEXT | 可空；显式 null 进入 input identity |
| `outcome_dataset_snapshot_id` | TEXT | 可空；缺失时 `signal_path/sector_regime` 局部受限 |
| `outcome_dataset_manifest_sha256` | TEXT | 可空；显式 null 进入 input identity |
| `input_manifest_sha256` | TEXT | pred/label/position/report/indicator/trade/order 输入及 `label_horizon/strategy_topk` 评价参数的组合 hash，缺失项以显式 null 固化 |
| `artifact_store_run_key` | TEXT | 成功后非空 |
| `artifact_manifest_sha256` | TEXT | 成功后非空 |
| `status` | TEXT | queued/running/succeeded/partial/failed/cancelled；只描述任务生命周期 |
| `family_status_json` | JSONB | 六个指标族各自的 COMPUTED/COMPUTED_WITH_LIMITATIONS/NOT_COMPUTABLE/NOT_VERIFIABLE、coverage 和 limitations |
| `platform_delivery_status_json` | JSONB | core/CAS/DB/API/MCP/UI/backfill/E2E 实施与持久化状态，不表达研究许可 |
| `data_action_plan_json` | JSONB | 缺失数据的获取、补归档、适配和补算计划 |
| `reason_code` | TEXT | 任务级异常时非空；指标族原因保存在 family status 中 |
| `reason_json` | JSONB | 结构化失败上下文，不存秘密 |
| `stats_json` | JSONB | 行数、覆盖、资源、成熟度 |
| `created_at/started_at/completed_at/updated_at` | TIMESTAMPTZ | 生命周期 |

唯一约束：

```text
(run_id, evaluation_type, profile_sha256,
 input_manifest_sha256, evaluator_source_sha256)
```

新增 `qe_archive.run_evaluation_metric`，不 ALTER `qe_archive.run_metric`：

| 字段 | 类型 | 约束/说明 |
|---|---|---|
| `evaluation_metric_id` | BIGSERIAL | PK |
| `evaluation_id` | TEXT | FK `run_evaluation(evaluation_id)`，ON DELETE CASCADE |
| `metric_key/metric_scope` | TEXT | 指标名；scope 为 signal_path/position_episode/portfolio_result/order_fill/execution_cause/sector_regime |
| `period_start/period_end` | DATE | slice 边界，可空 |
| `horizon` | INTEGER | 20/40/60/120/180，可空 |
| `sector_code` | TEXT | 申万 L2，可空 |
| `dimension_key` | TEXT | 非空；由 scope/slice/horizon/sector 规范化编码 |
| `value_num/value_text/value_json` | DOUBLE PRECISION/TEXT/JSONB | 三者按 metric schema 互斥使用 |
| `unit/direction` | TEXT | 单位与越大越好/越小越好语义 |
| `source_payload_path` | TEXT | receipt 中的确定性 JSON path |
| `quality_flag` | TEXT | ok/computed_with_limitations/insufficient_maturity/not_computable/not_verifiable/censored_only |

唯一键为 `(evaluation_id, metric_key, dimension_key)`，避免 nullable 维度破坏幂等；repository 必须重算并校验 `dimension_key`，不能信任客户端输入。按 `evaluation_id/metric_scope/horizon/sector_code` 建查询索引。逐信号和逐 episode 行不进入 PostgreSQL。

新增 `qe_archive.run_evaluation_artifact`，不复用或改写 `qe_archive.run_artifact`：

| 字段 | 类型 | 约束/说明 |
|---|---|---|
| `evaluation_artifact_id` | BIGSERIAL | PK |
| `evaluation_id` | TEXT | FK `run_evaluation(evaluation_id)`，ON DELETE CASCADE |
| `artifact_type/artifact_uri` | TEXT | QE-only allowlist 类型与 URI |
| `sha256/schema_sha256` | TEXT | 内容和列 schema hash |
| `size_bytes/row_count` | BIGINT | 大小与行数，可空 |
| `status` | TEXT | staged/published/failed |
| `metadata` | JSONB | 不存秘密，只存可复核元数据 |
| `created_at` | TIMESTAMPTZ | 创建时间 |

唯一键为 `(evaluation_id, artifact_type, sha256)`。三张新表均由 `long_trend_evaluation_repository.py` 独占写入；repository 只读取 `qe_archive.run` 的父身份，并在创建前验证 `source_system=quantevolver`、允许的 QE `run_type`、task/Loop identity 和 dataset contract。非 QE run 即使存在同名 `run_id` 也必须以 `QELT_NON_QE_SOURCE_REJECTED` 拒绝。

### 8.6 Identity and idempotency

```text
evaluation_id = sha256(
  run_id + profile_sha256 + evaluator_source_sha256
  + canonical(feature_dataset_manifest_sha256 or "<NULL>")
  + canonical(outcome_dataset_manifest_sha256 or "<NULL>")
  + input_manifest_sha256
)
```

所有缺失输入使用类型化显式 null marker 进入 canonical manifest 与 identity，不能省略字段或与空字符串混同；`label_horizon` 和实际 `strategy_topk` 同样进入 identity，禁止同一 `evaluation_id` 因运行参数不同产生不同 metric 集。超出注册 profile 的 horizon/K 产生结构化 `PROFILE_INVALID`，不得被静默忽略；新研究口径通过新 profile/version 表达，不形成方向淘汰或研究许可。后续补到新数据会自然产生新的 evaluation identity，并与旧的部分证据并存。

同 identity 重试返回已有 succeeded 结果；failed identity 可增加 attempt，但不能覆盖已成功 manifest。相同 identity 出现不同 receipt/artifact hash 时以 `QELT_DUPLICATE_IDENTITY_CONFLICT` fail-fast。

## 9. Execution State and Resource Contract / 执行与资源契约

### 9.1 阶段状态机

资源阶段扩展为：

```text
created -> bootstrap -> train -> predict -> gpu_phase_released
  -> backtest -> long_trend_eval -> finalize -> completed

historical long_trend_only:
created -> bootstrap -> long_trend_eval -> finalize -> completed
```

`long_trend_eval` 不占 GPU lease，记录 CPU time、RSS/VmHWM、读取字节、输出行数和 artifact size。评价 profile 关闭时保持现有状态机，不插入伪阶段。

阶段只能由 QE task/Loop completion hook 或 QE `long_trend_only` API 显式创建；不得注册全局 startup/cron scheduler，也不得从 Selection、Advisory、Paper、模拟盘、QMT 或 StrategyPackage 生命周期触发。自动评价仅表示已显式选择 profile 的 QE task 在其回测完成后进入下一 QE phase，不是平台级自动扫描。

### 9.2 并发和内存

1. 每节点 `cpu_postprocess` 默认 1 并发，避免多个任务同时全量读取 fixed-format H5。
2. loader 只允许 `daily_pv.h5` 和 `sector_data.h5`，只保留必需列，禁止调用加载全部八个基础文件的默认路径。
3. `daily_pv.h5` 为 fixed HDF 时允许一次完整读取，但必须按日期/股票裁剪后释放原父对象；sector 数据只保留 `l2_code_id`。
4. 分组计算按 signal-date chunk 执行，完成 chunk 后释放中间矩阵；内存不得随 horizon、因子数或 Loop 数单调增长。
5. 同节点有 GPU 训练时允许 CPU 评价并行，但 CPU/RSS/I/O 超过 profile 资源阈值时排队，不抢占或终止训练。

### 9.3 重启与恢复

1. `run_evaluation` 在派发前写 queued；worker 取得 fencing attempt 后改 running。
2. callback 复用每运行随机 token、任务/Loop/节点绑定和单调 sequence。
3. worker 本地 outbox 保存阶段/terminal receipt；后端恢复后幂等重放。
4. QE 服务恢复路径只 reconciliation 已存在的 `run_evaluation`，不扫描非 QE 表、不调用远端 kill、不重新启动仍存活 worker；普通后端启动不得自动创建新评价。
5. CAS manifest、DB metrics 和 evaluation terminal receipt 三方 hash 一致性写入 `platform_delivery_status`。不一致时保留 worker receipt、已计算的 family results 和差异详情，任务可标记 partial 并重试持久化；不得把平台写入问题解释成研究结果无效。

## 10. API / MCP / UI Contracts

### 10.1 Backend API

新增：

```text
POST /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/long-trend-evaluations
GET  /api/v1/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/long-trend-evaluations
GET  /api/v1/quantevolver/evolution/long-trend-evaluations/{evaluation_id}
GET  /api/v1/qe-archive/analytics/long-trend-quality
```

POST body 只允许：

```json
{
  "profile_id": "qe_long_trend_v1",
  "outcome_dataset_snapshot_id": "<registered snapshot id>"
}
```

不提供 `force` 覆盖。相同 identity 返回已有任务；profile、outcome snapshot 或 evaluator version 改变会自然生成新 identity。

创建 API 只把 QE task/Loop、`qe_archive.run`、QE dataset contract 与 QE workspace identity 作为硬边界；任何非 QuantEvolver/QE 来源返回 `QELT_NON_QE_SOURCE_REJECTED`。其余输入在创建后按指标族解析，不因 position、order、trade、价格成熟度或板块数据缺失拒绝任务。查询必须支持 `run_id/task_id/loop_index/model_type/label_horizon/evaluation_asof/horizon/sector_code/family_status/entry_execution_status/exit_execution_status` 过滤和有界 limit；默认 compact，不返回 Parquet 明细。通用 `/prediction-store` API、Paper/Selection/Advisory API 和 route registration 不变。

### 10.2 QE Archive MCP

增加只读 `qe_archive_query_long_trend_quality`：

- 默认 summary，最大 100 行；
- 显式参数才能返回 per-horizon/per-sector/per-execution-status；
- 只返回 artifact URI/hash，不内联明细 Parquet；
- 不提供创建评价、运行实验、DB DML、promotion 或交易工具。

### 10.3 Loop UI

Loop 详情增加“长期趋势”页签：

1. task status、六个 family status、platform delivery status、profile、feature/outcome snapshot、as-of 和 maturity；
2. 20–180D return/RankIC/MFE/MAE 表；
3. 30%/50%/70% precision、recall、AUCPR、time-to-hit；
4. episode capture、extended capture、false early-exit；
5. `filled_t1/delayed_fill/never_filled/not_attempted_by_strategy/not_verifiable` 入场分层、阻断损失和 `entry_delay_days`；
6. 退出延迟、跌停/停牌阻断、额外回撤和 `not_verifiable` 分层；
7. 板块 HHI、top1 sector share 和 L2 表；
8. reason code、maturity/execution coverage 和删失说明；
9. 已完成 Loop 的单一“生成/更新长期趋势评价”入口；同 identity 幂等，不设置含义重叠的强制按钮。

任务创建页只提供 profile 开关和 profile 说明，不允许编辑冻结参数。页面刷新从 DB 状态恢复进度，不依赖浏览器内存。

前端改动只允许落在 `/quantevolver/**` 与 `/qe-archive/**` 页面、其专属 API client 和测试中；不得改动 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 页面或其共享业务状态。若需要通用展示组件，只可无副作用复用，不得向组件注入长期评价的全局 context/provider。

### 10.4 QE Archive UI

增加跨 run 对比表，主键显示 run、model、seed、factor set、training label horizon、evaluation as-of；支持 horizon 和 sector 过滤。不同 outcome vintage 默认不混排，用户显式选择同一 as-of 或同一 snapshot 后才能横向比较。

## 11. Failure Modes and Reason Codes / 失败模式

| reason_code | 条件 | 行为 |
|---|---|---|
| `QELT_NON_QE_SOURCE_REJECTED` | 来源不是允许的 QE task/Loop/run，或缺 QE dataset/workspace identity | 创建前拒绝；不触碰非 QE 状态 |
| `QELT_PROFILE_INVALID` | profile 未注册或 hash 不一致 | 请求格式错误；返回可用版本，不对研究方向作结论 |
| `QELT_PROFILE_UNSUPPORTED_RUN_MODE` | 当前 evaluator 尚未实现该频率/模式 | 受影响族 `NOT_COMPUTABLE` + 适配计划；已支持族继续 |
| `QELT_PREDICTION_ARTIFACT_MISSING` | pred 缺失 | `signal_path=NOT_COMPUTABLE`；其他族继续，生成补归档计划 |
| `QELT_PREDICTION_SCHEMA_INVALID` | index/score 非法或重复键冲突 | `signal_path=COMPUTED_WITH_LIMITATIONS` 或 `NOT_COMPUTABLE`；保留冲突明细 |
| `QELT_FEATURE_DATASET_IDENTITY_MISSING` | 无法从原 Loop 证明 feature snapshot | 依赖快照的族局部不可计算；不猜当前默认，生成定位计划 |
| `QELT_OUTCOME_SNAPSHOT_NOT_EXTENSION` | 结果快照重叠区间不一致 | 依赖结果路径的族局部不可计算；保留差异并生成重导计划 |
| `QELT_DAILY_PV_SCHEMA_INVALID` | 缺 qfq OHLC、索引或正价格 | 依赖相应列的指标局部不可计算；其他族继续 |
| `QELT_SECTOR_DATA_SCHEMA_INVALID` | l2 数据缺失或非法 | `sector_regime=NOT_COMPUTABLE`；其他族继续 |
| `QELT_LABEL_PARITY_FAILED` | 同 horizon label 与 return 公式不一致 | `signal_path=COMPUTED_WITH_LIMITATIONS`；双方结果和差异并存 |
| `QELT_ENTRY_COVERAGE_LOW` | entry coverage 低于 profile 参考值 | 记录 coverage/损失估计，不淘汰结果 |
| `QELT_PATH_COVERAGE_LOW` | path coverage 低于 profile 参考值 | 记录 coverage/删失/损失估计，不淘汰结果 |
| `QELT_INSUFFICIENT_MATURITY` | 合法但部分期限尚未成熟 | 相应 horizon 记录不足；已成熟 horizon 继续 |
| `QELT_EXECUTION_EVIDENCE_INSUFFICIENT` | 无原因码、队列或撤单证据 | `execution_cause=NOT_VERIFIABLE`；`order_fill` 和其他族继续 |
| `QELT_POSITION_ARTIFACT_MISSING` | position 缺失 | `position_episode=NOT_COMPUTABLE`；其他族继续，生成补归档计划 |
| `QELT_POSITION_HISTORY_LEFT_CENSORED` | 首个 position 快照已经持仓 | 保留 observed episode 边界但不伪造 entry/capture；生成更早 position history 获取计划 |
| `QELT_EPISODE_RECONCILIATION_FAILED` | position 日期、快照完整性或价格日历无法重建 | `position_episode=NOT_COMPUTABLE` 或局部限制；其他族继续，保留冲突和补归档计划 |
| `QELT_EXECUTION_BRIDGE_RECONCILIATION_FAILED` | indicator/order/trade/position 的日期、数量或价格冲突 | 冲突侧局部受限；另一侧已有 entry/exit 证据继续计算；不得回退到日线猜测 |
| `QELT_PORTFOLIO_DIAGNOSTICS_INCOMPLETE` | return 可用但 cost/turnover 缺失或部分覆盖 | 权威收益继续计算，成本/换手字段显式 null 并报告覆盖与补归档计划 |
| `QELT_ARTIFACT_UPLOAD_FAILED` | CAS 发布失败 | platform CAS 状态失败；保留本地 receipt 和已计算结果，重试发布 |
| `QELT_ARCHIVE_PERSIST_FAILED` | DB 事务失败 | platform DB 状态失败；worker receipt 保留，重试入仓 |
| `QELT_DUPLICATE_IDENTITY_CONFLICT` | 同 identity 不同内容 | failed 并告警 |
| `QELT_RESOURCE_EVENT_INVALID` | token/sequence/绑定非法 | 拒绝事件，任务不假完成 |

禁止 `except: pass`、空 dict 当成功、用 0 替代缺失、用当前生产数据替代指定 snapshot，以及只记录日志不返回 reason code。局部 `NOT_COMPUTABLE/NOT_VERIFIABLE` 是显式科研信息，不是静默降级；系统必须继续计算不依赖该缺失项的指标族。

## 12. Implementation Plan / 实施方案

### Phase 1：QE 专属契约与纯计算引擎

1. 新增 profile/DTO/reason-code/identity 模块；
2. 新增严格 price/sector reader，只读 QE snapshot；
3. 实现 signal wide observation、maturity/censor、统计推断；
4. 实现 episode reconstruction、signal→fill/exit evidence bridge 和 capture；
5. 建立 formula、calendar、suspension、limit state、delist/data-gap、sector PIT、order/trade/position reconciliation 的 unit oracle。

### Phase 2：计算节点、资源阶段和制品

1. ConfigComposer 固化 profile、feature snapshot 和 evaluator source SHA；
2. worker wrapper 接入 normal Loop 与 `long_trend_only`；
3. 资源状态机加入 `long_trend_eval`，实现 CPU postprocess 单槽、outbox 和恢复；
4. 新增 QE-only `QELongTrendArtifactStore`、独立 root/manifest/allowlist，并原样保存可得的 QE order/trade/position 输入指纹；通用 Prediction Store 保持只读回归目标；
5. `read_exp_res.py` 只校验和挂载 compact receipt。

### Phase 3：数仓、API 与 MCP

1. 添加 migration/rollback/init schema 同步；
2. 添加 `RunEvaluationRecord/RunEvaluationMetricRecord/RunEvaluationArtifactRecord`、专属 repository 和三表事务 writer；
3. 由长期评价 receipt writer 直接解析并写专属表；通用 PayloadExtractor、run_metric/run_artifact writer 不变；
4. 实现有界 analytics API 和只读 MCP；
5. 对旧 Archive payload、旧 prediction manifest、旧 run_metric/run_artifact 和非 QE 路由/schema 做零变化回归。

### Phase 4：UI 与历史 R6 路径

1. Loop 创建页增加不可变 profile 开关；
2. Loop 详情增加进度、成熟度、长期指标与入场/退出可成交性分层；
3. Archive 页增加同 vintage、同 execution evidence quality 对比；
4. 对已归档 R6 执行输入可用性预览：分别列出 pred/position/report/indicator/dataset identity 的可得性；
5. UI 始终允许对 QE run 创建 `long_trend_only` 评价任务；缺失输入只把依赖它的指标族标为 `NOT_COMPUTABLE/NOT_VERIFIABLE`，并显示数据行动计划。

### Phase 5：真实验证与发布准备

1. 使用小型 deterministic fixture 验证所有公式；
2. 使用真实非生产 Recorder + snapshot 完成 worker → CAS → DEV DB → API/MCP → UI E2E；
3. 验证后端重启、重复 callback、CAS/DB 中途失败和恢复；
4. 在不启动训练的条件下，用一个已完成 Loop 做结果-only smoke；
5. 执行 ownership/import/route/schema 静态隔离验证，证明唯一硬边界——非 QE 模块和通用 Prediction Store 零影响；
6. DESIGN-COMPLIANCE-001、F2 validator 和实现检查结果随代码变更记录；未完成平台项进入 `platform_delivery_status`，不影响已有科研结果或后续 QE 实验。

### 12.1 并行工作流与完成语义

Phase 1–5 是依赖顺序，不要求所有开发串行。实际实施拆为：

| 工作流 | 覆盖 | 可独立到达的内部里程碑 |
|---|---|---|
| A：计算/统计/可成交性 | Phase 1、signal/episode schema、order/trade/position reconciliation、formula oracle | `CORE_COMPUTE_VERIFIED` |
| B：CAS/状态/三表 | Phase 2–3 的 artifact、identity、fencing、migration、repository、恢复 | 与 A 联调后形成可复算 receipt |
| C：API/MCP/UI/历史补算 | Phase 3–5 的查询、展示、`long_trend_only`、真实 E2E | 与 A/B 联调后形成完整用户链 |

A、B、C 分别维护 `platform_delivery_status`，不再产生任何全局 research-ready 或研究许可状态。A 中任一指标族形成可复算 receipt 后即可用于科研分析；B/C 的 CAS、DB、API/MCP/UI 和历史补算状态不改变该结果，只决定持久化、查询和展示能力。调用方不得把 `NOT_COMPUTABLE/NOT_VERIFIABLE` 伪装为已计算，但可以继续运行所有不依赖缺失项的实验与分析。

### 12.2 v1.5 当前实施进度（2026-07-16）

| 子项 | 状态 | 实现 / 证据 | 未完成边界 |
|---|---|---|---|
| versioned profile、reason、family status、identity | `CORE_VERIFIED` | `long_trend_evaluation_contract.py`；不可变注册 profile、显式 null 输入、稳定 `qelt_` identity、六族独立状态、完整 overlap receipt 与 feature/outcome 一致性 oracle | repository/task identity 绑定在 Phase 2–3 接入 |
| QE 严格数据读取与双快照 parity | `REAL_SNAPSHOT_VERIFIED` | `long_trend_data_reader.py`；仅允许 `daily_pv.h5/sector_data.h5`，校验文件内容 hash、QE dataset/workspace samefile 绑定、same/strict-extension full-overlap qfq OHLC 精确一致；R8B 六个真实 Loop 固定到 2026-06-30 snapshot | 计算节点自动 wrapper、CAS/DB identity readback 在 Phase 2–3 接入 |
| signal path | `REAL_R8B_6_OF_6_COMPUTED` | `long_trend_evaluation.py`；T+1→T+h+1、feature 截止日隔离、entry-day path 排除、20–180D 宽 schema、maturity/right censor、ordered stage/survival、稳定排名、RankIC、TopK、barrier、time-to-hit、MFE/MAE、AUCPR；每腿约 220.7 万行 | CAS/DB 自动发布仍在 Phase 2–3；本地 immutable Parquet/summary 已生成 |
| statistics / slices / sector | `REAL_R8B_6_OF_6_COMPUTED` | signal-day Newey-West、moving-block bootstrap、BH-FDR；全期/126/252 交易日位置切片；signal-date PIT L2 与逐板块指标；真实 sector coverage 约 99.99% | Archive/API readback 与更深 Recall@100 profile 继续追加 |
| position episode | `REAL_R8B_6_OF_6_COMPUTED_WITH_LIMITATIONS` | 0↔持仓转换、re-entry/open/right censor、首段 left censor、position 自身 as-of、outcome extended path、capture/post-exit/false early-exit；真实每腿约 2,187–2,420 episodes | 少量路径/日历 reconciliation limitation 保留在 family status；更早 position 与费用制品可历史补充 |
| order fill / execution cause | `REAL_ARTIFACT_REPLAYING_AFTER_QLIB_OVERFILL_FIX` | entry/exit 对称解析 Qlib indicator `amount/inner_amount/deal_amount/ffr`、trade、order intent、position transition；合法 `ffr>1` overfill 完整保存；一笔 trade 只归属一个信号；真实数量/时点/原因矛盾 fail-fast | 初次 R8B replay 暴露旧 overfill 误判，修复后使用新 source hash 重放；child-order/queue 原因证据仍可补充，cause 局部 `NOT_VERIFIABLE` |
| portfolio result | `REAL_R8B_6_OF_6_COMPUTED_WITH_LIMITATIONS` | 真实 Qlib portfolio report 独立校验并计算累计/年化收益、波动、Sharpe、最大回撤、成本和换手；不以 signal/episode close return 冒充组合成本后收益 | 原 report 中成本/换手诊断存在零值/缺失冲突时显式 limitation；权威收益仍保留 |
| family-local failure | `CORE_IMPLEMENTED` | prediction/sector/label/position/execution 可选输入独立定级；一个族异常不丢弃其他已计算族 | worker/CAS/DB/API/UI 对同一状态的贯通在 Phase 2–4 继续 |
| core tests | `VERIFIED` | 三个 F-014 test 文件 `61 passed`；三核心模块 line coverage `87.59%`、branch coverage `72.62%`；含 entry/exit overfill、真实冲突、source-change、strict-root oracle；ruff、py_compile、diff 与 ownership scan 通过 | DEV DB、API/MCP/UI、自动 worker 和重启恢复不属于本阶段已完成证据 |
| platform B/C | `PENDING_BY_DESIGN` | 设计与 acceptance id 保留 | CAS、资源、三表、migration、API、MCP、UI、历史补算、真实 E2E 未实现；不得宣称 F-014 整体完成 |

## 13. Verification Plan / 验证方案

### 13.1 L0/L1

- profile hash、schema 和非法覆盖；
- T/T+1/T+h+1 formula parity；
- entry 日 high/low 不得进入未来路径；
- 20/40/60/120/180 maturity 与右删失；
- ordered stage 单调性、删失调整的 barrier survival 与 prefix path coverage；
- suspension/missing/delist reason；
- 首个 position 快照已持仓的左删失、position 自身 as-of 与 outcome 扩展分离、全零持仓 schema；
- `filled_t1/delayed_fill/never_filled/not_attempted_by_strategy/not_verifiable` 入场状态与 delay；
- 涨停买入、跌停/停牌退出阻断的对称 fixture；仅日线触板时必须保持 `not_verifiable`；
- order/trade/position reconciliation 的一对一归属、数量/时点/原因冲突 fail-fast，不回退到价格猜测；数量 oracle 明确覆盖合法 `ffr>1` overfill、`deal<=inner_amount`、`ffr=deal/amount`，不得把 Qlib 分层目标差异误报为冲突；
- barrier nesting、time-to-hit、AUCPR、禁止普通 score Brier；
- stable tie rank；
- episode open/close/add/reduce/re-entry；
- false early-exit；
- block bootstrap/HAC/BH-FDR deterministic seed；
- identity content hash、workspace samefile、full-overlap receipt、same/strict-extension snapshot parity，以及 feature 截止日后的预测隔离；
- 非 QE source、缺 QE dataset/workspace identity 的 fail-fast；
- import ownership allowlist，禁止 QE 长期评价依赖任何 live/Paper/Selection/Advisory/QMT runtime。

### 13.2 L2

- ConfigComposer normal/all/selected/retry/rerun 路径；
- resource phase transition、auth、sequence、restart/outbox；
- QE-only artifact store allowlist、atomic CAS、hash conflict；
- migration apply/readback/rollback（DEV DB）；
- repository 三张 evaluation 表事务；
- 通用 Prediction Store manifest/API、Archive PayloadExtractor、run_metric/run_artifact schema 快照零变化；
- API/MCP bounded response、execution-status filter、QE source preflight 和错误映射；
- `git diff --name-only` ownership allowlist，以及 Selection/Advisory/Paper/模拟盘/QMT/StrategyPackage route/schema regression。

### 13.3 L3

真实非生产数据链：

```text
completed Recorder
 -> pred/position + exact feature snapshot
 -> outcome snapshot same/extension verification
 -> long-trend worker
 -> CAS artifacts
 -> qlib_results_enhanced receipt
 -> qe_archive.run_evaluation/run_evaluation_metric/run_evaluation_artifact
 -> API/MCP/UI readback
```

逐点核对 Parquet 行数、SHA、DB metric 数、UI 数值、execution evidence coverage 与 receipt 一致。Mock 不能替代该链；真实 canary 必须至少覆盖一个正常 T+1 成交、一个延迟/阻断或 `not_verifiable` 样本，并证明组合成本后收益仍来自 portfolio report。

### 13.4 L4/L5

- 一个历史 Loop 的 `long_trend_only` smoke，不训练、不回测；
- 同时运行下一 Loop GPU 训练，验证 CPU postprocess 不抢占 GPU lease；
- 后端重启期间 worker 持续，恢复后只归档一次；
- 多 seed/model 同 vintage Archive 查询；
- 生产 DDL、服务重启和真实 R6 批量评价必须分别取得明确授权。

### 13.5 Coverage

新增/修改 Python line coverage ≥80%、branch coverage ≥70%。核心 formula、censor、identity、transaction、failure branches 必须直接覆盖；UI 提供真实 API E2E，不以静态截图替代。

### 13.6 Phase 1 complexity review

- price/prediction/sector 仅规范化一次；signal path 按 signal-date chunk 生成，五个 horizon 在 NumPy 矩阵上计算，核心量级为 `O(H×N)`，不按股票逐行调用 H5；Phase 2 CAS worker 继续复用 chunk iterator 写明细，避免为持久化再复制一份全量 wide frame；
- RankIC、Top-K、sector 和 label parity 使用 `groupby/join` 的向量化聚合；label 只在 `(signal_date,instrument)` 唯一键上做一次 inner join，不做笛卡尔积；
- holding episode 的 price slice 每只股票只解析一次；episode→entry signal 先构建 `(instrument,date)` 索引，exit signal 与 trade matching 使用每只股票的日期 map/排序日期/`searchsorted`，由原始逐 episode 或逐 trade 全表过滤收敛为常见路径 `O(N + E log S + T log S)`；退出路径复用按股票缓存，不重复扫描全 price frame；无法唯一归属的 delayed trade 仍显式保留 ambiguity，而不是为性能猜测归属；
- guardrail 的 `ALGO-COMPLEXITY-001` P2 命中分别对应总编排、向量化状态汇总、唯一键 label join 和 episode bridge；本节记录其复杂度与内存边界，不把 P2 质量信息变成科研许可条件。真实大样本 resource receipt 仍在 Phase 2 smoke 中记录。

## 14. Risks / Failure Modes / 风险

| 风险 | 影响 | 控制 |
|---|---|---|
| 长 horizon 尾部大量未成熟 | 把近期强信号误判失败 | 右删失、maturity 数和 evaluation vintage |
| outcome snapshot 修改历史价格 | 结果不可复现 | overlap qfq OHLC parity + extension-only |
| outcome 扩展日期被误当成原实验 feature 日期 | 将未来预测混入原实验 | prediction signal date 必须落在 feature snapshot 窗口；outcome 只提供结果路径 |
| 重复信号造成伪样本量 | 置信度虚高 | 先按信号日聚合、block bootstrap/HAC |
| high/low 当成可成交 | 夸大收益能力 | path diagnostic 与 executable 字段隔离 |
| 无成交被一律归因涨停 | 把策略未下单或缺证据误判为市场阻断 | order/trade/position 权威等级；无意图/队列证据显式 `not_verifiable` |
| 只检查买入侧 | 低估趋势反转后的退出风险 | 对称记录跌停、停牌、延迟退出及额外回撤/持有天数 |
| 首个持仓快照已有仓位 | 伪造入场日和捕获率 | 显式 left censor，不计算 entry-based 指标，补取更早 position history |
| outcome 比 position artifact 更晚 | 错把回测结束后的未知仓位当持续持有 | 持仓 episode 以 position 自身 as-of 收口；更晚 outcome 只用于价格路径 |
| 正常成交被计入“缺阻断原因” | execution-cause coverage 永远偏低 | 原因覆盖分母只含失败/不可验证事件；正常成交与策略未尝试分开 |
| 百万明细写 DB | 表膨胀、查询变慢 | CAS wide Parquet，DB 只存标量/指针 |
| H5 并行全量读取 | 内存和磁盘抖动 | CPU postprocess 单槽、只读两文件、chunk 释放 |
| 历史 run 缺 snapshot identity | 可能错用当前数据 | 不猜默认数据；依赖该快照的指标族标记限制，并生成定位/重导/代理互证计划 |
| sector 当前成分污染历史 | 板块归因泄漏 | 信号日 PIT l2_code_id |
| 普通 score 计算 Brier | 伪校准 | 只有 probability semantics 才计算 |
| evaluator 失败被 UI 隐藏 | 假成功 | 独立状态/reason、Loop/Archive/MCP 同步展示 |
| 内部计算核与平台交付混淆 | 科研结果与 UI/数仓状态被混写 | 指标族证据和 platform delivery 分栏；任一可复算结果可分析，平台缺口继续补齐 |
| 自动评价阻塞下一训练 | 降低实验吞吐 | GPU release 后 CPU 后处理，独立资源槽 |
| 多评价版本混排 | 错误横向比较 | UI 默认同 outcome vintage/snapshot |
| 复用共享 store/table 产生跨模块耦合 | Paper/Selection/Advisory 被 schema 或 writer 变更波及 | QE-only CAS、additive 表和 ownership/import/route/schema 零变化；这是唯一硬边界 |

## 15. Rollout / Rollback / 发布回滚

### 15.1 发布顺序

1. `[PHASE1_VERIFIED_SOURCE_DELIVERY_THIS_CHANGESET]` 交付纯计算引擎和 tests；当前没有 composer/profile runtime 接入，因此天然 default-off；PR/merge 以 GitHub 外部状态为准；
2. 合入 QE-only CAS、资源阶段和三张 additive 数仓表 migration；
3. 在 DEV DB apply/readback/rollback；
4. 合入 API/MCP/UI，但生产 profile 保持关闭；
5. 明确授权后应用生产 DDL并验证；
6. 重启后只对单个已完成 canary Loop 执行 `long_trend_only`；
7. canary 全链通过后再启用 Type B 新任务自动评价；
8. R6 批量评价属于独立运行授权，不随代码发布自动执行。

步骤 1–7 只描述平台交付进度，可并行实施。步骤 1 的任一指标族一旦形成可复算 receipt 即可用于科研分析；CAS、DB、API/MCP/UI、canary 和批量补算完成度分别记录，不存在研究解锁状态。

### 15.2 回滚

1. 关闭 profile 创建入口和 auto-evaluate 配置，不影响训练/回测；
2. 停止新评价派发，允许已运行 worker 写 terminal receipt；
3. 回滚应用代码和 UI；已有 evaluation/metric/artifact 保持只读；
4. additive DB 对象默认保留，除非另有明确 DDL 回滚授权；
5. 不删除 pred/label、Recorder、QE run、历史评价或 CAS blob；
6. 回滚不得影响 Selection、Advisory、Paper v2、模拟盘、QMT、StrategyPackage 或通用 Prediction Store。

## 16. Production Gates / QE-only 唯一硬边界与交付状态（无科研门禁）

| 项目 | 本设计阶段 | 实施语义 |
|---|---|---|
| 唯一硬边界 | `QE_ONLY_ZERO_NON_QE_IMPACT` | 所有任务、数据读取、CAS、表、API/MCP/UI 和写入仅限 QE；非 QE 变更必须为零 |
| QE core compute | Phase 1 已实现并定向验证 | 纯函数、严格 QE reader 和 unit oracle 已存在；尚未创建真实 evaluation task，不代表平台完整交付 |
| QE additive schema | 设计完成、实现待办 | migration、apply/readback 和回滚状态进入 `platform_delivery_status`，不控制科研分析 |
| frontend/backend dependency | 当前无新增依赖 | 未来变化按平台状态记录，不形成研究门禁 |
| runtime restart | 未执行 | 仅是运行时动作状态，不影响已经完成的 QE 训练、回测或科研结论 |
| experiment/evaluation execution | 独立任务状态 | canary、历史补算和新实验可在 QE 范围内并行，不依赖平台全部完成 |
| data/metric availability | 按指标族记录 | 缺失、部分、未成熟或不可验证均生成 `data_action_plan`；其他指标族和研究方向继续 |

本设计不引入任何研究门禁、人工 approval/RBAC、双人复核或方向淘汰规则。schema、hash、数据身份、成熟度和制品完整性用于说明结果口径、可复算性与限制；任何缺口都转化为获取、补归档、补算、代理实验和互证任务。未经用户专门要求并确认，不得新增研究阻断状态。

## 17. Design Acceptance Matrix / 设计验收矩阵

本矩阵同时记录设计完整性与分阶段实施证据。`core_implemented_verified` 只表示 Phase 1 / 工作流 A 的纯计算能力完成；Phase 2–5 是已批准 rollout 的后续交付范围，不是 Phase 1 的设计偏差或验收缺口，因此统一在 `test_or_evidence` 中标明而不写入 `gap_or_exception`。不得把核心完成包装成 F-014 平台整体完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | QE task/Loop → QE worker → QE-only store/tables/UI | 第 4、6、9、10 节代码映射 | design_ready | none |
| F-002 | `long_trend_evaluation_contract.py` profile registry | profile hash、非法 profile、显式 null identity oracle；Phase 2 接入 task/repository binding | core_implemented_verified | none |
| F-003 | `long_trend_evaluation.py` return oracle | T+1→T+h+1 与 label parity tests；Phase 2 执行真实 R8 artifact smoke | core_implemented_verified | none |
| F-004 | close/path projection 分层字段 | entry-day high 排除、close/high-low 分栏 tests；Phase 2 执行真实 Parquet schema readback | core_implemented_verified | none |
| F-005 | maturity/censor state machine | mature/path-gap/right-censor/open/left-censored episode、position-asof/outcome-asof 分离 tests；Phase 2 接入 delist authoritative resolver | core_implemented_verified | none |
| F-006 | feature/outcome snapshot identity | content hash、workspace samefile、full-overlap receipt、same/strict-extension/missing-lineage/exact mismatch、feature-date isolation oracle；Phase 2 接入 actual snapshot manifest resolver | core_implemented_verified | none |
| F-007 | QE dataset contract + signal-date l2 | 双文件 allowlist、QE identity、PIT sector 与静态 import tests；Phase 2 接入 compute-node wrapper | core_implemented_verified | none |
| F-008 | signal metrics engine | horizon/barrier/ordered stage/survival/Top-K/RankIC/AUCPR/MFE/MAE tests；Phase 2 产生真实大样本 receipt | core_implemented_verified | none |
| F-009 | episode engine | exit/re-entry/open/left-censor/position-asof/false-early-exit normalized-position tests；Phase 2 接入 Qlib Position/trade resolver | core_normalized_input_verified | none |
| F-010 | slice/sector artifact schema | 126/252 交易日位置、L2 concentration/per-sector metrics tests；Phase 3 完成 CAS Parquet publish/readback | core_implemented_verified | none |
| F-011 | block bootstrap/HAC/BH-FDR | deterministic、empty/singleton/zero-variance fixtures；Phase 2 记录大样本 resource receipt | core_implemented_verified | none |
| F-012 | QE-only CAS Parquet + compact DB | namespace/manifest/size/row/hash E2E | design_ready | none |
| F-013 | 三张 additive `run_evaluation*` 表 | DEV migration/repository + shared-schema snapshot tests | design_ready | none |
| F-014 | normal + `long_trend_only` shared engine | path parity tests | design_ready | none |
| F-015 | resource phase/outbox/fencing | restart/duplicate callback E2E | design_ready | none |
| F-016 | independent evaluation status/reason | prediction/price/sector/position/portfolio/entry-or-exit evidence 缺失或冲突时保留其他可计算族的 tests；Phase 2–4 贯通 worker/Archive/UI state | core_implemented_verified | none |
| F-017 | Loop/Archive UI | real API Playwright E2E | design_ready | none |
| F-018 | bounded read-only MCP | manifest and response-bound tests | design_ready | none |
| F-019 | default-off compatibility | pure core has no composer/startup registration and no runtime side effect；Phase 2 接入后 normal task composer 仍保持 default-off | core_implemented_verified | none |
| F-020 | full delivery controls | F2 validator、DEV DB E2E、design compliance matrix | design_ready | none |
| F-021 | QE-only ownership/import/runtime isolation | 三个 core 文件的非 QE/import allowlist regression；Phase 4 补齐 platform route/schema diff tests | core_implemented_verified | none |
| F-022 | signal→fill/exit evidence bridge | entry/exit full/partial/delayed/never/not-attempted/not-verifiable、一对一 trade、indicator/trade 数量与时点矛盾、直接阻断损失 tests；Phase 2 接入真实 Recorder/Archive/CAS resolver 与 child-order/queue evidence | core_normalized_entry_exit_bridge_verified | none |
| F-023 | staged delivery truth | invalid sector/position/execution 不丢弃 signal；receipt 分列 family/platform 状态；Phase 4 保持 API/UI 同语义 | core_implemented_verified | none |
| F-024 | platform delivery status only | core receipt 显式标注 core 与 Phase 2–5 后续范围，不输出 research-ready；后续完成 CAS/DDL/API/MCP/UI/backfill/E2E | core_implemented_verified | none |

## 18. DESIGN-COMPLIANCE-001

- [x] `no_simplified_delivery / Phase 1`：本 changeset 完整实现批准的 Phase 1 工作流 A，不以缺 entry/exit、删失、survival、portfolio 或 identity 语义的子集冒充核心完成；F-014 平台整体仍明确为 Phase 2–5 pending。
- [x] `no_silent_error / Phase 1`：非法 profile/receipt、快照/feature 窗口漂移、预测/路径/position/执行/portfolio 冲突均显式 reason、family limitation 或 fail-fast；无 evidence 不伪装成功。
- [x] `no_business_semantic_drift / Phase 1`：纯核心 default-off，不注册 route/startup/scheduler，不改变训练标签、模型、回测结果、因子，也不触碰 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 或通用 Prediction Store。
- [x] `no_unrequested_gate_or_approval`：除 QE-only 零影响边界外，不增加研究门禁、人工审批或方向淘汰规则；数据缺口只形成指标族状态和 data action。
- [x] Phase 1 数据集、预测、evaluation context 与 receipt 使用同一 deterministic identity；feature/outcome 关系和输入 null 均进入身份。
- [ ] Phase 2–5 的 QE-only CAS、三张 additive evaluation 表、worker、API/MCP/UI、历史补算和真实 E2E 尚未实现；该事实是 platform delivery 状态，不阻断科研。
- [x] 理论机会、实际成交和证据不足三层明确分开；买入/退出阻断对称，日线触板不冒充订单真值，正常成交不被错误计入原因缺失分母。
- [x] 任一可复算指标族立即可用于科研分析；工程里程碑只表示 platform delivery 进度，不控制研究。
- [x] source merge、生产 DDL、服务重启、canary 和 R8 历史批量评价保持分离；本 changeset 未执行后三项。

## 19. Existing-Code Implementation Anchors / 现有代码实施锚点

允许修改/新增的 QE ownership：

- `backend/services/quantevolver/qe_dataset_contract.py`
- `backend/services/quantevolver/config_composer.py`
- `backend/services/quantevolver/templates/read_exp_res.py`
- `backend/services/quantevolver/results_only_retry.py`
- `backend/services/quantevolver/qe_resource_phase_service.py`
- `backend/services/quantevolver/long_trend_*.py`（新增）
- `backend/services/qe_archive/long_trend_*.py`（新增专属 model/repository/query）
- `backend/routers/quantevolver*.py` 与 `backend/routers/qe_archive.py` 的专属 endpoint registration
- `backend/mcp/modules/qe_archive.py` 的只读长期评价 query
- `frontend/src/app/quantevolver/**`、`frontend/src/app/qe-archive/**` 及其专属 API client/test
- versioned `backend/migrations/qe_long_trend_evaluation_f2_*.sql` 和 rollback/init mirror
- 与上述 ownership 一一对应的定向测试

只读实现参考/零变化回归锚点，不允许为本能力修改：

- `backend/services/model_store/artifact_store.py`
- `backend/routers/prediction_store.py`
- 通用 Prediction Store manifest/schema/API tests
- `backend/services/qe_archive/payload_extractor.py`
- 既有 `qe_archive.run_metric/run_curve/run_artifact/raw_payload` model/repository/writer/schema
- `backend/services/selection_center/**`
- `backend/services/advisory*`
- `backend/services/paper_trading/**`
- `backend/infra/qmt_client.py` 及 QMT/MiniQMT route
- `backend/services/strategy_package/**` runtime/promotion/writer
- Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 前端页面和 API client

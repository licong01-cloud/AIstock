# Paper v2 / QE 候选策略包与数仓边界正式设计

**日期**: 2026-05-12
**状态**: 正式边界替换稿
**适用范围**: QE 实验、QE Loop、QE 数仓、候选策略包、Paper v2 StrategyPackage、选股、AIstock 模拟盘、未来 miniQMT 模拟盘/实盘适配
**替换对象**: `docs/architecture/paper_v2_qe_integration_overhaul_20260512.md` 中与 StrategyPackage、HMM、ST/PIT、SOTA 殿堂、QE 数仓、删除持久化相关的边界
**优先级**: 与旧方案或 Claude Code 既有草案冲突时，以本文和用户 2026-05-12 最新要求为准
**执行说明**: 本文是设计文档；不代表已执行 DB DDL、运行服务、改生产配置或合并分支。

---

## 0. 替换声明

本文把用户最新要求落地为 Paper v2 / QE 集成的正式边界。以下边界替换旧方案：

1. HMM、ST/PIT 股票池、未来事件驱动独立信号，均属于 Paper v2 平台能力，与 StrategyPackage 隔离。
2. StrategyPackage 锁定的是因子、模型配置、策略配置及其版本化引用，不包含 HMM 数据、ST/PIT 数据、事件信号数据本体。
3. QE 数仓只做历史回测归档和组合分析，不在数据库内保存大体量文件或完整训练/预测数据。
4. SOTA 殿堂改名并重构为候选策略包；不再有演进自动判断进入 Paper v2 或实盘的语义。
5. 候选策略包、正式 StrategyPackage、QE 数仓归档，必须独立于原始 QE 实验/Loop 的删除生命周期。
6. QE 与 Paper v2 必须共用统一配置层和等价执行逻辑；Qlib bin、实时 DB、miniQMT 只是数据/执行适配器差异。

---

## 1. 设计目标

### 1.1 目标全流程

```text
QE 配置选择
  -> QE 实验或 Loop 执行
  -> QE 数仓归档轻量化证据
  -> 用户手动加入候选策略包
  -> 从候选策略包创建 Paper v2 StrategyPackage
  -> Paper v2 选股
  -> AIstock 模拟盘
  -> 未来 miniQMT 模拟盘 / 实盘适配
  -> 新表现回流 QE 数仓分析组合效果
```

### 1.2 必须解决的问题

| 问题 | 正式要求 |
|---|---|
| 模块彼此独立、接口不串联 | 以共享配置层、明确 source/ref、L3/L4 跨模块验收打通全流程 |
| StrategyPackage 被误当成运行时环境快照 | StrategyPackage 只锁定策略定义和资产引用；运行时环境由 Paper v2 平台解析 |
| ST/PIT、HMM 被误放入包限制 | ST/PIT、HMM、事件信号均是平台能力，不作为包内数据或选股/模拟盘硬门槛 |
| QE 数仓可能膨胀为文件仓库 | DB 只保存可查询摘要、配置、指标、hash、URI；大文件外置并受保留策略控制 |
| SOTA 自动语义不符合用户要求 | 改为用户显式操作的候选策略包，不允许自动晋升 |
| 删除 QE 源数据可能破坏候选或包 | 候选、包、数仓归档必须有独立持久化和独立删除动作 |
| 回测与模拟盘/实盘不一致 | 日频策略、分钟线执行、尾盘处理、模型权重策略、HMM 运行时均通过统一配置/适配器保证一致 |

---

## 2. 核心边界

### 2.1 QE 实验层

QE 负责：

- 选择因子、模型、训练周期、HMM 使用方式、日频策略、分钟线执行策略、尾盘处理策略、资金/成本/风控等配置。
- 基于 Qlib bin 和历史快照执行回测。
- 生成可归档证据：配置、指标、因子清单、模型训练摘要、随机种子、超参数、训练窗口、artifact manifest、hash、外部 URI。
- 把实验或 Loop 结果入 QE 数仓，用于后续组合比较和候选选择。

QE 不负责：

- 决定某结果自动成为 Paper v2 候选或实盘候选。
- 为 Paper v2 提供实时 HMM 快照、实时 ST/PIT 股票池或实时事件信号数据。
- 在数仓 DB 中保存模型权重、完整 pred.pkl、完整因子 parquet、完整分钟数据、完整日志等大文件。

### 2.2 QE 数仓层

QE 数仓负责：

- 归档历史回测证据，服务组合分析、复盘、候选策略包创建、可复现性审计。
- 保存不同因子组合、模型配置、训练窗口、HMM 配置、策略执行配置、尾盘处理配置下的性能差异。
- 保存大文件的 URI、hash、大小、生成程序版本、解析状态，不保存大文件本体。
- 在原始 QE 实验/Loop 删除后继续保留归档结果，除非用户执行独立归档删除流程。

QE 数仓不负责：

- 承担生产实时运行状态源。
- 承担策略包资产目录本体。
- 承担实时股票池、实时 HMM、实时事件信号的运行时查询。

### 2.3 候选策略包层

候选策略包负责：

- 保存用户显式加入的 QE 实验或 Loop 的策略候选快照。
- 作为从 QE 结果进入 Paper v2 StrategyPackage 的人工筛选池。
- 独立持久化，不因原 QE 实验/Loop 删除而消失。
- 支持从候选策略包再次生成新的候选版本，或创建正式 StrategyPackage。

候选策略包不负责：

- 表示已经支持选股、模拟盘或实盘。
- 自动审批、自动晋升或自动覆盖旧包。
- 保存 HMM/ST/PIT/事件信号数据本体。

### 2.4 Paper v2 StrategyPackage 层

StrategyPackage 负责锁定：

- 因子列表、因子顺序、因子变换/标准化配置、因子版本引用。
- 模型入口、模型类型、超参数、训练配置、评分配置、默认回测模型权重引用。
- 日频策略、分钟线执行策略、尾盘处理策略、组合/风控配置。
- 资产完整性 manifest：权重文件、模型入口、因子引用、训练 seed、超参数、训练窗口、artifact hash/URI。
- 包版本号和来源引用，包括 QE experiment、QE loop、candidate_strategy_package。

StrategyPackage 不锁定：

- ST/PIT 股票池数据范围。
- HMM 历史快照数据。
- 事件驱动独立信号数据。
- 实时行情、miniQMT 连接、Paper v2 runtime 状态。

### 2.5 Paper v2 平台能力层

Paper v2 平台负责：

- 基于最新数据生成 ST/PIT 股票池和可交易性过滤。
- 提供 HMM 实时/as-of 预测、HMM 模型滚动训练、HMM 版本注册和 QE 更新同步。
- 提供未来事件驱动独立信号的 as-of 查询、实时更新和平台级 profile。
- 提供模型滚动训练能力和权重策略选择。
- 提供选股、模拟盘、未来 miniQMT 模拟盘/实盘的统一执行入口。

---

## 3. 策略包资产与门槛

### 3.1 资产完整性

所有 StrategyPackage 的资产必须由 QE 更新后的程序或候选包创建程序校验完整性。至少包括：

| 资产 | 必须保存 |
|---|---|
| 模型入口 | 模型类/函数入口、模块路径、推理接口契约、版本 hash |
| 模型权重 | 默认回测权重 URI、sha256、文件大小、生成 run_id、训练窗口 |
| 训练配置 | seed、超参数、优化器参数、label horizon、训练/验证/测试切分 |
| 因子配置 | 因子名、顺序、版本、表达式/代码 hash、缺失处理、标准化方式 |
| 策略配置 | 日频选股/调仓规则、分钟线执行策略、尾盘处理策略、成本/滑点/风控 |
| 运行引用 | 可选 runtime profile 引用，例如 hmm_profile_id、stock_pool_profile_id、event_signal_profile_id |
| 可复现 manifest | 环境版本、依赖版本、artifact manifest hash、缺失项说明 |

### 3.2 缺失 seed/超参数的处理

旧实验中没有保存模型初始 seed、完整超参数或其他可复现配置时：

- 不得因此阻止进入选股或 AIstock 模拟盘。
- 不得因此作为股票池、选股、模拟盘的限制条件。
- 必须标记 live_approval_blocked 或 equivalent eligibility finding。
- 未来要进入实盘，必须重新设置参数、滚动训练、经过模拟盘验证，并生成新的可复现资产版本。

### 3.3 包状态最小化

不应继续扩展大量包状态。建议语义压缩为：

| 能力/状态 | 语义 |
|---|---|
| asset_validated | 资产完整，可用于选股和模拟盘 |
| selection_enabled | 可被 Selection Center 使用；可由 asset_validated 派生 |
| paper_enabled | 可被 AIstock Paper v2 模拟盘使用；可由 asset_validated + runtime profile 可用性派生 |
| live_approval_reserved | 预留实盘状态；实盘未开发前不启用，未来必须走确认/审批 |
| retired/deleted | 退役或显式删除；不得隐式由源 QE 删除触发 |

当前代码已有更多状态时，短期可以保留兼容，但新设计不再增加状态种类，应通过 eligibility/capability view 表达运行能力。

---

## 4. 模型权重与滚动训练

### 4.1 默认权重策略

StrategyPackage 创建后默认选择 QE 回测时的模型权重：

```text
model_weight_policy = backtest_default
```

该默认权重用于复现 QE 回测结果和包创建时的基准表现。

### 4.2 Paper v2 滚动训练

Paper v2 必须支持模型权重滚动训练，尤其用于选股、模拟盘和未来实盘：

| 权重策略 | 用途 |
|---|---|
| backtest_default | 使用 QE 回测权重，保证创建时基准可追溯 |
| rolling_latest | 使用最近几年数据滚动训练得到的最新权重 |
| rolling_asof | 使用指定日期之前可见数据训练得到的 as-of 权重 |

滚动训练产生的是 weight_revision，不覆盖原 StrategyPackage。只要因子集合、模型结构、超参和策略配置不变，权重 revision 可挂在同一包版本下；如果修改因子、模型超参或策略配置，必须创建新的 StrategyPackage 版本。

### 4.3 实盘准入

- backtest_default 可以用于选股和模拟盘。
- live_approval 必须要求可复现训练配置、最近数据滚动训练、模拟盘验证、人工确认。
- 旧实验缺少 seed/超参数时，不能直接进入未来实盘；可在模拟盘阶段重新设置参数、滚动训练、验证后生成新版本。

---

## 5. HMM 平台化设计

### 5.1 边界

HMM 不属于 StrategyPackage 资产门槛。策略包可以声明是否使用 HMM 以及引用哪个 HMM runtime profile，但不能携带 HMM 数据本体或把历史 HMM 快照作为选股/模拟盘/实盘条件。

### 5.2 回测与运行时区别

| 场景 | HMM 数据来源 |
|---|---|
| QE 回测 | 历史数据快照，仅用于历史评估和归档 |
| Paper v2 选股 | Paper v2 HMM runtime 基于最新/as-of 行情预测 |
| AIstock 模拟盘 | Paper v2 HMM runtime 每个交易日/as-of 预测 |
| miniQMT 模拟盘/实盘 | 同一 HMM runtime profile，通过 miniQMT 适配器消费结果 |

历史 HMM 快照只能归档为 backtest_context 的 hash/摘要，不能在运行时复用。

### 5.3 HMM 运行能力

Paper v2 需要提供：

- HMM model registry：模型版本、训练窗口、特征版本、训练 seed、超参、artifact URI/hash。
- HMM rolling trainer：基于最近几年数据滚动训练，可按计划或手动触发。
- HMM predictor：提供 latest/as-of regime、probability、confidence、effective_at。
- HMM profile：把策略配置中的 use_hmm 与具体 HMM 版本策略绑定，例如 rolling_latest 或 pinned_approved。
- QE 同步机制：QE 实验更新新的 HMM 版本后，通过 registry/outbox/API 自动同步到 Paper v2，不需要手工复制。

---

## 6. ST/PIT 股票池平台化设计

### 6.1 边界

ST、退市风险、停牌、涨跌停、上市天数、可交易性、PIT 数据范围等属于 Paper v2 股票池和交易日运行时能力，不属于 StrategyPackage 限制条件。

### 6.2 运行时生成

Paper v2 每次选股、模拟盘或未来实盘运行时，应基于最新可用数据生成 stock universe：

```text
trade_date/as_of
  -> stock_pool_profile
  -> ST/PIT filter
  -> suspend/limit/pre_close/tradability filter
  -> strategy score universe
  -> order intent universe
```

QE 回测可以归档当时的股票池规则、PIT 数据版本和 universe hash，但不能把该历史范围带入运行时。

### 6.3 与 Selection Center 的关系

Selection Center 只消费 Paper v2 平台生成的 as-of universe。StrategyPackage 的作用是提供打分和策略逻辑，不负责决定某只股票是否因最新 ST/PIT 数据被排除。

---

## 7. 事件驱动独立信号平台化设计

未来事件驱动信号与 HMM、ST/PIT 一样属于 Paper v2 平台能力：

- 信号数据由独立 ingestion/feature/runtime 服务维护。
- StrategyPackage 只能引用 event_signal_profile 或 signal_policy，不能携带信号数据本体。
- QE 回测中使用的事件信号快照只进入 QE 数仓作为 source version、hash、coverage、命中统计和指标贡献。
- Paper v2 运行时必须使用 as-of 事件数据，避免未来函数。

---

## 8. QE 数仓应保存的数据

### 8.1 必须保存的轻量结构化数据

| 类别 | 具体字段/对象 | 目的 |
|---|---|---|
| run identity | run_id、source_system、source_type、source_id、attempt_no、completed_at、git_sha | 建立可追踪主键 |
| source mapping | QE task、experiment、loop、recorder、MLflow run、node id、artifact URI | 找回来源，但不依赖来源生命周期 |
| canonical config | 因子配置、模型配置、训练窗口、HMM 配置、策略配置、执行配置、成本配置 | 比较组合和复现实验 |
| reproducibility manifest | seed、超参、依赖版本、环境版本、artifact manifest hash、缺失项 | 判断可复现等级和实盘准入风险 |
| data context | backtest_start/end、freq、benchmark、universe policy、PIT version/hash、Qlib data version | 分析数据窗口和样本差异 |
| scalar metrics | IC、RankIC、年化、回撤、换手、Sharpe、Calmar、胜率、成本后收益 | 排序、筛选、对比 |
| curves | return/drawdown/IC/RankIC/training loss 等抽样或压缩曲线 | 可视化和稳定性分析 |
| factor membership | 因子名、因子版本、来源、表达式 hash、分组/cluster、权重 | 分析因子组合效果 |
| factor attribution | importance、SHAP/Permutation summary、IC 分桶、相关性摘要 | 解释因子贡献 |
| model trial | 模型族、模型类型、超参、score、训练耗时、资源摘要 | 分析模型组合和训练效率 |
| training metrics | epoch/step loss、validation score、early stop、best iteration | 分析训练稳定性 |
| execution summary | 持仓、订单、成交、滑点、换手、容量摘要 | 对齐回测和模拟盘执行逻辑 |
| artifact refs | artifact_type、artifact_name、URI、sha256、size、parser_status | 追踪外部文件，不把文件放入 DB |
| candidate evidence | 候选来源、指标快照、配置快照、人工备注、创建人 | 支撑候选策略包 |

### 8.2 不应保存在 DB 的大文件

以下数据不得作为 DB 行大规模存储：

- 模型权重文件。
- 完整 pred.pkl / prediction parquet。
- 完整因子值 parquet 或全部历史日频/分钟线矩阵。
- 完整训练日志、完整 stdout/stderr。
- HMM 系数文件、完整 regime 历史矩阵。
- 原始分钟线行情、tick 行情、miniQMT 全量回放。

这些文件只能进入受控 artifact storage，并在 DB 中记录 URI、hash、size、生成程序版本、parser_status、retention tier。

### 8.3 当前 qe_archive 满足度

当前主分支已经具备较多基础表，方向基本正确：

| 当前能力 | 满足度 | 说明 |
|---|---|---|
| `qe_archive.run` / `run_source` | 满足一部分 | 可记录 run 和来源映射 |
| `run_config` | 基本满足 | 可存 canonical_config、raw_config、config hash；需要强化统一配置 schema |
| `run_reproducibility_manifest` | 基本满足 | 已覆盖可复现等级、artifact manifest、缺失项；需要接入 live approval 语义 |
| `run_data_context` | 基本满足 | 可记录 PIT/Qlib/universe 上下文；需明确只作历史归档，不作包限制 |
| `run_metric` / `run_curve` | 满足 | 支撑指标和曲线分析 |
| `run_factor` / `run_factor_importance` / `run_factor_pair` | 满足一部分 | 支撑因子组合分析；需与候选包 snapshot 对齐 |
| `run_model_trial` / `run_model_training_metric` | 满足一部分 | 支撑模型和训练分析；需保证 seed/超参捕获完整 |
| `run_artifact` | 方向正确 | 只存 URI/hash/size/parser 状态，不存大文件 |
| `optimization_candidate` | 可复用但非候选策略包 | 更偏未来优化建议，不能替代用户显式候选策略包 |
| `paper_v2_*` extension | 支撑 Paper v2 归档 | 与 Paper v2 runtime mirror 相关；不能替代 StrategyPackage/candidate 边界 |
| `factor_value` | 需加边界 | 只可作为受控、分区、可清理的运行分析表；不得变成全量因子文件仓库 |

### 8.4 关键缺口

1. 缺少独立、持久、用户显式创建的 candidate_strategy_package 表族。
2. 现有 SOTA leaderboard 仍依赖 `qe_sota_registry`、`qe_evolution_loops.is_sota` 和自动候选语义，与用户要求冲突。
3. `strategy_pkg.promotion_review` 只保存 review ledger 和指标快照，不足以作为候选策略包快照。
4. `strategy_pkg.promotion_review.source_type` 当前只允许 `qe_experiment`、`qe_evolution_loop`，还不支持 candidate_strategy_package source。
5. `StrategyPackageSource.SourceType` 当前只支持 QE experiment / QE evolution loop，还不支持 candidate_strategy_package。
6. 删除 QE 实验/Loop 的流程需要显式验证不会删除 qe_archive、candidate、StrategyPackage。
7. HMM runtime、rolling trainer、runtime profile 与 QE HMM 自动同步尚需按平台能力落地。
8. 统一配置层仍需把 QE Qlib 执行和 Paper v2 DB/miniQMT 执行收敛到同一策略语义。

---

## 9. 候选策略包设计

### 9.1 命名替换

旧模块：SOTA Hall / SOTA 殿堂
新模块：Candidate Strategy Packages / 候选策略包

旧的 SOTA 语义中，自动发现、is_sota、legacy registry、manual review 混在一起。新设计中：

- 只有用户点击“加入候选策略包”才会创建候选。
- QE 演进过程可以推荐，但不得自动创建 candidate。
- 候选不代表 approved、不代表 paper-enabled、不代表 live-ready。
- 候选是独立快照，不依赖原 QE 实验/Loop 的存活。

### 9.2 推荐表族

建议新增或重构为如下表族，schema 可位于 `strategy_pkg`：

```text
strategy_pkg.candidate_strategy_package
strategy_pkg.candidate_strategy_package_source
strategy_pkg.candidate_strategy_package_config_snapshot
strategy_pkg.candidate_strategy_package_metric_snapshot
strategy_pkg.candidate_strategy_package_artifact_ref
strategy_pkg.candidate_strategy_package_version
strategy_pkg.candidate_strategy_package_audit
```

最小可落地版本可以先合并为 2-3 张表，但必须满足下列字段：

| 字段 | 要求 |
|---|---|
| candidate_id | 稳定 ID，例如 csp_ 前缀 |
| candidate_version | 候选版本，默认 1；候选修改生成新版本 |
| source_type | qe_experiment / qe_evolution_loop / candidate_strategy_package |
| source_id | 原始来源 ID，允许来源删除后仍保留字符串 |
| archive_run_id | 可选，指向 qe_archive.run；归档删除需独立确认 |
| snapshot_config_json | 因子、模型、训练、HMM 配置引用、策略配置、执行配置快照 |
| factor_manifest_json | 因子清单、顺序、版本、hash |
| model_manifest_json | 模型入口、权重引用、seed、超参、训练窗口、缺失项 |
| strategy_manifest_json | 日频策略、分钟执行、尾盘处理、风控/成本配置 |
| metric_snapshot_json | 创建候选时的核心指标快照 |
| artifact_refs_json | URI/hash/size，不保存文件本体 |
| completeness_json | 资产完整性和缺失项，不作为选股/模拟盘限制 |
| eligibility_json | selection/paper/live 的可用性解释 |
| status | active / deleted；不要加入过多业务状态 |
| created_by / created_at | 用户显式操作审计 |
| deleted_by / deleted_at | 独立删除审计 |

### 9.3 候选创建规则

从 QE experiment 加入候选：

```text
QE experiment detail page
  -> Add to Candidate Strategy Packages
  -> 读取 QE source + qe_archive run + artifact manifest
  -> 生成 candidate snapshot
  -> 返回 candidate_id
```

从 QE loop 加入候选：

```text
QE loop detail page
  -> Add to Candidate Strategy Packages
  -> 读取 loop config/metrics + qe_archive run
  -> 生成 candidate snapshot
  -> 返回 candidate_id
```

从已有 candidate 生成新 candidate：

```text
candidate detail page
  -> Clone / Modify Candidate
  -> 修改因子/模型/策略配置
  -> 保存为新的 candidate_version 或新的 candidate_id
```

### 9.4 候选到 StrategyPackage

```text
candidate_strategy_package
  -> Validate asset references
  -> Create StrategyPackage source_type=candidate_strategy_package
  -> Create package version v1
  -> Default model_weight_policy=backtest_default
  -> selection/paper eligibility view
```

从候选创建正式包后：

- 删除原 QE 实验不影响 candidate。
- 删除 candidate 不影响已经创建的 StrategyPackage；只能在 UI 中显示 source candidate deleted。
- 修改 candidate 不覆盖已经创建的 StrategyPackage。

---

## 10. StrategyPackage 版本与来源

### 10.1 source_type 扩展

`StrategyPackageSource.SourceType` 必须扩展：

```text
qe_experiment
qe_evolution_loop
candidate_strategy_package
```

### 10.2 不可变版本

以下操作必须创建新的 StrategyPackage 版本，不得覆盖原包：

- 因子新增、删除、替换、顺序调整。
- 因子表达式或因子版本变化。
- 模型结构、模型入口或超参数变化。
- 训练窗口、label horizon、数据频率变化。
- 日频策略、分钟执行策略、尾盘处理策略变化。
- HMM 使用策略从不用改为使用，或 profile policy 改变。
- 事件信号 policy 改变。

以下操作可以作为 revision 挂在同一包版本，不覆盖原资产：

- 同一配置下的 rolling weight revision。
- 同一 runtime profile 下的每日 HMM prediction revision。
- 同一 stock_pool_profile 下的每日股票池结果。

### 10.3 能力与审批

- asset_validated 后即可支持选股和 AIstock 模拟盘。
- live_approval_reserved 只预留，不在实盘未开发前启用。
- 未来实盘审批必须校验：完整 seed/超参、滚动训练结果、模拟盘验证、人工审批、miniQMT adapter 验收。

---

## 11. 统一配置层

### 11.1 目标

QE 实验和 Paper v2 必须共用统一配置层。建议定义 `UnifiedStrategyRuntimeConfig`，至少包含：

| 配置域 | 字段示例 |
|---|---|
| factor_config | factor_ids、factor_order、transform、winsorize、normalize、missing_policy |
| model_config | model_family、model_entrypoint、hyperparams、seed、label_horizon、weight_policy |
| training_config | train_start/end、valid_start/end、rolling_window_years、retrain_schedule |
| scoring_config | score_direction、rank_method、topk、capacity、tie_breaker |
| hmm_config | use_hmm、hmm_profile_id、version_policy、feature_set、fallback_policy |
| stock_pool_config | stock_pool_profile_id、ST/PIT policy、tradability policy |
| event_signal_config | event_signal_profile_id、asof_policy、signal_merge_policy |
| daily_strategy_config | rebalance_frequency、holding_period、position_sizing、risk_budget |
| minute_execution_config | algo_name、bar_freq、participation_rate、limit_policy、suspend_policy |
| tail_handling_config | close_auction_policy、tail_risk_policy、cancel_policy |
| cost_config | commission、slippage、tax、impact_model |
| adapter_config | qlib_bin / paper_db / miniqmt / live_broker adapter parameters |

### 11.2 适配器原则

```text
UnifiedStrategyRuntimeConfig
  -> QE adapter: Qlib bin + historical snapshot
  -> Paper v2 adapter: DB/as-of market data + Paper broker
  -> miniQMT adapter: miniQMT quote/order/fill APIs
```

执行逻辑必须一致，差异只允许存在于 adapter：

- 数据读取方式不同，但 as-of 语义一致。
- 订单下发方式不同，但分钟线执行策略一致。
- 运行状态和错误处理不同，但策略决策等价。
- QE 可以使用历史 HMM snapshot；Paper/miniQMT 必须使用 runtime HMM prediction。

### 11.3 配置归档

QE 数仓保存 canonical config 和 hash。StrategyPackage 保存包内策略配置快照和引用。Paper v2 runtime 保存实际激活的 runtime profile/version。三者通过 config_sha256、package_version_id、runtime_profile_version_id 对齐。

---

## 12. UI / API 流程

### 12.1 UI 改造

| 页面 | 改造 |
|---|---|
| QE experiment detail | 新增“加入候选策略包”按钮，显示 asset completeness 和 archive status |
| QE loop detail | 新增“加入候选策略包”按钮，显示 loop metrics、config hash、archive run |
| SOTA Hall | 改名为“候选策略包”，移除自动 SOTA 晋升语义 |
| Candidate detail | 展示配置快照、指标快照、artifact refs、缺失项、创建正式包入口 |
| StrategyPackage detail | 展示来源 candidate/QE、包版本、默认权重、rolling revisions、selection/paper/live eligibility |
| QE archive detail | 展示归档证据、artifact refs、组合分析，不提供大文件内联存储 |

### 12.2 API 建议

```text
POST /api/strategy-packages/candidates/from-qe-experiment
POST /api/strategy-packages/candidates/from-qe-loop
POST /api/strategy-packages/candidates/{candidate_id}/clone
GET  /api/strategy-packages/candidates
GET  /api/strategy-packages/candidates/{candidate_id}
DELETE /api/strategy-packages/candidates/{candidate_id}
POST /api/strategy-packages/from-candidate/{candidate_id}
```

### 12.3 自动候选禁用

现有 leaderboard 中基于 `is_sota = TRUE` 自动构造 AUTO_CANDIDATE 的路径必须改造：

- 可保留历史展示为 legacy_sota 或 archived_sota。
- 不得把 `qe_evolution_loops.is_sota` 自动写入 candidate_strategy_package。
- 不得把 AUTO_CANDIDATE 当作可创建 Paper v2 包的来源。
- 所有 candidate 必须有 created_by、created_at、manual_action=true。

---

## 13. 删除与持久化策略

### 13.1 生命周期分离

```text
QE source lifecycle
  != QE archive lifecycle
  != candidate strategy package lifecycle
  != Paper v2 StrategyPackage lifecycle
  != external artifact retention lifecycle
```

### 13.2 删除规则

| 删除动作 | 允许影响 | 禁止影响 |
|---|---|---|
| 删除 QE experiment/loop | 删除原始任务运行目录、临时日志、可重建缓存 | 删除 qe_archive rows、candidate rows、StrategyPackage rows |
| 删除 candidate | 软删除或独立删除 candidate | 删除原 QE、qe_archive、已创建 StrategyPackage |
| 删除 StrategyPackage | 退役/软删除包版本 | 删除 candidate、QE archive、原 QE source |
| 删除 QE archive | 必须独立权限和确认 | 不能由 QE source cleanup 触发 |
| 清理 external artifacts | 按 retention 和引用计数执行 | 不能破坏 live/paper/candidate/package 正在引用的资产 |

### 13.3 技术约束

- candidate 表对 QE source 不使用 ON DELETE CASCADE。
- StrategyPackage 对 candidate/source 不使用 ON DELETE CASCADE。
- candidate 创建时必须复制关键配置和指标快照，而不是只保存 source_id。
- 删除 QE source 前必须检查 candidate/package/archive 引用，输出保护报告。
- 所有删除流程必须有 dry-run、confirm text、audit log。

---

## 14. 跨模块验收矩阵

| 场景 | 必须验证 |
|---|---|
| QE experiment -> archive | 配置、seed、超参、指标、artifact refs 入仓；大文件只存 URI/hash |
| QE loop -> archive | loop config/metrics/run mapping 完整；删除 loop 后 archive 仍可查 |
| QE experiment -> candidate | 用户点击后生成独立 candidate；删除 experiment 后 candidate 仍可查 |
| QE loop -> candidate | 用户点击后生成独立 candidate；删除 loop 后 candidate 仍可查 |
| candidate -> StrategyPackage | source_type=candidate_strategy_package；包资产完整；默认 backtest weight |
| StrategyPackage -> selection | ST/PIT 由 Paper v2 runtime 生成；包内无 ST/PIT 数据 gate |
| StrategyPackage -> AIstock paper | HMM 使用 runtime prediction；执行策略与 QE 配置等价 |
| rolling retrain -> paper | 新 weight_revision 可选择；不覆盖原包和回测权重 |
| missing seed old experiment -> paper | 可进入模拟盘但 live approval blocked |
| HMM QE update -> Paper v2 | 新 HMM 版本自动进入 runtime registry，可预测、可滚动训练 |
| event signal future path | 策略只引用 profile；runtime 使用 as-of signal |
| miniQMT future path | 与 Paper v2 共享配置和执行逻辑，只替换 broker/data adapter |
| deletion protection | 删除 QE source 不删除 archive/candidate/package；独立删除需显式确认 |

---

## 15. 当前代码改造重点

### 15.1 P0: 边界与 schema

1. 新增 candidate_strategy_package 表族或把 `promotion_review` 重构为 durable candidate snapshot。
2. 扩展 StrategyPackage source_type：增加 `candidate_strategy_package`。
3. 移除/禁用 `is_sota = TRUE` 自动候选语义。
4. 给 QE source 删除流程增加 archive/candidate/package 引用保护。
5. 明确 qe_archive artifact policy：DB 不存大文件，仅存 URI/hash/summary。

### 15.2 P1: UI/API

1. QE experiment detail 增加“加入候选策略包”。
2. QE loop detail 增加“加入候选策略包”。
3. SOTA Hall 改名为候选策略包，调整文案和业务语义。
4. Candidate list/detail/create-package API 和页面。
5. StrategyPackage detail 展示 candidate 来源、包版本、rolling weight revisions、live approval blockers。

### 15.3 P2: 统一配置和运行时

1. 定义 `UnifiedStrategyRuntimeConfig` schema。
2. QE executor 与 Paper v2 executor 共用策略配置解析和校验。
3. 日频策略、分钟线执行策略、尾盘处理策略提取为共享实现或共享 contract。
4. Qlib / Paper DB / miniQMT adapter 做差异隔离。
5. 增加跨适配器 contract tests。

### 15.4 P3: HMM / ST/PIT / 事件信号平台能力

1. HMM runtime registry、rolling trainer、predictor、profile API。
2. QE HMM 版本自动同步 Paper v2 registry。
3. Paper v2 stock_pool_profile 生成最新 ST/PIT runtime universe。
4. 事件信号 profile / as-of runtime 接口预留。
5. Selection/Paper/miniQMT 均通过 platform services 消费这些能力。

### 15.5 P4: 验证与发布

1. 新增 candidate lifecycle backend tests。
2. 新增 QE deletion protection tests。
3. 新增 archive independence tests。
4. 新增 StrategyPackage source_type candidate tests。
5. 新增 L3：QE -> archive -> candidate -> package -> selection -> paper。
6. 新增 L4：删除 QE source 后 candidate/package/archive 仍可用。
7. 新增未来 L5：miniQMT adapter dry-run/sandbox。

---

## 16. 与旧方案的冲突处理

| 旧方案或当前实现 | 新边界 |
|---|---|
| SOTA Hall / AUTO_CANDIDATE | 改为用户显式创建的候选策略包 |
| `qe_evolution_loops.is_sota` 自动候选 | 只可作为历史展示或推荐信号，不可直接进入 candidate |
| promotion_review 作为候选池 | 只能作为 review/audit ledger；候选需要持久 snapshot |
| ST/PIT 作为包门槛 | 改为 Paper v2 stock_pool runtime 能力 |
| HMM 历史快照用于运行时 | 禁止；运行时必须使用 Paper v2 HMM predictor |
| 缺 seed/超参数阻止模拟盘 | 禁止；只阻止未来 live approval |
| 数仓保存完整 artifact | 禁止；DB 只存 refs/hash/summary |
| 修改包覆盖原包 | 禁止；必须创建新版本 |
| QE 删除级联影响候选/包/数仓 | 禁止；生命周期分离 |

---

## 17. 最终结论

本文确定 Paper v2 / QE 集成的正式边界：

- QE 负责实验和历史回测证据。
- QE 数仓负责轻量归档和组合分析。
- 候选策略包负责用户显式选择的持久候选快照。
- StrategyPackage 负责版本化策略定义和资产引用。
- Paper v2 平台负责 HMM、ST/PIT、事件信号、滚动训练、运行时选股/模拟盘/未来 miniQMT 适配。

完成本文方案后，验收标准不是单个模块可运行，而是完整链路可走通：

```text
QE 配置 -> QE 执行 -> QE 数仓 -> 候选策略包 -> StrategyPackage -> 选股 -> AIstock 模拟盘 -> miniQMT 未来路径
```

任何实现如果让 StrategyPackage 携带 HMM/ST/PIT/事件信号数据本体、让 SOTA 自动晋升、让 QE 删除破坏候选/包/数仓、或让 QE 与 Paper v2 使用不同策略执行语义，均视为偏离本文边界。

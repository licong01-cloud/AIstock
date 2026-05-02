# QE 实时实验数据仓库顶层设计方案

> 日期：2026-05-02  
> 状态：顶层设计草案 v2.1，已吸收第一轮讨论确认  
> 文档位置：`docs/architecture/qe_realtime_experiment_warehouse_top_level_design_20260502.md`  
> 适用范围：QuantEvolver / QE 单次实验、自动演进 loop、自定义演进 loop、策略演进 loop、Multi-Alpha 训练与回测、后续模型调参与因子组合自动化探索。

## 1. 结论先行

AIstock 需要建立独立的 `qe_archive` 实时实验数据仓库，作为 QE 研究实验的长期记忆、对比分析层、图表数据源和未来自动演进决策源。

核心判断：

1. `qe_experiments`、`qe_evolution_tasks`、`qe_evolution_loops` 已保存部分配置与结果，但它们是运行态/业务态表，不适合作为长期分析仓库。
2. 因子库、模型库、独立因子指标、因子评级、因子相关性表已经具备基础，但缺少“每一次组合实验”的完整归档层。
3. Qlib Recorder / MLflow 适合做实验追踪、artifact 管理和模型产物索引，但不能替代 AIstock 自己的量化研究数仓。
4. 日频且未处理涨跌停/停牌约束的 QE 回测数据只保留审计价值，应默认 `research_valid=false`，不得进入有效排行榜、因子评级或自动演进训练样本。
5. 模型调参和因子组合自动化探索应作为数仓长期目标，但要走“历史证据 + 规则约束 + Optuna/进化搜索 + LLM 解释”的混合架构，不再采用 LLM 随机改参数/随机换因子的方式。

目标形态：

```text
QE / Qlib / RDAgent 运行完成
  -> 实时 outbox event
  -> QEArchiveWorker 分层采集
  -> qe_archive 结构化数仓 + artifact store
  -> 图表 / 排行榜 / 因子历史 / 模型 trial 分析
  -> Agent 只读工具 / Optuna warm-start / 因子组合候选生成
  -> 下一轮自定义演进或受控自动演进
```

## 2. 当前系统已具备的数据基础

### 2.1 QE 运行态表

| 表 | 当前职责 | 数仓使用方式 |
|---|---|---|
| `qe_experiments` | 单次实验、演进子实验、因子列表、模型、策略、`data_split`、`custom_params`、`result_metrics`、workspace 路径 | 作为 run 主数据和配置快照主要来源 |
| `qe_evolution_tasks` | 演进任务、任务类型、演进模式、label horizon、策略演进配置、黑名单 | 作为 task 维度、演进目标和搜索空间来源 |
| `qe_evolution_loops` | 每个 loop 的 `config_json`、`metrics_json`、`agent_analysis`、`experiment_id` | 作为 loop 级 run、Agent 决策链和指标来源 |
| `qe_loop_factor_records` | loop 内因子参与和组合指标 | 作为 `run_factor` 历史补录和辅助来源 |
| `qe_loop_model_records` | loop 内模型、参数、训练曲线、过拟合/收敛指标 | 作为 `run_model_trial` 和训练指标来源 |
| `qe_factor_experiment_metrics` | 因子参与某实验时的组合指标和部分交易统计 | 作为 factor-run 级历史表现来源 |

这些表继续保留为 QE 的运行态/业务态表，不直接承担长期 OLAP、图表和自动演进训练样本职责。

### 2.2 因子与模型基础表

| 表 | 当前职责 | 数仓使用方式 |
|---|---|---|
| `aistock_factor_catalog` | 因子主数据、source、表达式、标签、实现信息、可用性、asset bundle | `factor_catalog_id` 维表，不复制可变全文，只记录快照 hash 和关键字段 |
| `aistock_model_catalog` | 模型主数据、模型类型、配置、feature schema、artifact、workspace | `model_catalog_id` 维表，关联每个 run 的模型族和模型资产 |
| `aistock_factor_metrics` | 因子独立指标权威表，包含 IC、Rank IC、ICIR、年化 ICIR、方向、最佳周期、覆盖率、换手等 | 构建因子评级、因子候选池、因子历史质量视图的权威输入 |
| `aistock_factor_monthly_ic` | 月度 IC、Rank IC、EWMA、符号一致性、趋势、OOS/IS 等 | 因子稳定性、衰退、近期有效性判断输入 |
| `qe_factor_classification` | 因子语义分类、维度、信号机制、数据源分组 | 因子组合多样性和解释层输入 |
| `qe_factor_official_ratings` | 因子正式评级、维度得分、硬门禁 | 因子候选池过滤和优先级输入 |
| `qe_factor_correlations` | 因子间 Spearman/EWMA 相关性 | 因子组合去冗余、相关性惩罚、组合多样性输入 |

设计原则：基础表是“因子/模型独立评价层”，`qe_archive` 是“每一次组合实验评价层”。两者通过 catalog id、snapshot date、rule version、hash 关联，不相互替代。

### 2.3 Qlib Recorder / MLflow 现状

当前 `scripts/qrun_limit.py` 和 `scripts/qrun_limit_minute.py` 已在运行时设置 `MLFLOW_TRACKING_URI`，默认指向当前 workspace 下的 `mlruns`，并通过 Qlib Recorder 保存 `config`、`pred.pkl`、`label.pkl`、`params.pkl`、portfolio analysis 等对象。

Qlib Recorder 官方设计是 `ExperimentManager -> Experiment -> Recorder`，并提供基于 MLflow 的 `MLflowExpManager`。MLflow Tracking 将 run metadata、params、metrics 保存在 backend store，将模型、图片、Parquet、pkl 等大对象保存在 artifact store。

因此本方案采用：

```text
Qlib Recorder / MLflow
  = 通用实验追踪、模型/预测/artifact 记录层

qe_archive
  = AIstock 量化研究权威分析仓库、图表/排行榜/因子历史/自动演进证据层
```

## 3. 数仓顶层目标

### 3.1 短期目标

1. 每次单次实验或每个 loop 完成后，秒级到分钟级自动入仓。
2. 记录完整配置快照：因子列表、模型、超参、策略参数、数据切分、回测时间、执行算法、数据版本、成本参数、HMM/黑名单/股票池等。
3. 记录完整指标数据：IC、Rank IC、ICIR、收益、回撤、Sharpe/IR、换手、胜率、交易统计、执行诊断、分钟成交约束等。
4. 记录每个因子参与每次实验的记录、组合上下文、因子在模型中的重要性或贡献。
5. 记录非结构化 artifact manifest，支持复现和后续深度分析。
6. 支持实时图表、排行榜、横向/纵向对比、因子历史、模型 trial 历史。

### 3.2 中期目标

1. 支持模型超参数试验的统一 trial 仓库，替代仅从 `qe_evolution_loops` 读取历史 trial 的局限。
2. 支持因子组合评分：独立因子质量、分类多样性、相关性惩罚、历史共现收益、模型适配性、执行可行性、近期衰退风险。
3. 支持 `QEFactorComboOptimizer` 从数仓生成候选组合，再输出到自定义演进 batch。
4. 支持 Optuna/TPE 从 `qe_archive` warm-start，并用统一目标函数筛选下一轮参数。
5. 支持 LLM agent 通过受控只读工具读取数据仓库证据，生成实验假设和解释，不直接随机改生产策略。

### 3.3 长期目标

1. 恢复 QE 自动演进，但从“LLM 随机探索”升级为“数仓驱动的受控自动研究系统”。
2. 对每个模型族、label horizon、因子类型、执行算法形成可持续更新的经验库。
3. 形成自动 research priority queue：哪些组合值得优先复测、调参、扩展因子、打包为 StrategyPackage 或进入 Paper v2。
4. 让 LLM agent 能做综合研究分析：成功/失败模式、因子衰退、模型适配、过拟合风险、下一步实验建议。

## 4. 非目标与边界

1. 不把 `qe_archive` 设计为 QE 运行态替代品；运行态表继续承担任务编排、状态同步和 UI 操作职责。
2. 不用 MLflow Model Registry 替代 AIstock StrategyPackage；StrategyPackage 仍是冻结策略资产的权威中心。
3. 不把所有 workspace 文件直接塞进 PostgreSQL；大文件只保存 manifest、hash、URI。
4. 不让 LLM 直接自由 SQL 查询和写入生产库；必须通过只读工具、白名单视图、预算和审计。
5. 不把无涨跌停/停牌处理的日频策略回测纳入有效研究排名。
6. 不要求第一阶段迁移到集中式 MLflow Server；先兼容本地 `mlruns`，但预留集中化接口。

## 5. 数据采集范围

### 5.1 Run 身份与血缘

每一次真实执行必须生成一个不可变 `run_id`。

必须记录：

- `run_id`：一次真实执行。
- `logical_experiment_id`：逻辑实验或 loop，不随 rerun 改变。
- `attempt_no`：同一 logical source 的第几次执行。
- `source_type`：`single_experiment`、`auto_evolution_loop`、`custom_evo_loop`、`strategy_evo_loop`、`multi_alpha_group`、`multi_alpha_combined`、`manual_backfill`。
- `source_experiment_id`、`qe_task_id`、`qe_loop_id`、`loop_index`。
- `parent_run_id`：Multi-Alpha group 到 combined、rerun 到原 run、策略演进到模型来源等。
- `status`、`failure_type`、`error_digest`。
- `node_id`、workspace、WSL command、runner version。
- `created_at`、`started_at`、`completed_at`、`archived_at`。

### 5.2 配置快照

每个 run 的 canonical config 必须独立保存，不能依赖未来会变化的 catalog 或 UI 默认值。

必须纳入：

- 因子列表：名称、顺序、source、catalog id、实现版本、代码 hash、分组/alpha group、角色。
- 模型配置：model id、model catalog id、model type、超参、模型代码 hash、feature schema、label horizon。
- 策略配置：strategy id、TopK、n_drop、换仓参数、调仓频率、策略代码 hash。
- 数据切分：`train_start/end`、`valid_start/end`、`test_start/end`、`backtest_start/end`。
- 数据上下文：Qlib 数据目录、snapshot date、benchmark、universe、分钟线版本、日线版本、数据截止日。
- 交易约束：手续费、滑点、涨跌停处理、停牌处理、黑名单、行业黑名单、stock pool、min/max position。
- 执行算法：日频/分钟频率、execution algo、V24/V25/TailTWAP 参数、tail unfilled policy。
- HMM：snapshot id、preset、coefficient hash、是否启用。
- 多 Alpha：group 定义、group prediction、group/meta weight、combined prediction 来源。
- 环境：git commit、runner script hash、conda/env、Qlib version、MLflow tracking URI。

配置字段必须带 provenance：

```text
submitted_request
qe_experiments
qe_evolution_tasks.strategy_evo_config
qe_evolution_loops.config_json
conf.yaml
mlruns_recorder
workspace_file
inferred_default
legacy_unknown
```

### 5.3 指标与曲线

必须结构化保存：

- 预测指标：IC、Rank IC、ICIR、Rank ICIR、分 horizon 指标。
- 收益指标：年化收益、超额收益、含成本/不含成本收益、benchmark return。
- 风险指标：最大回撤、波动率、Calmar、drawdown duration。
- 风险调整指标：Sharpe、Information Ratio、Sortino。
- 稳定性指标：日胜率、周胜率、滚动 IC、滚动 Sharpe、月度稳定性。
- 交易指标：换手率、交易次数、平均持仓天数、胜率、盈亏比。
- 执行指标：成交率、未成交率、限价阻断、停牌阻断、缺分钟线、平均滑点、尾盘未成交。
- 过拟合指标：IS/OOS gap、train/valid loss gap、Deflated/Probabilistic Sharpe 预留字段、PBO/CSCV 预留字段。
- 曲线：NAV、benchmark NAV、excess NAV、daily return、drawdown、IC/RIC 序列、turnover、loss curve。

### 5.4 因子参与与重要性

每个因子在每次实验中必须有记录：

- 是否参与、因子顺序、所属 group、角色。
- 实验整体指标快照。
- 因子单独指标快照：从 `aistock_factor_metrics`、`aistock_factor_monthly_ic`、`qe_factor_official_ratings`、`qe_factor_classification` 拷贝关键摘要。
- 与其他因子的相关性摘要：最大相关、平均相关、高相关 pair 数。
- 组合上下文：同组因子列表、同组平均评级、组合相关性、历史共现得分。
- 模型训练中的重要性/贡献。

因子重要性记录有分析价值，但必须区分模型类型：

| 模型类型 | 建议记录 | 注意事项 |
|---|---|---|
| LGB/XGB/CatBoost | gain、split、cover、permutation importance、SHAP 摘要 | tree gain 易偏向高基数/高频特征，需保存 method |
| 线性/ElasticNet | coefficient、standardized coefficient、方向、显著性 | 必须标准化后才可跨因子比较 |
| LSTM/PTNN/深度模型 | permutation、occlusion/ablation、gradient x input、integrated gradients、DeepSHAP、时间步级 attribution | 不记录原始神经网络权重作为因子“权重”；需要保存 feature order、lookback window、attribution scope |
| Multi-Alpha | group weight、meta model weight、group prediction correlation | 高价值信号，应优先记录 |

趋势价值：长期记录每个因子在不同模型、不同 label horizon、不同市场阶段中的重要性变化，可以识别因子衰退、模型偏好和组合冗余。

第一轮讨论修正：因子重要性和训练表现需要覆盖所有 QE 实验中使用过的模型，不能只覆盖树模型。树模型只是 native importance 最容易落地的一类。由于当前最佳回测模型包含 LSTM，第一阶段必须至少为 LSTM 提供模型无关 attribution 和深度模型 attribution 的基础支持：

- 全模型通用：记录因子参与、feature order、训练/验证/测试指标、prediction performance、permutation importance、inference-time ablation/drop-column impact。
- LSTM 优先：记录 lookback window、特征维度映射、训练/验证 loss 曲线、early stop、gradient x input、integrated gradients 或 occlusion attribution。
- 树模型补充：记录 gain/split/cover/SHAP 等 native importance。
- 线性模型补充：记录标准化 coefficient。
- 所有方法都写入 `run_factor_importance.importance_method`，避免把不同模型的“权重”误认为同一含义。

### 5.5 组合、协同与候选决策

为了后续因子组合自动化，需要记录：

- run 级 factor set hash。
- 组合内平均独立指标、平均评级、评级分布。
- 组合内相关性分布：mean/max/95pct correlation。
- pair/triple 共现表现：历史共现次数、平均收益、胜率、稳定性。
- 组合 novelty：与历史高分组合的 Jaccard 距离、与近期实验的重复度。
- 候选生成来源：LLM、Optuna、规则生成、手工自定义演进、回填。
- 候选被执行/拒绝/排队的原因。

## 6. 存储分级方案

### 6.1 分级原则

前端图表和 agent 分析必须默认读取结构化热数据，不在请求时解析 pkl、超大 JSON 或 workspace 文件。

| 层级 | 存储对象 | 介质 | 访问特点 | 保留策略 |
|---|---|---|---|---|
| Hot DB | run、config 摘要、核心指标、因子参与、评分、最近曲线 | PostgreSQL/TimescaleDB，SSD/NVMe | 高频查询、排行榜、图表、agent 工具 | 长期保留 |
| Warm DB | 全量曲线、训练曲线、订单/成交/持仓摘要、执行事件 | TimescaleDB/分区表，SSD 优先 | 图表 drill-down、诊断分析 | 长期保留，可分区压缩 |
| Cold Detail | 大规模 predictions、positions、trades、factor exposure 明细 | Parquet 文件，SSD 冷区或 HDD | 批量分析、离线计算 | 长期保留，按 hash 去重 |
| Artifact Store | conf、日志、模型、pkl、报告、代码快照、workspace bundle | HDD/对象存储/NAS | 低频复现、手工分析 | 长期保留，可压缩 |
| Raw Payload | 原始 JSON、webhook payload、RDAgent response | PostgreSQL JSONB + 压缩归档 | 审计、补录、debug | 长期保留，可冷分区 |

### 6.2 SSD/HDD 判断

- PostgreSQL 主库、TimescaleDB 热表、索引、最近 N 天 artifact cache 应在 SSD/NVMe。
- 机械硬盘适合保存历史 artifact、pkl、模型权重、日志、报告、workspace 压缩包。
- 机械硬盘不适合作为实时图表和 agent 查询主路径；否则会在高频对比分析时拖慢体验。
- 大文件从 HDD 读取时应通过 manifest 定位，必要时异步加载或先复制到热缓存，不应阻塞前端主查询。

### 6.3 非结构化 artifact 目录建议

```text
qe_archive/
  artifacts/
    yyyy/mm/dd/
      run_id/
        manifest.json
        conf.yaml
        submit_request.json
        canonical_config.json
        qlib_results.json
        qlib_results_enhanced.json
        predictions.parquet
        pred.pkl
        model/
        logs/
        reports/
        feature_importance/
        code_snapshot/
        workspace_bundle.tar.zst
```

每个 artifact 必须有：`artifact_type`、`uri`、`sha256`、`size_bytes`、`created_at`、`storage_tier`、`source_system`、`metadata`。

第一轮讨论已确认：artifact 根目录使用 AIstock 仓库根目录下的 `qe_archive/artifacts/`，不是 `rdagent_assets/qe_archive/artifacts/`。`rdagent_assets` 继续保存 RD-Agent/QE 运行期资产；`qe_archive/artifacts` 作为长期归档 artifact store 入口。

重要边界：如果 QE/RD-Agent 运行在 WSL 或远程节点，AIstock Windows 后端不能直接通过路径转换、`\\wsl$` 或 DB `workspace_path` 读取 worker workspace。所有 artifact 归档必须通过节点 API、显式下载/同步任务或已归档到 `qe_archive/artifacts/` 的 AIstock-local cache 完成。

## 7. 数据库结构设计

### 7.1 Schema 选择

新增 schema：`qe_archive`。

保留旧 `archive` schema 作为历史通用归档骨架，不建议直接扩展为 QE 实验数仓。原因是旧 `archive.strategy_run_record` 更偏策略运行结果，且用 JSON 保存 equity/metrics，不能支撑因子、模型 trial、自动演进分析。

### 7.2 核心表总览

| 表 | 粒度 | 职责 |
|---|---|---|
| `qe_archive.run` | 一次真实执行 | run 主表、身份、状态、时间、有效性、核心关联 |
| `qe_archive.run_source` | run 到源系统 | 记录 qe_experiment、loop、task、mlflow、qlib recorder、workspace 的映射 |
| `qe_archive.run_config` | run 一行 | canonical config、hash、provenance |
| `qe_archive.run_data_context` | run 一行或多行 | 数据集版本、snapshot、universe、benchmark、PIT 数据约束 |
| `qe_archive.run_metric` | run + metric | 长表指标，支持新指标扩展 |
| `qe_archive.run_curve` | run + date + curve | NAV、drawdown、IC、turnover、loss 等曲线 |
| `qe_archive.run_factor` | run + factor | 因子参与、顺序、group、快照摘要 |
| `qe_archive.run_factor_importance` | run + factor + method | 模型内因子重要性/贡献/趋势 |
| `qe_archive.run_factor_pair` | run + pair | 本 run 内 pair 相关性、协同、共现上下文 |
| `qe_archive.run_model_trial` | run + model trial | 超参、目标值、Optuna trial、搜索空间、训练状态 |
| `qe_archive.run_model_training_metric` | run + epoch/step | 训练/验证 loss、收敛、过拟合 |
| `qe_archive.run_position` | run + date + symbol | 持仓权重和股票级收益摘要 |
| `qe_archive.run_order` | run + order | 订单意图和执行约束 |
| `qe_archive.run_trade` | run + trade | 成交明细和成本滑点 |
| `qe_archive.run_execution_event` | run + event | 涨跌停、停牌、缺数据、尾盘未成交等诊断 |
| `qe_archive.run_artifact` | run + artifact | 非结构化产物 manifest |
| `qe_archive.raw_payload` | run + payload | 原始 JSON/payload 审计 |
| `qe_archive.run_priority_score` | run + score_version | 排行榜和优化优先级 |
| `qe_archive.optimization_candidate` | candidate | 未来模型调参/因子组合候选 |
| `qe_archive.outbox_event` | event | 实时归档事件队列 |
| `qe_archive.archive_job` | job | 归档任务、重试、失败原因 |

### 7.3 核心字段草案

#### `qe_archive.run`

```sql
CREATE TABLE qe_archive.run (
    run_id                  TEXT PRIMARY KEY,
    logical_experiment_id   TEXT NOT NULL,
    source_type             TEXT NOT NULL,
    attempt_no              INTEGER NOT NULL DEFAULT 1,
    status                  TEXT NOT NULL,
    research_valid          BOOLEAN NOT NULL DEFAULT TRUE,
    exclusion_reason        TEXT,
    alpha_mode              TEXT,
    backtest_freq           TEXT,
    label_horizon           INTEGER,
    model_id                TEXT,
    model_catalog_id        BIGINT,
    strategy_id             TEXT,
    execution_algo          TEXT,
    factor_set_hash         TEXT,
    config_hash             TEXT,
    result_hash             TEXT,
    train_start             DATE,
    train_end               DATE,
    valid_start             DATE,
    valid_end               DATE,
    test_start              DATE,
    test_end                DATE,
    backtest_start          DATE,
    backtest_end            DATE,
    node_id                 TEXT,
    workspace_path          TEXT,
    created_at              TIMESTAMPTZ,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    archived_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

关键索引：

```sql
CREATE INDEX idx_qear_run_completed ON qe_archive.run (completed_at DESC);
CREATE INDEX idx_qear_run_model ON qe_archive.run (model_catalog_id, label_horizon, completed_at DESC);
CREATE INDEX idx_qear_run_valid_score ON qe_archive.run (research_valid, completed_at DESC);
CREATE UNIQUE INDEX uq_qear_logical_attempt ON qe_archive.run (logical_experiment_id, attempt_no);
```

#### `qe_archive.run_source`

```sql
CREATE TABLE qe_archive.run_source (
    id                    BIGSERIAL PRIMARY KEY,
    run_id                TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    source_system         TEXT NOT NULL,
    source_key            TEXT NOT NULL,
    source_subkey         TEXT,
    source_uri            TEXT,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_system, source_key, source_subkey)
);
```

示例：

- `source_system='qe_experiments'`，`source_key=experiment_id`
- `source_system='qe_evolution_loops'`，`source_key=loop_id`
- `source_system='mlflow'`，`source_key=mlflow_run_id`
- `source_system='qlib_recorder'`，`source_key=recorder_id`
- `source_system='workspace'`，`source_uri=workspace_path`

#### `qe_archive.run_config`

```sql
CREATE TABLE qe_archive.run_config (
    run_id              TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    config_version      TEXT NOT NULL,
    config_json         JSONB NOT NULL,
    config_hash         TEXT NOT NULL,
    provenance_json     JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`config_json` 中必须规范化为固定字段：`factors`、`model`、`strategy`、`data_split`、`execution`、`cost`、`universe`、`hmm`、`multi_alpha`、`environment`。

#### `qe_archive.run_metric`

```sql
CREATE TABLE qe_archive.run_metric (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    metric_key      TEXT NOT NULL,
    metric_value    DOUBLE PRECISION,
    metric_group    TEXT,
    metric_scope    TEXT,
    cost_mode       TEXT,
    horizon         TEXT,
    period_start    DATE,
    period_end      DATE,
    raw_key         TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (run_id, metric_key, metric_scope, cost_mode, horizon, period_start, period_end)
);
```

指标命名建议采用统一 taxonomy：

```text
prediction.ic.mean
prediction.rank_ic.mean
prediction.icir
return.annualized.with_cost
return.annualized.no_cost
risk.max_drawdown.with_cost
risk.volatility
risk.calmar
execution.fill_rate
execution.limit_block_count
training.valid_loss.final
overfit.is_oos_gap
```

#### `qe_archive.run_curve`

```sql
CREATE TABLE qe_archive.run_curve (
    run_id          TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    curve_type      TEXT NOT NULL,
    curve_date      DATE NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, curve_type, curve_date)
);
```

如果曲线数量增长明显，可将 `run_curve` 设为 TimescaleDB hypertable 或按 `curve_date`/月份分区。

#### `qe_archive.run_factor`

```sql
CREATE TABLE qe_archive.run_factor (
    id                          BIGSERIAL PRIMARY KEY,
    run_id                      TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    factor_name                 TEXT NOT NULL,
    factor_source               TEXT,
    factor_catalog_id           BIGINT,
    factor_order                INTEGER,
    group_name                  TEXT,
    role                        TEXT,
    factor_code_hash            TEXT,
    independent_metric_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    rating_snapshot             JSONB NOT NULL DEFAULT '{}'::jsonb,
    classification_snapshot     JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_snapshot        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (run_id, factor_name, COALESCE(group_name, ''))
);
```

#### `qe_archive.run_factor_importance`

```sql
CREATE TABLE qe_archive.run_factor_importance (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    factor_name         TEXT NOT NULL,
    factor_catalog_id   BIGINT,
    model_id            TEXT,
    model_catalog_id    BIGINT,
    importance_method   TEXT NOT NULL,
    scope               TEXT,
    raw_value           DOUBLE PRECISION,
    normalized_value    DOUBLE PRECISION,
    rank_in_run         INTEGER,
    direction           TEXT,
    epoch               INTEGER,
    stability_score     DOUBLE PRECISION,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (run_id, factor_name, importance_method, scope, COALESCE(epoch, -1))
);
```

#### `qe_archive.run_model_trial`

```sql
CREATE TABLE qe_archive.run_model_trial (
    run_id                  TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    model_id                TEXT,
    model_catalog_id        BIGINT,
    model_type              TEXT,
    label_horizon           INTEGER,
    search_method           TEXT,
    search_space_hash       TEXT,
    params_json             JSONB NOT NULL DEFAULT '{}'::jsonb,
    optuna_study_name       TEXT,
    optuna_trial_number     INTEGER,
    objective_key           TEXT,
    objective_value         DOUBLE PRECISION,
    objective_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
    training_status         TEXT,
    best_epoch              INTEGER,
    overfit_ratio           DOUBLE PRECISION,
    convergence_ratio       DOUBLE PRECISION,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

#### `qe_archive.run_priority_score`

```sql
CREATE TABLE qe_archive.run_priority_score (
    run_id                          TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    score_version                   TEXT NOT NULL,
    performance_score               DOUBLE PRECISION,
    risk_score                      DOUBLE PRECISION,
    predictive_score                DOUBLE PRECISION,
    execution_score                 DOUBLE PRECISION,
    stability_score                 DOUBLE PRECISION,
    novelty_score                   DOUBLE PRECISION,
    factor_quality_score            DOUBLE PRECISION,
    overfit_penalty                 DOUBLE PRECISION,
    correlation_penalty             DOUBLE PRECISION,
    turnover_penalty                DOUBLE PRECISION,
    score_total                     DOUBLE PRECISION,
    recommendation                  TEXT,
    reason_json                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    scored_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, score_version)
);
```

### 7.4 `score_total` 定义

`score_total` 是用于实时排行榜、优化优先级队列和单目标优化 warm-start 的统一标量分数。它不是替代明细指标，而是把多个维度压缩成一个便于排序的优先级值。

推荐第一版采用 0-100 分：

```text
score_total =
    performance_score
  + risk_score
  + predictive_score
  + execution_score
  + stability_score
  + novelty_score
  + factor_quality_score
  - overfit_penalty
  - correlation_penalty
  - turnover_penalty
```

原则：

- `score_total` 只用于默认排序和自动化候选优先级，不删除或隐藏原始指标。
- 所有分项分数必须同时保存，前端必须支持展开查看。
- `score_version` 必须版本化，评分规则调整后不覆盖旧分数。
- 日频无涨跌停/停牌处理实验的 `score_total=NULL`，不进入有效排行榜。
- 后续可以扩展为多套 score profile，例如 `research_score`、`paper_ready_score`、`factor_discovery_score`，或升级为多目标 Pareto 排序。

#### `qe_archive.outbox_event` 与 `qe_archive.archive_job`

```sql
CREATE TABLE qe_archive.outbox_event (
    event_id        BIGSERIAL PRIMARY KEY,
    event_type      TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    payload_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at      TIMESTAMPTZ,
    processed_at    TIMESTAMPTZ,
    error_message   TEXT
);

CREATE TABLE qe_archive.archive_job (
    job_id          TEXT PRIMARY KEY,
    event_id        BIGINT REFERENCES qe_archive.outbox_event(event_id),
    run_id          TEXT,
    job_level       TEXT NOT NULL,
    status          TEXT NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    summary_json    JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

## 8. 实时归档流程

### 8.1 Level A：快速入仓

目标：实验完成后 5-15 秒内可在排行榜和 run 列表看到。

写入：

- `run`
- `run_source`
- `run_config`
- `run_data_context`
- `run_factor`
- 核心 `run_metric`
- 初版 `run_priority_score`
- `raw_payload`

只依赖业务 DB 已有字段和小 JSON，不读取大 pkl。

### 8.2 Level B：增强入仓

目标：实验完成后 30-180 秒内补齐图表与诊断。

写入：

- enhanced metrics
- NAV / drawdown / IC / turnover 曲线
- 训练 loss 曲线
- model trial 和训练状态
- artifact manifest
- MLflow/Qlib recorder link
- 因子独立指标/评级/分类/相关性快照

### 8.3 Level C：深度入仓

目标：异步完成，不阻塞日常 UI。

写入：

- 持仓、订单、成交、执行事件
- predictions parquet
- feature importance / attribution
- pair synergy、组合相似度、候选评分更新
- workspace bundle hash

### 8.4 补偿扫描

每 1-5 分钟扫描：

- completed `qe_experiments` 无 `qe_archive.run`。
- completed `qe_evolution_loops` 无 `qe_archive.run`。
- `archive_job.status='failed'` 且可重试。
- workspace 中存在 artifact 但 `run_artifact` 无索引。
- `mlruns` 有 recorder 但缺少 `run_source` 关联。

## 9. 日频无涨跌停回测淘汰规则

QE 中无涨跌停/停牌处理的日频策略回测不应作为有效研究样本。

归档规则：

```text
if backtest_freq = 'day'
   and no authoritative limit/suspend handling:
       research_valid = false
       exclusion_reason = 'daily_backtest_without_limit_handling'
       score_total = null
       excluded from leaderboard / factor effectiveness / optimizer warm-start
```

保留原因：

- 审计历史实验。
- 回溯为何某些组合被排除。
- 未来如果补齐权威执行约束，可用原始配置做重新回测。

## 10. 与 MLflow / Qlib Recorder 的整合方式

### 10.1 推荐定位

MLflow/Qlib Recorder 负责：

- 保存参数、metrics、tags。
- 保存模型、预测、label、portfolio analysis、图片、报告等 artifact。
- 提供通用实验追踪 UI。
- 保存 Qlib 原生 recorder object。

`qe_archive` 负责：

- 标准化量化研究指标。
- 因子参与和组合上下文。
- 因子重要性趋势。
- 排行榜、优先级队列和自动演进证据。
- 与 AIstock 因子库、模型库、StrategyPackage、Paper v2 的业务衔接。

### 10.2 关联键

每个 run 尽量记录：

- `mlflow_experiment_id`
- `mlflow_run_id`
- `mlflow_tracking_uri`
- `mlflow_artifact_uri`
- `qlib_experiment_name`
- `qlib_recorder_id`
- `workspace_path`

如果历史记录无法确定 recorder id，也必须记录 `legacy_unknown` provenance，并保存 workspace/mlruns 扫描结果。

### 10.3 是否迁移集中式 MLflow Server

第一阶段不建议先做集中式 MLflow 改造。原因：

1. 当前本地 `mlruns` 已能支撑 Qlib Recorder。
2. 当前真正缺口是结构化量化分析仓库，而不是 MLflow UI。
3. 先建 `qe_archive` 可以直接改善前端、分析和自动演进。

中期可以考虑：

```text
MLFLOW_TRACKING_URI=http://127.0.0.1:xxxx
backend store = PostgreSQL or SQLite
artifact store = qe_archive/mlflow_artifacts or MinIO/NAS
```

但即使采用集中式 MLflow，`qe_archive` 仍然是 AIstock 量化分析的权威仓库。

## 11. 与因子库、模型库、独立指标、相关性分析的关系

### 11.1 因子库关系

`run_factor.factor_catalog_id` 关联 `aistock_factor_catalog.id`。

归档时必须复制快照摘要，而不是只存外键：

- factor name/source。
- factor type、data source、category、signal mechanism。
- is_available。
- factor code hash。
- official rating and score。
- independent metric snapshot。
- monthly recent trend snapshot。

原因：未来 catalog、评级规则、分类结果可能变化，但历史 run 的当时状态应可复现。

### 11.2 独立指标关系

`aistock_factor_metrics` 和 `aistock_factor_monthly_ic` 继续作为单因子独立评价的权威来源。

`qe_archive` 使用方式：

- 归档时复制 run 当时可见的 latest snapshot。
- 后续视图可动态 join 最新指标用于“当前再评价”。
- 区分 `factor_metric_at_run_time` 与 `factor_metric_latest`。

这样可以同时回答：

1. 当时为什么选择这个因子？
2. 今天看这个因子是否已经衰退？

### 11.3 相关性分析关系

`qe_factor_correlations` 是因子间相关性的权威输入。

`qe_archive` 需要生成组合级摘要：

- `combo_corr_mean`
- `combo_corr_max_abs`
- `combo_corr_p95_abs`
- `high_corr_pair_count`
- 每个因子的 `max_corr_with_combo`
- pair-level `run_factor_pair`

组合探索时把相关性作为硬约束和惩罚项：

```text
reject if max_abs_corr > hard_limit
penalize if avg_abs_corr > soft_limit
prefer category diversity and low redundancy
```

### 11.4 模型库关系

`run.model_catalog_id` 和 `run_model_trial.model_catalog_id` 关联 `aistock_model_catalog.id`。

必须区分：

- 模型族/配置模板。
- 本次训练产生的模型 artifact。
- StrategyPackage 中冻结的模型资产。
- Paper v2 当前可用模型状态。

`qe_archive` 只记录实验和训练结果，不直接提升模型到 StrategyPackage；提升仍需单独审批/打包流程。

## 12. 面向图表和分析的视图

### 12.1 核心视图

| 视图 | 用途 |
|---|---|
| `qe_archive.v_realtime_leaderboard` | 实时排行榜，默认过滤 `research_valid=false` |
| `qe_archive.v_priority_queue` | 值得下一轮优化/复测/打包的 run |
| `qe_archive.v_run_detail` | 实验详情页聚合配置、指标、artifact、来源 |
| `qe_archive.v_factor_history` | 每个因子参与过的实验和对应指标 |
| `qe_archive.v_factor_summary` | 因子在组合实验中的长期表现统计 |
| `qe_archive.v_factor_importance_trend` | 因子重要性随时间、模型、horizon 的趋势 |
| `qe_archive.v_model_trial_summary` | 模型超参 trial 历史和目标值 |
| `qe_archive.v_factor_pair_synergy` | 因子 pair 共现、相关性和组合收益 |
| `qe_archive.v_evolution_trace` | task/loop/Agent 决策链和结果趋势 |
| `qe_archive.v_excluded_runs` | 被排除的日频/失败/数据质量问题实验 |
| `qe_archive.v_data_quality` | 缺配置、缺指标、缺 artifact、缺 recorder 的归档质量 |

### 12.2 API 建议

```text
GET  /api/v1/qe-archive/runs
GET  /api/v1/qe-archive/runs/{run_id}
GET  /api/v1/qe-archive/runs/{run_id}/curves
GET  /api/v1/qe-archive/runs/{run_id}/artifacts
GET  /api/v1/qe-archive/factors/{factor_name}/experiments
GET  /api/v1/qe-archive/factors/summary
GET  /api/v1/qe-archive/factors/importance-trend
GET  /api/v1/qe-archive/factor-pairs/synergy
GET  /api/v1/qe-archive/model-trials
GET  /api/v1/qe-archive/priority-queue
POST /api/v1/qe-archive/agent-context
POST /api/v1/qe-archive/backfill/dry-run
POST /api/v1/qe-archive/backfill/execute
```

## 13. 数仓驱动模型调参自动化

### 13.1 推荐方式

模型超参优化不建议完全恢复旧 QE 自动演进模式。更好的方式是：

```text
用户选择模型族 / label horizon / 因子组合 / 预算
  -> qe_archive 检索相似历史 trial
  -> LLM 总结历史规律并收缩搜索空间
  -> Optuna/TPE 负责采样
  -> 自定义演进 loop 执行
  -> 结果实时入仓
  -> Optuna tell + priority score 更新
```

短期建议仍使用“自定义演进批次”作为执行载体，因为它更可控；旧自动演进可以等数仓稳定后再恢复。

### 13.2 数仓必须为调参保存的数据

- `model_type`
- `label_horizon`
- `factor_set_hash`
- `params_json`
- `search_space_hash`
- `search_method`
- `objective_key/objective_value`
- 多目标 `objective_json`
- train/valid/test/backtest split
- train loss / valid loss / best epoch / early stop
- overfit ratio / convergence ratio
- 运行资源：耗时、GPU、节点、失败原因
- 是否进入 StrategyPackage/Paper v2 后续路径

### 13.3 目标函数建议

不要只优化单一 IC。建议使用多目标或综合分：

```text
model_objective_score =
    0.25 * prediction_score
  + 0.25 * return_risk_score
  + 0.15 * stability_score
  + 0.15 * execution_score
  + 0.10 * factor_importance_stability
  + 0.10 * novelty_score
  - 0.20 * overfit_penalty
  - 0.15 * turnover_penalty
  - 0.10 * runtime_failure_penalty
```

第一阶段可以单目标化为 `score_total`，后续升级为多目标 Pareto。

### 13.4 模型调参执行场景与选项

推荐的近中期执行场景：

```text
用户选择模型族（例如 LSTM）/ label horizon / 因子组合 / 搜索预算
  -> qe_archive 检索相似历史 trial 和失败区域
  -> LLM 总结历史规律、解释风险、建议搜索空间边界
  -> Optuna/TPE 或其他优化器生成候选超参
  -> 通过自定义演进 batch 发起多轮 QE loop
  -> 每个 loop 完成后实时入仓
  -> 优化器更新 trial，priority score 更新
  -> 进入下一轮候选
```

这个方案的含义是：继续使用现有 QE 自定义演进/loop 执行通道承载实验，而不是立刻恢复旧的“自动演进 agent 自己决定下一轮”的模式。原因是自定义演进更可控、更容易审计、更容易限制预算，也更适合把 Optuna 的候选参数结构化地落到每一轮实验。

可选方案：

| 方案 | 自动化程度 | 优点 | 风险/代价 | 建议 |
|---|---:|---|---|---|
| A. 自定义演进 batch + Optuna/TPE | 中 | 可控、可审计、容易复用现有 QE 执行路径 | 需要用户或上层服务创建 batch | 近期推荐 |
| B. 恢复 QE 自动演进，但由数仓/优化器接管候选生成 | 高 | 更接近全自动研究系统 | 需要重构旧 LLM 随机决策，风控复杂 | 数仓稳定后做 |
| C. 独立 HPO 服务直接调度 QE run | 高 | 扩展性强，适合大规模并行 | 需要新增调度器、锁、资源预算和失败恢复 | 中长期可选 |
| D. 手工 LLM 辅助调参方案 | 低 | 人可控、最安全 | 效率较低，难以持续学习 | 适合早期验证 |
| E. Shadow mode 推荐，人工确认后执行 | 中 | 先验证推荐质量，避免自动化误伤 | 需要双流程维护 | 自动演进恢复前推荐 |

阶段建议：先用 A + E，等 `qe_archive` 有足够历史样本和质量门禁后，再评估 B 或 C。

## 14. 数仓驱动因子组合自动化

### 14.1 目标

有了数仓后，因子组合可以从“LLM 随机换因子”变成“证据驱动的候选生成”。

推荐模块：

```text
QEFactorComboOptimizer
  - FactorUniverseBuilder
  - FactorRatingService adapter
  - FactorDiversityService
  - FactorSynergyMiner
  - CandidateGenerator
  - CandidateScorer
  - CustomEvoLoopEmitter
  - PostRunLearner
```

### 14.2 候选生成方法论

组合选择应同时考虑：

1. 单因子质量：独立 IC、Rank IC、ICIR、稳定性、方向、覆盖率、换手、评级。
2. 因子分类：数据源、信号机制、持有期、行业暴露、价值/动量/质量/流动性/波动等维度。
3. 相关性控制：避免高度冗余，优先低相关且同向有效的组合。
4. 历史共现：pair 或 group 在历史 run 中是否有增益。
5. 模型适配：某些模型族对某类因子更有效，例如树模型处理非线性/交互更强。
6. 近期有效性：近期 3M/6M 是否衰退，月度 IC 趋势是否恶化。
7. 执行可行性：换手、涨跌停阻断、停牌暴露、分钟成交率。
8. 新颖性：保留少量探索因子，避免陷入局部最优。
9. 过拟合与多重检验：实验次数越多，对结果置信要求越高。

### 14.3 候选评分示例

```text
candidate_score =
    0.25 * avg_factor_predictive_score
  + 0.20 * avg_factor_stability_score
  + 0.15 * historical_synergy_score
  + 0.15 * category_diversity_score
  + 0.10 * model_fit_score
  + 0.10 * execution_feasibility_score
  + 0.05 * novelty_score
  - 0.20 * correlation_penalty
  - 0.15 * turnover_penalty
  - 0.15 * overfit_risk_penalty
```

### 14.4 推荐候选流程

```text
1. 从 aistock_factor_catalog 取可用因子
2. Join 最新独立指标、月度趋势、评级、分类、相关性
3. 应用硬门禁：可用性、覆盖率、近期严重负 IC、日频约束、数据完整性
4. 聚类/分类分桶：避免同质因子过多
5. 生成候选：高分核心因子 + 低相关补充因子 + 小比例探索因子
6. 用历史 run 计算相似组合表现和 pair synergy
7. 评分、去重、预算排序
8. 输出到自定义演进 loop
9. 执行后入仓，更新 synergy 和候选评分
```

### 14.5 学术/机构实践抽象

量化机构常见做法不是只看单因子收益，而是做组合层面的多约束选择：

- 因子 zoo 问题要求处理多重检验和数据挖掘偏差。
- 机器学习资产定价研究强调非线性和交互，但需要严格样本外验证。
- LASSO/ElasticNet/Double Selection、稳定性选择、正交化、聚类降冗余常用于大因子池筛选。
- 组合构建要约束相关性、换手、交易成本、行业/风格暴露和容量。
- 自动研究系统需要 evaluator 和客观评分函数，LLM 适合生成假设和代码/配置候选，不适合作为唯一评价者。

## 15. LLM Agent 使用数仓的方式

### 15.1 不建议方式

不建议：

- LLM 直接连接生产 DB 自由写 SQL。
- LLM 直接修改 QE 自动演进配置并提交。
- 把全部历史数据一次性塞进 prompt。
- 只按最近一个 loop 的自然语言反馈做下一轮决策。

### 15.2 推荐方式

提供受控只读 Agent Tools：

```text
get_run_leaderboard(filters)
get_run_detail(run_id)
get_factor_history(factor_name, filters)
get_factor_summary(filters)
get_factor_pair_synergy(factors, filters)
get_model_trials(model_id, filters)
get_similar_runs(run_id or candidate)
get_execution_failures(filters)
propose_factor_combo(seed_constraints)
propose_model_params(model_id, search_space)
explain_candidate(candidate_id)
```

所有工具必须：

- 只读。
- 有返回行数限制。
- 有过滤条件白名单。
- 自动排除 `research_valid=false`，除非显式请求诊断。
- 记录 `agent_query_audit`。
- 返回证据摘要和 run/factor ids，而不是全量原始表。

## 16. 历史数据补录方案

### 16.1 Dry-run Inventory

先只读盘点，不改数据：

- `qe_experiments`
- `qe_evolution_tasks`
- `qe_evolution_loops`
- `qe_factor_experiment_metrics`
- `qe_loop_factor_records`
- `qe_loop_model_records`
- `aistock_factor_metrics`
- `qe_factor_correlations`
- workspace / `mlruns` / RDAgent assets

输出缺失矩阵：

- 缺 factor list。
- 缺 data split。
- 缺 backtest start/end。
- 缺 result metrics。
- 缺 enhanced metrics。
- 缺 artifact。
- 缺 recorder id。
- 日频无涨跌停需 quarantine。

### 16.2 补录优先级

1. completed、分钟执行有效、非日频无约束的 QE 实验。
2. 自定义演进 loop。
3. 自动演进 loop。
4. Multi-Alpha group 与 combined run。
5. 失败实验的错误、日志和配置。
6. 日频无约束历史实验，仅标记 excluded。

### 16.3 历史缺失字段恢复

`data_split` 恢复优先级：

```text
qe_evolution_loops.config_json.data_split
qe_experiments.data_split
qe_evolution_tasks.strategy_evo_config.loops[].data_split
workspace/conf.yaml
RDAGENT_DEFAULT_DATA_SPLIT
legacy_unknown
```

任何推断值必须写入 `provenance_json`，不能伪装成原始提交值。

## 17. 实施阶段

### Phase 0：文档与规则确认

- 确认本方案中数据范围、日频淘汰规则、MLflow 定位、artifact 分层。
- 确认 `qe_archive` 是新 schema，不复用旧 `archive` schema。
- 确认第一阶段只做设计和 DDL，不改 StrategyPackage/Paper v2 资产。

### Phase 1：DDL 与基础 Repository

- 新增 `backend/db/init_qe_archive_schema.py`。
- 创建 `qe_archive` schema 和核心表。
- 实现 `QEArchiveRepository`。
- 定义 metric taxonomy、artifact type、source type、config schema version。

验证：DDL 幂等、索引存在、空库初始化通过。

### Phase 2：快速入仓

- 实现 `CanonicalConfigBuilder`。
- 实现 `QEMetricsExtractor`。
- 单次实验完成后写 outbox event。
- loop 完成后写 outbox event。
- Archive worker 写入 Level A 数据。

验证：完成一次 QE 实验后，`run`、`run_config`、`run_factor`、核心 `run_metric` 自动出现。

### Phase 3：视图与基础 API

- 实现 leaderboard、priority queue、run detail、factor history。
- 前端先做只读分析页面。
- 默认过滤 `research_valid=false`。

验证：可按模型、因子、日期、horizon、source type 对比历史实验。

### Phase 4：增强归档与 artifact manifest

- 解析 Qlib enhanced metrics。
- 收集 NAV/IC/turnover/drawdown 曲线。
- 收集 artifact manifest、MLflow/Qlib recorder link。
- 支持 artifact HDD 存储和热缓存。

验证：run 详情页可查看 artifact manifest 和曲线，不直接解析 pkl。

### Phase 5：模型 trial 与因子重要性

- 写入 `run_model_trial`、training metrics。
- 覆盖所有 QE 使用过的模型族，至少写入模型无关 attribution：permutation、inference-time ablation、prediction impact。
- LSTM 作为第一优先级深度模型支持：lookback/feature order、loss 曲线、occlusion/gradient x input/integrated gradients。
- LGB/XGB/CatBoost 补充 native feature importance：gain、split、cover、SHAP。
- Multi-Alpha 补充 group/meta weight 和 group prediction correlation。

验证：模型 trial 页面可按参数和目标值分析；因子重要性趋势可查询。

### Phase 6：历史补录

- Dry-run inventory。
- 分批 backfill。
- 数据质量报告。
- 日频无约束 run quarantine。

验证：历史 completed 有效实验大部分可在 leaderboard/factor history 中出现。

### Phase 7：数仓驱动优化

- Optuna 从 `qe_archive.run_model_trial` warm-start。
- `QEFactorComboOptimizer` 生成候选组合。
- LLM agent tools 只读接入；本阶段只预留接口和审计表，细节进入后续专项设计。
- 自定义演进 loop 执行候选。
- 自动演进恢复前先跑 shadow mode。

验证：候选有结构化理由、历史证据、预算约束、执行后可自动回写评分。

## 18. 风险与控制

1. 历史数据不完整：用 provenance 和 data quality 标记，不强行补假数据。
2. 日频历史污染：默认 excluded，只有显式诊断视图可看。
3. 大文件拖慢查询：前端只读 DB manifest，离线异步解析 artifact。
4. LLM 幻觉决策：LLM 只做解释和候选，不做最终评价；候选必须通过规则和数仓评分。
5. 多重检验过拟合：记录实验次数、相似度、搜索方法，预留 DSR/PBO/CSCV 统计。
6. 可变 catalog 导致历史不可复现：run 归档时复制因子/模型关键快照和 hash。
7. 与 StrategyPackage 混淆：归档 run 不是可交易资产，进入 Paper v2 仍需 StrategyPackage 审批。

## 19. 公开参考

- Qlib Recorder / `MLflowExpManager`：Qlib Recorder 由 ExperimentManager、Experiment、Recorder 组成，并提供基于 MLflow 的 `MLflowExpManager`。  
  https://qlib.readthedocs.io/en/stable/component/recorder.html
- MLflow Tracking：MLflow 将 run metadata、params、metrics 保存在 backend store，将模型、图片、Parquet、pkl 等大对象保存在 artifact store。  
  https://www.mlflow.org/docs/latest/ml/tracking  
  https://mlflow.org/docs/latest/self-hosting/architecture/overview/
- Optuna Study / TPE：Study 支持 `add_trial`、`enqueue_trial`、`ask`/`tell`，适合把历史 trial 注入并做受控搜索。  
  https://optuna.readthedocs.io/en/stable/reference/generated/optuna.study.Study.html  
  https://optuna.readthedocs.io/en/stable/reference/samplers/generated/optuna.samplers.TPESampler.html
- Harvey, Liu, Zhu, “... and the Cross-Section of Expected Returns”：因子 zoo 与多重检验问题。  
  https://academic.oup.com/rfs/article/29/1/5/1843824
- Gu, Kelly, Xiu, “Empirical Asset Pricing via Machine Learning”：机器学习资产定价和非线性/交互信号。  
  https://www.nber.org/papers/w25398
- Bailey & Lopez de Prado, “The Deflated Sharpe Ratio”：选择偏差、回测过拟合和非正态修正。  
  https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID2460551_code87814.pdf?abstractid=2460551&mirid=1
- FunSearch：LLM + systematic evaluator + evolutionary search。  
  https://www.nature.com/articles/s41586-023-06924-6
- AlphaEvolve：LLM coding agent + automated evaluator + evolutionary optimization。  
  https://deepmind.google/discover/blog/alphaevolve-a-gemini-powered-coding-agent-for-designing-advanced-algorithms/
- The AI Scientist：LLM 生成想法、代码、实验、图表和评审的自动科研 agent。  
  https://arxiv.org/abs/2408.06292

## 20. 第一轮讨论确认结果

1. 确认新建 `qe_archive` schema，不扩展旧 `archive` schema。
2. 确认 artifact 根目录为 AIstock 根目录下的 `qe_archive/artifacts/`。该目录已作为长期归档入口，历史冷数据允许放 HDD，但 DB 热表和索引应放 SSD/NVMe。
3. 确认所有日频且未处理涨跌停/停牌约束的 QE 回测全部 `research_valid=false`，默认 excluded。
4. 第一版保留单一 `score_total` 用于默认排序和优化优先级，但必须同时保存分项分数；后续可扩展多套 score profile 或 Pareto 多目标。
5. 因子重要性和模型训练分析必须覆盖所有 QE 使用过的模型。LSTM 当前应作为第一优先级深度模型支持，不能只支持树模型。
6. 模型调参近期推荐“自定义演进 batch + Optuna/TPE + 数仓 warm-start + LLM 解释/约束”的场景；同时保留自动演进恢复、独立 HPO 服务、shadow mode 等选项。
7. 允许 LLM agent 读取只读聚合视图并生成候选；细节放到后续阶段专项设计，但本方案必须预留 agent tool、审计、预算和只读接口。


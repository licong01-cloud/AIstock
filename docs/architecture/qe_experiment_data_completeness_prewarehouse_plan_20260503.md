# QE 实验阶段数据完整性前置改造方案

> 日期：2026-05-03；更新：2026-05-04  
> 状态：顶层设计草案 v1.1，待评审确认  
> 文档位置：`docs/architecture/qe_experiment_data_completeness_prewarehouse_plan_20260503.md`  
> 适用范围：QuantEvolver / QE 单次实验、自动演进 loop、自定义演进 loop、策略演进 loop、Multi-Alpha 训练与回测。  
> 明确边界：本文只规划 QE 实验创建、执行、完成阶段的数据采集与存储完整性；不设计数仓程序实现，也不要求重启或影响当前 QE 生产服务。

## 1. 结论先行

数仓建设的前提不是“先把当前已有 DB 字段搬到数仓”，而是先把 QE 实验本身变成一个可复现、可审计、可分析的数据生产系统。当前 QE 已经把一部分配置和指标写入 `qe_experiments`、`qe_evolution_tasks`、`qe_evolution_loops`，也通过 worker / Qlib Recorder 产出大量文件，但存在四个核心缺口：

1. QE DB 目前保存的是“运行态所需数据 + 部分增强指标缓存”，不是完整研究记录；某些关键字段只在 worker artifact 或日志里，甚至旧实验没有任何标准化 manifest。
2. 目前 `qe_evolution_loops.metrics_json` 与 `qe_experiments.result_metrics` 会重复保存较大的 enhanced JSON；继续把持仓、交易、事件级执行日志等大明细重复塞入两个 JSONB 字段是不合理的。
3. `result_files` 当前未形成有效 artifact manifest；本地只读统计显示 `qe_experiments.result_files` 为 0 条非空，这意味着很多非结构化产物只有路径或 worker 内部文件事实，AIstock 无法长期合规引用。
4. 对后续分析最有价值的缺口是：完整有效配置快照、持仓统计、订单/成交/未成交原因、tail-substitute 决策链、成本一致性、训练来源、模型训练诊断、因子重要性/attribution、artifact manifest 与采集质量状态。

建议采用“QE 运行态 DB + QE 托管 artifact store + 标准 worker API + 采集完整性门禁”的前置架构：

```text
QE 创建阶段
  -> 固化 raw_request / canonical_config / effective_config / provenance / collection_contract

QE 执行阶段
  -> 持久化状态事件、训练进度、订单意图、子单、成交/未成交、替补候选、成本流水、数据质量事件

QE 完成阶段
  -> worker API 返回标准 completion payload + artifact manifest
  -> AIstock 只保存一份权威明细或一份权威 artifact
  -> QE UI 与未来数仓都通过 manifest/API 引用同一份数据
```

核心原则：小而高频的摘要、索引、状态和可复现配置进入 PostgreSQL；大规模数组、明细、pickle、parquet、日志、模型和图表文件进入 AIstock 托管 artifact store；不在 QE DB 与未来数仓之间盲目复制两份大 JSON 或明细文件。

2026-05-04 修正后的边界：QE 运行态与未来数仓在“非大文件、低体积、高价值”的配置、摘要、质量状态和索引层面可以重复存储。QE 侧重复存储的目标是支持实验生成、重试、恢复、状态追踪和实验详情 UI；数仓侧重复存储的目标是支持长期、独立、可清理源系统后的研究分析。QE 侧仍应尽量避免在数据库中保存大文件、全量明细和巨型 JSON；未来 QE workspace、mlruns、worker 临时目录以及 QE DB 历史运行记录在完成归档核对后可能被定期清理，因此数仓必须保留独立完整副本，不能依赖 QE DB 或 workspace 才能回答历史分析问题。

## 2. 当前 QE 数据在 DB 与文件中的分布

### 2.1 本地只读统计快照

统计时间：2026-05-03，仅读取本地 PostgreSQL 元数据和 QE 表，不访问 WSL 或远端 worker workspace。

| 对象 | 当前统计 | 说明 |
|---|---:|---|
| `qe_experiments` 总行数 | 433 | 单次实验、演进子实验、部分 Multi-Alpha 实验等 |
| `qe_experiments.result_metrics` 非空 | 318 | 已有结果指标/增强指标缓存的实验 |
| `qe_experiments.result_files` 非空 | 0 | artifact manifest 基本未落地，这是后续必须补齐的关键缺口 |
| `qe_experiments.workspace_path` 非空 | 112 | 只能视为历史/远端元数据，不应作为 Windows 侧直接读文件依据 |
| `qe_experiments.custom_params` 非空 | 402 | 有较多参数，但不等同于完整 effective config |
| `qe_evolution_loops` 总行数 | 296 | 自动/自定义/策略演进 loop |
| `qe_evolution_loops.config_json` 非空 | 295 | 大多数 loop 有配置记录，但存在默认值/有效值不全的问题 |
| `qe_evolution_loops.metrics_json` 非空 | 276 | 大多数已完成 loop 有结果指标 |
| `metrics_json.enhanced_metrics` 非空 | 275 | 已广泛缓存增强指标，但字段覆盖不稳定 |
| `enhanced_metrics.position_summary` / `holding_audit` | 50 / 50 | 持仓汇总只覆盖少数 loop，不足以支撑统一 UI/数仓 |
| `enhanced_metrics.stock_trades` | 186 | 个股交易摘要已有一部分，但不是事件级订单/成交事实 |
| `enhanced_metrics.all_stocks` | 102 | 全部个股盈亏/持仓摘要覆盖不足 |
| `enhanced_metrics.training_diagnostics` 非空对象 | 72 | 字段键存在 275 次，但真正非空只有 72 次，backtest-only 需写训练来源 |
| `qe_evolution_loops` 表体积 | 约 45 MB | 主要来自 JSONB 指标 |
| `qe_experiments` 表体积 | 约 39 MB | 主要来自 JSONB 指标 |

这个统计说明：当前 DB 已经保存了大量“摘要 + 部分增强指标”，但还不是完整实验事实库；尤其是 artifact manifest、持仓统计、训练来源、事件级执行日志、成本一致性等关键字段覆盖不足。

### 2.2 当前已进入 AIstock DB 的主要数据

| 数据类别 | 当前主要 DB 位置 | 完整性判断 |
|---|---|---|
| 实验/loop 身份 | `qe_experiments.experiment_id`、`qe_evolution_tasks.task_id`、`qe_evolution_loops.loop_id` | 基本具备 |
| 任务类型/状态/loop 序号 | `qe_evolution_tasks`、`qe_evolution_loops` | 基本具备，但 task_type 等扩展字段历史兼容性需核对 |
| 因子列表 | `qe_experiments.factor_names`、`qe_evolution_loops.config_json.factor_list` | 基本具备，但缺因子 catalog 快照、版本/hash、顺序/角色 provenance |
| 模型/策略 ID | `qe_experiments.model_id/strategy_id`、`qe_evolution_loops.config_json` | 基本具备 |
| 数据切分 | `qe_experiments.data_split`、`qe_evolution_loops.config_json.data_split` | 有字段，但旧实验可能为空或不完整 |
| 用户/模型参数 | `custom_params`、`config_json.model_params`、`config_json.strategy_params` | 只保存部分输入/覆盖参数，不保证完整 effective config |
| 核心回测指标 | `result_metrics`、`metrics_json`、部分独立列 | 大多数已完成实验具备 |
| 增强指标 | `result_metrics.enhanced_metrics`、`metrics_json.enhanced_metrics` | 字段集不稳定，部分指标缺失 |
| 训练诊断 | `enhanced_metrics.training_diagnostics`、`qe_loop_model_records` | 覆盖不足；backtest-only 需要显式训练来源 |
| 个股盈亏/交易摘要 | `enhanced_metrics.all_stocks/top_stocks/bottom_stocks/stock_trades` | 一部分具备，但非订单/成交权威事件流 |
| agent 分析 | `qe_evolution_loops.agent_analysis` | 自动演进路径具备，自定义/策略演进不应依赖 SOTA 语义 |
| 节点信息 | `qe_evolution_loops.node_id`、payload 中的 `execution_node_id` | 部分具备，跨节点模型来源/产物来源仍需标准化 |

### 2.3 当前主要仍在文件或 worker artifact 中的数据

这些数据通常来自 Qlib Recorder / worker workspace / `mlruns` / 实验脚本输出，当前不应由 Windows FastAPI 直接读取，应通过 worker API 或 AIstock 托管 artifact store 获取。

| 数据类别 | 常见来源 | 当前问题 |
|---|---|---|
| Qlib 原始结果 | `portfolio_analysis/report_normal_1day.pkl`、`positions_normal_1day.pkl`、`indicators_normal_1day.pkl` | read_exp_res 已解析一部分，但未形成完整标准字段和 manifest |
| 预测与标签 | `pred.pkl`、`label.pkl` | 大对象，不适合直接塞 JSONB；需要 parquet/manifest 管理 |
| 模型与参数 | `params.pkl`、模型权重、features_order | 需要 hash、版本、来源 loop 和下载 API |
| 训练日志 | `run.log`、RD-Agent 日志 | 训练曲线解析不统一，LGB/PTNN/LSTM 等模型需统一 contract |
| 订单/成交对象 | indicators object、分钟执行对象、策略 runtime state | 当前只有聚合或派生摘要，缺事件级事实链 |
| tail-substitute 过程 | 策略运行时决策 | 旧实验只能部分还原；未来必须 runtime JSONL 记录 |
| 成本细节 | Qlib report、indicator cost、现金扣费路径 | 当前展示口径可能不一致，需要成本 reconcile payload |
| HMM 中间过程 | 快照、系数、调整前后排名 | 部分配置在 DB，归因/系数 manifest 不完整 |
| artifact 列表 | worker workspace 目录事实 | `result_files` 未落地，缺 hash/size/schema/source |

## 3. 是否需要在 QE DB 与数仓同时保存两份数据

结论：需要分层判断。非大文件、低体积、高价值的运行态配置、核心指标摘要、质量状态、血缘索引可以在 QE DB 与未来数仓重复存储；大文件、全量明细、模型、日志、长曲线和巨型 JSON 不应在 QE DB 与数仓之间盲目重复保存。

### 3.1 不该重复保存的内容

以下内容不应同时完整保存到 QE DB JSONB 和未来数仓原始表中：

- 全量持仓明细、全量订单/成交明细、全量逐分钟执行事件。
- `pred.pkl`、`label.pkl`、模型权重、训练中间张量。
- 超大 `qlib_results_enhanced.json`、按股票展开的交易明细 JSON。
- 原始日志、workspace bundle、图片、pickle、parquet。

这些内容应该只有一份权威托管副本，DB 保存 manifest、hash、schema_version、行数、时间范围、质量状态和查询入口。数仓如需高频分析，可把其中一部分转换成列式事实表或 Parquet 分区，而不是复制 worker 原始目录。

### 3.2 必须进入 QE DB 的内容

QE DB 仍然需要保存以下内容，因为它们是 UI、任务审计、复现、重试、恢复、质量门禁和后续数仓消费的基础：

- 身份与血缘：task、loop、experiment、attempt、node、source_model、source_loop。
- 完整 effective config 快照及 hash：保证未来 catalog 默认值改变后仍可复现实验。
- 核心指标摘要：收益、回撤、IC、RankIC、成本、换手、持仓范围、期初/期末资产。
- 采集状态：哪些数据已采集、哪些缺失、缺失原因、是否可补录、是否 archive-ready。
- artifact manifest：文件名、类型、URI、sha256、size、row_count、schema_version、来源 API、生成时间。
- 小型诊断摘要：训练曲线摘要、成本一致性摘要、执行质量摘要、HMM 摘要、因子重要性摘要。

这些内容即使未来也进入数仓，仍可在 QE DB 保留一份，因为它们直接支撑 QE 运行态能力：实验详情展示、失败重试、继续新增 loop、恢复任务、判断是否可清理源 workspace。

### 3.3 推荐的“只保留一份”方式

```text
大明细 / 原始文件：AIstock 托管 artifact store 保存一份
  -> QE UI 通过 API 按需读取或分页读取
  -> 未来数仓按需抽取结构化分析表或引用同一 manifest

QE DB：保存索引、摘要、hash、质量状态、manifest
  -> 高频列表、详情卡片、对比图表不解析大文件
  -> 不在 result_metrics / metrics_json 中重复保存大明细

未来数仓：保存面向分析的结构化投影和长期独立副本
  -> 可以保存订单/成交/持仓的规范化事实表
  -> 原始 pickle/log/model 文件进入数仓自有 artifact store 或长期对象目录
```

因此，如果当前默认没有进入 AIstock DB 的数据，并不意味着未来必须在 QE DB 与数仓各存一份。更合理的路径是：先让 worker API 生成标准 artifact 并交给 AIstock 托管一次；QE DB 保存 manifest 和摘要；未来数仓复制必要的结构化事实和长期 artifact 引用，形成脱离 QE 源系统的独立分析权威。

### 3.4 QE 与数仓的职责边界修正

| 组件 | 主要职责 | 保留周期 | 能否作为未来历史分析唯一来源 |
|---|---|---|---|
| QE runtime DB | 支持实验创建、状态流转、重试、恢复、详情 UI、完成采集门禁；可保存非大文件配置/摘要/索引 | 运行态和近期追踪为主，归档完成后可按策略清理历史记录 | 不能。未来可能删除历史 QE 记录 |
| Worker workspace / mlruns | 运行期临时产物、Qlib Recorder 原始输出、模型训练中间文件 | 临时或短期，归档完成后可清理 | 不能。不得被 Windows 后端直接访问，也不保证长期存在 |
| AIstock artifact store | 托管 QE 完成后交付的 artifact，保存 manifest 可校验对象 | 中长期；按热/温/冷分级 | 可以作为数仓原始 artifact 来源，但需由数仓接管长期索引 |
| 未来 QE 数仓 | 长期保存可分析、可复现、可审计的结构化事实、指标、维度、artifact manifest 和质量结果 | 长期 | 是。应成为 QE 历史分析、排行榜、LLM agent 和自动演进的权威来源 |

职责边界的关键变化是：QE runtime DB 不再被设计成永久历史库；数仓不是 QE DB 的读缓存，而是完成归档后的独立研究记录系统。

### 3.5 数仓独立性硬约束

未来数仓设计必须满足以下硬约束：

1. 数仓查询、历史 UI、统计分析、LLM agent 工具和自动演进不得依赖 QE DB 中仍存在源记录。
2. 数仓不得依赖 worker workspace、WSL 路径、远端节点路径或 mlruns 原始目录仍可访问。
3. 数仓必须拥有自己的 `warehouse_run_id` / `warehouse_loop_id` / `source_run_uid` 映射，避免源系统清理后失去身份。
4. 对长期有价值的非大文件数据，数仓必须复制一份结构化或 JSONB 副本，例如 effective config、核心指标、质量状态、血缘、因子列表、策略实际值。
5. 对大文件和全量明细，数仓必须复制、搬移或登记到数仓自有 artifact store，并保存 hash、size、schema、row_count、source hash 与解析版本。
6. QE 源数据清理必须以 archive completeness gate 为前置条件；未达到 `archive_ready=true` 的实验不得自动清理 workspace 或 QE DB 历史记录。
7. 数仓历史详情 API 的验收用例必须包含“模拟 QE DB 源记录不可用 / workspace 不可用后仍能返回完整历史详情”。

## 4. QE 实验创建阶段必须补齐的数据

创建阶段目标：在任务提交给 worker 之前，就把“这次实验到底打算做什么、所有默认值解析后是什么、未来如何复现”固化下来。不能等完成后再从文件倒推。

| 模块 | 必须采集字段 | 说明 |
|---|---|---|
| 用户原始请求 | raw_request、创建入口、UI 表单版本、用户说明、实验目标、优先级、触发来源 | 保留人类意图和实验说明，方便未来分析为什么跑这组实验 |
| 逻辑身份 | source_type、task_id、loop_id、experiment_id、attempt_no、parent/source task/loop | 支持 rerun、retry、backtest-only、跨节点来源模型 |
| 实验类型 | single、auto_evolution_loop、custom_evo_loop、strategy_evo_loop、multi_alpha_group、multi_alpha_combined | 自定义演进不应依赖 SOTA 语义；所有 loop 都必须可展示和采集 |
| 完整因子快照 | factor_names、顺序、factor_catalog_id、source、表达式/代码 hash、分类、评级快照、相关性摘要 | 不能只保存字符串列表，否则无法解释组合历史表现 |
| 完整模型快照 | model_id、model_catalog_id、model_type、超参、训练窗口、label_horizon、feature schema、模型代码 hash | LSTM、LGB、PTNN 等所有已用模型都必须支持 |
| 完整策略有效值 | strategy_id、strategy class/module、topk、n_drop、max_n_drop、min_n_drop、hold_thresh、risk_degree、weight_method、max/min weight、rebalance、only_tradable、forbid_all_trade_at_limit | 当前 `strategy_params` 可能只含用户覆盖值，必须保存 catalog 默认值合并后的实际值 |
| 交易/回测参数 | initial_cash、benchmark、open/close cost、min_cost、slippage、freq、executor、exchange 参数 | 期初资产、成本和回测口径必须可查 |
| 数据切分 | train/valid/test/backtest 起止时间、fit 起止时间、数据截止日 | 不能出现 `data_split={}` 的新实验 |
| 数据上下文 | Qlib day/minute provider、snapshot id/date、universe、stock_pool、benchmark、停牌/涨跌停数据版本 | 判断回测是否有研究价值和是否处理涨跌停/停牌 |
| 执行算法 | execution_algo、execution_algo_params、V24/V25/TailTWAP 参数、tail unfilled policy | 后续分析尾盘执行必须有配置基线 |
| 黑名单/行业池 | sector_blacklist、stock pool 文件 hash、过滤前后股票数、生成时间 | 防止未来黑名单配置变化导致历史无法复现 |
| HMM | enable flag、snapshot id/display name、signal preset、coefficient source/hash、训练配置版本 | UI 和归档都需要明确 HMM 快照 |
| backtest-only 来源 | training_source_task_id、training_source_loop_index、source_model_hash、source_feature_schema、source_label_horizon | 当前 loop 不训练时，训练卡片空是预期，但必须说明来源 |
| 环境快照 | AIstock git commit、runner script hash、Qlib/RD-Agent/QE parser version、Python/conda 环境 | 复现实验和排查历史差异 |
| 采集契约 | required_payload_schema_version、required_artifact_types、worker collector capabilities | worker 不支持关键采集项时应创建前或提交前 fail-fast |

创建阶段的关键设计要求：

1. `raw_request`、`canonical_config`、`effective_config` 分开保存：raw 表示用户原始输入，canonical 表示规范化结构，effective 表示所有默认值、catalog 默认、运行时注入之后的最终值。
2. 每个字段都带 provenance，例如 `ui_request`、`model_catalog`、`strategy_catalog.default_kwargs`、`config_composer`、`worker_conf_yaml`、`inferred_legacy`。
3. 新实验不得再依赖“策略目录当前默认值”作为历史事实；默认值必须在创建阶段快照。
4. 新实验不得只保存 worker path；必须保存 node_id 和后续 artifact API 入口。

## 5. QE 实验执行期间必须补齐的数据

执行阶段目标：把“发生了什么”按事件记录下来，尤其是后续无法从聚合结果中还原的策略决策和执行事实。

### 5.1 状态与运行事件

| 事件类型 | 字段 | 价值 |
|---|---|---|
| submission | job_id、node_id、callback_url、wsl_command hash、experiment_files hash、submit_at | 排查提交与 worker 执行是否一致 |
| worker lifecycle | queued、started、training_started、backtest_started、completed、failed、callback_received | 避免只知道 completed，不知道哪个阶段耗时或失败 |
| progress | epoch、step、current stage、elapsed、GPU/CPU/RAM 可选摘要 | 训练/回测资源诊断 |
| data readiness | 缺日线、缺分钟线、停牌数据缺失、涨跌停数据缺失、stock pool 不完整 | 判断实验是否研究有效 |
| parser event | read_exp_res version、解析 artifact 列表、解析错误、缺字段 | 防止“成功但详情为空” |

### 5.2 事件级执行日志

另一个窗口提出的“订单意图、子单、未成交原因、tail-substitute 候选、最终成交方向和金额”必须补充，且有明确分析价值。否则只能做聚合验证，无法回答“为什么尾盘没有买入/为什么替补了这个股票/成本到底扣在哪”。

建议事件链：

```text
signal_rank / target_position
  -> order_intent
  -> child_order / execution_plan
  -> market_state_check
  -> fill / partial_fill / unfilled
  -> tail_substitute_candidates
  -> substitute_selected / substitute_rejected
  -> final_position_delta
  -> cost_ledger_event
```

| 事件 | 核心字段 | 说明 |
|---|---|---|
| order_intent | event_id、trade_date、minute、symbol、side、target_weight、current_weight、target_amount、current_amount、intended_delta_value、reason、rank、score | 策略想做什么 |
| child_order | parent_event_id、child_order_id、algo_stage、submit_time、price_type、limit_price、planned_qty/value、lot_size_adjustment | 执行算法如何拆单 |
| market_state_check | symbol、trade_date、minute、is_suspended、limit_up/down、pre_close、minute_bar_available、tradability_result | 为什么能/不能交易 |
| fill | child_order_id、fill_time、fill_price、fill_qty、fill_value、commission、tax、slippage、cash_after | 实际成交事实 |
| unfilled | child_order_id、unfilled_qty/value、reason_code、reason_detail、blocked_by_limit/suspend/no_bar/cash/lot/policy | 未成交原因必须结构化 |
| tail_substitute_candidate | blocked_parent_id、candidate_symbol、candidate_rank、candidate_score、tradability_checks、reject_reason | 替补候选池，不能只记录最终结果 |
| tail_substitute_final | blocked_symbol、selected_symbol、final_side、final_value、final_qty、selection_reason、fallback_mode | 解释尾盘最终方向和金额 |
| position_delta | symbol、before_qty、after_qty、before_value、after_value、delta_qty/value、cash_delta | 与持仓快照对账 |

未成交原因建议使用稳定枚举：`limit_up`、`limit_down`、`suspended`、`no_minute_bar`、`no_price`、`insufficient_cash`、`lot_size_rounding`、`risk_limit`、`policy_hold_thresh`、`tail_window_closed`、`executor_exception`、`unknown`。所有 `unknown` 必须带原始异常或上下文。

存储建议：事件级日志通常较大，采用 JSONL/Parquet artifact 作为权威明细；QE DB 保存事件摘要和 manifest。如果未来 UI 需要按股票/日期检索，再把高价值字段异步规范化到分区表，不直接塞入 `metrics_json`。

### 5.3 成本 metric 与现金扣费一致性

另一个窗口提出的“修复/补充成本 metric 记录，让 report cost 与现金扣费路径一致展示”也必须补充。这个问题会直接影响用户对回测可信度的判断。

需要同时记录四类成本口径：

| 成本口径 | 字段 | 说明 |
|---|---|---|
| report cost | report_cost_total、report_cost_rate、excess_return_with_cost、excess_return_without_cost | Qlib 报告里的成本口径 |
| execution cost | fill_commission、fill_tax、fill_slippage、fill_min_cost、fill_total_cost | 订单/成交链路计算出的成本 |
| cash ledger cost | cash_before、cash_after、trade_value、cash_cost_deducted | 现金实际扣费路径 |
| reconciliation | cost_diff_abs、cost_diff_bp、matched、diff_reason、tolerance | 证明 report 与现金路径是否一致 |

展示原则：

1. UI 不只显示“含成本收益”，还要显示“成本是否已从现金扣除”。
2. 当 Qlib NestedExecutor 或策略路径已经在 `return` 中扣成本时，必须明确 `cost_in_return=true`，避免用户误以为没有扣成本。
3. 当 report cost 与现金 ledger 差异超过阈值，实验仍可完成，但采集质量必须标记 `cost_reconciliation_failed`，不能进入高可信排行榜。

### 5.4 训练过程与因子重要性趋势

每个因子在每次实验模型训练中的权重/贡献表现有分析价值，但不能把所有模型都简单理解为“神经网络原始权重”。推荐按模型族记录可解释 attribution：

| 模型族 | 执行期/完成期应记录 | 说明 |
|---|---|---|
| LGB/XGB/CatBoost | gain、split、cover、permutation、SHAP 摘要 | native importance 可用，但需保存 method，避免跨模型误比 |
| 线性模型 | coefficient、standardized coefficient、direction | 必须标准化后才有比较意义 |
| LSTM/GRU/ALSTM/TCN/Transformer/PTNN | loss 曲线、feature order、permutation/occlusion、gradient x input、integrated gradients、时间窗口 attribution 摘要 | 当前回测最佳模型包含 LSTM，必须优先支持深度模型 attribution |
| Multi-Alpha | group weight、meta model weight、group prediction correlation、group contribution | 高价值，应优先结构化摘要 |

存储原则：

- DB 保存 run-factor 级最终摘要和少量趋势摘要，例如训练早/中/晚期 top factors、稳定性、方向一致性、贡献衰退。
- per-epoch/per-date/per-sample attribution 明细保存为 Parquet artifact；manifest 记录行数、特征数、采样窗口、方法和随机种子。
- 旧实验如没有训练日志或 attribution artifact，不能伪造；只能标记为 `legacy_unavailable`。

## 6. QE 实验完成后必须补齐的数据

完成阶段目标：worker 完成后，必须通过 API 一次性或分批返回标准 payload，并由 AIstock 进行质量核对和持久化。不能只依赖 UI 点击详情时临时解析。

| 模块 | 必须采集字段 | 说明 |
|---|---|---|
| 核心指标 | IC、RankIC、ICIR、RankICIR、年化收益、含/不含成本收益、Sharpe/IR、Calmar、最大回撤、波动率、胜率 | QE loop 详情卡片和横向对比基础 |
| 绝对收益 | absolute_return、absolute_cagr、absolute_max_drawdown、NAV、benchmark NAV、excess NAV | 必须和 Qlib 超额收益分开展示 |
| 资产口径 | initial_cash、final_cash、final_stock_value、final_total_value、final_cash_ratio | 用户特别要求期初/期末资产必须采集 |
| 持仓统计 | min/avg/max/p95 position count、daily position count curve、holding days、最短/平均/最长持股时间 | 当前缺失较多，必须作为标准字段 |
| 交易统计 | total orders、fills、partial fills、unfilled、buy/sell value、turnover、win/loss、profit/loss ratio | 当前 stock_trades 不是完整执行事件 |
| 成本一致性 | report/execution/cash ledger 三方 reconcile | 可信回测门禁 |
| 执行质量 | 成交率、限价阻断、停牌阻断、缺分钟线、tail-substitute 触发次数、最终替补金额 | 分析分钟执行和尾盘策略 |
| 训练诊断 | train/valid loss、best epoch、early stop、overfit ratio、训练耗时 | backtest-only 要记录 training_source 而不是空卡片 |
| 因子表现 | run_factor 列表、importance、attribution、组合相关性、评级快照 | 未来自动因子组合的依据 |
| HMM 归因 | snapshot、preset、调整前后排名、行业系数摘要、贡献收益 | 判断 HMM 是否真实有效 |
| artifact manifest | conf、pred、label、model、report、positions、indicators、logs、execution_events、cost_reconcile、attribution | 每项含 URI/hash/size/schema/version |
| 数据质量报告 | missing_fields、parser_errors、unknown_reason_count、schema_version、completeness_grade | 是否允许进入数仓/排行榜 |
| 复现等级 | full、partial、audit_only、unreproducible | 基于 config、artifact、hash、环境、指标一致性判定 |

完成阶段必须区分两个状态：

```text
runtime_status = completed / failed / cancelled
collection_status = complete / partial / failed / retrying / legacy_unavailable
```

一个 loop 回测完成并不等于数据采集完整。UI 应允许显示“实验完成，但数据采集缺失 position_summary/cost_reconcile/execution_events”，并提供补采集入口。

## 7. 推荐存储方式

### 7.1 分层存储

| 层级 | 存储内容 | 推荐介质 | 是否权威 | 访问方式 |
|---|---|---|---|---|
| QE 运行态 DB | task/loop/experiment 状态、核心摘要、effective config hash、collection status | PostgreSQL | 是 | QE 后端直接查询 |
| QE 配置/指标 JSONB | canonical_config、effective_config、compact enhanced summary、quality report | PostgreSQL JSONB | 是 | QE UI 详情/对比 |
| QE 明细索引表 | artifact manifest、event summary、cost summary、run-factor summary | PostgreSQL / 分区表 | 是 | UI 筛选、后续数仓消费 |
| QE 托管 artifact store | parquet/jsonl/pkl/model/log/conf/report/attribution 大对象 | `qe_archive/artifacts` 或同级 AIstock-owned artifact 目录，HDD 可用于冷数据 | 是 | 后端 API 按需读取 |
| 临时 worker workspace | worker 运行目录和 mlruns | worker 本地盘 | 否，运行期事实来源 | 只能通过 node API 访问，不能由 Windows 直读 |
| 未来数仓 | 面向分析的规范化事实表和聚合表 | PostgreSQL/Timescale/Parquet | 分析权威 | 数仓 API/只读工具 |

说明：虽然目录名可能沿用 `qe_archive/artifacts`，本文强调的是“AIstock 托管 artifact store”能力，不代表本阶段要实现数仓入库程序。

### 7.2 数据类型的落点

| 数据 | QE DB 保存 | Artifact 保存 | 原因 |
|---|---|---|---|
| 身份、状态、时间、节点 | 完整保存 | 可选 manifest 备份 | 高频查询 |
| raw_request/canonical/effective config | 完整 JSONB + hash | `canonical_config.json` 备份 | 复现必须，体积可控 |
| 核心指标和卡片指标 | 独立列/摘要 JSONB | `metrics_summary.json` 可选 | UI 高频展示 |
| 收益/回撤/IC/换手曲线 | 最近/压缩摘要或 JSONB | parquet/json | 图表需要；长曲线更适合文件或分区表 |
| 全量持仓 | summary 入 DB | parquet 权威 | 明细大，不重复入两个 JSONB |
| 全量订单/成交/执行事件 | summary/索引入 DB | JSONL/Parquet 权威 | 需要可审计但体积可能大 |
| stock_trades/all_stocks | 小摘要可入 DB；全量入 artifact | parquet/json 权威 | 避免 `metrics_json` 持续膨胀 |
| pred/label | manifest 入 DB | pkl/parquet 权威 | 大对象，低频读取 |
| 模型权重 | manifest 入 DB | 文件权威 | 复现/打包使用 |
| 训练日志 | 摘要入 DB | log/jsonl 权威 | 训练卡片读摘要，排查读日志 |
| 因子 attribution 明细 | run-factor summary 入 DB | parquet 权威 | 大矩阵，不适合 JSONB |
| 成本 reconcile | 摘要和通过/失败入 DB | 明细 artifact | 可信门禁和排查都需要 |

### 7.3 不引入 NoSQL 的判断

当前阶段不建议引入 MongoDB/文档数据库作为 QE 实验数据主存储：

1. PostgreSQL JSONB 已能承载 config、manifest、quality report 等半结构化数据。
2. 大对象和明细更适合 Parquet/JSONL artifact，而不是文档库。
3. 再引入 NoSQL 会增加一致性、备份、权限和查询路径复杂度。
4. 后续若事件量达到千万级以上，优先考虑 PostgreSQL 分区表、TimescaleDB、Parquet lake，而不是先上文档库。

### 7.4 面向未来数仓分析的顶层预留

虽然本文优先规划 QE 实验阶段采集，但从数仓专家和量化架构师角度，采集设计必须提前满足未来高频分析场景，避免后续二次返工。

| 未来分析场景 | 必须提前采集/结构化的数据 | 说明 |
|---|---|---|
| 实验横向对比 | run summary、metric summary、数据切分、成本口径、质量等级 | 支持每天十几个到二十几个实验实时比较 |
| loop 演进轨迹 | task/loop 序号、parent/source、agent action、参数变化、指标变化 | 判断自动/自定义演进是否真正优化 |
| 模型超参数研究 | model type、完整 hyperparams、训练窗口、label horizon、seed、训练曲线、验证集表现 | 支持 LSTM/LGB/PTNN 等所有已用模型，不以 `score_total` 单一口径约束 |
| 因子组合研究 | factor bridge、factor category/rating snapshot、相关性、组合中的角色、importance/attribution | 支持从历史表现中有目标地探索组合，而不是 LLM 随机调整 |
| 因子贡献衰退 | run-factor metrics、按阶段 attribution、IC/RankIC 时间序列、稳定性 | 分析因子是否过拟合、是否在特定市场状态失效 |
| HMM 有效性 | snapshot/preset、调整前后排名、行业系数、收益/风险贡献 | 判断 HMM 是增益还是噪声 |
| 执行诊断 | order/fill/unfilled/tail-substitute event、limit/suspend/no_bar、成交率 | 支持尾盘执行和涨跌停/停牌处理验证 |
| 成本可信度 | report/execution/cash ledger 三方成本、差异原因 | 避免“指标显示含成本但现金路径无法证明”的问题 |
| 数据质量/研究有效性 | 日频/分钟频、涨跌停/停牌处理、缺数据事件、parser 版本、schema 版本 | QE 中日频策略回测若未处理涨跌停，后续可标记低价值或淘汰 |
| 自动演进依据 | 历史优先级、组合相似度、失败原因、参数敏感性、实验预算消耗 | 给自动调参/因子组合 agent 提供结构化证据 |

建议未来数仓至少预留以下逻辑数据域：

| 数据域 | 推荐形态 | 用途 |
|---|---|---|
| `dim_experiment` / `dim_loop` / `dim_model` / `dim_strategy` / `dim_factor` | 维度表 | 身份、分类、版本、可读标签 |
| `fact_run_summary` | 宽表/事实表 | 高频筛选、排行榜、横向对比 |
| `bridge_run_factor` | 关联表 | 一个 run 中因子参与、顺序、角色、快照属性 |
| `fact_metric_timeseries` | 长表或 Parquet 分区 | 收益、回撤、IC、RankIC、换手、持仓数等曲线 |
| `fact_execution_event` | 分区事实表或 Parquet | 订单、成交、未成交、替补、成本事件 |
| `fact_factor_attribution` | 分区事实表或 Parquet | 因子贡献、重要性、方法、训练阶段 |
| `artifact_manifest` | 表 + 对象存储路径 | pred/label/model/log/report/parquet 的长期索引 |
| `quality_gate_result` | 事实表 | completeness、reproducibility、cost_reconcile、source_cleanup gate |

性能与准确性预留：

1. 常用过滤字段必须结构化列化：模型、因子数、回测起止、label horizon、策略 topk/n_drop、HMM、成本口径、数据频率、研究有效性、质量等级。
2. 大曲线和事件级明细优先使用 Parquet/列式分区，按 `trade_date`、`warehouse_run_id`、`metric_name`、`factor_id`、`symbol` 建分区/索引。
3. 每个指标必须携带语义标签：`with_cost`、`absolute_or_excess`、`benchmark`、`freq`、`annualized`、`unit`、`source_artifact`、`parser_version`。
4. 每次入仓必须保存 source hash、artifact hash、row_count、schema_version、parser_version，确保未来可以核对“数仓值是否等于当时 QE 结果”。
5. 面向 LLM agent 的访问层应是只读、安全、带 schema 描述的查询 API 或语义视图；agent 不应直接读物理表和对象目录，也不能绕过质量等级过滤。
6. 后续可为高频聚合图表建立物化视图或预计算 cube，但必须能追溯到原始 fact 和 artifact manifest。

### 7.5 专家视角的补充建议

| 角色 | 建议 | 落地含义 |
|---|---|---|
| 数仓专家 | 采用“运行态源系统 + 长期分析数仓 + artifact lake”的分层，不把 QE DB 当永久历史库 | 数仓要有独立主键、独立 manifest、独立质量门禁和源清理模拟测试 |
| 数仓专家 | 将明细按分析频率分级：热数据结构化入 PG/Timescale，温冷明细放 Parquet，模型/日志放对象目录 | 支持每天二十个 loop 级别的实时分析，同时避免 JSONB 膨胀 |
| 量化架构师 | 指标必须区分超额/绝对、含成本/不含成本、日频/分钟频、是否处理涨跌停/停牌 | 防止不同实验不可比，日频未处理涨跌停策略可直接低价值标记 |
| 量化架构师 | 因子组合分析应保存因子分类、评级、相关性、贡献方法和样本期，不能只保存因子名 | 后续自动组合探索才能从历史证据出发，而不是随机拼因子 |
| 测试专家 | 数据采集、归档、清理、补录必须在设计阶段定义 oracle 和负例 | 防止“UI 能点开”被误认为“数据已完整入库” |
| 测试专家 | 覆盖率门禁必须覆盖 parser/config/cost/event/cleanup 分支，L3 UI 只能作为业务链路证明 | 代码覆盖率和全流程验证需要同时存在，不能互相替代 |

## 8. QE DB 侧建议的结构调整方向

当前不直接给出最终 DDL，但后续实施必须遵守：所有新增表和字段都要有 PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN` 注释，说明业务含义、单位、来源和质量语义。

### 8.1 避免 `metrics_json` / `result_metrics` 双份膨胀

当前兼容结构：

```text
qe_evolution_loops.metrics_json       保存 loop 指标 JSONB
qe_experiments.result_metrics         保存对应 experiment 指标 JSONB
```

改进方向：

1. 保留这两个字段作为兼容缓存，但只写核心摘要和必要字段，不再追加超大明细。
2. 新增或规划统一的 `qe_run_result_payload` / `qe_run_collection` 概念，由单一 `run_uid` 关联 experiment/loop。
3. `qe_experiments` 与 `qe_evolution_loops` 只保存 `run_uid/result_ref`、核心展示指标和状态。
4. 明细通过 artifact manifest 引用；未来数仓消费同一个 `run_uid`。

### 8.2 建议的 QE 运行态补充表

| 表/概念 | 作用 | 典型字段 |
|---|---|---|
| `qe_run_collection` | 每次真实执行的数据采集状态总表 | run_uid、source_type、experiment_id、loop_id、attempt_no、runtime_status、collection_status、missing_fields、quality_grade、reproducibility_level |
| `qe_run_effective_config` | 不可变有效配置快照 | run_uid、raw_request、canonical_config、effective_config、config_hash、provenance、schema_version |
| `qe_run_artifact_manifest` | artifact 文件索引 | run_uid、artifact_type、uri、sha256、size_bytes、row_count、schema_version、source_api、created_at、retention_tier |
| `qe_run_metric_summary` | 核心指标投影 | run_uid、metric_name、metric_value、unit、scope、with_cost、freq、source_payload、quality_flag |
| `qe_run_execution_summary` | 执行质量摘要 | run_uid、orders、fills、unfilled、tail_substitutions、limit_blocks、suspend_blocks、cost_total、cost_reconcile_status |
| `qe_run_factor_summary` | 因子参与和贡献摘要 | run_uid、factor_catalog_id、factor_name、role、importance_method、importance_value、rank、rating_snapshot、correlation_summary |
| `qe_run_model_training_summary` | 模型训练摘要 | run_uid、model_id、model_type、source_run_uid、best_epoch、loss_summary、overfit_ratio、training_source |

事件明细表可后置。如果第一阶段不想扩大 DB 写入量，事件级明细先作为 artifact 权威保存；但 manifest 和摘要必须进入 DB。

## 9. Worker API 与 parser contract

为避免“字段只在 worker 文件里，没经 API 回写 DB”，后续 worker 必须提供标准 contract。

### 9.1 完成 payload 必备顶层结构

```json
{
  "schema_version": "qe_completion_payload_v1",
  "task_id": "...",
  "loop_id": "Loop1",
  "experiment_id": "...",
  "runtime_status": "completed",
  "effective_config": {},
  "metrics_summary": {},
  "enhanced_metrics": {},
  "position_summary": {},
  "holding_audit": {},
  "execution_event_summary": {},
  "cost_reconciliation": {},
  "training_diagnostics": {},
  "training_source": {},
  "factor_importance_summary": [],
  "data_quality_report": {},
  "artifact_manifest": []
}
```

### 9.2 必须 fail-fast 或 partial 标记的场景

| 场景 | 处理方式 |
|---|---|
| worker parser 不支持 required schema | 提交前或完成采集时 fail-fast，不标记 collection complete |
| `position_summary` 缺失 | runtime 可 completed，但 collection_status=partial，missing_fields 包含 position_summary |
| `cost_reconciliation` 缺失 | 不允许标记高可信；UI 提醒成本口径未核对 |
| backtest-only 没有当前训练曲线 | 必须写 `training_source`，不能让训练卡片无解释地为空 |
| artifact manifest 缺失 | 不允许标记 archive-ready / reproducibility full |
| 事件级执行日志缺失 | 分钟执行/尾盘执行相关实验标记 execution_audit_missing |
| 旧实验无法补齐 | 标记 legacy_unavailable，不伪造字段 |

### 9.3 自动避免缺字段的程序机制

排除 API 未启动、worker 机器断电、artifact 已被人为删除等不可控场景，其他缺字段应通过程序机制避免：

1. 创建阶段保存 `required_payload_schema_version` 和 `required_artifact_types`。
2. 提交前调用 worker `capabilities` API，确认 parser 支持 required schema。
3. 完成回调只更新 `runtime_status`，不直接认为数据完整。
4. 后端 collector 拉取 `/metrics`、`/enhanced-metrics`、`/completion-payload`、`/artifacts/manifest`，并按 required list 校验。
5. 校验失败写入 `collection_status=partial/retrying` 和 `missing_fields`，后台重试，UI 可见。
6. worker artifact 清理必须等待 `collection_status=complete` 或明确用户确认；否则禁止清理。
7. 采集完成后计算 config hash、artifact hash、核心指标二次校验，才允许进入未来数仓或高可信分析。

## 10. UI 展示原则

1. 所有 loop 都必须可展开、可分析、可展示增强指标；自定义演进和策略演进不依赖 SOTA 展开逻辑。
2. Loop 详情总览必须展示实际策略配置值：持仓限制股票数量、每日换股数量、最短持股时间、初始资金、成本参数、执行算法、HMM、黑名单、数据切分。
3. 如果字段缺失，UI 显示“未采集/可补录/旧实验不可补录/worker API 不可用”，不得显示空值冒充 0 或默认值。
4. 大明细采用列表分页、日期/股票筛选和按需加载，不把大 JSON 一次性返回前端。
5. 训练卡片对 backtest-only loop 显示来源模型和来源 loop 的训练摘要链接，而不是空白。
6. 成本卡片同时显示 report cost、现金扣费、执行成本和 reconcile 结果。
7. 事件级执行面板支持按 `unfilled_reason`、`tail_substitute`、`limit/suspend/no_bar` 过滤。

## 11. 历史数据补录原则

历史补录只能补齐“artifact 仍可通过 API 获取、且 parser 能解析”的数据。不能从不存在的运行时状态中凭空生成事实。

| 数据 | 旧实验可补录性 | 说明 |
|---|---|---|
| position_summary / holding_audit | 高，如果 `positions_normal_1day.pkl` 仍在且 worker API 可解析 | 当前最优先补齐 |
| absolute_returns / final asset | 高 | 基于 positions/report 可补 |
| training_diagnostics | 中高，取决于 run.log/训练日志是否存在 | LGB/LSTM/PTNN parser 需统一 |
| cost_reconciliation | 中，取决于 report、indicators、现金路径是否可解析 | 旧实验可能只能部分核对 |
| stock_trades | 中高 | 当前已有派生逻辑，但需标准化 |
| event-level order/fill | 中，若 indicators object 有足够字段可补部分 | 旧实验很难还原完整 tail-substitute 决策链 |
| tail-substitute candidates | 低 | 未来必须 runtime audit；旧实验只能从结果推断，不能标记权威 |
| artifact manifest | 中 | 可对仍可下载的文件生成 manifest；已丢失则 legacy_missing |
| effective config | 中 | 已保存字段 + conf.yaml 可补；默认值若未快照只能标记 inferred_default |

历史补录 API 应只调用 AIstock 后端和 worker/node API，不直接访问 WSL 或远端目录。补录结果必须写清楚 `source=api_backfill`、`confidence`、`inferred_fields`。

## 12. 分阶段实施建议

### Phase 0 - 只读审计与 contract 固化

- 固化本文的数据字典和 required fields。
- 对当前 DB 做只读审计，列出不同任务类型字段缺口。
- 定义 worker completion payload JSON schema、artifact manifest schema、missing_fields 规范。
- 验证不访问 WSL/远端文件的红线。

验收：能对任一完成 loop 输出“已采集/缺失/可补录/不可补录”的质量报告。

### Phase 1 - 创建阶段 effective config 快照

- 在所有 QE 创建入口保存 raw_request、canonical_config、effective_config、provenance、config_hash。
- 保存完整策略实际值，包括 topk、n_drop、hold_thresh、initial_cash、成本参数。
- backtest-only 保存 training_source。
- 提交前校验 worker capabilities。

验收：新建实验无需读取 worker 文件，就能从 DB/API 还原完整配置和回测窗口。

### Phase 2 - 完成阶段标准 payload 与 manifest

- worker parser 补齐 position_summary、holding_audit、absolute returns、training_source、artifact_manifest。
- AIstock completion collector 校验 required fields，写 collection_status。
- 不再把大明细重复写入 `metrics_json` 和 `result_metrics`；只写摘要和 manifest。

验收：新 loop 完成后 UI 详情卡片、指标、持仓、训练来源、artifact 状态全部可查；缺字段有明确原因。

### Phase 3 - 执行事件与成本一致性

- 策略/执行模板输出 runtime audit JSONL：order_intent、child_order、fill/unfilled、tail_substitute、cost ledger。
- parser 生成 execution_event_summary 和 cost_reconciliation。
- UI 增加成本与执行审计卡片。

验收：可以解释任一尾盘未成交/替补/成本差异案例。

### Phase 4 - 因子 attribution 与模型训练诊断

- 统一 LGB、LSTM、PTNN 等模型训练日志解析。
- 对 LSTM 等深度模型实现模型无关 attribution 或采样 occlusion/integrated gradients 摘要。
- run-factor summary 入 DB，明细入 artifact。

验收：每个因子在每次实验中的参与、重要性方法、贡献摘要和质量标记可查。

### Phase 5 - 历史补录与未来数仓接入准备

- 通过 API 对历史实验执行补录，不手工运行脚本。
- 标记不可补录字段和 inferred_default 字段。
- 当 QE 阶段数据完整后，再启动数仓结构与自动入仓逻辑改造。

验收：历史列表可显示每个实验的数据完整度；未来数仓只消费 complete/partial-with-known-risk 的数据。

## 13. 设计阶段测试用例与质量门禁预留

本方案后续进入研发前，必须把测试用例、覆盖率门禁和全流水线验证一起纳入实施范围。测试不是实现完成后的补充动作，而是 QE 数据完整性和未来数仓可信度的一部分。

### 13.1 QE 数据完整性专项测试矩阵

| 测试编号 | 场景 | 层级 | 必须验证的 oracle |
|---|---|---|---|
| QE-COLLECT-001 | 创建阶段 effective config 完整快照 | L1/L2 | raw/canonical/effective 分层存在；策略实际值、initial_cash、成本、HMM、黑名单、数据切分、label horizon、hash 完整 |
| QE-COLLECT-002 | 非大文件 QE/数仓可重复存储 | L1/L2 | QE DB 保存运行态摘要，数仓保存长期副本；两者 hash/语义一致但主键独立 |
| QE-COLLECT-003 | 大文件外置与 manifest | L1/L2 | positions/trades/pred/label/model/log 不进入巨型 JSONB；manifest 有 uri、sha256、size、row_count、schema_version |
| QE-COLLECT-004 | completion payload required schema | L1/L2 | 缺 `position_summary`、`cost_reconciliation`、`artifact_manifest` 等字段时 collection_status=partial，不允许 archive_ready |
| QE-COLLECT-005 | loop 详情指标全覆盖 | L2/L3/UI | 绝对收益、回撤、期初资产、期末资产、持仓统计、训练来源、HMM、策略实际参数在 API/UI 一致展示 |
| QE-COLLECT-006 | 成本三方一致性 | L1/L2/UI | report cost、execution cost、cash ledger 可对账；超阈值写失败原因并阻断高可信评级 |
| QE-COLLECT-007 | 事件级执行链 | L1/L2 | order_intent -> child_order -> market_state_check -> fill/unfilled -> tail_substitute -> cost_event 可追踪 |
| QE-COLLECT-008 | backtest-only 训练来源 | L2/UI | 当前 loop 无训练曲线时必须显示 source_model/source_loop/source_metrics，不显示无解释空卡片 |
| QE-COLLECT-009 | 因子 attribution 摘要 | L1/L2 | LGB/LSTM/PTNN 等模型按模型族输出方法标记、top factors、稳定性和缺失原因 |
| QE-COLLECT-010 | 红线扫描 | L0/L1 | 后端不得直接访问 WSL/远端 workspace；历史补录只能经 API 或已托管 artifact |
| QE-COLLECT-011 | 历史补录 API | L2/L3/UI | 候选分页、dry-run、正式补录、增量新增 loop 补录、质量报告均可自动验证 |
| QE-COLLECT-012 | 数仓独立性与源清理模拟 | L4 | 归档后模拟 QE DB 源记录/workspace 不可用，数仓仍能返回完整历史详情、图表和 manifest |
| QE-COLLECT-013 | 清理门禁 | L2/L4 | archive completeness 未通过时禁止清理 QE workspace、mlruns 和 QE DB 历史记录 |

### 13.2 自动化流水线分层要求

| 层级 | QE/数仓相关要求 |
|---|---|
| L0 静态门禁 | secret 扫描、红线扫描、DB comment 检查、schema 文件检查、coverage 配置存在性检查 |
| L1 单元测试 | config merge、hash、schema validation、parser、cost reconcile、event reducer、quality grade、cleanup gate |
| L2 API/DB 集成 | repository/router/service、事务一致性、字段 comment、manifest 校验、补录 API、分页和增量 loop |
| L3 模块全流程 | 使用 8011/3011 或同类测试端口验证 QE UI/API/DB/worker mock 或测试 worker，不重启生产 8001 |
| L4 跨模块 | QE 完整采集 -> 数仓独立入仓 -> 源清理模拟 -> 历史 UI/统计分析/LLM 只读查询 |
| L5 发布候选 | 汇总覆盖率、业务 oracle、数据质量、资产安全、残余风险和版本关联记录 |

后续所有代码实现必须生成 run record，包含命令、环境、git commit、测试数据、coverage、失败原因、截图/trace/log/artifact manifest，并进入未来测试管理系统或当前 `tests/aistock_validation/history` 约定目录。

### 13.3 覆盖率门禁预留

当前设计建议单独按 `docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md` 落地覆盖率体系。最低要求如下：

1. 后端 Python 新增/修改代码启用 line + branch coverage，QE 数据完整性、数仓、成本、执行事件、清理门禁属于高风险模块。
2. 新增/修改代码 diff line coverage 初始不低于 80%，diff branch coverage 初始不低于 70%；稳定后提高到 85%/75% 以上。
3. 高风险模块单元/集成测试不得只覆盖 happy path，必须覆盖缺字段、parser 失败、成本差异、worker 不支持、manifest hash 不匹配、source cleanup 阻断等负例。
4. 前端关键业务组件需要覆盖按钮启用/禁用、dry-run/正式执行、分页、空值解释、错误态、loading 态；Playwright 负责业务路径，不替代组件/状态测试。
5. 任何 coverage gate 降级或跳过必须进入 run record 的 residual risk，不允许口头跳过。

### 13.4 未来测试管理 UI 预留

未来应建设专门的自动化测试流水线系统，具备自己的 UI、后端 API 和版本化数据模型。该系统至少应支持：测试用例库、运行计划、运行历史、覆盖率看板、失败分析、证据 artifact、版本候选质量门禁、模块健康趋势、flaky 标记和回归矩阵复用。QE 数据完整性与数仓归档应作为第一批高风险模块接入。

## 14. 本方案回答的关键问题

1. 当前 QE 实验数据既在 DB 中，也在 worker 文件中；DB 保存了多数配置摘要和部分 enhanced metrics，但文件中仍保留原始 Qlib 产物、模型、日志、预测、持仓/订单明细等，且 `result_files` manifest 当前基本缺失。
2. QE 与未来数仓可以在非大文件、低体积、高价值的数据上重复存储；这类重复是为了 QE 运行态生成/重试/恢复/UI 与数仓长期独立分析分别服务，不等于重复保存大文件或巨型 JSON。
3. 未来 QE 实验数据不是永久保留源；完成归档核对后，QE workspace、mlruns、临时模型、worker 目录和部分 QE DB 历史记录都可能清理，所以数仓必须拥有独立完整副本，不能依赖 QE DB 或 workspace。
4. 事件级执行日志必须补充，尤其是订单意图、子单、未成交原因、tail-substitute 候选、最终成交方向和金额；这类信息对验证尾盘执行和涨跌停/停牌处理非常有价值。
5. 成本 metric 必须补充 report、execution、cash ledger 三方一致性记录；否则 UI 只显示含成本/不含成本收益，用户仍可能误判成本是否真实扣除。
6. 不建议 QE DB 和未来数仓同时保存两份大 JSON/明细文件；建议一份 AIstock 托管 artifact 权威副本 + DB manifest/摘要 + 未来数仓结构化投影和长期 artifact 索引。
7. 在 QE 阶段，应优先补完整数据采集、有效配置快照、完成 payload、artifact manifest、质量门禁和 UI 缺失提示；这些完成后再进入数仓程序设计和改造。
8. 所有后续功能必须在设计阶段定义测试用例、覆盖率门禁和全流水线验收；尤其是 parser、成本核对、执行事件、数仓独立性、源清理门禁和 UI 数据一致性必须自动化验证。

## 15. 参考的本地证据

- `docs/codex_project_memory.md`：Codex 项目规范、DB comment 规范、QE 生产隔离规范、设计文档目录规范。
- `docs/architecture/qe_worker_workspace_read_refactor_validation_plan_20260502.md`：禁止 Windows FastAPI 直接读取 worker workspace，artifact 必须通过 node API 或 AIstock-owned store。
- `docs/architecture/qe_realtime_experiment_warehouse_top_level_design_20260502.md`：数仓顶层目标、分层存储、MLflow/Qlib Recorder 边界。
- `docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`：自动化测试流水线、覆盖率、质量门禁、测试管理 UI 与版本化测试系统设计，本文的第 13 节引用其作为后续研发质量约束。
- `docs/analysis/qe_diagnostics_output_frontend_fields_plan_20260501.md`：持仓统计、资产口径、训练诊断、审计文件建议。
- `backend/init_catalog_db.py`：`qe_experiments`、`qe_evolution_tasks`、`qe_evolution_loops` 当前表结构。
- `backend/services/quantevolver/qe_evolution_service.py`：loop 完成后从 worker API 拉取 metrics/enhanced，并写入 `metrics_json` 与 `result_metrics` 的当前路径。
- `backend/services/quantevolver/executors/backtest.py`：提交 worker 时已有 `experiment_files` 和 `wsl_command`，但当前结果 manifest 未标准化入库。
- `backend/services/quantevolver/experiment_config.py`：`ExperimentConfig` 是统一配置层，当前 `initial_cash` 不进入 `custom_params`，因此必须另行保存完整 effective strategy config。
- `backend/services/quantevolver/config_composer.py`：策略 catalog/default_kwargs 与用户参数合并后才得到实际执行的 strategy kwargs。
- `backend/services/quantevolver/templates/read_exp_res.py`：当前已解析 IC、收益曲线、trade diagnostics、all_stocks/stock_trades、absolute_returns 等，但 position_summary、holding_audit、artifact manifest、事件级执行日志和成本 reconcile 仍需标准化补齐。
- `backend/services/quantevolver/qe_workspace_client.py`：已有 worker API 客户端能力，包括 metrics、enhanced-metrics、artifact/file 下载，是后续合规采集入口。

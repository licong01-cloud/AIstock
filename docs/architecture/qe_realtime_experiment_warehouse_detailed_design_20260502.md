# QE 实时实验数据仓库详细设计方案

> 日期：2026-05-02  
> 状态：详细设计草案 v1.0，基于顶层方案 v2.1 与第一轮确认项  
> 文档位置：`docs/architecture/qe_realtime_experiment_warehouse_detailed_design_20260502.md`  
> 上游方案：`docs/architecture/qe_realtime_experiment_warehouse_top_level_design_20260502.md`  
> 适用范围：QE 单次实验、自动演进 loop、自定义演进 loop、策略演进 loop、Multi-Alpha、后续模型调参与因子组合自动化探索。

## 0. 设计结论

本详细设计将顶层方案落到可实施的数据库结构、服务模块、入仓流程、artifact 归档、历史补录和未来自动演进接口。

核心取向：

1. 新建独立 schema `qe_archive`，不扩展旧 `archive` schema。
2. 每个 QE 单次实验或每个 loop 结束后，通过 outbox + worker 实时入仓，不做月度批量更新。
3. 结构化数据进入 PostgreSQL / TimescaleDB 侧的 `qe_archive` 表；非结构化文件进入 AIstock 本地 artifact store `qe_archive/artifacts`，数据库只保存 manifest、hash、URI、大小和来源。
4. 所有日频且未具备权威涨跌停、停牌处理的 QE 回测都入仓但标记 `research_valid=false`，默认从排行榜、优化 warm-start、自动演进样本中排除。
5. 第一版保留单一 `score_total` 作为默认排序和优化优先级，但必须同时保存全部原始指标和分项分数。
6. 因子重要性、训练曲线、超参数 trial 必须覆盖所有已使用模型族；LSTM / 深度模型从第一阶段纳入，不只支持树模型。
7. MLflow / Qlib Recorder 作为实验追踪和 artifact 记录层接入，不能替代 AIstock 自己的量化研究数仓。
8. LLM agent 未来只能访问受控、只读、审计过的聚合视图或工具，不能直接自由 SQL 写库或随机修改演进配置。

## 1. Phase 0 文档发现与可用接口

### 1.1 已阅读的本地资料

- `docs/codex_project_memory.md`：确认架构文档目录、QE worker workspace 访问红线、artifact 根目录确认项。
- `docs/architecture/qe_realtime_experiment_warehouse_top_level_design_20260502.md`：确认顶层设计、数据范围、score_total、MLflow/Qlib、自动演进目标。
- `backend/init_catalog_db.py`：确认现有 QE 运行态表、因子/模型基础表、因子独立指标、相关性、评级表。
- `backend/db/init_archiving_schema.py`：确认旧 `archive` schema 只有通用策略运行归档骨架，不适合承载 QE 实验数仓。
- `backend/db/pg_pool.py`：确认数据库访问通过 `get_conn()`。
- `backend/services/quantevolver/qe_workspace_client.py`：确认 worker 节点访问只能通过 API，例如 `get_loop_metrics()`、`get_enhanced_metrics()`、`download_mlruns_params()`、`download_loop_assets()`、`download_workspace_file_bytes()`、`get_workspace_file()`。
- `backend/services/quantevolver/qe_evolution_service.py`：确认 `process_completed_loop()` / `process_strategy_evo_completed_loop()` 是演进 loop 完成后的关键处理路径。
- `backend/routers/quantevolver.py`：确认单次实验 `_update_experiment_with_metrics()`、`_update_experiment_status()`、`get_experiment_run_status()`、`/webhook/loop-completed`、`/experiments/{experiment_id}/enhanced-metrics`。
- `backend/routers/quantevolver_evolution.py`：确认演进 webhook `/api/v1/quantevolver/evolution/webhook/loop-completed`、loop enhanced metrics 代理接口、artifact sync 入口。
- `backend/services/quantevolver/analysis/metrics_store.py` 与 `backtest_analyzer.py`：确认已有指标写入抽象，可作为 metric extractor 的参考。
- `backend/services/quantevolver/optuna_optimizer.py`：确认当前 Optuna 从 `qe_evolution_loops` 注入历史 trial，后续应改为读 `qe_archive.run_model_trial`。
- `backend/services/quantevolver/qe_experiment_status_scanner.py` 与 `backend/main.py`：确认已有补偿扫描模式可复用到 archive scanner。
- `scripts/qrun_limit.py` 与 `scripts/qrun_limit_minute.py`：确认当前 Qlib 运行脚本会设置 `MLFLOW_TRACKING_URI`，并依赖 Qlib Recorder / `mlruns` 输出。

### 1.2 公开资料结论

- Qlib Recorder 的实验管理结构是 `ExperimentManager -> Experiment -> Recorder`；Qlib 提供基于 MLflow 的 `MLflowExpManager`，Recorder 支持记录参数、指标和 artifact。
- MLflow Tracking 将 run metadata、params、metrics 放入 backend store，将模型权重、图片、Parquet、pkl 等大对象放入 artifact store。
- Optuna `Study` 支持 `add_trial`、`enqueue_trial`、`ask()`、`tell()`、多目标 Pareto 的 `best_trials` 等能力，适合仓库 warm-start 后进行受控调参。
- 因子组合方法论需要防止 factor zoo 和多重检验问题，不能只按单次回测收益排序；应同时考虑稳健性、相关性冗余、近期衰退、样本外表现、交易成本与执行可行性。

### 1.3 第一阶段允许依赖的本地 API

```text
DB:
- backend.db.pg_pool.get_conn()

QE node API client:
- QEWorkspaceClient.get_loop_status(task_id, loop_id)
- QEWorkspaceClient.get_loop_metrics(task_id, loop_id)
- QEWorkspaceClient.get_enhanced_metrics(task_id, loop_id)
- QEWorkspaceClient.download_mlruns_params(task_id, loop_id)
- QEWorkspaceClient.download_loop_assets(task_id, loop_id, dest_dir)
- QEWorkspaceClient.download_workspace_file_bytes(task_id, loop_id, file_path)
- QEWorkspaceClient.get_workspace_file(task_id, loop_id, file_path)

QE completion hooks:
- backend.routers.quantevolver._update_experiment_with_metrics()
- backend.routers.quantevolver._update_experiment_status()
- AutoEvolutionScheduler.process_completed_loop()
- AutoEvolutionScheduler.process_strategy_evo_completed_loop()
- QEExperimentStatusScanner.scan_once()
```

### 1.4 明确禁止的反模式

1. Windows FastAPI 代码不得通过 `F:\...`、`/mnt/f/...`、`\\wsl$`、`workspace_path` 直接读取 worker workspace。
2. 不得把 `qe_experiments.workspace_path` 或 `aistock_model_catalog.workspace_path` 当作 artifact 归档来源直接 `Path.exists()` / `glob()` / `open()`。
3. 不得把大体积 pkl、模型权重、完整日志直接存入 PostgreSQL；数据库只保存结构化摘要、manifest、hash、URI。
4. 不得在实时接口同步解析大型 artifact；实时入仓先入结构化指标和 manifest，深度解析走异步 worker。
5. 不得让 LLM agent 直接执行自由 SQL 或写入生产表；只能走白名单视图、只读 API、预算限制和审计。
6. 不得把 `research_valid=false` 的实验默认展示在有效排行榜、优化器 warm-start 或自动演进样本中。

## 2. 已确认决策

1. 数据库使用新 schema `qe_archive`。
2. artifact 根目录使用 repo-root `qe_archive/artifacts`，目录已创建；未来 artifact 文件默认被 `.gitignore` 排除，只保留目录结构。
3. 所有日频且缺少权威涨跌停、停牌处理的 QE 回测都 `research_valid=false`，默认 excluded。
4. `score_total` 是默认排序和优化优先级的单一标量，不替代分项指标和原始指标。
5. 模型分析覆盖所有已实验模型；LSTM / 深度模型从第一阶段支持。
6. 近期模型调参建议使用“自定义演进 batch + Optuna/TPE + 数仓 warm-start + LLM 解释/约束”，旧自动演进待数仓稳定后恢复。
7. 未来允许为 LLM agent 预留只读、受控、审计接口。

## 3. 目标数据范围

### 3.1 必须入仓的数据

每一次 run 必须尽量记录以下数据：

- 身份：run_id、logical_experiment_id、attempt_no、source_system、task_id、loop_id、experiment_id、node_id、创建/开始/完成/归档时间。
- 状态：completed / failed / interrupted / partial_archived、是否 latest attempt、是否 research_valid、无效原因。
- 配置：因子列表、模型族、模型参数、策略参数、数据切分、回测窗口、label horizon、freq、universe、benchmark、成本、执行算法、HMM、股票池、黑名单、Alpha158 开关、Multi-Alpha 分组。
- 数据上下文：Qlib 数据版本、dataset snapshot、交易日范围、股票池版本、limit/suspend 数据是否权威、PIT cutoff、数据刷新审计。
- 指标：IC、Rank IC、ICIR、收益、回撤、IR/Sharpe、换手、成本、胜率、交易数量、容量、执行失败事件、缺数据事件、训练/验证 loss、过拟合/收敛诊断。
- 曲线：净值、benchmark、excess return、drawdown、turnover、IC/RIC 时序、训练/验证 loss、学习率、feature importance trend。
- 因子：每个因子在每次 run 中是否参与、顺序、来源、catalog id、快照评级、独立指标快照、相关性 cluster、组合上下文。
- 因子重要性：每个因子在模型中的重要性、贡献、方向、稳定性、time bucket / epoch 趋势，深度模型需记录 attribution 方法与 feature/time 维度映射。
- 模型 trial：模型族、超参、搜索空间、optimizer 信息、trial number、objective value、score_total、训练状态。
- 持仓/订单/成交/执行事件：分钟策略和权威回测优先记录，日频无权威执行约束只作审计。
- artifact manifest：配置文件、日志、pred/label/params、模型权重、训练曲线、报告、图片、position/trade 文件、MLflow recorder link。
- raw payload：webhook payload、metrics json、enhanced metrics、agent analysis、异常堆栈摘要，保证后续补录不丢信息。

### 3.2 默认不入热表的数据

- 完整模型权重张量。
- 完整 pkl 内容。
- worker workspace 原始目录树。
- 原始分钟 bar 全量数据。
- 大规模逐股票逐分钟 feature matrix。

这些数据只进入 artifact store 或外部数据源，通过 manifest、hash、data version 关联。

### 3.3 QE loop 详情页指标覆盖要求

第二轮讨论确认：数仓第一版必须覆盖当前 QE loop 卡片、loop 展开详情和点击 loop 详情页展示的全部配置与指标。不能只覆盖 IC、年化收益、回撤等核心指标。

当前前端主要来源：

- `frontend/src/app/quantevolver/evolution/[taskId]/page.tsx`：任务页 loop 卡片和展开详情。
- `frontend/src/app/quantevolver/evolution/[taskId]/loops/[loopIndex]/page.tsx`：点击 loop 后的独立详情页。
- `frontend/src/app/quantevolver/evolution/components/LoopDetailPanel.tsx`：主页面右侧 loop 详情看板。
- `frontend/src/app/quantevolver/evolution/components/loopDiagnostics.ts`：从 `metrics_json`、`enhanced_metrics`、`absolute_returns`、`position_summary`、`holding_audit` 合并展示字段。
- `frontend/src/app/quantevolver/components/StrategyConfigCard.tsx`：模型、策略、label、执行算法、尾盘处理、行业黑名单、HMM 等配置展示。
- `frontend/src/app/quantevolver/components/AllStocksTable.tsx`、`TopStocksTable.tsx`、`StockTradeDetail.tsx`：股票级盈亏摘要和逐股票交易明细。
- `frontend/src/app/quantevolver/components/charts/ReturnCurveChart.tsx`、`IcSeriesChart.tsx`、`LossCurveChart.tsx`：收益曲线、IC 序列、训练曲线。

必须结构化入仓的字段范围：

```text
Loop 基础与状态:
  loop_index, loop_id, status, action_type, is_sota, task_id, experiment_id,
  node_id, created_at, started_at, completed_at, updated_at, agent_analysis。

配置展示:
  model_id, model_type, factor_list, factor_count, strategy_id,
  label_type, label_horizon, hold_thresh,
  use_alpha158 / alpha158 / enable_alpha158,
  execution_algo, execution_algo_params,
  unfilled_handler, unfilled_backup_depth, unfilled_params,
  stock_pool, sector_blacklist, blacklist_enabled,
  sector_blacklist_snapshot.items/warning/as_of_date,
  HMM enabled/version/config_id/snapshot_id/model_snapshot_id/signal_preset,
  runtime_profile, custom_params, model_params, strategy_params, dataset_params。

相对基准 / 超额收益核心指标:
  IC, ICIR, Rank_IC, Rank_ICIR,
  sharpe / information_ratio,
  annualized_return,
  max_drawdown,
  annualized_return_no_cost,
  max_drawdown_no_cost,
  sharpe_no_cost / information_ratio_no_cost,
  daily_return, daily_return_no_cost。

绝对收益 / 账户指标:
  initial_capital,
  final_total_value,
  final_account_value / final_account / final_nav_value alias,
  total_return,
  cagr,
  max_drawdown,
  max_drawdown_date,
  sharpe,
  annualized_volatility,
  avg_cash_ratio,
  final_cash,
  final_stock_value,
  final_stock_count,
  final_cash_ratio,
  n_trading_days。

持仓摘要:
  position_count_min / min_position_count / holding_count_min,
  position_count_avg / avg_position_count / holding_count_avg,
  position_count_max / max_position_count / holding_count_max,
  position_count_p95 / p95_position_count / holding_count_p95,
  final_position_count / end_position_count。

交易效率:
  avg_turnover,
  total_turnover,
  annualized_turnover,
  daily_trade_count_avg,
  cost_drag_annualized。

股票级摘要:
  all_stocks / top_stocks / bottom_stocks:
  code, profit, profit_pct, avg_cost, last_price,
  holding_days, first_date, last_date。

逐股票交易:
  stock_trades:
  date, type, price, amount, pnl。

IC 诊断曲线:
  dates, ic_series, rank_ic_series,
  ic_rolling_30d_mean, ic_rolling_30d_std, ic_positive_ratio。

收益与回撤曲线:
  return_dates / dates,
  cumulative_excess_no_cost,
  cumulative_excess_with_cost,
  cumulative_portfolio_with_cost,
  cumulative_benchmark,
  drawdown_series。

训练过程:
  train_loss_curve, val_loss_curve,
  best_epoch, overfit_ratio, convergence_ratio。

预测诊断:
  pred_std, pred_autocorr_1d, pred_rank_turnover, top30_stability。

因子与模型解释:
  factor_analysis.feature_importance,
  feature_importance,
  independent factor metrics snapshot,
  factor classification/rating snapshot。

其他增强诊断:
  stock_pnl_summary, limit_analysis,
  multi_alpha_detail, multi_alpha_analysis,
  enhanced_metrics 原始 payload。
```

落库要求：

- 上述所有 scalar 指标必须进入 `qe_archive.run_metric`，并保留 source key、source path、单位、方向和质量标记。
- 账户级绝对收益和期初/期末资产必须额外进入 `qe_archive.run_account_summary`，便于详情页、排行榜和后续分析直接查询。
- 股票级 `all_stocks` / `top_stocks` / `bottom_stocks` 必须进入 `qe_archive.run_symbol_summary`，不能只留在 JSONB。
- `stock_trades` 必须进入 `qe_archive.run_trade`，并保留原始 `date/type/price/amount/pnl`。
- 收益、回撤、IC、训练 loss 等数组必须进入 `qe_archive.run_curve`。
- 原始 `metrics_json`、`enhanced_metrics`、`summary`、`absolute_returns`、`trade_diagnostics` 等完整 payload 仍写入 `qe_archive.raw_payload`，用于后续补录和字段口径校验。

### 3.4 实验可复现配置契约

第二轮进一步确认：数仓必须保存每个实验/loop 的所有配置和使用参数，目标不是只做展示分析，而是能基于归档记录最大限度重现实验，并核对结果准确性。

结论：设计上必须做到“结构化可查询 + 原始配置不丢失 + artifact/hash 可追溯”三层同时保存。

必须保存的配置与参数范围：

```text
业务入口配置:
  task_id, loop_id, experiment_id, source_system, task_type,
  action_type, evolution_mode, custom_evo/strategy_evo/multi_alpha 标志,
  source task / source loop / base experiment 血缘。

完整原始配置:
  qe_evolution_tasks 原始任务行关键字段,
  qe_evolution_loops.config_json 完整 JSON,
  qe_experiments.custom_params / data_split / result_metrics 原始 JSON,
  Qlib conf.yaml / workflow yaml / rendered config,
  model_params / strategy_params / dataset_params / execution_algo_params / runtime_profile。

模型配置:
  model_id, model_type, model_family, model_catalog_id,
  model class/module, model init params, fit params,
  random seed, device/GPU, early_stop, epochs, batch_size, lr,
  optimizer, loss, feature schema, feature order, lookback window。

因子配置:
  factor_list 原始顺序,
  factor_catalog_id/source/version,
  factor expression/code hash,
  Alpha158/Alpha360 开关,
  factor_set_hash,
  Multi-Alpha group configs。

数据配置:
  freq, label_type, label_horizon, train/valid/test/backtest 起止日期,
  universe, benchmark, qlib provider uri, dataset snapshot,
  PIT cutoff, stock_pool, blacklist snapshot,
  suspend/limit/stk_limit 数据版本和权威性。

策略与执行配置:
  strategy_id, topk, n_drop, hold_thresh,
  initial_cash / initial_capital,
  cost/open_cost/close_cost/min_cost/slippage,
  runtime_mode, bar_freq, backtest_freq,
  execution_algo, execution_algo_params,
  unfilled_handler, backup depth, minute execution policy,
  HMM config/snapshot/signal_preset。

运行环境:
  node_id, node API base, runner script, runner script sha256,
  git commit, git dirty flag, Python/Qlib/MLflow/Torch/LightGBM 等版本,
  environment variables whitelist, conda env name, CUDA/GPU info。

结果校验输入:
  metrics payload sha256,
  enhanced metrics payload sha256,
  artifact manifest sha256,
  qlib recorder id / mlflow run id / artifact uri,
  pred/label/params/portfolio/trade artifact hash。
```

### 5.2.1 QE minute runtime contract update (2026-05-04)

New QE generation and loop completion must persist an explicit minute runtime contract into `qe_experiments.custom_params` and loop/archive payloads:

```json
{
  "runtime_mode": "minute",
  "bar_freq": "1m",
  "backtest_freq": "1min",
  "execution_algo": "TWAP | V24_PLAN | V25_TWO_STAGE | future minute algo",
  "execution_algo_params": {},
  "runtime_contract_version": "qe_minute_runtime_contract_v1",
  "runtime_contract_source": "config_composer | evolution_loop_config | history_backfill_from_loop_config"
}
```

`backtest_freq` and `bar_freq` are derived compatibility/audit fields, not user-selected mode switches. `execution_algo` and `execution_algo_params` remain the variable part of the contract because V25 is not fixed and future minute execution algorithms may be added.

Historical backfill is allowed only when explicit minute evidence exists in `qe_evolution_loops.config_json` or task execution settings. Legacy daily `CLOSE_PRICE` experiments and rows with no minute evidence must stay unconverted and should not enter StrategyPackage/Paper v2 promotion paths.

可复现等级：

```text
full:
  配置、数据版本、代码版本、关键 artifact hash、环境版本完整；
  可以用归档配置重新提交同等实验，并对结果做数值容差比较。

partial:
  核心配置和指标完整，但缺少部分 artifact、环境版本或历史数据快照；
  可以解释和近似复跑，但不能承诺 bit-level 或严格数值一致。

audit_only:
  只保留历史 DB JSON 或部分结果；
  可用于审计和分析，不作为复现实验样本。
```

实现要求：

- `qe_archive.run_config` 保存 canonical config、raw config、各配置分区和 `config_sha256`。
- 新增 `qe_archive.run_reproducibility_manifest` 保存完整复现清单、hash、代码/环境版本、缺失项和复现等级。
- 所有用于复现的文件类配置只在 DB 保存 hash、artifact id、URI 和 manifest，真实文件放 `qe_archive/artifacts`。
- 归档 worker 入仓时必须计算 `config_sha256`、`factor_set_hash`、关键 payload sha256；后续重跑时使用这些 hash 判断配置是否一致。
- 任何字段缺失都不得静默默认；必须写入 `missing_items`，并降低 `reproducibility_level`。

## 4. 存储分级设计

```text
Level 0 Hot Summary
  PostgreSQL qe_archive.run / run_metric / run_priority_score
  用于排行榜、筛选、实时对比、优化 warm-start。

Level 1 Analysis Detail
  run_config / run_data_context / run_factor / run_model_trial / run_model_training_metric
  用于配置复现、因子历史、模型 trial 分析。

Level 2 Time Series Detail
  run_curve / run_position / run_order / run_trade / run_execution_event
  建议 Timescale hypertable 或日期分区，用于图表和执行诊断。

Level 3 Raw Payload
  raw_payload JSONB，保留源系统返回内容和补录依据。

Level 4 Artifact Manifest
  run_artifact 只保存 manifest，真实文件在 qe_archive/artifacts。

Level 5 Cold Artifact
  历史模型、pkl、日志、压缩包、图表，可放 HDD/NAS/对象存储。
```

性能原则：

- PostgreSQL 主库、索引、最近数据和热门曲线应放 SSD / NVMe。
- `qe_archive/artifacts` 内的大文件可以放机械硬盘；前提是在线图表只读结构化表，不在请求路径同步读取 HDD 大文件。
- artifact 深度解析通过异步 worker 做限速、断点重试和 hash 校验。
- 后续如实验量显著增加，可增加 Parquet/DuckDB 冷导出，但权威索引仍在 PostgreSQL。

### 4.1 是否引入 NoSQL / 文档数据库

第一阶段不建议引入 MongoDB、DocumentDB、Elasticsearch/OpenSearch 或其他 NoSQL 作为 QE 数仓主存储。推荐继续采用 PostgreSQL / TimescaleDB + JSONB + 本地 artifact store 的组合。

原因：

1. 当前核心需求是“实验、因子、模型、trial、指标、曲线、交易、artifact manifest”的强关联查询，关系模型更适合。
2. PostgreSQL JSONB 已能保存原始 `metrics_json`、`enhanced_metrics`、agent payload 和可变配置，并支持 GIN 索引；不需要额外文档库来重复存一份 JSON。
3. 引入 NoSQL 会带来双写一致性、权限、备份、查询口径分裂和运维复杂度，不符合第一阶段“先建权威数仓”的目标。
4. 每天十几个到二十几个实验/loop 的规模，对 PostgreSQL 长表 + 索引 + 分区/Timescale 来说很小，瓶颈主要在 artifact 下载和深度解析，不在文档存储。

第一阶段存储形态决策：

```text
权威结构化仓库:
  PostgreSQL / TimescaleDB
  qe_archive.run, run_metric, run_curve, run_factor, run_model_trial,
  run_account_summary, run_symbol_summary, run_trade 等。

半结构化原始 payload:
  PostgreSQL JSONB
  qe_archive.raw_payload, run_config.canonical_config, run_artifact.metadata。

非结构化 artifact:
  文件系统 qe_archive/artifacts
  后续可迁移到 NAS / MinIO / S3-compatible object storage。

临时缓存 / 队列:
  第一阶段可用 DB outbox；如后续需要高并发，可引入 Redis，但 Redis 不作为数仓存储。
```

未来可选扩展，但不是第一阶段必需：

- ClickHouse：当 `run_curve`、`run_trade`、分钟级执行事件达到千万级以上，并且 PostgreSQL 查询明显吃力时，可作为只读 OLAP 加速层。
- Parquet + DuckDB：适合冷数据离线分析、批量导出和本地 notebook 研究，不替代 PostgreSQL 权威索引。
- MinIO / NAS / 对象存储：适合管理大量模型权重、pkl、报告、压缩包，比 NoSQL 更适合 artifact。
- OpenSearch：仅当需要对大量日志、Agent 文本、报告做全文检索时再考虑；不存权威指标。
- Vector DB：仅当 LLM agent 需要对研究报告、日志摘要、实验解释做语义检索时再考虑；不存权威指标和不参与排行榜口径。

## 5. Canonical Run 身份设计

### 5.1 核心概念

- `run_id`：一次真实执行的不可变主键。
- `logical_experiment_id`：业务上的同一实验槽位，例如同一个自定义 loop。
- `attempt_no`：同一个 logical experiment 的第几次执行，用于 rerun。
- `is_latest_attempt`：同一 logical id 下最新一次有效尝试。
- `source_system`：`qe_single` / `qe_evolution` / `qe_custom_evo` / `qe_strategy_evo` / `qe_multi_alpha` / `backfill`。

### 5.2 推荐 run_id 规则

第一阶段使用确定性 text 主键，便于幂等补录：

```text
source_fingerprint = source_system + ':' + experiment_id + ':' + task_id + ':' + loop_id + ':' + attempt_no
run_id = 'qear_' + sha256(source_fingerprint)[0:24]
```

如果某些历史 run 无 attempt_no，则补录时先按完成时间排序推断 attempt_no；无法推断时置为 `attempt_no=1`，并在 `raw_payload` 记录 `provenance_level='inferred'`。

### 5.3 rerun 行为

- rerun 不覆盖旧 run。
- 同一个 `logical_experiment_id` 下新增 `attempt_no=N+1`。
- 旧 run 保留用于比较和审计。
- `is_latest_attempt` 只允许一个 true；写入新 attempt 后旧 attempt 置 false。
- 默认排行榜只展示 latest attempt，但分析页可切换显示全部 attempts。

## 6. 数据库结构详细设计

### 6.1 迁移文件

新增文件建议：

```text
backend/db/init_qe_archive_schema.py
backend/migrations/qe_archive_schema.sql  （可选，便于人工审阅）
```

要求：

- DDL 幂等：全部使用 `CREATE ... IF NOT EXISTS`、`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`、`CREATE INDEX IF NOT EXISTS`。
- DDL 不放在业务 service 构造函数中执行。
- 初始化可由显式脚本或启动 bootstrap 调用，但不应在每个请求路径执行。
- 所有表记录 `created_at`、`updated_at` 或至少 `created_at`。
- JSONB 字段保留源 payload，但查询关键字段必须拆列。
- DDL 必须为新建 schema/table/column 写入 PostgreSQL COMMENT 元数据；每张表使用 `COMMENT ON TABLE`，每个字段使用 `COMMENT ON COLUMN`，注释需说明业务语义、来源或单位，方便后续程序、LLM agent 和数据治理工具读取。
- COMMENT 覆盖率必须进入测试或 review checklist；新增字段没有 comment 不得视为完成。
- QE 归档开发不得影响当前 QE 生产运行：运行时接入必须显式 feature flag 或后续单独确认，默认不启用，不重启生产 backend 8001，不改变现有 webhook 成败语义。

### 6.2 DDL 草案：核心 run / config / data / metric

以下是第一阶段推荐 DDL 骨架。实施时可按模块拆分，但语义应保持一致。

```sql
CREATE SCHEMA IF NOT EXISTS qe_archive;

CREATE TABLE IF NOT EXISTS qe_archive.schema_version (
    version             TEXT PRIMARY KEY,
    applied_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    description         TEXT
);

CREATE TABLE IF NOT EXISTS qe_archive.run (
    run_id                  TEXT PRIMARY KEY,
    logical_experiment_id   TEXT NOT NULL,
    attempt_no              INTEGER NOT NULL DEFAULT 1,
    is_latest_attempt       BOOLEAN NOT NULL DEFAULT TRUE,
    source_system           TEXT NOT NULL,
    run_type                TEXT NOT NULL,
    task_id                 TEXT,
    loop_id                 TEXT,
    loop_index              INTEGER,
    experiment_id           TEXT,
    node_id                 TEXT,
    model_catalog_id        BIGINT,
    model_family            TEXT,
    model_type              TEXT,
    factor_set_hash         TEXT,
    factor_count            INTEGER,
    freq                    TEXT,
    label_horizon           INTEGER,
    status                  TEXT NOT NULL,
    research_valid          BOOLEAN NOT NULL DEFAULT TRUE,
    invalid_reason          TEXT,
    exclusion_tags          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    score_total             DOUBLE PRECISION,
    score_version           TEXT,
    priority_rank           INTEGER,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    archived_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_created_at       TIMESTAMPTZ,
    source_updated_at       TIMESTAMPTZ,
    CONSTRAINT uq_qear_run_logical_attempt UNIQUE (logical_experiment_id, attempt_no),
    CONSTRAINT ck_qear_run_status CHECK (status IN (
        'pending','running','completed','failed','interrupted','partial_archived','archived'
    ))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_run_latest
    ON qe_archive.run(logical_experiment_id)
    WHERE is_latest_attempt = TRUE;

CREATE INDEX IF NOT EXISTS idx_qear_run_valid_score
    ON qe_archive.run(research_valid, score_total DESC NULLS LAST, completed_at DESC);

CREATE INDEX IF NOT EXISTS idx_qear_run_task_loop
    ON qe_archive.run(task_id, loop_index, loop_id);

CREATE INDEX IF NOT EXISTS idx_qear_run_model
    ON qe_archive.run(model_family, model_type, label_horizon, freq, completed_at DESC);

CREATE INDEX IF NOT EXISTS idx_qear_run_factor_hash
    ON qe_archive.run(factor_set_hash, completed_at DESC);

CREATE TABLE IF NOT EXISTS qe_archive.run_source (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    source_system           TEXT NOT NULL,
    source_type             TEXT NOT NULL,
    source_id               TEXT NOT NULL,
    source_sub_id           TEXT,
    source_status           TEXT,
    source_uri              TEXT,
    recorder_experiment_id  TEXT,
    recorder_id             TEXT,
    mlflow_tracking_uri     TEXT,
    mlflow_artifact_uri     TEXT,
    qlib_recorder_name      TEXT,
    node_api_base_url       TEXT,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_run_source_source
    ON qe_archive.run_source(source_system, source_type, source_id, COALESCE(source_sub_id, ''));

CREATE INDEX IF NOT EXISTS idx_qear_run_source_run
    ON qe_archive.run_source(run_id);

CREATE TABLE IF NOT EXISTS qe_archive.run_config (
    run_id                  TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    config_schema_version   TEXT NOT NULL,
    config_sha256           TEXT NOT NULL,
    canonical_config        JSONB NOT NULL,
    raw_config              JSONB,
    factor_list             JSONB NOT NULL DEFAULT '[]'::jsonb,
    factor_set_hash         TEXT,
    model_config            JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_params            JSONB NOT NULL DEFAULT '{}'::jsonb,
    strategy_config         JSONB NOT NULL DEFAULT '{}'::jsonb,
    backtest_config         JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_split              JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_config        JSONB NOT NULL DEFAULT '{}'::jsonb,
    runtime_flags           JSONB NOT NULL DEFAULT '{}'::jsonb,
    agent_context           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_run_config_hash
    ON qe_archive.run_config(config_sha256);

CREATE INDEX IF NOT EXISTS gin_qear_run_config_canonical
    ON qe_archive.run_config USING GIN(canonical_config);

CREATE TABLE IF NOT EXISTS qe_archive.run_reproducibility_manifest (
    run_id                      TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    manifest_schema_version     TEXT NOT NULL,
    reproducibility_level       TEXT NOT NULL,
    verification_status         TEXT NOT NULL DEFAULT 'not_verified',
    config_sha256               TEXT,
    canonical_config_sha256     TEXT,
    raw_config_sha256           TEXT,
    factor_set_hash             TEXT,
    qlib_config_sha256          TEXT,
    model_params_sha256         TEXT,
    strategy_config_sha256      TEXT,
    data_context_sha256         TEXT,
    metrics_payload_sha256      TEXT,
    enhanced_metrics_sha256     TEXT,
    artifact_manifest_sha256    TEXT,
    git_commit                  TEXT,
    git_dirty                   BOOLEAN,
    runner_script               TEXT,
    runner_script_sha256        TEXT,
    python_version              TEXT,
    qlib_version                TEXT,
    mlflow_version              TEXT,
    torch_version               TEXT,
    package_versions            JSONB NOT NULL DEFAULT '{}'::jsonb,
    random_seed                 BIGINT,
    deterministic_flags         JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_config_paths         JSONB NOT NULL DEFAULT '{}'::jsonb,
    required_artifact_types     TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    missing_items               JSONB NOT NULL DEFAULT '[]'::jsonb,
    manifest_json               JSONB NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_qear_repro_level CHECK (reproducibility_level IN ('full','partial','audit_only')),
    CONSTRAINT ck_qear_repro_status CHECK (verification_status IN ('not_verified','verified','failed','not_reproducible'))
);

CREATE INDEX IF NOT EXISTS idx_qear_repro_level
    ON qe_archive.run_reproducibility_manifest(reproducibility_level, verification_status);

CREATE TABLE IF NOT EXISTS qe_archive.run_data_context (
    id                          BIGSERIAL PRIMARY KEY,
    run_id                      TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    context_type                TEXT NOT NULL DEFAULT 'primary',
    freq                        TEXT,
    market                      TEXT,
    universe                    TEXT,
    benchmark                   TEXT,
    train_start                 DATE,
    train_end                   DATE,
    valid_start                 DATE,
    valid_end                   DATE,
    test_start                  DATE,
    test_end                    DATE,
    backtest_start              DATE,
    backtest_end                DATE,
    label_horizon               INTEGER,
    qlib_provider_uri           TEXT,
    qlib_dataset_version        TEXT,
    dataset_snapshot_id         TEXT,
    feature_snapshot_id         TEXT,
    factor_cache_snapshot_id    TEXT,
    data_version_hash           TEXT,
    pit_cutoff_date             DATE,
    limit_handling              TEXT,
    suspend_handling            TEXT,
    limit_suspend_authoritative BOOLEAN NOT NULL DEFAULT FALSE,
    cost_config                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    stock_pool_config           JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_quality_flags          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_data_context_run
    ON qe_archive.run_data_context(run_id);

CREATE INDEX IF NOT EXISTS idx_qear_data_context_dates
    ON qe_archive.run_data_context(freq, backtest_start, backtest_end);

CREATE TABLE IF NOT EXISTS qe_archive.run_account_summary (
    run_id                  TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    initial_capital         DOUBLE PRECISION,
    final_total_value       DOUBLE PRECISION,
    final_account_value     DOUBLE PRECISION,
    final_nav_value         DOUBLE PRECISION,
    total_return            DOUBLE PRECISION,
    cagr                    DOUBLE PRECISION,
    max_drawdown            DOUBLE PRECISION,
    max_drawdown_date       DATE,
    sharpe                  DOUBLE PRECISION,
    annualized_volatility   DOUBLE PRECISION,
    avg_cash_ratio          DOUBLE PRECISION,
    final_cash              DOUBLE PRECISION,
    final_stock_value       DOUBLE PRECISION,
    final_stock_count       INTEGER,
    final_cash_ratio        DOUBLE PRECISION,
    n_trading_days          INTEGER,
    position_count_min      DOUBLE PRECISION,
    position_count_avg      DOUBLE PRECISION,
    position_count_max      DOUBLE PRECISION,
    position_count_p95      DOUBLE PRECISION,
    source_payload_path     TEXT,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_account_summary_return
    ON qe_archive.run_account_summary(total_return DESC NULLS LAST, max_drawdown ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS qe_archive.metric_taxonomy (
    metric_key              TEXT PRIMARY KEY,
    metric_group            TEXT NOT NULL,
    display_name            TEXT NOT NULL,
    unit                    TEXT,
    direction               TEXT NOT NULL DEFAULT 'higher_better',
    canonical_description   TEXT,
    source_aliases          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS qe_archive.run_metric (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    metric_key          TEXT NOT NULL,
    metric_scope        TEXT NOT NULL DEFAULT 'run',
    period_start        DATE,
    period_end          DATE,
    horizon             INTEGER,
    freq                TEXT,
    value_num           DOUBLE PRECISION,
    value_text          TEXT,
    value_json          JSONB,
    unit                TEXT,
    direction           TEXT,
    source_key          TEXT,
    source_payload_path TEXT,
    quality_flag        TEXT NOT NULL DEFAULT 'ok',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_metric_key_value
    ON qe_archive.run_metric(metric_key, value_num DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_qear_metric_run
    ON qe_archive.run_metric(run_id, metric_key);

CREATE INDEX IF NOT EXISTS idx_qear_metric_scope_period
    ON qe_archive.run_metric(metric_scope, period_start, period_end);

CREATE TABLE IF NOT EXISTS qe_archive.run_curve (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    curve_key       TEXT NOT NULL,
    ts              TIMESTAMPTZ,
    trade_date      DATE,
    step            INTEGER,
    epoch           INTEGER,
    split_name      TEXT,
    value_num       DOUBLE PRECISION,
    value_json      JSONB,
    source_key      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_curve_run_key_ts
    ON qe_archive.run_curve(run_id, curve_key, ts, step);

CREATE INDEX IF NOT EXISTS idx_qear_curve_key_date
    ON qe_archive.run_curve(curve_key, trade_date);
```

### 6.3 DDL 草案：因子 / 模型 / 交易 / artifact / 任务

```sql
CREATE TABLE IF NOT EXISTS qe_archive.run_factor (
    id                           BIGSERIAL PRIMARY KEY,
    run_id                       TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    factor_catalog_id            BIGINT,
    factor_name                  TEXT NOT NULL,
    factor_source                TEXT,
    factor_version               TEXT,
    factor_order                 INTEGER,
    factor_group                 TEXT,
    factor_classification        JSONB NOT NULL DEFAULT '{}'::jsonb,
    factor_expression_hash       TEXT,
    factor_asset_hash            TEXT,
    inclusion_reason             TEXT,
    inclusion_source             TEXT,
    is_alpha158                  BOOLEAN NOT NULL DEFAULT FALSE,
    independent_metrics_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    official_rating_snapshot     JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_cluster          TEXT,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_run_factor
    ON qe_archive.run_factor(run_id, factor_name, COALESCE(factor_source, ''));

CREATE INDEX IF NOT EXISTS idx_qear_factor_name
    ON qe_archive.run_factor(factor_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_qear_factor_catalog
    ON qe_archive.run_factor(factor_catalog_id);

CREATE TABLE IF NOT EXISTS qe_archive.run_factor_importance (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    factor_catalog_id   BIGINT,
    factor_name         TEXT NOT NULL,
    feature_name        TEXT,
    feature_index       INTEGER,
    model_family        TEXT,
    model_type          TEXT,
    method              TEXT NOT NULL,
    method_version      TEXT,
    split_name          TEXT,
    time_bucket         TEXT,
    epoch               INTEGER,
    step                INTEGER,
    importance_value    DOUBLE PRECISION NOT NULL,
    normalized_value    DOUBLE PRECISION,
    signed_value        DOUBLE PRECISION,
    rank_in_run         INTEGER,
    sample_count        INTEGER,
    reliability         TEXT NOT NULL DEFAULT 'unknown',
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_importance_run
    ON qe_archive.run_factor_importance(run_id, method, split_name);

CREATE INDEX IF NOT EXISTS idx_qear_importance_factor
    ON qe_archive.run_factor_importance(factor_name, method, created_at DESC);

CREATE TABLE IF NOT EXISTS qe_archive.run_factor_pair (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    factor_a_catalog_id BIGINT,
    factor_b_catalog_id BIGINT,
    factor_a_name       TEXT NOT NULL,
    factor_b_name       TEXT NOT NULL,
    corr_method         TEXT NOT NULL DEFAULT 'spearman',
    corr_value          DOUBLE PRECISION,
    corr_as_of_date     DATE,
    corr_window         TEXT,
    same_cluster        BOOLEAN,
    synergy_score       DOUBLE PRECISION,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_qear_factor_pair_order CHECK (factor_a_name < factor_b_name)
);

CREATE INDEX IF NOT EXISTS idx_qear_factor_pair_run
    ON qe_archive.run_factor_pair(run_id);

CREATE INDEX IF NOT EXISTS idx_qear_factor_pair_names
    ON qe_archive.run_factor_pair(factor_a_name, factor_b_name);

CREATE TABLE IF NOT EXISTS qe_archive.run_symbol_summary (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    symbol              TEXT NOT NULL,
    source_list         TEXT NOT NULL DEFAULT 'all_stocks',
    profit              DOUBLE PRECISION,
    profit_pct          DOUBLE PRECISION,
    avg_cost            DOUBLE PRECISION,
    last_price          DOUBLE PRECISION,
    holding_days        INTEGER,
    first_date          DATE,
    last_date           DATE,
    rank_in_list        INTEGER,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_symbol_summary
    ON qe_archive.run_symbol_summary(run_id, source_list, symbol);

CREATE INDEX IF NOT EXISTS idx_qear_symbol_summary_symbol
    ON qe_archive.run_symbol_summary(symbol, first_date, last_date);

CREATE INDEX IF NOT EXISTS idx_qear_symbol_summary_profit
    ON qe_archive.run_symbol_summary(run_id, profit DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS qe_archive.run_model_trial (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    model_catalog_id        BIGINT,
    model_family            TEXT NOT NULL,
    model_type              TEXT NOT NULL,
    trial_source            TEXT NOT NULL DEFAULT 'qe',
    optimizer_name          TEXT,
    optimizer_study_name    TEXT,
    optimizer_trial_number  INTEGER,
    search_space            JSONB NOT NULL DEFAULT '{}'::jsonb,
    params                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    fixed_params            JSONB NOT NULL DEFAULT '{}'::jsonb,
    objective_name          TEXT,
    objective_value         DOUBLE PRECISION,
    objective_values        JSONB,
    score_total             DOUBLE PRECISION,
    score_version           TEXT,
    trial_state             TEXT NOT NULL DEFAULT 'complete',
    pruned_reason           TEXT,
    train_wall_seconds      DOUBLE PRECISION,
    gpu_info                JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_model_trial_model_score
    ON qe_archive.run_model_trial(model_family, model_type, score_total DESC NULLS LAST, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_qear_model_trial_optimizer
    ON qe_archive.run_model_trial(optimizer_name, optimizer_study_name, optimizer_trial_number);

CREATE TABLE IF NOT EXISTS qe_archive.run_model_training_metric (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    trial_id        BIGINT REFERENCES qe_archive.run_model_trial(id) ON DELETE CASCADE,
    metric_key      TEXT NOT NULL,
    split_name      TEXT,
    epoch           INTEGER,
    step            INTEGER,
    value_num       DOUBLE PRECISION,
    value_json      JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_training_metric_run
    ON qe_archive.run_model_training_metric(run_id, metric_key, split_name, epoch, step);

CREATE TABLE IF NOT EXISTS qe_archive.run_position (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    trade_date          DATE NOT NULL,
    symbol              TEXT NOT NULL,
    weight              DOUBLE PRECISION,
    shares              DOUBLE PRECISION,
    price               DOUBLE PRECISION,
    score               DOUBLE PRECISION,
    rank_in_portfolio   INTEGER,
    return_contribution DOUBLE PRECISION,
    industry_code       TEXT,
    industry_name       TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_position_run_date
    ON qe_archive.run_position(run_id, trade_date);

CREATE INDEX IF NOT EXISTS idx_qear_position_symbol_date
    ON qe_archive.run_position(symbol, trade_date);

CREATE TABLE IF NOT EXISTS qe_archive.run_order (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    order_uid           TEXT,
    trade_date          DATE,
    ts                  TIMESTAMPTZ,
    symbol              TEXT NOT NULL,
    side                TEXT,
    target_weight       DOUBLE PRECISION,
    target_qty          DOUBLE PRECISION,
    limit_price         DOUBLE PRECISION,
    order_price         DOUBLE PRECISION,
    order_qty           DOUBLE PRECISION,
    status              TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_order_run_date
    ON qe_archive.run_order(run_id, trade_date, ts);

CREATE TABLE IF NOT EXISTS qe_archive.run_trade (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    trade_uid           TEXT,
    order_uid           TEXT,
    trade_date          DATE,
    ts                  TIMESTAMPTZ,
    symbol              TEXT NOT NULL,
    side                TEXT,
    price               DOUBLE PRECISION,
    quantity            DOUBLE PRECISION,
    amount              DOUBLE PRECISION,
    commission          DOUBLE PRECISION,
    tax                 DOUBLE PRECISION,
    slippage            DOUBLE PRECISION,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_trade_run_date
    ON qe_archive.run_trade(run_id, trade_date, ts);

CREATE TABLE IF NOT EXISTS qe_archive.run_execution_event (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    event_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trade_date      DATE,
    symbol          TEXT,
    event_type      TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'info',
    message         TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_exec_event_run
    ON qe_archive.run_execution_event(run_id, event_type, event_ts DESC);

CREATE TABLE IF NOT EXISTS qe_archive.run_artifact (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    artifact_type           TEXT NOT NULL,
    artifact_name           TEXT NOT NULL,
    storage_tier            TEXT NOT NULL DEFAULT 'local_hot',
    artifact_uri            TEXT NOT NULL,
    local_rel_path          TEXT,
    source_system           TEXT,
    source_uri              TEXT,
    source_node_id          TEXT,
    sha256                  TEXT,
    size_bytes              BIGINT,
    content_type            TEXT,
    compression             TEXT,
    collected_status        TEXT NOT NULL DEFAULT 'pending',
    collected_at            TIMESTAMPTZ,
    parser_status           TEXT NOT NULL DEFAULT 'not_required',
    parser_error            TEXT,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_artifact_run_type
    ON qe_archive.run_artifact(run_id, artifact_type, collected_status);

CREATE INDEX IF NOT EXISTS idx_qear_artifact_sha
    ON qe_archive.run_artifact(sha256);

CREATE TABLE IF NOT EXISTS qe_archive.raw_payload (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    payload_type        TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_id           TEXT,
    payload_sha256      TEXT,
    payload_json        JSONB,
    payload_text        TEXT,
    provenance_level    TEXT NOT NULL DEFAULT 'direct',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_raw_payload_run
    ON qe_archive.raw_payload(run_id, payload_type, created_at DESC);

CREATE INDEX IF NOT EXISTS gin_qear_raw_payload_json
    ON qe_archive.raw_payload USING GIN(payload_json);

CREATE TABLE IF NOT EXISTS qe_archive.run_priority_score (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    score_version           TEXT NOT NULL,
    score_total             DOUBLE PRECISION,
    alpha_score             DOUBLE PRECISION,
    return_score            DOUBLE PRECISION,
    risk_score              DOUBLE PRECISION,
    stability_score         DOUBLE PRECISION,
    execution_score         DOUBLE PRECISION,
    novelty_score           DOUBLE PRECISION,
    data_quality_score      DOUBLE PRECISION,
    penalty_score           DOUBLE PRECISION,
    score_components        JSONB NOT NULL DEFAULT '{}'::jsonb,
    exclusion_reason        TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_priority_run_version
    ON qe_archive.run_priority_score(run_id, score_version);

CREATE INDEX IF NOT EXISTS idx_qear_priority_score
    ON qe_archive.run_priority_score(score_version, score_total DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS qe_archive.optimization_candidate (
    candidate_id            TEXT PRIMARY KEY,
    candidate_type          TEXT NOT NULL,
    generated_by            TEXT NOT NULL,
    generator_version       TEXT,
    status                  TEXT NOT NULL DEFAULT 'proposed',
    priority_score          DOUBLE PRECISION,
    model_family            TEXT,
    model_type              TEXT,
    factor_set_hash         TEXT,
    label_horizon           INTEGER,
    freq                    TEXT,
    candidate_config        JSONB NOT NULL,
    evidence_summary        JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_run_ids          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    submitted_task_id       TEXT,
    submitted_loop_ids      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    result_run_ids          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_by              TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_candidate_status_priority
    ON qe_archive.optimization_candidate(status, priority_score DESC NULLS LAST, created_at DESC);

CREATE TABLE IF NOT EXISTS qe_archive.agent_query_audit (
    audit_id            BIGSERIAL PRIMARY KEY,
    agent_name          TEXT NOT NULL,
    tool_name           TEXT NOT NULL,
    request_id          TEXT,
    user_intent         TEXT,
    query_scope         JSONB NOT NULL DEFAULT '{}'::jsonb,
    row_count           INTEGER,
    token_budget        INTEGER,
    allowed             BOOLEAN NOT NULL DEFAULT TRUE,
    denial_reason       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_agent_audit_time
    ON qe_archive.agent_query_audit(agent_name, created_at DESC);

CREATE TABLE IF NOT EXISTS qe_archive.outbox_event (
    event_id            TEXT PRIMARY KEY,
    event_type          TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    source_sub_id       TEXT,
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL DEFAULT 'pending',
    retry_count         INTEGER NOT NULL DEFAULT 0,
    next_retry_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by           TEXT,
    locked_at           TIMESTAMPTZ,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_outbox_pending
    ON qe_archive.outbox_event(status, next_retry_at, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qear_outbox_source_terminal
    ON qe_archive.outbox_event(event_type, source_system, source_id, COALESCE(source_sub_id, ''));

CREATE TABLE IF NOT EXISTS qe_archive.archive_job (
    job_id              TEXT PRIMARY KEY,
    event_id            TEXT REFERENCES qe_archive.outbox_event(event_id) ON DELETE SET NULL,
    run_id              TEXT REFERENCES qe_archive.run(run_id) ON DELETE SET NULL,
    job_type            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    level               TEXT NOT NULL DEFAULT 'A',
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    error_message       TEXT,
    stats               JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qear_archive_job_status
    ON qe_archive.archive_job(status, level, created_at);
```

### 6.4 Timescale / 分区建议

第一阶段如果本地 TimescaleDB 已可用，可将以下表转为 hypertable：

- `qe_archive.run_curve`：时间列 `ts`，无 `ts` 时用 `trade_date` 派生日末时间。
- `qe_archive.run_position`：时间列 `trade_date`。
- `qe_archive.run_order`：时间列 `ts`。
- `qe_archive.run_trade`：时间列 `ts`。
- `qe_archive.run_execution_event`：时间列 `event_ts`。

如果不启用 Timescale，则先使用 B-tree 索引；等数据规模达到百万级以上再做日期分区或 hypertable 迁移。不要为了第一阶段过早引入复杂迁移。

### 6.5 表关系说明

```text
qe_archive.run
  1--1 run_config / run_reproducibility_manifest / run_account_summary
  1--N run_source / run_data_context / run_metric / run_curve
  1--N run_factor / run_factor_importance / run_factor_pair / run_symbol_summary
  1--N run_model_trial / run_model_training_metric
  1--N run_position / run_order / run_trade / run_execution_event
  1--N run_artifact / raw_payload / run_priority_score

optimization_candidate
  N--N source_run_ids / result_run_ids（数组第一阶段即可，后续可拆关联表）

outbox_event
  1--N archive_job
```

## 7. 服务模块详细设计

新增包建议：

```text
backend/services/qe_archive/
  __init__.py
  models.py
  repository.py
  service.py
  config_builder.py
  metric_taxonomy.py
  metric_extractor.py
  factor_snapshot.py
  model_attribution.py
  artifact_collector.py
  score.py
  worker.py
  scanner.py
  backfill.py
  agent_views.py
```

### 7.1 `repository.py`

职责：

- 封装所有 `qe_archive` 表写入和查询。
- 提供幂等 upsert：`upsert_run()`、`upsert_run_config()`、`upsert_metrics()`、`upsert_artifacts()`。
- 统一事务边界，避免 service 中散落 SQL。
- 所有写入方法接受 `run_id`，不在 repository 内自行推断业务来源。

关键方法建议：

```python
class QEArchiveRepository:
    def upsert_run(self, run: ArchiveRun) -> None: ...
    def mark_latest_attempt(self, logical_experiment_id: str, run_id: str) -> None: ...
    def insert_outbox_event(self, event: OutboxEvent) -> bool: ...
    def claim_outbox_events(self, worker_id: str, limit: int) -> list[OutboxEvent]: ...
    def complete_outbox_event(self, event_id: str) -> None: ...
    def fail_outbox_event(self, event_id: str, error: str, retry_after_seconds: int) -> None: ...
    def upsert_metric_batch(self, run_id: str, metrics: list[MetricRecord]) -> None: ...
    def upsert_curve_batch(self, run_id: str, curves: list[CurveRecord]) -> None: ...
    def upsert_factor_batch(self, run_id: str, factors: list[FactorRecord]) -> None: ...
    def upsert_artifact_manifest(self, run_id: str, artifacts: list[ArtifactRecord]) -> None: ...
```

### 7.2 `service.py`

职责：

- 编排一次 archive job 的 Level A / B / C 流程。
- 根据 source_system 加载 `qe_experiments`、`qe_evolution_tasks`、`qe_evolution_loops`。
- 构建 canonical run identity。
- 调用 config、metric、factor、artifact、score 子模块。
- 捕获可恢复错误，保证主 run 可 partial archive。

关键方法建议：

```python
class QEArchiveService:
    async def enqueue_from_single_experiment(self, experiment_id: str, reason: str) -> bool: ...
    async def enqueue_from_loop(self, task_id: str, loop_id: str, reason: str) -> bool: ...
    async def archive_event(self, event: OutboxEvent) -> ArchiveResult: ...
    async def archive_run_level_a(self, source: SourceContext) -> str: ...
    async def archive_run_level_b(self, run_id: str, source: SourceContext) -> None: ...
    async def archive_run_level_c(self, run_id: str, source: SourceContext) -> None: ...
```

### 7.3 `config_builder.py`

职责：

- 将不同入口的 `config_json`、`custom_params`、`data_split`、`strategy_config`、`model_params` 合并为 canonical config。
- 输出 `config_sha256`、`factor_set_hash`。
- 对关键字段做显式缺失标记，不静默默认。
- 记录字段来源路径，例如 `qe_evolution_loops.config_json.factor_list`。

canonical config 必须至少包含：

```json
{
  "schema_version": "qe_archive_config_v1",
  "source": {"source_system": "qe_custom_evo", "task_id": "...", "loop_id": "..."},
  "model": {"family": "lstm", "type": "LSTM", "params": {}, "catalog_id": null},
  "factors": [{"name": "...", "source": "...", "catalog_id": null, "order": 0}],
  "data": {"freq": "1min", "label_horizon": 5, "segments": {}, "universe": "...", "benchmark": "..."},
  "backtest": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "cost": {}, "strategy": {}},
  "execution": {"algo": "...", "limit_handling": "...", "suspend_handling": "..."},
  "runtime_flags": {"disable_alpha158": true, "multi_alpha": false}
}
```

### 7.4 `metric_taxonomy.py` 与 `metric_extractor.py`

`metric_taxonomy.py` 维护 Qlib / enhanced metrics / QE DB 字段到 canonical metric key 的映射，并初始化 `qe_archive.metric_taxonomy`。新增指标应优先扩展 taxonomy 和 extractor，而不是新增列。

第一版 canonical metric 示例：

```text
alpha.ic_mean
alpha.icir
alpha.rank_ic_mean
alpha.rank_icir
portfolio.annualized_return.with_cost
portfolio.annualized_return.without_cost
portfolio.max_drawdown.with_cost
portfolio.max_drawdown.without_cost
portfolio.information_ratio.with_cost
portfolio.information_ratio.without_cost
portfolio.daily_excess_return_mean.with_cost
portfolio.daily_excess_return_mean.without_cost
portfolio.turnover.mean
portfolio.cost.mean
trade.win_rate
trade.trade_count
execution.limit_block_count
execution.suspend_block_count
model.train_loss.final
model.valid_loss.final
model.overfit_gap
model.convergence_score
data.coverage_ratio
archive.quality_score
```

Qlib source key 映射示例：

```text
IC                                                   -> alpha.ic_mean
ICIR                                                 -> alpha.icir
Rank IC                                             -> alpha.rank_ic_mean
Rank ICIR                                           -> alpha.rank_icir
1day.excess_return_with_cost.annualized_return      -> portfolio.annualized_return.with_cost
1day.excess_return_with_cost.max_drawdown           -> portfolio.max_drawdown.with_cost
1day.excess_return_with_cost.information_ratio      -> portfolio.information_ratio.with_cost
1day.excess_return_without_cost.annualized_return   -> portfolio.annualized_return.without_cost
1day.excess_return_without_cost.max_drawdown        -> portfolio.max_drawdown.without_cost
1day.excess_return_without_cost.information_ratio   -> portfolio.information_ratio.without_cost
```

注意：source key 中的 `1day` 不应写死为日频有效性。extractor 应解析真实 freq，并结合 `run_data_context.freq` 判断研究有效性。

`metric_extractor.py` 负责从 `qe_experiments.result_metrics`、`qe_evolution_loops.metrics_json`、enhanced metrics、Qlib recorder 摘要中提取指标。缺失值记录 `quality_flag='missing'`，不得填 0。

### 7.5 `factor_snapshot.py`

职责：

- 将本次 run 的因子列表与 `aistock_factor_catalog` 对齐。
- 抓取每个因子在 run 完成时可见的独立指标快照：`aistock_factor_metrics`、`aistock_factor_monthly_ic`、`qe_factor_official_ratings`、`qe_factor_classification`。
- 抓取本组合内部相关性：`qe_factor_correlations`。
- 生成 `run_factor` 与 `run_factor_pair`。

原则：

- 不复制因子源码全文，只保存 factor id、source、version/hash、表达式 hash、asset hash 和指标快照。
- 对无法匹配 catalog 的历史因子，仍然入 `run_factor`，`factor_catalog_id=NULL`，并记录 `metadata.match_status='unmatched'`。
- 相关性没有当日数据时，使用最近一次 `computed_at <= run.completed_at` 的快照，并记录 as-of date。

### 7.6 `model_attribution.py`

职责：统一所有模型族的因子重要性提取，产出 `run_factor_importance` 和必要的 `run_curve`。重点是 LSTM 等深度模型必须保存 feature/time attribution，而不是误把神经网络权重当作因子权重。

接口建议：

```python
class ModelAttributionAdapter(Protocol):
    model_families: set[str]
    async def extract(self, context: AttributionContext) -> list[FactorImportanceRecord]: ...
```

第一阶段适配策略：

```text
Tree / GBDT / LGBM / XGBoost:
  - native gain / split importance
  - permutation importance
  - 可选 SHAP summary，作为 Level C 深度解析

Linear / ElasticNet / Ridge:
  - 标准化系数
  - permutation importance
  - 系数符号稳定性

LSTM / GRU / TCN / MLP / PTNN:
  - feature order + lookback window 必须入仓
  - occlusion / ablation importance 作为第一优先级模型无关方法
  - gradient x input / integrated gradients 作为深度模型 attribution
  - 记录 feature 维度、time step 聚合、split、sample_count、method_version

Attention / Transformer / TabNet（如后续出现）:
  - attention / mask 只能作为辅助解释
  - 必须配合 permutation 或 ablation 做稳健校验
```

记录这些数据有分析价值：

- 判断因子在不同模型和不同超参下是否稳定贡献。
- 判断 LSTM 是否只依赖少数近期特征或过度拟合某些噪声因子。
- 支持因子组合优化时做“模型适配性”评分。
- 支持因子衰退和组合冗余分析。
- 支持后续 LLM agent 给出有证据的调参/换因子解释。

### 7.7 `artifact_collector.py`

职责：通过 node API 或已归档本地 cache 收集 artifact manifest，将文件落到 `qe_archive/artifacts`，并计算 sha256、大小、content type、compression。

目录规范：

```text
qe_archive/artifacts/
  runs/
    YYYY/
      MM/
        {run_id}/
          manifest.json
          configs/
          metrics/
          qlib/
          mlflow/
          models/
          predictions/
          positions/
          trades/
          logs/
          reports/
          attribution/
  backfill/
    YYYYMMDD_HHMMSS/
      inventory.json
      missing_artifacts.json
```

artifact 类型白名单：

```text
config_yaml
canonical_config_json
metrics_json
enhanced_metrics_json
mlflow_params_bundle
qlib_recorder_manifest
model_params_pkl
model_weight
pred_pkl
label_pkl
portfolio_analysis
position_file
trade_file
training_curve
feature_order
attribution_report
run_log
error_log
html_report
image_report
compressed_workspace_snapshot
```

关键边界：

- `source_uri` 可以记录 `node_api://{node_id}/tasks/{task_id}/loops/{loop_id}/...` 或 MLflow artifact URI。
- `local_rel_path` 只能是 `qe_archive/artifacts` 下的相对路径。
- 不保存 worker 本地绝对路径作为可读取路径。
- 下载失败不得导致 run 归档整体失败；应记录 `collected_status='failed'` 和错误。

### 7.8 `score.py`

第一版 score version：`qe_score_v1.0.0`。

推荐公式：

```text
score_total =
    0.28 * alpha_score
  + 0.22 * risk_adjusted_return_score
  + 0.18 * oos_return_score
  + 0.12 * stability_score
  + 0.10 * execution_score
  + 0.05 * novelty_score
  + 0.05 * data_quality_score
  - penalty_score
```

分项说明：

- `alpha_score`：Rank IC、ICIR、Rank ICIR 的稳健标准化组合。
- `risk_adjusted_return_score`：with-cost IR/Sharpe、最大回撤惩罚。
- `oos_return_score`：回测区间收益和样本外收益，优先使用 with-cost。
- `stability_score`：滚动窗口 IC 符号一致性、月度衰退、训练/验证 gap、seed 稳定性。
- `execution_score`：换手、交易成本、涨跌停/停牌阻塞、缺分钟数据、未成交比例。
- `novelty_score`：相对已有高分组合的因子多样性和非冗余增益，不奖励纯随机新组合。
- `data_quality_score`：配置完整度、artifact 完整度、数据版本可信度、PIT/limit/suspend 可信度。
- `penalty_score`：过拟合、数据泄漏疑点、日频无权威约束、异常样本、指标缺失。

硬规则：

- `research_valid=false` 时 `score_total=NULL`。
- 任一关键指标缺失时不填 0；该分项为 NULL 或按缺失惩罚处理。
- 所有原始指标保留在 `run_metric` / `raw_payload`。
- 后续可增加多 score profile 或 Pareto 多目标，但第一版保留一个默认 score。

### 7.9 `worker.py`、`scanner.py`、`backfill.py`

`worker.py` 周期性 claim `outbox_event`，运行 archive service，支持并发限制、失败重试、指数退避，并写 `archive_job`。

```text
poll interval: 5-15 秒
batch size: 10-50
单 run Level A 目标耗时: < 2 秒
Level B 目标耗时: < 30 秒
Level C 不阻塞 Level A/B，可异步延迟执行
重试: 1m, 5m, 30m, 2h, 1d
最大重试后 status='dead_letter'
```

`scanner.py` 补偿 webhook 丢失、backend reload、worker 停机导致的漏归档。扫描 completed / failed / interrupted 且未入仓的 `qe_experiments` 与 `qe_evolution_loops`，生成 outbox event。

`backfill.py` / `backfill_service.py` 负责历史 dry-run inventory、补录计划、缺失清单、分批创建 outbox event 或直接调用 archive service，并输出补录报告到 `qe_archive/artifacts/backfill/...`。

第一阶段 UI 补录不再要求人工粘贴 ID。`source_assembler.py` 必须从现有 QE 公共数据库表生成候选清单：

- evolution task 候选：按 `qe_evolution_tasks` 聚合其 loop，展示任务类型、说明、总 loop 数、符合状态的 loop 数、已入库/待入库数、模型、label horizon、开始/结束时间。
- single experiment 候选：展示单次实验类型、说明、因子数量、模型、状态、执行时间、是否已入库。
- 选择一个 evolution task 后，后端 `task_ids` 必须展开为该任务下所有符合状态的 loop，并逐个写入 `qe_archive.run` 及其配置、指标、曲线、因子、raw payload 等结构化数据。
- 补录 UI 的“最少指标 / 最少曲线 / 最少因子”是写入后的质量门槛，默认固定为第一阶段校验值；它们不得作为采集范围开关，也不得让用户误以为只采集最少数据。
- 对于当前可从 DB payload 解析的数据，补录必须尽量全量写入；artifact 深度解析或远端 worker 文件拉取仍按后续 artifact collector 阶段执行，不允许 UI 隐式直接读取 worker workspace。

## 8. 实时入仓流程

### 8.1 单次实验完成路径

```text
QE 单次实验完成
  -> backend/routers/quantevolver.py
  -> _update_experiment_with_metrics(experiment_id, metrics)
  -> commit qe_experiments result_metrics/status
  -> QEArchiveService.enqueue_from_single_experiment(experiment_id, reason='terminal_metrics_update')
  -> qe_archive.outbox_event pending
  -> QEArchiveWorker Level A/B/C
```

如果 `_update_experiment_status()` 将实验标记为 failed / interrupted，也应产生 terminal outbox，保证失败实验有审计记录。

### 8.2 演进 loop 完成路径

```text
QE/RDAgent loop completed webhook
  -> backend/routers/quantevolver_evolution.py
  -> scheduler.process_completed_loop(task_id, loop_id)
  -> qe_evolution_loops.metrics_json/status 更新
  -> QEArchiveService.enqueue_from_loop(task_id, loop_id, reason='loop_completed')
  -> qe_archive.outbox_event pending
  -> QEArchiveWorker Level A/B/C
```

自定义演进、策略演进、Multi-Alpha 均走同一 outbox，只是 `source_system` 和 extractor 分支不同。

### 8.3 Level A / B / C 入仓边界

```text
Level A 快速入仓:
  run / run_source / run_config / run_reproducibility_manifest / run_data_context / run_account_summary /
  run_metric / raw_payload / run_factor / run_model_trial / 初步 score
  只依赖 DB 源行和已有 JSON，几秒内完成。

Level B 增强入仓:
  通过 QEWorkspaceClient 拉 enhanced metrics、训练曲线、IC 时序、收益曲线、feature importance 摘要、artifact manifest。
  补齐 run_account_summary、run_symbol_summary、run_trade、run_curve 中可从 enhanced metrics 直接提取的字段。
  node API 不可用时记录 unavailable，不影响 run 主记录。

Level C 深度入仓:
  第一版必须解析 pred/label/portfolio/trade 文件，补齐 position/order/trade/symbol_summary，
  对 LSTM 做最小可用 attribution，重算 stability/execution/novelty。
  大文件只从 qe_archive/artifacts 本地 cache 或 node API 下载，不直接读 worker workspace。
```

## 9. 日频无权威约束回测淘汰规则

判断逻辑：

```text
if freq in ('day', '1day', 'daily')
   and not limit_suspend_authoritative:
       research_valid = false
       invalid_reason = 'daily_backtest_without_authoritative_limit_suspend'
       exclusion_tags += ['daily_no_limit_suspend', 'excluded_by_default']
       score_total = NULL
```

权威条件：

- 回测配置明确使用涨跌停处理，且来源可追溯到权威 `stk_limit` 或等价数据。
- 回测配置明确使用停牌处理，且来源可追溯到权威 `suspend_d` 或等价数据。
- 执行逻辑对涨停买入、跌停卖出、停牌不可交易有明确约束。
- 以上证据写入 `run_data_context.limit_suspend_authoritative=true` 和 `data_quality_flags`。

默认展示规则：

- 排行榜、优化器、自动演进候选默认 `WHERE research_valid=true`。
- 诊断页可以显式 include excluded，但 UI 必须标红原因。
- excluded run 可用于“为什么历史结果不可用”的审计，不用于推荐下一轮优化。

## 10. MLflow / Qlib Recorder 整合设计

MLflow / Qlib Recorder 负责通用实验 tracking、params、metrics、tags、模型、预测、label、portfolio analysis、图片和报告等 artifact。`qe_archive` 负责 AIstock 量化研究权威分析视图、因子级/组合级/模型 trial 级/执行诊断级统计、排行榜、图表、优化候选和 LLM agent 证据。

结论：整合但不替代。

`run_source` 应保存：

```text
recorder_experiment_id
recorder_id
qlib_recorder_name
mlflow_tracking_uri
mlflow_artifact_uri
node_api_base_url
```

`run_artifact` 应保存从 MLflow/Qlib recorder 发现的 artifact manifest。

第一阶段不强制迁移集中式 MLflow server，原因：

1. 当前最急缺口是结构化量化分析仓库。
2. 现有 `qrun_limit.py` / `qrun_limit_minute.py` 已能使用本地 `MLFLOW_TRACKING_URI`。
3. 迁移 MLflow backend store 会带来额外运维、权限和 artifact path 一致性问题。

中期可选升级：

```text
MLflow Tracking Server
  backend-store-uri = PostgreSQL
  default-artifact-root = qe_archive/mlflow_artifacts 或 MinIO/NAS
```

即使升级，`qe_archive` 仍然是 AIstock 的量化研究数仓；MLflow 只保存通用 tracking 语义。

## 11. 图表、统计分析和 API 设计

### 11.1 视图

建议新增只读 views：

```sql
CREATE OR REPLACE VIEW qe_archive.v_realtime_leaderboard AS
SELECT
    r.run_id,
    r.logical_experiment_id,
    r.attempt_no,
    r.source_system,
    r.task_id,
    r.loop_index,
    r.experiment_id,
    r.model_family,
    r.model_type,
    r.freq,
    r.label_horizon,
    r.factor_count,
    r.research_valid,
    r.invalid_reason,
    r.score_total,
    r.completed_at
FROM qe_archive.run r
WHERE r.is_latest_attempt = TRUE
  AND r.research_valid = TRUE
  AND r.status IN ('completed','archived','partial_archived');

CREATE OR REPLACE VIEW qe_archive.v_factor_run_history AS
SELECT
    f.factor_name,
    f.factor_source,
    f.factor_catalog_id,
    r.run_id,
    r.completed_at,
    r.model_family,
    r.model_type,
    r.freq,
    r.label_horizon,
    r.score_total,
    r.research_valid,
    f.independent_metrics_snapshot,
    f.official_rating_snapshot
FROM qe_archive.run_factor f
JOIN qe_archive.run r ON r.run_id = f.run_id;

CREATE OR REPLACE VIEW qe_archive.v_model_trial_leaderboard AS
SELECT
    t.id AS trial_id,
    t.run_id,
    t.model_family,
    t.model_type,
    t.optimizer_name,
    t.optimizer_study_name,
    t.optimizer_trial_number,
    t.objective_name,
    t.objective_value,
    t.score_total,
    t.params,
    r.completed_at,
    r.research_valid
FROM qe_archive.run_model_trial t
JOIN qe_archive.run r ON r.run_id = t.run_id
WHERE r.research_valid = TRUE;
```

其他建议视图：

```text
qe_archive.v_run_detail
qe_archive.v_run_metric_wide
qe_archive.v_evolution_trace
qe_archive.v_factor_importance_trend
qe_archive.v_factor_pair_context
qe_archive.v_archive_quality
qe_archive.v_agent_context_runs
qe_archive.v_optimization_candidate_queue
```

### 11.2 后端 API

新增 router：`backend/routers/qe_archive.py`。

```text
GET  /api/v1/qe-archive/runs
GET  /api/v1/qe-archive/runs/{run_id}
GET  /api/v1/qe-archive/runs/{run_id}/metrics
GET  /api/v1/qe-archive/runs/{run_id}/curves
GET  /api/v1/qe-archive/runs/{run_id}/factors
GET  /api/v1/qe-archive/runs/{run_id}/factor-importance
GET  /api/v1/qe-archive/runs/{run_id}/model-trial
GET  /api/v1/qe-archive/runs/{run_id}/artifacts
GET  /api/v1/qe-archive/leaderboard
GET  /api/v1/qe-archive/factors/{factor_name}/history
GET  /api/v1/qe-archive/models/{model_family}/trials
GET  /api/v1/qe-archive/compare?run_ids=...
GET  /api/v1/qe-archive/archive-jobs
GET  /api/v1/qe-archive/backfill-candidates
POST /api/v1/qe-archive/backfill
POST /api/v1/qe-archive/recompute-scores
```

当前已落地的第一阶段 API 包括：

- `GET /api/v1/qe-archive/health`：数仓摘要、入库 run 数、research_valid 计数、pending outbox、job 状态。
- `GET /api/v1/qe-archive/backfill-candidates`：补录候选列表；支持 `status=completed|terminal|all`、`limit`、`include_archived`。
- `POST /api/v1/qe-archive/backfill`：dry-run 或 confirmed write；写入必须带 `confirm_write=QE_ARCHIVE_WRITE`；支持 `experiment_ids`、`loop_ids`、`task_ids`。
- `GET /api/v1/qe-archive/runs/{run_id}/quality`：run 级配置、来源、账户摘要、指标、曲线、因子、raw payload 完整性。
- `GET /api/v1/qe-archive/outbox`、`GET /api/v1/qe-archive/jobs`、`POST /api/v1/qe-archive/worker/run-once`：默认不常驻 worker 的队列监控和一次性处理入口。

### 11.3 前端图表能力

第一阶段支持：

- 历史补录候选页：展示未完整入库的 QE 演进任务和单次实验，支持多选、选择全部待入库、dry-run 预览、确认写入数仓。
- 实时排行榜：score_total、收益、回撤、Rank IC、模型、因子数、有效性。
- run 详情：配置、指标、曲线、因子列表、artifact manifest。
- run 对比：多 run 指标雷达图/柱状图/曲线对比。
- 因子历史：某因子参与过的所有 run、每次 run 指标、模型重要性趋势。
- 模型 trial：某模型族的超参分布、objective、score_total、训练曲线。
- 归档质量：缺配置、缺 enhanced metrics、缺 artifact、excluded 原因。

第二阶段支持：因子相关性/组合网络图、LSTM feature/time attribution 热力图、优化候选队列、shadow mode 推荐面板、Agent 证据摘要页。

## 12. 模型调参自动化目标

近期推荐流程：

```text
用户选择模型族 / 模型类型 / label horizon / 因子组合 / 搜索预算
  -> 查询 qe_archive.run_model_trial 历史 trial
  -> 按 research_valid、freq、universe、数据窗口、factor_set_hash 过滤
  -> Optuna/TPE warm-start
  -> 生成 N 个候选超参
  -> 写 qe_archive.optimization_candidate
  -> 创建 QE 自定义演进 batch
  -> 每个 loop 完成后实时归档
  -> Optuna tell + score 重算 + LLM 解释
```

这比直接恢复旧 QE 自动演进更合适，原因：自定义演进 batch 更可控、可审计、可限制预算；Optuna 负责搜索，LLM 负责解释和约束，不让 LLM 随机改参数；每个候选都有历史证据和 score breakdown；失败 trial 也入仓，有利于避免重复踩坑。

数仓需要支持：

- 每次 trial 的完整 params、search_space、fixed_params。
- 模型族、label horizon、freq、factor_set_hash、数据切分。
- objective value、score_total、全部分项指标。
- train/valid loss、过拟合 gap、收敛状态、训练耗时、GPU 信息。
- artifact link：模型权重、params.pkl、training curves。
- 失败原因和 pruned reason。

数仓稳定后，QE 自动演进可以升级为：

```text
QEArchiveEvidenceProvider
  -> CandidateGenerator（Optuna / 因子组合优化 / 规则）
  -> ConstraintChecker（预算、黑名单、相关性、数据质量）
  -> LLMResearcher（解释候选、提出假设，不直接随机改配置）
  -> CustomEvoExecutor 或 AutoEvoExecutor
  -> Archive feedback loop
```

自动演进恢复前应先跑 shadow mode：只生成建议，不自动执行；人工确认后再提交。

## 13. 因子组合自动化目标

因子组合不应再由 LLM 随机替换因子。应基于数仓统计和因子基础设施生成候选：

1. 硬过滤：剔除 unavailable、评级过低、覆盖率不足、明显泄漏、日频无效链路依赖的因子。
2. 独立质量评分：使用 `aistock_factor_metrics`、`aistock_factor_monthly_ic`、官方评级、近期衰退。
3. 冗余惩罚：使用 `qe_factor_correlations` 和分类 cluster 降低高度相关因子共存概率。
4. 组合历史：使用 `qe_archive.run_factor` 与 `run_factor_importance` 统计共现表现。
5. 模型适配：对 LSTM、GBDT、线性模型分别统计因子重要性和稳定性。
6. 执行可行性：结合换手、容量、涨跌停/停牌事件、分钟执行表现。
7. 样本外验证：优先用 walk-forward / rolling OOS，避免单次回测过拟合。
8. 多重检验控制：记录尝试次数、候选来源、失败样本，不只保存成功结果。

第一阶段可实现规则 + 贪心：

```text
FactorScore = independent_quality
            + recent_stability
            + model_fit
            - corr_redundancy
            - decay_penalty
            - execution_penalty

Greedy selection:
  start from high FactorScore factors
  add factor only if marginal diversity and historical co-run evidence pass threshold
  cap factors per classification cluster
  cap max pair correlation
  output top K candidate factor sets
```

第二阶段接入 Optuna categorical / evolutionary search 搜索因子集合，升级为多目标 Pareto：alpha、risk、stability、execution、diversity。LLM 只做解释、命名研究假设、检查约束冲突。

已经在本设计中预留字段：`run_factor.independent_metrics_snapshot`、`run_factor.official_rating_snapshot`、`run_factor.correlation_cluster`、`run_factor_pair.corr_value`、`run_factor_pair.synergy_score`、`run_factor_importance.*`、`optimization_candidate.candidate_type='factor_combo'`、`optimization_candidate.evidence_summary`、`agent_query_audit`。

## 14. LLM Agent 访问设计

允许的只读工具：

```text
get_qe_leaderboard(filters, limit)
get_run_summary(run_id)
get_factor_history(factor_name, filters)
get_model_trial_summary(model_family, filters)
get_candidate_evidence(candidate_id)
compare_runs(run_ids)
explain_score(run_id)
```

工具必须默认过滤 `research_valid=false`，限制 row_count、时间范围、字段集合，隐藏本地绝对路径和敏感环境变量，记录 `agent_query_audit`，只读且不提供写库能力。

禁止：LLM 直接拿生产库账号自由 SQL；直接修改 `qe_evolution_tasks`、`qe_evolution_loops`；直接读取 artifact 大文件；将 excluded run 当作高质量样本推荐。

## 15. 历史补录设计

### 15.1 Dry-run inventory

先做只读 inventory，不立即写入主表。扫描来源：

- `qe_experiments` completed / failed / interrupted。
- `qe_evolution_tasks` 与 `qe_evolution_loops`。
- `qe_loop_factor_records`、`qe_loop_model_records`、`qe_factor_experiment_metrics`。
- `aistock_factor_catalog`、`aistock_model_catalog`。
- `aistock_factor_metrics`、`aistock_factor_monthly_ic`。
- `qe_factor_correlations`、`qe_factor_official_ratings`。
- 已归档或可通过 node API 获取的 artifact。

inventory 输出保存到：`qe_archive/artifacts/backfill/{timestamp}/inventory.json`。

```json
{
  "total_experiments": 0,
  "total_loops": 0,
  "eligible_research_valid": 0,
  "excluded_daily_no_limit_suspend": 0,
  "missing_config": [],
  "missing_metrics": [],
  "missing_factor_catalog_match": [],
  "missing_model_catalog_match": [],
  "artifact_available": [],
  "artifact_missing": [],
  "recommended_batches": []
}
```

### 15.2 补录优先级

1. 最近有效分钟级 / limit-suspend 权威回测。
2. 当前最佳模型族 LSTM 相关实验。
3. 自定义演进 loop。
4. SOTA registry 相关 loop。
5. 单次实验。
6. 自动演进历史 loop。
7. 日频无权威约束回测：只做审计归档，默认 excluded。

### 15.3 provenance 分级

```text
direct_db     直接来自 DB 结构化字段。
db_json       来自 result_metrics / metrics_json / config_json。
node_api      通过 QEWorkspaceClient 获取。
artifact_parsed 从 qe_archive/artifacts 本地归档 artifact 解析。
inferred      根据命名、时间、loop_index 推断。
missing       源数据不可恢复。
```

每条 raw_payload 和关键字段都应能追踪 provenance。

## 16. 数据质量与归档质量

入仓后计算 `archive.quality_score`：

```text
100 分制：
- run identity 完整：10
- config 完整：20
- data_context 完整：15
- core metrics 完整：20
- factor snapshot 完整：10
- model trial 完整：10
- artifact manifest 完整：10
- raw payload 完整：5
```

低于 70 的 run 标记 `archive_quality_low`，但不等于 research invalid；research validity 由交易/数据约束决定。

常见质量事件写入 `run_execution_event` 或 `raw_payload`：

```text
missing_config
missing_metric
missing_enhanced_metrics
missing_factor_catalog_match
missing_model_catalog_match
artifact_download_failed
artifact_hash_mismatch
daily_no_limit_suspend
data_leakage_suspected
train_valid_gap_high
overfit_suspected
node_api_unavailable
```

## 17. 实施阶段

### Phase 1：DDL 与 Repository

交付：`backend/db/init_qe_archive_schema.py`、可选 `backend/migrations/qe_archive_schema.sql`、`backend/services/qe_archive/repository.py`、DDL 幂等测试。

验证：重复执行 DDL 不报错；`information_schema` 可查到全部表和索引；`qe_archive/artifacts` 路径存在且 artifact 文件不进入 git。

### Phase 2：Level A 实时入仓

交付：outbox event 写入、worker 基础循环、单次实验和 loop 完成后写 run/config/data_context/account_summary/metric/factor/model_trial/raw_payload、`research_valid=false` 规则、score v1 初步计算。

验证：fake completed `qe_experiments` / `qe_evolution_loops` 可入仓；重复 webhook 不产生重复 run；日频无权威约束 run 被 excluded。

### Phase 3：基础视图与 API

交付：`backend/routers/qe_archive.py`、leaderboard / run detail / factor history / model trials API、只读视图。

验证：API 默认过滤 `research_valid=false`；run detail 不因 artifact 缺失返回 500；指标查询能返回 canonical key 和 source key。

### Phase 4：Level B artifact manifest 与 enhanced metrics

交付：`artifact_collector.py`、enhanced metrics extractor、artifact manifest 写入、`qe_archive/artifacts/runs/YYYY/MM/{run_id}` 目录规范；从 enhanced metrics 结构化提取 absolute_returns、trade_diagnostics、all_stocks/top_stocks/bottom_stocks、stock_trades、IC/收益/训练曲线。

验证：所有 worker artifact 访问只通过 `QEWorkspaceClient`；grep 不出现新增直接读 `workspace_path` 的代码；artifact 下载失败只标记失败，不影响 run 主记录；当前 QE loop 详情卡片和点击详情页显示的字段均能从 `qe_archive` 查询还原。

### Phase 5：模型 trial 与因子重要性

交付：`model_attribution.py` adapters、Tree/linear 基础 importance、LSTM occlusion / gradient x input / integrated gradients 的接口与最小可用实现、`run_model_training_metric` 写入。

验证：LSTM run 至少记录 feature_order、lookback、训练曲线、一种模型无关 attribution；不把原始神经网络权重当作因子权重；因子重要性可按 factor_name 跨 run 查询趋势。

第一版实施口径：Phase 4 与 Phase 5 属于首个可用版本的必做范围，不后延到第二阶段。也就是说，第一版上线时必须能解析完整 position/trade/symbol summary，并具备 LSTM 最小可用 attribution；不能只上线 DDL + 核心指标。

### Phase 6：历史补录

交付：dry-run inventory、分批 backfill、补录报告。

验证：dry-run 不写主表；backfill 可中断重跑；provenance_level 正确；excluded 历史日频 run 不进入默认 leaderboard。

### Phase 7：数仓驱动优化入口

交付：Optuna warm-start 从 `qe_archive.run_model_trial` 读取、`optimization_candidate` 写入、shadow mode 推荐 API、LLM agent 只读工具接口。

验证：同样历史 trial 可复现 Optuna study 注入；candidate 有 evidence_summary；agent_query_audit 有记录；自动执行默认关闭。

## 18. 测试与验收计划

单元测试：config_builder、metric_taxonomy、metric_extractor、factor_snapshot、score、run_id 幂等。

集成测试：fake 单次实验入仓、fake loop 入仓、duplicate webhook 幂等、node API unavailable partial archive、artifact download failed 不影响详情 API。

静态红线测试：新增 grep，禁止新增 `Path(...workspace_path...).exists/glob/rglob/open`、`/mnt/f` 路径转换读取 QE artifact、`\\wsl$` 读取 QE artifact、`shutil.copytree(worker workspace)`；允许 `QEWorkspaceClient.*`、`qe_archive/artifacts` 本地 cache、DB cached summaries。

性能验收：

```text
每天 20 个 run 估算：
- Level A 入仓 P95 < 5 秒。
- leaderboard P95 < 500ms。
- run detail P95 < 1s，不读大 artifact。
- factor history P95 < 2s。
- worker 单次 artifact 下载可慢，但不得阻塞 API。
```

## 19. 风险与控制

1. 源配置不统一：通过 canonical config + provenance 解决。
2. 历史 artifact 缺失：允许 partial archive，保留 raw payload 与缺失原因。
3. 大文件拖慢系统：数据库只读 manifest，大文件异步解析，HDD 冷存储可行。
4. 指标口径漂移：metric_taxonomy 版本化，source_key 永久保留。
5. 日频无效数据污染优化：research_valid 默认过滤，score_total=NULL。
6. LSTM attribution 成本高：第一阶段可先做样本抽样 occlusion，后续增强 integrated gradients。
7. LLM 误用数据：只读工具、默认过滤、审计、预算限制。
8. 过早 MLflow 集中化增加复杂度：先做 link 与 manifest，中期再迁移。

## 20. 第二轮确认结果

用户于 2026-05-02 确认以下事项：

1. `score_total` 第一版按本文档默认公式和默认分项权重执行，不简化为三项指标。
2. 在确保当前 QE loop 卡片、展开详情和点击详情页展示的全部指标均可入仓和查询还原的前提下，可以进入后续研发实施。
3. 第一版可用版本就需要解析完整 position/trade/symbol summary，并提供 LSTM 最小可用 attribution；不能只做 DDL、核心指标和 artifact manifest。
4. 第一阶段不引入额外 NoSQL / 文档数据库作为权威存储；使用 PostgreSQL / TimescaleDB + JSONB + `qe_archive/artifacts`，未来按规模再评估 ClickHouse、Parquet/DuckDB、MinIO、OpenSearch 或 Vector DB。
5. 数仓必须保存每个实验/loop 的所有配置和使用参数，并通过 `run_reproducibility_manifest` 记录复现等级、hash、环境版本、artifact 清单和缺失项，确保后续能基于归档记录复现实验并核对结果准确性。


### 20.1 实施前复现能力确认（2026-05-02）

针对“目前的数仓是否记录每个实验中的所有配置和使用参数，是否足以复现实验并确认结果准确”这一点，实施前确认如下：

1. 现有 QE 业务表不是严格意义上的完整实验数仓；历史 `qe_experiments`、`qe_evolution_tasks`、`qe_evolution_loops`、`result_metrics`、`config_json`、`custom_params` 等能提供部分配置与指标，但不能保证覆盖 Qlib 渲染配置、模型 fit 参数、环境版本、artifact hash、训练曲线、position/trade/symbol 明细和复现缺口。
2. 新建 `qe_archive` 数仓必须以“能最大限度复现实验”为硬约束：每个 experiment/loop 都必须写入 `qe_archive.run_config`，其中 `canonical_config` 是统一口径可查询配置，`raw_config` 是来源原始配置合集，`config_sha256` 是复现比对主 hash。
3. `qe_archive.run_config` 额外记录 `config_capture_complete`、`config_provenance`、`missing_config_items`。如果任何来源配置缺失，不允许假装完整，只能降低复现等级并写入缺失项。
4. `qe_archive.run_reproducibility_manifest` 记录复现实验所需的完整清单：config/hash、Qlib/MLflow recorder、代码版本、runner 脚本 hash、Python/Qlib/MLflow/Torch/package 版本、随机种子、确定性设置、artifact manifest、指标 payload hash、缺失项和复现等级。
5. 结果准确性确认不依赖单一指标表：标量指标进入 `run_metric`，账户级绝对收益/期初期末资产进入 `run_account_summary`，收益/回撤/IC/loss 曲线进入 `run_curve`，position/order/trade/symbol 明细进入对应明细表，原始 payload 进入 `raw_payload`，artifact 文件只保存 manifest/hash/URI 到 DB。
6. 归档 worker 后续入仓时的判定规则：只有在核心配置、数据上下文、模型参数、因子列表、执行/回测参数、环境版本、关键 artifact hash 和原始指标 payload 都可追溯时，才能标记 `reproducibility_level='full'`；否则只能是 `partial` 或 `audit_only`。
7. 当前实施阶段先落地 schema、repository 和可复现契约测试；实时接入、artifact 异步解析、历史补录和前端分析视图按后续 Phase 继续实施，不在业务接口中直接读取 QE/RD-Agent worker workspace。

## 21. 后续讨论点

1. LSTM attribution 第一阶段采用 occlusion 为主，还是同时接 integrated gradients。
2. 前端第一版先做排行榜 + run 详情 + 因子历史，还是加入模型 trial 页面。
3. 历史补录是否先只补最近 3 个月有效实验，再补全历史。
4. 中期是否部署集中式 MLflow server，还是继续本地 `mlruns` + `qe_archive` manifest。


## 21.1 全流程自动化测试产线要求（2026-05-02）

借鉴 Paper Trading v2 / Selection Center 的结果导向测试产线，QE 实时数仓从后续开发开始必须按分层测试推进，不能只做单元测试或人工点击验证。

生产隔离前提：

1. 不重启当前生产 backend `8001`；开发验证使用 `8011/8012` 后端和 `3011/3012` 前端。
2. 后续 QE archive runtime hook、worker、scanner、optimizer、agent 工具必须默认关闭或显式 feature flag 控制。
3. archive 写入失败不得改变现有 QE 实验/loop 的成功、失败、回调和页面状态。
4. 所有 artifact 访问仍必须走 node API、显式下载或 AIstock-owned `qe_archive/artifacts`，不得直接读取 worker workspace。

测试入口：

```powershell
python -m nox -s qe_archive_backend       # schema/repository/event capture backend regression
python -m nox -s qe_archive_data_quality  # read-only DB schema/comment/version smoke
$env:QE_ARCHIVE_L3_SKIP_UI='1'
python -m nox -s qe_archive_l3            # L3 local suite, UI not implemented yet时显式跳过
```

阶段门禁：

- 后端 workflow 阶段：必须覆盖 outbox、archive_job、run/config/data/metric/raw_payload 写入、失败重试、幂等、默认关闭 feature flag。
- artifact/parser 阶段：必须覆盖 node API 访问、artifact manifest、hash、parser status、下载失败显式记录、无 worker 路径直读。
- API 阶段：必须覆盖默认过滤 `research_valid=false`、run detail、job status、错误透传、无本地绝对路径泄露。
- UI 阶段：必须新增 Playwright E2E，覆盖 dashboard、run detail、指标/曲线/因子/模型 trial 页面；遇到 console error、pageerror、requestfailed、非预期 4xx/5xx 必须失败。
- 每次 L2/L3 验证必须在 `tests/aistock_validation/history/` 留存 run record。

测试矩阵文件：`tests/aistock_validation/modules/qe_archive.md`。

## 22. 参考资料

- Qlib Recorder 文档：`https://qlib.readthedocs.io/en/stable/component/recorder.html`
- MLflow Backend Store 文档：`https://mlflow.org/docs/latest/self-hosting/architecture/backend-store/`
- MLflow Artifact Store 文档：`https://mlflow.org/docs/latest/self-hosting/architecture/artifact-store/`
- Optuna Study API：`https://optuna.readthedocs.io/en/stable/reference/generated/optuna.study.Study.html`
- Harvey, Liu, Zhu, “... and the Cross-Section of Expected Returns”：`https://www.nber.org/papers/w20592`
- Feng, Giglio, Xiu, “Taming the Factor Zoo: A Test of New Factors”：`https://www.nber.org/papers/w25481`

## 23. 2026-05-02 ?????Durable Outbox ?????

??????????hook ???????? best-effort ??????? durable outbox ????????? QE Archive UI?

### 23.1 ????????

- `QE_ARCHIVE_REALTIME_ENABLED` ?????????? QE ?? hook ??????????
- ??????`QE_ARCHIVE_REALTIME_MODE` ??? `outbox`?loop/experiment ?? hook ??? `qe_archive.outbox_event`??? queued/event_id??? QE ?????????
- `QE_ARCHIVE_REALTIME_MODE=direct` ????????????????????????
- `outbox_event` ???? deterministic event_id / unique source key ??????????????????

### 23.2 Worker ??????

- ?? API ? one-shot worker?`POST /api/v1/qe-archive/worker/run-once`?
- ?????? `confirm_run=QE_ARCHIVE_WORKER_RUN`??????? claim ????????????? FastAPI startup scheduler?
- Worker handler ???? `qe.loop.completed` ? `qe.experiment.completed`????? backfill/archive service?????? `archive_job` ? outbox?????? retry/error ???
- ?????????`GET /api/v1/qe-archive/outbox` ? `GET /api/v1/qe-archive/jobs`?? UI ???????????????????

### 23.3 ???? UI

- ?? `frontend/src/app/qe-archive/page.tsx`??? Paper v2 ???/??????????? raw JSON ???
- ????????????outbox/job ??????? dry-run/confirmed write??? worker run-once?run quality ????? outbox/job ???
- ??? QuantEvolver ???? `/qe-archive` ???
- ?????????/??/IC ?????? run-detail API ????????? return/drawdown/IC/RankIC/training curve ???

### 23.4 ????

- ?????? realtime outbox ?????direct ???worker service handler?worker API confirmation?
- UI ???? `frontend/tests/qe-archive/qe-archive-dashboard.spec.ts`??? mocked QE Archive API ?? dashboard/backfill/worker/quality ????????? `8001`?
- `nox -s qe_archive_ui` ?? `QE_ARCHIVE_UI_MOCK_API=1`???? dev backend ??? UI route/type/E2E ???live API ?????? `8011/8012` dev backend?

## 24. Symbol and Trade Structured Archive Implementation (2026-05-03)

This phase moves stock-level and trade-level data that already exists in QE DB/API payloads from raw JSONB into queryable structured tables. The implementation still follows the hard red line: it does not directly read WSL or remote worker files. Input data is limited to `qe_evolution_loops.metrics_json`, `qe_experiments.result_metrics`, and payloads that the archive service has already obtained through DB/API paths.

### 24.1 Confirmed Collectable Fields

A read-only audit of 16 archived runs and 48 `qe_archive.raw_payload` rows confirmed these stable enhanced-metrics fields:

- `all_stocks`: about 700 to 1300 symbol summary rows per run, with `code`, `profit`, `profit_pct`, `avg_cost`, `last_price`, `holding_days`, `first_date`, and `last_date`.
- `top_stocks` / `bottom_stocks`: 10 best/worst symbol summary rows per run, with the same field family as `all_stocks`.
- `stock_trades`: symbol-keyed trade lists. Current source fields are `date`, `type`, `price`, `amount`, and `pnl`. No reliable `quantity` or `shares` field is present, so `amount` is stored as source-reported amount and quantity is not inferred.
- `trade_diagnostics` and `execution_trace`: stored as execution/parser events for later data-quality and execution-lineage analysis.

### 24.2 Structured Write Rules

- `all_stocks`, `top_stocks`, and `bottom_stocks` are written to `qe_archive.run_symbol_summary` with unique key `(run_id, source_list, symbol)`, `rank_in_list`, and small-row raw metadata.
- `stock_trades` is written to `qe_archive.run_trade` with deterministic `trade_uid`, `symbol`, `side`, `trade_date`, `price`, `amount`, `pnl`, `source_payload_path`, and raw trade metadata. Missing `quantity` is left null rather than fabricated.
- Parser summary, `trade_diagnostics`, and `execution_trace` are written to `qe_archive.run_execution_event`.
- `get_run_quality_summary()`, the data-quality smoke, API responses, and the UI quality panel now expose `symbol_summary_count`, `trade_count`, and `execution_event_count`.
- Confirmed backfill and realtime worker paths reuse the same `QEArchiveService`, so historical and realtime structured writes have the same semantics.

### 24.3 8011/3011 Validation Sample

The dev backend `8011` and frontend `3011` were restarted to load this code. Production backend `8001` was not restarted. The already archived task `qe_20260502_131502_9b54` was reprocessed through API confirmed backfill:

```text
POST http://127.0.0.1:8011/api/v1/qe-archive/backfill
source=task, task_ids=[qe_20260502_131502_9b54], write=true, confirm_write=QE_ARCHIVE_WRITE
```

All 4 loops passed quality gates. Sample run `qear_run_61fe6f6dccabca49b1228033` stored `metric_count=67`, `curve_count=3489`, `factor_count_rows=57`, `symbol_summary_count=792`, `trade_count=4322`, `execution_event_count=3`, and `raw_payload_count=3`.

### 24.4 Remaining Work

- Current payloads do not contain full structured daily/minute position snapshots. Future artifact parsers must collect them through node APIs or AIstock-owned artifact copies before writing `run_position`.
- Current trade payloads do not provide authoritative `quantity`, `shares`, `commission`, `tax`, or `slippage`; these fields remain null until a later order/fill artifact parser supplies them.
- Factor weight trends and feature importance remain reserved for `run_factor_importance` and `run_model_training_metric`; this phase only structures the stock/trade/event data already present in current payloads.

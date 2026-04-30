# QE 诊断信息输出与前端字段补充方案

生成日期：2026-05-01
适用范围：QE 演进任务（例如 `qe_20260430_010121_d55f`）、已完成 Loop、后续新增 QE 回测。
约束条件：不修改 Qlib 源码；只使用 AIstock / RD-Agent 侧 workspace 产物、后处理脚本、后端缓存/API 与前端展示。

## 1. 总结论

不修改 Qlib 源码的情况下，可以补充绝大多数诊断信息。关键思路是把 Qlib recorder 产物和 QE workspace 文件作为事实来源，后处理生成轻量摘要写入 DB，大明细保留为 workspace 文件，前端按需展示。

当前前端已经具备部分展示基础，例如 enhanced metrics、Loss 曲线、模型训练诊断卡片；但当前数据产物没有稳定输出这些字段，所以卡片经常为空。对于已经运行结束的 Loop，只要 workspace / mlruns 产物仍在，大部分数据都可以回填；只有精确的 TAIL_SUBSTITUTE 替补映射、V25 每分钟计划等运行时决策，更适合后续在 AIstock 策略模板里新增 JSONL 审计日志。

```text
+-----------------------------+------------------+----------------------+------------------------------------------+
| 诊断/展示项                 | 需改 Qlib 源码   | 已完成 Loop 可回填   | 主要动作                                 |
+-----------------------------+------------------+----------------------+------------------------------------------+
| 最小/最大/平均持仓数        | 否               | 是                   | 解析 positions_normal_1day.pkl           |
| 回测结束现金/股票市值       | 否               | 是                   | 解析最后一个持仓快照                     |
| CAGR / 绝对最大回撤         | 否               | 是                   | 用 account NAV 重新计算                  |
| 回测使用模型                | 否               | 是                   | 读取 config.json/conf.yaml/model catalog |
| HMM 启用/版本/快照          | 否               | 大部分可回填         | 读取 config/custom_params/HMM artifact   |
| 训练周期 1D/5D/10D/20D      | 否               | 是                   | 读取 label_horizon 或 label 表达式       |
| 订单成交失败审计            | 否               | 是                   | 解析 indicators_normal_*_obj.pkl         |
| 价格口径/停牌/涨跌停审计    | 否               | 是，需要 DB/Qlib     | Qlib 字段与 DB 日线/分钟线联合检查       |
| 训练 Loss 曲线              | 否               | 日志存在即可回填     | 修复/统一 LGB 日志解析器                 |
| 精确替补买入映射            | 否               | 旧 Loop 只能部分还原 | 后续策略模板新增 runtime audit           |
+-----------------------------+------------------+----------------------+------------------------------------------+
```

## 2. 已经运行结束的 Loop 可通过分析获取哪些数据

下表列出不重跑回测、仅分析已有 workspace / mlruns / DB / Qlib 数据可以获取的内容。

```text
+-----------------------------+-----------------------------------------------+----------------------+------------------------------+
| 数据项                      | 现有来源                                      | 旧 Loop 可回填       | 可靠性                       |
+-----------------------------+-----------------------------------------------+----------------------+------------------------------+
| IC / Rank IC / ICIR         | qlib_results_llm.json, qlib_results_enhanced  | 是                   | 高                           |
| 收益曲线                    | report_normal_1day.pkl, qlib_results_enhanced | 是                   | 高                           |
| CAGR 绝对年化收益           | positions/account NAV                         | 是                   | 高                           |
| 绝对最大回撤                | positions/account NAV                         | 是                   | 高                           |
| 每日现金金额                | positions_normal_1day.pkl                     | 是                   | 高                           |
| 回测结束现金金额            | 最后一个 positions 快照                       | 是                   | 高                           |
| 每日股票市值                | positions_normal_1day.pkl                     | 是                   | 高                           |
| 回测结束股票市值            | 最后一个 positions 快照                       | 是                   | 高                           |
| 最小持仓数量                | positions_normal_1day.pkl                     | 是                   | 高                           |
| 最大持仓数量                | positions_normal_1day.pkl                     | 是                   | 高                           |
| 平均持仓数量                | positions_normal_1day.pkl                     | 是                   | 高                           |
| 每日持仓数量曲线            | positions_normal_1day.pkl                     | 是                   | 高                           |
| 换手率                      | report/indicators/positions artifacts          | 是                   | 高                           |
| 每日交易笔数                | indicators_normal_1day.pkl/object             | 是                   | 高                           |
| 订单 amount/deal/ffr        | indicators_normal_1day_obj.pkl/minute obj     | 是                   | 高                           |
| 买卖方向                    | order indicator 的 trade_dir                  | 是                   | 高                           |
| 成交价/交易金额/交易成本    | order indicators                              | 是                   | 高                           |
| 预测分数与排名              | pred.pkl                                       | 是                   | 高                           |
| 个股盈亏摘要                | positions + stock_trades 提取                 | 是                   | 中高                         |
| 模型 id / 模型类型          | config.json, conf.yaml, model catalog          | 是                   | 高                           |
| 因子列表/因子数量           | config.json                                    | 是                   | 高                           |
| 策略 id / 策略参数          | config.json, conf.yaml                         | 是                   | 高                           |
| 训练标签周期                | model_params.label_horizon / conf label        | 是                   | 高                           |
| HMM 是否启用                | config/custom_params/strategy_params           | 大部分是             | 配置保留则高                 |
| HMM 版本/快照               | HMM 参数、预计算 artifact、配置字段            | 大部分是             | 中高                         |
| 训练 Loss                   | run.log / RD-Agent log pkl                    | 日志存在即可         | 高                           |
| LGB best iteration          | run.log                                        | 是                   | 高                           |
| 数据缺失原因                | Qlib fields + DB 日线/分钟线/停牌/涨跌停      | 是，需要联合检查     | 实现后高                     |
| 涨跌停阻塞原因              | Qlib/DB close + limit + order ffr              | 是，需要联合检查     | 实现后高                     |
| 精确替补映射                | 策略运行时状态                                | 旧 Loop 只能部分还原 | 中                           |
| 精确 V25 分钟计划           | V25 运行时状态                                | 旧 Loop 只能部分还原 | 中                           |
+-----------------------------+-----------------------------------------------+----------------------+------------------------------+
```

## 3. 卡片总览需要补充的字段

卡片总览建议补充持仓范围和回测结束资产结构。所有字段都能从已经完成的 Loop 里回填。

```text
+--------------------------+----------------------------+------------------------------+------------------------------+
| 字段                     | 来源                       | 定义                         | 展示建议                     |
+--------------------------+----------------------------+------------------------------+------------------------------+
| min_position_count       | positions_normal_1day.pkl  | 回测期每日持仓数最小值       | 整数                         |
| max_position_count       | positions_normal_1day.pkl  | 回测期每日持仓数最大值       | 整数 + 超阈值提示            |
| avg_position_count       | positions_normal_1day.pkl  | 回测期每日持仓数均值         | 保留 1 位小数                |
| p95_position_count       | positions_normal_1day.pkl  | 回测期持仓数 P95             | 可选风险字段                 |
| final_cash_amount        | 最后一个 positions 快照    | 回测结束现金金额             | 金额，保留 2 位小数          |
| final_stock_market_value | 最后一个 positions 快照    | 回测结束股票市值             | 金额，保留 2 位小数          |
| final_total_account      | cash + stock market value  | 回测结束账户总权益           | 金额，保留 2 位小数          |
| final_cash_ratio         | cash / final_total_account | 回测结束现金占比             | 百分比                       |
+--------------------------+----------------------------+------------------------------+------------------------------+
```

建议的卡片预警阈值：

```text
+----------------------+-----------------------------+----------------------------------------------+
| 指标                 | 预警条件                    | 原因                                         |
+----------------------+-----------------------------+----------------------------------------------+
| max_position_count   | > 65                        | 目标 50 只可波动，但 65+ 偏离较大             |
| p95_position_count   | > 60                        | 持续超限比单日极值更值得关注                 |
| avg_position_count   | < 45 或 > 60                | 太低可能成交受阻，太高可能替补/漂移过多       |
| final_cash_ratio     | > 20%                       | 可能存在买入受阻、尾盘替补不足或现金拖累      |
+----------------------+-----------------------------+----------------------------------------------+
```

建议卡片排版：

```text
+-------------------+-------------------+-------------------+-------------------+
| 最小持仓          | 平均持仓          | 最大持仓          | P95 持仓          |
+-------------------+-------------------+-------------------+-------------------+
| 结束现金          | 结束股票市值      | 结束总权益        | 结束现金占比      |
+-------------------+-------------------+-------------------+-------------------+
| CAGR 绝对年化     | 绝对最大回撤      | 含成本年化        | 含成本最大回撤    |
+-------------------+-------------------+-------------------+-------------------+
```

## 4. 演进轨迹下方表格需要补充的字段

演进轨迹下方的 Loop 对比表应该补充模型、HMM、训练周期、绝对收益和持仓统计，方便不展开详情就能横向比较。

```text
+--------------------------+-----------------------------+------------------------------+------------------------------+
| 列名                     | 来源                        | 含义                         | 备注                         |
+--------------------------+-----------------------------+------------------------------+------------------------------+
| Loop                     | qe_evolution_loops          | Loop 序号                    | 已有                         |
| Status                   | qe_evolution_loops          | completed/failed/running     | 已有                         |
| SOTA                     | qe_evolution_loops          | 是否被选为 SOTA              | 已有                         |
| Action                   | qe_evolution_loops          | param/model/factor 等动作    | 已有                         |
| Model                    | config_json/model catalog   | 回测使用的模型 id/name       | 新增必选                     |
| Model Type               | model catalog/config        | LGB/PTNN/GRU/ALSTM 等类型    | 新增建议                     |
| HMM Enabled              | config/custom_params        | 是否启用 HMM                 | 新增必选                     |
| HMM Version              | HMM artifact/config         | HMM 算法或配置版本           | 新增必选                     |
| HMM Snapshot             | HMM artifact/config         | 使用的快照 id/hash/path      | 新增必选                     |
| Label Horizon            | model_params/conf label     | 1D/5D/10D/20D 训练标签周期   | 新增必选                     |
| CAGR                     | absolute_return summary     | 账户 NAV 绝对年化收益        | 新增必选                     |
| Absolute MaxDD           | absolute_return summary     | 账户 NAV 绝对最大回撤        | 新增必选                     |
| AnnRet With Cost         | metrics_json                | Qlib 含成本超额年化          | 保留                         |
| MaxDD With Cost          | metrics_json                | Qlib 含成本超额最大回撤      | 保留                         |
| Sharpe / IR              | metrics_json                | 风险调整收益                 | 保留                         |
| IC / Rank IC             | enhanced metrics            | 信号质量                     | 保留                         |
| Min/Avg/Max Position     | holding_audit summary       | 持仓范围                     | 新增建议                     |
| Final Cash               | holding_audit summary       | 回测结束现金                 | 新增建议                     |
| Final Stock Value        | holding_audit summary       | 回测结束股票市值             | 新增建议                     |
+--------------------------+-----------------------------+------------------------------+------------------------------+
```

默认展示列建议控制数量，更多列放到横向滚动或展开项：

```text
+------+--------+------+--------+------------+------+----------+-----------+-----------+----------+----------+----------+----------+
| Loop | SOTA   | Act  | Model  | Horizon    | HMM  | CAGR     | Abs MaxDD | AnnRet    | MaxDD    | Pos Avg  | Pos Max  | Cash End |
+------+--------+------+--------+------------+------+----------+-----------+-----------+----------+----------+----------+----------+
```

可展开的补充列：

```text
+------------+--------------+-------------+-------------+----------+----------+----------+-------------+
| Model Type | HMM Version  | HMM Snapshot| SignalPreset| IC       | Rank IC  | Sharpe   | Stock Value |
+------------+--------------+-------------+-------------+----------+----------+----------+-------------+
```

## 5. 后端建议写入的摘要字段

建议把小摘要写入每个 Loop 的 `metrics_json.enhanced_metrics.summary` 或新增 `loop_diagnostics_summary` 对象。大明细不直接塞进 DB JSONB，保留为 workspace 文件。

```text
+-----------------------------+----------------------+------------------------------+
| 字段                        | 类型                 | 说明                         |
+-----------------------------+----------------------+------------------------------+
| position_count_min          | int                  | 回测期每日最小持仓数         |
| position_count_max          | int                  | 回测期每日最大持仓数         |
| position_count_avg          | float                | 回测期每日平均持仓数         |
| position_count_p95          | float                | 回测期每日持仓数 P95         |
| final_cash                  | float                | 回测结束现金                 |
| final_stock_value           | float                | 回测结束股票市值             |
| final_account_value         | float                | 回测结束总权益               |
| final_cash_ratio            | float                | 结束现金 / 结束总权益        |
| cagr_absolute               | float                | 基于账户 NAV 的 CAGR         |
| max_drawdown_absolute       | float                | 基于账户 NAV 的最大回撤      |
| model_id                    | string               | 回测模型 id                  |
| model_type                  | string               | 模型类型                     |
| label_horizon_days          | int                  | 1/5/10/20 等                 |
| hmm_enabled                 | bool                 | 是否启用 HMM                 |
| hmm_version                 | string/null          | HMM 配置/算法版本            |
| hmm_snapshot_id             | string/null          | HMM 快照 id                  |
| hmm_signal_preset           | string/null          | HMM 信号预设                 |
| audit_artifact_paths        | object               | 明细审计文件路径             |
+-----------------------------+----------------------+------------------------------+
```

## 6. 建议生成的明细文件

```text
+------------------------------+------------------+------------------------------+------------------------------+
| 文件                         | 旧 Loop 可回填   | 来源                         | 用途                         |
+------------------------------+------------------+------------------------------+------------------------------+
| holding_audit.json           | 是               | positions_normal_1day.pkl    | 持仓数量、结束现金/股票市值  |
| order_fill_audit.parquet     | 是               | indicators_normal_*_obj.pkl  | 订单 ffr/deal/price/cost     |
| price_basis_audit.parquet    | 是               | Qlib + DB 日线/分钟线        | 停牌/涨跌停/价格口径         |
| training_diagnostics.json    | 日志存在即可     | run.log / RD-Agent log pkl   | Loss 曲线、best iteration    |
| factor_config_audit.json     | 是               | config.json                  | 因子数量、因子组、alpha158   |
| model_config_audit.json      | 是               | config.json/conf.yaml/catalog| 模型、超参、训练周期         |
| hmm_rank_audit.parquet       | 大部分可回填     | pred.pkl + HMM coefficients  | HMM 调整前后排名归因         |
| substitute_audit.jsonl       | 后续最佳         | 策略运行时审计               | 涨停阻塞买入 -> 替补买入映射 |
| v25_plan_audit.jsonl         | 后续最佳         | V25 运行时审计               | 分钟计划、尾盘执行行为       |
+------------------------------+------------------+------------------------------+------------------------------+
```

## 7. 当前代码证据

```text
+----------------------+--------------------------------------------+------------------------------------------+
| 证据                 | 路径                                       | 说明                                     |
+----------------------+--------------------------------------------+------------------------------------------+
| Enhanced API client  | backend/services/quantevolver/qe_workspace_client.py | 可拉取 enhanced metrics         |
| Loop metrics cache   | backend/services/quantevolver/qe_evolution_service.py | 可写入 metrics_json/enhanced     |
| Evolution API        | backend/routers/quantevolver_evolution.py | 前端可请求 Loop enhanced metrics         |
| Single exp API       | backend/routers/quantevolver.py           | 可加载本地 qlib_results_enhanced.json    |
| Loop model table     | backend/init_catalog_db.py                | qe_loop_model_records 已有训练字段       |
| Model catalog API    | backend/routers/quantevolver.py           | 模型列表 API 已返回训练字段              |
| Training chart       | frontend/src/app/quantevolver/components/charts/LossCurveChart.tsx | Loss 图组件已存在      |
| Loop training tab    | frontend/src/app/quantevolver/evolution/components/LoopDetailPanel.tsx | 训练 Tab 已存在 |
| Model list card      | frontend/src/app/quantevolver/components/ModelList.tsx | 训练诊断 UI 已存在      |
+----------------------+--------------------------------------------+------------------------------------------+
```

## 8. 当前训练卡片为空的原因

以 `qe_20260430_010121_d55f/Loop10` 为例，`qlib_results_enhanced.json` 里的 `training_diagnostics` 是空对象，但 `run.log` 中存在 LightGBM 的 `train's l2` / `valid's l2` 训练日志和 best iteration。这说明训练数据存在，只是当前 Loop 内的 `read_exp_res.py` 没覆盖 LightGBM 日志格式。

`backend/scripts/backfill_model_training_from_logs.py` 已经有 LightGBM regex，所以正确做法不是改 Qlib 源码，而是把这套解析逻辑统一接入标准 enhanced metrics 生成和历史回填路径。

```text
+----------------------+------------------------------+------------------------------------------+
| 项目                 | 当前事实                     | 结论                                     |
+----------------------+------------------------------+------------------------------------------+
| qlib_results_enhanced| training_diagnostics = {}    | 前端没有曲线可展示                       |
| run.log              | 有 train/valid l2 行         | 训练曲线可以回填                         |
| parser gap           | read_exp_res 漏 LGB 格式     | 需要修复 AIstock/RD-Agent 侧解析器        |
| backfill parser      | 已有 LGB regex               | 可以复用到已完成 Loop 回填               |
+----------------------+------------------------------+------------------------------------------+
```

## 9. 优先级建议

```text
+----------+--------------------------------+----------------------+------------------------------------------+
| 优先级   | 工作                           | 适用于旧 Loop        | 收益                                     |
+----------+--------------------------------+----------------------+------------------------------------------+
| P0       | 持仓/结束资产摘要回填           | 是                   | 卡片总览立即可用于判断持仓漂移           |
| P0       | CAGR/绝对最大回撤回填           | 是                   | 区分绝对收益与 Qlib 超额收益             |
| P0       | 修复并回填训练日志解析           | 是                   | 训练卡片可用于模型取舍和调参             |
| P0       | 演进轨迹表格新增模型/HMM/周期列  | 是                   | 不展开详情即可跨 Loop 比较               |
| P1       | 订单/价格口径审计回填            | 是                   | 成交失败原因不再依赖猜测                 |
| P1       | HMM 元数据与归因审计             | 大部分是             | 判断 HMM 是否真实提升收益                |
| P1       | 新增前端诊断明细面板             | API 完成后           | 可按日期/股票钻取异常订单                |
| P2       | 新增替补映射 runtime audit       | 后续 Loop 最准确     | 精确记录 blocked buy -> backup stock     |
| P2       | 新增 V25 分钟计划 runtime audit  | 后续 Loop 最准确     | 精确诊断 V25 每分钟计划与尾盘行为        |
+----------+--------------------------------+----------------------+------------------------------------------+
```

## 10. 口径注意事项

- `CAGR` 和 `绝对最大回撤` 应基于账户 NAV，也就是现金加股票市值，不要用仅股票市值，也不要直接用 Qlib 的超额收益字段。
- Qlib 常见的 `1day.excess_return_with_cost.annualized_return` 和 `1day.excess_return_with_cost.max_drawdown` 是超额收益口径，应该继续保留，但要与绝对收益口径分列展示。
- 已完成 Loop 的价格口径审计依赖本地 Qlib bin 与 DB 日线/分钟线/停牌/涨跌停数据覆盖对应交易日。
- 已完成 Loop 的精确替补映射和 V25 计划只能部分还原；后续应该在策略模板层记录 JSONL，不需要改 Qlib 源码。
- 分钟级明细对象可能很大，不应直接返回给前端；应生成摘要和可筛选的明细文件，前端按 Loop/日期/股票 lazy-load。

## 11. 本次前端实施范围

本次实施只使用已经存在的 `loop.config_json`、`loop.metrics_json`、`metrics_json.enhanced_metrics` 和当前选中 Loop 的 enhanced metrics API 返回值，不修改实验运行、回测策略、Qlib 源码，也不新增实验期间审计日志。

```text
+----------------------+------------------------------------------------------------+------------------------------+
| 页面区域             | 本次已处理内容                                             | 数据来源                     |
+----------------------+------------------------------------------------------------+------------------------------+
| Loop 详情总览        | 新增模型/HMM/因子摘要卡片                                  | config_json/enhanced metrics |
| Loop 详情总览        | 新增最小/平均/最大/P95/期末持仓、结束现金、股票市值卡片    | enhanced metrics/holding 摘要|
| Loop 详情核心指标    | 继续分列显示信号质量、含成本收益、账户绝对收益             | metrics_json/absolute_returns|
| 演进轨迹下方表格     | 新增模型、周期、HMM、快照、CAGR、绝对 MaxDD、持仓与资金列  | loop 列表缓存字段            |
| 左侧演进拓扑         | 每个 Loop 增加模型/周期/HMM 标签和注释说明                 | agent_analysis/config_json   |
+----------------------+------------------------------------------------------------+------------------------------+
```

旧 Loop 未回填 `position_count_min/max/avg/p95` 时，页面显示 `-`，不会为了展示字段在前端触发重跑或临时解析大型 pickle。后续如需要完整填充这些字段，应按本文 P0 回填方案生成 holding summary 并写入 enhanced metrics。

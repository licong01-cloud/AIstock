# 因子 OOS 指标与历史回测选因偏差问题记录

> 状态：后续待解决问题，短期暂不改造  
> 日期：2026-05-07  
> 范围：因子独立指标、OOS 指标语义、QE 历史回测选因、未来因子组合自动选择

## 1. 当前结论

短期不需要改造因子库独立指标架构，也不需要马上补齐独立计算 OOS 多周期指标。当前 QE 回测的主要目的，是作为模拟盘执行和未来实盘选股前的候选策略压力测试，而不是证明某个历史时点可以无偏地自动选出这些因子。

当前可继续沿用现有模式：

- 因子选择继续以人工/半人工方式执行。
- 继续参考 full、recent、覆盖率、相关性、因子逻辑解释、交易可行性等信息。
- OOS 指标暂时只作为回顾性审计和近期表现观察，不作为同区间历史回测的自动选因依据。
- 当前因子组合是否真实有效，最终主要依赖模拟盘/未来实盘 forward validation 验证。

## 2. 当前实现背景

当前 QE 默认切分大致为：

| 用途 | 时间段 |
|---|---|
| train | 2018-08-01 ~ 2022-12-31 |
| valid | 2023-01-01 ~ 2024-06-30 |
| test / signal | 2024-07-01 ~ 默认最新信号日 |
| QE portfolio backtest | 2024-07-01 ~ 默认安全回测结束日 |
| factor `out_sample` | 2024-07-01 ~ 当前因子快照结束日 |

相关代码位置：

- `backend/services/quantevolver/config_composer.py`：QE 默认 train/valid/test/backtest 切分。
- `backend/services/quantevolver/qe_eval_v2_metric_engine.py`：因子独立指标 eval_window 定义，其中 `out_sample` 固定从 `2024-07-01` 开始。
- `backend/services/quantevolver/factor_official_evaluation_service.py`：官方独立指标写入、派生字段保存逻辑。

## 3. 风险分析

### 3.1 OOS 与回测期重合时的风险

如果使用 `2024-07-01 ~ 2026-04` 的 OOS 指标挑选因子，然后再回测 `2024-07-01 ~ 2026-04`，就会形成选择层面的未来信息泄漏：

- 选因时已经知道了回测区间内哪些因子表现更好。
- 选因时已经知道了回测区间内哪个 horizon 更好。
- 选因时可能已经知道了回测区间内方向是否稳定、是否衰退。

这类问题不是单个因子计算的未来函数，而是 research process leakage / selection leakage / backtest overfitting。

### 3.2 full 指标的风险比 OOS-only 小，但不等于无偏

使用整个周期 full 指标选因，比只用 OOS 指标挑最近两年赢家更稳健，因为 full 混合了更长历史区间，不会完全贴合最近行情。但如果 full 指标包含回测期数据，则对历史回测开始日来说仍然包含未来信息。

因此：

| 选因依据 | 对历史回测是否严格无偏 | 说明 |
|---|---:|---|
| 只用同区间 OOS 排名 | 否 | 最容易直接挑中回测期赢家 |
| 用包含回测期的 full 指标 | 否 | 偏差较弱，但仍使用未来信息 |
| 只用回测开始日前可见指标 | 是 | 满足 as-of 约束 |
| 每月/每季度滚动只用过去指标 | 是 | 最接近实盘 |

## 4. OOS 指标的正确定位

OOS 的价值不是用于同区间选因，而是用于检验、审计和观察泛化能力。

合理用途：

- 因子开发完成后，检验是否能泛化到后续样本。
- 对比 full 与 OOS，判断因子是否衰退。
- 观察 OOS 方向、RankIC、最佳周期是否与 full 一致。
- 作为当前时点未来模拟盘/实盘选因的历史参考。
- 回测后解释为什么某些因子在测试期表现好或差。

不合理用途：

- 用同一段 OOS 指标自动选因，再回测同一段 OOS 时间。
- 反复根据 OOS 排名调因子、调阈值、调权重，再把该 OOS 作为最终证明。
- 把当前回顾性 OOS 当作历史时点可见的生产 OOS。

当前 `out_sample` 更准确的语义应理解为：

> 站在当前快照日期，回看 QE 默认测试期之后的回顾性测试段指标。

它不是严格意义上的历史 as-of OOS。

## 5. 为什么短期可以暂不处理

当前阶段的目标不是自动因子组合选择，而是：

- 确保 H5/Bin 数据与 ST PIT 股票池可用。
- 确保 QE 回测、模拟盘、未来实盘选股链路一致。
- 使用 QE 回测做候选策略压力测试和执行链路验证。
- 通过模拟盘验证当前因子组合在未来未知行情中的真实表现。

在这个目标下，当前架构可以继续使用：

- 当前最新 full/recent/OOS 指标对今天以后的模拟盘和实盘不是未来信息。
- 因子选择目前主要是人工/半人工判断，不是自动用 OOS 硬筛历史回测。
- 历史回测的定位是候选策略验证，不是论文级无偏历史证明。

因此短期不需要：

- 新增 as-of 因子指标快照表。
- 重算所有历史时点独立指标。
- 立即补齐非 full 窗口的多周期 OOS RankIC。
- 重构因子评级和因子库 UI。
- 建设 walk-forward 因子自动选择流程。

## 6. 当前阶段使用原则

### 6.1 当前模拟盘/实盘候选因子选择

可以继续使用当前最新指标：

- full 长期 IC/RankIC/ICIR。
- recent_6m / recent_3m 衰退观察。
- coverage、turnover、相关性、因子逻辑解释。
- OOS 作为历史回顾性参考，而不是唯一排序依据。

原因：对当前日期之后的模拟盘/实盘来说，这些指标都是已知历史。

### 6.2 历史 QE 回测解释

历史 QE 回测结果应定位为：

> 站在当前时点，根据当前因子库认知选择候选因子/策略，并用历史区间验证其交易表现、稳定性、风险和执行可行性，供后续模拟盘/实盘观察。

不应表述为：

> 在 2024-07-01 当时，一定可以根据当时信息无偏选出这些因子并获得该回测收益。

### 6.3 OOS 指标

短期建议：

- OOS 只做展示、审计和近期表现观察。
- 不把 OOS 指标作为 QE 历史回测前置自动选因硬门槛。
- 如果 UI 或报告展示 OOS，应标注“回顾性测试段指标”。

## 7. 后续需要解决的问题

当系统进入因子自动组合选择、因子权重优化、历史可复现选因验证阶段时，需要解决以下问题。

### 7.1 指标 as-of 口径

未来历史回测选因必须满足：

```text
metrics_data_end <= backtest_start - embargo
label_matured_end <= selection_asof_date - max(return_horizon, label_horizon)
```

即：任何会影响因子选择、方向、权重、最佳周期、过滤规则的指标，都必须在回测开始前已经可见。

### 7.2 因子指标快照

未来可新增或扩展因子指标快照能力：

- `metrics_asof_date`
- `data_start`
- `data_end`
- `label_matured_end`
- `eval_window = full_asof / trailing_24m / trailing_12m / trailing_6m`
- `ic_mean / rank_ic_mean / icir / rank_icir`
- `rank_ic_1d / rank_ic_5d / rank_ic_10d / rank_ic_20d`
- `best_horizon / direction / best_horizon_advantage`
- `coverage / turnover / monotonicity`
- `universe / universe_rule_version / coverage_semantics`

### 7.3 月度 IC 轻量 as-of 选因

当前已有 `aistock_factor_monthly_ic`，未来可以先做轻量版本：

- 只使用回测开始日前的月度 IC。
- 计算 trailing 12m / 24m IC 和 RankIC。
- 使用 `sign_consistency_12m`、`trend_slope_12m`、`oos_is_ratio` 做衰退判断。

这可以作为完整 as-of 指标快照前的过渡方案，但不能完全替代 coverage、turnover、相关性、多周期 RankIC 等指标。

### 7.4 walk-forward 因子组合验证

未来如果要验证“近两年量化占比提高，近期表现好的因子更适合未来”这一假设，应使用 walk-forward：

1. 每月或每季度设定一个决策日。
2. 只用决策日前可见的 trailing 指标选因。
3. 固定因子组合和权重，交易未来 1 个月或 1 个季度。
4. 滚动拼接结果，比较 full-history 选因、recent-window 选因、人工选因等方案。

### 7.5 因子评级和 UI 语义隔离

未来需要区分：

| 指标用途 | 说明 |
|---|---|
| current_snapshot_metrics | 当前时点可用于未来模拟盘/实盘参考 |
| historical_asof_metrics | 历史回测开始日前可见指标 |
| retrospective_test_metrics | 回测后审计指标，不参与同区间选因 |
| live_forward_metrics | 模拟盘/实盘启动后的真实后验表现 |

## 8. 触发后续改造的条件

满足以下任一条件时，应启动后续改造：

- 开始实现基于因子指标的自动因子组合选择。
- 开始用因子指标自动决定因子权重、方向或最佳持有周期。
- 需要证明历史 QE 回测是严格 as-of、无未来选择偏差。
- 需要比较不同因子选择方法在历史上的优劣。
- 需要把历史回测收益作为上线或资金分配的强依据。
- 需要生成可对外解释的论文级/机构级无偏 OOS 研究报告。

## 9. 当前短期决策

短期决策如下：

1. 不改造因子库独立指标主流程。
2. 不立即补齐非 full 窗口的 OOS 多周期 RankIC。
3. 不新增 as-of 指标快照表。
4. 不实现自动因子组合选择。
5. 继续使用当前指标辅助人工/半人工因子选择。
6. QE 回测继续作为模拟盘/实盘前压力测试和执行链路验证。
7. 因子组合真实有效性以模拟盘和未来实盘 forward validation 为核心依据。

## 10. 后续实施优先级建议

| 优先级 | 任务 | 触发时机 |
|---|---|---|
| P0 | 在文档/UI 中标注 OOS 是回顾性测试段指标 | 下次因子库 UI 或报告改造时 |
| P1 | 增加 `metrics_asof_date` 概念和回测报告记录 | 开始自动选因前 |
| P1 | 基于月度 IC 做轻量 as-of 选因验证 | 需要验证近期表现选因假设时 |
| P2 | 完整 as-of 因子指标快照 | 自动因子组合选择前 |
| P2 | walk-forward 因子组合评估 | 自动组合/权重优化前 |
| P3 | 重构评级服务区分 current/asof/retrospective/live | 因子评级全面产品化时 |

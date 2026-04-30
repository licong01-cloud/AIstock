# HMM 后续优化方向规划

日期：2026-04-28

目的：基于当前 HMM 训练、PIT 重训、脚本级 6 个月验证结果，整理后续 HMM 优化方向，作为后续 QE 实验和 HMM 训练迭代的规划依据。

## 1. 当前结论

当前 HMM 的核心问题不是单纯缺少 5D/10D/20D 校准，而是 **HMM 如何干预 Top50 选股** 仍不稳定。

已完成的关键动作：

- 删除旧泄漏未来的 `HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore`，避免 QE 实验误选。
- 新 PIT w5/zscore 版本已重训，但没有复现旧 diagnostic 版本的收益。
- Horizon v2 已经做了 5D/10D/20D validation utility 校准，但脚本回测表现差。
- 当前正式 PIT-compatible 脚本候选中，`HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B` 表现最好。

当前主要候选：

| 候选 | 定位 |
|---|---|
| No-HMM | 必须保留的真实基准 |
| `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B` | 当前最优 PIT-compatible 脚本候选 |
| `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_A` | 新 PIT w5/zscore 温和加权候选 |
| `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_B` | 新 PIT w5/zscore 激进加权候选 |
| `HMM_HORIZON_V2...::preset_horizon_v2_risk_only` | 仅用于诊断 horizon-aware / risk-only 思路，不建议默认进入 QE 对比 |

## 2. 问题复盘

### 2.1 脚本回测不是最终真实效果

当前脚本回测使用：

```text
raw_score = trailing 5D/10D/20D return rank
adjusted_score = raw_score * sector_hmm_coefficient
```

该 raw score 只是 proxy，不是真实 QE 模型预测分数，也不包含：

- 真实 QE 因子权重；
- 5D/10D/20D RankIC 加权后的模型输出；
- V25 minute execution；
- 停牌、涨跌停、买入未成交；
- 真实持仓路径和资金使用率。

因此脚本结果只适合作为 HMM overlay 初筛，不能替代 QE 实验。

### 2.2 负收益主要来自 raw signal 弱，而不是最后几天大跌

同窗口宽基指数整体为正，而脚本中的 Raw Top50 为负，说明该 proxy raw score 在该窗口选股表现弱。HMM 只是对 raw ranking 做行业状态加权，无法稳定修复一个弱 raw signal。

后续必须用真实 QE score 验证 HMM 效果。

### 2.3 Horizon v2 证明了 state utility 不等于 portfolio utility

Horizon v2 已按 5D/10D/20D validation utility 校准状态系数，但仍然表现较差。

原因是它优化的是：

```text
行业状态平均 forward return
```

而真实组合收益取决于：

```text
HMM 替换进 Top50 的股票是否优于被替换出去的 raw-only 股票
```

因此下一版优化目标必须从 state utility 转向 replacement utility。

## 3. P0：真实 QE Score 验证

下一步首先应使用 QE 实验验证真实效果。要求：

- 同一 QE 模型；
- 同一回测窗口；
- 同一 stock pool；
- 同一 V25 执行配置；
- 同一资金、TopK、持仓、调仓参数；
- 唯一变量是 HMM snapshot + preset，或关闭 HMM。

建议 QE 对比候选只保留：

1. No-HMM
2. `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore::preset_B`
3. `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_A`
4. `HMM_COVFIX_w5_zscore_PIT_6m__n3_diag_rw5_zscore::preset_B`

不应在 QE 选择列表里展示已删除或不需要的 diagnostic 候选。

Horizon v2 只有在专门分析 horizon-aware 失败原因时才加入，不建议作为默认 QE 对比项。

## 4. P1：从 State Utility 改为 Replacement Utility

这是最高优先级的 HMM 训练/校准方向。

当前校准逻辑：

```text
state_utility = mean(label forward return)
```

建议改为：

```text
replacement_utility =
  mean(HMM-only forward return) - mean(raw-only forward return)
```

需要分别计算：

- 5D replacement spread；
- 10D replacement spread；
- 20D replacement spread；
- 5D/10D/20D 加权 replacement spread；
- HMM 替换胜率；
- 替换贡献对 NAV 的影响。

只有当 HMM-only 稳定优于 raw-only 时，才允许提高 HMM 干预强度。

建议固定输出字段：

| 字段 | 含义 |
|---|---|
| `avg_replaced_count` | 每期 HMM 替换 Top50 股票数 |
| `hmm_only_fwd5/10/20` | HMM 新选入股票未来收益 |
| `raw_only_fwd5/10/20` | 被 HMM 换出股票未来收益 |
| `replacement_spread_5/10/20` | HMM-only 减 raw-only |
| `replacement_win_rate` | 替换行为胜出的期数占比 |
| `replacement_contribution` | 替换对组合收益的贡献 |

## 5. P2：限制 HMM 替换数量

当前乘法方式：

```text
adjusted_score = raw_score * sector_coefficient
```

问题是：小幅系数差也可能在 Top50 边界造成大量替换。

建议加入 replacement cap：

| 方案 | 含义 |
|---|---|
| `cap_0` | No-HMM |
| `cap_3` | 每期最多替换 3 只 |
| `cap_5` | 每期最多替换 5 只 |
| `cap_10` | 每期最多替换 10 只 |
| `uncapped` | 当前无限制乘法方案 |

优先测试 `cap_3` 和 `cap_5`。

## 6. P3：从 Multiplicative Overlay 改为 Rank Overlay

乘法 score 依赖 raw score 的数值尺度，容易放大边界扰动。

建议测试：

```text
adjusted_rank = raw_rank + alpha * hmm_rank_delta
```

或：

```text
adjusted_score = raw_score + alpha * hmm_signal_score
```

候选方案：

| 方案 | 说明 |
|---|---|
| `multiplicative` | 当前乘法方案 |
| `additive_rank_0.05` | HMM 贡献 5% rank 调整 |
| `additive_rank_0.10` | HMM 贡献 10% rank 调整 |
| `rank_cap_5` | rank overlay + 最多替换 5 只 |

## 7. P4：HMM 作为 Risk Filter，而不是 Alpha Boost

旧 `preset_A/B` 默认奖励 `trending`，但多个验证结果说明 `trending` 在中长周期不一定是好状态。

更稳健的方向是只做风险过滤：

```json
{
  "good_state": 1.00,
  "neutral": 1.00,
  "bad_state": 0.95
}
```

候选方案：

| 方案 | 含义 |
|---|---|
| `risk_filter_only_0.98` | 差状态轻微降权 |
| `risk_filter_only_0.95` | 差状态中等降权 |
| `no_buy_bad_state` | 差状态行业不允许新买入 |
| `hold_allowed_no_new_buy` | 已持仓可继续持有，但不新增 |

## 8. P5：使用 Posterior Confidence

当前通常取：

```text
state = argmax(posterior)
```

如果状态后验概率接近，HMM 判断并不可靠，不应强行调整。

建议加入：

```text
confidence = max_posterior - second_max_posterior
```

只有当置信度超过阈值才启用 HMM。

候选阈值：

| 阈值 | 含义 |
|---|---|
| `0.00` | 当前：总是启用 |
| `0.10` | 状态略明确才启用 |
| `0.20` | 状态较明确才启用 |
| `0.30` | 高置信度才启用 |

## 9. P6：Rolling/PIT 多快照

单一 snapshot 容易产生窗口依赖。

建议每月训练一个 PIT snapshot：

| Snapshot | Train | Validation | Test |
|---|---|---|---|
| 2025-09 | 2022-06 ~ 2025-05 | 2025-06 ~ 2025-08 | 2025-09 |
| 2025-10 | 2022-07 ~ 2025-06 | 2025-07 ~ 2025-09 | 2025-10 |
| 2025-11 | 2022-08 ~ 2025-07 | 2025-08 ~ 2025-10 | 2025-11 |

验收重点：

- 每个 snapshot 的训练截止日早于测试月；
- 每个月是否稳定改善 Raw；
- 不接受只在单一窗口有效的 HMM。

## 10. P7：按 Holding Horizon 拆分 Preset

`preset_A/B` 命名不透明，且默认奖励 `trending`。

建议后续使用语义化命名：

| 新 preset | 含义 |
|---|---|
| `horizon_5d_calibrated` | 面向 5D 收益 |
| `horizon_10d_calibrated` | 面向 10D 收益 |
| `horizon_20d_calibrated` | 面向 20D 收益 |
| `horizon_5_10_20_blend` | 5D/10D/20D 混合 |
| `risk_filter_5d` | 5D 风险过滤 |
| `replacement_capped_5` | 每期最多替换 5 只 |

QE 选择列表中应展示语义化名称，避免用户误解 `preset_A/B`。

## 11. P8：特征优化

可以继续优化行业状态特征，但必须严格 PIT。

候选特征方向：

| 特征方向 | 说明 |
|---|---|
| sector breadth | 行业内上涨股票占比 |
| sector dispersion | 行业内收益分化程度 |
| sector volatility regime | 行业波动率状态 |
| sector drawdown | 行业短中期回撤 |
| excess momentum 5/10/20 | 行业相对指数动量 |
| reversal signal | 行业短期过热/反转 |
| money flow persistence | 资金流持续性 |
| turnover/liquidity | 成交活跃与流动性 |
| cross-sector rank | 行业相对全行业排名 |

`limit_up_ratio` 需谨慎：除非训练、预计算、QE runtime 完全 PIT 一致，否则不要重新加入默认特征。

### 11.1 行业特征数据复杂度与运行方式

这些行业级特征在现有数据源中基本都可以精确计算，复杂度总体可控。当前更重要的问题不是算力，而是 **PIT 对齐、数据刷新时点、特征缓存版本、以及实盘选股链路的稳定性**。

本方向作为中长期优化储备，暂不列入近期最高优先级实施项。近期仍应优先完成真实 QE score 验证、replacement utility、replacement cap/rank overlay、risk filter only 和 rolling/PIT 多快照验证。

复杂度分层：

| 特征 | 主要数据来源 | 复杂度 | 实盘是否适合临时计算 | 说明 |
|---|---|---|---|---|
| sector breadth | `kline_daily_raw` + `adj_factor` + `sw_index_member` | 中 | 不建议 | 需要股票级收益和 PIT 行业成员聚合 |
| sector dispersion | `kline_daily_raw` + `adj_factor` + `sw_index_member` | 中 | 不建议 | 需要行业内股票收益标准差、IQR 或分位差 |
| sector volatility regime | `sw_daily` / `sector_data` | 低 | 可以，但建议预计算 | 行业指数 rolling volatility，计算量很小 |
| sector drawdown | `sw_daily` / `sector_data` | 低 | 可以，但建议预计算 | 行业级 rolling max/min 即可 |
| excess momentum 5/10/20 | `sw_daily` + `index_daily` | 低 | 可以，但建议预计算 | 行业收益减基准收益 |
| reversal signal | `sw_daily` / `sector_data` | 低 | 可以 | 多数是 1D/3D/5D 过热或反转信号 |
| money flow persistence | `moneyflow_ts` + `sw_index_member` | 中高 | 不建议 | 股票级资金流按 PIT 行业聚合，数据量较大 |
| turnover/liquidity | `daily_basic` / `sw_daily` | 中或低 | 视实现 | 用行业指数低复杂度，用股票级加权聚合则中等复杂度 |
| cross-sector rank | 上述行业特征结果 | 极低 | 可以 | 约 131 个行业横截面排序，几乎无压力 |

建议的工程落地方式是“离线/盘后预计算，选股时只读结果”：

```text
daily market refresh
  -> build sector_features_daily
  -> run HMM sector inference
  -> write hmm_sector_coefficients_daily / coefficient artifact
  -> Selection Center / QE runtime / Paper v2 read coefficient only
```

不建议在模拟盘或实盘选股请求中临时执行：

```text
selection request
  -> query all stock daily data
  -> aggregate sector features
  -> run HMM inference
  -> adjust score
```

原因是该方式虽然算力上可行，但工程上更容易出现数据刷新不完整、PIT 错位、运行耗时不稳定、模型输入不可审计等问题。

### 11.2 回测、模拟盘、实盘的数据时点要求

回测阶段可以提前计算全区间特征，但每个 `trade_date` 的特征必须只使用当时可见数据：

- 如果模拟 T 日开盘买入，只能使用 T-1 收盘后已经可见的数据生成 HMM coefficient。
- 如果模拟 T 日收盘生成 T+1 调仓信号，可以使用 T 日收盘后的日线、资金流和 `daily_basic` 数据。
- 行业成员必须使用 PIT 条件：`in_date <= trade_date AND (out_date IS NULL OR out_date >= trade_date)`。
- validation forward return 只能用于训练和校准，不允许进入当日选股特征。
- 所有特征缓存都应记录 `feature_version`、`source_max_trade_date`、`pit_membership_policy`、`adjustment_policy` 和生成时间。

模拟盘/实盘阶段建议流程：

```text
T 日盘后数据刷新完成
  -> 数据刷新审计通过
  -> 计算 T 日 sector_features_daily
  -> 生成 T+1 可用 HMM coefficient
  -> T+1 选股时读取 coefficient artifact
```

选股时真正需要的输入应保持轻量：

- QE 原始股票分数；
- 股票 PIT 行业归属；
- `trade_date + sector_code + preset/model_snapshot_id` 对应的 HMM coefficient；
- 调整后分数和完整 trace。

如果数据源未刷新完成、行业特征缺失、HMM coefficient 缺失，正式 runtime 应 fail-fast 或使用明确允许的上一版 artifact，不应静默回退为 neutral。

### 11.3 后续可选落地形态

未来如果要实施该方向，建议新增独立缓存和脚本，不覆盖现有 HMM 训练脚本：

| 组件 | 建议定位 |
|---|---|
| `sector_features_daily` | 每日行业特征缓存，可先落 parquet，稳定后再入库 |
| `hmm_sector_coefficients_daily` | 每日行业 HMM 系数缓存或 artifact |
| `scripts/hmm_build_sector_features_*.py` | PIT 行业特征构建脚本 |
| `scripts/hmm_precompute_sector_coefficients_*.py` | 根据模型 snapshot 生成每日行业系数 |
| runtime trace | 记录股票行业、原始分数、HMM 系数、最终分数、缺失原因 |

优先级建议：

1. 先验证现有 HMM 是否能在真实 QE score 中稳定增益。
2. 再优化 HMM 对 Top50 的替换方式和干预强度。
3. 最后再扩展上述行业特征，避免在 HMM runtime 机制尚未稳定前增加特征复杂度。

## 12. P9：模型结构优化

如果 HMM 状态持续时间仍然不稳定，可考虑：

| 模型 | 优点 |
|---|---|
| sticky HMM | 增强状态持续性 |
| Hidden Semi-Markov Model | 显式建模状态持续时间 |
| Markov Switching Regression | 状态直接解释收益分布 |
| regime classifier + calibration | 用分类器替代 HMM 状态解码 |

短期不建议立即替换模型结构，应优先解决 replacement utility 和干预强度问题。

## 13. P10：保护强股票 Alpha

HMM 是行业状态，QE score 是股票 alpha。HMM 不应覆盖极强股票 alpha。

建议：

```text
Top30 股票不受 HMM 影响；
HMM 只调整 Rank 31~80；
HMM 不允许 Rank 200 以后股票进入 Top50。
```

这样可以减少 HMM 对核心 alpha 的破坏。

## 14. 固定验收表

后续每个 HMM 版本必须输出：

| 类型 | 指标 |
|---|---|
| 收益 | 总收益、年化、Sharpe、MaxDD |
| 替换 | 平均替换数、替换股票列表 |
| HMM-only | 5D/10D/20D forward return |
| Raw-only | 5D/10D/20D forward return |
| 差值 | HMM-only - Raw-only |
| 月度 | 每月收益差异 |
| 贡献 | top/bottom 股票贡献 |
| 交易 | 资金使用率、买入未成交率、最终持仓数 |
| 稳定性 | rolling 月胜率 |
| PIT | train/validation/test 是否重叠 |

如果 HMM-only 长期低于 raw-only，即使总收益偶尔变好，也不应接受。

## 15. 推荐实施顺序

1. **P0：真实 QE Score 验证**
   先确认在真实 QE 分数下，w3 preset_B 是否仍是最优。

2. **P1：Replacement Utility 训练/校准**
   不再只优化行业状态平均收益。

3. **P2 + P3：Replacement Cap / Rank Overlay**
   限制每期替换数量，降低 HMM 误伤。

4. **P4：Risk Filter Only**
   HMM 优先作为风险过滤模块，而不是 alpha boost。

5. **P6：Rolling/PIT 多快照**
   验证跨月稳定性，避免窗口依赖。

6. **P8：行业特征扩展与预计算架构**
   作为未来优化方向保留，当前不作为近期高优先级实施项；应等真实 QE score 验证和 HMM 干预方式稳定后再推进。

## 16. 一句话结论

HMM 下一阶段的优化方向应从“给行业状态设置乘数”升级为“受控的 Top50 替换和风险过滤模块”。核心验收标准是：

```text
HMM 替换进来的股票，是否在 5D/10D/20D 上稳定优于被替换出去的股票。
```

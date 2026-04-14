# 因子库深度诊断报告

生成时间: 2026-03-31 23:00
因子总数: 704 | 有指标: 1702

## 1. IC 分布与信号质量诊断

有指标因子总数: **1702**

### 1.1 IC 方向分布（正=多头有效 / 负=需反转）

| IC 区间 | 数量 | 占比 |
|---------|------|------|
| IC>0.03 (强) | 10 | 0% |
| 0.02<IC≤0.03 | 43 | 2% |
| 0.01<IC≤0.02 | 173 | 10% |
| 0<IC≤0.01 (弱) | 395 | 23% |
| IC≤0 (无效/反向) | 1081 | 63% |

### 1.2 |IC| 绝对值分布（信号强度）

| |IC| 区间 | 数量 | 占比 |
|----------|------|------|
| |IC|>0.03 | 183 | 10% |
| 0.02<|IC|≤0.03 | 228 | 13% |
| 0.01<|IC|≤0.02 | 374 | 21% |
| |IC|≤0.01 | 917 | 53% |

### 1.3 IC 统计摘要

- 均值: -0.0063
- 中位数: -0.0032
- 标准差: 0.0158
- 最大值: 0.0355
- 最小值: -0.0595
- 正IC因子占比: 36%
- |IC|>0.02 因子数: 411 (24%)


## 2. 因子同质化 / 重复性诊断

### 2.1 名称高度相似的因子组（共 31 组）

- **4个**: Volatility_Adjusted_Momentum_14D, Volatility_Adjusted_Momentum_20D, volatility_10D, volatility_20d
- **3个**: Excess_Industry_Return_Factor, Excess_Return_Industry, Industry_Excess_Return
- **3个**: Liquidity_Turnover_Factor, Liquidity_Turnover_Rate, liquidity_turnover
- **3个**: MF_Main_Net_Amt_Momentum_5D, MF_Main_Net_Amt_Ratio_5D, mf_main_net_amt_ratio_5d
- **3个**: Momentum_5D_Return, Momentum_Return_20d, Return_5D
- **3个**: Price_Volume_Divergence_10D, Volume_Price_Divergence_10D, volume_price_divergence_5d
- **3个**: cost_pressure, cost_pressure_adjusted, cost_pressure_factor
- **3个**: momentum_price_volume, price_volume_momentum, volume_price_momentum
- **3个**: value_momentum_reversal, value_reversal_composite, value_reversal_factor
- **3个**: volume_momentum_5d, volume_ratio_5D, volume_ratio_5d
- **2个**: Cost_Distribution_Dispersion, cost_distribution_dispersion_factor
- **2个**: MF_Elg_Share_In_Main_Amt, mf_elg_share_in_main_amt
- **2个**: MainNetInflowRatio_1D, MainNetInflowRatio_5D
- **2个**: Net_Inflow_Momentum_Composite, net_inflow_ratio
- **2个**: PE_Static_Dynamic_Ratio, dynamic_static_pe_ratio
- ... 另有 16 组

### 2.2 IC 几乎相同的因子对（差异<0.0001，共 467 对）

| 因子A | 因子B | IC_A | IC_B | 类别A | 类别B |
|-------|-------|------|------|-------|-------|
| size_adjusted_turnover | size_liquidity_composite | -0.0488 | -0.0488 | LIQ | SIZE |
| size_liquidity_composite | size_log_mv_turnover_interacti | -0.0488 | -0.0488 | SIZE | VAL |
| Turnover_Rate_Factor | liquidity_turnover | -0.0472 | -0.0472 | LIQ | LIQ |
| SizeAdjTurnover | SizeAdjustedTurnover | -0.0454 | -0.0454 | VAL | VAL |
| turnover_adjusted_volatility | IntradayVolatility_5D | -0.0450 | -0.0449 | LIQ | VOL |
| IntradayVolatility_5D | large_order_inflow_strength | -0.0449 | -0.0448 | VOL | VAL |
| size_adjusted_turnover_5d | PriceStrength_10D | -0.0397 | -0.0397 | VAL | MOM |
| Momentum_5D | close_5d_ret | -0.0390 | -0.0390 | LIQ | MOM |
| close_5d_ret | bid_ask_spread_change_factor | -0.0390 | -0.0390 | MOM | VOL |
| volatility_20d | momentum_price_volume | -0.0377 | -0.0377 | VOL | LIQ |
| momentum_price_volume | momentum_volume_ratio | -0.0377 | -0.0376 | LIQ | LIQ |
| high_amount_turnover_momentum_ | Momentum_Return_20d | -0.0371 | -0.0371 | LIQ | MOM |
| defensive_low_vol_high_turnove | PriceMomentum20D | -0.0359 | -0.0358 | LIQ | MOM |
| PriceMomentum20D | momentum_20D | -0.0358 | -0.0357 | MOM | MOM |
| Momentum_5D_Return | Value_Liquidity_Adjustment | -0.0347 | -0.0346 | MOM | VAL |
| price_strength_volume_adjusted | db_value_turnover | -0.0335 | -0.0334 | LIQ | VAL |
| momentum_5d_volume_weighted | Composite_Factor_Multi_Dim | -0.0332 | -0.0332 | MOM | VOL |
| AdjustedPriceMomentumVolume | Momentum_Volume_Confirmation | -0.0327 | -0.0327 | LIQ | LIQ |
| mf_main_net_amt_std_5d | vol_adjusted_momentum | -0.0326 | -0.0325 | LIQ | MOM |
| turnover_anomaly | Large_Cap_Momentum_Bias | -0.0309 | -0.0308 | LIQ | VAL |

*另有 447 对...*

### 2.3 代码计算模式相同的因子组（≥3个因子使用相同操作组合，共 6 组）

- **16个因子**共享模式 `.mean(:.mean(|.std(:.std(...`
  例: Composite_Factor_Multi_Dim, MF_Stability_Enhanced_5D, composite_score, composite_sentiment_liquidity
- **8个因子**共享模式 `.shift((\d+)):1|.std(:.std(...`
  例: InvVol_20D, VolAdjMomentum_10D, VolAdj_Momentum_10D, Volatility_Inverse_10D
- **4个因子**共享模式 `.shift((\d+)):20|.std(:.std(...`
  例: adaptive_volatility_momentum_20d, dynamic_valuation_sentiment, roe_change_momentum, volatility_adjusted_value_momentum
- **4个因子**共享模式 `.shift((\d+)):5|.std(:.std(...`
  例: dynamic_pe_inv_momentum_turnover_ratio, moneyflow_momentum, short_term_reversal_5d, turnover_volatility_adjusted_valuation
- **3个因子**共享模式 `.shift((\d+)):1,5|.std(:.std(...`
  例: MomentumReversal_5D_20V, close_return_5d_vol_adj, flow_intensity_volatility_adjusted_composite
- **3个因子**共享模式 `.mean(:.mean(,.mean(|.std(:.std(...`
  例: flow_residual_volatility_weighted, sentiment_adjusted_flow_residual, vol_adj_momentum


## 3. 数据集利用深度分析

### 3.1 各数据集字段使用频次

**daily_pv.h5**:

| 字段 | 使用次数 | 状态 |
|------|---------|------|
| factor | 699 | 充分 |
| low | 580 | 充分 |
| close | 572 | 充分 |
| amount | 535 | 充分 |
| volume | 522 | 充分 |
| high | 515 | 充分 |
| open | 503 | 充分 |

**daily_basic.h5**:

| 字段 | 使用次数 | 状态 |
|------|---------|------|
| db_turnover_rate | 80 | 充分 |
| db_circ_mv | 51 | 充分 |
| db_pe | 43 | 中等 |
| db_pb | 34 | 中等 |
| db_total_mv | 31 | 中等 |
| db_pe_ttm | 25 | 中等 |
| db_close | 14 | 中等 |
| db_turnover_rate_f | 14 | 中等 |
| db_volume_ratio | 8 | 不足 |
| db_ps | 7 | 不足 |
| db_dv_ratio | 7 | 不足 |
| db_ps_ttm | 4 | 不足 |
| db_dv_ttm | 4 | 不足 |
| db_float_share | 3 | 不足 |
| db_total_share | 1 | 不足 |
| db_free_share | 1 | 不足 |

**moneyflow.h5**:

| 字段 | 使用次数 | 状态 |
|------|---------|------|
| mf_elg_buy_amt | 116 | 充分 |
| mf_elg_sell_amt | 110 | 充分 |
| mf_lg_buy_amt | 79 | 充分 |
| mf_net_amt | 76 | 充分 |
| mf_lg_sell_amt | 70 | 充分 |
| mf_sm_buy_amt | 32 | 中等 |
| mf_sm_sell_amt | 31 | 中等 |
| mf_md_buy_amt | 22 | 中等 |
| mf_md_sell_amt | 22 | 中等 |
| mf_elg_buy_vol | 6 | 不足 |
| mf_sm_buy_vol | 5 | 不足 |
| mf_lg_buy_vol | 4 | 不足 |
| mf_elg_sell_vol | 4 | 不足 |
| mf_md_buy_vol | 3 | 不足 |
| mf_lg_sell_vol | 3 | 不足 |
| mf_net_vol | 3 | 不足 |
| mf_sm_sell_vol | 2 | 不足 |
| mf_md_sell_vol | 1 | 不足 |

**bak_basic.h5**:

| 字段 | 使用次数 | 状态 |
|------|---------|------|
| bb_pe_dyn | 60 | 充分 |
| bb_gpr | 26 | 中等 |
| bb_profit_yoy | 24 | 中等 |
| bb_npr | 22 | 中等 |
| bb_eps | 19 | 中等 |
| bb_rev_yoy | 19 | 中等 |
| bb_bvps | 10 | 不足 |
| bb_total_assets | 8 | 不足 |
| bb_liquid_assets | 6 | 不足 |
| bb_fixed_assets | 5 | 不足 |
| bb_reserved | 2 | 不足 |
| bb_undp | 2 | 不足 |
| bb_holder_num | 2 | 不足 |
| bb_reserved_pershare | 1 | 不足 |
| bb_per_undp | 1 | 不足 |

**cyq_perf.h5**:

| 字段 | 使用次数 | 状态 |
|------|---------|------|
| cp_cost_50pct | 53 | 充分 |
| cp_cost_5pct | 35 | 中等 |
| cp_weight_avg | 34 | 中等 |
| cp_winner_rate | 34 | 中等 |
| cp_cost_95pct | 33 | 中等 |
| cp_cost_15pct | 32 | 中等 |
| cp_cost_85pct | 32 | 中等 |
| cp_his_high | 18 | 中等 |
| cp_his_low | 13 | 中等 |

**sector_data.h5**:

| 字段 | 使用次数 | 状态 |
|------|---------|------|
| sw2_pct_change | 17 | 中等 |
| sw2_pb | 10 | 不足 |
| sw2_close | 8 | 不足 |
| sw2_pe | 7 | 不足 |
| sw2_vol | 6 | 不足 |
| sw2_amount | 4 | 不足 |
| sw2_total_mv | 4 | 不足 |
| sw2_high | 3 | 不足 |
| sw2_low | 2 | 不足 |
| sw2_open | 0 | **未使用** |

### 3.2 未使用/低使用字段汇总

**完全未使用 (1 个):**
- `sector_data.h5` → `sw2_open`

**使用<5次 (21 个):**
- `daily_basic.h5` → `db_ps_ttm` (4次)
- `daily_basic.h5` → `db_dv_ttm` (4次)
- `daily_basic.h5` → `db_total_share` (1次)
- `daily_basic.h5` → `db_float_share` (3次)
- `daily_basic.h5` → `db_free_share` (1次)
- `moneyflow.h5` → `mf_sm_sell_vol` (2次)
- `moneyflow.h5` → `mf_md_buy_vol` (3次)
- `moneyflow.h5` → `mf_md_sell_vol` (1次)
- `moneyflow.h5` → `mf_lg_buy_vol` (4次)
- `moneyflow.h5` → `mf_lg_sell_vol` (3次)
- `moneyflow.h5` → `mf_elg_sell_vol` (4次)
- `moneyflow.h5` → `mf_net_vol` (3次)
- `bak_basic.h5` → `bb_reserved` (2次)
- `bak_basic.h5` → `bb_reserved_pershare` (1次)
- `bak_basic.h5` → `bb_undp` (2次)
- `bak_basic.h5` → `bb_per_undp` (1次)
- `bak_basic.h5` → `bb_holder_num` (2次)
- `sector_data.h5` → `sw2_high` (3次)
- `sector_data.h5` → `sw2_low` (2次)
- `sector_data.h5` → `sw2_amount` (4次)
- `sector_data.h5` → `sw2_total_mv` (4次)

### 3.3 跨数据集组合使用频次

| 数据集组合 | 因子数 |
|-----------|--------|
| daily_pv + moneyflow | 102 |
| daily_basic + daily_pv | 83 |
| cyq_perf + daily_pv | 81 |
| daily_basic + daily_pv + moneyflow | 71 |
| bak_basic + daily_pv | 57 |
| bak_basic + daily_basic + daily_pv | 46 |
| bak_basic + cyq_perf + daily_pv | 30 |
| cyq_perf + daily_basic + daily_pv | 18 |
| daily_pv + sector_data | 17 |
| daily_basic + daily_pv + sector_data | 16 |
| cyq_perf + daily_pv + moneyflow | 15 |
| bak_basic + daily_pv + sector_data | 13 |
| cyq_perf + daily_basic + daily_pv + moneyflow | 9 |
| bak_basic + daily_pv + moneyflow | 9 |
| bak_basic + daily_basic + daily_pv + moneyflow | 5 |


## 4. 各类别内部质量诊断

| 类别 | 因子数 | 正IC数 | 正IC率 | 平均IC | 中位IC | 最佳IC | IC>0.02数 | 平均Sharpe | 平均年化 |
|------|--------|--------|--------|--------|--------|--------|----------|-----------|---------|
| 筹码 (CHIP) | 309 | 123 | 39% | -0.0054 | -0.0034 | 0.0219 | 7 | 0.47 | 11.1% |
| 相关性 (CORR) | 29 | 6 | 20% | -0.0058 | -0.0063 | 0.0290 | 4 | 0.37 | 9.5% |
| 流动性 (LIQ) | 481 | 108 | 22% | -0.0102 | -0.0080 | 0.0355 | 19 | 0.36 | 8.5% |
| 资金流 (MF) | 164 | 70 | 42% | -0.0022 | -0.0006 | 0.0286 | 3 | 0.39 | 9.6% |
| 机器学习 (ML) | 0 | - | - | - | - | - | - | - | - |
| 动量 (MOM) | 109 | 20 | 18% | -0.0193 | -0.0232 | 0.0312 | 1 | 0.10 | 2.3% |
| 质量 (QUAL) | 45 | 32 | 71% | 0.0011 | 0.0015 | 0.0091 | 0 | 0.60 | 15.0% |
| 规模 (SIZE) | 3 | 2 | 66% | -0.0152 | 0.0016 | 0.0016 | 0 | 0.33 | 7.4% |
| 统计 (STAT) | 9 | 1 | 11% | -0.0030 | -0.0082 | 0.0170 | 0 | 0.71 | 16.2% |
| 技术 (TECH) | 13 | 3 | 23% | -0.0114 | -0.0151 | 0.0015 | 0 | 0.16 | 3.7% |
| 价值 (VAL) | 420 | 228 | 54% | 0.0002 | 0.0015 | 0.0324 | 16 | 0.52 | 11.9% |
| 波动率 (VOL) | 120 | 28 | 23% | -0.0123 | -0.0104 | 0.0353 | 3 | 0.30 | 7.4% |

### 4.1 各类别有效因子率（IC>0.02 视为有效）

- **筹码**: 7/309 (2%)
- **相关性**: 4/29 (13%)
- **流动性**: 19/481 (3%)
- **资金流**: 3/164 (1%)
- **动量**: 1/109 (0%)
- **质量**: 0/45 (0%)
- **规模**: 0/3 (0%)
- **统计**: 0/9 (0%)
- **技术**: 0/13 (0%)
- **价值**: 16/420 (3%)
- **波动率**: 3/120 (2%)


## 5. 反向因子分析（IC<0 但 |IC|>0.02）

这些因子虽然 IC 为负，但取反后可能是有效的 alpha 信号:

**反向因子总数: 358**

| 因子名 | IC | ICIR | 类别 | 取反后IC |
|--------|-----|------|------|---------|
| chip_support_intensity_free_turnove | -0.0595 | -0.398 | CHIP | 0.0595 |
| chip_support_intensity_free_turnove | -0.0595 | -0.398 | CHIP | 0.0595 |
| Liquidity_Turnover_Rate | -0.0495 | -0.293 | LIQ | 0.0495 |
| Liquidity_Turnover_Rate | -0.0495 | -0.293 | LIQ | 0.0495 |
| Liquidity_Turnover_Rate | -0.0495 | -0.293 | LIQ | 0.0495 |
| size_liquidity_composite | -0.0488 | -0.314 | SIZE | 0.0488 |
| size_adjusted_turnover | -0.0488 | -0.314 | LIQ | 0.0488 |
| size_log_mv_turnover_interaction | -0.0488 | -0.314 | VAL | 0.0488 |
| Turnover_Rate_Factor | -0.0472 | -0.304 | LIQ | 0.0472 |
| Turnover_Rate_Factor | -0.0472 | -0.304 | LIQ | 0.0472 |
| liquidity_turnover | -0.0472 | -0.304 | LIQ | 0.0472 |
| Turnover_Rate_Factor | -0.0472 | -0.304 | LIQ | 0.0472 |
| turnover_rate | -0.0466 | -0.301 | LIQ | 0.0466 |
| free_turnover_log | -0.0457 | -0.285 | LIQ | 0.0457 |
| SizeAdjTurnover | -0.0454 | -0.294 | VAL | 0.0454 |
| SizeAdjustedTurnover | -0.0454 | -0.294 | VAL | 0.0454 |
| SizeAdjustedTurnover | -0.0454 | -0.294 | VAL | 0.0454 |
| SizeAdjustedTurnover | -0.0454 | -0.294 | VAL | 0.0454 |
| turnover_adjusted_volatility | -0.0450 | -0.269 | LIQ | 0.0450 |
| turnover_adjusted_volatility | -0.0450 | -0.269 | LIQ | 0.0450 |


## 6. 评级瓶颈分析

| 评级 | 因子数 | 加权平均IC | 加权平均Sharpe | 加权平均年化 | 加权平均回撤 |
|------|--------|-----------|--------------|------------|------------|
| S | 0 | - | - | - | - |
| A | 0 | - | - | - | - |
| B | 9 | 0.0252 | 1.12 | 28.6% | -39.0% |
| C | 221 | 0.0154 | 0.80 | 18.3% | -31.1% |
| D | 1472 | -0.0098 | 0.34 | 8.2% | -49.5% |

### 6.1 D 级因子根因分析

- D 级因子总数: 1472
- IC≤0（无正向信号）: 1078 (73%)
- 0<IC≤0.01（信号极弱）: 382 (25%)
- Sharpe≤0: 278 (18%)
- 最大回撤>30%: 1390 (94%)


## 7. 因子构造复杂度分析

### 7.1 数据集使用数量 vs 平均IC

| 使用数据集数 | 因子数 | 平均IC | 中位IC | IC>0.02率 |
|-------------|--------|--------|--------|----------|
| 1 | 118 | -0.0185 | -0.0234 | 2% |
| 2 | 340 | -0.0077 | -0.0049 | 2% |
| 3 | 224 | -0.0025 | -0.0008 | 2% |
| 4 | 18 | 0.0003 | 0.0007 | 0% |
| 5 | 2 | -0.0088 | -0.0088 | 0% |

### 7.2 操作复杂度 vs 平均IC

| 操作数区间 | 因子数 | 平均IC | IC>0.02率 |
|-----------|--------|--------|----------|
| 0-2 | 463 | -0.0061 | 1% |
| 3-5 | 136 | -0.0117 | 5% |
| 6-8 | 49 | -0.0104 | 0% |
| 9-11 | 30 | -0.0094 | 3% |
| 12-14 | 8 | -0.0133 | 0% |
| 15-17 | 5 | -0.0002 | 0% |
| 18-20 | 3 | -0.0145 | 0% |
| 21-23 | 3 | 0.0061 | 0% |
| 24-26 | 1 | -0.0006 | 0% |
| 27-29 | 2 | -0.0006 | 0% |
| 30-32 | 1 | 0.0019 | 0% |
| 36-38 | 1 | -0.0163 | 0% |


## 8. 关键缺失因子类型识别

### 8.1 经典量化因子覆盖检测（20 种经典因子）

- 已覆盖: 10
- **未覆盖: 10**

**缺失的经典因子类型:**

| 因子类型 | 所属类别 | 经济学意义 |
|---------|---------|-----------|
| Beta/市场敏感度 | 相关性 | 系统性风险暴露，CAPM核心风险因子 |
| 残差动量 | 相关性 | 剥离市场影响后的个股alpha，Blitz et al. (2011) |
| Amihud非流动性 | 流动性 | 价格冲击成本，流动性风险溢价 |
| Hurst指数 | 统计 | 时序记忆性检验，趋势/均值回复判断 |
| 峰度因子 | 统计 | 收益分布厚尾程度，极端事件频率 |
| 信息比率 | 统计 | 超额收益稳定性，因子有效性度量 |
| MACD/信号线 | 技术 | 趋势跟踪信号，捕捉价格动量转折 |
| OBV/能量潮 | 技术 | 量价关系确认，资金方向判断 |
| 自由现金流收益率 | 价值 | 企业真实造血能力，价值投资核心 |
| 最大回撤因子 | 波动率 | 下行风险度量，投资者行为锚定 |

**已覆盖的经典因子:** ATR/平均真实波幅, PEG, ROE/ROA动量, RSI/相对强弱, 偏度因子, 布林带/BB, 机构持仓变化, 股息率动量, 营收增速, 资金集中度


## 9. 综合问题清单（按严重度排序）

### 🔴 CRITICAL

- **63% 因子 IC≤0** — 超过半数因子无正向预测能力，因子库整体信号质量堪忧
- **86% 因子评级 D** — 绝大多数因子质量不合格，需要系统性重构

### 🟡 HIGH

- **467 对因子IC几乎相同** — 存在大量同质化因子，信息冗余
- **机器学习类仅0个因子** — 类别严重不足
- **统计类仅3个因子** — 类别严重不足
- **规模类仅2个因子** — 类别严重不足

### 🟠 MEDIUM

- **`sector_data.h5`→`sw2_open` 完全未使用** — 潜在信号未被挖掘
- **类别极度不平衡（最大/最小=160x）** — LIQ/VAL 过多，STAT/SIZE/TECH 过少

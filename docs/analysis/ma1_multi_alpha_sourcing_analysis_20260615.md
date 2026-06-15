# MA1 多 Alpha Sourcing Round1 实验分析

> **实验**：MA1-GPU (`qe_20260614_022643_edaf`) + MA1-CPU (`qe_20260614_022428_433d`)
> **日期**：2026-06-15（实验完成于 2026-06-14）
> **类型**：QE custom_evo · 多 Alpha 信号源勘探
> **回测区间**：2024-07-01 ~ 2026-04-27（1min 级）
> **目的**：用不同因子组合 × 不同模型族寻找更多**独立 alpha 信号源**，为多 Alpha 架构准备

---

## 0. TL;DR

- 两个对称实验，**3 个零重叠主题块（Flow / TurnMom / FundVal）× 2 个模型族（LSTM / LGBM）× 5 seeds = 30 loops**，全部完成。
- **C_FundVal × LSTM 是种子鲁棒性冠军**：ICIR=0.811（全场最高）、CAGR=83.8%、Sharpe=2.20、**种子 CV=2.7%**（彩票免疫）。
- **A_Flow 是 R20 5-Alpha 架构缺失的真·新独立信号源**（微观资金流），两模型接近、模型不敏感。
- **B_TurnMom 本质是低波/反转，与 R20 α4 VOL12 概念重叠**，且种子最不稳定（LSTM CV=18.7%），暂缓。
- **因子×模型强交互再次被证实**：FundVal/TurnMom → LSTM 优（+9~10pp CAGR），Flow → 两模型接近。
- **待办**：测 A_Flow / C_FundVal 预测值与 R20 α1/α2/α3 的互相关（因子零重叠 ≠ 预测独立）。

---

## 1. 实验概览

| 维度 | MA1-GPU (edaf) | MA1-CPU (433d) |
|------|----------------|----------------|
| Task ID | `qe_20260614_022643_edaf` | `qe_20260614_022428_433d` |
| 节点 | `wsl2-5080` (parallel_2) | `rdagent-node1` (parallel_4) |
| 模型 | `__seed_LSTM_10D_hs64_d02__`（序列） | `__seed_LGBModel_conservative_v1__`（树） |
| 结构 | 3 主题块 × 5 seeds = 15 loops | 同左 |
| Seeds | {42, 888, 2024, 2026, 12345} | 同左 |
| 配置 | topk25 / n_drop=2 / label_horizon=20 / V25_1_SMALL_CAP / no-HMM / 10M | 同左 |
| 状态 | completed (15/15) | completed (15/15) |

**设计意图**：同一组主题因子集喂给两种模型族，量化「因子集 × 模型」交互；3 个主题块天然去相关，是多 Alpha 架构信号源勘探的标准设计。GPU 跑 LSTM（序列模型）、CPU 跑 LGBM（树模型），双节点并行。

---

## 2. 三个零重叠主题块（各 12 因子）

### Block A — Flow（微观资金流）
```
dynamic_flow_volatility_sentiment
LargeOrder_Cost_Interaction
neg_Composite_Factor_Multi_Dim
ChipWinnerRateEliteBuyIntensity
neg_Elite_Sell_Size_Adjusted
neg_mf_main_net_amt_std_5d
bid_ask_spread_change_factor
SmallOrderIntensityBreakoutFactor
Industry_Extra_Large_Buy_Individual_Extra_Large_Sell_Strength_Ratio
small_order_flow_intensity
MF_Stability_Enhanced_5D
m_turnover_mf_divergence
```
**信号本质**：大/小单资金流、筹码胜率、买卖强度 —— 微观资金面、散户 vs 主力博弈。

### Block B — TurnMom（换手率动量）
```
m_tech_atr_ratio_14d
neg_PriceMomentum20D
Industry_Volatility_Liquidity_Cross_Factor
m_turnover_breakout_ratio
neg_TurnoverVolatilityEnhancement
m_free_turnover_rate
m_volume_contraction
neg_high_amount_turnover_momentum_5d
m_turnover_accel
m_ind_neutral_rev_5d
m_turnover_zscore_60d
m_free_turnover_ind_neutral
```
**信号本质**：自由换手率、ATR、行业中性反转、量缩 —— 量价换手 / 低波动反转。

### Block C — FundVal（基本面估值）
```
neg_PBTurnoverInteractionStd
Value_PBInv_Momentum_20D
neg_Value_Liquidity_Adjustment
roe_stability_score
neg_Market_Cap_Adjusted_Momentum
dynamic_valuation_factor
Price_Deviation_Historical_High
DividendToFreeTurnover_Ratio
Price_ChipNormalized_Position
book_value_price_ratio
cost_pressure_winner_rate
Valuation_Cost_Deviation
```
**信号本质**：PB / 账面价值、ROE 稳定性、价格历史高点偏离、动态估值 —— 价值 / 价格位置反转。

> 三块因子集**零重叠**，信号域完全不同。

---

## 3. 结果

### 3.1 主题块 × 模型矩阵（每块 5 seeds 均值）

| 主题块 | 模型 | CAGR | Sharpe | ICIR | IC | MaxDD | 换手 | 种子 CV |
|--------|------|------|--------|------|-----|-------|------|---------|
| A_Flow | LSTM | 76.3% | 2.09 | 0.524 | 0.065 | -18.1% | 14.4 | 8.0% |
| A_Flow | LGBM | 78.6% | 2.06 | 0.578 | 0.073 | -18.8% | 15.2 | 5.4% |
| B_TurnMom | LSTM | 60.9% | 1.99 | 0.592 | 0.059 | -13.7% | 13.4 | **18.7%** ⚠ |
| B_TurnMom | LGBM | 50.8% | 1.76 | 0.685 | 0.068 | -14.7% | 14.6 | 4.1% |
| **C_FundVal** | **LSTM** | **83.8%** | **2.20** | **0.811** | 0.071 | -18.1% | 13.0 | **2.7%** ★ |
| C_FundVal | LGBM | 74.5% | 2.08 | 0.624 | 0.074 | -15.6% | 15.4 | 9.7% |

> SOTA 参考（历史）：PLUS3×LSTM=96.2%、FM12+×LSTM=86.5%、IF18×LGBM=83.6%。

### 3.2 Per-seed 详情（CAGR% / Sharpe）

```
-- A_Flow --
  LSTM: L1:76%/2.1  L2:84%/2.1  L3:66%/1.9  L4:76%/2.1  L5:79%/2.2
  LGBM: L1:86%/2.2  L2:80%/2.1  L3:78%/2.1  L4:77%/2.0  L5:73%/2.0

-- B_TurnMom --
  LSTM: L6:55%/1.9  L7:72%/2.2  L8:52%/1.8  L9:77%/2.3  L10:49%/1.8
  LGBM: L6:54%/1.9  L7:52%/1.8  L8:48%/1.6  L9:51%/1.8  L10:49%/1.7

-- C_FundVal --
  LSTM: L11:87%/2.3  L12:81%/2.1  L13:83%/2.3  L14:85%/2.1  L15:83%/2.2
  LGBM: L11:81%/2.2  L12:65%/1.9  L13:67%/1.9  L14:76%/2.1  L15:83%/2.2
```

**最佳单 loop**：LSTM `L11 (C_FundVal)` CAGR=87.4% / Sharpe=2.28 / ICIR=0.792；LGBM `L1 (A_Flow)` CAGR=85.9% / Sharpe=2.16 / ICIR=0.576。

### 3.3 种子鲁棒性（CV% 越低越可部署）

| 排名 | 组合 | CV% | 评价 |
|------|------|-----|------|
| 1 | C_FundVal × LSTM | 2.7% | 极稳，L11-L15 全在 81~87% |
| 2 | B_TurnMom × LGBM | 4.1% | 稳，但绝对收益低 |
| 3 | A_Flow × LGBM | 5.4% | 稳 |
| 4 | A_Flow × LSTM | 8.0% | 中 |
| 5 | C_FundVal × LGBM | 9.7% | 中 |
| 6 | B_TurnMom × LSTM | 18.7% | 不稳（L9=77% 拉高均值，彩票风险） |

> LSTM `best_epoch≈1`（第 1 个 epoch 即收敛），val_loss≈1.01。说明信号温和但稳定，选股有效；C_FundVal 的高 ICIR + 极低 CV 表明非过拟合彩票。

---

## 4. 驱动因子分析（Top-6 importance，pytorch_correlation）

### C_FundVal × LSTM (L11, CAGR=87.4%)
| # | 因子 | weight% |
|---|------|---------|
| 1 | Price_Deviation_Historical_High | 15.47% |
| 2 | Price_ChipNormalized_Position | 15.41% |
| 3 | dynamic_valuation_factor | 13.07% |
| 5 | book_value_price_ratio | 11.85% |
| 6 | cost_pressure_winner_rate | 10.92% |
→ 核心：**价格位置 × 估值 × 质量**，典型价值反转信号。

### A_Flow × LSTM (L1, CAGR=76.1%)
| # | 因子 | weight% |
|---|------|---------|
| 2 | SmallOrderIntensityBreakoutFactor | 16.74% |
| 3 | m_turnover_mf_divergence | 15.44% |
| 4 | small_order_flow_intensity | 14.98% |
| 5 | bid_ask_spread_change_factor | 9.83% |
| 6 | neg_Composite_Factor_Multi_Dim | 9.70% |
→ 核心：**小单资金流 + 换手背离**，微观资金面 / 散户行为信号。

### B_TurnMom × LSTM (L6, CAGR=54.8%)
| # | 因子 | weight% |
|---|------|---------|
| 1 | Industry_Volatility_Liquidity_Cross_Factor | 13.70% |
| 2 | m_free_turnover_rate | 13.66% |
| 3 | m_free_turnover_ind_neutral | 12.11% |
| 4 | neg_TurnoverVolatilityEnhancement | 12.02% |
| 5 | m_tech_atr_ratio_14d | 11.61% |
→ 核心：**自由换手率 + 波动率 + 反转**，与低波 alpha 概念高度重叠。

---

## 5. 关键发现

1. **C_FundVal × LSTM 是最佳组合且最可部署**：ICIR=0.811 全场最高，种子 CV=2.7%，5 seeds 全在 81~87%。价值/价格位置反转 + LSTM 时序建模的组合，是种子鲁棒性冠军。

2. **因子×模型强交互再次被证实**（与 R19 结论一致）：
   - FundVal → LSTM 显著优（+9pp CAGR、+0.19 ICIR）
   - TurnMom → LSTM 优（+10pp CAGR）
   - Flow → 两模型接近（LGBM ICIR 略高 +0.054）—— 信号模型无关，最稳健

3. **B_TurnMom 是最弱、最不稳定的信号**：本质是低波/反转，与传统低波 alpha 重叠；LSTM 下 CV=18.7% 不可靠。

4. **三块信号本质完全不同**：Flow（微观资金流）/ TurnMom（换手低波反转）/ FundVal（估值位置反转），因子集零重叠。

---

## 6. 与 R20 多 Alpha 架构的关系

R20 现有计划 5 个 alpha：α1 PLUS3×LSTM(96.2%) / α2 IF18×LGBM(83.6%) / α3 FM12+×LSTM(86.5%) / α4 VOL12(波动率) / α5 MARG10(融资融券)。

| MA1 块 | 与 R20 关系 | 独立性判断 | 建议 |
|--------|-------------|-----------|------|
| **A_Flow** | R20 **无**纯微观资金流 alpha（α2 IF18 是机构资金流，≠微观大/小单） | ★ **真·新独立源** | **优先纳入为 α6**，LGBM 版即可（ICIR 0.58、CV 5.4%） |
| **C_FundVal** | 与 α2 IF18（基本面）概念近，但 IF18 偏财务质量、FundVal 偏估值/价格位置；与 α3 FM12+ 也可能重叠 | 需测相关性 | 候选，若与 IF18 相关 < 0.5 则纳入 |
| **B_TurnMom** | 与 α4 VOL12（低波动）高度重叠 | 独立性差 | **暂缓**，价值最低且种子不稳 |

**MA1 对多 Alpha 的净贡献**：A_Flow 补充了 R20 缺失的微观资金流信号域（建议为 α6）；C_FundVal 待相关性确认。

---

## 7. 结论与下一步

### 结论
MA1 成功验证了「主题块 × 模型族」对称对照设计，确认了因子×模型强交互规律，并**发现 A_Flow 这个 R20 缺失的独立微观资金流信号源**。C_FundVal×LSTM 作为种子鲁棒性冠军，是高价值候选。

### 下一步建议（优先级排序）

1. **【关键】测预测值互相关（Phase0）**：从数仓拉 A_Flow / C_FundVal 的 LSTM 预测值，与 R20 α1/α2/α3 预测值算互相关矩阵。因子集零重叠 ≠ 预测独立（可能选出相关股票）。相关 < 0.5 视为独立。
2. **A_Flow 补种子**：当前 LSTM CV=8.0%，建议补到 8+ 种子锁定配置；LGBM 与 LSTM 双模型族同信号域 = 天然集成候选。
3. **C_FundVal 升级模型**：LSTM 下 ICIR=0.81 且种子极稳，值得试更大 LSTM（hidden=128 / 多层）或 TCN，看 best_epoch=1 是否压制了上限。
4. **B_TurnMom 暂停**：除非相关性测试显示与 VOL12 独立。

### 关联
- R20 多 Alpha 设计：`pending_qe_r20_multi_alpha_20260611`（memory）
- R20 进度快照：`r20_progress_handoff_20260611`（memory，Phase0 正交性已完成 α1/α2/α3）
- QE 方法论：`F:\Dev\AIstock\docs\methodology\qe\`

---

---

# MA2 Round2 实验设计（2026-06-15，进行中）

## 8.1 设计依据（基于 MA1 + R20B 已有数据）

| 信号域 | × LSTM | × LGBM_C | MA2 决策 |
|--------|--------|----------|---------|
| VOL12 波动率 | **从未测试**（R20A GPU 失败缺口） | R20B 已测 4 seed（均 CAGR≈25%） | GPU 全新高价值；CPU 统一 5-seed 框架 |
| MARG10 融资融券 | **从未测试**（R20A GPU 失败缺口） | R20B 已测 4 seed（best 83.5%） | GPU 全新高价值；CPU 统一 5-seed 框架 |

**因子独立性验证（MA2 相对 MA1 A_Flow 块）：**
- **VOL12**（12 因子）：与 A_Flow **零重叠** ✅ 纯新波动率信号域
- **MARG10**（10 因子）：与 A_Flow 重叠 **6** 个，但核心 `m_md_rz_rq_sentiment`（ICIR=0.69）+ `sentiment_order_imbalance` + `m_gap_frequency_20d` + `dynamic_valuation_factor` 共 4 个独立 → Phase0 量化重叠

**设计逻辑**：MA2 用 MA1 验证的正确配置（h20/V25/topk25/nd2）测两个新信号域。GPU(LSTM) 全新数据填补 R20A 失败缺口；CPU(LGBM_C) 虽与 R20B 4-seed 部分重叠，但统一到 5-seed 框架使 **5-domain × 2-model 全景矩阵完整可比**（Flow/TurnMom/FundVal from MA1 + Vol/Marg from MA2）。

## 8.2 实验规格

| Task | 节点 | 并行度 | 模型 | 内容 | Loops |
|------|------|--------|------|------|-------|
| **MA2-GPU** | `wsl2-5080` | parallel_2 | `__seed_LSTM_10D_hs64_d02__`（cuda） | L1-5 VOL12×5seed + L6-10 MARG10×5seed | 10 |
| **MA2-CPU** | `rdagent-node1` | parallel_4 | `__seed_LGBModel_conservative_v1__`（cpu） | L1-5 VOL12×5seed + L6-10 MARG10×5seed | 10 |

- Seeds：`{42, 888, 2024, 2026, 12345}`（每个块 5 seed）
- 配置：topk=25 / n_drop=2 / label_horizon=20 / V25_1_SMALL_CAP / no-HMM / 10M（同 MA1）
- Groups：`a4_vol_{lstm,lgbmc}` + `a5_marg_{lstm,lgbmc}`
- 回测：2024-07-01 ~ 2026-04-27，stock_pool=`filtered_pool_20260428`

## 8.3 因子集（取自 R20B 权威配置，已验证可运行）

**VOL12（波动率域，12 因子）：**
`m_atr_compression, m_vol_of_vol_20d, m_idio_vol_60d, neg_momentum_volatility_ratio, liquidity_adjusted_volatility, neg_volatility_breakout_momentum_v2, neg_volatility_10D, m_intraday_range_compress, m_tech_atr_ratio_14d, m_ind_residual_vol_ratio, neg_turnover_adjusted_volatility, conditional_momentum_volatility`

**MARG10（融资融券情绪域，10 因子）：**
`m_md_rz_rq_sentiment, dynamic_flow_volatility_sentiment, ChipWinnerRateEliteBuyIntensity, sentiment_order_imbalance, m_turnover_mf_divergence, m_gap_frequency_20d, neg_Composite_Factor_Multi_Dim, dynamic_valuation_factor, bid_ask_spread_change_factor, small_order_flow_intensity`

## 8.4 执行进度

- [x] 设计完成（基于 MA1 数据 + R20B 因子集）
- [x] loops 生成 + 校验（factor_count、device、model_id、seeds、group 全部核对）
- [x] 因子重叠验证（VOL12 零重叠 / MARG10 6 重叠）
- [x] loops 工件落盘：`docs/analysis/ma2_loops/ma2_gpu_lstm_loops.json` + `ma2_cpu_lgbmc_loops.json`
- [ ] **创建 MA2-GPU task**（`qe_custom_evo_create_pending`）
- [ ] **创建 MA2-CPU task**（`qe_custom_evo_create_pending`）
- [ ] **启动两个 task**（`qe_custom_evo_run_confirmed`）
- [ ] 记录 task_id 到本节

## 8.5 ⚡ 重启 Claude Code 后的恢复步骤

> 进度已全部持久化。重启后 MEMORY.md 会自动提示「MA2 待创建启动」，按以下步骤继续即可，**无需重新设计/生成 loops**。

**第一步：创建 MA2-GPU task** —— 调用 `qe_custom_evo_create_pending`：
```
task_name      = "MA2-GPU MultiAlpha LSTM 2blocks(Vol/Marg) h20 5seed"
target_desc    = "MA2 GPU wsl2-5080 parallel_2. 10 loops: 2 decorrelated-theme blocks (D=VOL12 volatility / E=MARG10 margin_sentiment) x LSTM_10D_hs64_d02 h20 5seed(42/888/2024/2026/12345). Multi-alpha sourcing round2, fills R20A GPU LSTM gap (VOL12/MARG10 never tested on LSTM). topk25/nd2/h20/V25_1_SMALL_CAP/no-HMM/10M/cuda."
node_id        = "wsl2-5080"
node_parallelism = {"wsl2-5080": 2}
engine_mode    = "unified"
loops          = 读 docs/analysis/ma2_loops/ma2_gpu_lstm_loops.json（10 个完整 loop dict，已含 loop_index）
```

**第二步：创建 MA2-CPU task** —— 同上，参数替换：
```
task_name      = "MA2-CPU MultiAlpha LGBM_C 2blocks(Vol/Marg) h20 5seed"
target_desc    = "MA2 CPU rdagent-node1 parallel_4. 10 loops: 2 blocks (D=VOL12 / E=MARG10) x LGBM_C h20 5seed(42/888/2024/2026/12345). Multi-alpha sourcing round2, unified 5seed framework (R20B tested 4seed LGBM_C; MA2 adds seed 888 + aligns to MA1). topk25/nd2/h20/V25_1_SMALL_CAP/no-HMM/10M/cpu."
node_id        = "rdagent-node1"
node_parallelism = {"rdagent-node1": 4}
engine_mode    = "unified"
loops          = 读 docs/analysis/ma2_loops/ma2_cpu_lgbmc_loops.json
```

**第三步：启动** —— 对每个 task 调用 `qe_custom_evo_run_confirmed(task_id=<返回值>)`。

**第四步**：把返回的 task_id 填回本节 8.4，commit。

> 注：loops JSON 每个含 loop_index + 完整 strategy_params + execution_algo_params（cuda/cpu）+ runtime_flags，可直接作为 `loops` 参数传入，无需再加工。MCP 后端为内网（端口 8001），不走外网代理。

---

*实验数据来源：QE 数仓（qe_archive）· MCP aistock-qe。本文档由 worktree `docs/ma1-multi-alpha-sourcing-20260615` 编写。MA2 设计与进度同录于此文档（§8）。*

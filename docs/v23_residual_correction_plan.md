# v23 执行策略：规则 + SL 修正 + RL 微调

## 概述

v23 在 v20 HybridExecutor 基础上，添加两项改进：
1. **R6/R7 硬规则**：冲涨停追买 / 砸跌停追卖，以涨跌停价挂单
2. **CorrectionNet 修正层**：31 维特征（以 price_vs_prev_close 为核心），
   学习"偏离昨收多少时调整执行量"的阈值和分档

## 架构

```
订单 → Layer 0: 不可交易过滤 (一字板/停牌)
     → Layer 1: 硬规则 R1-R7 (确定性场景, <1ms)
        R1: 买入+跌停 → 全量买入 (最佳价)
        R2: 卖出+涨停 → 全量卖出 (最佳价)
        R3: 买入+涨停 → 不执行 (封板)
        R4: 卖出+跌停 → 不执行 (封板)
        R6: 买入+冲涨停 (距离+放量+动量) → 以涨停价全量追买 [NEW]
        R7: 卖出+砸跌停 (距离+放量+动量) → 以跌停价全量追卖 [NEW]
        R5a/b: 接近涨跌停温和加速
        R5: 最后30分钟强制完成
     → Layer 2: v19 计划 × exp(CorrectionNet) (模型学习, ~2ms)
     → Layer 3: TWAP 兜底
```

## R6/R7 设计

### 核心原则
以涨停价/跌停价挂单：`urgency_bps = dist_to_limit × 10000`。
模拟器成交价 ≈ 涨跌停价，防止延迟导致交易失败。

### 阈值 (分板块)

| 参数 | 主板 (10%) | 创业板/科创板 (20%) |
|------|-----------|-------------------|
| CHASE_DIST | 3.0% | 5.0% |
| VOL_SURGE_MIN | 3.0x | 3.0x |
| RET_5M_MIN | 0.8% | 1.5% |
| RET_1M_MIN | 0.3% | 0.5% |

三重条件同时满足才触发。

## CorrectionNet 设计

### 31 维特征

| Group | Dims | 说明 |
|-------|------|------|
| A: 价格偏离 | 0-6 | **price_vs_prev_close** (原始/signed/abs), vs_open, vs_vwap, vs_high, vs_low |
| B: 价格位置 | 7-9 | price_rank, dist_to_limit_up, dist_to_limit_down |
| C: 动量+波动 | 10-15 | ret_1m/5m/10m, momentum_sign, volatility_5m/20m |
| D: 量能 | 16-18 | vol_surge, vol_ratio_day, rsi_14 |
| E: 执行进度 | 19-25 | exec_progress, remaining, time, urgency, plan_frac/cum |
| F: 上下文 | 26-30 | is_buy, is_warmup, limit_pct, is_near_limit, hour_sin |

### 网络结构

```
31 → Linear(128) → LayerNorm → GELU → Dropout(0.1)
   → Linear(64)  → LayerNorm → GELU → Dropout(0.1)
   → Linear(32)  → LayerNorm → GELU → Dropout(0.1)
   → Linear(1)   → clamp(-3, 3)    [zero-init]
```

14,913 参数。初始 exp(0) = 1.0 = 不修正。

### 训练标签

```python
label = clip(log(dp_frac / plan_frac), -3, 3)
```
- dp_frac: DP 最优执行比例 (事后全知)
- plan_frac: v19 计划比例

DP 标签天然编码了"买入时价格越低执行越多"的信息。

### SL 训练配置

| 参数 | 值 |
|------|-----|
| Loss | MSE + 0.01 × pred² |
| Optimizer | Adam lr=1e-3 wd=1e-5 |
| Scheduler | CosineAnnealing T=30 |
| Batch | 2048 |
| Epochs | 30, patience=5 |
| Data | ~200k orders → ~20M samples |

### RL 微调 (可选)

PPO, Actor LR=3e-5, Critic LR=1e-4, entropy=0.005。
SL checkpoint 做 rollback 基线。

## 文件清单

### 修改
| 文件 | 修改内容 |
|------|---------|
| `rl_execution/executor/v20_hybrid_executor.py` | R6/R7 + 31维特征计算 + 分板块阈值 |
| `rl_execution/simulator/limit_aware_simulator.py` | urgency>100 bps 时 fill_prob≥0.95 |

### 新建/覆盖
| 文件 | 说明 |
|------|------|
| `rl_execution/network/correction_net.py` | 31维 CorrectionNet (覆盖旧版25维) |
| `scripts/rl_execution/v23_gen_correction_data.py` | 数据生成 (单进程) |
| `scripts/rl_execution/v23_train_correction.py` | SL 训练 |
| `scripts/rl_execution/v23_evaluate.py` | 评估 (6种消融 + 涨幅分档 + 涨停专项) |

## 版本演进

| 版本 | 方法 | PA (bps) | 状态 |
|------|------|----------|------|
| v19 | 纯 SL 执行计划 | +8.04 | 生产基线 |
| v20 | 硬规则+v19+TWAP | ~+8 | 已上线 |
| v22 | Decision Transformer | +1.75 | 失败 |
| **v23** | **v20+R6/R7+CorrectionNet** | **TBD** | **训练中** |

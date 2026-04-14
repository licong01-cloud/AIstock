# RL 执行策略 v16 设计文档

## 版本演进总结

| 版本 | Best Oracle Gap | Best PA | 核心改动 | 主要问题 |
|------|----------------|---------|---------|---------|
| v13 | 201.31 bps | -1.81 bps | GRU + rolling oracle reward | v_loss爆炸, GRU h=None |
| v14 | 189.08 bps | -0.25 bps | Sequence BPTT + TWAP reward + 24动作 | 探索效率低, 前置执行 |
| v15 | 188.84 bps | +6.07 bps | SL预训练 + RL微调 | steps膨胀(346k), entropy过低 |

## v16 设计目标

1. 修复 v15 的 steps 膨胀（限制最大执行步数 + 强化 pacing）
2. 保留 SL 预训练的 PA 优势（+6 bps）
3. 加入日内价格预测模块，突破 189 bps 的 oracle_gap plateau
4. 加入前一日波动率/流动性特征，提供跨日信息

## 架构设计

### 整体架构: 双模块 — 价格预测器 + 执行策略

```
┌─────────────────────────────────────────────────┐
│  Price Predictor (独立 LSTM, 冻结权重)           │
│  输入: 过去30分钟 OHLCV                          │
│  输出: 3维 [未来10m方向概率, 价格分位数, 波动率预测] │
└──────────────────────┬──────────────────────────┘
                       │ 3维预测特征
                       ▼
┌─────────────────────────────────────────────────┐
│  Execution Policy (GRU + PPO)                    │
│  输入: 38维 state (35维原有 + 3维预测)            │
│  输出: 24维动作概率                               │
│  初始化: SL预训练权重 (v15 sl_pretrain_v15.pt)    │
└─────────────────────────────────────────────────┘
```

### 模块 1: 日内价格预测器 (Price Predictor)

独立训练的监督学习模型，推理时冻结权重，输出作为 RL state 的额外特征。

输入: 过去 30 分钟的分钟线特征 (30 × 5维: close_ret, vol_ratio, high_low_range, vwap_dev, rsi)
输出 3 维:
- dim 0: 未来 10 分钟价格方向概率 P(上涨) ∈ [0,1]
- dim 1: 当前价格在全天价格分布中的估计分位数 ∈ [0,1]
  (0=接近当日最低, 1=接近当日最高)
- dim 2: 未来 30 分钟预期波动率 / 历史均值

架构: 2层 LSTM, hidden=64, 轻量级 (~50K 参数)
训练数据: 历史分钟线, 标签用事后真实值
推理延迟: <0.5ms/步 (CPU)

### 模块 2: 前一日跨日特征 (2 维)

在 gen_market_features.py 中预计算，加入 market_features.pkl:
- dim 0: 前一日日内已实现波动率 (预测今日波动率水平)
- dim 1: 前一日尾盘30分钟成交量占比 (预测今日流动性)

### State 空间扩展: 35 → 40 维

```
原有 35 维 (v14/v15):
  [0-1]   时间/进度
  [2-5]   价格动态
  [6-8]   涨跌停距离
  [9-11]  成交量
  [12]    方向
  [13-16] 技术指标
  [17-20] 预测辅助
  [21-23] v14微观结构 (日内成交量分布/均值回归z-score/波动率变化率)
  [24-28] 板块特征
  [29-34] 大盘特征

v16 新增 5 维:
  [35-37] 价格预测器输出 (方向概率/价格分位数/波动率预测)
  [38-39] 前一日跨日特征 (波动率/尾盘量占比)
```

## 训练流程

### Phase 1: 训练价格预测器 (独立 SL)

```
数据: 历史分钟线 (2024-01 ~ 2025-06)
标签:
  - 方向: 未来10分钟收益率 > 0 → 1, 否则 → 0
  - 分位数: 当前close在全天[min,max]中的位置
  - 波动率: 未来30分钟已实现波动率 / 过去30分钟波动率
训练: BCE + MSE loss, 10 epochs
产出: price_predictor_v16.pt
```

### Phase 2: 生成扩展 market_features (预计算)

```
对每个交易日, 计算:
  - 前一日日内已实现波动率
  - 前一日尾盘30分钟成交量占比
追加到 market_features.pkl
```

### Phase 3: SL 预训练 (用 40 维 state)

```
复用 P1 流程, 但 state 扩展到 40 维
标签: 改进版 oracle 标签 (考虑剩余仓位和时间)
产出: sl_pretrain_v16.pt
```

### Phase 4: RL 微调

```
初始化: sl_pretrain_v16.pt (network + actor_head)
价格预测器: 冻结权重, 每步推理输出 3 维特征
关键超参调整 (vs v15):
  - entropy_coef: 0.005 → 0.015 (增加探索)
  - pacing_penalty: 0.05 → 0.15 (惩罚过度分散)
  - 新增: max_episode_steps=60 (超过60步强制全量执行)
  - lr: 3e-5 (保持, 防止破坏SL权重)
```

## v15 问题修复

### Steps 膨胀修复

三管齐下:
1. action_interpreter 中加入 max_episode_steps=60 硬约束:
   超过 60 步后强制 exec_frac=1.0 (全量执行)
2. pacing_penalty 从 0.05 提到 0.15:
   落后进度时给更大惩罚
3. SL 标签改进: 加入时间衰减因子,
   越接近 episode 结束, 最优动作越倾向大比例执行

### Entropy 过低修复

- entropy_coef: 0.005 → 0.015
- entropy_anneal_start: 20 → 30 (更晚开始退火)
- entropy_anneal_end: 0.001 → 0.005 (最低值更高)

## 预期效果

| 改动 | 预期 Oracle Gap 改善 | 预期 PA 改善 |
|------|---------------------|-------------|
| 修复 steps 膨胀 | 0-2 bps | 保持 +6 bps |
| 价格预测器特征 | 5-10 bps | +2-5 bps |
| 前一日跨日特征 | 1-3 bps | +1 bps |
| 改进 SL 标签 | 2-5 bps | +1-2 bps |
| **合计** | **8-20 bps** | **+10-14 bps** |
| **v16 目标** | **170-180 bps** | **>+10 bps** |

## 实施步骤

```
Step 1: 训练价格预测器 (~2小时)
  → price_predictor_v16.pt

Step 2: 扩展 market_features (~30分钟)
  → market_features_v16.pkl

Step 3: 修改 state_interpreter (STATE_DIM=40)
  → state_interpreter.py 更新

Step 4: 并行预计算 40 维 state 缓存 (~5分钟)
  → sl_states_cache_v16.pkl

Step 5: SL 预训练 (~10分钟)
  → sl_pretrain_v16.pt

Step 6: RL 微调训练 (~2-3小时)
  → checkpoints_v16/policy_best.pt
```

## 文件清单

```
新增:
  scripts/rl_execution/train_price_predictor.py   # Phase 1
  scripts/rl_execution/gen_market_features_v16.py  # Phase 2
  scripts/rl_execution/train_v16.py                # Phase 4
  rl_execution/config/train_ppo_v16.yaml           # 配置
  rl_execution/network/price_predictor.py          # 预测器网络
  run_train_v16.sh                                 # 启动脚本

修改:
  rl_execution/interpreter/state_interpreter.py    # STATE_DIM 35→40
  rl_execution/interpreter/action_interpreter_v14.py # max_episode_steps
  rl_execution/reward/oracle_price_reward_v14.py   # pacing_penalty 增强
```

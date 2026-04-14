# RL 执行策略 v17 设计文档

## v13-v16 复盘总结

| 版本 | Best Oracle Gap | Best PA | 核心问题 |
|------|----------------|---------|---------|
| v13 | 201.31 bps | -1.81 bps | v_loss爆炸, GRU h=None |
| v14 | 189.08 bps | -0.25 bps | 探索慢, 75 epochs才到best |
| v15 | 188.84 bps | +6.07 bps | steps膨胀(346k), SL权重完整但entropy过低 |
| v16 | 188.51 bps | -0.56 bps | SL权重只匹配6个参数, entropy过高 |

### 核心发现

1. Oracle gap 在 188-189 bps 是当前架构的硬天花板
2. SL 预训练的价值在 PA（+6 bps），不在 oracle_gap
3. Steps 膨胀是 SL 预训练 + RL 微调的固有矛盾
4. v15 是 PA 最好的版本，v16 是 oracle_gap 最好的版本
5. 两者的优势没有叠加，因为 v16 的 SL 权重不完整

## v17 设计目标

**合并 v15 和 v16 的优势**：
- v15 的完整 SL 权重加载 → PA 优势
- v16 的价格预测器 + 涨跌停修复 → oracle_gap 改善
- 解决 steps 膨胀问题

## 核心改动

### 1. 完整 40 维 SL 预训练

v16 失败的直接原因：SL 预训练用 35 维 state，RL 用 40 维，导致 input 层和 output 层
权重不匹配。

v17 方案：在 SL 预训练阶段就包含价格预测器的 3 维输出和跨日 2 维特征。
预计算 state 缓存时加载价格预测器，生成完整 40 维 state。
这样 SL 预训练的网络维度和 RL 完全一致，权重 100% 匹配。

### 2. 改进 SL 标签 — 加入 max_episode_steps 约束

v15 的 steps 膨胀根因：SL 标签在大部分"普通价格"分钟标记为低执行比例（5%），
RL 微调时 agent 保持了这个保守习惯。

v17 方案：SL 标签生成时模拟 max_episode_steps=60 的约束。
- 前 45 步（75%）：正常的价格质量 + 时间衰减标签
- 第 46-59 步：强制提高执行比例（至少 35%）
- 第 60 步：强制全量执行

### 3. Entropy 精调

v15: 0.005 → 过低，不探索
v16: 0.015 → 过高，不收敛

v17: entropy_coef=0.008，anneal_start=20，anneal_end=0.003
这个范围在 v15 和 v16 之间，应该能平衡探索和收敛。

### 4. 保留 v16 的所有修复

- 涨跌停硬规则（跌停全量买入，涨停全量卖出）
- Oracle 基准排除不可交易分钟
- Reward 涨跌停感知（pacing/TWAP/completion）
- 价格预测器（dir_acc=61.2%）
- max_episode_steps=60

## 实施步骤

```
Step 1: 预计算 40 维 state 缓存（含价格预测器输出）
  - 修改 p1_precompute_parallel.py 加载价格预测器
  - 输出: sl_states_cache_v17.pkl (STATE_DIM=40)

Step 2: SL 预训练（40 维 → 30 output → 24 actions）
  - 用 v16 标签 + 40 维 state
  - 输出: sl_pretrain_v17.pt

Step 3: v17 RL 训练
  - 100% 权重加载（无部分匹配）
  - entropy=0.008
  - 输出: checkpoints_v17/policy_best.pt
```

## 预期效果

| 指标 | v15 best | v16 best | v17 预期 |
|------|----------|----------|---------|
| Oracle Gap | 188.84 | 188.51 | 185-188 |
| PA | +6.07 | -0.56 | +5-8 |
| Steps 稳定性 | 膨胀 | 膨胀 | 稳定(max 60步) |

## 超参配置

```yaml
policy:
  lr: 3.0e-5
  critic_lr: 1.0e-4
  entropy_coef: 0.008
  entropy_anneal_start: 20
  entropy_anneal_end: 0.003
  vf_coef: 0.5
  vf_clip: 10.0

action:
  max_episode_steps: 60

reward:
  pacing_penalty: 0.15
  terminal_oracle_weight: 5.0
  step_reward_weight: 0.5

trainer:
  repeat_per_collect: 2
  batch_size: 64
```

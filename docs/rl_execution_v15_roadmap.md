# RL 执行策略优化路线图（v15+）

## 背景

v14 训练结果：oracle_gap=189.08 bps, PA=-0.25 bps。相比 v13（201.31 bps）改善 12 bps，但 epoch 30 后进入 plateau。

### v14 Plateau 根因

1. **探索效率低**：从随机策略起步，前 20 epoch 在瞎摸，avg_reward 始终 -0.10~-0.11 未改善
2. **过早收敛到"前置执行"**：agent 学会前几步执行完大部分仓位（steps 从 65k 降到 14k），放弃了择时
3. **collect-update 不一致**：collect 用 batch 推理，update 用逐步前向，GRU hidden state 传播路径不同
4. **240 步 episode 太长**：GAE credit assignment 困难，终局 oracle reward 信号稀疏
5. **离散动作空间粒度限制**：6 级执行比例跳跃仍然较大，无法精确控制

---

## 优化方向（按优先级排序）

### P0：Oracle 模式分析（零成本，指导所有后续改进）

**目标**：理解"事后最优执行轨迹长什么样"，为后续所有改进提供数据支撑。

**方法**：
- 对历史每个订单，回看当天完整价格序列，计算事后最优执行轨迹
- 统计最优执行的共性模式：
  - 最优执行通常集中在一天的什么时段？（开盘/午盘/尾盘）
  - 最优执行时刻的价格/成交量/波动率有什么共同特征？
  - 不同板块、市值、买卖方向的最优模式差异
  - 最优策略 vs TWAP 的差异分布
- 按 oracle_gap 难度对订单分桶，识别"困难场景"（单边行情、低流动性等）

**产出**：
- 最优执行模式统计报告
- 困难场景分类和占比
- SL 预训练的标签数据

**预期周期**：1-2 天

---

### P1：SL 预训练 + RL 微调（最高杠杆）

**目标**：用监督学习给 RL 一个好的起点，解决探索效率低的问题。

**方法**：

第一步：构造监督标签
- 对每个历史订单，计算事后最优执行轨迹（基于 P0 的分析结果）
- 每分钟的最优动作 = argmin(exec_price - oracle_price) 对应的 (exec_frac, urgency) 组合
- 标签格式：(state_35dim, optimal_action_24class)

第二步：SL 预训练
- 用同样的 GRU 网络架构（35→256→27→24）
- CrossEntropy loss 训练分类任务
- 训练数据：全量历史订单的每一步（约 140 万订单 × 平均 10 步 = 1400 万样本）
- 预期 5-10 个 epoch 收敛

第三步：RL 微调
- 用 SL 预训练的网络权重初始化 PPO 的 network + actor_head
- critic_head 随机初始化（SL 没有 value 信号）
- 降低 lr（1e-4 → 3e-5），减小 entropy_coef（0.02 → 0.005），避免破坏 SL 学到的知识
- 预期从 ~180 bps 起步，微调到 175-180 bps

**预期改善**：oracle_gap 从 189 → 175-180 bps（~10 bps 改善）

**预期周期**：3-5 天

---

### P2：修复 Sequence BPTT 的 collect-update 不一致

**目标**：消除 collect 和 update 阶段的 GRU hidden state 计算差异。

**方法**：
- update 阶段改用 batch 推理：把同一 batch 内的 episodes 按最长长度 pad
- 用 `torch.nn.utils.rnn.pack_padded_sequence` 做 GRU 前向
- 这样 collect 和 update 的计算路径完全一致
- 同时把 `repeat_per_collect` 从 2 降到 1

**预期改善**：减少 p_loss 的偏差，预期 2-5 bps 改善

**预期周期**：1 天

---

### P3：SL 价格预测作为额外状态特征

**目标**：给 agent 更强的"未来信息"，替代当前简单的线性回归斜率。

**方法**：

训练独立的价格预测模型：
- 输入：过去 30 分钟的分钟线特征（OHLCV + 技术指标）
- 输出：
  - 未来 10 分钟价格方向概率（上/下/平，3 分类）
  - 未来 30 分钟预期最低价分位数（回归）
  - 当前价格在全天价格分布中的百分位估计（回归）
- 架构：轻量 LSTM 或 1D-CNN，独立训练
- 推理时冻结权重，输出作为 RL 状态空间的额外 3 维特征（STATE_DIM: 35 → 38）

**预期改善**：给 agent 择时能力，预期 3-8 bps 改善

**预期周期**：3-5 天

---

### P4：Curriculum Learning 分阶段训练

**目标**：防止 agent 过早收敛到"前置执行"策略。

**方法**：

阶段 1（epoch 0-30）：纯 terminal oracle reward
- 去掉所有 step reward 和 pacing penalty
- 只在 episode 结束时给 oracle reward
- 让 agent 先学会"什么时候执行最优"的全局视角

阶段 2（epoch 30-60）：引入 TWAP step reward
- 在全局最优的基础上学习平滑执行
- step_reward_weight 从 0 线性增加到 0.5

阶段 3（epoch 60-100）：精细化
- 降低 entropy，增加 exploitation
- 可选：按难度分层采样，困难订单权重加大

**预期改善**：避免局部最优，预期 3-5 bps 改善

**预期周期**：2-3 天

---

### P5：Hierarchical RL（多时间尺度决策）

**目标**：从根本上解决 240 步 episode 的 credit assignment 问题。

**方法**：

两层架构：
- 高层策略（Meta Policy）：每 30 分钟决定一次"这个时段执行多少比例"
  - 8 个决策点（240 分钟 / 30 分钟）
  - 动作空间：该时段的目标执行比例 [0%, 10%, 20%, ..., 100%]
  - 奖励：该时段的执行质量 vs oracle
- 低层策略（Execution Policy）：在每个 30 分钟窗口内逐分钟执行
  - 30 步的短 episode
  - 动作空间：同 v14 的 24 动作
  - 奖励：完成高层分配的目标 + 执行质量

高层学全局择时（"什么时候买便宜"），低层学局部执行（"怎么买到便宜"）。

**预期改善**：大幅改善 credit assignment，预期 10-15 bps 改善

**预期周期**：5-7 天

---

### P6：连续动作空间 + SAC

**目标**：突破离散动作的粒度限制。

**方法**：
- 执行比例：Beta 分布参数化，输出 [0, 1] 连续值
- 追价力度：Gaussian 分布，输出 [0, 30] bps 连续值
- 用 SAC（Soft Actor-Critic）替代 PPO：
  - Off-policy，支持 experience replay
  - 最大熵框架自动平衡探索/利用
  - 对连续动作空间支持更好

**预期改善**：精确控制执行比例，预期 5-10 bps 改善

**预期周期**：5-7 天

---

### P7：多场景专家策略 + Gating Network

**目标**：针对不同市场状态使用不同策略。

**方法**：
- 基于 P0 的分析，将交易日分为 3-4 类：趋势上涨日、趋势下跌日、震荡日、极端波动日
- 为每类场景训练专门的执行策略
- 训练一个 Gating Network，根据开盘后 30 分钟的特征判断当天属于哪类场景
- 运行时由 Gating Network 选择对应的专家策略

**预期改善**：减少困难场景的拖累，预期 5-8 bps 改善

**预期周期**：7-10 天

---

## 实施路线

```
Phase 1（1-2天）: P0 Oracle 模式分析
    ↓
Phase 2（3-5天）: P1 SL预训练 + RL微调 + P2 修复BPTT不一致
    ↓
Phase 3（3-5天）: P3 价格预测特征 + P4 Curriculum Learning
    ↓
Phase 4（5-7天）: P5 Hierarchical RL 或 P6 SAC（二选一）
    ↓
Phase 5（7-10天）: P7 多场景专家（可选）
```

### 预期目标

| 阶段 | 预期 Oracle Gap | 累计改善 |
|------|----------------|---------|
| v14 baseline | 189 bps | — |
| Phase 2 完成 | 175-180 bps | ~10 bps |
| Phase 3 完成 | 168-175 bps | ~15-20 bps |
| Phase 4 完成 | 155-168 bps | ~20-35 bps |

---

## 版本规划

| 版本 | 包含改进 | 目标 Oracle Gap |
|------|---------|----------------|
| v15 | P0 + P1 + P2 | 175-180 bps |
| v16 | P3 + P4 | 168-175 bps |
| v17 | P5 或 P6 | 155-168 bps |
| v18 | P7 | <155 bps |

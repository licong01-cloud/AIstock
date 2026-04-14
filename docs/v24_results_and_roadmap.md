# v24 非对称日内执行策略 — 完整结果与后续路线

> 日期: 2026-04-06
> 设计文档: `F:\Dev\AIstock\docs\v24_asymmetric_execution_design.md`

---

## 一、全量 Oracle 统计 (基础数据)

数据源: 261.6 万 stock-days (5033股 × 533天, 2024-01-02 ~ 2026-03-19)
数据文件:
- `/home/lc999/data/rl_orders/oracle_full_dist.pkl` — 全量 Oracle 统计
- `/home/lc999/data/rl_orders/oracle_full_dist_with_gap.pkl` — + prev_close + gap_pct

### 1.1 买卖 Oracle 时间分布

| 时段 | 买入 Oracle (最低价) | 卖出 Oracle (最高价) |
|------|---------------------|---------------------|
| 开盘 30m | 50.0% | 52.3% |
| 上午 30m-2h | 18.3% | 22.3% |
| 下午 2h-3.5h | 17.4% | 17.7% |
| 尾盘 30m | **14.3%** | **7.6%** |

Oracle 分钟统计:
- 买入: mean=76.0, median=30, P90=223
- 卖出: mean=63.2, median=25, P90=194

### 1.2 开盘价 vs 尾盘价 vs VWAP

| 操作 | 开盘价 vs VWAP | 尾盘价 vs VWAP |
|------|---------------|---------------|
| 买入 | -14.8 bps (贵) | -4.3 bps (贵) |
| 卖出 | +14.8 bps (好) | +4.3 bps (好) |

结论: 卖出在开盘执行系统性优 +14.8 bps, 买入在尾盘略优。

### 1.3 10% 板 vs 20% 板 — 绝对缺口下规律完全反转

同样 "低开 7%":
- **10% 板** (走了 70% 跌停距离): 反弹, 开盘买 **省 123 bps**, Oracle 前30m = 89.3%
- **20% 板** (走了 35% 跌停距离): 继续跌, 开盘买 **亏 91 bps**, Oracle 前30m = 39.0%

同样 "高开 8%":
- **10% 板**: 冲板或回落, 开盘买 **亏 122 bps**
- **20% 板**: 继续涨, 开盘买 **省 91 bps**

→ 必须用**归一化缺口** `gap_ratio = gap_pct / limit_pct`

### 1.4 归一化缺口下规律统一

| 归一化缺口 (占限幅%) | 10%板 买入@开盘 | 20%板 买入@开盘 | 统一含义 |
|---------------------|----------------|----------------|---------|
| 低开 > 70% 限 | -123 bps | -214 bps | 都是反转, 加量买 |
| 低开 50~70% | -1 bps | +2 bps | 中性, 不加量 |
| 低开 30~50% | +1 bps | +140 bps | **陷阱区间** |
| 高开 > 70% 限 | +186 bps | +323 bps | 都不该买 |

### 1.5 PA 理论上界

基于 178 万 train stock-days 模拟:

| 策略 | PA (bps) |
|------|---------|
| TWAP | 0.0 |
| 方向不对称 (卖80%早盘, 买80%TWAP) | +7.1 |
| 方向不对称 + 缺口条件 | +8.0 |
| v19/v20 (实测) | +8.0 |
| 3选1完美择时 | +114.3 |
| Oracle | +202.4 |

### 1.6 数据质量

| 异常类型 | 数量 | 占比 |
|---------|------|------|
| 日振幅=0 (疑似停牌) | 3,413 | 0.19% |
| 前后30m均零成交 | 3 | 0.00% |
| 缺口 > 20% | 278 | 0.02% |

---

## 二、v24 训练结果

### 2.1 数据生成

| 步骤 | 脚本 | 耗时 | 产出 |
|------|------|------|------|
| Plan data | v24_gen_plan_data.py | 4 min | 521万条, 7.8 GB |
| DP labels (numpy 向量化) | v24_gen_dp_labels.py | 16 min | 521万条, 4.8 GB |
| Correction data | v24_gen_correction_data.py | 52 min | 2001万条, 1.58 GB |

DP 向量化加速: 原始纯 Python loop 5 小时 → numpy 向量化 16 分钟 (**19x 加速**)

### 2.2 Layer 2 Plan Net 训练

| 指标 | v19 | v24 | 改善 |
|------|-----|-----|------|
| val_kl | 0.5829 | **0.5463** | **-6.3%** |
| top10_overlap | 18.0% | **24.1%** | **+33%** |
| 买入 val_kl | — | 0.5274 | — |
| 卖出 val_kl | — | 0.5651 | — |

模型: ExecutionPlanNetV24, 168K 参数, 19 epochs (early stop), 44 min GPU
买入 kl (0.5274) < 卖出 kl (0.5651) — 模型学到了方向不对称

### 2.3 Layer 3 Correction Net 训练

| 指标 | v23 | v24 |
|------|-----|-----|
| val_loss | 0.0851 | **0.001139** |
| 标签分布 | 85.5% clamp -3 | mean=-0.003, std=0.046 |

模型: CorrectionNetV24, 4.6K 参数, 30 epochs, 77 min GPU
标签质量大幅改善 (DP max_steps=240 + 加性修正解决了 v23 的标签退化)

### 2.4 模型文件

```
/home/lc999/data/rl_models/v24/
├── v24_plan_net.pt          # Layer 2 (168K params)
├── v24_correction_net.pt    # Layer 3 (4.6K params)
├── plan_data.pkl            # Layer 2 训练数据 (7.8 GB)
├── dp_labels_210.pkl        # DP 标签 (4.8 GB)
└── correction_data.npz      # Layer 3 训练数据 (1.58 GB)
```

---

## 三、评估结果 (LimitAwareSimulator, 9980 orders)

### 3.1 总览

| 模式 | PA (bps) | vs v20 增量 | Oracle Gap | 说明 |
|------|---------|-------------|-----------|------|
| A0 TWAP | +2.00 | -3.11 | — | 基准 |
| A1 v19 plan | +6.02 | +0.91 | 178.31 | v19 不分方向 |
| **A2 v20** | **+5.11** | **0** | **179.06** | **当前生产基线** |
| **B1 v24 plan** | **+6.35** | **+1.25** | **177.97** | **最优配置** |
| B2 v24 + warmup | +5.40 | +0.29 | 178.76 | warmup 查表反而拖累 |
| B3 v24 full | +4.99 | -0.11 | 178.99 | correction 恶化卖出 |

### 3.2 买卖分组

| 模式 | 买入 PA | 卖出 PA |
|------|--------|--------|
| A2 v20 | +11.61 | -1.60 |
| **B1 v24 plan** | **+15.14 (+30%)** | -2.70 |
| B3 v24 full | +16.10 (+39%) | -6.46 (恶化) |

### 3.3 缺口分桶 (B1 v24 plan)

| 缺口 | n | PA | 买入 PA | 卖出 PA |
|------|---|-----|--------|--------|
| 极端低开 | 61 | +23.70 | +24.48 | +22.95 |
| 低开 | 1216 | +12.25 | +16.26 | +7.92 |
| 平开 | 7707 | +2.70 | +10.55 | -5.38 |
| 高开 | 892 | +20.23 | +38.92 | +0.77 |
| 极端高开 | 104 | +79.34 | +157.80 | +24.02 |

### 3.4 关键结论

1. **B1 (v24 plan only) 是最优配置**: PA=+6.35 bps, vs v20 +24%
2. **核心价值来自 Plan Net 的方向感知 + 缺口 embedding**: 买入 PA +30%
3. **Layer 1.5 WARMUP 查表规则负面影响**: 查表与 plan 模型的分配有冲突
4. **Layer 3 Correction 对卖出恶化**: correction 信号偏噪声, 卖出 PA 从 -2.70 降到 -6.46
5. **Oracle Gap 178 bps 是信息天花板**: 所有策略只吃到 2~4% 的理论空间, PA +1.25 相当于 Oracle Gap -1.09 (绝对值一致, 百分比基数不同)
6. **A1 v19 (+6.02) > A2 v20 (+5.11)**: v20 的 chase 规则在此测试集上轻微拖累

---

## 四、后续改进方向

### P0: 生产部署 (推荐立即执行)

- [ ] 用 B1 配置 (v24 plan only + Layer 0 硬规则) 替换 v20
- [ ] 不启用 Layer 1.5 / Layer 3 / R6/R7
- [ ] 在 QE 完整回测流程中验证
- [ ] 前端 rl-execution 页面新增 v24 选项

### P1: 调试 Layer 1.5 WARMUP 查表

**问题**: B2 (+5.40) < B1 (+6.35), warmup 查表反而拖累。

**可能原因**:
1. 查表阈值基于全量统计, 但评估用的是 train.pkl 子集, 分布可能不同
2. warmup 改变前30m分配后, Layer 2 plan 模型在第30分钟的观察窗口特征也变了 → plan 生成的分布不匹配 (因为训练时 warmup=20% 固定)
3. warmup 增加某个方向的分配可能导致那个时段涨跌停约束下更多未成交

**修复方向**:
- 方案 A: 在 Layer 2 训练数据中用 warmup_alloc 实际值作为输入特征, 让 plan 模型适配不同 warmup
- 方案 B: 不用查表, 让 plan 模型本身覆盖全天 240 分钟 (包括前30m), 不再分 warmup/plan 两阶段
- 方案 C: 在 valid 集上 grid search warmup 阈值

### P2: 调试 Layer 3 Correction 卖出恶化

**问题**: B3 卖出 PA 从 -2.70 降到 -6.46。

**可能原因**:
1. 修正数据只用了 1000 只股票 (20M 上限提前截断), 可能有 selection bias
2. correction 标签 mean=-0.003, std=0.046, 但模型实际输出的修正方向可能对卖出系统性错误
3. 条件比例差标签 `clip(dp_cond - plan_cond, -0.3, +0.3)` 中, plan 模型对卖出已经很准了 (val_kl=0.5651), 修正空间小且噪声大

**修复方向**:
- 方案 A: 分方向训练两个 correction 网络 (买入/卖出)
- 方案 B: 只对买入启用 correction, 卖出关闭
- 方案 C: 用全量数据重新生成 correction 标签 (去掉 max_samples 限制, 优化速度)
- 方案 D: 增大 correction 网络容量 (当前只有 4.6K 参数, 可能欠拟合)

### P3: 扩大验证

- [ ] 50K orders 评估确认稳定性
- [ ] valid 集 + test 集分开评估 (检查 train/test 漂移)
- [ ] 按年度/季度分桶评估 (2024 vs 2025 vs 2026)
- [ ] 高波动日单独评估 (这是最大改善空间)

### P4: 架构级改进

**方向 A: 全天 Plan (消除 warmup/plan 分离)**
- 改为观察 t=0 (仅开盘价) → 生成 240 分钟全天计划
- 前 30 分钟也由模型决策, 不硬编码
- 需要重新设计网络输入 (只有开盘价+prev_close+gap, 无30m特征)

**方向 B: 条件 Plan 更新**
- 每 30 分钟用最新市场信息重新生成剩余 plan (类似 v22 DT 但简化)
- 需要 rolling window 特征 + 增量 plan 生成

**方向 C: 波动率分层专家**
- 高波动日 Oracle Gap 357-720 bps, 是最大改善空间
- 训练 high/low volatility 两个 plan 模型, 根据前30m波动率选择
- v21 曾尝试但未完成

**方向 D: 更强价格预测**
- v16 的 dir_acc=61.2% 不够
- 可以用 Transformer 或更丰富特征 (订单流/资金流) 做分钟级价格预测
- 预测 + 执行解耦: 预测模型���供 "未来 10 分钟方向概率" → 执行模型据此调整

---

## 五、代码位置

### 分析脚本
| 文件 | 说明 |
|------|------|
| `scripts/rl_execution/oracle_time_distribution.py` | Oracle 全量统计 (全股票全日期) |
| `scripts/rl_execution/oracle_gap_validate.py` | 缺口条件验证 (10%/20% 板分析) |

### v24 数据生成
| 文件 | 说明 |
|------|------|
| `scripts/rl_execution/v24_utils.py` | 共享工具函数 |
| `scripts/rl_execution/v24_gen_plan_data.py` | Layer 2 训练数据 |
| `scripts/rl_execution/v24_gen_dp_labels.py` | DP 标签 (numpy 向量化) |
| `scripts/rl_execution/v24_gen_correction_data.py` | Layer 3 训练数据 |

### v24 网络
| 文件 | 说明 |
|------|------|
| `rl_execution/network/execution_plan_net_v24.py` | Layer 2 网络 |
| `rl_execution/network/correction_net_v24.py` | Layer 3 网络 |

### v24 训练 & 评估
| 文件 | 说明 |
|------|------|
| `scripts/rl_execution/v24_train_plan.py` | Layer 2 训练 |
| `scripts/rl_execution/v24_train_correction.py` | Layer 3 训练 |
| `scripts/rl_execution/v24_evaluate.py` | 简化评估 (不推荐, 符号问题) |
| `scripts/rl_execution/v24_evaluate_sim.py` | **LimitAwareSimulator 评估 (推荐)** |

### v24 执行器
| 文件 | 说明 |
|------|------|
| `rl_execution/executor/v24_hybrid_executor.py` | 五层混合执行器 |

---

## 六、版本全线对比

| 版本 | 方法 | PA (bps) | Oracle Gap | 状态 |
|------|------|----------|-----------|------|
| v14 | SeqBPTT+PPO | -0.25 | 189.08 | — |
| v15 | SL预训练+PPO | +6.07 | 188.84 | steps膨胀 |
| v19 | 纯SL(1D-CNN) | +6.02* | 178.31* | 不分方向 |
| **v20** | **Hybrid Executor** | **+5.11*** | **179.06*** | **当前生产** |
| v22 | Decision Transformer | +1.75 | 196.57 | 不如v19 |
| v23 | v19+残差修正 | +1.97 | 182.01 | 失败(标签错配) |
| **v24 B1** | **方向+缺口 Plan** | **+6.35*** | **177.97*** | **推荐上线** |

*标注: v19/v20/v24 的数值来自同一次 10K orders 评估 (v24_evaluate_sim.py), 与之前 v19 独立评估的 +8.04 bps 不完全可比 (不同订单集)。

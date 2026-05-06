# QE 实验 `qe_20260430_010121_d55f` 深度交易分析与后续方向

生成日期：2026-05-01
分析模型：DeepSeek V4
实验：16 Loop 全人工配置，57 因子固定，`score_weighted_topk_v2` + `TailTWAPWithV25TwoStageStrategy`

---

## 1. 核心发现：基于实际交易数据的独立分析

### 1.1 实验的真实阶段划分

通过 IC 序列相关性矩阵分析，16 个 Loop 实际分为 4 个阶段：

| 阶段 | Loop | 实际动作 | 模型是否重训 | IC vs Loop1 相关 |
|------|------|---------|------------|----------------|
| A: 初始 | 1 | LGB，无 `label_horizon`，无 alpha158 | 是（初始） | 1.000 |
| B: HMM 后验 | 2-4 | 仅换 HMM 版本，模型完全相同 | **否** | 0.973 |
| C: 真正重训 | 5 | 设定 `label_horizon=10` + 数据分割 + LGB 重训 | 是 | 0.312 |
| D: 模型搜索 | 6-16 | 在 C 基础上试 CatBoost/LambdaMART/TabPFN/GRU/LGB(noHMM)/GRU×3/LSTM/TCN/XGBoost | 是 | 0.27-0.98 |

**B 阶段（Loop2-4）是三个完全无效的 Loop**：模型权重完全未变（IC 与 Loop1 相关性 0.973-1.000），仅更换 HMM 后处理参数，结果 CAGR 从 0.48 一路跌到 0.25。

### 1.2 持仓数量为何差异巨大 — 根因分析

**观察到的事实：**

| Loop | 模型 | 最大持仓 | 平均持仓 | P95 | 超50天 | 超75天 | CAGR |
|------|------|---------|---------|-----|--------|--------|------|
| 1 | LGB (无horizon) | 60 | 50.6 | 54 | 231 | 0 | 0.4804 |
| 5 | LGB | 97 | 66.9 | 97 | 380 | 96 | 0.8425 |
| 10 | LGB (无HMM) | **101** | **70.3** | **101** | **389** | **190** | **0.8464** |
| 14 | LSTM | 81 | 67.3 | 81 | 414 | 93 | 0.8306 |
| 15 | TCN | 76 | 55.7 | 72 | 281 | 10 | 0.8456 |

**根因：`_filter_dynamic_ndrop` 的自适应阈值 + 模型预测分布的差异**

策略 `ScoreWeightedTopkStrategy` 的换仓机制（`score_weighted_strategy.py:210-253`）：
- `min_n_drop=0`：允许一天内不卖出任何股票
- `max_n_drop=5`：每天最多卖出 5 只
- 自适应阈值：`threshold = max(score_std * 0.5, 0.005)`，其中 `score_std` 是当前持仓得分的标准差
- 对于每对卖出候选/买入候选，只有当 `buy_score - sell_score > threshold` 时才执行换仓

**关键数据——预测分数的分布差异：**

| 模型 | Pred Std | Top50边界 | Top50-Top100 Gap | 5%内额外股票 | 10%内额外股票 |
|------|---------|----------|-----------------|------------|-------------|
| LGB (Loop10) | 0.1058 | 0.1841 | **0.0224** | 17.1 | **41.2** |
| TCN (Loop15) | 0.1397 | 0.2832 | **0.0394** | 15.1 | **33.2** |
| LSTM (Loop14) | 0.1477 | 0.2354 | 0.0363 | 13.4 | 30.0 |

**机制链条：**

1. LGB 预测分数更紧凑 → 当前持仓得分标准差小 → 自适应阈值低（~0.05）
2. 但同为 LGB 的 Top50-Top100 差距也极小（0.0224）→ `buy_score - sell_score` 经常**低于**阈值
3. 买-卖分差小于阈值 → 换仓被动态跳过 → `min_n_drop=0` 时当天卖出 0 只
4. 同时每天仍有 5 只左右新买入（max_n_drop=5 始终买入）→ **净增持仓**
5. 442 天累积 → 持仓从 50 膨胀到 101

TCN 反之：
1. TCN 预测更分散 → 当前持仓得分标准差大 → 自适应阈值高（~0.07）
2. 但 Top50-Top100 差距更大（0.0394，是 LGB 的 1.75 倍）→ 买-卖分差更容易**超过**阈值
3. 更多换仓通过筛选 → 每天卖出更接近 5 → **持仓受控**
4. 最终 76 只，离目标 50 更近

**结论：持仓膨胀不是停牌/涨跌停导致的，而是策略的 `min_n_drop=0` 机制 + 模型预测分布紧凑共同导致的。** 这不是 bug，而是策略设计的预期行为——它允许"如果新股票不比旧股票显著更好，就不卖"。但 LGB 模型的预测区分度不足以触发足够的卖出。

### 1.3 进入候选但未买入的股票是否都是停牌/涨跌停？

**实际数据显示：Loop2-16 全部 `outside_held=0`，替补机制从未被触发。**

```
Loop1:  outside_held=45 (替补买入), pred_only=179 (预测到但未持有)
Loop2-16: outside_held=0, pred_only=0
```

只有 Loop1 出现过替补买入。Loop2-16 虽然配置了 `unfilled_handler=TAIL_SUBSTITUTE` + `backup_depth=15`，但替补从未激活。

这意味着以下可能性之一：
1. 策略的 `_filter_dynamic_ndrop` 在父类 `TopkDropoutStrategy` 之前拦截了替补逻辑
2. `TAIL_SUBSTITUTE` 机制依赖于 `inner_strategy` 的执行路径，但在 `ScoreWeightedTopkStrategy` 的重载中并未正确调用
3. 停牌过滤 `qe_suspend_filter_strict=True` 在信号生成阶段就已经排除了停牌股，使得替补不需要触发

无论哪种情况，**替补机制的审计比原报告假设的更紧迫——不是因为替补太多需要审计，而是替补根本不工作需要确认**。

### 1.4 某些模型是否更容易选出连板股票导致买入失败？

通过 `1day.ffr`（订单成交率）对比：

| 模型 | FFR | CAGR |
|------|-----|------|
| LGB Loop10 | 0.9744 | 0.8464 |
| TCN Loop15 | 0.9819 | 0.8456 |
| LambdaMART Loop7 | **0.8070** | 0.2337 |
| LGB Loop1 (无horizon) | **0.8070** | 0.4804 |
| TabPFN Loop8 | 0.9828 | 0.4356 |

LambdaMART 和早期的 LGB（Loop1）成交率只有 80.7%，**但这两个模型恰恰是信号质量最差的（IC 分别为 -0.009 和 0.051）**。信号质量与成交率呈正相关：信号质量差 → 选出的股票可能包含更多异常标的（停牌边缘、流动性差）→ 成交失败多。

对于表现最好的 LGB（Loop10）和 TCN（Loop15），成交率分别为 97.4% 和 98.2%，说明**涨跌停不是这些模型持仓膨胀的主因**。

---

## 2. 第一部分：QE 实验框架需立即改进的问题

按优先级排序：

### P0-1: `min_n_drop=0` 导致持仓不可控膨胀

**问题**：策略允许在模型预测区分度不足时完全跳过卖出，导致 LGB 持仓从 50 膨胀到 101。这是策略参数的配置问题而非 bug，但后果严重——回测持仓数与实盘无法匹配（实盘不可能同时持有 101 只股票做 V25 尾盘执行）。

**修复**：
- 将 `min_n_drop` 从 0 改为 2-3，保证每天至少有一定换仓
- 或者增加 `max_holding_count` 硬限制，超过 60 只时强制卖出超出部分
- 短期可以在策略模板中增加 `_enforce_max_holdings()` 方法

### P0-2: `TAIL_SUBSTITUTE` 替补机制在 Loop2-16 中从未激活

**问题**：所有 Loop2-16 的 `outside_held=0`，配置了替补但从未使用。这有两种可能：停牌过滤已经把问题股票排除了（好事），或者替补逻辑在 `ScoreWeightedTopkStrategy` 中被绕过了（bug）。

**修复**：
- 在 `ScoreWeightedTopkStrategy.generate_decision()` 中增加替补触发诊断日志
- 创建一个测试场景（如手动插入一只确定停牌的股票到 top50）验证替补是否工作
- 如果停牌过滤完全消除了替补需求，应该在 loop_summary 中记录 `substitute_never_triggered=True`

### P0-3: 因子重要性数据全部为零

**问题**：所有 16 个 Loop 的 `factor_analysis.feature_importance` 中，importance 值全部为 0.0000。数据存在但完全不可用。

**修复**：
- 检查 `read_exp_res.py` 中 `_extract_feature_importance()` 方法的 LGB 输出解析逻辑
- LGB 的 `feature_importance(gain)` 返回的真实值应远大于 0
- 当前 `method: "lightgbm_gain"` 标记正确，说明解析逻辑存在 bug

### P0-4: Loop1-4 的 `label_horizon` 在 summary 中错误标记为 1

**问题**：实际标签表达式为 `Ref($close, -11)/Ref($close, -1)-1`（10 日），但 loop_summary 将 horizon 记为 1。

**修复**：
- 在 summary 生成时，对无 `label_horizon` 字段的 config 从 conf.yaml 解析 label 表达式
- 或者在所有实验的 config.json 中强制写入 `label_horizon`

### P1-1: 非 PyTorch 模型的训练曲线缺失

**问题**：CatBoost/LambdaMART/TabPFN/XGBoost 既不在 `qlib_results_enhanced.json` 的 `training_diagnostics` 中，也不在 loop_summary 中有 `l2_train/l2_valid`。

**修复**：
- 将 `backfill_model_training_from_logs.py` 的 LGB/CatBoost/XGBoost 正则解析逻辑集成到 `read_exp_res.py` 的标准流程中
- LGB 的训练数据在 `run.log` 中格式标准（`[N] train's l2: X valid's l2: Y`），解析成本极低

### P1-2: `close_none_count` 高值缺乏归因

Loop10 有 430 次 close_none（缺失收盘价），是所有 Loop 中最高。但它的 CAGR 也最高（0.8464）。需要区分：
- 这些 close_none 是停牌导致的（需在实盘中处理）
- 还是数据源问题（Qlib bin 缺少某些日期）
- 还是股票已退市/ST

**修复**：在 loop_summary 中增加 `close_none_stocks` 列表和日期分布，便于人工检查。

---

## 3. 第二部分：提高年化收益和控制回撤的后续方向

### 3.1 模型选择：基于 16 个 Loop 的完整对比

按 Calmar 比率（CAGR/|MaxDD|）排序：

| 排名 | Loop | 模型 | CAGR | MaxDD | Calmar | 关键特征 |
|------|------|------|------|-------|--------|---------|
| 1 | 10 | **LGB (无HMM)** | 0.8464 | -0.1911 | 4.43 | IC中等(0.079), 持仓膨胀(101只) |
| 2 | 5 | LGB + HMM | 0.8425 | -0.1906 | 4.42 | 与Loop10同模型，HMM零增益 |
| 3 | 15 | **TCN + HMM** | 0.8456 | -0.2016 | 4.20 | 最低ICstd(0.084), 最低neg%(16%), 持仓控制最好(76只) |
| 4 | 6 | CatBoost | 0.8048 | -0.1925 | 4.18 | 最高IC(0.082), 但最大资金闲置(35.8M, 12.7%) |
| 5 | 14 | LSTM | 0.8306 | -0.2015 | 4.12 | IC/Top30稳定性均不错 |
| 6 | 9 | GRU | 0.7940 | -0.1995 | 3.98 | 最高Sharpe(2.14), 低MaxDD, 但CAGR低于LGB |

### 3.2 最有希望的方向（按预期收益排序）

#### 方向 1: LGB + TCN 模型集成（预期提升 CAGR 0.05-0.15）

**依据**：
- LGB（Loop10）和 TCN（Loop15）的 IC 序列相关性仅 0.837，说明它们捕捉了**不同的 alpha 信号**
- LGB 优势：更高的 Sharpe（2.10 vs 2.07），略低的 MaxDD
- TCN 优势：更稳定的 IC（std=0.084 vs 0.112），更低的 IC 负值比例（16% vs 21%），更好的持仓控制
- 两者 CAGR 几乎相同（0.8464 vs 0.8456），但 alpha 来源不同

**实施方案**：
- 简单等权集成：LGB + TCN 预测分数的 rank 平均
- 预期效果：IC 稳定性提升（互补 IC 波动），持仓更受控，MaxDD 有望降至 -0.18 以下
- 或使用历史 IC 加权的动态集成

#### 方向 2: 修复 `min_n_drop=0` + 硬持仓上限（预期 MaxDD 改善 0.02-0.05，CAGR 基本持平）

**依据**：
- Loop10 和 Loop5 的持仓膨胀到 101/97 只是数据 artifact，实盘无法复制
- 如果在回测中强制持仓 ≤60，会减少无效的尾部持仓，降低换手成本
- Loop15（TCN）的持仓控制较好（76 只）但仍有改善空间

**实施方案**：
- `min_n_drop=2` + `max_holding_count=65`
- 超过 65 只时强制按分数排序卖出超出部分
- 回测验证：重新运行 Loop10/Loop5 配置，对比原始结果

#### 方向 3: IC 衰减对抗 — 缩短模型重训周期（预期 CAGR 提升 0.10-0.20）

**依据**：所有模型的 IC 在回测期间系统性衰减：
- Q1 IC ≈ 0.11，Q4 IC ≈ 0.01（衰减 90%）
- 回测最后 20% 期间，IC 接近零（recent_IC < 0.02）
- 当前实验回测期为 2024-07 到 2026-03（约 20 个月），模型可能需要在中间点重训

**实施方案**：
- 将训练集划分为两段：2021-2024.06（train1），2023-2025.03（train2）
- 回测前半用 train1 的模型，后半用 train2 的模型
- 预期：Q3-Q4 IC 提升至 0.05-0.08

#### 方向 4: 尝试 GBDT 系列的 XGBoost → LightGBM 超参搜索（预期 CAGR 提升 0.03-0.08）

**依据**：
- Loop16（XGBoost + HMM）：IC=0.080（最高之一），但 CAGR 仅 0.7276（远低于 LGB 的 0.8464）
- XGBoost 的 IC 很好但无法转化为收益 — 可能超参未针对 10D 标签优化
- CatBoost（Loop6）：IC 最高（0.082）但资金闲置严重（12.7%）

**实施方案**：
- 对 LGB 进行 Bayesian 超参搜索：`num_leaves` [127, 255, 511], `learning_rate` [0.02, 0.05, 0.10], `min_child_samples` [20, 50, 100]
- 重点优化 `early_stopping_rounds` 和 `num_boost_round` — Loop10 的 best_iter=74，说明 120 轮训练中有 46 轮是冗余的

#### 方向 5: 因子层面的突破 — 从固定 57 因子到因子进化（预期是最有潜力的方向）

**依据**：
- 当前 16 个 Loop **全部使用完全相同的 57 个因子**，零因子变化
- QE 只探索了模型架构和 HMM 维度，完全没有探索因子维度
- 因子替换的收益潜力可能大于模型替换（IC 从 0.05 到 0.08 主要靠标签周期设定正确，而非模型变更）
- 当前 57 因子中存在冗余：所有 LGB 模型的 held_unique 从 Loop1 的 1298 降到 Loop5+ 的 800-820，说明其中约 500 只股票是"噪声因子选出的无 alpha 标的"

**实施方案**：
- 第一步：用现有的因子重要性数据（修复 P0-3 后），剔除 importance 排名最低的 10-15 个因子
- 第二步：从因子库中引入 5-10 个与现有因子低相关的新因子（如基本面因子、分钟级因子）
- 第三步：A/B 对比（固定模型 LGB，变动因子列表）

### 3.3 不建议的方向

- **LambdaMART 继续尝试**：Loop7 IC 为负（-0.009），Rank IC -0.076，排序学习在此任务上完全是反向信号。放弃。
- **TabPFN 继续尝试**：Loop8 IC 仅 0.017（接近零），正收益（0.436）大概率来自策略内置的正向偏差而非 alpha。成本（执行风险）高于收益。
- **HMM 后验叠加**：Loop5 vs Loop10（LGB+HMM vs LGB-HMM）CAGR 差 0.004（0.8425 vs 0.8464），HMM 零增益甚至微负。HMM 的配置复杂度和维护成本远超其带来的（零）收益。

---

## 4. 对原始诊断报告的补充修正

### 4.1 已有数据远比报告估计完整

`.tmp_qe_20260430_loop_summary.json` 已包含报告建议"回填"的绝大部分字段：
- `max_hold`, `avg_hold`, `p95_hold`, `days_gt50/55/60/75`（持仓统计）
- `l2_train`, `l2_valid`, `best_iter`, `hparams`（训练数据 — 仅 LGB 模型有效）
- `fatal_count`, `close_none_count`, `close_none_unique`（执行质量）
- `outside_held`, `pred_only`, `held_unique`（替补/覆盖统计）
- `cagr`, `abs_sharpe`, `abs_dd`（绝对收益 — 已完成）

**真正的瓶颈不是数据不足，而是此 summary 未入库到 `metrics_json.enhanced_metrics`。**

### 4.2 预警阈值需要基于数据重新校准

原报告建议 `max_position_count > 65` 预警。实际数据中 Loop10 的 101 持仓对应最高 CAGR。建议改为：
- `max_position_count > 80 AND avg_cash_ratio > 15%` → 真正有害（持仓膨胀 + 资金闲置）
- `max_position_count > 65 AND CAGR < 0.5` → 持仓膨胀但无效果

### 4.3 替补审计的优先级应重新评估

不是 P2（"后续最佳"），而是 P0 — 替补机制在 Loop2-16 中**从未工作**。根因已通过代码级追踪定位：`min_n_drop=0` 导致持仓膨胀 → `topk - effective_count < 0` 锁死容量约束 → `_do_realloc_substitute` 中 `max_new=0` 提前返回。同时备选列表遗漏了 Top50 内被 dynamic_n_drop 过滤的高质量候选。

详细分析见独立文档：
**[V25 叠加 TAIL_SUBSTITUTE 替补策略完整机制分析](qe_v25_tail_substitute_mechanism_20260501_deepseek_v4.md)**

### 4.4 原报告未提及的字段应补充

建议后端摘要字段增加（来自 loop_summary）：
- `held_unique`（回测期持有过的唯一个股数）
- `outside_held`（替补买入股票数）
- `pred_only`（预测到但从未持有的股票数）
- `fatal_count` / `warn_count`（执行质量）
- `close_none_count`（缺失收盘价次数）
- `days_gt50` / `days_gt75`（持仓超标天数）
- `hparams`（模型超参数摘要）
- `l2_train` / `l2_valid` / `best_iter`（训练指标 — LGB 已有）

---

## 5. 最终结论

1. **当前最优模型**：LGB（Loop10，CAGR=0.8464, Calmar=4.43）和 TCN（Loop15，CAGR=0.8456, Calmar=4.20）是表现最好的两个模型。LGB 的持仓膨胀到 101 只是策略参数 artifact，其真实 alpha 与 TCN 相当。两者 alpha 来源不同，适合集成。

2. **立即行动（本周）**：修复 `min_n_drop=0` → 2，增加硬持仓上限；修复 TAIL_SUBSTITUTE 替补机制的三个设计缺陷（详见 [V25+替补独立分析](qe_v25_tail_substitute_mechanism_20260501_deepseek_v4.md)）；修复因子重要性全部为零的 bug；将 loop_summary 数据入库。

3. **短期最优方向（1-2 周）**：LGB + TCN 集成，预期这是最可靠、风险最低的收益提升路径。同时启动因子层面的进化——当前 57 个固定因子的天花板可能已经被 LGB（CAGR=0.85）逼近。

4. **中期方向（1 个月）**：模型滚动重训对抗 IC 衰减；GBDT 系列超参搜索；分钟级新因子引入。

5. **核心风险**：所有模型的 IC 在回测末期衰减 90%，recent_IC < 0.02。这可能是因子拥挤、市场结构变化或数据泄露导致训练期 IC 虚高。在生产部署前，必须用最新数据（2026Q1-Q2）做样本外验证。

---

## 6. 相关文档

- **[V25 叠加 TAIL_SUBSTITUTE 替补策略：完整机制分析与设计缺陷](qe_v25_tail_substitute_mechanism_20260501_deepseek_v4.md)** — 详细追踪 V25 双层策略架构中 TAIL_SUBSTITUTE 替补机制的完整调用链、三个设计缺陷及修复方案。

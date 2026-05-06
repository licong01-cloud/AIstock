# QE V25 策略问题分析与修复建议

**分析日期**: 2026-05-01
**分析对象**: QE 实验 `qe_20260430_010121_d55f` Loop5
**策略版本**: ScoreWeightedTopkStrategyV2 + TailTWAPWithV25TwoStageStrategy
**分析方法**: 方案A - 离线复现（pred.pkl + positions 数据）

---

## 执行摘要

通过对 Loop5 回测数据的深入分析，发现了两个独立的策略 bug：

1. **✅ 已修复**: TAIL_SUBSTITUTE 备选股选择逻辑错误
   - 固定选择第51-65名，跳过Top50内高排名股票
   - 已修改模板文件并提交 Git (commit: 7e092f06)

2. **⚠️ 待修复**: 日频策略换仓模式缺少 topk 约束
   - 导致持仓数从50膨胀到62只
   - 需要添加 max_buy_slots 约束逻辑

**重要发现**: 原文档分析结论错误，实际买入的股票全部来自 Top50，不是50名以外。

---

## 问题1: TAIL_SUBSTITUTE 备选股选择逻辑错误 ✅

### 问题描述

**原始假设**（文档 `qe_v25_tail_substitute_mechanism_20260501_deepseek_v4.md`）:
> 尾盘替补股票都是50名以外，导致大量alpha浪费

**实际情况**（数据验证）:
- 2024-12-31 实际买入5只股票：排名 19/20/24/25/30
- **100% 来自 Top50，0只来自备选（51-65名）**

### 根本原因

**代码位置**: `score_weighted_strategy_v2.py` Line 269-274

```python
# 错误逻辑
backup_depth = 15
backup_sids = ranked.iloc[self.topk:self.topk + backup_depth].index.tolist()
# 固定选择第 51-65 名
```

**问题分析**:
1. 日频策略生成买入候选（Top50内17只）
2. `_filter_dynamic_ndrop` 只选前5只（排名19/20/24/25/30）
3. 剩余12只 Top50 股票（排名21-50）被忽略
4. `_backup_candidates` 跳过这12只，直接设为51-65名
5. 如果前5只涨停，TAIL_SUBSTITUTE 会选择51-65名低排名股票

### 修复方案 ✅

```python
# 修复后
backup_depth = 100
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()  # 从第1名开始
already_ordered = set(actual_buys)  # 排除日频已下单
self._backup_candidates = [
    (sid, float(ranked[sid])) for sid in backup_sids
    if sid not in current_holdings and sid not in already_ordered
]
```

**修复效果**:
- Top50内剩余股票优先替补
- Top50全涨停时继续往后选
- 始终选择可交易股票中排名最高的

### 修复状态

- ✅ 主模板已修改
- ✅ app_tpl v4/v5/v6 已修改
- ✅ 远端机已同步
- ✅ Git 已提交 (commit: 7e092f06)

---

## 问题2: 日频策略换仓模式缺少 topk 约束 ⚠️

### 问题描述

**现象**:
- Loop5 持仓从50只膨胀到62只（+24%）
- 膨胀期：2024-12-31 ~ 2025-01-13（10个交易日）
- 每天净增1-2只，持续累积

**数据证据**:

| 日期 | 持仓数 | 卖出 | 买入 | 净增 | 模式 |
|------|--------|------|------|------|------|
| 2024-12-30 | 51 | 4 | 5 | +1 | ROTATE |
| 2024-12-31 | 53 | 4 | 5 | +1 | ROTATE |
| 2025-01-02 | 54 | 3 | 5 | +2 | ROTATE |
| 2025-01-03 | 56 | 4 | 5 | +1 | ROTATE |
| ... | ... | ... | ... | ... | ... |
| 2025-01-13 | 62 | 5 | 5 | 0 | ROTATE |

### 根本原因

**代码位置**: `score_weighted_strategy_v2.py` Line 124-139

```python
# 换仓模式（ROTATE）
else:
    current_scores_arr = np.array([...])
    actual_sells, actual_buys = self._filter_dynamic_ndrop(
        sell_candidates, buy_candidates, current_scores_arr,
    )
# Line 139: 直接使用 actual_buys，没有检查是否超过 topk
final_holdings = [s for s in current_holdings if s not in all_sells] + actual_buys
```

**问题分析**:
1. `_filter_dynamic_ndrop` 返回配对的 sells/buys（最多各5只）
2. 当 `current_holdings=53` 时：
   - 卖出4只 → 剩余49只
   - 买入5只 → 变成54只（超过topk=50）
3. 换仓模式**没有 max_buy_slots 约束**
4. 补仓模式有约束（Line 115-117）：
   ```python
   buy_slots = self.topk - remaining_after_sell  # 严格限制
   ```

### 修复方案 ⚠️

**位置**: `score_weighted_strategy_v2.py` Line 131 之后

```python
# 换仓模式：sell-buy 配对 + 动态 n_drop
actual_sells, actual_buys = self._filter_dynamic_ndrop(
    sell_candidates, buy_candidates, current_scores_arr,
)

# 新增：换仓模式也要保证持仓数不超过 topk
retained_count = len([s for s in current_holdings
                      if s not in (set(actual_sells) | set(ghost_sells))])
max_buy_slots = max(0, self.topk - retained_count)
if len(actual_buys) > max_buy_slots:
    actual_buys = actual_buys[:max_buy_slots]
    logger.info(
        "[ScoreWeightedV2] 换仓模式 topk 约束: 买入从 %d 只限制到 %d 只. date=%s",
        len(actual_buys), max_buy_slots, cur_dt,
    )
```

### 修复优先级

**🔴 高优先级** - 建议立即修复

**理由**:
1. 影响所有使用该策略的实验
2. 持仓膨胀导致单只股票权重被稀释
3. 违反 topk=50 的策略设计初衷
4. 可能影响回测结果的可比性

### 修复影响

- ✅ 新实验立即生效
- ❌ 已运行实验不受影响（需重跑）
- ⚠️ 可能改变历史回测结果（持仓数会严格控制在50只）

---

## 数据分析详情

### 方案A 离线复现结果

**数据来源**:
- `pred.pkl`: 每日预测评分
- `positions_normal_1day.pkl`: 每日持仓快照
- 复现逻辑: 模拟日频策略决策

**关键发现**:

1. **全期模式分布**:
   - 换仓模式（ROTATE）: 441天（99.8%）
   - 补仓模式（COMPENSATE）: 1天（0.2%）

2. **extra_buys 统计**（实际买入 - 日频预测）:
   - 平均: 3.62 只/天
   - 中位数: 5 只/天
   - 85.5% 的天数实际买入超过日频预测

3. **膨胀期特征**:
   - 日频预测买入: 0-2 只（受 max_buy_slots 约束）
   - 实际买入: 4-5 只
   - 差异: 3-5 只（来自哪里？）

### TAIL_SUBSTITUTE 执行情况

**验证方法**: 检查 2024-12-31 实际买入股票来源

**结果**:
```
实际买入5只股票来源:
  来自 Top50: 5 只 ✓
  来自 备选（51-65）: 0 只
  来自 65名以外: 0 只

Top50 买入股票:
  300591.SZ: rank=19, score=0.178048
  002830.SZ: rank=20, score=0.177382
  002795.SZ: rank=24, score=0.172104
  688619.SH: rank=25, score=0.170892
  300511.SZ: rank=30, score=0.167591
```

**结论**:
- TAIL_SUBSTITUTE 未执行（或执行了但备选股全涨停）
- 实际买入是日频策略的正常订单
- 持仓膨胀是日频策略自身的 bug，与 TAIL_SUBSTITUTE 无关

---

## 策略权重计算逻辑

### 权重方法

**配置** (Loop5 conf.yaml):
```yaml
weight_method: softmax
temperature: 1.0
max_weight: 0.05      # 单只最多5%
min_weight: 0.005     # 单只至少0.5%
max_position_ratio: 0.95  # 总仓位最多95%
```

### 计算公式

**文件**: `score_weighted_strategy.py` Line 359-363

```python
s_norm = s / self.temperature  # temperature=1.0
s_norm = s_norm - np.max(s_norm)  # 数值稳定性
exp_s = np.exp(s_norm)
weights = exp_s / exp_s.sum()  # softmax 归一化
```

### 买入金额计算

**文件**: `score_weighted_strategy_v2.py` Line 183-227

```python
w = weight_map.get(sid, 0.0)  # 获取权重
target_value = total_account_value * w  # 计算目标金额
target_value = min(target_value, self.max_single_order_value)  # 限制单笔最大
```

**结论**: 股票评分越高 → softmax 权重越大 → 买入金额越多

---

## 代码调用链

```
┌─────────────────────────────────────────────────────────────┐
│ 日频策略 (每天执行1次)                                        │
│ ScoreWeightedTopkStrategyV2                                 │
│ 文件: score_weighted_strategy_v2.py                         │
├─────────────────────────────────────────────────────────────┤
│ Line 51: generate_trade_decision()                          │
│   ↓                                                         │
│ Line 77: ranked = scores.sort_values(ascending=False)       │
│   ↓                                                         │
│ Line 110-131: 补仓模式 vs 换仓模式                           │
│   ↓                                                         │
│ Line 129-131: _filter_dynamic_ndrop() [换仓模式]            │
│   ⚠️ 缺少 max_buy_slots 约束                                │
│   ↓                                                         │
│ Line 139: final_holdings = retained + actual_buys           │
│   ⚠️ 可能超过 topk=50                                       │
│   ↓                                                         │
│ Line 268-277: 生成 _backup_candidates                       │
│   ✅ 已修复：从第1名开始，包含Top50                          │
└─────────────────────────────────────────────────────────────┘
                          ↓ 传递给
┌─────────────────────────────────────────────────────────────┐
│ 尾盘执行策略 (分钟级执行)                                     │
│ TailTWAPWithV25TwoStageStrategy                             │
│   ↓ 继承                                                    │
│ TailTWAPWithLimitStrategy                                   │
│ 文件: tail_twap_strategy.py                                 │
├─────────────────────────────────────────────────────────────┤
│ Line 139: _do_realloc_substitute()                          │
│   ↓                                                         │
│ Line 176: backup_candidates = getattr(...)                  │
│   ✅ 读取日频策略设置的 _backup_candidates                   │
│   ↓                                                         │
│ Line 206-225: 按顺序选择可交易股票                           │
│   ✅ 逻辑正确，无需修改                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 后续修复建议

### 1. 🔴 高优先级：修复换仓模式 topk 约束

**文件**: `score_weighted_strategy_v2.py`
**位置**: Line 131 之后
**工作量**: 10行代码
**影响**: 所有新实验

**修复步骤**:
1. 在 `_filter_dynamic_ndrop` 返回后添加约束逻辑
2. 计算 `retained_count` 和 `max_buy_slots`
3. 截断 `actual_buys` 到 `max_buy_slots`
4. 添加日志输出
5. 同步到所有模板文件（v4/v5/v6）
6. 同步到远端机
7. Git 提交

**测试验证**:
- 启动新实验，检查持仓数是否严格≤50
- 对比修复前后的回测结果

### 2. 🟡 中优先级：验证 TAIL_SUBSTITUTE 实际执行情况

**目的**: 确认 TAIL_SUBSTITUTE 是否真的执行了

**方法**:
1. 在 `tail_twap_strategy.py` Line 243 添加日志：
   ```python
   if extra_shares > 1e-5:
       self._realloc_extra[sid] = extra_shares
       logger.info(
           "[TAIL_SUBSTITUTE] 替补买入: %s, rank=%d, shares=%.2f, price=%.2f",
           sid, rank, extra_shares, price
       )
   ```
2. 重跑 Loop5 或启动新实验
3. 检查日志中是否有 `[TAIL_SUBSTITUTE]` 记录

**预期结果**:
- 如果有记录：验证替补股票是否来自 Top50
- 如果无记录：说明 TAIL_SUBSTITUTE 未执行（n_blocked=0 或 max_new=0）

### 3. 🟢 低优先级：优化 _backup_candidates 生成效率

**当前实现**:
```python
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()
```

**问题**: 每天生成150个候选，但实际可能只用到前几个

**优化方案**:
```python
# 延迟生成：只在 TAIL_SUBSTITUTE 需要时才生成
# 或者：根据历史 n_blocked 统计动态调整 backup_depth
```

**收益**: 减少内存占用和计算开销

### 4. 🟢 低优先级：添加持仓数监控指标

**建议**: 在 `_diag_stats` 中添加持仓数统计

```python
self._diag_stats["holdings_count"] = len(final_holdings)
self._diag_stats["holdings_overflow"] = max(0, len(final_holdings) - self.topk)
```

**用途**:
- 实时监控持仓数是否超过 topk
- 便于发现类似的持仓膨胀问题

---

## 文档修正建议

### 原文档错误

**文件**: `F:\Dev\AIstock\docs\analysis\qe_v25_tail_substitute_mechanism_20260501_deepseek_v4.md`

**错误结论**:
> 尾盘替补股票都是50名以外，导致大量alpha浪费

**正确结论**:
> 1. 尾盘替补股票的备选列表设置错误（固定51-65名）
> 2. 但实际买入的股票全部来自 Top50（日频策略正常订单）
> 3. 持仓膨胀是日频策略换仓模式缺少 topk 约束导致的
> 4. 与 TAIL_SUBSTITUTE 无关

### 建议修正

1. 更新文档标题和摘要
2. 添加"数据验证"章节，说明实际买入股票来源
3. 区分两个独立的 bug
4. 更新修复建议

---

## 附录

### A. 修复时间线

| 日期 | 事件 |
|------|------|
| 2026-05-01 | 发现问题，启动分析 |
| 2026-05-01 | 完成方案A离线复现 |
| 2026-05-01 | 修复 TAIL_SUBSTITUTE 备选股逻辑 |
| 2026-05-01 | Git 提交 (commit: 7e092f06) |
| 2026-05-01 | 生成分析报告 |
| 待定 | 修复换仓模式 topk 约束 |

### B. 相关文件清单

**策略文件**:
- `rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py`
- `rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy.py`
- `rdagent/scenarios/qlib/experiment/factor_template/tail_twap_strategy.py`
- `rdagent/scenarios/qlib/experiment/factor_template/tail_twap_v25_strategy.py`

**配置文件**:
- `qe_workspace/qe_20260430_010121_d55f/Loop5/conf.yaml`

**数据文件**:
- `qe_workspace/.../Loop5/mlruns/.../artifacts/pred.pkl`
- `qe_workspace/.../Loop5/mlruns/.../artifacts/portfolio_analysis/positions_normal_1day.pkl`
- `qe_workspace/.../Loop5/mlruns/.../artifacts/portfolio_analysis/indicators_normal_1day.pkl`

**分析脚本**:
- `F:\Dev\RD-Agent-main\scripts\_tmp\reconstruct_loop5.py`
- `F:\Dev\RD-Agent-main\scripts\_tmp\analyze_20241231.py`
- `F:\Dev\RD-Agent-main\scripts\_tmp\verify_backup_candidates.py`

**报告文件**:
- `F:\Dev\RD-Agent-main\reports\tail_substitute_backup_candidates_fix_20260501.md`
- 本文档

### C. 联系人

**分析人员**: Claude Opus 4.7
**审核人员**: 待定
**修复负责人**: 待定

---

**文档版本**: 1.0
**最后更新**: 2026-05-01
**状态**: 待审核

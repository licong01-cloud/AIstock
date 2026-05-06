# V25 叠加 TAIL_SUBSTITUTE 替补策略：完整机制分析与设计缺陷

生成日期：2026-05-01
分析模型：DeepSeek V4
分析对象：`qe_20260430_010121_d55f` 全部 16 Loop 的 `ScoreWeightedTopkStrategyV2` + `TailTWAPWithV25TwoStageStrategy` 执行链

---

## 1. 双层策略架构总览

```
┌─────────────────────────────────────────────────────────────┐
│ 第一层：外层的日频选股（ScoreWeightedTopkStrategyV2）         │
│ 每天 09:30 运行 1 次，决定当天买卖哪些股票                      │
│                                                             │
│ 输入：模型预测分数（所有股票 × 1 天）                         │
│ 输出：TradeDecisionWO（sell_orders + buy_orders）            │
│       + _backup_candidates（替补候选列表）                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ TradeDecisionWO
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ Qlib NestedExecutor（日频 → 分钟频桥接）                      │
│ 将外层的日频决策分发给内层的分钟频执行器                         │
└──────────────────────────┬──────────────────────────────────┘
                           │ 每分钟
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ 第二层：内层的分钟频执行（TailTWAPWithV25TwoStageStrategy）    │
│ 每天 09:31-15:00 运行 240 次，决定每分钟执行多少               │
│                                                             │
│ 输入：outer_trade_decision + V25 RL 模型                    │
│ 核心功能：                                                   │
│   - P0 首分钟：跌停买入/涨停卖出全量立即执行                      │
│   - 正常分钟：V25 RL 模型个性化分配（前30分钟88.79%，后210分钟11.21%）│
│   - 尾盘14:55：检测未成交订单 → TAIL_SUBSTITUTE 替补买入         │
└─────────────────────────────────────────────────────────────┘
```

## 2. 外层策略：日频选股逻辑（custom_strategy.py）

### 2.1 完整选股流程

```python
# custom_strategy.py: generate_trade_decision()
# 每天 09:30 执行一次

# Step 1: 获取预测分数
scores = model.predict(all_stocks)  # 所有股票的预测分数

# Step 2: 当前持仓
current_holdings = trade_position.get_stock_list()  # 昨日收盘后的持仓

# Step 3: Top50 候选
ranked = scores.sort_values(ascending=False)
topk_stocks = set(ranked.head(50).index)  # 排名 1-50

# Step 4: 确定买卖候选
sell_candidates = []  # 持仓中但不在 Top50 的 → 卖出
for sid in current_holdings:
    if sid not in topk_stocks:
        sell_candidates.append((sid, scores[sid]))
sell_candidates.sort(key=lambda x: x[1])  # 低分优先卖

buy_candidates = []  # Top50 中但不在持仓的 → 买入（全在 Top50 内！）
for sid in topk_stocks:
    if sid not in current_holdings:
        buy_candidates.append((sid, scores[sid]))
buy_candidates.sort(key=lambda x: -x[1])  # 高分优先买

# Step 5: 动态 n_drop 过滤
if len(valid_holdings) < topk:
    # 补仓模式：卖出受 max_n_drop 约束，买入不限速
    actual_sells = sell_candidates[:max_n_drop]
    actual_buys = buy_candidates[:buy_slots]
else:
    # 换仓模式：sell-buy 配对 + 自适应阈值
    actual_sells, actual_buys = _filter_dynamic_ndrop(
        sell_candidates, buy_candidates, current_scores_arr
    )

# Step 6: 填充备选列表 ← 问题所在
backup_depth = 15
backup_sids = ranked.iloc[50:65].index.tolist()  # 排名 51-65
_backup_candidates = [
    (sid, score) for sid in backup_sids
    if sid not in current_holdings
]
```

### 2.2 动态 n_drop 自适应阈值（score_weighted_strategy.py:210-253）

```python
def _filter_dynamic_ndrop(sell_candidates, buy_candidates, current_scores):
    # sell_candidates: 持仓中不在 Top50 的，按分数升序（最差的在前）
    # buy_candidates: Top50 中未持仓的，按分数降序（最好的在前）

    threshold = max(np.std(current_scores) * 0.5, 0.005)

    # 硬上限截断
    sell_cands = sell_candidates[:max_n_drop]  # 最多 5 只
    buy_cands = buy_candidates[:max_n_drop]    # 最多 5 只

    # 配对检查：第 i 对 (sell, buy) 的分数差必须 > threshold
    actual_sells, actual_buys = [], []
    for i in range(min(len(sell_cands), len(buy_cands))):
        if buy_score[i] - sell_score[i] > threshold:
            actual_sells.append(sell_cands[i])
            actual_buys.append(buy_cands[i])
        else:
            break  # 一旦一对不满足，后续全部跳过

    # min_n_drop=0 → 即使 0 对通过也接受

    return actual_sells, actual_buys
```

**关键含义**：`buy_candidates` 中排名靠前但分数与待卖股票差距不够大的，会被直接丢弃，且**不进入任何替补候选列表**。

## 3. 内层策略：V25 分钟频执行逻辑（tail_twap_v25_strategy.py）

### 3.1 P0 首分钟检查（仅第 1 分钟执行一次）

```python
# tail_twap_v25_strategy.py:436-519
# 对每只 order，在第一个交易分钟检查：

close_price = 当日收盘价（Qlib 模拟器实时提供）
limit_up = 涨停价
limit_down = 跌停价

if direction == BUY:
    if close_price <= limit_down:
        → "p0_limit_buy_at_down_limit"  # 跌停板买入：全量立即执行
    elif close_price >= limit_up:
        → "limit_up_buy_blocked"          # 涨停板买入：阻塞！无法执行
    else:
        → 正常执行，进入 V25 分钟分配

if direction == SELL:
    if close_price >= limit_up:
        → "p0_limit_sell_at_up_limit"    # 涨停板卖出：全量立即执行
    elif close_price <= limit_down:
        → "limit_down_sell_blocked"       # 跌停板卖出：阻塞！无法执行
```

### 3.2 V25 分钟分配（分钟 1-239）

V25 使用两个 RL 模型为每只股票个性化生成 240 分钟的分配计划：

```python
# tail_twap_v25_strategy.py:312-367
def _generate_plan_for_order(stock_id, direction, ...):
    # 输入特征
    open_price   = 当日开盘价
    prev_close   = 昨日收盘价
    limit_pct    = 涨跌停幅度（主板 10%，科创板 20%）
    gap_pct      = (open - prev_close) / prev_close  # 开盘跳空
    gap_ratio    = gap_pct / limit_pct                # 跳空相对涨跌停的比例
    gap_bucket   = 离散化为 9 个桶                     # 跳空程度分档

    # EarlyNet: 前 30 分钟分配
    pred_early = EarlyPlanNetEnhanced(gap_bucket, gap_ratio, gap_ratio_signed,
                                       limit_pct, is_buy, day_features)
    # 输出：30 维 softmax 权重

    # LateNet: 后 210 分钟分配
    pred_late = LatePlanNet(gap_bucket, gap_ratio, is_buy,
                             early_weight_raw, early_peak_pos, early_concentration)
    # 输出：210 维权重

    # 合并：前 30 分钟 ~88.79% 总权重，后 210 分钟 ~11.21%
    plan = concat(pred_early * 0.8879, pred_late * 0.1121)
    return plan / plan.sum()  # 归一化
```

**V25 的 RL 模型作用**：根据开盘跳空情况，决定每只股票在 240 分钟内的执行节奏。跳空大的股票可能在早盘分配更多，跳空小的可能均匀分配。这是纯时序优化，**不改变买卖哪些股票的决策**。

### 3.3 尾盘替补检查（14:55，分钟 235）

```python
# tail_twap_v25_strategy.py:398-403
trigger_step = 235 - 1  # REALLOC_OFFSET=235

if rel_trade_step >= trigger_step and not _realloc_done:
    _realloc_done = True
    if unfilled_handler == "TAIL_SUBSTITUTE":
        _do_realloc_substitute()  # 替补买入
    else:
        _do_realloc()              # 加仓已有持仓（TAIL_BOOST）
```

## 4. TAIL_SUBSTITUTE 替补买入的完整逻辑（tail_twap_strategy.py:139-243）

```python
def _do_realloc_substitute(trade_start_time, trade_end_time):
    # === 阶段 1: 统计被阻塞订单 ===
    blocked_cash = 0.0
    n_blocked = 0
    for order in outer_trade_decision.get_decision():
        if order.direction != BUY:
            continue
        fill_rate = 1.0 - trade_amount_remain[order.stock_id] / original_amount
        if fill_rate < 0.2:  # BLOCKED_FILL_THRESHOLD = 0.2
            price = get_deal_price(order.stock_id)
            blocked_cash += trade_amount_remain[order.stock_id] * price
            n_blocked += 1

    if blocked_cash <= 0 or n_blocked == 0:
        return  # 没有阻塞订单 → 直接返回

    # === 阶段 2: 获取备选列表 ===
    outer_strategy = outer_trade_decision.strategy
    backup_candidates = getattr(outer_strategy, "_backup_candidates", [])
    if not backup_candidates:
        _do_realloc()  # 备选为空 → 回退到 TAIL_BOOST
        return

    # === 阶段 3: 容量约束检查 ← 关键瓶颈 ===
    topk = getattr(outer_strategy, "topk", None)
    current_holdings = set(outer_strategy.trade_position.get_stock_list())
    already_added = set(_realloc_extra.keys())
    n_selling = sum(1 for o in outer_trade_decision if o.direction == SELL)

    effective_count = len(current_holdings) + len(already_added) - n_selling

    if topk is not None:
        max_new = max(0, min(n_blocked, topk - effective_count))
        #         当 effective_count >= 50 时 → max_new = 0 → 替补被锁死！

    if max_new == 0:
        return  # ← 既不做替补，也不回退到 TAIL_BOOST

    # === 阶段 4: 从备选列表中筛选 ===
    selected = []
    for sid, score in backup_candidates:
        if len(selected) >= max_new:
            break
        if sid in current_holdings or sid in already_added:
            continue
        if not is_stock_tradable(sid):  # 停牌/涨跌停检查
            continue
        price = get_deal_price(sid)
        if price is valid:
            selected.append((sid, price, score))

    if not selected:
        _do_realloc()  # 没有可交易的备选股 → 回退到 TAIL_BOOST
        return

    # === 阶段 5: 执行替补买入 ===
    cash_per_stock = blocked_cash / len(selected)
    for sid, price, score in selected:
        extra_shares = cash_per_stock / price
        _realloc_extra[sid] = extra_shares  # 在后续分钟中买入
```

## 5. 三个设计缺陷

### 缺陷 1：备选列表遗漏 Top50 内未被选中的候选（custom_strategy.py:231-236）

**场景重现**：

```
今日预测排名：
  #1-#45: 已在持仓中 → 持有不动
  #46-#50: 未持仓 → buy_candidates (5 只，全在 Top50 内，是当日最值得买的新股票)

sell_candidates: 25 只在 Top50 外但被持有的股票

_filter_dynamic_ndrop(
    sell_candidates = [sell_#175, sell_#203, ...]  # 分数低的
    buy_candidates  = [buy_#46, buy_#47, buy_#48, buy_#49, buy_#50]  # 分数高的
)

自适应阈值 threshold = 0.05
  对 1 (sell_#175 vs buy_#46): gap = 0.12 → 通过 ✓ → actual_buys 加入 buy_#46
  对 2 (sell_#203 vs buy_#47): gap = 0.07 → 通过 ✓ → actual_buys 加入 buy_#47
  对 3 (sell_#211 vs buy_#48): gap = 0.03 → 不通过 ✗ → break
  (buy_#49, buy_#50 完全不参与配对，直接被丢弃)

_backup_candidates = ranked[50:65]  # 排名 #51-#65
  → buy_#49, buy_#50 未进入替补列表！它们虽然被阈值过滤掉，但分数远高于 #51-#65
```

**后果**：如果 buy_#46（涨停）买不进去，替补从 #51-#65 选。但 #49 和 #50 的质量显著高于 #51+，却被浪费了。

**修复**：

```python
# custom_strategy.py:230-236 当前代码
backup_depth = 15
backup_sids = ranked.iloc[self.topk:self.topk + backup_depth].index.tolist()
self._backup_candidates = [
    (sid, float(ranked[sid])) for sid in backup_sids
    if sid not in current_holdings
]

# 应改为：
# 优先级 1: buy_candidates 中未被 actual_buys 选中的（Top50 内，质量最高）
unselected_buy_ids = {b[0] for b in buy_candidates} - set(actual_buys)
unselected_buys = [(sid, float(ranked[sid])) for sid in unselected_buy_ids]

# 优先级 2: Top50 之外的备选（原逻辑）
backup_depth = 15
backup_sids = ranked.iloc[self.topk:self.topk + backup_depth].index.tolist()
tail_backups = [
    (sid, float(ranked[sid])) for sid in backup_sids
    if sid not in current_holdings and sid not in unselected_buy_ids
]

# 合并：Top50 内未选中的优先
self._backup_candidates = unselected_buys + tail_backups
```

### 缺陷 2：`topk - effective_count` 容量约束在持仓膨胀时锁死替补（tail_twap_strategy.py:195-197）

```python
effective_count = len(current_holdings) + already_added - n_selling
max_new = max(0, min(n_blocked, topk - effective_count))
```

当 `min_n_drop=0` 导致持仓从 50 膨胀到 70+：
- `effective_count ≈ 70 - n_selling ≈ 67`（即使卖 5 只）
- `topk - effective_count = 50 - 67 = -17`
- `max_new = 0` → **替补永远不触发**

**Loop 数据验证**：

| Loop | avg_hold | topk-effective_count (典型) | 替补能否触发 | outside_held |
|------|----------|---------------------------|------------|-------------|
| 1 | 50.6 | 50 - 48 = +2 | **能** | **45** |
| 5 | 66.9 | 50 - 62 = -12 | 否 | 0 |
| 10 | 70.3 | 50 - 65 = -15 | 否 | 0 |
| 14 | 67.3 | 50 - 62 = -12 | 否 | 0 |
| 15 | 55.7 | 50 - 51 = -1 | 极少 | 0 |

**修复**：

```python
# tail_twap_strategy.py:195-197 当前代码
effective_count = len(current_holdings) + len(already_added) - n_selling
if topk is not None:
    max_new = max(0, min(n_blocked, topk - effective_count))

# 应改为：替补应使用宽松约束
# 替补股票次日若不在 Top50 中会被正常卖出，不会永久撑大持仓
effective_count = len(current_holdings) + len(already_added) - n_selling
if topk is not None:
    # 允许超出 topk 最多 n_blocked 只（替代被阻塞的订单）
    # 这些超额持仓在次日会被正常换仓机制卖出
    max_new = min(n_blocked, self._backup_depth)
    # 但仍然控制总持仓不超过 topk + backup_depth
    hard_cap = (topk + self._backup_depth) - effective_count
    max_new = max(0, min(max_new, hard_cap))
```

### 缺陷 3：`_do_realloc_substitute` 中 `max_new=0` 时不回退到 TAIL_BOOST

```python
# tail_twap_strategy.py:201-202
if max_new == 0:
    return  # ← 直接返回，不做任何处理
```

当容量约束阻止替补时，被阻塞的资金（blocked_cash）完全闲置。应该回退到 `_do_realloc()` 将闲置资金加仓到已有持仓（TAIL_BOOST 模式）。

对比：`backup_candidates` 为空时（第 177-179 行）有正确的回退逻辑：

```python
if not backup_candidates:
    self._do_realloc(trade_start_time, trade_end_time)
    return
```

但 `max_new=0` 时没有这个回退，是一个不一致的处理。

**修复**：

```python
if max_new == 0:
    self._do_realloc(trade_start_time, trade_end_time)  # 回退到 TAIL_BOOST
    return
```

## 6. 数据证据：Loop1 为何是唯一触发替补的

| 指标 | Loop1 | Loop5 | Loop10 | 说明 |
|------|-------|-------|--------|------|
| 因子 | 57 (无alpha158) | 215 (含alpha158) | 215 (含alpha158) | alpha158 使模型预测质量大幅提升 |
| IC | 0.051 | 0.079 | 0.079 | 信号质量差异 |
| `1day.ffr` | **0.807** | 0.975 | 0.974 | Loop1 有 19.3% 订单金额未成交 |
| avg_hold | **50.6** | 66.9 | 70.3 | Loop1 持仓紧贴 topk |
| p95_hold | **54** | 97 | 101 | |
| `topk-effective_count` | **正值（≈+2）** | 负值（≈-12） | 负值（≈-15） | Loop1 容量约束允许替补 |
| `outside_held` | **45** | 0 | 0 | |

**Loop1 触发替补的三要素**：
1. 弱模型（IC=0.05，无 alpha158）→ 选出的 Top50 包含更多难交易的股票 → `1day.ffr` 仅 80.7%
2. 高排名换手率（pred_rank_turnover=0.185）→ 每天更多股票进出 Top50 → 更多卖出释放仓位
3. 持仓紧贴 50 → `effective_count` 在卖出后低于 50 → 容量约束允许替补

**Loop2+ 不触发替补的三要素**：
1. 强模型（IC=0.08+，含 alpha158）→ 选出更可交易的股票 → `1day.ffr` 高达 97%+
2. 低排名换手率（pred_rank_turnover=0.09）→ 持仓稳定 → 累积膨胀
3. `min_n_drop=0` → 持仓膨胀到 70+ → 容量约束锁死

## 7. 三个缺陷的优先级和修复顺序

| 优先级 | 缺陷 | 影响 | 修复成本 |
|--------|------|------|---------|
| P0 | 缺陷 2 + 缺陷 3 | 替补机制在持仓 >50 时完全锁死，且不 fallback 到 TAIL_BOOST | 低（改 2 行代码） |
| P0 | 上游 `min_n_drop=0` | 持仓持续膨胀的根源，导致缺陷 2 的条件永远成立 | 低（改配置默认值） |
| P1 | 缺陷 1 | 备选列表漏了 Top50 内质量更好的候选 | 低（改 5 行代码） |

## 8. V25 RL 模型的角色边界

**V25 做什么**：
- 为每只待交易的股票个性化生成 240 分钟的执行分配计划
- 输入开盘跳空信息，输出每分钟的执行比例
- 这是一个**时序优化器**：优化"何时交易"而非"交易什么"

**V25 不做什么**：
- 不参与股票选择（由外层 ScoreWeightedTopkStrategyV2 决定）
- 不参与替补候选生成（由外层 `_backup_candidates` 提供）
- 不参与买卖方向决策

**V25 与替补的唯一交互点**：尾盘 14:55 调用 `_do_realloc_substitute()`，从外层获取 `_backup_candidates`，统计 blocked_cash，执行替补买入。V25 本身对替补逻辑无额外贡献。

---

## 附录：涉及的关键文件

| 文件 | 角色 |
|------|------|
| `custom_strategy.py` | 外层策略，日频选股 + 生成 `_backup_candidates` |
| `score_weighted_strategy.py` | 外层策略基类，`_filter_dynamic_ndrop` 自适应阈值 |
| `tail_twap_v25_strategy.py` | 内层策略，V25 分钟频执行 + P0 涨跌停处理 |
| `tail_twap_strategy.py` | 内层策略基类，`_do_realloc_substitute` 替补逻辑 |

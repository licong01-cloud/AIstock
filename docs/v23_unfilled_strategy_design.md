# v23 日内执行策略分析 & 涨停未成交资金处理方案

> 日期: 2026-04-06
> 状态: 设计中

---

## 一、v23 修正网络训练分析

### 1.1 Pipeline 完成状态

v23 全流程于 2026-04-04 完成（数据生成→训练→评估）。

| 步骤 | 状态 | 耗时 | 产出 |
|------|------|------|------|
| 数据生成 (50K orders → 10.4M samples) | 完成 | 142.8 min | correction_data.npz (617MB) |
| SL 训练 (30 epochs, 14.9K params) | 完成 | ~53 min | v23_correction_sl.pt (64KB) |
| 评估 (10K orders, 4种模式) | 完成 | ~15 min | v23_pipeline.log |

### 1.2 评估结果 — v23 修正层劣化

| 模式 | PA (bps) | Oracle Gap | 结论 |
|------|----------|------------|------|
| TWAP | +2.00 | — | 基准 |
| v20_baseline (v19 plan) | **+5.10** | 178.95 | 当前生产线 |
| v23_rules (仅R6/R7) | +5.10 | 178.95 | 规则层无增量 |
| v23_sl (修正网络) | **+1.97** | 182.01 | 比v20差3.13 bps |

### 1.3 训练数据覆盖

| 维度 | 值 |
|------|-----|
| 订单池 | 1,781,863 单 (train.pkl) |
| 时间范围 | 2024-01-02 ~ 2025-06-30 (18个月) |
| 股票覆盖 | 5,033 只 (全A) |
| 实际处理 | 50,000 单 → 49,826 成功 → 10,421,216 samples |
| pkl 行情 | 5,033 只, 2024-01-02 ~ 2026-03-19 (~530 交易日) |

### 1.4 数据质量问题

#### Bug 1: price_vs_vwap 特征爆炸

`$volume0` 确认为**前复权股数**（来自 Qlib bin: `volume_hand * 100 / qfq_factor`）。

28 只停牌/零成交股的 volume 全为 0，导致:
```
vwap = sum(close * 0) / (0 + 1e-8) = 0
feat[4] = (cur - 0) / (0 + 1e-8) = cur / 1e-8 ≈ 10^9
```

影响: 7,350 / 10,421,216 = 0.07% 样本，mean 被拉到 952,300。

**修复**: `process_order()` 入口加 `if vol.sum() < 1e-6: return None` 跳过零成交日。

#### Bug 2: DP max_steps=60 导致标签空间错配（根因）

DP 求解器 `solve_dp()` 的 `max_steps=60`，强制在前 60 分钟内执行完毕。
而 v19 plan 覆盖 210 分钟（第 30~239 分钟）。

标签 `log(dp_dist_norm / plan_dist_norm)` 的分布:

| 时段 | 样本数 | label mean | label > 0 | 含义 |
|------|--------|-----------|-----------|------|
| 早盘 (t=0.1~0.3) | 209万 | **+0.185** | **72.2%** | DP 说"多执行" |
| 盘中 (t=0.3~0.6) | 358万 | **-3.000** | **0.0%** | DP 已执行完，plan 还在分配 |
| 午后 (t=0.6~0.8) | 297万 | **-3.000** | **0.0%** | 同上 |
| 尾盘 (t=0.8~1.0) | 178万 | **-3.000** | **0.0%** | 同上 |

85.5% 的标签打满 clamp -3，96.9% 的 |label| > 1。修正网络学到的是"全面压缩执行量"的常数偏置。

**这不是 DP 认为后面不该执行，而是 DP 的时间窗口只有 60 分钟，根本没覆盖后面的时段。**

### 1.5 v23 结论

v23 修正网络在当前标签设计下无法工作。需要先解决标签空间问题，再重新训练。
标签重设计将在策略层改造完成后进行（见第三部分）。

---

## 二、涨停未成交资金处理 — 两个新策略

### 2.1 问题描述

当前系统中，涨停买不进的订单:
- `ExecutionEngine.step()`: 检测到涨停封板 → `continue` 跳过
- `ExecutionEngine.finalize()`: 未成交信号标记为 `skipped`
- 未成交资金留在现金中，**没有任何再分配逻辑**

强势行情中（top 股票频繁涨停），资金利用率低下。

### 2.2 现有架构

```
信号生成 (signal_generator.py)
    ↓ score_items (全量评分, 降序)
再平衡策略 (topk_dropout.py / topk_dropout_rc.py)
    ↓ trade_signals (BUY/SELL)
执行引擎 (execution_engine.py)
    ↓ step() × 240 分钟
    ↓ finalize()
持仓更新 + 净值快照
```

关键接口:
- `BaseRebalanceStrategy.generate_orders()` → `List[signal]`
- `ExecutionEngine.step(bar_data_map, market_context, bar_time)` → `List[fill]`
- 策略注册: `@register` 装饰器 + `STRATEGY_CODE` 类属性

### 2.3 策略 A: 尾盘加仓持仓股 (TOPK_DROPOUT_BOOST)

**触发条件**: 14:50 (尾盘最后10分钟)
**逻辑**:
1. 检查所有 BUY 信号的执行状态
2. 找出因涨停而未成交（或部分成交）的买单
3. 计算释放的现金 = 未成交数量 × 目标价格
4. 从当前持仓中选择可加仓的股票:
   - 排除当日买入的 (T+0 不可卖，加仓无意义)
   - 排除当前已涨停的 (加仓也买不进)
   - 按持仓权重等比例分配
5. 生成加仓 BUY 信号，注入执行引擎

**实现位置**: `ExecutionEngine` 内部，通过 `unfilled_handler` 配置切换

**配置**:
```json
{
  "unfilled_handler": "BOOST_EXISTING",
  "unfilled_trigger_minute": 210
}
```

### 2.4 策略 B: 尾盘替补买入 (TOPK_DROPOUT_SUBSTITUTE)

**触发条件**: 14:50 (尾盘最后10分钟)
**逻辑**:
1. 同策略 A 的步骤 1-3
2. 从 score_items 中取排名 topk+1 ~ topk+backup_depth 的候选股
3. 过滤:
   - 排除已持仓的
   - 排除当前已涨停的
   - 排除当日已有买单的
4. 按排名顺序逐个分配资金（等权，每只 = 释放现金 / 涨停未成交数量）
5. 生成替补 BUY 信号，注入执行引擎

**配置**:
```json
{
  "unfilled_handler": "SUBSTITUTE_BUY",
  "unfilled_trigger_minute": 210,
  "unfilled_backup_depth": 15
}
```

### 2.5 实现方案

#### 改动文件清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `backend/execution_engine_handlers/__init__.py` | 新建 | handler 注册表 |
| `backend/execution_engine_handlers/base.py` | 新建 | 基类 |
| `backend/execution_engine_handlers/boost_existing.py` | 新建 | 策略 A |
| `backend/execution_engine_handlers/substitute_buy.py` | 新建 | 策略 B |
| `backend/services/paper_trading/execution_engine.py` | 修改 | 新增 unfilled 检测 + handler 调用 |

#### Handler 基类

```python
class BaseUnfilledHandler(ABC):
    """涨停未成交资金处理器基类."""

    @abstractmethod
    def generate_replacement_orders(
        self,
        unfilled_buy_orders: List[Dict],   # 涨停未成交的买单
        released_cash: float,               # 释放的现金
        current_positions: Dict[str, Dict], # 当前持仓
        score_items: List[Dict],            # 全量评分列表
        bar_data_map: Dict[str, Dict],      # 当前 bar 数据
        limit_prices: Dict[str, Dict],      # 涨跌停价格
        config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """返回替代买入信号列表."""
```

#### ExecutionEngine 改动

```python
class ExecutionEngine:
    def __init__(self, portfolio_id, trade_date):
        # ... 现有属性 ...
        self._unfilled_handler = None
        self._score_items = []
        self._unfilled_checked = False

    def initialize(self, score_items=None):
        # ... 现有逻辑 ...
        # 新增: 加载 handler
        handler_code = self._config.get("unfilled_handler")
        if handler_code:
            from ...execution_engine_handlers import get_handler
            self._unfilled_handler = get_handler(handler_code)
        if score_items:
            self._score_items = score_items

    def step(self, bar_data_map, market_context, bar_time=None):
        # ... 现有逻辑 ...

        # 新增: 尾盘 unfilled 检查 (只触发一次)
        trigger_minute = int(self._config.get("unfilled_trigger_minute", 210))
        if (self._step_count == trigger_minute
                and not self._unfilled_checked
                and self._unfilled_handler):
            self._handle_unfilled(bar_data_map)
            self._unfilled_checked = True

        return fills

    def _handle_unfilled(self, bar_data_map):
        """检测涨停未成交买单，调用 handler 生成替代订单."""
        unfilled = []
        released_cash = 0.0
        for key, state in self._orders.items():
            if state.side != "BUY":
                continue
            remaining = state.total_quantity - state.executed_quantity
            if remaining <= 0:
                continue
            # 检查是否因涨停导致
            limits = self._limit_prices.get(state.symbol)
            bar = bar_data_map.get(state.symbol, {})
            if limits and bar:
                cur_price = bar.get("close", 0)
                if cur_price >= limits["up_limit"] * 0.999:
                    unfilled.append({
                        "symbol": state.symbol,
                        "remaining_qty": remaining,
                        "target_price": cur_price,
                    })
                    released_cash += remaining * cur_price

        if not unfilled or released_cash < 1000:
            return

        new_orders = self._unfilled_handler.generate_replacement_orders(
            unfilled_buy_orders=unfilled,
            released_cash=released_cash,
            current_positions=self._positions,
            score_items=self._score_items,
            bar_data_map=bar_data_map,
            limit_prices=self._limit_prices,
            config=self._config,
        )

        # 注入新订单
        for order in new_orders:
            symbol = order["symbol"]
            qty = order["quantity"]
            state = self._algo.init_order(symbol, "BUY", qty)
            state.child_fills.append({"signal_id": None, "reason": order.get("reason", "unfilled_handler")})
            self._orders[f"{symbol}_BUY_sub"] = state
```

### 2.6 回测对比方案

创建 3 个 portfolio，使用相同信号源:

| Portfolio | unfilled_handler | 说明 |
|-----------|-----------------|------|
| baseline | 不配置 | 现有行为，涨停 skip |
| boost | BOOST_EXISTING | 策略 A: 加仓持仓股 |
| substitute | SUBSTITUTE_BUY | 策略 B: 替补买入 |

对比指标:
- 年化收益率、最大回撤、夏普比率
- 资金利用率 = 日均已投资金额 / 总资产
- 涨停日收益差异
- 换手率变化

推荐测试区间: 2024-09 (924行情, 大量涨停) + 2024-01~2025-06 (完整周期)

---

## 三、后续模型训练计划 (策略确认后执行)

### 3.1 v23 标签重设计

**问题**: DP max_steps=60 导致 85% 标签打满 clamp。

**方案 A: 扩大 DP 时间窗口**
- `max_steps` 从 60 扩大到 240 (覆盖全天)
- DP 计算量增加 4x: O(240 × 21 × 6) ≈ 30K → 120K
- 标签空间自然平衡

**方案 B: 条件执行比例修正**
- 不用 log(dp_dist / plan_dist)
- 改用 `clip(dp_cond_frac - plan_cond_frac, -0.3, +0.3)`
- 避免零值爆炸，标签在 [-0.3, +0.3] 范围内

**方案 C: 仅训练有信号的分钟**
- 过滤掉 DP 和 plan 都接近均匀的分钟
- 只保留涨跌停附近、大幅波动、DP 明显偏离 plan 的分钟
- 样本量减少但信噪比高

### 3.2 数据修复

1. `_compute_features()`: 零成交日 vwap guard
   ```python
   if vwap < 1e-4:
       vwap = cur
   ```
2. `process_order()`: 跳过零成交日
   ```python
   if vol.sum() < 1e-6:
       return None
   ```

### 3.3 训练计划

1. 修复数据 bug + 标签重设计
2. 重新生成 correction_data.npz (200K orders, ~3小时)
3. SL 训练 (~10分钟 GPU)
4. 评估 + 消融 (plan_only / v23_sl / v23_rules)
5. 如果 PA > v19 baseline → 集成到 v20 Hybrid Executor

### 3.4 涨停场景专项模型 (可选)

基于策略 A/B 的回测数据，训练一个专门处理涨停场景的模型:
- 输入: 涨停时刻的市场特征 (距涨停距离、量能、时间)
- 输出: 加仓 vs 替补 的最优决策
- 方法: 离线 RL 或 contextual bandit

---

## 四、执行优先级

| 优先级 | 任务 | 依赖 |
|--------|------|------|
| P0 | 实现策略 A (BOOST_EXISTING) | 无 |
| P0 | 实现策略 B (SUBSTITUTE_BUY) | 无 |
| P1 | 回测对比 baseline / A / B | P0 |
| P2 | v23 标签重设计 + 数据修复 | 无 |
| P2 | v23 重新训练 + 评估 | P2 |
| P3 | 涨停场景专项模型 | P1 + P2 |

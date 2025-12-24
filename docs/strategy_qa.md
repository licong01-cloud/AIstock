# 策略常见问题解答

> **说明**: 本文档回答策略使用中的常见问题。

---

## Q1: RealtimeQuoteSubscriber 服务是什么？

**A**: `RealtimeQuoteSubscriber` 是一个**实时行情订阅服务**，用于从 miniQMT 获取实时行情数据。

**功能**：
- 订阅股票实时行情（通过 xtquant）
- 接收行情回调推送
- 提供获取最新行情的接口

**当前状态**：
- ✅ 已实现，但**尚未集成到策略自动执行流程**
- ⚠️ 目前策略使用的是**定时调度**，不是事件驱动

**如何使用**：
```python
from backend.infra.realtime_quote_subscriber import get_realtime_quote_subscriber

subscriber = get_realtime_quote_subscriber()

# 订阅实时行情
def on_quote(stock_code, quote):
    price = quote.get('lastPrice')
    print(f"{stock_code}: {price}")

seq = subscriber.subscribe(["600519.SH"], on_quote)
subscriber.start()  # 启动订阅服务
```

---

## Q2: 如何让策略自动执行？

**A**: 策略已经实现了**自动执行**，无需人工触发！

### 自动执行机制

1. **策略调度器** (`StrategyScheduler`) 在后端启动时自动启动
2. **从数据库加载**所有启用的策略配置
3. **根据调度配置**设置定时任务
4. **定时触发执行**，策略自动判断买卖点并下单

### 配置示例

**策略配置**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "1d",
  "symbols": ["600519.SH"],
  "position_size": 0.1
}
```

**调度配置**：
```json
{
  "type": "daily",
  "time": "09:30"
}
```

**执行流程**：
```
每日09:30自动触发
   ↓
对每个股票运行策略
   ↓
获取数据、计算指标
   ↓
判断买卖信号
   ↓
自动下单（如果有信号）
   ↓
记录执行结果
```

**无需人工干预！**

---

## Q3: 双均线策略是否可以在已有持仓的情况下，根据15分钟线做双均线策略？

**A**: ✅ **是的，完全可以！**

### 支持15分钟线

双均线策略已支持**可配置的数据周期**：

**配置示例**：
```json
{
  "ma_short": 5,
  "ma_long": 20,
  "period": "15m",  // 15分钟线
  "symbols": ["600519.SH"],
  "position_size": 0.1
}
```

**调度配置**：
```json
{
  "type": "minute",
  "interval": 15  // 每15分钟执行
}
```

### 已有持仓的处理

策略会：
1. ✅ **检查当前持仓**
2. ✅ **如果有持仓，可以卖出**
3. ✅ **如果没有持仓，可以买入**
4. ✅ **自动判断买卖信号**

**卖出逻辑**：
- 如果 MA5 下穿 MA20，**自动卖出全部持仓**
- 卖出数量 = 当前可卖数量（`can_sell`）

**买入逻辑**：
- 如果 MA5 上穿 MA20，**自动买入**
- 买入数量 = 根据仓位大小计算

### 日内交易场景

**配置**：
- 数据周期：`15m`（15分钟线）
- 调度频率：每15分钟执行一次
- 均线周期：MA5 / MA20（基于15分钟线）

**执行流程**：
```
每15分钟自动执行
   ↓
获取15分钟K线数据（从xtquant）
   ↓
计算MA5和MA20（基于15分钟线）
   ↓
判断交叉信号
   ↓
如果有信号，自动执行买卖
```

---

## Q4: 是否策略运行后自动判断买卖点，不需要人工触发？

**A**: ✅ **是的，完全自动！**

### 自动执行流程

1. **策略调度器自动启动**（后端启动时）
2. **定时触发执行**（根据调度配置）
3. **自动获取数据**（历史K线 + 实时行情）
4. **自动计算指标**（均线等）
5. **自动判断信号**（交叉、突破等）
6. **自动执行交易**（下单）
7. **自动记录结果**（数据库）

### 无需人工干预

- ✅ 无需手动触发
- ✅ 无需手动判断
- ✅ 无需手动下单
- ✅ 全自动运行

**唯一需要做的**：
1. 配置策略参数（通过UI或API）
2. 启用策略（`enabled: true`）
3. 设置调度时间

---

## Q5: 获取实时行情数据的字段和策略中使用的字段是否已经完全对齐？

**A**: ✅ **已对齐，但需要说明**

### 字段映射

#### xtquant 实时行情字段
- `lastPrice` → `close`（最新价 → 收盘价）
- `volume` → `volume`（成交量，已对齐）

#### TDX/Tushare 实时行情字段
- `price` → `close`（当前价格 → 收盘价）
- `volume` → `volume`（成交量，已对齐）

### 当前处理

**在 `RealtimeQuoteSubscriber.get_latest_quote()` 中**：
```python
return {
    "lastPrice": float(df_price.iloc[0, 0]),  # xtquant字段
    "close": float(df_price.iloc[0, 0]),      # 对齐策略字段 ✅
    "volume": float(df_volume.iloc[0, 0]),    # 已对齐 ✅
    ...
}
```

**在策略代码中**：
```python
# 字段对齐：优先使用close，如果没有则使用lastPrice或price
current_price = quote.get("close") or quote.get("lastPrice") or quote.get("price")
```

### ✅ 字段已对齐

- ✅ `close` 字段已统一（xtquant的`lastPrice`和TDX的`price`都映射到`close`）
- ✅ `volume` 字段已对齐（所有数据源都使用`volume`）
- ✅ 策略统一使用 `close` 和 `volume` 字段

---

## 总结

### ✅ 已实现的功能

1. **策略自动执行**：
   - ✅ 定时调度自动执行
   - ✅ 自动判断买卖点
   - ✅ 自动下单

2. **支持15分钟线**：
   - ✅ 可配置数据周期
   - ✅ 支持日内交易
   - ✅ 已有持仓可以卖出

3. **字段对齐**：
   - ✅ 实时行情字段已对齐
   - ✅ 策略统一使用close和volume

### 📝 使用建议

**日频策略**：
```json
{
  "period": "1d",
  "schedule": {"type": "daily", "time": "09:30"}
}
```

**日内策略（15分钟线）**：
```json
{
  "period": "15m",
  "schedule": {"type": "minute", "interval": 15}
}
```

---

**所有功能已实现，策略可以全自动运行！**


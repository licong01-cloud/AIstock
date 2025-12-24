# 策略实时行情数据说明

> **说明**: 本文档说明策略如何获取和使用实时行情数据。

---

## 当前实现

### 数据获取方式

**当前策略使用的是历史K线数据**，不是实时行情：

1. **数据源**: `UnifiedDataAccess.get_stock_data()` 
   - 获取3个月的历史K线数据
   - 数据来源：TDX API / Tushare / Akshare

2. **数据用途**:
   - 计算均线（MA5, MA20等）
   - 判断技术指标
   - 生成交易信号

3. **执行时机**:
   - 定时调度执行（如每日09:30）
   - 手动API触发
   - **不是实时行情触发**

### 已添加的实时行情支持

在最新版本中，策略已添加**可选的实时行情更新**：

- 获取历史数据后，会尝试获取实时行情
- 如果实时价格与历史数据最后一条不同，会更新最新价格
- 这样可以基于最新价格判断信号

**代码位置**:
- `backend/strategies/ma_cross_strategy.py` - `_fetch_stock_data()` 方法
- `backend/strategies/trend_following_strategy.py` - `_fetch_stock_data()` 方法

---

## 实时行情数据获取

### 可用的实时行情接口

系统提供了实时行情获取接口：

1. **UnifiedDataAccess.get_realtime_quotes()**
   ```python
   from backend.core.unified_data_access_impl import UnifiedDataAccess
   
   data_access = UnifiedDataAccess()
   quote = data_access.get_realtime_quotes("600519.SH")
   # 返回: {
   #   "symbol": "600519.SH",
   #   "price": 1800.0,  # 当前价格
   #   "volume": 1000000,  # 成交量
   #   "change_percent": 2.5,  # 涨跌幅
   #   ...
   # }
   ```

2. **数据源优先级**:
   - TDX API（本地，最快）
   - Tushare（需要token）
   - Akshare（兜底）

### 实时行情数据字段

实时行情包含以下字段：

- `price`: 当前价格
- `volume`: 成交量
- `change_percent`: 涨跌幅
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `pre_close`: 昨收价
- `amount`: 成交额
- `timestamp`: 时间戳

---

## 策略执行流程

### 当前流程（基于历史数据）

```
1. 定时触发（如09:30）
   ↓
2. 获取历史K线数据（3个月）
   ↓
3. 计算技术指标（均线等）
   ↓
4. 判断交易信号（交叉、突破等）
   ↓
5. 执行交易（下单）
```

### 实时行情触发流程（可选）

如果需要实时行情触发，可以：

```
1. 订阅实时行情（WebSocket/轮询）
   ↓
2. 收到行情更新
   ↓
3. 获取历史数据 + 最新实时价格
   ↓
4. 计算技术指标
   ↓
5. 判断交易信号
   ↓
6. 执行交易
```

---

## 如何实现实时行情触发

### 方案1：在策略中添加实时行情检查

修改策略的 `run()` 方法：

```python
def run(self, symbol: str) -> Dict[str, Any]:
    # 1. 获取历史数据
    data = self._fetch_stock_data(symbol)
    
    # 2. 获取实时行情
    realtime_quote = self._get_realtime_quote(symbol)
    
    # 3. 合并数据
    if realtime_quote:
        # 更新最新价格
        data.iloc[-1, data.columns.get_loc("close")] = realtime_quote["price"]
    
    # 4. 生成信号
    signal = self.generate_signal({"symbol": symbol, "data": data})
    
    # 5. 执行信号
    if signal:
        return self.execute_signal(signal)
```

### 方案2：使用事件驱动架构

1. **创建行情订阅服务**:
   ```python
   class RealtimeQuoteSubscriber:
       def subscribe(self, symbols: List[str], callback: Callable):
           # 订阅实时行情
           # 收到更新时调用 callback
   ```

2. **策略注册回调**:
   ```python
   subscriber.subscribe(["600519.SH"], lambda quote: strategy.run(quote["symbol"]))
   ```

### 方案3：使用 xtquant 的实时行情

如果使用 miniQMT，可以通过 `xtquant` 获取实时行情：

```python
import xtquant.xtdata as xtdata

# 订阅实时行情
xtdata.subscribe_quote(["600519.SH"], period="tick")

# 获取实时行情
quote = xtdata.get_market_data_ex(
    stock_list=["600519.SH"],
    period="tick",
    count=1
)
```

---

## 当前策略的数据使用

### 双均线策略

- **数据**: 历史K线数据（3个月）
- **计算**: MA5, MA20
- **信号**: MA5上穿/下穿MA20
- **实时更新**: ✅ 已添加（更新最新价格）

### 趋势跟踪策略

- **数据**: 历史K线数据（3个月）
- **计算**: MA20, 成交量均值
- **信号**: 价格突破均线 + 成交量放大
- **实时更新**: ✅ 已添加（更新最新价格和成交量）

---

## 建议

### 对于日频策略（当前实现）

- ✅ **当前方式已足够**: 每日定时执行，使用历史数据计算信号
- ✅ **实时行情更新**: 已添加，可以基于最新价格判断

### 对于高频策略（未来扩展）

- ⚠️ **需要实时行情触发**: 使用事件驱动架构
- ⚠️ **需要低延迟**: 使用 WebSocket 或 xtquant 实时订阅
- ⚠️ **需要快速执行**: 优化策略执行逻辑

---

## 总结

1. **当前策略**: 使用历史K线数据 + 可选实时行情更新
2. **执行方式**: 定时调度（如每日09:30），不是实时行情触发
3. **实时行情支持**: 已添加，用于更新最新价格
4. **未来扩展**: 可以实现事件驱动的实时行情触发

**注意**: 当前实现适合日频策略。如果需要高频实时触发，需要实现事件驱动架构。


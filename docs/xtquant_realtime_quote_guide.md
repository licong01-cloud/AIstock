# xtquant 实时行情数据获取指南

> **说明**: 本文档说明如何使用 xtquant 从 miniQMT 获取实时行情数据。

---

## ✅ 是的，可以从 miniQMT 获取实时行情！

xtquant 提供了完整的实时行情订阅和获取接口。

---

## 核心接口

### 1. 订阅单股实时行情

```python
import xtquant.xtdata as xtdata

# 订阅单股行情（支持回调）
seq = xtdata.subscribe_quote(
    stock_code="600519.SH",  # 股票代码
    period="tick",           # 周期：tick(分笔), 1m(1分钟), 5m(5分钟), 1d(日线)等
    start_time="",           # 开始时间（空表示实时）
    end_time="",             # 结束时间（空表示实时）
    count=0,                 # 数量（0表示实时推送）
    callback=on_data         # 回调函数（可选）
)

# 回调函数示例
def on_data(datas):
    """
    datas格式: {stock_code: [data1, data2, ...]}
    """
    for stock_code in datas:
        for data in datas[stock_code]:
            print(f"{stock_code}: 价格={data.get('lastPrice')}, 时间={data.get('time')}")
```

### 2. 订阅全推行情（推荐）

```python
# 订阅全市场行情
seq = xtdata.subscribe_whole_quote(
    code_list=["SH", "SZ"],  # 市场代码：SH(上海), SZ(深圳)
    callback=on_data
)

# 或订阅指定股票
seq = xtdata.subscribe_whole_quote(
    code_list=["600519.SH", "000001.SZ"],
    callback=on_data
)

# 回调函数
def on_data(datas):
    """
    datas格式: {stock_code: {time, lastPrice, open, high, low, volume, ...}}
    """
    for stock_code, quote in datas.items():
        print(f"{stock_code}: 价格={quote.get('lastPrice')}, 成交量={quote.get('volume')}")
```

### 3. 主动获取实时行情（从缓存）

```python
# 获取实时行情数据（从缓存）
data = xtdata.get_market_data(
    field_list=["time", "lastPrice", "open", "high", "low", "volume"],
    stock_list=["600519.SH"],
    period="tick",      # tick(分笔), 1m(1分钟), 5m(5分钟), 1d(日线)
    start_time="",
    end_time="",
    count=1             # 获取最新1条
)

# 返回格式: {field: DataFrame}
# data["lastPrice"] 是 DataFrame，index是股票代码，columns是时间
```

### 4. 获取全推数据

```python
# 获取全推数据（最新快照）
tick_data = xtdata.get_full_tick(["600519.SH", "000001.SZ"])

# 返回格式: {stock_code: {time, lastPrice, open, high, low, volume, ...}}
```

### 5. 获取 Level2 行情

```python
# 获取 Level2 行情快照（需要Level2权限）
l2_data = xtdata.get_l2_quote(
    field_list=["time", "lastPrice", "bidPrice", "askPrice", "bidVol", "askVol"],
    stock_code="600519.SH",
    start_time="",
    end_time="",
    count=1
)
```

---

## 支持的数据周期

### Level1 数据
- `tick` - 分笔数据（实时）
- `1m` - 1分钟线
- `5m` - 5分钟线
- `15m` - 15分钟线
- `30m` - 30分钟线
- `1h` - 1小时线
- `1d` - 日线

### Level2 数据（需要权限）
- `l2quote` - Level2实时行情快照
- `l2order` - Level2逐笔委托
- `l2transaction` - Level2逐笔成交
- `l2quoteaux` - Level2实时行情补充（总买总卖）
- `l2thousand` - Level2千档盘口

---

## 数据字段说明

### tick 分笔数据字段
```python
{
    'time': 1733118954000,        # 时间戳（毫秒）
    'lastPrice': 11.39,           # 最新价
    'open': 11.39,                # 开盘价
    'high': 11.4,                 # 最高价
    'low': 11.31,                 # 最低价
    'lastClose': 11.38,           # 昨收价
    'amount': 862127800.0,        # 成交额
    'volume': 758613,             # 成交量
    'pvolume': 75861284,          # 盘口成交量
    'stockStatus': 3,             # 股票状态
    'transactionNum': 37062,      # 成交笔数
    'askPrice': [11.4, 11.41, ...],  # 卖盘价格（5档）
    'bidPrice': [11.39, 11.38, ...], # 买盘价格（5档）
    'askVol': [10929, 12401, ...],   # 卖盘量（5档）
    'bidVol': [2429, 7127, ...],     # 买盘量（5档）
}
```

### 全推数据字段（与tick类似）
```python
{
    'time': 1733118954000,
    'lastPrice': 11.39,
    'open': 11.39,
    'high': 11.4,
    'low': 11.31,
    'lastClose': 11.38,
    'amount': 862127800.0,
    'volume': 758613,
    'stockStatus': 3,
    'askPrice': [11.4, 11.41, ...],
    'bidPrice': [11.39, 11.38, ...],
    'askVol': [10929, 12401, ...],
    'bidVol': [2429, 7127, ...],
}
```

---

## 使用示例

### 示例1：订阅实时行情并处理

```python
import xtquant.xtdata as xtdata
import time

# 回调函数
def on_realtime_quote(datas):
    for stock_code, quote in datas.items():
        price = quote.get('lastPrice')
        volume = quote.get('volume')
        print(f"[{stock_code}] 价格: {price}, 成交量: {volume}")

# 订阅全推行情
seq = xtdata.subscribe_whole_quote(
    code_list=["600519.SH", "000001.SZ"],
    callback=on_realtime_quote
)

# 保持运行（阻塞线程）
try:
    xtdata.run()  # 阻塞运行，持续接收回调
except KeyboardInterrupt:
    # 取消订阅
    xtdata.unsubscribe_quote(seq)
    print("已取消订阅")
```

### 示例2：主动获取最新价格

```python
import xtquant.xtdata as xtdata

# 获取最新价格（从缓存）
def get_latest_price(stock_code):
    data = xtdata.get_market_data(
        field_list=["lastPrice"],
        stock_list=[stock_code],
        period="tick",
        count=1
    )
    
    if data and "lastPrice" in data:
        df = data["lastPrice"]
        if not df.empty:
            return df.iloc[0, 0]  # 获取最新价格
    return None

# 使用
price = get_latest_price("600519.SH")
print(f"最新价格: {price}")
```

### 示例3：在策略中使用实时行情

```python
import xtquant.xtdata as xtdata
from backend.strategies.base_strategy import BaseStrategy

class RealtimeStrategy(BaseStrategy):
    def __init__(self, strategy_id, executor, config):
        super().__init__(strategy_id, executor, config)
        self.subscribed_stocks = config.get("symbols", [])
        self.subscription_seq = None
    
    def start_realtime_subscription(self):
        """启动实时行情订阅"""
        def on_quote(datas):
            for stock_code, quote in datas.items():
                if stock_code in self.subscribed_stocks:
                    self.process_realtime_quote(stock_code, quote)
        
        # 订阅全推行情
        self.subscription_seq = xtdata.subscribe_whole_quote(
            code_list=self.subscribed_stocks,
            callback=on_quote
        )
        
        # 在后台线程中运行
        import threading
        thread = threading.Thread(target=xtdata.run, daemon=True)
        thread.start()
    
    def process_realtime_quote(self, stock_code, quote):
        """处理实时行情"""
        price = quote.get('lastPrice')
        volume = quote.get('volume')
        
        # 判断交易信号
        signal = self.generate_signal_from_realtime(stock_code, price, volume)
        
        if signal:
            self.execute_signal(signal)
    
    def stop_realtime_subscription(self):
        """停止实时行情订阅"""
        if self.subscription_seq:
            xtdata.unsubscribe_quote(self.subscription_seq)
```

---

## 在策略中集成 xtquant 实时行情

### 方案1：在策略执行时获取实时行情

修改策略的 `_fetch_stock_data()` 方法：

```python
def _fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
    """获取股票数据（历史 + 实时）"""
    try:
        # 1. 获取历史数据
        from ..core.unified_data_access_impl import UnifiedDataAccess
        data_access = UnifiedDataAccess()
        df = data_access.get_stock_data(symbol, period="3mo")
        
        # 2. 从 xtquant 获取实时行情
        try:
            import xtquant.xtdata as xtdata
            
            # 获取最新tick数据
            tick_data = xtdata.get_market_data(
                field_list=["time", "lastPrice", "volume"],
                stock_list=[symbol],
                period="tick",
                count=1
            )
            
            if tick_data and "lastPrice" in tick_data:
                latest_price = tick_data["lastPrice"].iloc[0, 0]
                latest_time = tick_data["time"].iloc[0, 0]
                
                # 更新最新价格
                if len(df) > 0:
                    df.iloc[-1, df.columns.get_loc("close")] = latest_price
                    
        except Exception as e:
            self.logger.warning(f"获取xtquant实时行情失败: {e}")
        
        return df
        
    except Exception as e:
        self.logger.error(f"获取股票数据失败: {e}", exc_info=True)
        return None
```

### 方案2：订阅实时行情，事件驱动执行

创建实时行情订阅服务：

```python
# backend/infra/realtime_quote_subscriber.py
import xtquant.xtdata as xtdata
import threading
from typing import Dict, Callable, List

class RealtimeQuoteSubscriber:
    """实时行情订阅服务"""
    
    def __init__(self):
        self.subscriptions: Dict[int, List[str]] = {}  # seq -> stocks
        self.callbacks: Dict[str, List[Callable]] = {}  # stock -> callbacks
        self.running = False
        self.thread: Optional[threading.Thread] = None
    
    def subscribe(self, stocks: List[str], callback: Callable):
        """订阅股票实时行情"""
        for stock in stocks:
            if stock not in self.callbacks:
                self.callbacks[stock] = []
            self.callbacks[stock].append(callback)
        
        # 订阅全推行情
        seq = xtdata.subscribe_whole_quote(
            code_list=stocks,
            callback=self._on_quote
        )
        
        if seq > 0:
            self.subscriptions[seq] = stocks
            return seq
        return None
    
    def _on_quote(self, datas: Dict):
        """行情回调"""
        for stock_code, quote in datas.items():
            if stock_code in self.callbacks:
                for callback in self.callbacks[stock_code]:
                    try:
                        callback(stock_code, quote)
                    except Exception as e:
                        import logging
                        logging.error(f"行情回调执行失败: {e}", exc_info=True)
    
    def start(self):
        """启动订阅服务"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
    
    def _run(self):
        """运行订阅循环"""
        try:
            xtdata.run()  # 阻塞运行
        except Exception as e:
            import logging
            logging.error(f"实时行情订阅服务异常: {e}", exc_info=True)
        finally:
            self.running = False
    
    def unsubscribe(self, seq: int):
        """取消订阅"""
        if seq in self.subscriptions:
            xtdata.unsubscribe_quote(seq)
            del self.subscriptions[seq]
```

---

## 注意事项

1. **连接状态**: 确保 miniQMT 已启动并登录
2. **订阅数量**: 单股订阅数量不宜过多（建议 < 100）
3. **回调性能**: 回调函数应快速执行，避免阻塞
4. **线程安全**: 多线程环境下注意数据同步
5. **错误处理**: 订阅失败返回 -1，需要处理异常情况

---

## 优势

使用 xtquant 获取实时行情的优势：

1. ✅ **低延迟**: 直接从 miniQMT 获取，延迟最低
2. ✅ **数据完整**: 支持 Level1 和 Level2 数据
3. ✅ **实时推送**: 支持回调推送，无需轮询
4. ✅ **全市场支持**: 可以订阅全市场行情
5. ✅ **稳定可靠**: miniQMT 提供稳定的行情源

---

## 总结

**是的，可以从 miniQMT 获取实时行情！**

- ✅ 支持订阅实时行情（回调推送）
- ✅ 支持主动获取实时行情（从缓存）
- ✅ 支持全推行情（推荐）
- ✅ 支持 Level2 行情（需要权限）
- ✅ 数据延迟低，稳定可靠

建议在策略中使用 `subscribe_whole_quote()` 订阅全推行情，通过回调函数实时处理行情数据，触发策略执行。


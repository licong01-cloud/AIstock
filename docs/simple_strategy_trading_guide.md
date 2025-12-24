# 简单策略交易实现指南

## 1. 问题分析

### 1.1 简单策略的特点

对于**趋势跟踪、网格交易、双均线策略**等简单策略：
- ✅ **逻辑简单**：规则明确，易于实现
- ✅ **计算快速**：不需要复杂的AI推理
- ✅ **信号明确**：直接根据技术指标计算买卖点
- ✅ **执行频繁**：可能需要高频执行（如网格交易）

### 1.2 是否需要完整框架？

**答案：不需要完整框架，但需要基础封装**

对于简单策略，**不需要**：
- ❌ 复杂的交易意图数据结构
- ❌ 策略管理UI
- ❌ 复杂的信号追溯系统

**但需要**：
- ✅ **基本的执行封装**（避免策略代码直接调用xtquant）
- ✅ **基础风控**（资金检查、持仓检查）
- ✅ **幂等性保护**（防止重复下单）
- ✅ **错误处理**（下单失败时的处理）

## 2. 轻量级实现方案

### 2.1 简化版策略执行接口

提供一个**轻量级的策略执行接口**，简单策略可以直接调用：

```python
# backend/infra/strategy_executor.py

class SimpleStrategyExecutor:
    """简单策略执行器 - 轻量级版本"""
    
    def __init__(self, qmt_client: BaseQMTClient):
        self.qmt_client = qmt_client
        self._lock = threading.RLock()  # 串行执行锁
        self._order_cache: Dict[str, str] = {}  # 幂等性缓存：{key: order_id}
    
    def execute_signal(
        self,
        strategy_id: str,
        symbol: str,
        side: str,  # "BUY" / "SELL"
        quantity: int,
        price_type: str = "LIMIT",  # "LIMIT" / "MARKET"
        price: float = 0.0,
        reason: str = "",
        idempotency_key: str = None,  # 幂等键（可选）
    ) -> Dict[str, Any]:
        """执行交易信号（简化版）
        
        Args:
            strategy_id: 策略ID，如 "ma_cross_001"
            symbol: 股票代码，如 "600519.SH"
            side: 买卖方向
            quantity: 数量
            price_type: 价格类型
            price: 价格（限价单时使用）
            reason: 交易原因
            idempotency_key: 幂等键（格式：{strategy_id}:{date}:{symbol}:{side}）
        
        Returns:
            {
                "success": bool,
                "order_id": int,  # xtquant返回的订单编号
                "message": str,
            }
        """
        with self._lock:
            # 1. 幂等性检查
            if idempotency_key:
                if idempotency_key in self._order_cache:
                    return {
                        "success": True,
                        "order_id": self._order_cache[idempotency_key],
                        "message": "已存在相同订单（幂等保护）",
                    }
            
            # 2. 基础风控检查
            account_info = self.qmt_client.get_account_info()
            if side == "BUY":
                required_cash = quantity * (price if price > 0 else account_info.get("market_value", 0) / quantity)
                if account_info.get("available_cash", 0) < required_cash:
                    return {
                        "success": False,
                        "order_id": -1,
                        "message": f"资金不足，需要 {required_cash:.2f}，可用 {account_info.get('available_cash', 0):.2f}",
                    }
            elif side == "SELL":
                positions = self.qmt_client.get_positions()
                pos = next((p for p in positions if p["stock_code"] == symbol), None)
                if not pos or pos["can_sell"] < quantity:
                    return {
                        "success": False,
                        "order_id": -1,
                        "message": f"持仓不足，可卖 {pos['can_sell'] if pos else 0}，需要 {quantity}",
                    }
            
            # 3. 价格类型映射
            price_type_map = {
                "LIMIT": 11,  # FIX_PRICE
                "MARKET": 5,  # LATEST_PRICE
            }
            
            # 4. 下单
            order_id, msg = self.qmt_client.place_order(
                stock_code=symbol,
                order_type=23 if side == "BUY" else 24,
                order_volume=quantity,
                price_type=price_type_map.get(price_type, 11),
                price=price,
                strategy_name=strategy_id,
                order_remark=reason,
            )
            
            # 5. 缓存订单ID（幂等性）
            if idempotency_key and order_id > 0:
                self._order_cache[idempotency_key] = str(order_id)
            
            return {
                "success": order_id > 0,
                "order_id": order_id,
                "message": msg,
            }
```

### 2.2 简单策略示例

#### 示例1：双均线策略

```python
# backend/strategies/ma_cross_strategy.py

from backend.infra.strategy_executor import SimpleStrategyExecutor
from backend.infra.qmt_client import build_qmt_client_from_env
from backend.core.stock_data_impl import StockDataFetcher

class MACrossStrategy:
    """双均线交叉策略"""
    
    def __init__(self):
        self.executor = SimpleStrategyExecutor(build_qmt_client_from_env())
        self.data_fetcher = StockDataFetcher()
        self.strategy_id = "ma_cross_001"
    
    def run(self, symbol: str):
        """运行策略"""
        # 1. 获取数据
        df = self.data_fetcher.get_stock_data(symbol, period="3m")
        if df.empty:
            return
        
        # 2. 计算均线
        df["MA5"] = df["Close"].rolling(5).mean()
        df["MA20"] = df["Close"].rolling(20).mean()
        
        # 3. 判断信号
        current_ma5 = df["MA5"].iloc[-1]
        current_ma20 = df["MA20"].iloc[-1]
        prev_ma5 = df["MA5"].iloc[-2]
        prev_ma20 = df["MA20"].iloc[-2]
        current_price = df["Close"].iloc[-1]
        
        # 4. 金叉买入
        if prev_ma5 <= prev_ma20 and current_ma5 > current_ma20:
            result = self.executor.execute_signal(
                strategy_id=self.strategy_id,
                symbol=symbol,
                side="BUY",
                quantity=100,  # 固定100股
                price_type="LIMIT",
                price=current_price,
                reason=f"MA5上穿MA20，金叉买入信号",
                idempotency_key=f"{self.strategy_id}:{symbol}:BUY:{df.index[-1].date()}",
            )
            print(f"买入信号执行结果: {result}")
        
        # 5. 死叉卖出
        elif prev_ma5 >= prev_ma20 and current_ma5 < current_ma20:
            result = self.executor.execute_signal(
                strategy_id=self.strategy_id,
                symbol=symbol,
                side="SELL",
                quantity=100,
                price_type="LIMIT",
                price=current_price,
                reason=f"MA5下穿MA20，死叉卖出信号",
                idempotency_key=f"{self.strategy_id}:{symbol}:SELL:{df.index[-1].date()}",
            )
            print(f"卖出信号执行结果: {result}")
```

#### 示例2：网格交易策略

```python
# backend/strategies/grid_trading_strategy.py

class GridTradingStrategy:
    """网格交易策略"""
    
    def __init__(self, symbol: str, grid_size: float = 0.02):
        self.executor = SimpleStrategyExecutor(build_qmt_client_from_env())
        self.strategy_id = "grid_trading_001"
        self.symbol = symbol
        self.grid_size = grid_size  # 网格大小（2%）
        self.base_price = None  # 基准价格
        self.grid_levels = []  # 网格价位列表
    
    def initialize(self, base_price: float):
        """初始化网格"""
        self.base_price = base_price
        # 生成上下各5格的网格
        for i in range(-5, 6):
            level_price = base_price * (1 + i * self.grid_size)
            self.grid_levels.append({
                "level": i,
                "price": level_price,
                "filled": False,
            })
    
    def check_and_trade(self, current_price: float):
        """检查价格并执行交易"""
        if not self.base_price:
            self.initialize(current_price)
            return
        
        # 检查是否触发网格
        for grid in self.grid_levels:
            if grid["filled"]:
                continue
            
            # 买入网格：价格跌到网格价位
            if grid["level"] < 0 and current_price <= grid["price"]:
                result = self.executor.execute_signal(
                    strategy_id=self.strategy_id,
                    symbol=self.symbol,
                    side="BUY",
                    quantity=100,
                    price_type="LIMIT",
                    price=grid["price"],
                    reason=f"网格买入，价位 {grid['level']}",
                    idempotency_key=f"{self.strategy_id}:{self.symbol}:BUY:{grid['level']}",
                )
                if result["success"]:
                    grid["filled"] = True
                    print(f"网格买入成功: {result}")
            
            # 卖出网格：价格涨到网格价位
            elif grid["level"] > 0 and current_price >= grid["price"]:
                result = self.executor.execute_signal(
                    strategy_id=self.strategy_id,
                    symbol=self.symbol,
                    side="SELL",
                    quantity=100,
                    price_type="LIMIT",
                    price=grid["price"],
                    reason=f"网格卖出，价位 {grid['level']}",
                    idempotency_key=f"{self.strategy_id}:{self.symbol}:SELL:{grid['level']}",
                )
                if result["success"]:
                    grid["filled"] = True
                    print(f"网格卖出成功: {result}")
```

#### 示例3：趋势跟踪策略

```python
# backend/strategies/trend_following_strategy.py

class TrendFollowingStrategy:
    """趋势跟踪策略"""
    
    def __init__(self):
        self.executor = SimpleStrategyExecutor(build_qmt_client_from_env())
        self.strategy_id = "trend_following_001"
        self.data_fetcher = StockDataFetcher()
    
    def run(self, symbol: str):
        """运行策略"""
        # 1. 获取数据
        df = self.data_fetcher.get_stock_data(symbol, period="6m")
        if df.empty:
            return
        
        # 2. 计算趋势指标
        df["MA20"] = df["Close"].rolling(20).mean()
        df["MA60"] = df["Close"].rolling(60).mean()
        df["ATR"] = self._calculate_atr(df, 14)
        
        current_price = df["Close"].iloc[-1]
        ma20 = df["MA20"].iloc[-1]
        ma60 = df["MA60"].iloc[-1]
        atr = df["ATR"].iloc[-1]
        
        # 3. 判断趋势
        # 上升趋势：MA20 > MA60，且价格在MA20上方
        if ma20 > ma60 and current_price > ma20:
            # 计算买入数量（基于ATR）
            quantity = int(10000 / current_price)  # 固定金额买入
            result = self.executor.execute_signal(
                strategy_id=self.strategy_id,
                symbol=symbol,
                side="BUY",
                quantity=quantity,
                price_type="LIMIT",
                price=current_price,
                reason=f"上升趋势，MA20={ma20:.2f} > MA60={ma60:.2f}",
                idempotency_key=f"{self.strategy_id}:{symbol}:BUY:{df.index[-1].date()}",
            )
            print(f"趋势买入: {result}")
        
        # 下降趋势：MA20 < MA60，且价格在MA20下方
        elif ma20 < ma60 and current_price < ma20:
            # 检查持仓
            positions = self.executor.qmt_client.get_positions()
            pos = next((p for p in positions if p["stock_code"] == symbol), None)
            if pos and pos["can_sell"] > 0:
                result = self.executor.execute_signal(
                    strategy_id=self.strategy_id,
                    symbol=symbol,
                    side="SELL",
                    quantity=min(pos["can_sell"], 100),  # 卖出部分或全部
                    price_type="LIMIT",
                    price=current_price,
                    reason=f"下降趋势，MA20={ma20:.2f} < MA60={ma60:.2f}",
                    idempotency_key=f"{self.strategy_id}:{symbol}:SELL:{df.index[-1].date()}",
                )
                print(f"趋势卖出: {result}")
    
    def _calculate_atr(self, df, period=14):
        """计算ATR"""
        high_low = df["High"] - df["Low"]
        high_close = abs(df["High"] - df["Close"].shift())
        low_close = abs(df["Low"] - df["Close"].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(period).mean()
```

### 2.3 策略调度

简单策略可以通过现有的调度系统运行：

```python
# backend/strategies/scheduler.py

import schedule
import time
from backend.strategies.ma_cross_strategy import MACrossStrategy
from backend.strategies.grid_trading_strategy import GridTradingStrategy

def run_simple_strategies():
    """运行简单策略"""
    # 双均线策略（每天运行一次）
    ma_strategy = MACrossStrategy()
    ma_strategy.run("600519.SH")
    ma_strategy.run("000001.SZ")
    
    # 网格交易策略（每5分钟检查一次）
    grid_strategy = GridTradingStrategy("600519.SH")
    # 需要实时价格，可以通过订阅或定时查询获取
    current_price = get_current_price("600519.SH")
    grid_strategy.check_and_trade(current_price)

# 定时调度
schedule.every().day.at("09:35").do(run_simple_strategies)  # 开盘后5分钟
schedule.every(5).minutes.do(run_simple_strategies)  # 每5分钟

while True:
    schedule.run_pending()
    time.sleep(60)
```

## 3. 对比：轻量级 vs 完整框架

| 特性 | 轻量级版本 | 完整框架 |
|------|-----------|---------|
| **适用场景** | 简单策略（双均线、网格、趋势跟踪） | 复杂策略（AI分析、量化模型） |
| **代码复杂度** | 低（~200行） | 高（~2000行） |
| **数据库依赖** | 可选（幂等性缓存可用内存） | 必需（交易意图表） |
| **风控** | 基础（资金、持仓检查） | 完整（多维度风控） |
| **状态跟踪** | 简单（订单ID缓存） | 完整（意图状态机） |
| **UI支持** | 无 | 有（策略管理、意图列表） |
| **实现时间** | 1-2天 | 2-4周 |

## 4. 实现建议

### 4.1 分阶段实现

**阶段一：轻量级版本（推荐先实现）**
- ✅ 实现 `SimpleStrategyExecutor`
- ✅ 提供基础风控（资金、持仓检查）
- ✅ 内存级幂等性保护
- ✅ 简单策略示例（双均线、网格、趋势跟踪）

**阶段二：按需演进**
- 如果简单策略运行良好，可以逐步添加：
  - 数据库持久化（可选）
  - 更完善的风控（可选）
  - 策略管理UI（可选）

### 4.2 代码组织

```
backend/
├── infra/
│   ├── qmt_client.py          # QMT客户端（已有）
│   └── strategy_executor.py   # 简单策略执行器（新增）
├── strategies/                 # 策略目录（新增）
│   ├── __init__.py
│   ├── ma_cross_strategy.py   # 双均线策略
│   ├── grid_trading_strategy.py  # 网格交易策略
│   ├── trend_following_strategy.py  # 趋势跟踪策略
│   └── base_strategy.py       # 策略基类（可选）
└── routers/
    └── strategies.py           # 策略API路由（可选）
```

## 5. 总结

### 5.1 简单策略不需要完整框架 ✅

对于**趋势跟踪、网格交易、双均线策略**等简单策略：
- ✅ **可以直接使用轻量级版本**
- ✅ **实现简单**：只需 `SimpleStrategyExecutor` + 策略逻辑
- ✅ **快速上线**：1-2天即可实现

### 5.2 轻量级版本包含的核心功能

1. **执行封装**：统一调用QMT客户端
2. **基础风控**：资金、持仓检查
3. **幂等性保护**：防止重复下单
4. **错误处理**：下单失败时的处理

### 5.3 何时需要完整框架？

只有在以下情况才需要完整框架：
- 需要复杂的策略管理UI
- 需要详细的交易意图追溯
- 需要多维度风控（集中度、回撤等）
- 需要策略回测和性能分析

**对于简单策略，轻量级版本完全够用！**


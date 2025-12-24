# QMT 交易系统设计方案与规范

> **版本**: v1.2  
> **日期**: 2024-12-20  
> **目标**: 统一设计 miniQMT 连接、持仓管理、交易功能和轻量级策略交易系统  
> **状态**: ✅ 核心功能已实现（数据库、执行器、风控、策略基类、API、双均线策略、趋势跟踪策略）

---

## 1. 系统概述

### 1.1 目标

在 AIstock 中实现完整的 QMT（miniQMT）交易系统，包括：
- ✅ **连接管理**：与 miniQMT 客户端的连接和状态管理
- ✅ **账户查询**：资金、持仓、委托、成交查询
- ✅ **手动交易**：股票买入/卖出、撤单
- ✅ **策略交易**：轻量级策略自动执行（双均线、网格、趋势跟踪等）
- ✅ **高级功能**：新股申购、银证转账

### 1.2 设计原则

1. **分层架构**：策略层 → 执行层 → QMT层
2. **统一封装**：所有 xtquant 调用集中在 `qmt_client.py`
3. **串行执行**：所有交易操作串行执行，避免并发问题
4. **幂等性**：防止重复下单
5. **可扩展**：简单策略轻量级，复杂策略可演进到完整框架

---

## 2. 系统架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                     前端 UI 层                                │
│  - 持仓管理页面 (/qmt/positions)                             │
│  - 交易下单界面                                               │
│  - 策略管理界面（可选）                                        │
└───────────────────────┬─────────────────────────────────────┘
                         │ HTTP API
┌───────────────────────▼─────────────────────────────────────┐
│                   后端 API 层                                 │
│  - /api/v1/qmt/* (连接、查询、交易)                           │
│  - /api/v1/strategies/* (策略执行，可选)                       │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                   业务逻辑层                                   │
│  ┌──────────────────┐  ┌──────────────────┐                  │
│  │ 策略执行器        │  │ 风控服务         │                  │
│  │ (轻量级)         │  │ (基础风控)       │                  │
│  └──────────────────┘  └──────────────────┘                  │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                   QMT 客户端层                                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  qmt_client.py (统一封装)                            │   │
│  │  - XtQuantQMTClient (xtquant实现)                    │   │
│  │  - SimulatorQMTClient (模拟占位)                     │   │
│  └──────────────────────────────────────────────────────┘   │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                    xtquant 库                                 │
│  - xttrader (交易接口)                                        │
│  - xttype (数据类型)                                          │
│  - xtconstant (常量)                                          │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                  miniQMT 客户端                                │
│  (独立运行，需要先启动并登录)                                   │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件

#### 2.2.1 QMT 客户端 (`backend/infra/qmt_client.py`)

**职责**：
- 封装所有 xtquant 调用
- 管理连接状态
- 提供统一的查询和执行接口

**接口**：
```python
class BaseQMTClient:
    # 连接管理
    def connect() -> Tuple[bool, str]
    def disconnect() -> Tuple[bool, str]
    def status() -> QMTStatus
    
    # 查询接口
    def get_account_info() -> Dict[str, Any]
    def get_positions() -> List[Dict[str, Any]]
    def get_orders(cancelable_only: bool) -> List[Dict[str, Any]]
    def get_trades() -> List[Dict[str, Any]]
    
    # 交易接口
    def place_order(...) -> Tuple[int, str]
    def cancel_order(order_id: str) -> Tuple[bool, str]
    def cancel_order_by_sysid(market: int, order_sysid: str) -> Tuple[bool, str]
    
    # 高级功能
    def query_new_purchase_limit() -> Dict[str, Any]
    def query_ipo_data() -> List[Dict[str, Any]]
    def bank_transfer_in(...) -> Tuple[bool, str]
    def bank_transfer_out(...) -> Tuple[bool, str]
    def query_bank_info() -> List[Dict[str, Any]]
```

#### 2.2.2 策略执行器 (`backend/infra/strategy_executor.py`) - 新增

**职责**：
- 为简单策略提供轻量级执行接口
- 基础风控检查
- 幂等性保护

**接口**：
```python
class SimpleStrategyExecutor:
    def execute_signal(
        strategy_id: str,
        symbol: str,
        side: str,  # "BUY" / "SELL"
        quantity: int,
        price_type: str = "LIMIT",  # "LIMIT" / "MARKET"
        price: float = 0.0,
        reason: str = "",
        idempotency_key: str = None,
    ) -> Dict[str, Any]
```

#### 2.2.3 风控服务 (`backend/infra/risk_control.py`) - 新增

**职责**：
- 基础风控检查（资金、持仓）
- 可扩展为完整风控系统

**接口**：
```python
class RiskControlService:
    def check_buy_signal(
        symbol: str,
        quantity: int,
        price: float,
        account_info: Dict[str, Any]
    ) -> Tuple[bool, str]  # (passed, reason)
    
    def check_sell_signal(
        symbol: str,
        quantity: int,
        positions: List[Dict[str, Any]]
    ) -> Tuple[bool, str]
```

---

## 3. 数据模型

### 3.1 QMT 状态 (`QMTStatus`)

```python
@dataclass
class QMTStatus:
    enabled: bool  # 是否启用
    connected: bool  # 是否已连接
    mode: str  # "SIM" / "LIVE"
    account_id: Optional[str]  # 资金账号
    provider: str  # "xtquant" / "simulator"
    userdata_path: Optional[str]  # userdata_mini 路径
    session_id: Optional[int]  # 会话ID
    last_error: Optional[str]  # 最后错误
```

### 3.2 账户信息 (`AccountInfo`)

```python
{
    "provider": "xtquant",
    "connected": True,
    "mode": "SIM",
    "account_id": "62266303",
    "available_cash": 20415385.78,  # 可用资金
    "total_asset": 21017635.78,     # 总资产
    "market_value": 602250.0,       # 持仓市值
    "frozen_cash": 0.0,              # 冻结资金
    "fetch_balance": 20415385.78,   # 可取资金
}
```

### 3.3 持仓信息 (`Position`)

```python
{
    "stock_code": "688981.SH",
    "stock_name": "中芯国际",
    "quantity": 5000,              # 持仓数量
    "can_sell": 5000,              # 可卖数量
    "open_price": 116.922844,      # 开仓价
    "cost_price": 116.922844,      # 成本价
    "current_price": 120.45,       # 当前价
    "market_value": 602250.0,       # 市值
    "float_profit": 34100.0,       # 浮动盈亏
    "profit_rate": 0.0301,         # 盈亏率
    "secu_account": "A307647174",  # 股东账户
}
```

### 3.4 委托信息 (`Order`)

```python
{
    "order_id": "123456",           # 订单编号
    "order_sysid": "789012",        # 柜台合同编号
    "stock_code": "600519.SH",
    "stock_name": "贵州茅台",
    "order_time": "20241220103000", # 委托时间
    "order_type": 23,               # 23=买入，24=卖出
    "order_type_name": "买入",
    "order_volume": 100,            # 委托数量
    "price_type": 11,               # 11=限价，5=最新价
    "price": 1800.0,                # 委托价
    "traded_volume": 0,             # 成交量
    "traded_price": 0.0,            # 成交价
    "order_status": 50,             # 委托状态
    "status_msg": "已报",           # 状态描述
    "strategy_name": "ma_cross_001", # 策略名称
    "order_remark": "MA5上穿MA20",  # 委托备注
    "secu_account": "A307647174",
}
```

### 3.5 成交信息 (`Trade`)

```python
{
    "traded_id": "345678",
    "stock_code": "600519.SH",
    "stock_name": "贵州茅台",
    "order_type": 23,
    "order_type_name": "买入",
    "traded_time": "20241220103015",
    "traded_price": 1800.0,
    "traded_volume": 100,
    "traded_amount": 180000.0,
    "order_id": "123456",
    "order_sysid": "789012",
    "commission": 5.0,              # 手续费
    "strategy_name": "ma_cross_001",
    "order_remark": "MA5上穿MA20",
    "secu_account": "A307647174",
}
```

### 3.6 交易信号 (`TradeSignal`) - 策略使用

```python
@dataclass
class TradeSignal:
    """策略生成的交易信号（轻量级版本）"""
    strategy_id: str  # 策略ID，如 "ma_cross_001"
    symbol: str  # 股票代码，如 "600519.SH"
    side: str  # "BUY" / "SELL"
    quantity: int  # 数量（股）
    price_type: str  # "LIMIT" / "MARKET"
    price: float  # 价格（限价单时使用）
    reason: str  # 交易原因
    idempotency_key: str  # 幂等键（格式：{strategy_id}:{date}:{symbol}:{side}）
    signal_data: Dict[str, Any]  # 原始信号数据（可选）
```

---

## 4. API 接口规范

### 4.1 QMT 基础接口

#### 4.1.1 连接管理

```
GET  /api/v1/qmt/status
     返回：QMTStatus

POST /api/v1/qmt/connect
     返回：{"success": bool, "message": str, "status": QMTStatus}

POST /api/v1/qmt/disconnect
     返回：{"success": bool, "message": str, "status": QMTStatus}

POST /api/v1/qmt/reload
     重新加载配置
     返回：{"ok": bool, "status": QMTStatus}
```

#### 4.1.2 查询接口

```
GET  /api/v1/qmt/account
     返回：AccountInfo

GET  /api/v1/qmt/positions
     返回：List[Position]

GET  /api/v1/qmt/orders?cancelable_only=false
     返回：List[Order]

GET  /api/v1/qmt/trades
     返回：List[Trade]

GET  /api/v1/qmt/snapshot
     返回：{"status": QMTStatus, "account": AccountInfo, "positions": List[Position]}
```

#### 4.1.3 交易接口

```
POST /api/v1/qmt/order
     请求体：
     {
         "stock_code": "600519.SH",
         "order_type": 23,  # 23=买入，24=卖出
         "order_volume": 100,
         "price_type": 11,  # 11=限价，5=最新价
         "price": 1800.0,
         "strategy_name": "",  # 可选
         "order_remark": ""   # 可选
     }
     返回：{"success": bool, "order_id": int, "message": str}

POST /api/v1/qmt/cancel
     请求体：
     {
         "order_id": "123456"  # 或
         // "market": 0, "order_sysid": "789012"
     }
     返回：{"success": bool, "message": str}

POST /api/v1/qmt/order/batch
     批量下单
     请求体：
     {
         "orders": [
             {"stock_code": "...", "order_type": 23, ...},
             ...
         ]
     }
     返回：{"success": bool, "total": int, "succeeded": int, "failed": int, "results": [...]}
```

#### 4.1.4 高级功能接口

```
GET  /api/v1/qmt/ipo/limit
     查询新股申购额度
     返回：{"account_id": str, "market": int, "purchase_limit": float}

GET  /api/v1/qmt/ipo/list
     查询新股信息
     返回：List[{"stock_code": str, "stock_name": str, "issue_price": float, ...}]

POST /api/v1/qmt/bank/transfer-in
     银行转证券
     请求体：{"bank_no": str, "bank_account": str, "bank_pwd": str, "amount": float}

POST /api/v1/qmt/bank/transfer-out
     证券转银行
     请求体：同上

GET  /api/v1/qmt/bank/info
     查询银行信息
     返回：List[{"bank_no": str, "bank_account": str, "bank_name": str, ...}]
```

### 4.2 策略交易接口（轻量级版本）

```
POST /api/v1/strategies/execute
     执行交易信号（策略调用）
     请求体：
     {
         "strategy_id": "ma_cross_001",
         "symbol": "600519.SH",
         "side": "BUY",
         "quantity": 100,
         "price_type": "LIMIT",  # "LIMIT" / "MARKET"
         "price": 1800.0,
         "reason": "MA5上穿MA20",
         "idempotency_key": "ma_cross:600519.SH:BUY:2024-12-20"  # 可选
     }
     返回：{"success": bool, "order_id": int, "message": str}

GET  /api/v1/strategies/list
     获取策略列表（可选）
     返回：List[{"strategy_id": str, "name": str, "enabled": bool, ...}]
```

---

## 5. 配置管理

### 5.1 环境变量配置

```env
# ========== MiniQMT量化交易配置 ==========
MINIQMT_ENABLED="true"                    # 是否启用
MINIQMT_ACCOUNT_ID="62266303"             # 资金账号
MINIQMT_MODE="SIM"                        # SIM=模拟盘，LIVE=实盘
MINIQMT_USERDATA_PATH="F:/国金QMT交易端模拟/userdata_mini"  # userdata_mini路径（必填）
MINIQMT_SESSION_ID="123456"                # 会话ID（建议固定）
MINIQMT_XTQUANT_DIR=""                    # xtquant目录（可选，默认使用项目内xtquant）
MINIQMT_HOST="127.0.0.1"                  # 兼容字段（当前不使用）
MINIQMT_PORT="58610"                      # 兼容字段（当前不使用）
```

### 5.2 配置优先级

1. 环境变量（`.env` 文件）
2. 前端配置页面（通过 `/api/v1/config/env` 保存）
3. 默认值（代码中定义）

---

## 6. 轻量级策略执行器设计

### 6.1 核心功能

**SimpleStrategyExecutor** 提供：
1. **统一执行接口**：策略只需调用 `execute_signal()`
2. **基础风控**：资金检查、持仓检查
3. **幂等性保护**：通过 `idempotency_key` 防止重复下单
4. **错误处理**：统一的错误处理和返回格式

### 6.2 实现细节

```python
class SimpleStrategyExecutor:
    def __init__(self, qmt_client: BaseQMTClient, db_conn=None):
        self.qmt_client = qmt_client
        self.db_conn = db_conn
        self._lock = threading.RLock()  # 串行执行锁
    
    def execute_signal(self, ...) -> Dict[str, Any]:
        with self._lock:
            # 1. 幂等性检查（查询数据库）
            # 2. 创建交易意图记录（数据库）
            # 3. 风控检查
            # 4. 调用 qmt_client.place_order
            # 5. 更新交易意图记录（订单ID、状态）
            # 6. 返回结果
```

### 6.3 风控规则（基础版）

1. **买入检查**：
   - 可用资金 >= 委托金额（数量 × 价格）
   - 价格 > 0（限价单）

2. **卖出检查**：
   - 持仓中存在该股票
   - 可卖数量 >= 委托数量

3. **通用检查**：
   - 数量 > 0
   - 股票代码格式正确

---

## 7. 策略实现规范

### 7.1 策略基类（可选）

```python
# backend/strategies/base_strategy.py

class BaseStrategy:
    """策略基类"""
    
    def __init__(self, strategy_id: str, executor: SimpleStrategyExecutor):
        self.strategy_id = strategy_id
        self.executor = executor
    
    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略（子类实现）"""
        raise NotImplementedError
    
    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """生成交易信号（子类实现）"""
        raise NotImplementedError
```

### 7.2 策略实现示例

#### 双均线策略

```python
class MACrossStrategy(BaseStrategy):
    """双均线交叉策略"""
    
    def __init__(self):
        executor = SimpleStrategyExecutor(build_qmt_client_from_env())
        super().__init__("ma_cross_001", executor)
        self.data_fetcher = StockDataFetcher()
    
    def run(self, symbol: str):
        # 1. 获取数据
        # 2. 计算均线
        # 3. 判断信号
        # 4. 执行交易
        signal = self.generate_signal(data)
        if signal:
            return self.executor.execute_signal(...)
```

### 7.3 策略调度

策略可以通过以下方式运行：
1. **定时任务**：使用 `schedule` 库或 APScheduler
2. **API 触发**：通过 `/api/v1/strategies/execute` 手动触发
3. **事件驱动**：响应市场数据变化（未来扩展）

---

## 8. 错误处理与日志

### 8.1 错误分类

1. **连接错误**：miniQMT 未启动、未登录、连接断开
2. **参数错误**：股票代码格式错误、数量/价格无效
3. **风控错误**：资金不足、持仓不足
4. **执行错误**：下单失败、撤单失败

### 8.2 错误处理策略

- **连接错误**：返回明确错误信息，提示用户检查 miniQMT
- **参数错误**：返回 400 Bad Request，包含详细错误信息
- **风控错误**：返回 400，说明风控失败原因
- **执行错误**：返回 500，记录详细日志

### 8.3 日志记录

- 所有交易操作记录日志（策略ID、股票代码、数量、价格等）
- 错误信息记录详细堆栈
- 关键操作记录到数据库（可选）

---

## 9. 安全考虑

### 9.1 模拟盘优先

- 初期仅在模拟盘实现和测试
- 实盘功能需要额外审核和风控

### 9.2 风控保护

- 基础风控：资金、持仓检查
- 幂等性：防止重复下单
- 串行执行：避免并发问题

### 9.3 操作确认

- 重要操作（下单、撤单）需要用户确认（前端实现）
- 策略自动交易需要明确启用标志

---

## 10. 实现步骤

### 阶段一：基础功能（已完成 ✅）

- [x] QMT 客户端封装 (`qmt_client.py`)
- [x] 连接管理接口
- [x] 查询接口（资金、持仓、委托、成交）
- [x] 交易接口（下单、撤单）
- [x] 前端持仓管理页面

### 阶段二：数据库与核心组件（待实现）

- [ ] 数据库表结构（策略配置、交易意图、策略执行记录）
- [ ] `SimpleStrategyExecutor` 类
- [ ] `RiskControlService` 类（基础风控）
- [ ] 策略基类 `BaseStrategy`
- [ ] 策略执行 API (`/api/v1/strategies/execute`)

### 阶段三：策略实现（待实现）

- [ ] 双均线策略 (`MACrossStrategy`)
- [ ] 趋势跟踪策略 (`TrendFollowingStrategy`)
- [ ] 策略调度器（使用 `schedule` 库）

### 阶段四：策略管理（待实现）

- [ ] 策略管理 API（CRUD）
- [ ] 策略管理 UI（前端页面）
- [ ] 策略执行记录查询

### 阶段五：增强功能（未来）

- [ ] 事件驱动策略执行
- [ ] 策略回测功能
- [ ] 高级风控（集中度、回撤等）
- [ ] 策略性能统计
- [ ] RDagent 多因子策略集成

---

## 11. 文件结构

```
backend/
├── infra/
│   ├── qmt_client.py              # QMT客户端（已有）
│   ├── strategy_executor.py       # 策略执行器（新增）
│   └── risk_control.py            # 风控服务（新增）
├── routers/
│   ├── qmt.py                     # QMT API路由（已有）
│   └── strategies.py              # 策略API路由（新增）
├── strategies/                    # 策略目录（新增）
│   ├── __init__.py
│   ├── base_strategy.py           # 策略基类（新增）
│   ├── ma_cross_strategy.py       # 双均线策略（新增）
│   └── trend_following_strategy.py # 趋势跟踪策略（新增）
├── schedulers/
│   └── strategy_scheduler.py      # 策略调度器（新增，使用schedule库）
└── db/
    └── init_qmt_schema.py         # QMT相关数据库表（新增）

frontend/
└── src/app/qmt/
    ├── positions/
    │   └── page.tsx               # 持仓管理页面（已有）
    └── strategies/
        └── page.tsx               # 策略管理页面（新增）
```

---

## 12. 接口规范细节

### 12.1 股票代码格式

- **统一格式**：`{code}.{market}`
  - 上海：`600519.SH`
  - 深圳：`000001.SZ`
- **自动转换**：前端/策略可以输入 `600519`，后端自动转换为 `600519.SH`

### 12.2 价格类型映射

| 前端/策略 | xtquant 常量值 | 说明 |
|----------|---------------|------|
| `LIMIT` / `FIX_PRICE` | 11 | 限价 |
| `MARKET` / `LATEST_PRICE` | 5 | 最新价 |
| `MARKET_PEER_PRICE_FIRST` | 44 | 对手方最优 |
| `MARKET_MINE_PRICE_FIRST` | 45 | 本方最优 |

### 12.3 委托类型

| 值 | 说明 |
|---|------|
| 23 | 买入 (`STOCK_BUY`) |
| 24 | 卖出 (`STOCK_SELL`) |

### 12.4 委托状态（参考）

| 值 | 说明 |
|---|------|
| 48 | 未报 |
| 49 | 待报 |
| 50 | 已报 |
| 51 | 已报待撤 |
| 52 | 部成待撤 |
| 53 | 部撤 |
| 54 | 已撤 |
| 55 | 部成 |
| 56 | 已成 |
| 57 | 废单 |

---

## 13. 测试规范

### 13.1 单元测试

- QMT 客户端方法测试
- 策略执行器测试
- 风控服务测试

### 13.2 集成测试

- 端到端交易流程测试
- 策略执行流程测试

### 13.3 模拟盘测试

- 所有功能先在模拟盘测试
- 验证资金、持仓、委托、成交查询
- 验证下单、撤单功能

---

## 14. 已确认事项 ✅

### 14.1 设计确认

- ✅ **调度库**：使用 `schedule` 库（与现有程序一致）
- ✅ **策略执行方式**：
  - API 触发：通过 `/api/v1/strategies/execute` 手动触发
  - 定时任务：使用 `schedule` 库（与 `tdx_scheduler.py` 一致）
  - 事件驱动：未来实现
- ✅ **策略优先级**：优先实现双均线策略和趋势跟踪策略
- ✅ **策略管理UI**：需要实现
- ✅ **数据库持久化**：需要持久化交易意图
- ✅ **策略配置管理**：策略配置固化到数据库
- ✅ **未来演进**：需要演进到完整框架（RDagent 多因子策略）

### 14.2 策略基类说明

**策略基类（BaseStrategy）的价值**：

1. **统一接口**：所有策略实现相同的接口（`run()`, `generate_signal()`），便于管理和调用
2. **代码复用**：公共逻辑（数据获取、日志记录、错误处理）可以在基类中实现
3. **类型安全**：明确的接口定义，IDE 和类型检查工具可以提供更好的支持
4. **扩展性**：未来添加新功能（如性能统计、回测）时，只需在基类中添加，所有策略自动继承
5. **测试友好**：可以创建 Mock 策略进行单元测试

**实现建议**：
- 初期：实现策略基类，但保持简单（仅定义接口）
- 后期：逐步将公共逻辑移到基类

### 14.3 策略配置管理方案

**方案**：策略配置固化到数据库，便于：
- 动态修改策略参数（无需重启）
- 版本管理和回滚
- 多策略实例管理
- 策略启用/禁用控制

**数据库表设计**：见第 15 章

---

## 15. 数据库设计

### 15.1 数据库表结构

所有表放在 `trading` schema 下，为交易策略单独创建 schema，与量化模型表（`app` schema）分离。

#### 15.1.1 策略配置表 (`trading.strategy_config`)

存储策略的配置信息，支持动态修改参数。

```sql
CREATE TABLE IF NOT EXISTS app.strategy_config (
    id                  BIGSERIAL PRIMARY KEY,
    strategy_id         TEXT NOT NULL UNIQUE,  -- 策略ID，如 "ma_cross_001"
    strategy_name       TEXT NOT NULL,         -- 策略名称
    strategy_type       TEXT NOT NULL,         -- 策略类型：MA_CROSS, TREND_FOLLOWING, GRID, etc.
    description         TEXT,                 -- 策略描述
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,  -- 是否启用
    config_json         JSONB NOT NULL,       -- 策略参数配置（JSON格式）
    schedule_config     JSONB,                -- 调度配置（cron表达式或频率）
    risk_config         JSONB,                 -- 风控配置（可选）
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by          TEXT,
    updated_by          TEXT
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_strategy_config_strategy_id ON trading.strategy_config(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_config_enabled ON trading.strategy_config(enabled);
CREATE INDEX IF NOT EXISTS idx_strategy_config_type ON trading.strategy_config(strategy_type);
```

**config_json 示例（双均线策略）**：
```json
{
    "ma_short": 5,
    "ma_long": 20,
    "symbols": ["600519.SH", "000001.SZ"],
    "position_size": 0.1,
    "price_type": "LIMIT",
    "min_price_change": 0.01
}
```

**schedule_config 示例**：
```json
{
    "type": "daily",  // daily, hourly, minute, cron
    "time": "09:30",  // 执行时间
    "timezone": "Asia/Shanghai"
}
```

#### 15.1.2 交易意图表 (`trading.trade_intent`)

存储策略生成的交易意图，用于幂等性保护和审计。

```sql
CREATE TABLE IF NOT EXISTS app.trade_intent (
    id                  BIGSERIAL PRIMARY KEY,
    strategy_id         TEXT NOT NULL,        -- 策略ID
    symbol              TEXT NOT NULL,        -- 股票代码
    side                TEXT NOT NULL,        -- BUY / SELL
    quantity            INTEGER NOT NULL,     -- 数量（股）
    price_type          TEXT NOT NULL,        -- LIMIT / MARKET
    price               NUMERIC(12, 4),       -- 价格（限价单）
    reason              TEXT,                 -- 交易原因
    idempotency_key     TEXT NOT NULL UNIQUE, -- 幂等键
    status              TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, EXECUTING, EXECUTED, FAILED, CANCELLED
    order_id            TEXT,                 -- QMT订单ID（执行后填充）
    order_sysid         TEXT,                 -- QMT系统订单号
    error_message       TEXT,                 -- 错误信息
    risk_check_passed   BOOLEAN,              -- 风控检查是否通过
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_at         TIMESTAMPTZ,          -- 执行时间
    signal_data         JSONB,                -- 原始信号数据（可选）
    CONSTRAINT chk_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'EXECUTING', 'EXECUTED', 'FAILED', 'CANCELLED'))
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_trade_intent_strategy_id ON trading.trade_intent(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trade_intent_symbol ON trading.trade_intent(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_intent_status ON trading.trade_intent(status);
CREATE INDEX IF NOT EXISTS idx_trade_intent_idempotency_key ON trading.trade_intent(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_trade_intent_created_at ON trading.trade_intent(created_at);
```

**idempotency_key 格式**：
```
{strategy_id}:{date}:{symbol}:{side}:{hash}
例如：ma_cross_001:2024-12-20:600519.SH:BUY:a1b2c3d4
```

#### 15.1.3 策略执行记录表 (`trading.strategy_execution`)

记录策略每次执行的详细信息。

```sql
CREATE TABLE IF NOT EXISTS app.strategy_execution (
    id                  BIGSERIAL PRIMARY KEY,
    strategy_id         TEXT NOT NULL,        -- 策略ID
    execution_type      TEXT NOT NULL,        -- SCHEDULED, MANUAL, EVENT
    trigger_source      TEXT,                 -- 触发来源（scheduler, api, event）
    status              TEXT NOT NULL,        -- RUNNING, SUCCESS, FAILED
    start_time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time            TIMESTAMPTZ,
    duration_seconds    DOUBLE PRECISION,
    symbols_processed    INTEGER DEFAULT 0,   -- 处理的股票数量
    signals_generated   INTEGER DEFAULT 0,    -- 生成的信号数量
    signals_executed    INTEGER DEFAULT 0,    -- 执行的信号数量
    error_message       TEXT,
    execution_log       TEXT,                 -- 执行日志（可选）
    metrics_json        JSONB,                -- 执行指标（JSON格式）
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_strategy_execution_strategy_id ON trading.strategy_execution(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_execution_status ON trading.strategy_execution(status);
CREATE INDEX IF NOT EXISTS idx_strategy_execution_start_time ON trading.strategy_execution(start_time);
```

#### 15.1.4 策略性能统计表 (`trading.strategy_performance`) - 可选

用于记录策略的绩效指标（未来扩展）。

```sql
CREATE TABLE IF NOT EXISTS app.strategy_performance (
    id                  BIGSERIAL PRIMARY KEY,
    strategy_id         TEXT NOT NULL,
    date                DATE NOT NULL,        -- 统计日期
    total_trades        INTEGER DEFAULT 0,     -- 总交易次数
    win_trades          INTEGER DEFAULT 0,     -- 盈利交易次数
    loss_trades         INTEGER DEFAULT 0,     -- 亏损交易次数
    total_profit        NUMERIC(12, 2) DEFAULT 0,  -- 总盈亏
    total_return        NUMERIC(8, 4) DEFAULT 0,    -- 总收益率
    max_drawdown        NUMERIC(8, 4) DEFAULT 0,    -- 最大回撤
    sharpe_ratio        NUMERIC(8, 4),         -- 夏普比率
    metrics_json        JSONB,                -- 其他指标
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(strategy_id, date)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_strategy_performance_strategy_id ON trading.strategy_performance(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_performance_date ON trading.strategy_performance(date);
```

### 15.2 数据库初始化脚本

已创建 `backend/db/init_trading_schema.py`：

```python
"""初始化交易策略相关表（QMT 交易系统专用）.

本脚本创建以下表，全部放在 trading schema 下：
- trading.strategy_config
- trading.trade_intent
- trading.strategy_execution
- trading.strategy_performance
"""

from typing import List
from dotenv import load_dotenv
from .pg_pool import get_conn

load_dotenv(override=True)

DDL: List[str] = [
    "CREATE SCHEMA IF NOT EXISTS trading",
    # ... 上述 SQL 语句 ...
]

def init_trading_schema():
    """初始化交易策略相关数据库表"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()
```

**执行方式**：
```bash
# 方式1：直接运行脚本
python -m backend.db.init_trading_schema

# 方式2：在 Python 中调用
from backend.db.init_trading_schema import init_trading_schema
init_trading_schema()
```

### 15.3 数据访问层（DAO）

建议创建 `backend/db/trading_dao.py` 提供数据访问接口：

```python
class StrategyConfigDAO:
    def get_by_strategy_id(strategy_id: str) -> Optional[Dict]
    def create(config: Dict) -> int
    def update(strategy_id: str, updates: Dict) -> bool
    def list_all(enabled_only: bool = False) -> List[Dict]

class TradeIntentDAO:
    def create(intent: Dict) -> int
    def get_by_idempotency_key(key: str) -> Optional[Dict]
    def update_status(intent_id: int, status: str, order_id: str = None) -> bool
    def list_by_strategy(strategy_id: str, limit: int = 100) -> List[Dict]

class StrategyExecutionDAO:
    def create(execution: Dict) -> int
    def update_status(execution_id: int, status: str, **kwargs) -> bool
    def get_latest_by_strategy(strategy_id: str) -> Optional[Dict]
```

**注意**：所有 SQL 查询需要使用 `trading.` schema 前缀。

---

## 16. 策略管理方案

### 16.1 策略配置管理流程

1. **创建策略配置**：
   - 通过 API 或 UI 创建策略配置
   - 配置存储在 `trading.strategy_config` 表
   - 支持 JSON 格式的灵活参数配置

2. **启用/禁用策略**：
   - 修改 `enabled` 字段
   - 调度器自动识别并启动/停止策略

3. **修改策略参数**：
   - 更新 `config_json` 字段
   - 策略下次执行时自动加载新配置
   - 支持版本管理（可选）

### 16.2 策略执行流程

1. **策略调度**：
   - 调度器读取 `trading.strategy_config` 中 `enabled=true` 的策略
   - 根据 `schedule_config` 执行策略

2. **信号生成**：
   - 策略运行，生成交易信号
   - 创建 `trading.trade_intent` 记录（状态：PENDING）

3. **信号执行**：
   - `SimpleStrategyExecutor` 读取 PENDING 状态的交易意图
   - 执行风控检查
   - 调用 QMT 下单
   - 更新 `trading.trade_intent` 状态和订单ID

4. **执行记录**：
   - 记录到 `trading.strategy_execution` 表
   - 包含执行时间、处理的股票数、生成的信号数等

### 16.3 幂等性保护

- **幂等键生成**：`{strategy_id}:{date}:{symbol}:{side}:{hash}`
- **检查机制**：执行前查询 `trading.trade_intent` 表，如果已存在相同 `idempotency_key` 且状态为 EXECUTED，则跳过
- **数据库约束**：`idempotency_key` 字段设置 UNIQUE 约束，防止重复插入

---

## 17. 附录

### 17.1 相关文档

- `docs/xtquant_miniqmt_integration_memo.md` - 集成备忘录
- `docs/qmt_sim_xtquant_setup.md` - 安装配置指南
- `docs/qmt_trading_capabilities_analysis.md` - 交易功能分析
- `docs/strategy_trading_implementation_plan.md` - 完整策略交易方案
- `docs/simple_strategy_trading_guide.md` - 简单策略指南

### 17.2 参考代码

- `backend/infra/qmt_client.py` - QMT客户端实现
- `backend/routers/qmt.py` - QMT API路由
- `frontend/src/app/qmt/positions/page.tsx` - 前端页面
- `backend/ingestion/tdx_scheduler.py` - 调度器参考实现（使用 schedule 库）

---

**文档版本**: v1.1  
**最后更新**: 2024-12-20  
**已确认需求，准备开始实现**


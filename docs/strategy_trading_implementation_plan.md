# 策略交易功能实现方案

## 1. 可行性分析

### 1.1 策略交易功能可以实现 ✅

**xtquant 完全支持策略交易**：
- 下单接口支持 `strategy_name` 参数，可以标记策略来源
- 支持同步和异步下单，适合策略自动执行
- 支持订阅推送，可以实时接收委托/成交回报

**AIstock 已有策略运行基础**：
- ✅ **多智能体分析系统** (`StockAnalysisAgents`) - 可以生成买卖信号
- ✅ **板块策略引擎** (`SectorStrategyEngine`) - 板块级别的策略分析
- ✅ **量化模型调度器** (`model_scheduler`) - LSTM、DeepAR 等模型的训练和推理
- ✅ **定时任务系统** - 已有调度器可以定时运行策略

### 1.2 是否需要先支持本地运行策略？

**答案：已有基础，但需要整合**

AIstock **已经具备策略运行能力**，但当前策略主要做：
- **分析**：生成分析报告、买卖建议
- **信号生成**：输出评级、目标价、买卖点建议

**缺失的环节**：
- ❌ 策略信号到交易意图的转换
- ❌ 交易意图到实际下单的执行链路
- ❌ 策略交易的风控检查
- ❌ 策略交易的状态跟踪和日志

## 2. 实现方案

### 2.1 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                    AIstock 策略层                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ AI 分析策略  │  │ 量化模型策略 │  │ 技术指标策略 │ │
│  │ (多智能体)   │  │ (LSTM/DeepAR)│  │ (指标选股)   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ │
│         │                  │                  │         │
│         └──────────────────┼──────────────────┘         │
│                            │                            │
│                    ┌──────▼───────┐                    │
│                    │  信号生成层   │                    │
│                    │ (Signal Gen) │                    │
│                    └──────┬───────┘                    │
│                           │                            │
│                    ┌──────▼───────┐                    │
│                    │  交易意图层   │                    │
│                    │ (Trade Intent)│                    │
│                    │  - 股票代码   │                    │
│                    │  - 买卖方向   │                    │
│                    │  - 数量/金额  │                    │
│                    │  - 价格类型   │                    │
│                    │  - 策略ID     │                    │
│                    └──────┬───────┘                    │
│                           │                            │
│                    ┌──────▼───────┐                    │
│                    │  风控检查层   │                    │
│                    │ (Risk Control)│                    │
│                    │  - 资金检查   │                    │
│                    │  - 持仓检查   │                    │
│                    │  - 限额检查   │                    │
│                    │  - 白名单检查 │                    │
│                    └──────┬───────┘                    │
│                           │                            │
└───────────────────────────┼────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  QMT 执行层     │
                    │ (xtquant)      │
                    │  - 下单        │
                    │  - 撤单        │
                    │  - 查询        │
                    └────────────────┘
```

### 2.2 核心组件设计

#### 2.2.1 交易意图（Trade Intent）数据结构

```python
@dataclass
class TradeIntent:
    """交易意图 - 策略生成的交易请求"""
    client_order_id: str  # 幂等键，格式：{strategy_id}:{date}:{symbol}:{side}:{seq}
    strategy_id: str  # 策略标识
    strategy_name: str  # 策略名称（用于xtquant的strategy_name参数）
    run_id: str  # 策略运行批次ID
    symbol: str  # 股票代码，统一格式：600519.SH
    side: str  # BUY / SELL
    quantity: int  # 委托数量（股）
    order_type: str  # MARKET / LIMIT
    limit_price: float  # 限价（限价单时使用）
    price_type: int  # xtquant价格类型（11=限价，5=最新价等）
    reason: str  # 交易原因/信号摘要
    signal_data: Dict[str, Any]  # 原始信号数据（用于追溯）
    status: str  # NEW / SENT / ACKED / FILLED / REJECTED / CANCELED / ERROR
    created_at: datetime
    updated_at: datetime
```

#### 2.2.2 策略交易服务（Strategy Trading Service）

```python
class StrategyTradingService:
    """策略交易服务 - 连接策略信号和交易执行"""
    
    def __init__(self, qmt_client: BaseQMTClient):
        self.qmt_client = qmt_client
        self.intent_queue = queue.Queue()  # 交易意图队列
        self.executor_thread = None  # 单线程执行器
        
    def submit_intent(self, intent: TradeIntent) -> str:
        """提交交易意图（异步）"""
        # 1. 验证意图
        # 2. 写入数据库（幂等检查）
        # 3. 加入执行队列
        # 4. 返回 client_order_id
        
    def _execute_worker(self):
        """单线程执行器（串行执行所有xtquant调用）"""
        while True:
            intent = self.intent_queue.get()
            try:
                # 1. 风控检查
                # 2. 调用 qmt_client.place_order
                # 3. 更新状态
                # 4. 记录日志
            except Exception as e:
                # 错误处理
```

#### 2.2.3 风控服务（Risk Control Service）

```python
class RiskControlService:
    """风控服务"""
    
    def check_intent(self, intent: TradeIntent, account_info: Dict) -> Tuple[bool, str]:
        """检查交易意图是否通过风控
        
        Returns:
            (passed, reason)
        """
        # 1. 资金检查：买入时检查可用资金
        # 2. 持仓检查：卖出时检查可卖数量
        # 3. 限额检查：单笔最大金额、单日交易限额
        # 4. 白名单检查：是否允许交易该股票
        # 5. 交易时段检查：是否在交易时间内
        # 6. 冷却期检查：同一股票是否在冷却期内
```

### 2.3 实现步骤

#### 阶段一：基础框架（1-2周）

1. **创建交易意图数据结构**
   - 定义 `TradeIntent` 数据类
   - 创建数据库表 `strategy_trade_intents`
   - 实现意图的 CRUD 操作

2. **实现策略交易服务**
   - `StrategyTradingService` 类
   - 单线程执行器（串行执行）
   - 基本的意图提交和执行流程

3. **实现基础风控**
   - `RiskControlService` 类
   - 资金检查、持仓检查
   - 基本限额检查

#### 阶段二：策略集成（2-3周）

1. **AI 分析策略集成**
   - 修改 `StockAnalysisAgents`，在生成买卖建议时创建交易意图
   - 支持从分析结果中提取交易信号

2. **量化模型策略集成**
   - 修改模型推理流程，在预测结果触发交易条件时创建交易意图
   - 支持 LSTM、DeepAR 等模型的交易信号

3. **技术指标策略集成**
   - 修改指标选股服务，在满足条件时创建交易意图
   - 支持基于技术指标的自动交易

#### 阶段三：增强功能（2-3周）

1. **高级风控**
   - 持仓集中度控制
   - 最大回撤控制
   - 单日交易限额
   - 白名单/黑名单管理

2. **策略管理 UI**
   - 策略列表和状态展示
   - 交易意图列表和详情
   - 策略启停控制
   - 风控参数配置

3. **监控和日志**
   - 策略执行日志
   - 交易意图状态跟踪
   - 性能统计（胜率、盈亏等）

## 3. 数据库设计

### 3.1 交易意图表

```sql
CREATE TABLE app.strategy_trade_intents (
    id SERIAL PRIMARY KEY,
    client_order_id VARCHAR(255) UNIQUE NOT NULL,  -- 幂等键
    strategy_id VARCHAR(100) NOT NULL,
    strategy_name VARCHAR(200),
    run_id VARCHAR(100),
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,  -- BUY/SELL
    quantity INTEGER NOT NULL,
    order_type VARCHAR(20) NOT NULL,  -- MARKET/LIMIT
    limit_price DECIMAL(10, 3),
    price_type INTEGER,
    reason TEXT,
    signal_data JSONB,
    status VARCHAR(20) NOT NULL,  -- NEW/SENT/ACKED/FILLED/REJECTED/CANCELED/ERROR
    broker_order_id VARCHAR(100),  -- xtquant返回的订单编号
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    executed_at TIMESTAMP
);

CREATE INDEX idx_intents_strategy ON app.strategy_trade_intents(strategy_id, created_at DESC);
CREATE INDEX idx_intents_status ON app.strategy_trade_intents(status);
CREATE INDEX idx_intents_symbol ON app.strategy_trade_intents(symbol);
```

### 3.2 策略配置表（可选）

```sql
CREATE TABLE app.strategy_configs (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(100) UNIQUE NOT NULL,
    strategy_name VARCHAR(200) NOT NULL,
    strategy_type VARCHAR(50),  -- AI_ANALYSIS / QUANT_MODEL / TECHNICAL_INDICATOR
    enabled BOOLEAN DEFAULT TRUE,
    risk_config JSONB,  -- 风控配置
    schedule_config JSONB,  -- 调度配置
    params JSONB,  -- 策略参数
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

## 4. API 设计

### 4.1 策略交易 API

```python
# POST /api/v1/strategy-trading/intent
# 提交交易意图（策略调用）
{
    "strategy_id": "ai_analysis_001",
    "strategy_name": "AI多智能体分析",
    "symbol": "600519.SH",
    "side": "BUY",
    "quantity": 100,
    "order_type": "LIMIT",
    "limit_price": 1800.0,
    "reason": "技术面+基本面+资金面综合评分A+",
    "signal_data": {...}
}

# GET /api/v1/strategy-trading/intents
# 查询交易意图列表
# 参数：strategy_id, status, start_date, end_date

# POST /api/v1/strategy-trading/strategy/{strategy_id}/enable
# 启用策略

# POST /api/v1/strategy-trading/strategy/{strategy_id}/disable
# 禁用策略
```

## 5. 示例：AI 分析策略集成

### 5.1 修改 StockAnalysisAgents

```python
class StockAnalysisAgents:
    def __init__(self, ..., trading_service: StrategyTradingService = None):
        self.trading_service = trading_service
    
    def run_multi_agent_analysis(self, ...):
        # ... 现有分析逻辑 ...
        
        # 生成最终决策
        final_decision = self._generate_final_decision(results)
        
        # 如果启用自动交易，创建交易意图
        if self.trading_service and final_decision.get("action") in ["BUY", "SELL"]:
            intent = TradeIntent(
                client_order_id=f"ai_analysis:{date}:{symbol}:{final_decision['action']}:{seq}",
                strategy_id="ai_analysis_001",
                strategy_name="AI多智能体分析",
                symbol=symbol,
                side=final_decision["action"],
                quantity=self._calculate_quantity(final_decision, account_info),
                order_type="LIMIT",
                limit_price=final_decision.get("target_price"),
                reason=final_decision.get("summary"),
                signal_data=final_decision,
            )
            self.trading_service.submit_intent(intent)
        
        return results
```

## 6. 总结

### 6.1 策略交易功能可以实现 ✅

- **xtquant 支持**：完全支持策略标记和自动下单
- **AIstock 基础**：已有策略运行能力，只需整合交易执行

### 6.2 是否需要先支持本地运行策略？

**答案：已有基础，但需要整合**

- ✅ **已有**：策略分析、信号生成、模型推理
- ❌ **缺失**：信号到交易的转换、交易执行、风控、状态跟踪

### 6.3 实现建议

1. **先实现基础框架**（交易意图、执行服务、基础风控）
2. **再集成现有策略**（AI分析、量化模型、技术指标）
3. **最后增强功能**（高级风控、UI管理、监控日志）

### 6.4 关键设计原则

1. **策略与执行分离**：策略只生成意图，不直接调用xtquant
2. **串行执行**：所有xtquant调用通过单线程执行器，避免并发问题
3. **幂等性**：通过 `client_order_id` 保证不重复下单
4. **可追溯**：所有交易意图和信号数据落库，便于回放和审计
5. **风控优先**：每笔交易都经过风控检查


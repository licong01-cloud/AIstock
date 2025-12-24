# 策略基类（BaseStrategy）说明

## 什么是策略基类？

策略基类是一个抽象类或接口，定义了所有策略必须实现的方法。所有具体策略（如双均线策略、趋势跟踪策略）都继承自这个基类。

## 策略基类的价值

### 1. **统一接口规范**

所有策略实现相同的接口，便于管理和调用：

```python
class BaseStrategy:
    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略（子类必须实现）"""
        raise NotImplementedError
    
    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """生成交易信号（子类必须实现）"""
        raise NotImplementedError
```

**好处**：
- 策略调度器可以用统一的方式调用所有策略
- 代码更清晰，易于理解
- IDE 可以提供更好的代码补全和类型检查

### 2. **代码复用**

公共逻辑可以在基类中实现，避免重复代码：

```python
class BaseStrategy:
    def __init__(self, strategy_id: str, executor: SimpleStrategyExecutor):
        self.strategy_id = strategy_id
        self.executor = executor
        self.logger = logging.getLogger(f"strategy.{strategy_id}")
    
    def fetch_data(self, symbol: str, days: int = 30):
        """获取股票数据（所有策略共用）"""
        # 统一的数据获取逻辑
        pass
    
    def log_signal(self, signal: TradeSignal):
        """记录交易信号（所有策略共用）"""
        self.logger.info(f"策略 {self.strategy_id} 生成信号: {signal}")
    
    def run(self, symbol: str):
        """模板方法：定义策略执行的通用流程"""
        # 1. 获取数据
        data = self.fetch_data(symbol)
        # 2. 生成信号
        signal = self.generate_signal(data)
        # 3. 执行信号
        if signal:
            return self.executor.execute_signal(...)
```

**好处**：
- 减少重复代码
- 统一错误处理、日志记录等逻辑
- 修改公共逻辑时，所有策略自动受益

### 3. **类型安全和IDE支持**

明确的接口定义，IDE 可以提供更好的支持：

```python
# IDE 知道所有策略都有 run() 方法
strategies: List[BaseStrategy] = [
    MACrossStrategy(...),
    TrendFollowingStrategy(...)
]

for strategy in strategies:
    result = strategy.run("600519.SH")  # IDE 知道这里有 run() 方法
```

**好处**：
- 编译时类型检查（使用类型检查工具如 mypy）
- IDE 自动补全
- 减少运行时错误

### 4. **扩展性**

未来添加新功能时，只需在基类中添加：

```python
class BaseStrategy:
    # 现有方法...
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取策略绩效指标（新增功能）"""
        # 所有策略自动拥有这个方法
        pass
    
    def validate_config(self, config: Dict) -> Tuple[bool, str]:
        """验证策略配置（新增功能）"""
        # 所有策略自动拥有这个方法
        pass
```

**好处**：
- 新功能自动应用到所有策略
- 不需要修改每个具体策略的代码

### 5. **测试友好**

可以创建 Mock 策略进行单元测试：

```python
class MockStrategy(BaseStrategy):
    """用于测试的 Mock 策略"""
    def run(self, symbol: str):
        return {"success": True, "signal": None}
    
    def generate_signal(self, data: Dict):
        return None

# 测试策略执行器
def test_strategy_executor():
    mock_strategy = MockStrategy("test", executor)
    result = mock_strategy.run("600519.SH")
    assert result["success"] == True
```

**好处**：
- 可以独立测试策略执行器
- 不需要依赖真实策略实现

## 策略基类 vs 直接实现

### 不使用基类（直接实现）

```python
# 双均线策略
class MACrossStrategy:
    def __init__(self, ...):
        # 每个策略都要写一遍
        pass
    
    def run(self, symbol: str):
        # 每个策略都要实现数据获取、日志等
        pass

# 趋势跟踪策略
class TrendFollowingStrategy:
    def __init__(self, ...):
        # 重复代码
        pass
    
    def run(self, symbol: str):
        # 重复代码
        pass
```

**问题**：
- 代码重复
- 接口不统一（可能有些策略有 `run()`，有些有 `execute()`）
- 难以统一管理

### 使用基类

```python
# 基类定义统一接口
class BaseStrategy:
    def run(self, symbol: str):
        raise NotImplementedError

# 具体策略继承基类
class MACrossStrategy(BaseStrategy):
    def run(self, symbol: str):
        # 只关注策略逻辑
        pass

class TrendFollowingStrategy(BaseStrategy):
    def run(self, symbol: str):
        # 只关注策略逻辑
        pass
```

**优势**：
- 代码复用
- 接口统一
- 易于管理

## 实现建议

### 初期实现（简单版）

```python
class BaseStrategy:
    """策略基类（简单版）"""
    
    def __init__(self, strategy_id: str, executor: SimpleStrategyExecutor):
        self.strategy_id = strategy_id
        self.executor = executor
    
    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略（子类必须实现）"""
        raise NotImplementedError
    
    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """生成交易信号（子类必须实现）"""
        raise NotImplementedError
```

### 后期扩展（完整版）

```python
class BaseStrategy:
    """策略基类（完整版）"""
    
    def __init__(self, strategy_id: str, executor: SimpleStrategyExecutor, config: Dict):
        self.strategy_id = strategy_id
        self.executor = executor
        self.config = config
        self.logger = logging.getLogger(f"strategy.{strategy_id}")
    
    def fetch_data(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """获取股票数据（公共方法）"""
        # 统一的数据获取逻辑
        pass
    
    def validate_config(self) -> Tuple[bool, str]:
        """验证配置（公共方法）"""
        # 统一的配置验证逻辑
        pass
    
    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略（模板方法）"""
        # 1. 验证配置
        # 2. 获取数据
        # 3. 生成信号
        # 4. 执行信号
        # 5. 记录日志
        pass
    
    def generate_signal(self, data: Dict[str, Any]) -> Optional[TradeSignal]:
        """生成交易信号（子类必须实现）"""
        raise NotImplementedError
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取策略绩效指标"""
        # 从数据库查询绩效数据
        pass
```

## 总结

策略基类的核心价值：
1. **统一接口**：所有策略遵循相同的接口规范
2. **代码复用**：公共逻辑在基类中实现，避免重复
3. **类型安全**：明确的类型定义，IDE 和类型检查工具支持更好
4. **易于扩展**：新功能只需在基类中添加，所有策略自动继承
5. **测试友好**：可以创建 Mock 策略进行单元测试

**建议**：初期实现简单版基类，后期逐步扩展功能。


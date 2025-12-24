"""策略基类（QMT 交易系统）"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..infra.strategy_executor import SimpleStrategyExecutor


class TradeSignal:
    """交易信号数据类"""

    def __init__(
        self,
        strategy_id: str,
        symbol: str,
        side: str,  # "BUY" / "SELL"
        quantity: int,
        price_type: str = "LIMIT",  # "LIMIT" / "MARKET"
        price: float = 0.0,
        reason: str = "",
        signal_data: Optional[Dict[str, Any]] = None,
    ):
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.side = side
        self.quantity = quantity
        self.price_type = price_type
        self.price = price
        self.reason = reason
        self.signal_data = signal_data or {}


class BaseStrategy(ABC):
    """策略基类

    所有具体策略都应继承此类并实现必要的方法。
    """

    def __init__(
        self,
        strategy_id: str,
        executor: Optional[SimpleStrategyExecutor] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """初始化策略

        Args:
            strategy_id: 策略ID（唯一标识）
            executor: 策略执行器（如果为 None，则自动创建）
            config: 策略配置（从数据库加载或传入）
        """
        self.strategy_id = strategy_id
        self.executor = executor
        self.config = config or {}
        import logging

        self.logger = logging.getLogger(f"strategy.{strategy_id}")

    @abstractmethod
    def run(self, symbol: str) -> Dict[str, Any]:
        """运行策略（子类必须实现）

        Args:
            symbol: 股票代码（如 "600519.SH"）

        Returns:
            执行结果字典，包含：
            - success: 是否成功
            - signals: 生成的信号列表
            - message: 消息
        """
        raise NotImplementedError

    @abstractmethod
    def generate_signal(
        self, data: Dict[str, Any]
    ) -> Optional[TradeSignal]:
        """生成交易信号（子类必须实现）

        Args:
            data: 策略所需的数据（通常是股票历史数据）

        Returns:
            交易信号（如果无信号则返回 None）
        """
        raise NotImplementedError

    def execute_signal(self, signal: TradeSignal) -> Dict[str, Any]:
        """执行交易信号

        这是一个便捷方法，调用策略执行器执行信号。

        Args:
            signal: 交易信号

        Returns:
            执行结果
        """
        if self.executor is None:
            from ..infra.strategy_executor import SimpleStrategyExecutor

            self.executor = SimpleStrategyExecutor()

        return self.executor.execute_signal(
            strategy_id=signal.strategy_id,
            symbol=signal.symbol,
            side=signal.side,
            quantity=signal.quantity,
            price_type=signal.price_type,
            price=signal.price,
            reason=signal.reason,
            signal_data=signal.signal_data,
        )


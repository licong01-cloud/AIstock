"""执行算法抽象基类 — 定义日内拆单接口."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class OrderState:
    """单个订单的执行状态."""
    symbol: str
    side: str                         # BUY / SELL
    total_quantity: int
    executed_quantity: int = 0
    step: int = 0
    is_complete: bool = False
    child_fills: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StepResult:
    """单步执行结果 — 一次子成交."""
    symbol: str
    side: str
    quantity: int
    price: float
    reason: str = ""


class BaseExecutionAlgo(ABC):
    """执行算法抽象基类.

    每个算法实现三个核心方法:
    - init_order: 初始化订单状态
    - compute_step: 每个 bar 计算本步应执行的数量和价格
    - is_complete: 判断是否已完成
    """

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        self.config = config or {}

    def init_order(
        self, symbol: str, side: str, total_quantity: int,
    ) -> OrderState:
        """初始化订单执行状态."""
        return OrderState(
            symbol=symbol,
            side=side,
            total_quantity=self._round_lot(total_quantity),
        )

    @abstractmethod
    def compute_step(
        self,
        state: OrderState,
        bar_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Optional[StepResult]:
        """计算本步执行.

        Args:
            state: 当前订单状态
            bar_data: 当前 bar 行情 {open, high, low, close, volume, vwap, ...}
            market_context: 全局市场上下文 {volume_profile, sigma, close_series, ...}

        Returns:
            StepResult 或 None（本步不执行）
        """

    def is_complete(self, state: OrderState) -> bool:
        """判断订单是否执行完毕."""
        return state.is_complete or state.executed_quantity >= state.total_quantity

    @staticmethod
    def _round_lot(qty: int, lot_size: int = 100) -> int:
        """A 股 100 股整手取整（向下）."""
        return (qty // lot_size) * lot_size

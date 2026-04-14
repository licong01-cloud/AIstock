"""尾盘加仓持仓股执行算法 (TAIL_BOOST).

触发条件: 14:50 (第210分钟) 检测到涨停未成交买单
逻辑: 释放资金按持仓市值等比例加仓已持有股票
排除: T+0 当日买入股 / 当前涨停股

本算法的 compute_step 为尾盘一次性全额执行（类似 CLOSE_PRICE），
实际的资金分配逻辑由 ExecutionEngine._generate_boost_orders() 完成。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class TailBoostAlgo(BaseExecutionAlgo):
    """尾盘加仓持仓股 — 涨停未成交资金再分配."""

    ALGO_CODE = "TAIL_BOOST"

    def compute_step(
        self,
        state: OrderState,
        bar_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Optional[StepResult]:
        if self.is_complete(state):
            return None

        remaining = state.total_quantity - state.executed_quantity
        if remaining <= 0:
            state.is_complete = True
            return None

        price = bar_data.get("close", 0)
        if price <= 0:
            return None

        state.executed_quantity += remaining
        state.step += 1
        state.is_complete = True

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=remaining,
            price=price,
            reason="TAIL_BOOST 尾盘加仓",
        )

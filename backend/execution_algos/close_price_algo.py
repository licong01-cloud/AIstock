"""收盘价执行算法 — 一次性全额成交."""
from __future__ import annotations

from typing import Any, Dict, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class ClosePriceAlgo(BaseExecutionAlgo):
    ALGO_CODE = "CLOSE_PRICE"

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
            reason="收盘价一次性执行",
        )

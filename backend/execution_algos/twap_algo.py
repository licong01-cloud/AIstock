"""TWAP 均匀拆分执行算法."""
from __future__ import annotations

from typing import Any, Dict, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class TWAPAlgo(BaseExecutionAlgo):
    ALGO_CODE = "TWAP"

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.split_count = self.config.get("split_count", 6)

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

        # 最后一步：强制执行全部剩余
        if state.step >= self.split_count - 1:
            step_qty = remaining
        else:
            # 期望累计执行量
            expected_executed = state.total_quantity / self.split_count * (state.step + 1)
            step_qty = expected_executed - state.executed_quantity

        step_qty = self._round_lot(int(step_qty))
        step_qty = min(step_qty, remaining)

        if step_qty <= 0:
            state.step += 1
            return None

        state.executed_quantity += step_qty
        state.step += 1
        if state.executed_quantity >= state.total_quantity:
            state.is_complete = True

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=step_qty,
            price=price,
            reason=f"TWAP step {state.step}/{self.split_count}",
        )

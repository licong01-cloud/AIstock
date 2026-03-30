"""POV 参与率执行算法 — 按市场成交量的固定比例参与."""
from __future__ import annotations

from typing import Any, Dict, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class POVAlgo(BaseExecutionAlgo):
    ALGO_CODE = "POV"

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.target_participation = self.config.get("target_participation", 0.05)
        self.max_participation = self.config.get("max_participation", 0.20)

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

        # 当前 bar 实际成交量
        market_volume = bar_data.get("volume", 0)
        if market_volume <= 0:
            state.step += 1
            return None

        # 按参与率计算执行量，但不超过最大参与率限制
        step_qty = int(market_volume * self.target_participation)
        max_qty = int(market_volume * self.max_participation)
        step_qty = min(step_qty, max_qty)
        step_qty = self._round_lot(step_qty)
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
            reason=f"POV {self.target_participation:.1%} of vol={market_volume}",
        )

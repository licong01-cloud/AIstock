"""VWAP 成交量加权执行算法."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class VWAPAlgo(BaseExecutionAlgo):
    ALGO_CODE = "VWAP"

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

        # volume_profile: 每个 bar 的历史平均成交量分布
        volume_profile: List[float] = market_context.get("volume_profile", [])
        total_bars = len(volume_profile)

        if total_bars == 0 or state.step >= total_bars:
            # 无 profile 数据则 fallback 到均匀拆分
            step_qty = remaining
        else:
            total_vol = sum(volume_profile)
            if total_vol <= 0:
                step_qty = remaining
            else:
                weight = volume_profile[state.step] / total_vol
                step_qty = int(state.total_quantity * weight)
                step_qty = self._round_lot(step_qty)
                step_qty = min(step_qty, remaining)
                # 最后一个 bar 强制执行剩余
                if state.step >= total_bars - 1:
                    step_qty = remaining

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
            reason=f"VWAP step {state.step}/{total_bars}",
        )

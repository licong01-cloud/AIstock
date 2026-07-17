"""VWAP execution algorithm with an authoritative volume-profile contract."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


class VWAPVolumeProfileError(ValueError):
    """Raised when VWAP cannot prove its required volume-profile input."""

    reason_code = "VWAP_VOLUME_PROFILE_INVALID"

    def __init__(self, message: str, *, context: Dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = dict(context or {})


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

        raw_profile = market_context.get("volume_profile")
        if not isinstance(raw_profile, (list, tuple)) or not raw_profile:
            raise VWAPVolumeProfileError(
                "VWAP requires a non-empty authoritative volume_profile",
                context={
                    "symbol": state.symbol,
                    "profile_type": type(raw_profile).__name__,
                },
            )

        volume_profile: List[float] = []
        for index, raw_value in enumerate(raw_profile):
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                raise VWAPVolumeProfileError(
                    "VWAP volume_profile entries must be numeric",
                    context={
                        "symbol": state.symbol,
                        "index": index,
                        "value_type": type(raw_value).__name__,
                    },
                )
            value = float(raw_value)
            if not math.isfinite(value) or value < 0:
                raise VWAPVolumeProfileError(
                    "VWAP volume_profile entries must be finite and non-negative",
                    context={
                        "symbol": state.symbol,
                        "index": index,
                        "value": repr(raw_value),
                    },
                )
            volume_profile.append(value)

        total_bars = len(volume_profile)
        if state.step >= total_bars:
            raise VWAPVolumeProfileError(
                "VWAP volume_profile is exhausted before the order is complete",
                context={
                    "symbol": state.symbol,
                    "step": state.step,
                    "profile_length": total_bars,
                    "remaining_quantity": remaining,
                },
            )

        total_vol = sum(volume_profile)
        if not math.isfinite(total_vol) or total_vol <= 0:
            raise VWAPVolumeProfileError(
                "VWAP volume_profile total must be finite and positive",
                context={
                    "symbol": state.symbol,
                    "profile_length": total_bars,
                    "total_volume": total_vol,
                },
            )

        weight = volume_profile[state.step] / total_vol
        step_qty = int(state.total_quantity * weight)
        step_qty = self._round_lot(
            step_qty,
            symbol=state.symbol,
            side=state.side,
        )
        step_qty = min(step_qty, remaining)
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

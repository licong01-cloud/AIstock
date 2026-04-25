"""Strict V24 minute execution algorithm.

V24_PLAN is the QE minute execution policy used by existing experiments. This
implementation intentionally fails if its model or required minute context is
missing; it must never silently fall back to TWAP or daily execution.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


class V24PlanUnavailableError(RuntimeError):
    """Raised when V24_PLAN cannot run authoritatively."""


@register
class V24PlanAlgo(BaseExecutionAlgo):
    ALGO_CODE = "V24_PLAN"

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        model_path = str(self.config.get("model_path") or "").strip()
        if not model_path:
            raise V24PlanUnavailableError("V24_PLAN requires config.model_path")
        if not Path(model_path).exists():
            raise V24PlanUnavailableError(f"V24_PLAN model_path does not exist: {model_path}")

        try:
            from rl_execution.executor.v24_hybrid_executor import V24HybridExecutor
        except Exception as exc:  # pragma: no cover - depends on optional runtime imports
            raise V24PlanUnavailableError(
                f"failed to import V24HybridExecutor: {type(exc).__name__}: {exc}"
            ) from exc

        self._executor = V24HybridExecutor(
            plan_model_path=model_path,
            device=str(self.config.get("device") or "cpu"),
            enable_warmup_gap=bool(self.config.get("enable_warmup_gap", False)),
            enable_correction=bool(self.config.get("enable_correction", False)),
            enable_chase_rules=bool(self.config.get("enable_chase_rules", False)),
        )
        if getattr(self._executor, "plan_model", None) is None:
            raise V24PlanUnavailableError("V24_PLAN plan model was not loaded")

        self._initialized = False

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

        close_arr = self._require_array(market_context, "full_day_close")
        vol_arr = self._require_array(market_context, "full_day_volume")
        high_arr = self._require_array(market_context, "full_day_high")
        low_arr = self._require_array(market_context, "full_day_low")
        if not (len(close_arr) == len(vol_arr) == len(high_arr) == len(low_arr)):
            raise V24PlanUnavailableError("V24_PLAN full-day arrays must have equal length")
        if len(close_arr) < 31:
            raise V24PlanUnavailableError("V24_PLAN requires at least 31 minute bars")

        prev_close = self._require_positive(market_context, "prev_close")
        cur_price = float(bar_data.get("close") or 0)
        open_price = float(bar_data.get("open") or cur_price)
        if cur_price <= 0 or open_price <= 0:
            raise V24PlanUnavailableError("V24_PLAN requires positive open/close prices")

        is_buy = state.side.upper() == "BUY"
        stock_id = str(market_context.get("stock_id") or state.symbol)
        limit_pct = float(market_context.get("limit_pct") or self._infer_limit_pct(stock_id))
        is_limit_up = self._is_limit_up(bar_data, cur_price, prev_close, limit_pct)
        is_limit_down = self._is_limit_down(bar_data, cur_price, prev_close, limit_pct)

        if not self._initialized:
            self._executor.reset(
                total_amount=float(state.total_quantity),
                open_price=open_price,
                prev_close=prev_close,
                stock_id=stock_id,
                is_buy=is_buy,
            )
            self._initialized = True

        frac, _urgency_bps = self._executor.decide(
            cur_step=state.step,
            remaining=float(remaining),
            is_buy=is_buy,
            cur_price=cur_price,
            prev_close=prev_close,
            limit_pct=limit_pct,
            is_limit_up=is_limit_up,
            is_limit_down=is_limit_down,
            close_arr=close_arr,
            vol_arr=vol_arr,
            high_arr=high_arr,
            low_arr=low_arr,
            stock_id=stock_id,
            max_step=len(close_arr),
        )

        if state.step >= self._executor.WARMUP and getattr(self._executor, "_current_plan", None) is None:
            raise V24PlanUnavailableError("V24_PLAN failed to generate a plan")

        step_qty = self._round_lot(int(remaining * float(frac)))
        if state.step >= len(close_arr) - 1:
            step_qty = remaining
        step_qty = min(step_qty, remaining)

        state.step += 1
        if step_qty <= 0:
            return None

        state.executed_quantity += step_qty
        if state.executed_quantity >= state.total_quantity:
            state.is_complete = True

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=step_qty,
            price=cur_price,
            reason=f"V24_PLAN step {state.step}/{len(close_arr)}",
        )

    @staticmethod
    def _require_array(market_context: Dict[str, Any], key: str) -> np.ndarray:
        value = market_context.get(key)
        if value is None:
            raise V24PlanUnavailableError(f"V24_PLAN requires market_context.{key}")
        arr = np.asarray(value, dtype=float)
        if arr.ndim != 1 or arr.size == 0 or np.isnan(arr).any():
            raise V24PlanUnavailableError(f"V24_PLAN market_context.{key} is invalid")
        return arr

    @staticmethod
    def _require_positive(market_context: Dict[str, Any], key: str) -> float:
        value = float(market_context.get(key) or 0)
        if value <= 0:
            raise V24PlanUnavailableError(f"V24_PLAN requires positive market_context.{key}")
        return value

    @staticmethod
    def _infer_limit_pct(symbol: str) -> float:
        code = symbol.split(".")[0]
        if code.startswith(("300", "301", "688", "689")):
            return 0.20
        return 0.10

    @staticmethod
    def _is_limit_up(bar_data: Dict[str, Any], cur_price: float, prev_close: float, limit_pct: float) -> bool:
        limit_up = bar_data.get("limit_up")
        if limit_up:
            return cur_price >= float(limit_up) - 1e-8
        return cur_price >= prev_close * (1 + limit_pct) - 1e-8

    @staticmethod
    def _is_limit_down(bar_data: Dict[str, Any], cur_price: float, prev_close: float, limit_pct: float) -> bool:
        limit_down = bar_data.get("limit_down")
        if limit_down:
            return cur_price <= float(limit_down) + 1e-8
        return cur_price <= prev_close * (1 - limit_pct) + 1e-8

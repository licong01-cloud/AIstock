"""SBB-EMA 择时执行算法 — EMA 双线交叉信号控制执行节奏."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register

TREND_MID = 0
TREND_SHORT = 1
TREND_LONG = 2


@register
class SBBEMAAlgo(BaseExecutionAlgo):
    ALGO_CODE = "SBB_EMA"

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.fast_period = self.config.get("fast_period", 5)
        self.slow_period = self.config.get("slow_period", 20)
        self.trend_strength = self.config.get("trend_strength", 0.001)
        self.split_count = self.config.get("split_count", 6)
        self._last_trend = TREND_MID

    def _compute_trend(self, close_series: List[float]) -> int:
        """计算当前 EMA 趋势状态."""
        if len(close_series) < self.slow_period:
            return TREND_MID

        # 手动计算 EMA（不依赖 pandas/numpy）
        ema_fast = self._ema(close_series, self.fast_period)
        ema_slow = self._ema(close_series, self.slow_period)

        diff = ema_fast - ema_slow
        current_price = close_series[-1]
        if current_price <= 0:
            return TREND_MID

        normalized_diff = diff / current_price
        if abs(normalized_diff) < self.trend_strength:
            return TREND_MID
        return TREND_LONG if diff > 0 else TREND_SHORT

    @staticmethod
    def _ema(series: List[float], span: int) -> float:
        """计算 EMA 最新值."""
        alpha = 2.0 / (span + 1)
        ema = series[0]
        for v in series[1:]:
            ema = alpha * v + (1 - alpha) * ema
        return ema

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

        # close_series: 近 N 根 bar 的收盘价序列
        close_series: List[float] = market_context.get("close_series", [])
        trend = self._compute_trend(close_series) if close_series else TREND_MID

        # 买入：趋势看多时执行；卖出：趋势看空时执行
        # TREND_MID 时执行均匀拆分量的一半（保守执行）
        if state.side == "BUY":
            if trend == TREND_SHORT:
                state.step += 1
                self._last_trend = trend
                return None  # 看空时暂缓买入
            multiplier = 1.5 if trend == TREND_LONG else 0.5
        else:  # SELL
            if trend == TREND_LONG:
                state.step += 1
                self._last_trend = trend
                return None  # 看多时暂缓卖出
            multiplier = 1.5 if trend == TREND_SHORT else 0.5

        # 基础量 = TWAP 均匀拆分
        base_qty = state.total_quantity / self.split_count
        step_qty = int(base_qty * multiplier)
        step_qty = self._round_lot(step_qty)
        step_qty = min(step_qty, remaining)
        step_qty = max(step_qty, self._round_lot(100))  # 至少 100 股
        step_qty = min(step_qty, remaining)

        # 最后一步强制执行剩余
        if state.step >= self.split_count - 1:
            step_qty = remaining

        if step_qty <= 0:
            state.step += 1
            return None

        state.executed_quantity += step_qty
        state.step += 1
        if state.executed_quantity >= state.total_quantity:
            state.is_complete = True

        trend_label = {TREND_MID: "MID", TREND_LONG: "LONG", TREND_SHORT: "SHORT"}.get(trend, "?")
        self._last_trend = trend

        return StepResult(
            symbol=state.symbol,
            side=state.side,
            quantity=step_qty,
            price=price,
            reason=f"SBB-EMA trend={trend_label} step {state.step}/{self.split_count}",
        )

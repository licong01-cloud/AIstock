"""Almgren-Chriss 最优执行算法 — 基于市场冲击模型的最优执行策略."""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class ACOptimalAlgo(BaseExecutionAlgo):
    ALGO_CODE = "AC_OPTIMAL"

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        super().__init__(config)
        # lambda: 风险厌恶参数
        self.risk_aversion = self.config.get("risk_aversion", 1e-6)
        # eta: 临时冲击系数
        self.eta = self.config.get("eta", 2.5e-7)
        self.total_bars = self.config.get("total_bars", 6)

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

        T = self.total_bars
        t = state.step

        # sigma: 波动率，从 market_context 获取
        sigma = market_context.get("sigma", 0.02)

        # kappa = sqrt(lambda * sigma^2 / eta)
        kappa = math.sqrt(self.risk_aversion * sigma * sigma / self.eta) if self.eta > 0 else 0.5

        # 最后一步强制执行剩余
        if t >= T - 1:
            step_qty = remaining
        else:
            # Almgren-Chriss 最优策略: ratio[t] = sinh(kappa*(T-t)) / sinh(kappa*T)
            sinh_total = math.sinh(kappa * T) if kappa * T < 700 else 1e300
            if sinh_total <= 0:
                step_qty = remaining
            else:
                ratio_t = math.sinh(kappa * (T - t)) / sinh_total
                ratio_t1 = math.sinh(kappa * (T - t - 1)) / sinh_total
                # 本步执行比例 = ratio_t - ratio_t1
                frac = ratio_t - ratio_t1
                step_qty = int(state.total_quantity * frac)

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
            reason=f"AC-Optimal step {state.step}/{T}",
        )

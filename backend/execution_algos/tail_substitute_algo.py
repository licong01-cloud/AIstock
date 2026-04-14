"""尾盘替补买入执行算法 (TAIL_SUBSTITUTE).

触发条件: 14:50 (第210分钟) 检测到涨停未成交买单
逻辑: 释放资金按评分排名顺序买入 topk 之后的候选股票
排除: 已持仓 / 当前涨停 / 当日已有买单

本算法的 compute_step 为尾盘一次性全额执行（类似 CLOSE_PRICE），
实际的候选选择逻辑由 ExecutionEngine._generate_substitute_orders() 完成。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from .base_algo import BaseExecutionAlgo, OrderState, StepResult
from .registry import register


@register
class TailSubstituteAlgo(BaseExecutionAlgo):
    """尾盘替补买入 — 涨停未成交资金按排名替补."""

    ALGO_CODE = "TAIL_SUBSTITUTE"

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
            reason="TAIL_SUBSTITUTE 尾盘替补",
        )

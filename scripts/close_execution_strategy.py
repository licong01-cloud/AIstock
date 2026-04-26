"""Close-price minute execution strategy for QE NestedExecutor."""
from __future__ import annotations

from qlib.backtest.decision import Order, TradeDecisionWO
from qlib.contrib.strategy.rule_strategy import TWAPStrategy
from qlib.backtest.utils import get_start_end_idx


class CloseExecutionStrategy(TWAPStrategy):
    """Execute the remaining outer decision at the last intraday step only."""

    def __init__(self, **kwargs):
        super().__init__()

    def reset(self, outer_trade_decision=None, **kwargs):
        super().reset(outer_trade_decision=outer_trade_decision, **kwargs)

    def generate_trade_decision(self, execute_result=None):
        if self.outer_trade_decision is None or len(self.outer_trade_decision.get_decision()) == 0:
            return TradeDecisionWO(order_list=[], strategy=self)
        trade_step = self.trade_calendar.get_trade_step()
        start_idx, end_idx = get_start_end_idx(self.trade_calendar, self.outer_trade_decision)
        if trade_step < start_idx or trade_step > end_idx:
            return TradeDecisionWO(order_list=[], strategy=self)
        if execute_result is not None:
            for order, _, _, _ in execute_result:
                if order.stock_id in self.trade_amount_remain:
                    self.trade_amount_remain[order.stock_id] -= order.deal_amount
        if trade_step != end_idx:
            return TradeDecisionWO(order_list=[], strategy=self)
        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)
        order_list = []
        for order in self.outer_trade_decision.get_decision():
            amount = self.trade_amount_remain.get(order.stock_id, order.amount)
            if amount and amount > 1e-5:
                order_list.append(Order(
                    stock_id=order.stock_id,
                    amount=amount,
                    direction=order.direction,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                ))
        return TradeDecisionWO(order_list=order_list, strategy=self)

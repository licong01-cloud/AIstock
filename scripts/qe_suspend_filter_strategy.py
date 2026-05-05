"""TopK strategy wrapper with explicit suspend_d signal filtering."""
from __future__ import annotations

import copy

import numpy as np
import pandas as pd
from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy

from qe_event_risk_policy import QEEventRiskPolicy
from qe_suspend_filter import QESuspendFilter


class SuspendFilterTopkDropoutStrategy(TopkDropoutStrategy):
    """Qlib TopkDropoutStrategy plus fail-fast suspend_d filtering."""

    def __init__(
        self,
        signal=None,
        topk=50,
        n_drop=5,
        filter_suspended_on_signal=False,
        suspend_filter_file=None,
        suspend_filter_strict=True,
        risk_policy_enabled=False,
        risk_policy_file=None,
        risk_policy_strict=True,
        **kwargs,
    ):
        super().__init__(signal=signal, topk=topk, n_drop=n_drop, **kwargs)
        self._qe_suspend_filter = QESuspendFilter(
            enabled=filter_suspended_on_signal,
            suspend_filter_file=suspend_filter_file,
            strict=suspend_filter_strict,
        )
        self._qe_risk_policy = QEEventRiskPolicy(
            enabled=risk_policy_enabled,
            risk_policy_file=risk_policy_file,
            strict=risk_policy_strict,
        )

    def generate_trade_decision(self, execute_result=None):
        trade_step = self.trade_calendar.get_trade_step()
        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)
        pred_start_time, pred_end_time = self.trade_calendar.get_step_time(trade_step, shift=1)
        pred_score = self.signal.get_signal(start_time=pred_start_time, end_time=pred_end_time)
        if isinstance(pred_score, pd.DataFrame):
            pred_score = pred_score.iloc[:, 0]
        if pred_score is None:
            forced_orders = self._build_forced_exit_orders(trade_start_time, trade_end_time)
            return TradeDecisionWO(forced_orders, self)
        pred_score = self._qe_suspend_filter.filter_scores(pred_score, trade_start_time)
        pred_score = self._qe_risk_policy.filter_scores(pred_score, trade_start_time)
        if pred_score is None or pred_score.empty:
            forced_empty_orders = self._build_forced_exit_orders(trade_start_time, trade_end_time)
            return TradeDecisionWO(forced_empty_orders, self)

        if self.only_tradable:
            def get_first_n(li, n, reverse=False):
                cur_n = 0
                res = []
                for si in reversed(li) if reverse else li:
                    if self.trade_exchange.is_stock_tradable(
                        stock_id=si, start_time=trade_start_time, end_time=trade_end_time
                    ):
                        res.append(si)
                        cur_n += 1
                        if cur_n >= n:
                            break
                return res[::-1] if reverse else res

            def get_last_n(li, n):
                return get_first_n(li, n, reverse=True)

            def filter_stock(li):
                return [
                    si for si in li
                    if self.trade_exchange.is_stock_tradable(
                        stock_id=si, start_time=trade_start_time, end_time=trade_end_time
                    )
                ]
        else:
            def get_first_n(li, n):
                return list(li)[:n]

            def get_last_n(li, n):
                return list(li)[-n:]

            def filter_stock(li):
                return li

        current_temp = copy.deepcopy(self.trade_position)
        sell_order_list = []
        buy_order_list = []
        cash = current_temp.get_cash()
        current_stock_list = current_temp.get_stock_list()
        forced_exit = self._qe_risk_policy.force_exit_symbols(current_stock_list, trade_start_time)
        eligible_current_stock_list = [code for code in current_stock_list if str(code) not in forced_exit]
        last = pred_score.reindex(eligible_current_stock_list).sort_values(ascending=False).index
        if self.method_buy == "top":
            today = get_first_n(
                pred_score[~pred_score.index.isin(last)].sort_values(ascending=False).index,
                self.n_drop + self.topk - len(last),
            )
        elif self.method_buy == "random":
            topk_candi = get_first_n(pred_score.sort_values(ascending=False).index, self.topk)
            candi = list(filter(lambda x: x not in last, topk_candi))
            n = self.n_drop + self.topk - len(last)
            try:
                today = np.random.choice(candi, n, replace=False)
            except ValueError:
                today = candi
        else:
            raise NotImplementedError("This type of input is not supported")

        comb = pred_score.reindex(last.union(pd.Index(today))).sort_values(ascending=False).index
        if self.method_sell == "bottom":
            sell = last[last.isin(get_last_n(comb, self.n_drop))]
        elif self.method_sell == "random":
            candi = filter_stock(last)
            try:
                sell = pd.Index(np.random.choice(candi, self.n_drop, replace=False) if len(last) else [])
            except ValueError:
                sell = candi
        else:
            raise NotImplementedError("This type of input is not supported")

        normal_sell_count = len(sell)
        sell = pd.Index(list(dict.fromkeys([*list(sell), *sorted(forced_exit)])))
        buy = today[: normal_sell_count + self.topk - len(last)]
        for code in current_stock_list:
            if not self.trade_exchange.is_stock_tradable(
                stock_id=code,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=None if self.forbid_all_trade_at_limit else OrderDir.SELL,
            ):
                continue
            if code in sell:
                time_per_step = self.trade_calendar.get_freq()
                if current_temp.get_stock_count(code, bar=time_per_step) < self.hold_thresh:
                    continue
                sell_amount = current_temp.get_stock_amount(code=code)
                sell_order = Order(
                    stock_id=code,
                    amount=sell_amount,
                    start_time=trade_start_time,
                    end_time=trade_end_time,
                    direction=Order.SELL,
                )
                if self.trade_exchange.check_order(sell_order):
                    sell_order_list.append(sell_order)
                    trade_val, trade_cost, _trade_price = self.trade_exchange.deal_order(
                        sell_order, position=current_temp
                    )
                    cash += trade_val - trade_cost

        value = cash * self.risk_degree / len(buy) if len(buy) > 0 else 0
        for code in buy:
            if not self.trade_exchange.is_stock_tradable(
                stock_id=code,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=None if self.forbid_all_trade_at_limit else OrderDir.BUY,
            ):
                continue
            buy_price = self.trade_exchange.get_deal_price(
                stock_id=code, start_time=trade_start_time, end_time=trade_end_time, direction=OrderDir.BUY
            )
            buy_amount = value / buy_price
            factor = self.trade_exchange.get_factor(stock_id=code, start_time=trade_start_time, end_time=trade_end_time)
            buy_amount = self.trade_exchange.round_amount_by_trade_unit(buy_amount, factor)
            buy_order_list.append(Order(
                stock_id=code,
                amount=buy_amount,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=Order.BUY,
            ))
        return TradeDecisionWO(sell_order_list + buy_order_list, self)

    def _build_forced_exit_orders(self, trade_start_time, trade_end_time):
        current_stock_list = self.trade_position.get_stock_list()
        forced_exit = self._qe_risk_policy.force_exit_symbols(current_stock_list, trade_start_time)
        orders = []
        for code in current_stock_list:
            if str(code) not in forced_exit:
                continue
            if not self.trade_exchange.is_stock_tradable(
                stock_id=code,
                start_time=trade_start_time,
                end_time=trade_end_time,
                direction=None if self.forbid_all_trade_at_limit else OrderDir.SELL,
            ):
                continue
            amount = self.trade_position.get_stock_amount(code=code)
            if amount is not None and float(amount) > 0:
                orders.append(
                    Order(
                        stock_id=code,
                        amount=amount,
                        start_time=trade_start_time,
                        end_time=trade_end_time,
                        direction=Order.SELL,
                    )
                )
        return orders

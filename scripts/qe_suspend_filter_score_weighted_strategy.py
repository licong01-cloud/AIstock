"""Score-weighted strategy wrappers with explicit suspend_d signal filtering."""
from __future__ import annotations

try:
    from custom_strategy import ScoreWeightedTopkStrategy
except ImportError:
    from score_weighted_strategy import ScoreWeightedTopkStrategy
try:
    from custom_strategy import ScoreWeightedTopkStrategyV2
except ImportError:
    from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2
try:
    from custom_strategy import ScoreWeightedTopkStrategyV2CapacityV1
except ImportError:
    try:
        from score_weighted_strategy_v2_capacity_v1 import ScoreWeightedTopkStrategyV2CapacityV1
    except ImportError:
        ScoreWeightedTopkStrategyV2CapacityV1 = ScoreWeightedTopkStrategyV2
import pandas as pd
from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO

from qe_event_risk_policy import QEEventRiskPolicy
from qe_suspend_filter import QESuspendFilter


class _SuspendFilterScoreWeightedMixin:
    """Filter the normalized daily score Series before score-weighted ranking."""

    def __init__(
        self,
        *args,
        filter_suspended_on_signal=False,
        suspend_filter_file=None,
        suspend_filter_strict=True,
        risk_policy_enabled=False,
        risk_policy_file=None,
        risk_policy_strict=True,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
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
        self._qe_suspend_filter_trade_time = None
        self._qe_suspend_filter_trade_end_time = None

    def generate_trade_decision(self, execute_result=None):
        trade_step = self.trade_calendar.get_trade_step()
        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)
        self._qe_suspend_filter_trade_time = trade_start_time
        self._qe_suspend_filter_trade_end_time = trade_end_time
        decision = super().generate_trade_decision(execute_result)
        base_orders = self._filter_untradable_orders(
            list(decision.get_decision()),
            trade_start_time,
            trade_end_time,
        )
        forced_orders = self._build_missing_forced_exit_orders(
            base_orders,
            trade_start_time,
            trade_end_time,
        )
        if not forced_orders and len(base_orders) == len(decision.get_decision()):
            return decision
        return TradeDecisionWO(forced_orders + base_orders, self)

    def _normalize_signal_scores(self, all_pred_scores, pred_end_time):
        scores = super()._normalize_signal_scores(all_pred_scores, pred_end_time)
        trade_time = self._qe_suspend_filter_trade_time or pred_end_time
        scores = self._qe_suspend_filter.filter_scores(
            scores,
            trade_time,
        )
        scores = self._qe_risk_policy.filter_scores(scores, trade_time)
        return self._filter_scores_without_close(scores, trade_time)

    def _get_current_price(self, stock_id, trade_step, direction):
        trade_start_time, trade_end_time = self.trade_calendar.get_step_time(trade_step)
        if self._qe_suspend_filter.is_suspended(stock_id, trade_start_time):
            return None
        if not self._has_close_price(stock_id, trade_start_time, trade_end_time):
            return None
        return super()._get_current_price(stock_id, trade_step, direction)

    def _is_missing_quote_value(self, value) -> bool:
        if value is None:
            return True
        try:
            if hasattr(value, "isna"):
                return bool(value.isna().all())
            return bool(pd.isna(value))
        except (TypeError, ValueError):
            return False

    def _has_close_price(self, stock_id, trade_start_time, trade_end_time) -> bool:
        close = self.trade_exchange.get_close(stock_id, trade_start_time, trade_end_time)
        return not self._is_missing_quote_value(close)

    def _filter_scores_without_close(self, scores, trade_start_time):
        if scores is None or scores.empty:
            return scores
        trade_end_time = self._qe_suspend_filter_trade_end_time or trade_start_time
        mask = pd.Series(
            [
                self._has_close_price(stock_id, trade_start_time, trade_end_time)
                for stock_id in scores.index
            ],
            index=scores.index,
        )
        excluded = int((~mask).sum())
        if excluded:
            self._qe_suspend_filter.logger.info(
                "[QESuspendFilter] trade_date=%s excluded=%d missing_close_price",
                self._qe_suspend_filter._date_key(trade_start_time),
                excluded,
            )
        return scores.loc[mask]

    def _is_orderable_without_warning(self, stock_id, trade_start_time, trade_end_time, direction):
        if self._qe_suspend_filter.is_suspended(stock_id, trade_start_time):
            return False
        # Qlib emits noisy get_deal_price warnings when $close is missing.  Guard
        # before valuation/order generation and let the position wait until a
        # real tradable close exists.
        if not self._has_close_price(stock_id, trade_start_time, trade_end_time):
            return False
        return self.trade_exchange.is_stock_tradable(
            stock_id=stock_id,
            start_time=trade_start_time,
            end_time=trade_end_time,
            direction=direction,
        )

    def _filter_untradable_orders(self, orders, trade_start_time, trade_end_time):
        filtered = []
        for order in orders:
            direction = getattr(order, "direction", None)
            stock_id = getattr(order, "stock_id", None)
            if stock_id is None:
                continue
            if self._is_orderable_without_warning(
                stock_id,
                trade_start_time,
                trade_end_time,
                direction=direction,
            ):
                filtered.append(order)
        return filtered

    def _build_missing_forced_exit_orders(self, existing_orders, trade_start_time, trade_end_time):
        current_stock_list = list(self.trade_position.get_stock_list())
        forced_exit = self._qe_risk_policy.force_exit_symbols(current_stock_list, trade_start_time)
        if not forced_exit:
            return []
        existing_sell = {
            str(order.stock_id)
            for order in existing_orders
            if getattr(order, "direction", None) == OrderDir.SELL
        }
        orders = []
        for code in current_stock_list:
            if str(code) not in forced_exit or str(code) in existing_sell:
                continue
            direction = None if self.forbid_all_trade_at_limit else OrderDir.SELL
            if not self._is_orderable_without_warning(code, trade_start_time, trade_end_time, direction):
                continue
            amount = self.trade_position.get_stock_amount(code)
            if amount is not None and float(amount) > 0:
                orders.append(Order(code, amount, OrderDir.SELL, trade_start_time, trade_end_time))
        return orders


class SuspendFilterScoreWeightedTopkStrategy(
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategy,
):
    pass


class SuspendFilterScoreWeightedTopkStrategyV2(
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategyV2,
):
    pass


class SuspendFilterScoreWeightedTopkStrategyV2CapacityV1(
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategyV2CapacityV1,
):
    pass

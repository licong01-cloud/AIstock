"""Optional full absolute-target strategy using the existing Qlib exchange."""
from __future__ import annotations

import json
import math
from pathlib import Path

from qe_absolute_targets import AbsoluteTargets, MANIFEST_FILE, require

from qe_event_risk_policy import QEEventRiskPolicy
from qe_suspend_filter import QESuspendFilter

SELL, BUY = 0, 1


def target_orders(*, targets, held, nav, cash, quote, buy_allowed, sell_allowed,
                  open_cost, close_cost, min_cost):
    """Sell first; intentions use real quotes and never claim expected fills."""
    require(math.isfinite(nav) and nav > 0 and math.isfinite(cash) and cash >= 0,
            "QE_ABSOLUTE_TARGET_INVALID_ACCOUNT")
    require(all(math.isfinite(x) and x >= 0 for x in (open_cost, close_cost, min_cost)),
            "QE_ABSOLUTE_TARGET_INVALID_COSTS")
    requests, events = [], []
    for stock in sorted(set(targets) | set(held)):
        amount = float(held.get(stock, 0.0))
        require(math.isfinite(amount) and amount >= 0, "QE_ABSOLUTE_TARGET_INVALID_HOLDING")
        value = targets.get(stock, 0.0) * nav
        event = {"instrument": stock, "requested_weight": targets.get(stock, 0.0),
                 "held_amount": amount, "target_value": value, "actual_fill_claimed": False}
        events.append(event)
        observation = quote(stock)
        if observation is None:
            event["reason"] = "QUOTE_UNAVAILABLE_OR_SUSPENDED"
            continue
        price, unit = observation
        require(all(math.isfinite(x) and x > 0 for x in (price, unit)), "QE_ABSOLUTE_TARGET_INVALID_QUOTE")
        delta = value / price - amount
        if abs(delta) < 1e-8:
            event["reason"] = "ALREADY_AT_TARGET"
            continue
        direction = BUY if delta > 0 else SELL
        reason = buy_allowed(stock) if direction == BUY else sell_allowed(stock)
        if reason:
            event["reason"] = reason
            continue
        requests.append((stock, direction, abs(delta), price, unit, event))
    planned_cash, orders = cash, []
    for stock, direction, desired, price, unit, event in sorted(requests, key=lambda row: (int(row[1]), row[0])):
        if direction == SELL:
            quantity = held[stock] if targets.get(stock, 0.0) == 0 else min(held[stock], math.floor(desired / unit) * unit)
        else:
            affordable = max(0.0, min(planned_cash - min_cost, planned_cash / (1 + open_cost)))
            quantity = math.floor(min(desired, affordable / price) / unit) * unit
        if quantity <= 0:
            event["reason"] = "BELOW_TRADE_UNIT_OR_CASH"
            continue
        value = quantity * price
        fee = max(value * (open_cost if direction == BUY else close_cost), min_cost)
        # A tiny forced liquidation can cost more than its proceeds. Do not
        # silently overdraw; the same real fee/cash constraint applies to both arms.
        if direction == SELL and planned_cash + value < fee:
            event["reason"] = "INSUFFICIENT_CASH_FOR_SELL_FEE"
            continue
        planned_cash += value - fee if direction == SELL else -value - fee
        require(planned_cash >= -1e-7, "QE_ABSOLUTE_TARGET_NEGATIVE_PLANNED_CASH")
        orders.append((stock, float(quantity), direction))
        event.update(reason="ABSOLUTE_TARGET_DIFFERENCE", direction=int(direction), amount=float(quantity),
                     estimated_notional=value, estimated_fee=fee)
    return orders, events, planned_cash


class AbsoluteTargetMixin:
    def __init__(self, *, manifest_file=MANIFEST_FILE, calendar_file, **kwargs):
        super().__init__(**kwargs)
        contract = json.loads(Path(manifest_file).read_text(encoding="utf-8"))
        calendar = Path(calendar_file).read_text(encoding="utf-8").splitlines()
        self.targets = AbsoluteTargets(contract, path="absolute_targets.parquet", calendar=calendar)
        self.risk = QEEventRiskPolicy(True, "qe_event_risk_policy.json", True)
        self.suspend = QESuspendFilter(True, "qe_suspend_filter.json", True)
        self._seen = set()
        self._backup_candidates = []

    def generate_trade_decision(self, execute_result=None):
        from qlib.backtest.decision import Order, TradeDecisionWO

        step = self.trade_calendar.get_trade_step()
        start, end = self.trade_calendar.get_step_time(step)
        day = str(start.date())
        require(day not in self._seen, "QE_ABSOLUTE_TARGET_DUPLICATE_DAY")
        requested, summary = self.targets.day(day)
        held = {s: float(self.trade_position.get_stock_amount(s)) for s in self.trade_position.get_stock_list()}
        forced = self.risk.force_exit_symbols(held, start)
        effective = {s: w for s, w in requested.items() if s not in forced}

        def quote(stock):
            if self.suspend.is_suspended(stock, start):
                return None
            price = self.trade_exchange.get_deal_price(stock_id=stock, start_time=start, end_time=end,
                                                      direction=BUY)
            if price is None or not math.isfinite(float(price)):
                return None
            factor = self.trade_exchange.get_factor(stock_id=stock, start_time=start, end_time=end)
            require(factor is not None and math.isfinite(float(factor)) and factor > 0,
                    "QE_ABSOLUTE_TARGET_INVALID_ADJUSTMENT_FACTOR")
            unit = self.trade_exchange.get_amount_of_trade_unit(factor=factor, stock_id=stock,
                                                                start_time=start, end_time=end)
            require(unit is not None, "QE_ABSOLUTE_TARGET_MISSING_TRADE_UNIT")
            return float(price), float(unit)

        def buy_allowed(stock):
            if not self.risk.is_buy_allowed(stock, start):
                return "PIT_OR_EVENT_BUY_BLOCK"
            if not self.trade_exchange.is_stock_tradable(stock, start, end, direction=BUY):
                return "BUY_NOT_TRADABLE"
            return None

        def sell_allowed(stock):
            if not self.trade_exchange.is_stock_tradable(stock, start, end, direction=SELL):
                return "SELL_NOT_TRADABLE"
            if stock not in forced and self.trade_position.get_stock_count(stock, bar="day") < 1:
                return "HOLD_THRESH"
            return None

        nav = float(self.trade_position.calculate_value())
        cash = float(self.trade_position.get_cash())
        raw_orders, events, planned_cash = target_orders(
            targets=effective, held=held, nav=nav, cash=cash, quote=quote,
            buy_allowed=buy_allowed, sell_allowed=sell_allowed,
            open_cost=float(self.trade_exchange.open_cost), close_cost=float(self.trade_exchange.close_cost),
            min_cost=float(self.trade_exchange.min_cost))
        summary.update(arm=self.targets.contract["arm"], requested_targets=requested, effective_targets=effective,
                       forced_exit=sorted(forced), predecision_nav=nav, predecision_cash=cash,
                       held_amounts=held, rebalance=events, planned_cash=planned_cash,
                       planned_sell_proceeds_are_not_fills=True)
        with Path("qe_absolute_target_actions.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(summary, ensure_ascii=False, allow_nan=False) + "\n")
        self._seen.add(day)
        self._backup_candidates = []
        return TradeDecisionWO([Order(s, a, d, start, end) for s, a, d in raw_orders], self)


def __getattr__(name):
    if name == "AbsoluteTargetStrategy":
        from qlib.strategy.base import BaseStrategy

        class AbsoluteTargetStrategy(AbsoluteTargetMixin, BaseStrategy):
            pass

        return AbsoluteTargetStrategy
    raise AttributeError(name)

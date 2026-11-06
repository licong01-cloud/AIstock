"""One absolute-target order contract for B* and frozen-list relative loss R."""
from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from qe_l2_relative_consumer import CONTRACT, RelativeInputs, finite, require, validate_base


class RelativeMixin:
    def __init__(self, *args, qe_l2_relative_manifest_file=None,
                 qe_l2_relative_action_log="qe_l2_relative_actions.jsonl", **kwargs):
        require(qe_l2_relative_manifest_file is not None, "QE_L2_RELATIVE_MANIFEST_REQUIRED")
        self._relative_inputs = RelativeInputs(qe_l2_relative_manifest_file)
        require(bool(self._relative_inputs.manifest.get("execution_files_sha256")),
                "QE_L2_RELATIVE_EXECUTION_CODE_REQUIRED")
        self._relative_log = Path(qe_l2_relative_action_log)
        require(not self._relative_log.is_absolute() and ".." not in self._relative_log.parts,
                "QE_L2_RELATIVE_LOG_PATH")
        self._relative_seen = set()
        self._relative_base = None
        super().__init__(*args, **kwargs)
        require(self.max_weight == 0.05, "QE_L2_RELATIVE_STRATEGY_WEIGHT_CAP")

    def _adjust_target_weight_map(self, weight_map, trade_start_time, *, ranked):
        require(self._relative_inputs.manifest["arm"] == "BSTAR", "QE_L2_RELATIVE_SELECTION_ON_R")
        base = {str(s): float(w) for s, w in weight_map.items() if w > 0}
        ordered = [str(s) for s in ranked.index if str(s) in base]
        validate_base(base, ordered)
        self._relative_base = base, ordered
        return dict(weight_map)

    def _build_additional_rebalance_orders(self, **kwargs):
        # The selected legacy AF is used only to produce its original targets.
        # Both arms' final orders are generated below, by exactly the same code.
        return []

    def _relative_orders(self, targets, ordered, step, start, end):
        from qlib.backtest.decision import Order, OrderDir

        held = {str(s): finite(float(self.trade_position.get_stock_amount(s)), str(s))
                for s in self.trade_position.get_stock_list()}
        nav = finite(float(self.trade_position.calculate_value()), "predecision_nav")
        cash = finite(float(self.trade_position.get_cash()), "cash")
        require(nav > 0, "QE_L2_RELATIVE_NONPOSITIVE_NAV")
        forced = self._qe_risk_policy.force_exit_symbols(list(held), start)
        require(not (set(targets) & set(forced)), "QE_L2_RELATIVE_FORCED_EXIT_IN_TARGETS")
        events, requests = [], []
        sequence = list(ordered) + sorted(set(held) - set(targets))
        for stock in sequence:
            target_value = targets.get(stock, 0.0) * nav
            amount = held.get(stock, 0.0)
            event = {"instrument": stock, "target_value": target_value,
                     "held_amount": amount, "order_generated": False, "actual_fill_claimed": False}
            events.append(event)
            # Real exchange quote and trading constraints; never invent a price.
            mark = self._get_current_price(stock, step, OrderDir.SELL)
            if mark is None:
                event["reason"] = "QUOTE_UNAVAILABLE_OR_SUSPENDED"
                continue
            require(math.isfinite(float(mark)) and float(mark) > 0, "QE_L2_RELATIVE_PRICE_INVALID")
            delta = target_value - amount * float(mark)
            if abs(delta) < 1e-8:
                event["reason"] = "ALREADY_AT_TARGET"
                continue
            direction = OrderDir.BUY if delta > 0 else OrderDir.SELL
            if direction == OrderDir.BUY and not self._qe_risk_policy.is_buy_allowed(stock, start):
                event["reason"] = "PIT_OR_EVENT_BUY_BLOCK"
                continue
            if direction == OrderDir.SELL and stock not in forced and not self._can_sell_under_hold_thresh(stock, start):
                event["reason"] = "HOLD_THRESH"
                continue
            exchange_direction = None if self.forbid_all_trade_at_limit else direction
            if not self._is_orderable_without_warning(stock, start, end, exchange_direction):
                event["reason"] = "NOT_TRADABLE"
                continue
            price = self._get_current_price(stock, step, direction)
            require(price is not None and math.isfinite(float(price)) and float(price) > 0,
                    "QE_L2_RELATIVE_PRICE_INVALID")
            price = float(price)
            if direction == OrderDir.BUY and not self.min_trade_price <= price <= self.max_trade_price:
                event["reason"] = "PRICE_BOUND"
                continue
            factor = self._get_current_factor(stock, step)
            require(factor is not None and math.isfinite(float(factor)) and float(factor) > 0,
                    "QE_L2_RELATIVE_FACTOR_INVALID")
            unit = self.trade_exchange.get_amount_of_trade_unit(
                factor=factor, stock_id=stock, start_time=start, end_time=end)
            require(unit is not None and math.isfinite(float(unit)) and float(unit) > 0,
                    "QE_L2_RELATIVE_TRADE_UNIT_INVALID")
            desired = max(0.0, target_value / price - amount) if direction == OrderDir.BUY else max(0.0, amount - target_value / price)
            requests.append((stock, direction, price, float(unit), desired, event))
        open_cost = finite(float(self.trade_exchange.open_cost), "open_cost")
        close_cost = finite(float(self.trade_exchange.close_cost), "close_cost")
        min_cost = finite(float(self.trade_exchange.min_cost), "min_cost")
        planned_cash, orders = cash, []
        # Use a deterministic shared order, sell first. Expected proceeds only
        # budget order intentions; the unchanged executor checks actual cash.
        for stock, direction, price, unit, desired, event in sorted(requests, key=lambda r: int(r[1])):
            if direction == OrderDir.SELL:
                quantity = held[stock] if stock not in targets else min(held[stock], math.floor(desired / unit) * unit)
            else:
                budget = max(0.0, min(planned_cash - min_cost, planned_cash / (1 + open_cost),
                                      float(self.max_single_order_value)))
                quantity = math.floor(min(desired, budget / price) / unit) * unit
            if quantity <= 0:
                event["reason"] = "BELOW_TRADE_UNIT_OR_CASH"
                continue
            value = quantity * price
            fee = max(value * (open_cost if direction == OrderDir.BUY else close_cost), min_cost)
            planned_cash += value - fee if direction == OrderDir.SELL else -value - fee
            require(planned_cash >= -1e-7, "QE_L2_RELATIVE_PLANNED_CASH_NEGATIVE")
            orders.append(Order(stock, float(quantity), direction, start, end))
            event.update(order_generated=True, direction=int(direction), amount=float(quantity),
                         estimated_notional=value, estimated_fee=fee, reason="ABSOLUTE_TARGET_DIFFERENCE")
        return orders, events, {"predecision_nav": nav, "predecision_cash": cash,
                                "planned_cash_after_orders": planned_cash,
                                "planned_sell_proceeds_are_not_fills": True}

    def generate_trade_decision(self, execute_result=None):
        from qlib.backtest.decision import TradeDecisionWO

        inputs = self._relative_inputs
        step = self.trade_calendar.get_trade_step()
        start, end = self.trade_calendar.get_step_time(step)
        day = str(pd.Timestamp(start).date())
        require(day in inputs.as_of and day not in self._relative_seen, "QE_L2_RELATIVE_DUPLICATE_OR_UNKNOWN_DAY")
        self._qe_suspend_filter_trade_time = start
        self._qe_suspend_filter_trade_end_time = end
        self._relative_base = None
        if inputs.manifest["arm"] == "BSTAR":
            super().generate_trade_decision(execute_result)
            if self._relative_base is None:
                require(not self.trade_position.get_stock_list(), "QE_L2_RELATIVE_TARGET_MISSING_WITH_HOLDINGS")
                base, ordered = {}, []
            else:
                base, ordered = self._relative_base
        else:
            row = inputs.references[day]
            base, ordered = dict(row["base_targets"]), list(row["af_order"])
        targets, groups, risk = inputs.targets(day, base, ordered)
        orders, events, budget = self._relative_orders(targets, ordered, step, start, end)
        # No alternative name may enter the frozen target list through inner
        # execution's backup candidates. Unfilled budget remains cash.
        self._backup_candidates = []
        action = {
            "contract": CONTRACT, "trade_date": day, "as_of_date": inputs.as_of[day],
            "arm": inputs.manifest["arm"], "seed": inputs.manifest["seed"],
            "base_contract_sha256": inputs.manifest["base_contract_sha256"],
            "execution_sha256": inputs.manifest["execution_sha256"],
            "base_targets": base, "af_order": ordered, "target_weights": targets, "groups": groups,
            "target_exposure": math.fsum(targets.values()), "target_cash": 1 - math.fsum(targets.values()),
            "risk_inputs": risk, "rebalance": events, "order_budget": budget,
            "planned_orders": [{"instrument": str(o.stock_id), "direction": int(o.direction),
                                "amount": float(o.amount), "actual_fill_claimed": False} for o in orders],
        }
        with self._relative_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(action, ensure_ascii=False, allow_nan=False) + "\n")
        self._relative_seen.add(day)
        return TradeDecisionWO(orders, self)


def __getattr__(name):
    if name == "L2RelativeScoreWeightedTopkStrategyV2":
        from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2

        class L2RelativeScoreWeightedTopkStrategyV2(RelativeMixin, SuspendFilterScoreWeightedTopkStrategyV2):
            pass

        return L2RelativeScoreWeightedTopkStrategyV2
    raise AttributeError(name)

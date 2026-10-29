"""Optional buy/add gate; preserves AF scores and the original sell contract."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from qlib.backtest.decision import OrderDir, TradeDecisionWO

from qe_stock_probability_gate import ACTION_FILE, StockProbabilityInputs, require
from qe_suspend_filter_score_weighted_strategy import _SuspendFilterScoreWeightedMixin
try:
    from custom_strategy import ScoreWeightedTopkStrategyV2
except ImportError:
    from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2


class StockProbabilityGateMixin:
    def __init__(self, *args, qe_stock_probability_enabled=False,
                 qe_stock_probability_manifest_file=None, **kwargs):
        require(type(qe_stock_probability_enabled) is bool, "ENABLED_INVALID")
        self._stock_probability_enabled = qe_stock_probability_enabled
        self._stock_probability_inputs = None
        self._stock_probability_seen = set()
        if qe_stock_probability_enabled:
            require(bool(qe_stock_probability_manifest_file), "MANIFEST_REQUIRED")
            self._stock_probability_inputs = StockProbabilityInputs(qe_stock_probability_manifest_file)
            self._stock_probability_log = Path(ACTION_FILE)
            require(not self._stock_probability_log.exists(), "EXISTING_ACTION_LOG")
        super().__init__(*args, **kwargs)

    def _risk(self, sid):
        return self._stock_probability_inputs.evaluate(sid, self._probability_day, self._probability_signal)

    def _prepare_buy_candidates(self, ranked, current_holdings, buy_candidates):
        original = super()._prepare_buy_candidates(ranked, current_holdings, buy_candidates)
        if not self._stock_probability_enabled:
            return original
        self._probability_original = [str(s) for s, _ in original]
        if not original:
            return original
        held, original_ids = set(current_holdings), set(self._probability_original)
        chosen = []
        # Keep the selected source's original tie ordering, then replenish in
        # its ranked order. Even the older catalog strategy remains unchanged
        # when every original candidate is permitted.
        candidates = list(original) + [(sid, float(score)) for sid, score in ranked.items()
                                      if sid not in held and str(sid) not in original_ids]
        ranks = {sid: i for i, sid in enumerate(ranked.index, start=1)}
        for sid, score in candidates:
            if sid in held:
                continue
            row = self._risk(sid)
            self._probability_scanned.append({**row, "af_rank": ranks[sid], "af_score": float(score),
                                               "original_buy_candidate": str(sid) in original_ids})
            if row["allowed"]:
                chosen.append((sid, float(score)))
            if len(chosen) == len(original):
                break
        self._probability_selected = [str(s) for s, _ in chosen]
        return chosen

    def generate_trade_decision(self, execute_result=None):
        if not self._stock_probability_enabled:
            return super().generate_trade_decision(execute_result)
        step = self.trade_calendar.get_trade_step()
        start, _ = self.trade_calendar.get_step_time(step)
        _, signal_end = self.trade_calendar.get_step_time(step, shift=1)
        day, signal = self._stock_probability_inputs.validate_time(start, signal_end)
        require(day not in self._stock_probability_seen, "DUPLICATE_DECISION")
        self._probability_day, self._probability_signal = day, signal
        self._probability_original, self._probability_selected, self._probability_scanned = [], [], []
        held = set(self.trade_position.get_stock_list())
        decision = super().generate_trade_decision(execute_result)
        retained, order_actions = [], []
        for order in decision.get_decision():
            if order.direction == OrderDir.BUY:
                row = self._risk(order.stock_id)
                order_actions.append({**row, "requested_amount": float(order.amount),
                                      "is_add": order.stock_id in held, "planned_order_is_fill": False})
                if not row["allowed"]:
                    continue
            retained.append(order)
        backups, backup_actions = [], []
        for sid, score in getattr(self, "_backup_candidates", []):
            row = self._risk(sid)
            backup_actions.append(row)
            if row["allowed"]:
                backups.append((sid, score))
        self._backup_candidates = backups
        record = {
            "trade_date": str(pd.Timestamp(day).date()), "signal_date": str(pd.Timestamp(signal).date()),
            "contract": self._stock_probability_inputs.manifest["contract"],
            "arm": self._stock_probability_inputs.manifest["arm"],
            "threshold": self._stock_probability_inputs.threshold,
            "original_buy_candidates": self._probability_original, "scanned_candidates": self._probability_scanned,
            "selected_buy_candidates": self._probability_selected, "buy_order_actions": order_actions,
            "backup_actions": backup_actions, "retained_sell_orders": sum(o.direction == OrderDir.SELL for o in retained),
            "planned_order_is_fill": False,
        }
        with self._stock_probability_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
        self._stock_probability_seen.add(day)
        if len(retained) == len(decision.get_decision()):
            return decision
        return TradeDecisionWO(retained, self)


class StockProbabilityScoreWeightedTopkStrategyV2(StockProbabilityGateMixin, ScoreWeightedTopkStrategyV2):
    pass


class StockProbabilitySuspendFilterScoreWeightedTopkStrategyV2(
    StockProbabilityGateMixin, _SuspendFilterScoreWeightedMixin, ScoreWeightedTopkStrategyV2
):
    pass

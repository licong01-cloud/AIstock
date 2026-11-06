"""Post-decision natural-buy consumer for the exact frozen score-weighted strategy."""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

import numpy as np

from qe_trend_entry_admission import ACTION_FILE, TrendEntryInputs, canonical_sha, require


def order_record(order):
    return {"stock_id": str(order.stock_id), "direction": int(order.direction), "amount": float(order.amount),
            "start_time": str(order.start_time), "end_time": str(order.end_time)}


class TrendEntryMixin:
    def __init__(self, *args, qe_trend_entry_manifest_file, **kwargs):
        self._trend_inputs = TrendEntryInputs(qe_trend_entry_manifest_file)
        self._trend_log = Path(qe_trend_entry_manifest_file).parent / ACTION_FILE
        require(not self._trend_log.exists(), "EXISTING_ACTION_LOG")
        self._trend_seen = set()
        super().__init__(*args, **kwargs)

    def _qe_check_trend_prediction(self, predictions):
        self._trend_inputs.check_prediction(predictions, self._trend_signal, self._trend_day)

    def _qe_record_trend_slot(self, sid, amount, price, factor, target_value, amount_unit):
        require(sid not in self._trend_slots, "DUPLICATE_SLOT")
        require(factor is not None and np.isfinite(float(factor)) and float(factor) > 0, "SLOT_FACTOR")
        conversion = float(self._shares_to_adjusted_amount(1.0, factor))
        require(np.isfinite(conversion) and conversion > 0 and np.isfinite(float(amount_unit))
                and float(amount_unit) > 0 and np.isfinite(float(price)) and float(price) > 0, "SLOT_UNITS")
        budget = float(amount) / conversion * float(price)
        require(np.isfinite(budget) and budget > 0 and budget <= float(target_value) + 1e-6, "SLOT_BUDGET")
        self._trend_slots[sid] = {"amount": float(amount), "price": float(price), "factor": float(factor),
                                  "amount_unit": float(amount_unit), "conversion": conversion,
                                  "target_value": float(target_value), "budget": budget}

    def _qe_capture_trend_context(self, ranked, scores, held, valid, sells, buys, all_sells,
                                 step, start, end, threshold):
        require(set(held) == self._trend_held and np.isfinite(float(threshold)), "CONTEXT_STATE")
        paired = len(valid) >= self.topk
        require(not paired or len(sells) == len(buys), "PAIR_MAPPING")
        self._trend_context = {"ranked": ranked, "scores": scores, "paired": paired,
                               "pairs": dict(zip(buys, sells)) if paired else {},
                               "sells": set(all_sells), "step": step, "start": start, "end": end,
                               "threshold": float(threshold), "actual_buys": list(buys),
                               "actual_sells": list(sells)}

    def _trend_quote_replacement(self, sid, budget):
        from qlib.backtest.decision import Order, OrderDir
        c = self._trend_context
        if not self._is_orderable_without_warning(sid, c["start"], c["end"], OrderDir.BUY):
            return None, {"stage": "UNTRADABLE"}
        price = self._get_current_price(sid, c["step"], OrderDir.BUY)
        if price is None or not np.isfinite(float(price)) or float(price) <= 0:
            return None, {"stage": "INVALID_PRICE"}
        if not self.min_trade_price <= float(price) <= self.max_trade_price:
            return None, {"stage": "PRICE_RANGE"}
        factor = self._get_current_factor(sid, c["step"])
        require(factor is not None and np.isfinite(float(factor)) and float(factor) > 0, "REPLACEMENT_FACTOR")
        conversion = float(self._shares_to_adjusted_amount(1., factor))
        require(np.isfinite(conversion) and conversion > 0, "REPLACEMENT_UNITS")
        cash = float(self.trade_position.get_cash())
        require(np.isfinite(cash) and cash >= 0, "CASH_INVALID")
        amount = min(self._shares_to_adjusted_amount(budget / float(price), factor),
                     self._shares_to_adjusted_amount(cash / float(price), factor))
        unit = self.trade_exchange.get_amount_of_trade_unit(
            factor=factor, stock_id=sid, start_time=c["start"], end_time=c["end"])
        if unit is None or not np.isfinite(float(unit)) or float(unit) <= 0:
            unit = self._shares_to_adjusted_amount(self.lot_size, factor)
        require(unit is not None and np.isfinite(float(unit)) and float(unit) > 0, "REPLACEMENT_LOT")
        amount = float(np.floor(amount / float(unit)) * float(unit))
        detail = {"price": float(price), "factor": float(factor), "amount_unit": float(unit),
                  "conversion": conversion, "amount": amount, "budget": budget,
                  "notional": amount / conversion * float(price), "cash_before": cash}
        require(np.isfinite(amount) and detail["notional"] <= min(budget, cash) + 1e-6, "REPLACEMENT_BUDGET")
        if amount <= 0:
            return None, {**detail, "stage": "INSUFFICIENT_LOT_OR_CASH"}
        return Order(sid, amount, OrderDir.BUY, c["start"], c["end"]), {**detail, "stage": "FORMED"}

    def _trend_replace(self, original, occupied, excluded):
        c, sid = self._trend_context, original.stock_id
        require(c is not None and sid in self._trend_slots and sid in c["actual_buys"], "UNRECORDED_NATURAL_BUY")
        slot = self._trend_slots[sid]
        require(float(original.amount) == slot["amount"], "FORMED_ORDER_CHANGED")
        sold = c["pairs"].get(sid)
        sell_score = float(c["scores"][sold]) if sold is not None else None
        counts, examples = Counter(), []
        row = {"original_stock": str(sid), "original_slot": slot, "paired_sell": sold,
               "original_sell_score": sell_score, "threshold": c["threshold"] if c["paired"] else None}
        for rank, (candidate, score) in enumerate(c["ranked"].items(), start=1):
            if candidate in occupied or candidate in excluded:
                counts["HELD_SELL_OR_OCCUPIED"] += 1
                continue
            score = float(score)
            require(np.isfinite(score), "NONFINITE_RANKED_SCORE")
            if sold is not None and not score - sell_score > c["threshold"]:
                # Stable descending order proves every remaining candidate fails this same slot.
                counts["THRESHOLD_STOP"] += 1
                break
            condition = self._trend_inputs.evaluate(candidate, self._trend_signal)
            if not condition["allowed"]:
                counts["QUALIFICATION_NA" if condition["availability"] == "unavailable" else "QUALIFICATION_FALSE"] += 1
                continue
            replacement, quote = self._trend_quote_replacement(candidate, slot["budget"])
            counts[quote["stage"]] += 1
            if replacement is not None:
                return replacement, {**row, "replacement": str(candidate), "replacement_rank": rank,
                                     "replacement_score": score, "replacement_qualification": condition, "quote": quote,
                                     "scan_counts": dict(counts), "quote_failure_examples": examples}
            # Bounded diagnostic examples, not a truncation of the candidate set or scan.
            if len(examples) < 20:
                examples.append({"stock": str(candidate), "rank": rank, **quote})
        return None, {**row, "replacement": None, "reason": "NO_ADMISSIBLE_REPLACEMENT",
                      "scan_counts": dict(counts), "quote_failure_examples": examples}

    def generate_trade_decision(self, execute_result=None):
        from qlib.backtest.decision import OrderDir, TradeDecisionWO
        step = self.trade_calendar.get_trade_step()
        start, _ = self.trade_calendar.get_step_time(step)
        signal_start, _ = self.trade_calendar.get_step_time(step, shift=1)
        self._trend_day, self._trend_signal = self._trend_inputs.validate_time(start, signal_start)
        require(self._trend_day not in self._trend_seen, "DUPLICATE_DECISION")
        self._trend_held = set(self.trade_position.get_stock_list())
        self._trend_slots, self._trend_context = {}, None
        original = super().generate_trade_decision(execute_result)
        orders = list(original.get_decision())
        require(all(o.direction in (OrderDir.BUY, OrderDir.SELL) for o in orders), "UNKNOWN_ACTION")
        require(len({o.stock_id for o in orders if o.direction == OrderDir.BUY}) ==
                sum(o.direction == OrderDir.BUY for o in orders), "DUPLICATE_BUY")
        sells = {o.stock_id for o in orders if o.direction == OrderDir.SELL}
        excluded = self._trend_held | sells
        if self._trend_context:
            excluded |= self._trend_context["sells"]
        qualifications, occupied, actions = {}, set(), []
        # Reserve every unaffected original BUY before scanning any replacements.
        for order in orders:
            if order.direction != OrderDir.BUY:
                continue
            if order.stock_id in self._trend_held:
                occupied.add(order.stock_id)
                continue
            require(order.stock_id not in sells, "SAME_DAY_REBUY")
            q = self._trend_inputs.evaluate(order.stock_id, self._trend_signal)
            qualifications[order.stock_id] = q
            if q["allowed"]:
                occupied.add(order.stock_id)
        retained = []
        for order in orders:
            if order.direction != OrderDir.BUY:
                retained.append(order)
                continue
            if order.stock_id in self._trend_held:
                retained.append(order)
                actions.append({"stock": str(order.stock_id), "action": "KEEP_ORIGINAL_ADD"})
                continue
            q = qualifications[order.stock_id]
            if q["allowed"]:
                retained.append(order)
                actions.append({"stock": str(order.stock_id), "action": "KEEP_ORIGINAL_NEW_BUY", "qualification": q,
                                "original_slot": self._trend_slots.get(order.stock_id)})
            else:
                replacement, detail = self._trend_replace(order, occupied, excluded)
                actions.append({"action": "REPLACE" if replacement is not None else "WITHHOLD",
                                "qualification": q, **detail})
                if replacement is not None:
                    retained.append(replacement)
                    occupied.add(replacement.stock_id)
        require([o for o in retained if o.direction == OrderDir.SELL] ==
                [o for o in orders if o.direction == OrderDir.SELL], "SELL_CHANGED")
        backups = []
        for sid, score in getattr(self, "_backup_candidates", []):
            if sid not in excluded and sid not in occupied and self._trend_inputs.evaluate(sid, self._trend_signal)["allowed"]:
                backups.append((sid, score))
        self._backup_candidates = backups
        for order in retained:
            if order.direction == OrderDir.BUY and order.stock_id not in self._trend_held:
                require(self._trend_inputs.evaluate(order.stock_id, self._trend_signal)["allowed"], "FINAL_NEW_BUY_NOT_ALLOWED")
        base_records, final_records = [order_record(o) for o in orders], [order_record(o) for o in retained]
        context = self._trend_context
        original_context = None if context is None else {
            "paired_mode": context["paired"], "actual_sells": context["actual_sells"],
            "actual_buys": context["actual_buys"], "pairs": context["pairs"], "threshold": context["threshold"],
            "all_planned_sells": sorted(context["sells"]), "formed_slots": self._trend_slots,
        }
        record = {"contract": self._trend_inputs.manifest["contract"], "arm": self._trend_inputs.manifest["arm"],
                  "seed": self._trend_inputs.manifest["seed"], "trade_date": self._trend_day, "signal_date": self._trend_signal,
                  "held_before": sorted(self._trend_held), "original_orders": base_records,
                  "original_decision_sha256": canonical_sha(base_records), "final_orders": final_records,
                  "original_context": original_context,
                  "actions": actions, "cash_before_execution": float(self.trade_position.get_cash()),
                  "actual_fills": "USE_ACCOUNT_EXECUTION_LEDGER", "cash_after_execution": "UNKNOWN_AT_DECISION",
                  "planned_order_is_fill": False, "tail_budget_source": "FINAL_OUTER_BUY_ONLY",
                  "backup_consumed_by_tail_boost": False}
        with self._trend_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
        self._trend_seen.add(self._trend_day)
        return original if retained == orders else TradeDecisionWO(retained, self)


def __getattr__(name):
    if name != "TrendEntryScoreWeightedTopkStrategyV2":
        raise AttributeError(name)
    from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2

    class TrendEntryScoreWeightedTopkStrategyV2(TrendEntryMixin, SuspendFilterScoreWeightedTopkStrategyV2):
        pass

    return TrendEntryScoreWeightedTopkStrategyV2

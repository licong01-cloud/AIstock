"""Member loss affects natural new-buy amounts only; inherited order ownership."""
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
from qe_l2_member_loss_consumer import CONTRACT, MemberEntryInputs, adjust_member_entry_budgets, require


class MemberEntryMixin:
    def __init__(
        self, *args, qe_l2_member_loss_manifest_file=None,
        qe_l2_member_loss_action_log="qe_l2_member_loss_actions.jsonl", **kwargs
    ):
        require(qe_l2_member_loss_manifest_file is not None, "MANIFEST_REQUIRED")
        self._member_inputs = MemberEntryInputs(qe_l2_member_loss_manifest_file)
        self._member_log = Path(qe_l2_member_loss_action_log)
        require(not self._member_log.is_absolute() and ".." not in self._member_log.parts, "LOG_PATH")
        self._member_seen = set()
        self._member_audit = None
        super().__init__(*args, **kwargs)
        require(self.max_weight == 0.05, "MAX_WEIGHT")

    def _qe_adjust_natural_entry(
        self, *, decision_date, ordered_targets, natural_new_buys, base_budgets_cny, nav_cny, held_before
    ):
        require(not set(natural_new_buys) & set(held_before), "EXISTING_HOLDING")
        require(self._member_audit is None, "REPEATED_BUDGET_HOOK")
        day = str(pd.Timestamp(decision_date).date())
        inputs = self._member_inputs
        require(day in inputs.execution_as_of, "EXECUTION_DAY")
        result, audit = adjust_member_entry_budgets(
            day, inputs.as_of[day], ordered_targets, natural_new_buys, base_budgets_cny, float(nav_cny),
            {"max_weight": self.max_weight, "max_single_order_value": float(self.max_single_order_value)},
            inputs.risk_context(day, natural_new_buys),
        )
        audit["held_before"] = sorted(map(str, held_before))
        self._member_audit = audit
        return result

    def generate_trade_decision(self, execute_result=None):
        from qlib.backtest.decision import OrderDir

        inputs = self._member_inputs
        step = self.trade_calendar.get_trade_step()
        start, _ = self.trade_calendar.get_step_time(step)
        day = str(pd.Timestamp(start).date())
        require(day in inputs.execution_as_of and day not in self._member_seen, "DUPLICATE_OR_UNKNOWN_DAY")
        previous, _ = self.trade_calendar.get_step_time(step, shift=1)
        require(str(pd.Timestamp(previous).date()) == inputs.as_of[day], "EXTRA_LAG")
        self._member_audit = None
        decision = super().generate_trade_decision(execute_result)
        if self._member_audit is None:
            require(all(o.direction == OrderDir.SELL for o in decision.get_decision()), "BUYS_WITHOUT_BUDGET_HOOK")
            self._qe_adjust_natural_entry(
                decision_date=start, ordered_targets=[], natural_new_buys=[], base_budgets_cny={},
                nav_cny=float(self.trade_position.calculate_value()), held_before=self.trade_position.get_stock_list(),
            )
        audit = dict(self._member_audit)
        audit.update(
            contract=CONTRACT, arm=inputs.manifest["arm"], seed=inputs.manifest["seed"],
            execution_sha256=inputs.manifest["execution_sha256"],
            planned_orders=[{"instrument": str(o.stock_id), "direction": int(o.direction), "amount": float(o.amount)}
                            for o in decision.get_decision()],
        )
        with self._member_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(audit, ensure_ascii=False, allow_nan=False) + "\n")
        self._member_seen.add(day)
        return decision


def __getattr__(name):
    if name == "L2MemberEntryScoreWeightedTopkStrategyV2":
        from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2

        class L2MemberEntryScoreWeightedTopkStrategyV2(MemberEntryMixin, SuspendFilterScoreWeightedTopkStrategyV2):
            pass

        return L2MemberEntryScoreWeightedTopkStrategyV2
    raise AttributeError(name)

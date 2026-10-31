"""Natural-entry hook only; the inherited strategy remains the sole order owner."""

from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from qe_l2_natural_entry_consumer import CONTRACT, NaturalEntryInputs, adjust_natural_entry_budgets, require


class NaturalEntryMixin:
    def __init__(
        self,
        *args,
        qe_l2_natural_entry_manifest_file=None,
        qe_l2_natural_entry_action_log="qe_l2_natural_entry_actions.jsonl",
        **kwargs,
    ):
        require(qe_l2_natural_entry_manifest_file is not None, "QE_L2_NATURAL_MANIFEST_REQUIRED")
        self._natural_inputs = NaturalEntryInputs(qe_l2_natural_entry_manifest_file)
        require(
            bool(self._natural_inputs.manifest.get("execution_files_sha256")), "QE_L2_NATURAL_EXECUTION_CODE_REQUIRED"
        )
        self._natural_log = Path(qe_l2_natural_entry_action_log)
        require(not self._natural_log.is_absolute() and ".." not in self._natural_log.parts, "QE_L2_NATURAL_LOG_PATH")
        self._natural_seen = set()
        self._natural_audit = None
        super().__init__(*args, **kwargs)
        require(self.max_weight == 0.05, "QE_L2_NATURAL_MAX_WEIGHT")

    def _qe_adjust_natural_entry(
        self, *, decision_date, ordered_targets, natural_new_buys, base_budgets_cny, nav_cny, held_before
    ):
        require(not set(natural_new_buys) & set(held_before), "QE_L2_NATURAL_EXISTING_HOLDING")
        require(self._natural_audit is None, "QE_L2_NATURAL_REPEATED_BUDGET_HOOK")
        day = str(pd.Timestamp(decision_date).date())
        inputs = self._natural_inputs
        result, audit = adjust_natural_entry_budgets(
            day,
            inputs.as_of[day],
            ordered_targets,
            natural_new_buys,
            base_budgets_cny,
            float(nav_cny),
            {"max_weight": self.max_weight, "max_single_order_value": float(self.max_single_order_value)},
            inputs.risk_context(day, natural_new_buys),
        )
        audit["held_before"] = sorted(map(str, held_before))
        self._natural_audit = audit
        return result

    def generate_trade_decision(self, execute_result=None):
        from qlib.backtest.decision import OrderDir

        inputs = self._natural_inputs
        step = self.trade_calendar.get_trade_step()
        start, _ = self.trade_calendar.get_step_time(step)
        day = str(pd.Timestamp(start).date())
        require(day in inputs.as_of and day not in self._natural_seen, "QE_L2_NATURAL_DUPLICATE_OR_UNKNOWN_DAY")
        previous, _ = self.trade_calendar.get_step_time(step, shift=1)
        require(str(pd.Timestamp(previous).date()) == inputs.as_of[day], "QE_L2_NATURAL_EXTRA_LAG")
        self._natural_audit = None
        decision = super().generate_trade_decision(execute_result)
        if self._natural_audit is None:
            require(
                all(order.direction == OrderDir.SELL for order in decision.get_decision()),
                "QE_L2_NATURAL_BUYS_WITHOUT_BUDGET_HOOK",
            )
            # Empty signals can still have mandatory PIT/ST exits from the
            # inherited wrapper. Keep these orders and read the complete risk day.
            self._qe_adjust_natural_entry(
                decision_date=start,
                ordered_targets=[],
                natural_new_buys=[],
                base_budgets_cny={},
                nav_cny=float(self.trade_position.calculate_value()),
                held_before=self.trade_position.get_stock_list(),
            )
        audit = dict(self._natural_audit)
        audit.update(
            contract=CONTRACT,
            arm=inputs.manifest["arm"],
            seed=inputs.manifest["seed"],
            execution_sha256=inputs.manifest["execution_sha256"],
            planned_orders=[
                {"instrument": str(o.stock_id), "direction": int(o.direction), "amount": float(o.amount)}
                for o in decision.get_decision()
            ],
        )
        with self._natural_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(audit, ensure_ascii=False, allow_nan=False) + "\n")
        self._natural_seen.add(day)
        return decision


def __getattr__(name):
    if name == "L2NaturalEntryScoreWeightedTopkStrategyV2":
        from qe_suspend_filter_score_weighted_strategy import SuspendFilterScoreWeightedTopkStrategyV2

        class L2NaturalEntryScoreWeightedTopkStrategyV2(NaturalEntryMixin, SuspendFilterScoreWeightedTopkStrategyV2):
            pass

        return L2NaturalEntryScoreWeightedTopkStrategyV2
    raise AttributeError(name)

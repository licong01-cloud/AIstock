from decimal import Decimal as D

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError, PositionState, cutoff_on
from backend.services.position_timing.action_value_advice import decide_stock_day, public_advice
from backend.services.position_timing.action_value_model import HEADS


class RecordingModel:
    """Unit-test decision arithmetic only; never a research/serving artifact."""
    metadata = {"model_sha256": "1" * 64}

    def __init__(self, entry=5, exit=-5):
        self.entry, self.exit = entry, exit
        self.objectives = []

    def predict(self, frame, objectives, *, decision_as_of):
        self.objectives.extend(objectives)
        self.frame = frame
        return np.asarray([self.entry if head == HEADS[0] else self.exit for head in objectives])


def inputs():
    index = pd.bdate_range("2024-01-01", periods=60)
    bars = pd.DataFrame({"open": 10., "high": 10.1, "low": 9.9, "close": 10.,
                         "volume": 1000., "factor": 1.}, index=index)
    return {"symbol": "000001.SZ", "state": PositionState(1000, 1000, D(20000), D(30000)),
            "bars": bars, "benchmark": pd.Series(3000., index=index),
            "decision_as_of": cutoff_on(index[-1].date())}


def test_both_heads_form_one_stock_action_not_two_conflicting_cards():
    model = RecordingModel(entry=2, exit=5)
    decision = decide_stock_day(**inputs(), model=model)
    assert set(model.objectives) == set(HEADS)
    assert decision.action == "REDUCE"  # equal-valued sells: least turnover
    assert decision.plan.delta < 0
    assert sum(row["objective"] == "NO_ACTION" for row in decision.candidates) == 1


def test_no_positive_value_keeps_current_holdings():
    decision = decide_stock_day(**inputs(), model=RecordingModel(-1, -2))
    assert decision.action == "HOLD" and decision.plan.delta == 0


def test_risk_override_precedes_model_and_missing_cost_is_not_zero():
    args = inputs()
    args["state"] = PositionState(1000, 1000, D(20000), D(30000), D(11))
    decision = decide_stock_day(**args, model=None)
    assert decision.action == "EXIT" and decision.authority == "FROZEN_RULE_RISK_OVERRIDE"
    args["state"] = PositionState(1000, 1000, D(20000), D(30000))
    with pytest.raises(ActionValueError, match="MODEL_UNAVAILABLE"):
        decide_stock_day(**args, model=None)


def test_confirmed_terminal_listing_blocks_watchlist_open_without_model():
    args = inputs()
    args["state"] = PositionState(0, 0, D(100000), D(100000))

    decision = decide_stock_day(**args, model=None, delist_risk=True)

    assert decision.action == "WAIT"
    assert decision.plan.delta == 0
    assert decision.authority == "FROZEN_RULE_RISK_OVERRIDE"
    assert decision.reason_codes == ("TERMINAL_LISTING_BUY_BLOCKED",)


def test_reference_scenario_never_publishes_quantities_or_executable_alert():
    args = inputs()
    args["state"] = PositionState(0, 0, D(100000), D(100000))
    decision = decide_stock_day(**args, model=RecordingModel())
    assert decision.action == "OPEN" and decision.plan.delta > 0
    public = public_advice(decision, direction_only=True)
    assert public["planned_delta_qty"] is None and public["executable_alert"] is False
    assert public["reference_notional_cny"] == "100000"
    assert "REFERENCE_NOT_USER_BUDGET" in public["reason_codes"]
    assert "candidates" not in public


def test_current_source_missing_or_future_is_not_model_success():
    args = inputs()
    args["bars"].loc[args["bars"].index[-2], "close"] = np.nan
    with pytest.raises(ActionValueError, match="CURRENT_CORE_FEATURE"):
        decide_stock_day(**args, model=RecordingModel())
    args = inputs()
    args["decision_as_of"] = args["decision_as_of"].replace(hour=15)
    with pytest.raises(ActionValueError, match="DECISION_CLOCK"):
        decide_stock_day(**args, model=RecordingModel())

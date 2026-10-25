"""Real Qlib order objects: entry gate must preserve AF order/cash semantics."""
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pandas as pd
import unittest
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

try:
    from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
except ModuleNotFoundError as exc:
    if exc.name != "qlib":
        raise
    raise unittest.SkipTest("Requires the QE worker Qlib environment") from exc
from qe_l2_risk_consumer import L2EntryGateMixin, L2RiskInputs, L2ContractError


def inputs():
    obj = L2RiskInputs.__new__(L2RiskInputs)
    obj.as_of = {"2026-04-01": "2026-03-31", "2026-04-02": "2026-04-01"}
    rows = [{"as_of_date": a, "instrument": s, "l2_code": c, "mapping_status": status,
             "mapping_reason": None if status == "resolved" else "classification:classification_authority_unavailable"}
            for a in obj.as_of.values() for s, c, status in [
                ("000001.SZ", "a", "resolved"), ("000002.SZ", "b", "resolved"),
                ("000003.SZ", None, "unavailable"), ("000004.SZ", "c", "resolved")]]
    obj.mapping = pd.DataFrame(rows).set_index(["as_of_date", "instrument"])
    obj.risk = {(d, c): {"sector_code": c, "probability": p,
                        "availability": "unavailable" if p is None else "available",
                        "warning": None if p is None else p >= .20,
                        "reason_code": "source_unavailable" if p is None else None}
                for d in obj.as_of for c, p in [("a", .20), ("b", .1999), ("c", None)]}
    obj.risk["2026-04-02", "a"]["probability"] = .10
    obj.risk["2026-04-02", "a"]["warning"] = False
    return obj


def check_threshold_missing_recovery_and_time():
    obj = inputs()
    assert obj.evaluate_buy("SZ000001", "2026-04-01", "2026-03-31")[:2] == (False, "RISK_WARNING_BLOCK")
    assert obj.evaluate_buy("000002.SZ", "2026-04-01", "2026-03-31")[0]
    assert obj.evaluate_buy("000003.SZ", "2026-04-01", "2026-03-31")[1] == "INPUT_UNAVAILABLE_BLOCK"
    assert obj.evaluate_buy("000004.SZ", "2026-04-01", "2026-03-31")[1] == "INPUT_UNAVAILABLE_BLOCK"
    assert obj.evaluate_buy("000001.SZ", "2026-04-02", "2026-04-01")[0]
    with unittest.TestCase().assertRaisesRegex(L2ContractError, "TIME_INVALID"):
        obj.evaluate_buy("000001.SZ", "2026-04-02", "2026-03-31")
    with unittest.TestCase().assertRaisesRegex(L2ContractError, "ORDER_MAPPING_MISSING"):
        obj.evaluate_buy("999999.SZ", "2026-04-01", "2026-03-31")


class AFStub:
    def generate_trade_decision(self, execute_result=None):
        return self.original


class Gate(L2EntryGateMixin, AFStub):
    pass


def check_buy_only_no_resize_no_cash_mutation_and_disabled_identity(tmp_path):
    # Real Qlib decision retains order references, amounts, time range and AF sells.
    gate = Gate.__new__(Gate)
    gate.trade_calendar = SimpleNamespace(get_trade_step=lambda: 0,
        get_step_time=lambda step=None, shift=0: (pd.Timestamp("2026-03-31" if shift else "2026-04-01"), pd.Timestamp("2026-04-01")))
    gate._l2_inputs = inputs()
    gate._l2_log = tmp_path / "actions.jsonl"
    gate._l2_seen_dates = set()
    gate._l2_enabled = False
    gate.position = {"000001.SZ": 200, "cash": 123456.0}
    times = {"start_time": pd.Timestamp("2026-04-01"), "end_time": pd.Timestamp("2026-04-01 23:59:59")}
    orders = [Order(stock_id="000001.SZ", amount=100, direction=OrderDir.SELL, **times),
              Order(stock_id="000001.SZ", amount=100, direction=OrderDir.BUY, **times),
              Order(stock_id="000002.SZ", amount=300, direction=OrderDir.BUY, **times)]
    gate.original = TradeDecisionWO(orders, gate)
    assert gate.generate_trade_decision() is gate.original
    assert gate.original.get_decision() == orders
    gate._l2_enabled = True
    decision = gate.generate_trade_decision()
    assert decision is gate.original
    assert decision.get_decision() == [orders[0], orders[2]]
    assert decision.get_decision()[0] is orders[0]
    assert decision.get_decision()[1] is orders[2] and orders[2].amount == 300
    assert gate.position == {"000001.SZ": 200, "cash": 123456.0}
    ledger = json.loads(gate._l2_log.read_text())
    assert ledger["buy_actions"][0]["action"] == "RISK_WARNING_BLOCK"
    assert ledger["buy_actions"][0]["warning"] is True
    assert ledger["buy_actions"][1]["retained_amount"] == 300
    assert not ledger["buy_actions"][0]["planned_order_is_fill"]
    with unittest.TestCase().assertRaisesRegex(L2ContractError, "DUPLICATE_DECISION"):
        gate.generate_trade_decision()


def check_empty_decision_still_validates_time(tmp_path):
    gate = Gate.__new__(Gate)
    gate.trade_calendar = SimpleNamespace(get_trade_step=lambda: 0,
        get_step_time=lambda step=None, shift=0: (pd.Timestamp("2026-04-01"), pd.Timestamp("2026-04-01")))
    gate._l2_inputs = inputs()
    gate._l2_log = tmp_path / "actions.jsonl"
    gate._l2_seen_dates = set()
    gate._l2_enabled = True
    gate.original = TradeDecisionWO([], gate)
    with unittest.TestCase().assertRaisesRegex(L2ContractError, "DECISION_TIME_INVALID"):
        gate.generate_trade_decision()
    assert not gate._l2_log.exists()


class StrategyContractTests(unittest.TestCase):
    def test_threshold(self):
        check_threshold_missing_recovery_and_time()

    def test_order_invariants(self):
        with tempfile.TemporaryDirectory() as folder:
            check_buy_only_no_resize_no_cash_mutation_and_disabled_identity(Path(folder))

    def test_empty_decision_time(self):
        with tempfile.TemporaryDirectory() as folder:
            check_empty_decision_still_validates_time(Path(folder))

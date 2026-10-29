"""Real Qlib decisions: buy replacement must never turn risk into forced sells."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
try:
    from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
except ModuleNotFoundError as exc:
    if exc.name != "qlib":
        raise
    raise unittest.SkipTest("Requires the existing QE worker Qlib environment") from exc
from qe_stock_probability_gate import StockProbabilityInputs, validate_risk_frame
from qe_stock_probability_gate_strategy import StockProbabilityGateMixin, StockProbabilityScoreWeightedTopkStrategyV2


def risk_inputs():
    obj = StockProbabilityInputs.__new__(StockProbabilityInputs)
    obj.calendar = pd.DatetimeIndex(pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07"]))
    obj.decision_dates, obj.threshold, obj._day_cache = obj.calendar[2:], .1, {}
    obj.manifest = {"contract": "QE-STOCK-PROBABILITY-ENTRY-GATE-V1", "arm": "F"}
    rows = [{"signal_date": "2026-01-06", "input_as_of": "2026-01-05", "instrument": f"00000{i}.SZ",
             "probability": p, "availability": "unknown" if p is None else "available",
             "reason": "TEN_INPUT_MASK" if p is None else None}
            for i, p in enumerate([.2, .2, None, .01, .1, .02], 1)]
    obj.frame = validate_risk_frame(pd.DataFrame(rows), obj.calendar)
    return obj


def calendar():
    return SimpleNamespace(get_trade_step=lambda: 0, get_freq=lambda: "day",
        get_step_time=lambda step=None, shift=0: (pd.Timestamp("2026-01-06" if shift else "2026-01-07"),
                                                pd.Timestamp("2026-01-06" if shift else "2026-01-07")))


def enable(gate, path):
    gate._stock_probability_enabled = True
    gate._stock_probability_inputs = risk_inputs()
    gate._stock_probability_seen = set()
    gate._stock_probability_log = path
    return gate


def real_strategy(path, enabled):
    scores = pd.Series([.9, .8, .7, .6, .5, .4], index=[f"SZ00000{i}" for i in range(1, 7)])
    signal = pd.Series(scores.to_numpy(), index=pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-01-06")], scores.index], names=["datetime", "instrument"]))
    obj = StockProbabilityScoreWeightedTopkStrategyV2(signal=signal, topk=3, n_drop=1,
        max_n_drop=1, weight_method="equal", max_weight=1., min_weight=0., max_position_ratio=1., hold_thresh=0)
    pos = SimpleNamespace(get_stock_list=lambda: ["SZ000001"], get_cash=lambda: 1_000_000.,
                          get_stock_amount=lambda sid: 100 if sid == "SZ000001" else 0)
    obj.level_infra = {"trade_calendar": calendar()}
    obj.common_infra = {"trade_account": SimpleNamespace(current_position=pos)}
    obj._trade_exchange = SimpleNamespace(trade_w_adj_price=False, trade_unit=100.,
        get_deal_price=lambda **kw: 10., get_factor=lambda **kw: 1., get_amount_of_trade_unit=lambda **kw: 100.)
    if enabled:
        enable(obj, path)
    return obj


class DummyAF:
    def _prepare_buy_candidates(self, ranked, current_holdings, buy_candidates):
        return buy_candidates

    def generate_trade_decision(self, execute_result=None):
        return self.decision


class OrderGuard(StockProbabilityGateMixin, DummyAF):
    pass


class StockProbabilityStrategyTests(unittest.TestCase):
    def test_real_base_gate_replenishes_without_forcing_high_risk_hold_sale(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "actions.jsonl"
            strategy = real_strategy(path, True)
            orders = strategy.generate_trade_decision().get_decision()
            self.assertEqual([o.stock_id for o in orders], ["SZ000004", "SZ000005"])
            self.assertTrue(all(o.direction == OrderDir.BUY for o in orders))
            self.assertEqual(strategy.trade_position.get_cash(), 1_000_000.)
            self.assertEqual(strategy.trade_position.get_stock_list(), ["SZ000001"])
            # Catalog V2 retains already-ordered safe backups; newer V2 removes
            # them. The gate must preserve either base convention, and only
            # remove unsafe/unknown names.
            backups = [s for s, _ in strategy._backup_candidates]
            self.assertIn("SZ000006", backups)
            self.assertTrue(set(backups).issubset({"SZ000004", "SZ000005", "SZ000006"}))
            record = json.loads(path.read_text())
            self.assertEqual(record["original_buy_candidates"], ["SZ000002", "SZ000003"])
            self.assertEqual([r["action"] for r in record["scanned_candidates"]],
                             ["RISK_WARNING_BLOCK", "UNKNOWN_BLOCK", "ALLOW_BUY", "ALLOW_BUY"])

    def test_disabled_is_original_orders_and_no_log(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "unused.jsonl"
            orders = real_strategy(path, False).generate_trade_decision().get_decision()
            self.assertEqual([o.stock_id for o in orders], ["SZ000002", "SZ000003"])
            self.assertFalse(path.exists())

    def test_final_add_guard_preserves_base_sell_and_amounts(self):
        with tempfile.TemporaryDirectory() as folder:
            obj = enable(OrderGuard.__new__(OrderGuard), Path(folder) / "actions.jsonl")
            obj.trade_calendar = calendar()
            obj.trade_position = SimpleNamespace(get_stock_list=lambda: ["SZ000001"])
            obj._backup_candidates = [("SZ000002", .9), ("SZ000006", .8)]
            orders = [Order("SZ000001", 100, direction, pd.Timestamp("2026-01-07"), pd.Timestamp("2026-01-07"))
                      for direction in [OrderDir.SELL, OrderDir.BUY]]
            obj.decision = TradeDecisionWO(orders, obj)
            result = obj.generate_trade_decision().get_decision()
            self.assertEqual(result, [orders[0]])
            self.assertIs(result[0], orders[0])
            self.assertEqual(obj._backup_candidates, [("SZ000006", .8)])
            self.assertTrue(json.loads(obj._stock_probability_log.read_text())["buy_order_actions"][0]["is_add"])

    def test_equal_score_original_candidate_order_is_preserved(self):
        obj = enable(OrderGuard.__new__(OrderGuard), Path("unused"))
        obj._probability_day, obj._probability_signal = "2026-01-07", "2026-01-06"
        obj._probability_scanned = []
        ranked = pd.Series([.8, .8, .7], index=["SZ000004", "SZ000005", "SZ000006"])
        original = [("SZ000005", .8), ("SZ000004", .8)]
        self.assertEqual(obj._prepare_buy_candidates(ranked, [], original), original)


if __name__ == "__main__":
    unittest.main()

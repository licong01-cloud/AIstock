"""Execute the frozen Qlib strategy and its actual TAIL_BOOST method."""
from pathlib import Path
import json
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
try:
    from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
    from qlib.backtest.utils import TradeCalendarManager
except ModuleNotFoundError as exc:
    if exc.name != "qlib":
        raise
    raise unittest.SkipTest("Requires the existing QE Qlib worker; no fake engine") from exc
# Worker imports must follow the explicit Qlib availability gate and scripts path.
from backend.services.quantevolver.trend_entry_consumer import bind_trend_entry_hooks  # noqa: E402
from qe_suspend_filter_score_weighted_strategy import _SuspendFilterScoreWeightedMixin  # noqa: E402
from qe_trend_entry_admission import CONTRACT, TrendEntryContractError, TrendEntryInputs  # noqa: E402
from qe_trend_entry_strategy import TrendEntryMixin, order_record  # noqa: E402
from tail_twap_strategy import TailTWAPWithLimitStrategy  # noqa: E402


ORIGINAL_SOURCE = (ROOT / "scripts/score_weighted_strategy_v2_deterministic_v1.py").read_text(encoding="utf-8")
original_module, adapted_module = ModuleType("trend_test_original"), ModuleType("trend_test_adapted")
exec(compile(ORIGINAL_SOURCE, "frozen_original.py", "exec"), original_module.__dict__)
exec(compile(bind_trend_entry_hooks(ORIGINAL_SOURCE)[0], "frozen_adapted.py", "exec"), adapted_module.__dict__)
Original = type("Original", (_SuspendFilterScoreWeightedMixin, original_module.ScoreWeightedTopkStrategyV2), {})
Adapted = type("Adapted", (TrendEntryMixin, _SuspendFilterScoreWeightedMixin, adapted_module.ScoreWeightedTopkStrategyV2), {})
CODES = [f"SZ00000{i}" for i in range(1, 8)]


def inputs(values):
    obj = TrendEntryInputs.__new__(TrendEntryInputs)
    obj.calendar = pd.DatetimeIndex(pd.to_datetime(["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-08"]))
    obj.decision_dates = obj.calendar[1:3]
    obj.manifest = {"contract": CONTRACT, "arm": "S10", "seed": 42, "account": {"start": "2026-09-29"}}
    row = np.array(values, dtype=np.float32)
    obj.arrays = {"codes": np.array([f"00000{i}.SZ" for i in range(1, 8)]),
                  "stock_indices": np.tile(np.arange(7, dtype=np.int32), 2),
                  "dates": np.array(["2026-09-28", "2026-09-29"]),
                  "offsets": np.array([0, 7, 14], np.int64),
                  "return20": np.tile(row, 2), "return60": np.full(14, .1, dtype=np.float32),
                  "reason": np.tile(np.where(np.isnan(row), 1, 0).astype(np.uint8), 2)}
    obj._dates = {"2026-09-28": 0, "2026-09-29": 1}
    obj._cache_day, obj._cache = None, {}
    return obj


def make_strategy(path, values=None, held=(), prices=None, factors=None, scores=None, cash=1_000_000.,
                  topk=2, max_drop=1, threshold=.05, day="2026-09-30"):
    gate = values is not None
    scores = scores if scores is not None else [.9, .8, .7, .6, .3, .2, .1]
    index = pd.MultiIndex.from_product([[pd.Timestamp("2026-09-29")], CODES], names=["datetime", "instrument"])
    signal = pd.Series(scores, index=index)
    obj = (Adapted if gate else Original).__new__(Adapted if gate else Original)
    _SuspendFilterScoreWeightedMixin.__init__(obj, signal=signal, topk=topk, n_drop=1, max_n_drop=max_drop,
        min_n_drop=0, threshold_method="fixed", min_improvement=threshold, threshold_floor=threshold,
        weight_method="equal", max_weight=1., min_weight=0., max_position_ratio=1., hold_thresh=0)
    pos = SimpleNamespace(get_stock_list=lambda: list(held), get_cash=lambda: cash,
                          get_stock_amount=lambda sid: 1000. if sid in held else 0.)
    calendar = SimpleNamespace(get_trade_step=lambda: 0, get_freq=lambda: "day",
        get_step_time=lambda step=None, shift=0: (pd.Timestamp("2026-09-29" if shift else day),
                                                pd.Timestamp("2026-09-29" if shift else day)))
    obj.level_infra = {"trade_calendar": calendar}
    obj.common_infra = {"trade_account": SimpleNamespace(current_position=pos)}
    prices, factors = prices or {}, factors or {}
    obj._trade_exchange = SimpleNamespace(trade_w_adj_price=False, trade_unit=100.,
        get_close=lambda *a, **kw: 10., is_stock_tradable=lambda **kw: True,
        get_deal_price=lambda stock_id, **kw: prices.get(stock_id, 10.),
        get_factor=lambda stock_id, **kw: factors.get(stock_id, 1.),
        get_amount_of_trade_unit=lambda stock_id, factor=None, **kw: 100. / (factor or factors.get(stock_id, 1.)))
    if gate:
        obj._trend_inputs, obj._trend_seen, obj._trend_log = inputs(values), set(), path
    return obj


def records(strategy):
    return [order_record(o) for o in strategy.generate_trade_decision().get_decision()]


class TrendEntryStrategyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)

    def test_all_pass_matches_original_frozen_decision(self):
        tmp_path = self.folder
        for held in ((), (CODES[5],), (CODES[5], CODES[6])):
            baseline = make_strategy(tmp_path / "unused", held=held)
            gate = make_strategy(tmp_path / (str(len(held)) + ".jsonl"), [.1] * 7, held=held)
            assert records(gate) == records(baseline)


    def test_same_state_sells_unchanged_when_all_new_buys_fail(self):
        tmp_path = self.folder
        held = (CODES[5], CODES[6])
        expected = [o for o in records(make_strategy(tmp_path / "unused", held=held)) if o["direction"] == int(OrderDir.SELL)]
        actual = records(make_strategy(tmp_path / "audit", [-.1] * 7, held=held))
        assert actual == expected and actual
        row = json.loads((tmp_path / "audit").read_text())
        assert row["cash_after_execution"] == "UNKNOWN_AT_DECISION"
        assert row["actions"][0]["action"] == "WITHHOLD"


    def test_keep_safe_original_buy_and_only_replace_formed_failed_slot(self):
        tmp_path = self.folder
        base = records(make_strategy(tmp_path / "unused"))
        gate = make_strategy(tmp_path / "audit", [-.1, .1, .1, .1, .1, .1, .1])
        result = records(gate)
        assert [r["stock_id"] for r in result] == [CODES[2], CODES[1]]
        assert result[1] == base[1]
        row = json.loads((tmp_path / "audit").read_text())
        assert row["actions"][0]["replacement_rank"] == 3
        assert row["actions"][0]["quote"]["notional"] <= row["actions"][0]["original_slot"]["budget"]


    def test_missing_original_quote_does_not_create_extra_replacement_slot(self):
        tmp_path = self.folder
        gate = make_strategy(tmp_path / "audit", [-.1, .1, .1, .1, .1, .1, .1], prices={CODES[0]: None})
        result = records(gate)
        assert [r["stock_id"] for r in result] == [CODES[1]]


    def test_slot_pair_threshold_is_strict_and_not_recomputed_on_filtered_scores(self):
        tmp_path = self.folder
        # Sold score=.1; replacement score=.6; threshold=.5, so equality cannot pass.
        scores = [.9, .8, .6, .4, .3, .2, .1]
        gate = make_strategy(tmp_path / "audit", [-.1, -.1, .1, .1, .1, .1, .1],
                             held=(CODES[5], CODES[6]), scores=scores, threshold=.5)
        result = records(gate)
        assert all(o["direction"] == int(OrderDir.SELL) for o in result)
        audit = json.loads((tmp_path / "audit").read_text())
        assert audit["actions"][0]["paired_sell"] == CODES[6]
        assert audit["actions"][0]["scan_counts"]["THRESHOLD_STOP"] == 1


    def test_factor_conversion_and_lot_do_not_inflate_slot_budget(self):
        tmp_path = self.folder
        gate = make_strategy(tmp_path / "audit", [-.1, .1, .1, .1, .1, .1, .1],
                             factors={CODES[0]: 2., CODES[2]: .25}, prices={CODES[2]: 13.})
        records(gate)
        action = json.loads((tmp_path / "audit").read_text())["actions"][0]
        assert action["original_slot"]["conversion"] == .5
        assert action["quote"]["conversion"] == 4.
        assert action["quote"]["notional"] <= action["original_slot"]["budget"]
        assert action["quote"]["amount"] % action["quote"]["amount_unit"] == 0


    def test_legal_na_is_blocked_and_invalid_factor_is_an_error(self):
        tmp_path = self.folder
        gate = make_strategy(tmp_path / "audit", [np.nan, .1, .1, .1, .1, .1, .1])
        assert records(gate)[0]["stock_id"] == CODES[2]
        audit = json.loads((tmp_path / "audit").read_text())
        assert audit["actions"][0]["qualification"]["availability"] == "unavailable"
        bad = make_strategy(tmp_path / "absent", [-.1, .1, .1, .1, .1, .1, .1], factors={CODES[2]: np.nan})
        with self.assertRaisesRegex(TrendEntryContractError, "REPLACEMENT_FACTOR"):
            records(bad)
        assert not (tmp_path / "absent").exists()


    def test_no_fill_or_position_mutation_and_original_add_kept(self):
        tmp_path = self.folder
        class Fixed:
            def generate_trade_decision(self, execute_result=None):
                return self.decision
        cls = type("Guard", (TrendEntryMixin, Fixed), {})
        guard = cls.__new__(cls)
        source = make_strategy(tmp_path / "audit", [-.1] * 7, held=(CODES[0],))
        guard.__dict__.update(source.__dict__)
        # The test seam supplies genuine Qlib order objects from an original held-stock add.
        guard.trade_calendar = source.trade_calendar
        guard.trade_position = source.trade_position
        start = pd.Timestamp("2026-09-30")
        buy = Order(CODES[0], 100, OrderDir.BUY, start, start)
        sell = Order(CODES[0], 200, OrderDir.SELL, start, start)
        guard.decision = TradeDecisionWO([sell, buy], guard)
        result = guard.generate_trade_decision().get_decision()
        assert result == [sell, buy] and result[0] is sell and result[1] is buy
        assert guard.trade_position.get_stock_list() == [CODES[0]]


    def test_tail_boost_only_uses_final_outer_buys_and_never_backups(self):
        tmp_path = self.folder
        gate = make_strategy(tmp_path / "audit", [-.1, .1, .1, .1, .1, .1, .1])
        final = gate.generate_trade_decision()
        buys = final.get_decision()
        assert {o.stock_id for o in buys} == {CODES[1], CODES[2]}
        gate._backup_candidates = [(CODES[0], 99.)]  # even a poisoned reserve cannot be consumed by BOOST
        inner = TailTWAPWithLimitStrategy.__new__(TailTWAPWithLimitStrategy)
        inner.outer_trade_decision = final
        inner._original_amount = {o.stock_id: o.amount for o in buys}
        inner.trade_amount_remain = {o.stock_id: o.amount if o.stock_id == CODES[2] else .5 * o.amount for o in buys}
        inner._realloc_extra = {}
        inner._trade_exchange = gate.trade_exchange
        inner._do_realloc(pd.Timestamp("2026-09-30"), pd.Timestamp("2026-09-30 14:55"))
        assert set(inner._realloc_extra) == {CODES[1]}
        assert CODES[0] not in inner._original_amount and CODES[0] not in inner._realloc_extra


    def test_qlib_previous_session_and_last_day_never_use_same_day_signal(self):
        tmp_path = self.folder
        gate = make_strategy(tmp_path / "audit", [.1] * 7)
        dates = pd.DatetimeIndex(pd.to_datetime(["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-08"]))
        source = SimpleNamespace(calendar=lambda **kwargs: dates.to_numpy(), locate_index=lambda *a, **kw: (None, None, 2, 2))
        with mock.patch("qlib.backtest.utils.Cal", source):
            cal = TradeCalendarManager(freq="day", start_time="2026-09-30", end_time="2026-09-30")
        gate.level_infra = {"trade_calendar": cal}
        records(gate)
        row = json.loads((tmp_path / "audit").read_text())
        assert row["signal_date"] == "2026-09-29" and row["trade_date"] == "2026-09-30"
        with self.assertRaisesRegex(TrendEntryContractError, "SIGNAL_TIME"):
            gate._trend_inputs.validate_time("2026-09-30", "2026-09-30")
        with self.assertRaisesRegex(TrendEntryContractError, "SIGNAL_TIME"):
            gate._trend_inputs.validate_time("2026-10-08", "2026-09-30")

if __name__ == "__main__":
    unittest.main()

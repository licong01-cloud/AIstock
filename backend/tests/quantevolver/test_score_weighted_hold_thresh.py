from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _install_qlib_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    qlib = types.ModuleType("qlib")
    qlib_contrib = types.ModuleType("qlib.contrib")
    qlib_contrib_strategy = types.ModuleType("qlib.contrib.strategy")
    qlib_signal_strategy = types.ModuleType("qlib.contrib.strategy.signal_strategy")
    qlib_backtest = types.ModuleType("qlib.backtest")
    qlib_decision = types.ModuleType("qlib.backtest.decision")

    class TopkDropoutStrategy:
        pass

    class Order:
        def __init__(self, *args):
            self.args = args

    class OrderDir:
        BUY = 1
        SELL = 0

    class TradeDecisionWO:
        def __init__(self, orders, strategy):
            self.orders = orders
            self.strategy = strategy

    qlib_signal_strategy.TopkDropoutStrategy = TopkDropoutStrategy
    qlib_decision.Order = Order
    qlib_decision.OrderDir = OrderDir
    qlib_decision.TradeDecisionWO = TradeDecisionWO

    for name, module in {
        "qlib": qlib,
        "qlib.contrib": qlib_contrib,
        "qlib.contrib.strategy": qlib_contrib_strategy,
        "qlib.contrib.strategy.signal_strategy": qlib_signal_strategy,
        "qlib.backtest": qlib_backtest,
        "qlib.backtest.decision": qlib_decision,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _load_strategy_module(monkeypatch: pytest.MonkeyPatch):
    _install_qlib_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    sys.modules.pop("score_weighted_strategy", None)
    return importlib.import_module("score_weighted_strategy")


class _Calendar:
    @staticmethod
    def get_trade_step():
        return 0

    @staticmethod
    def get_step_time(_step, shift=0):
        if shift:
            return "2026-07-15", "2026-07-15"
        return "2026-07-16", "2026-07-16"

    @staticmethod
    def get_freq():
        return "day"


class _Position:
    def __init__(self, counts: dict[str, float | None]):
        self.counts = counts

    def get_stock_count(self, stock_id: str, *, bar=None):
        assert bar == "day"
        return self.counts[stock_id]


class _RuntimePosition(_Position):
    @staticmethod
    def get_stock_list():
        return ["young", "stable"]

    @staticmethod
    def get_stock_amount(_stock_id):
        return 100.0

    @staticmethod
    def get_cash():
        return 100_000.0


class _Signal:
    @staticmethod
    def get_signal(*, start_time, end_time):
        assert start_time == end_time == "2026-07-15"
        return object()


def _strategy(module, *, hold_thresh: int, counts: dict[str, float | None]):
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.topk = 2
    strategy.hold_thresh = hold_thresh
    strategy.trade_calendar = _Calendar()
    strategy.trade_position = _Position(counts)
    return strategy


def test_hold_thresh_blocks_young_sell_and_does_not_overfill(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _strategy(module, hold_thresh=10, counts={"young": 9})

    sells, buys, blocked = strategy._apply_hold_thresh_to_rebalance(
        ["young"],
        ["replacement"],
        ["young", "stable"],
        "2026-07-16",
    )

    assert sells == []
    assert buys == []
    assert blocked == ["young"]


def test_hold_thresh_allows_sell_at_threshold_and_replacement(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _strategy(module, hold_thresh=10, counts={"mature": 10})

    sells, buys, blocked = strategy._apply_hold_thresh_to_rebalance(
        ["mature"],
        ["replacement"],
        ["mature", "stable"],
        "2026-07-16",
    )

    assert sells == ["mature"]
    assert buys == ["replacement"]
    assert blocked == []


def test_generate_trade_decision_applies_hold_thresh_before_buying(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.topk = 2
    strategy.hold_thresh = 10
    strategy.max_n_drop = 1
    strategy.trade_calendar = _Calendar()
    strategy.trade_position = _RuntimePosition({"young": 9})
    strategy.signal = _Signal()
    strategy._last_diag_date = None
    strategy._diag_stats = {}
    strategy.weight_method = "equal"
    strategy.max_single_order_value = 1_000_000.0
    strategy.min_trade_price = 0.5
    strategy.max_trade_price = 5000.0
    strategy.lot_size = 100.0
    strategy.trade_exchange = types.SimpleNamespace(
        get_amount_of_trade_unit=lambda **_kwargs: 100.0,
    )
    scores = module.pd.Series({"replacement": 3.0, "stable": 2.0, "young": 1.0})
    strategy._normalize_signal_scores = lambda _raw, _end: scores
    strategy._apply_hmm_adjustment = lambda current_scores, _date: current_scores
    strategy._filter_dynamic_ndrop = lambda *_args: (["young"], ["replacement"])
    strategy._compute_weights = lambda current_scores: module.np.full(
        len(current_scores), 1.0 / len(current_scores)
    )
    strategy._get_current_price = lambda *_args: 10.0
    strategy._get_current_factor = lambda *_args: 1.0
    strategy._shares_to_adjusted_amount = lambda shares, _factor: shares

    decision = strategy.generate_trade_decision()

    assert decision.orders == []
    assert strategy._diag_stats["hold_blocked_sells"] == 1
    assert strategy._backup_candidates == []


def test_hold_thresh_missing_count_fails_explicitly(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _strategy(module, hold_thresh=10, counts={"unknown": None})

    with pytest.raises(RuntimeError, match="holding-period evidence"):
        strategy._apply_hold_thresh_to_rebalance(
            ["unknown"],
            ["replacement"],
            ["unknown", "stable"],
            "2026-07-16",
        )

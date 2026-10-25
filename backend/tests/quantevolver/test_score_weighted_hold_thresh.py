from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _install_qlib_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    modules = {
        name: types.ModuleType(name)
        for name in (
            "qlib",
            "qlib.contrib",
            "qlib.contrib.strategy",
            "qlib.contrib.strategy.signal_strategy",
            "qlib.backtest",
            "qlib.backtest.decision",
        )
    }

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

    modules["qlib.contrib.strategy.signal_strategy"].TopkDropoutStrategy = TopkDropoutStrategy
    modules["qlib.backtest.decision"].Order = Order
    modules["qlib.backtest.decision"].OrderDir = OrderDir
    modules["qlib.backtest.decision"].TradeDecisionWO = TradeDecisionWO
    for name, module in modules.items():
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
        return ("2026-07-15", "2026-07-15") if shift else ("2026-07-16", "2026-07-16")

    @staticmethod
    def get_freq():
        return "day"


class _Position:
    def __init__(self, counts: dict[str, float | None]):
        self.counts = counts

    def get_stock_count(self, stock_id: str, *, bar=None):
        assert bar == "day"
        return self.counts[stock_id]


class _LegacyPosition(_Position):
    def get_stock_count(self, stock_id: str):
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


def _strategy(module, *, hold_thresh: int, counts: dict[str, float | None]):
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.topk = 2
    strategy.hold_thresh = hold_thresh
    strategy.trade_calendar = _Calendar()
    strategy.trade_position = _Position(counts)
    return strategy


def test_hold_thresh_blocks_young_sell_without_overfilling(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _strategy(module, hold_thresh=10, counts={"young": 9})

    sells, buys, blocked = strategy._apply_hold_thresh_to_rebalance(
        ["young"], ["replacement"], ["young", "stable"], "2026-07-16"
    )

    assert sells == []
    assert buys == []
    assert blocked == ["young"]


def test_hold_thresh_allows_sell_at_threshold_and_legacy_position(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _strategy(module, hold_thresh=10, counts={"mature": 10})
    strategy.trade_position = _LegacyPosition({"mature": 10})

    sells, buys, blocked = strategy._apply_hold_thresh_to_rebalance(
        ["mature"], ["replacement"], ["mature", "stable"], "2026-07-16"
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
    strategy.signal = types.SimpleNamespace(get_signal=lambda **_kwargs: object())
    strategy._last_diag_date = None
    strategy._diag_stats = {}
    strategy.weight_method = "equal"
    strategy.enable_sector_hmm = False
    strategy.max_single_order_value = 1_000_000.0
    strategy.min_trade_price = 0.5
    strategy.max_trade_price = 5000.0
    strategy.lot_size = 100.0
    strategy.trade_exchange = types.SimpleNamespace(
        get_amount_of_trade_unit=lambda **_kwargs: 100.0,
    )
    scores = module.pd.Series({"replacement": 3.0, "stable": 2.0, "young": 1.0})
    strategy._normalize_signal_scores = lambda _raw, _end, *, allow_latest_date_fallback: scores
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


def test_hold_thresh_missing_count_fails_closed(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _strategy(module, hold_thresh=10, counts={"unknown": None})

    with pytest.raises(RuntimeError, match="holding-period evidence"):
        strategy._apply_hold_thresh_to_rebalance(
            ["unknown"], ["replacement"], ["unknown", "stable"], "2026-07-16"
        )


def _weight_strategy(module, *, method="softmax", cap=0.05, floor=0.005, budget=0.95):
    strategy = object.__new__(module.ScoreWeightedTopkStrategy)
    strategy.weight_method = method
    strategy.temperature = 1.0
    strategy.score_clip_quantile = 0.0
    strategy.max_weight = cap
    strategy.min_weight = floor
    strategy.max_position_ratio = budget
    return strategy


def test_score_weights_bound_real_mae50_borda_scores(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    # MA-E50 seed42, signal 2024-07-01: old normalization produced 16.1017%.
    scores = module.np.array([
        4766, 4752, 4745.5, 4737.5, 4735.5, 4735, 4729.5, 4722, 4719, 4718,
        4716, 4714.5, 4711, 4706, 4703.5, 4703.5, 4698, 4695.5, 4691.5, 4690,
        4689.5, 4687, 4685.5, 4684.5, 4680.5, 4679.5, 4677, 4677, 4676, 4675,
        4672.5, 4671, 4670.5, 4669.5, 4669, 4664, 4662.5, 4662, 4658, 4648.5,
        4645.5, 4641, 4640.5, 4633, 4630, 4629.5, 4624, 4621, 4615, 4612,
    ])
    original = scores.copy()
    strategy = _weight_strategy(module)
    weights = strategy._compute_weights(scores)

    assert module.np.isfinite(weights).all()
    assert (weights >= 0).all()
    assert weights.max() <= strategy.max_weight
    assert weights.sum() == pytest.approx(strategy.max_position_ratio, abs=1e-12)
    assert weights[0] == pytest.approx(0.05)
    assert weights[1:] == pytest.approx(module.np.full(49, 0.9 / 49))
    assert module.np.array_equal(scores, original)


@pytest.mark.parametrize("method", ["equal", "rank", "linear", "softmax"])
@pytest.mark.parametrize("count", [3, 20, 50])
def test_score_weights_respect_cap_and_feasible_budget(monkeypatch, method, count):
    module = _load_strategy_module(monkeypatch)
    strategy = _weight_strategy(module, method=method)
    weights = strategy._compute_weights(module.np.linspace(100, 0, count))

    assert module.np.isfinite(weights).all()
    assert (weights >= 0).all()
    assert weights.max() <= strategy.max_weight
    assert weights.sum() == pytest.approx(min(0.95, count * 0.05), abs=1e-12)


def test_score_weights_do_not_resurrect_underflowed_candidates(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _weight_strategy(module)
    weights = strategy._compute_weights(module.np.array([10000.0] + [0.0] * 49))

    assert weights[0] == pytest.approx(0.05)
    assert module.np.count_nonzero(weights[1:]) == 0
    assert weights.sum() == pytest.approx(0.05)


@pytest.mark.parametrize("method", ["equal", "rank", "linear", "softmax"])
def test_score_weights_preserve_already_valid_legacy_output(monkeypatch, method):
    module = _load_strategy_module(monkeypatch)
    strategy = _weight_strategy(module, method=method, floor=0)
    scores = module.np.arange(50, dtype=float) if method != "softmax" else module.np.zeros(50)
    if method == "rank":
        preference = module.np.arange(1, 51, dtype=float)
    elif method == "linear":
        preference = scores + 1e-8
    else:
        preference = module.np.ones(50)
    expected = preference / preference.sum()
    expected = expected / expected.sum() * 0.95

    assert module.np.array_equal(strategy._compute_weights(scores), expected)


@pytest.mark.parametrize("bad_score", [float("nan"), float("inf"), -float("inf")])
def test_score_weights_reject_nonfinite_scores(monkeypatch, bad_score):
    module = _load_strategy_module(monkeypatch)
    with pytest.raises(ValueError, match="finite"):
        _weight_strategy(module)._compute_weights(module.np.array([1, bad_score]))


@pytest.mark.parametrize("options", [
    {"cap": -0.05}, {"floor": 0.06}, {"budget": -0.95}, {"budget": float("nan")},
])
def test_score_weights_reject_inconsistent_bounds(monkeypatch, options):
    module = _load_strategy_module(monkeypatch)
    with pytest.raises(ValueError, match="bounds"):
        _weight_strategy(module, **options)._compute_weights(module.np.array([1, 2]))


@pytest.mark.parametrize("options", [{"budget": 0}, {"cap": 0, "floor": 0}])
def test_score_weights_zero_budget_or_cap_stays_cash(monkeypatch, options):
    module = _load_strategy_module(monkeypatch)
    weights = _weight_strategy(module, **options)._compute_weights(module.np.array([1, 2]))
    assert module.np.array_equal(weights, module.np.zeros(2))


def test_score_weights_series_permutation(monkeypatch):
    module = _load_strategy_module(monkeypatch)
    strategy = _weight_strategy(module)
    scores = module.pd.Series(module.np.linspace(100, 0, 50), index=[f"stock_{i}" for i in range(50)])
    expected = strategy._compute_weights(scores)
    reversed_weights = strategy._compute_weights(scores.iloc[::-1])
    assert module.np.asarray(reversed_weights[::-1]) == pytest.approx(module.np.asarray(expected))
    assert len(strategy._compute_weights(module.np.array([]))) == 0

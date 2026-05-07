"""V25.1 QE wrapper config-alias regression tests."""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
WRAPPER_PATH = PROJECT_ROOT / "scripts" / "tail_twap_v25_1_strategy.py"


def _load_v25_1_wrapper(monkeypatch):
    qlib_mod = types.ModuleType("qlib")
    backtest_mod = types.ModuleType("qlib.backtest")
    decision_mod = types.ModuleType("qlib.backtest.decision")
    decision_mod.Order = type("Order", (), {})
    qlib_mod.backtest = backtest_mod
    backtest_mod.decision = decision_mod
    monkeypatch.setitem(sys.modules, "qlib", qlib_mod)
    monkeypatch.setitem(sys.modules, "qlib.backtest", backtest_mod)
    monkeypatch.setitem(sys.modules, "qlib.backtest.decision", decision_mod)

    parent_mod = types.ModuleType("tail_twap_v25_strategy")

    class ParentStrategy:
        def __init__(self, *args, **kwargs):
            self.parent_args = args
            self.parent_kwargs = dict(kwargs)

    parent_mod.EARLY_LEN = 30
    parent_mod.LATE_LEN = 210
    parent_mod.TOTAL_LEN = 240
    parent_mod.TailTWAPWithV25TwoStageStrategy = ParentStrategy
    parent_mod._V25MarketNoFill = RuntimeError
    parent_mod._is_valid_factor = lambda value: True
    parent_mod._is_valid_price = lambda value: True
    monkeypatch.setitem(sys.modules, "tail_twap_v25_strategy", parent_mod)

    spec = importlib.util.spec_from_file_location(
        "tail_twap_v25_1_strategy_alias_test", WRAPPER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.TailTWAPWithV25_1SmallCapStrategy


def test_v25_1_wrapper_accepts_generic_catalog_aliases(monkeypatch):
    strategy_cls = _load_v25_1_wrapper(monkeypatch)

    strategy = strategy_cls(
        min_cost=7.0,
        commission_rate=0.00025,
        tolerance_bps=6.5,
        max_buckets=12,
        device="cpu",
    )

    assert strategy._v25_1_min_cost == 7.0
    assert strategy._v25_1_commission_rate == 0.00025
    assert strategy._v25_1_tolerance_bps == 6.5
    assert strategy._v25_1_max_buckets == 12
    assert strategy.parent_kwargs == {"device": "cpu"}


def test_v25_1_wrapper_still_accepts_prefixed_config(monkeypatch):
    strategy_cls = _load_v25_1_wrapper(monkeypatch)

    strategy = strategy_cls(
        v25_1_min_cost=6.0,
        v25_1_commission_rate=0.0004,
        v25_1_tolerance_bps=9.0,
        v25_1_max_buckets=18,
    )

    assert strategy._v25_1_min_cost == 6.0
    assert strategy._v25_1_commission_rate == 0.0004
    assert strategy._v25_1_tolerance_bps == 9.0
    assert strategy._v25_1_max_buckets == 18
    assert strategy.parent_kwargs == {}


def test_v25_1_wrapper_rejects_conflicting_generic_and_prefixed_config(monkeypatch):
    strategy_cls = _load_v25_1_wrapper(monkeypatch)

    with pytest.raises(ValueError, match="conflicting config aliases"):
        strategy_cls(v25_1_max_buckets=30, max_buckets=12)


def test_v25_1_wrapper_allows_same_numeric_alias_value(monkeypatch):
    strategy_cls = _load_v25_1_wrapper(monkeypatch)

    strategy = strategy_cls(v25_1_max_buckets=12, max_buckets="12")

    assert strategy._v25_1_max_buckets == 12
    assert strategy.parent_kwargs == {}

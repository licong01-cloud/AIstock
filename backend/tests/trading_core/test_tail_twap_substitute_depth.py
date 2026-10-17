from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _load_tail_strategy(monkeypatch: pytest.MonkeyPatch):
    qlib = types.ModuleType("qlib")
    qlib_backtest = types.ModuleType("qlib.backtest")
    qlib_decision = types.ModuleType("qlib.backtest.decision")
    qlib_utils = types.ModuleType("qlib.backtest.utils")
    qlib_contrib = types.ModuleType("qlib.contrib")
    qlib_strategy = types.ModuleType("qlib.contrib.strategy")
    qlib_rule_strategy = types.ModuleType("qlib.contrib.strategy.rule_strategy")

    class Order:
        BUY = 0
        SELL = 1

    class OrderDir:
        BUY = 0

    class TradeDecisionWO:
        pass

    class TWAPStrategy:
        def __init__(self, *args, **kwargs):
            pass

    qlib_decision.Order = Order
    qlib_decision.OrderDir = OrderDir
    qlib_decision.TradeDecisionWO = TradeDecisionWO
    qlib_utils.get_start_end_idx = lambda *args, **kwargs: (0, 239)
    qlib_rule_strategy.TWAPStrategy = TWAPStrategy

    for name, module in {
        "qlib": qlib,
        "qlib.backtest": qlib_backtest,
        "qlib.backtest.decision": qlib_decision,
        "qlib.backtest.utils": qlib_utils,
        "qlib.contrib": qlib_contrib,
        "qlib.contrib.strategy": qlib_strategy,
        "qlib.contrib.strategy.rule_strategy": qlib_rule_strategy,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    sys.modules.pop("tail_twap_strategy", None)
    return importlib.import_module("tail_twap_strategy")


def test_substitute_never_inspects_candidates_beyond_configured_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_tail_strategy(monkeypatch)
    strategy = module.TailTWAPWithLimitStrategy(
        unfilled_handler="TAIL_SUBSTITUTE",
        unfilled_backup_depth=2,
    )

    blocked_order = types.SimpleNamespace(stock_id="BLOCKED", direction=module.Order.BUY)
    trade_position = types.SimpleNamespace(get_stock_list=lambda: [])
    outer_strategy = types.SimpleNamespace(
        _backup_candidates=[("A", 3.0), ("B", 2.0), ("C", 1.0)],
        topk=20,
        trade_position=trade_position,
    )
    strategy.outer_trade_decision = types.SimpleNamespace(
        get_decision=lambda: [blocked_order],
        strategy=outer_strategy,
    )
    strategy._original_amount = {"BLOCKED": 100.0}
    strategy.trade_amount_remain = {"BLOCKED": 100.0}
    strategy._realloc_extra = {}

    inspected: list[str] = []

    class Exchange:
        def get_deal_price(self, stock_id, **kwargs):
            return 10.0

        def is_stock_tradable(self, stock_id, **kwargs):
            inspected.append(stock_id)
            return stock_id == "C"

        def get_amount_of_trade_unit(self, **kwargs):
            return 100

    strategy.trade_exchange = Exchange()
    fallback_calls: list[tuple[object, object]] = []
    strategy._do_realloc = lambda start, end: fallback_calls.append((start, end))

    strategy._do_realloc_substitute("14:55", "14:56")

    assert inspected == ["A", "B"]
    assert strategy._realloc_extra == {}
    assert fallback_calls == [("14:55", "14:56")]


def test_boost_behavior_does_not_gain_a_substitute_only_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_tail_strategy(monkeypatch)

    strategy = module.TailTWAPWithLimitStrategy(
        unfilled_handler="TAIL_BOOST",
        unfilled_backup_depth=0,
    )

    assert strategy._unfilled_handler == "TAIL_BOOST"

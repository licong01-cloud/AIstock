from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
REGISTER_SCRIPT = PROJECT_ROOT / "scripts" / "register_score_weighted_strategy_v2_capacity_v1.py"
STRATEGY_SOURCE = PROJECT_ROOT / "scripts" / "score_weighted_strategy_v2_capacity_v1.py"
LEGACY_STRATEGY_SOURCE = PROJECT_ROOT / "scripts" / "score_weighted_strategy.py"
LEGACY_STRATEGY_V2_SOURCE = PROJECT_ROOT / "scripts" / "score_weighted_strategy_v2.py"


def _load_register_module():
    spec = importlib.util.spec_from_file_location("register_score_weighted_strategy_v2_capacity_v1", REGISTER_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_capacity_strategy_registration_metadata_has_new_strategy_id_and_file():
    module = _load_register_module()

    assert module.STRATEGY_ID == "score_weighted_topk_v2_capacity_v1"
    assert module.CLASS_NAME == "ScoreWeightedTopkStrategyV2CapacityV1"
    assert module.STRATEGY_FILE == STRATEGY_SOURCE
    assert module.DEFAULT_KWARGS["max_single_order_value"] == 1_000_000_000.0


def test_capacity_strategy_param_schema_exposes_capacity_fields():
    module = _load_register_module()
    fields = {field["name"]: field for field in module.PARAM_SCHEMA}

    assert fields["max_single_order_value"]["default"] == 1_000_000_000.0
    assert fields["max_weight"]["default"] == 0.05
    assert fields["max_position_ratio"]["default"] == 0.95


def _install_qlib_strategy_stubs(monkeypatch):
    qlib = types.ModuleType("qlib")
    qlib_contrib = types.ModuleType("qlib.contrib")
    qlib_contrib_strategy = types.ModuleType("qlib.contrib.strategy")
    qlib_signal_strategy = types.ModuleType("qlib.contrib.strategy.signal_strategy")
    qlib_backtest = types.ModuleType("qlib.backtest")
    qlib_decision = types.ModuleType("qlib.backtest.decision")

    class TopkDropoutStrategy:
        def __init__(self, signal=None, topk=50, n_drop=5, **kwargs):
            self.signal = signal
            self.topk = topk
            self.n_drop = n_drop
            for key, value in kwargs.items():
                setattr(self, key, value)

    class Order:
        pass

    class OrderDir:
        BUY = 1
        SELL = 0

    class TradeDecisionWO:
        pass

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


def test_capacity_strategy_imports_with_aistock_scripts_only(monkeypatch):
    assert STRATEGY_SOURCE.exists()
    assert LEGACY_STRATEGY_SOURCE.exists()
    assert LEGACY_STRATEGY_V2_SOURCE.exists()

    _install_qlib_strategy_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(PROJECT_ROOT / "scripts"))
    for module_name in (
        "score_weighted_strategy",
        "score_weighted_strategy_v2",
        "score_weighted_strategy_v2_capacity_v1",
    ):
        sys.modules.pop(module_name, None)

    module = importlib.import_module("score_weighted_strategy_v2_capacity_v1")
    strategy = module.ScoreWeightedTopkStrategyV2CapacityV1(signal="signal", topk=3, n_drop=1)

    assert strategy.topk == 3
    assert strategy.n_drop == 1
    assert strategy.max_single_order_value == 1_000_000_000.0
    assert strategy.max_weight == 0.05
    assert strategy.max_position_ratio == 0.95

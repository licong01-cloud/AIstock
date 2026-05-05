import importlib
import sys
import types
from pathlib import Path

import numpy as np
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _install_import_stubs(monkeypatch):
    torch = types.ModuleType("torch")
    nn = types.ModuleType("torch.nn")
    functional = types.ModuleType("torch.nn.functional")

    class _DummyModule:
        def __init__(self, *args, **kwargs):
            pass

        def to(self, *args, **kwargs):
            return self

        def eval(self):
            return self

        def load_state_dict(self, *args, **kwargs):
            return None

    nn.Module = _DummyModule
    nn.Sequential = lambda *args, **kwargs: _DummyModule()
    nn.Linear = lambda *args, **kwargs: _DummyModule()
    nn.ReLU = lambda *args, **kwargs: _DummyModule()
    nn.Dropout = lambda *args, **kwargs: _DummyModule()
    nn.Embedding = lambda *args, **kwargs: _DummyModule()
    torch.nn = nn
    torch.device = lambda value: value
    torch.load = lambda *args, **kwargs: {}

    qlib = types.ModuleType("qlib")
    qlib_backtest = types.ModuleType("qlib.backtest")
    qlib_decision = types.ModuleType("qlib.backtest.decision")
    qlib_utils = types.ModuleType("qlib.backtest.utils")
    qlib_contrib = types.ModuleType("qlib.contrib")
    qlib_strategy = types.ModuleType("qlib.contrib.strategy")
    qlib_rule_strategy = types.ModuleType("qlib.contrib.strategy.rule_strategy")

    class _Order:
        BUY = 0
        SELL = 1

        def __init__(self, stock_id, amount, start_time, end_time, direction):
            self.stock_id = stock_id
            self.amount = amount
            self.start_time = start_time
            self.end_time = end_time
            self.direction = direction

    class _TradeDecisionWO:
        def __init__(self, order_list, strategy):
            self.order_list = order_list
            self.strategy = strategy

    class _TWAPStrategy:
        def reset(self, *args, **kwargs):
            return None

    qlib_decision.Order = _Order
    qlib_decision.OrderDir = _Order
    qlib_decision.TradeDecisionWO = _TradeDecisionWO
    qlib_utils.get_start_end_idx = lambda *args, **kwargs: (0, 239)
    qlib_rule_strategy.TWAPStrategy = _TWAPStrategy

    for name, module in {
        "torch": torch,
        "torch.nn": nn,
        "torch.nn.functional": functional,
        "qlib": qlib,
        "qlib.backtest": qlib_backtest,
        "qlib.backtest.decision": qlib_decision,
        "qlib.backtest.utils": qlib_utils,
        "qlib.contrib": qlib_contrib,
        "qlib.contrib.strategy": qlib_strategy,
        "qlib.contrib.strategy.rule_strategy": qlib_rule_strategy,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _load_v25_module(monkeypatch):
    _install_import_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    sys.modules.pop("tail_twap_strategy", None)
    sys.modules.pop("tail_twap_v25_strategy", None)
    return importlib.import_module("tail_twap_v25_strategy")


def _load_v25_1_module(monkeypatch):
    _install_import_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    sys.modules.pop("tail_twap_strategy", None)
    sys.modules.pop("tail_twap_v25_strategy", None)
    sys.modules.pop("tail_twap_v25_1_strategy", None)
    return importlib.import_module("tail_twap_v25_1_strategy")


class _Quote:
    def __init__(self, values):
        self.values = values

    def get_data(self, stock_id, start_time, end_time, field, method):
        return self.values.get(field)


class _Exchange:
    def __init__(self, close=5.6, quote_values=None, suspended=False):
        self.close = close
        self.quote = _Quote(quote_values or {})
        self.suspended = suspended

    def get_close(self, stock_id, start_time, end_time, method):
        return self.close

    def check_stock_suspended(self, stock_id, start_time, end_time):
        return self.suspended


def _strategy(module, exchange):
    strategy = object.__new__(module.TailTWAPWithV25TwoStageStrategy)
    strategy.trade_exchange = exchange
    strategy._qe_suspend_filter = None
    strategy._v25_no_fill_reasons = {}
    return strategy


def test_v25_prev_close_nan_with_zero_volume_is_market_no_fill(monkeypatch):
    module = _load_v25_module(monkeypatch)
    strategy = _strategy(
        module,
        _Exchange(
            close=5.6,
            quote_values={"$prev_close": np.nan, "$volume": 0.0},
        ),
    )

    with pytest.raises(module._V25MarketNoFill) as exc_info:
        strategy._generate_plan_for_order("600027.SH", module.Order.BUY, "2024-07-19 09:30:00", "2024-07-19 09:30:00")

    assert exc_info.value.reason == "intraday_halt_or_no_bar"


def test_v25_prev_close_nan_with_positive_volume_remains_data_error(monkeypatch):
    module = _load_v25_module(monkeypatch)
    strategy = _strategy(
        module,
        _Exchange(
            close=5.6,
            quote_values={"$prev_close": np.nan, "$volume": 100.0},
        ),
    )

    with pytest.raises(RuntimeError, match="prev_close_missing_data_error"):
        strategy._generate_plan_for_order("600027.SH", module.Order.BUY, "2024-07-18 09:30:00", "2024-07-18 09:30:00")


def test_v25_exchange_suspended_has_explicit_reason(monkeypatch):
    module = _load_v25_module(monkeypatch)
    strategy = _strategy(module, _Exchange(suspended=True))

    assert strategy._market_block_reason("600027.SH", "2024-07-19", "2024-07-19") == "suspended_by_exchange"


def test_v25_qlib_adjusted_price_is_converted_to_raw_with_factor(monkeypatch):
    module = _load_v25_module(monkeypatch)
    strategy = _strategy(
        module,
        _Exchange(
            quote_values={
                "$factor": 0.5088173721747152,
                "$volume": 100.0,
            },
        ),
    )

    raw_price, factor = strategy._require_raw_price(
        "003010.SZ",
        "2024-07-09 09:31:00",
        "2024-07-09 09:31:00",
        adjusted_price=5.261172,
        field="$close",
    )

    assert factor == pytest.approx(0.5088173721747152)
    assert raw_price == pytest.approx(10.34, abs=1e-4)


def test_v25_missing_factor_is_data_error_without_market_evidence(monkeypatch):
    module = _load_v25_module(monkeypatch)
    strategy = _strategy(
        module,
        _Exchange(
            quote_values={
                "$factor": np.nan,
                "$volume": 100.0,
            },
        ),
    )

    with pytest.raises(RuntimeError, match="factor_missing_data_error"):
        strategy._require_raw_price(
            "003010.SZ",
            "2024-07-09 09:31:00",
            "2024-07-09 09:31:00",
            adjusted_price=5.261172,
            field="$close",
        )


def test_v25_missing_factor_with_no_bar_is_market_no_fill(monkeypatch):
    module = _load_v25_module(monkeypatch)
    strategy = _strategy(
        module,
        _Exchange(
            quote_values={
                "$factor": np.nan,
                "$volume": 0.0,
            },
        ),
    )

    with pytest.raises(module._V25MarketNoFill) as exc_info:
        strategy._require_raw_price(
            "003010.SZ",
            "2024-07-09 09:31:00",
            "2024-07-09 09:31:00",
            adjusted_price=5.261172,
            field="$close",
        )

    assert exc_info.value.reason == "intraday_halt_or_no_bar"


def test_v25_limit_checks_use_raw_price_not_adjusted_price(monkeypatch):
    module = _load_v25_module(monkeypatch)

    raw_close = module._to_raw_price(5.261172, 0.5088173721747152)
    assert raw_close == pytest.approx(10.34, abs=1e-4)
    assert not module._price_at_or_below(raw_close, 9.30)

    raw_limit_up_close = module._to_raw_price(12.393847, 0.987557473142882)
    assert raw_limit_up_close == pytest.approx(12.55, abs=1e-4)
    assert module._price_at_or_above(raw_limit_up_close, 12.55)


def test_v25_1_star_child_orders_are_not_rounded_by_qlib_trade_unit(monkeypatch):
    module = _load_v25_1_module(monkeypatch)
    strategy = object.__new__(module.TailTWAPWithV25_1SmallCapStrategy)

    assert strategy._minimum_child_order_amount("688981.SH", module.Order.BUY, 100) == 200
    assert strategy._legalize_child_order_amount(
        "688981.SH", module.Order.BUY, 199, 199, 100
    ) == 0
    assert strategy._legalize_child_order_amount(
        "688981.SH", module.Order.BUY, 200, 200, 100
    ) == 200
    assert strategy._legalize_child_order_amount(
        "688981.SH", module.Order.BUY, 201, 201, 100
    ) == 201
    assert strategy._legalize_child_order_amount(
        "688981.SH", module.Order.BUY, 202, 202, 100
    ) == 202


def test_v25_1_star_sell_residual_and_main_board_rules(monkeypatch):
    module = _load_v25_1_module(monkeypatch)
    strategy = object.__new__(module.TailTWAPWithV25_1SmallCapStrategy)

    assert strategy._legalize_child_order_amount(
        "688981.SH",
        module.Order.SELL,
        150,
        150,
        100,
        allow_sell_residual=True,
    ) == 150
    assert strategy._legalize_child_order_amount(
        "688981.SH", module.Order.SELL, 501, 501, 100
    ) == 501
    assert strategy._legalize_child_order_amount(
        "000001.SZ", module.Order.BUY, 99, 99, 100
    ) == 0
    assert strategy._legalize_child_order_amount(
        "000001.SZ", module.Order.BUY, 201, 201, 100
    ) == 200
    assert strategy._legalize_child_order_amount(
        "000001.SZ",
        module.Order.SELL,
        250,
        250,
        100,
        allow_sell_residual=True,
    ) == 200


def test_v25_1_child_order_legalization_uses_qlib_factor_when_trade_unit_disabled(monkeypatch):
    module = _load_v25_1_module(monkeypatch)
    strategy = object.__new__(module.TailTWAPWithV25_1SmallCapStrategy)

    factor = 0.9845601320266724
    strategy._read_quote_data = lambda *_args, **_kwargs: factor
    main_amount = strategy._legalize_child_order_amount(
        "300750.SZ",
        module.Order.BUY,
        107.6,
        107.6,
        None,
        trade_start_time="2026-04-21 09:33:00",
        trade_end_time="2026-04-21 09:33:59",
    )
    assert main_amount * factor == pytest.approx(100.0)

    star_factor = 0.9998064041137695
    strategy._read_quote_data = lambda *_args, **_kwargs: star_factor
    star_amount = strategy._legalize_child_order_amount(
        "689009.SH",
        module.Order.BUY,
        203.4 / star_factor,
        203.4 / star_factor,
        None,
        trade_start_time="2026-04-21 09:34:00",
        trade_end_time="2026-04-21 09:34:59",
    )
    assert star_amount * star_factor == pytest.approx(203.0)


def test_v25_1_minimum_child_amount_uses_qlib_factor_when_trade_unit_disabled(monkeypatch):
    module = _load_v25_1_module(monkeypatch)
    strategy = object.__new__(module.TailTWAPWithV25_1SmallCapStrategy)

    factor = 0.5
    strategy._read_quote_data = lambda *_args, **_kwargs: factor

    assert strategy._minimum_child_order_amount(
        "688981.SH",
        module.Order.BUY,
        None,
        trade_start_time="2026-04-21 09:33:00",
        trade_end_time="2026-04-21 09:33:59",
    ) == pytest.approx(400.0)
    assert strategy._minimum_child_order_amount(
        "000001.SZ",
        module.Order.BUY,
        None,
        trade_start_time="2026-04-21 09:33:00",
        trade_end_time="2026-04-21 09:33:59",
    ) == pytest.approx(200.0)


def test_v25_1_empty_buy_schedule_fails_instead_of_falling_back_to_v25(monkeypatch):
    module = _load_v25_1_module(monkeypatch)
    strategy = object.__new__(module.TailTWAPWithV25_1SmallCapStrategy)
    strategy.trade_amount_remain = {"688981.SH": 199}
    strategy._v25_1_min_cost = 5.0
    strategy._v25_1_commission_rate = 0.0003
    strategy._v25_1_tolerance_bps = 10.0
    strategy._v25_1_max_buckets = 30
    monkeypatch.setattr(
        module.TailTWAPWithV25TwoStageStrategy,
        "_generate_plan_for_order",
        lambda self, *_args: np.ones(module.TOTAL_LEN) / module.TOTAL_LEN,
    )
    strategy._read_quote_data = lambda *_args, **_kwargs: 10.0
    strategy._require_raw_price = lambda *_args, **_kwargs: (10.0, 1.0)

    with pytest.raises(module._V25MarketNoFill) as exc_info:
        strategy._generate_plan_for_order(
            "688981.SH", module.Order.BUY, "2026-05-05 09:30:00", "2026-05-05 09:30:00"
        )

    assert exc_info.value.reason == "buy_below_board_lot"

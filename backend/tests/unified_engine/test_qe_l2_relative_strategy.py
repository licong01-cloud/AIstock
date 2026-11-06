"""Direct order invariants using native Qlib Order; also runnable on a worker."""
# ruff: noqa: E402 -- explicit worker module path.
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
from qe_l2_relative_consumer import relative_targets
from qe_l2_relative_strategy import RelativeMixin


def example(*, factor=1.0, held=None, cash=60000.0, nav=100000.0, unavailable=(), blocked=(), hold_ok=True):
    held = {"a": 3000.0, "old": 1000.0} if held is None else held
    obj = RelativeMixin.__new__(RelativeMixin)
    obj.trade_position = SimpleNamespace(get_stock_list=lambda: list(held), get_stock_amount=lambda s: held[s],
                                        calculate_value=lambda: nav, get_cash=lambda: cash)
    obj.trade_exchange = SimpleNamespace(open_cost=.000095, close_cost=.000595, min_cost=5.0,
                                        get_amount_of_trade_unit=lambda **kw: 100 / factor)
    obj._qe_risk_policy = SimpleNamespace(force_exit_symbols=lambda stocks, day: set(),
                                         is_buy_allowed=lambda stock, day: stock not in blocked)
    obj._get_current_price = lambda stock, step, direction: None if stock in unavailable else 10.0
    obj._get_current_factor = lambda stock, step: factor
    obj._can_sell_under_hold_thresh = lambda stock, day: hold_ok
    obj._is_orderable_without_warning = lambda *args: True
    obj.forbid_all_trade_at_limit = False
    obj.min_trade_price, obj.max_trade_price, obj.max_single_order_value = 0., 10000., 10000000.
    return obj


def require_native():
    try:
        from qlib.backtest.decision import OrderDir
        return OrderDir
    except ImportError:
        import pytest
        pytest.skip("native Qlib is verified in the existing worker environment")


def test_native_absolute_targets_sell_before_buy_and_cost_cash():
    direction = require_native()
    obj = example()
    orders, _, cash = obj._relative_orders({"a": .02, "b": .03}, ["a", "b"], 0, "2026-04-01", "2026-04-01")
    assert [(o.stock_id, o.direction, o.amount) for o in orders] == [
        ("a", direction.SELL, 2800.), ("old", direction.SELL, 1000.), ("b", direction.BUY, 300.)]
    expected = 60000 + 28000 - 28000 * .000595 + 10000 - 10000 * .000595 - 3000 - 5
    assert abs(cash["planned_cash_after_orders"] - expected) < 1e-8
    assert cash["planned_sell_proceeds_are_not_fills"] is True


def test_native_adjusted_units_and_missing_quotes_do_not_invent_amounts():
    require_native()
    obj = example(factor=.5, held={}, cash=100000., unavailable=["a"])
    orders, events, _ = obj._relative_orders({"a": .02, "b": .03}, ["a", "b"], 0, "2026-04-01", "2026-04-01")
    assert len(orders) == 1 and orders[0].stock_id == "b" and orders[0].amount == 200
    assert events[0]["reason"] == "QUOTE_UNAVAILABLE_OR_SUSPENDED"


def test_native_neutral_target_control_and_hold_or_buy_blocks():
    require_native()
    base = {"a": .02, "b": .03}
    neutral, _ = relative_targets(base, ["a", "b"], {"a": .5, "b": .5}, apply_risk=True)
    left = example()._relative_orders(base, ["a", "b"], 0, "2026-04-01", "2026-04-01")
    right = example()._relative_orders(neutral, ["a", "b"], 0, "2026-04-01", "2026-04-01")
    assert [(o.stock_id, o.direction, o.amount) for o in left[0]] == [(o.stock_id, o.direction, o.amount) for o in right[0]]
    assert left[1:] == right[1:]
    orders, events, _ = example(blocked=["b"], hold_ok=False)._relative_orders(
        base, ["a", "b"], 0, "2026-04-01", "2026-04-01")
    assert not orders and {e["reason"] for e in events} == {"HOLD_THRESH", "PIT_OR_EVENT_BUY_BLOCK"}


def test_native_cash_and_fee_cap_no_overcommit():
    require_native()
    obj = example(held={}, cash=1005.0)
    orders, _, budget = obj._relative_orders({"a": .03, "b": .03}, ["a", "b"], 0, "2026-04-01", "2026-04-01")
    assert len(orders) == 1 and orders[0].amount == 100.0
    assert budget["planned_cash_after_orders"] == 0.0


if __name__ == "__main__":
    # No pytest dependency when run in the already installed native worker.
    from qlib.backtest.decision import Order
    assert Order is not None
    for test in [test_native_absolute_targets_sell_before_buy_and_cost_cash,
                 test_native_adjusted_units_and_missing_quotes_do_not_invent_amounts,
                 test_native_neutral_target_control_and_hold_or_buy_blocks,
                 test_native_cash_and_fee_cap_no_overcommit]:
        test()
    print("NATIVE_QLIB_ORDER_TESTS_PASSED=4")

from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.services.trading_core.errors import InvalidStateTransitionError
from backend.services.trading_core.models import Fill, OrderIntent, OrderSide, OrderStatus, StepFill
from backend.services.trading_core.oms import OMS


def make_intent(quantity: int = 300) -> OrderIntent:
    return OrderIntent(
        package_id="pkg_1",
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=quantity,
        target_trade_date=date(2024, 1, 2),
    )


def test_oms_partial_and_full_fill() -> None:
    oms = OMS()
    order = oms.create_order(make_intent())
    assert order.status == OrderStatus.SUBMITTED

    fill_1 = Fill(
        order_id=order.order_id,
        symbol=order.symbol,
        side=order.side,
        quantity=100,
        price=10.0,
        trade_time=datetime(2024, 1, 2, 9, 31),
        reason="unit fill",
    )
    order, event_1 = oms.apply_fill(order, fill_1)
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_quantity == 100
    assert event_1.fill == fill_1

    fill_2 = Fill(
        order_id=order.order_id,
        symbol=order.symbol,
        side=order.side,
        quantity=200,
        price=11.0,
        trade_time=datetime(2024, 1, 2, 9, 32),
        reason="unit fill",
    )
    order, _ = oms.apply_fill(order, fill_2)
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == 300
    assert round(order.avg_fill_price or 0, 6) == round((100 * 10 + 200 * 11) / 300, 6)


def test_oms_rejects_overfill_and_final_cancel() -> None:
    oms = OMS()
    order = oms.create_order(make_intent(quantity=100))
    fill = Fill(
        order_id=order.order_id,
        symbol=order.symbol,
        side=order.side,
        quantity=100,
        price=10.0,
        trade_time=datetime(2024, 1, 2, 9, 31),
        reason="unit fill",
    )
    order, _ = oms.apply_fill(order, fill)

    with pytest.raises(InvalidStateTransitionError, match="cannot cancel"):
        oms.cancel_order(order, "too late")


def test_order_intent_requires_round_lot() -> None:
    with pytest.raises(ValueError, match="board-lot rules"):
        make_intent(quantity=50)


def test_star_market_order_intent_accepts_board_lot_increment() -> None:
    intent = OrderIntent(
        package_id="pkg_1",
        portfolio_id="paper_1",
        symbol="688678.SH",
        side=OrderSide.BUY,
        quantity=233,
        target_trade_date=date(2024, 1, 2),
    )

    assert intent.quantity == 233


def test_star_market_fill_accepts_board_lot_increment() -> None:
    fill = Fill(
        order_id="ord_1",
        symbol="688678.SH",
        side=OrderSide.BUY,
        quantity=233,
        price=23.91,
        trade_time=datetime(2024, 1, 2, 9, 31),
        reason="star board fill",
    )
    step = StepFill(
        symbol="688678.SH",
        side=OrderSide.BUY,
        quantity=233,
        price=23.91,
        bar_time=datetime(2024, 1, 2, 9, 31),
        reason="star board step",
    )

    assert fill.quantity == 233
    assert step.quantity == 233


def test_main_board_fill_still_requires_100_share_round_lot() -> None:
    with pytest.raises(ValueError, match="board-lot rules"):
        Fill(
            order_id="ord_1",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=233,
            price=10.0,
            trade_time=datetime(2024, 1, 2, 9, 31),
            reason="invalid main board fill",
        )

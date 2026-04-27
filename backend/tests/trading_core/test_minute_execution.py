from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.trading_core.errors import DataUnavailableError, ExecutionAlgoError, UnsupportedFeatureError
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderStatus
from backend.services.trading_core.oms import OMS
from backend.services.paper_trading_v2.models import OrderExecutionState


def make_order(quantity: int = 600):
    intent = OrderIntent(
        package_id="pkg_1",
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=quantity,
        target_trade_date=date(2024, 1, 2),
    )
    return OMS().create_order(intent)


def make_bars(count: int = 3, volume: int = 10000) -> list[MinuteBar]:
    start = datetime(2024, 1, 2, 9, 31)
    return [
        MinuteBar(
            symbol="000001.SZ",
            bar_time=start + timedelta(minutes=i),
            open=10.0 + i * 0.01,
            high=10.2 + i * 0.01,
            low=9.9 + i * 0.01,
            close=10.1 + i * 0.01,
            volume=volume,
            limit_up=11.0,
            limit_down=9.0,
        )
        for i in range(count)
    ]


def test_minute_execution_twap_fills_order() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=600)

    final_order, fills, events = engine.execute_order(
        order=order,
        minute_bars=make_bars(3),
        algo_code="TWAP",
        algo_config={"split_count": 3},
        allow_partial_fill=False,
    )

    assert final_order.status == OrderStatus.FILLED
    assert sum(fill.quantity for fill in fills) == 600
    assert len(fills) == 3
    assert len(events) == 3


def test_minute_execution_requires_minute_bars() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(DataUnavailableError, match="minute bars are required"):
        engine.execute_order(
            order=make_order(),
            minute_bars=[],
            algo_code="TWAP",
            algo_config={"split_count": 3},
        )


def test_minute_execution_rejects_unsupported_algo() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(UnsupportedFeatureError, match="not registered"):
        engine.execute_order(
            order=make_order(),
            minute_bars=make_bars(),
            algo_code="UNKNOWN_ALGO",
            algo_config={},
        )


def test_minute_execution_rejects_unavailable_v24_plan_model() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(ExecutionAlgoError, match="V24_PLAN is unavailable"):
        engine.execute_order(
            order=make_order(),
            minute_bars=make_bars(31),
            algo_code="V24_PLAN",
            algo_config={"model_path": "missing/v24_plan_net.pt"},
            market_context={
                "prev_close": 10.0,
                "full_day_close": [10.0] * 31,
                "full_day_volume": [10000] * 31,
                "full_day_high": [10.2] * 31,
                "full_day_low": [9.8] * 31,
            },
        )


def test_minute_execution_fails_when_participation_rate_exceeded() -> None:
    engine = MinuteExecutionEngine()
    with pytest.raises(ExecutionAlgoError, match="max_participation_rate"):
        engine.execute_order(
            order=make_order(quantity=600),
            minute_bars=make_bars(1, volume=1000),
            algo_code="CLOSE_PRICE",
            algo_config={"max_participation_rate": 0.1},
        )


def test_incremental_minute_execution_advances_cursor_without_duplicate_fills() -> None:
    engine = MinuteExecutionEngine()
    order = make_order(quantity=600)
    state = OrderExecutionState(
        session_id="psess_1",
        run_id="prun_1",
        order_id=order.order_id,
        symbol=order.symbol,
        trade_date=date(2024, 1, 2),
        algo_code="TWAP",
        filled_quantity=0,
        remaining_quantity=order.quantity,
        status=order.status.value,
    )

    updated_order, updated_state, fills, events = engine.execute_order_incremental(
        order=order,
        execution_state=state,
        new_bars=make_bars(1),
        algo_code="TWAP",
        algo_config={"split_count": 3},
    )

    assert updated_order.status == OrderStatus.PARTIALLY_FILLED
    assert updated_state.last_processed_bar_time == make_bars(1)[0].bar_time
    assert sum(fill.quantity for fill in fills) == 200
    assert len(events) == 1

    replayed_order, replayed_state, replayed_fills, replayed_events = engine.execute_order_incremental(
        order=updated_order,
        execution_state=updated_state,
        new_bars=[],
        algo_code="TWAP",
        algo_config={"split_count": 3},
    )
    assert replayed_order == updated_order
    assert replayed_state == updated_state
    assert replayed_fills == []
    assert replayed_events == []

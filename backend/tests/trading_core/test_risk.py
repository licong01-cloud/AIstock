from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.trading_core.errors import DataUnavailableError, RiskRuleError
from backend.services.trading_core.models import (
    MinuteBar,
    OrderIntent,
    OrderSide,
    StepFill,
)
from backend.services.trading_core.oms import OMS
from backend.services.trading_core.risk import RiskEngine


def make_order(side: OrderSide = OrderSide.BUY):
    intent = OrderIntent(
        package_id="pkg_1",
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=side,
        quantity=300,
        target_trade_date=date(2024, 1, 2),
    )
    return OMS().create_order(intent)


def make_bars(*, close: float = 10.1, volume: int = 10000) -> list[MinuteBar]:
    start = datetime(2024, 1, 2, 9, 31)
    return [
        MinuteBar(
            symbol="000001.SZ",
            bar_time=start + timedelta(minutes=i),
            open=close,
            high=max(close, 10.2),
            low=min(close, 9.9),
            close=close,
            volume=volume,
            limit_up=11.0,
            limit_down=9.0,
        )
        for i in range(3)
    ]


def test_risk_engine_passes_normal_minute_context() -> None:
    decision = RiskEngine().validate_order_execution_context(
        order=make_order(),
        minute_bars=make_bars(),
    )

    assert decision.passed is True


def test_risk_engine_requires_limit_price_data() -> None:
    bars = [
        bar.model_copy(update={"limit_up": None, "limit_down": None})
        for bar in make_bars()
    ]

    with pytest.raises(DataUnavailableError, match="limit price is required"):
        RiskEngine().validate_order_execution_context(
            order=make_order(),
            minute_bars=bars,
        )


def test_risk_engine_blocks_buy_fill_at_limit_up() -> None:
    order = make_order(OrderSide.BUY)
    bar = make_bars(close=11.0)[0]
    step = StepFill(
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        price=11.0,
        bar_time=bar.bar_time,
        reason="unit",
    )

    with pytest.raises(RiskRuleError, match="limit up"):
        RiskEngine().validate_step_fill(order=order, step_fill=step, bar=bar)


def test_risk_engine_blocks_sell_context_when_all_bars_at_limit_down() -> None:
    with pytest.raises(RiskRuleError, match="limit down"):
        RiskEngine().validate_order_execution_context(
            order=make_order(OrderSide.SELL),
            minute_bars=make_bars(close=9.0),
        )

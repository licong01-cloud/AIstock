from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from backend.services.trading_core.errors import DataUnavailableError, RiskRuleError
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.models import Fill, OrderSide, PositionLot


def make_fill(
    *,
    side: OrderSide,
    quantity: int = 1000,
    price: float = 10.0,
    trade_time: datetime = datetime(2024, 1, 2, 9, 31),
) -> Fill:
    return Fill(
        order_id="ord_1",
        symbol="000001.SZ",
        side=side,
        quantity=quantity,
        price=price,
        trade_time=trade_time,
        reason="unit fill",
    )


def test_ledger_buy_settle_and_sell() -> None:
    ledger = InMemoryLedger(
        portfolio_id="paper_1",
        initial_cash=100_000.0,
        fee_model=FeeModel(open_cost=0.001, close_cost=0.002, min_cost=5.0),
    )
    buy = make_fill(side=OrderSide.BUY, quantity=1000, price=10.0)
    ledger.apply_fill(buy)

    assert ledger.cash == pytest.approx(100_000 - 10_000 - 10)
    assert ledger.positions["000001.SZ"].quantity == 1000
    assert ledger.positions["000001.SZ"].available_quantity == 0

    ledger.settle_trade_date(datetime(2024, 1, 3).date())
    assert ledger.positions["000001.SZ"].available_quantity == 1000

    sell = make_fill(
        side=OrderSide.SELL,
        quantity=400,
        price=11.0,
        trade_time=datetime(2024, 1, 3, 9, 31),
    )
    ledger.apply_fill(sell)

    assert ledger.positions["000001.SZ"].quantity == 600
    assert ledger.positions["000001.SZ"].available_quantity == 600
    assert ledger.cash == pytest.approx(100_000 - 10_000 - 10 + 4_400 - 8.8)


def test_ledger_rejects_insufficient_cash() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=1_000.0)
    with pytest.raises(RiskRuleError, match="insufficient cash"):
        ledger.apply_fill(make_fill(side=OrderSide.BUY, quantity=1000, price=10.0))


def test_ledger_rejects_t_plus_one_sell() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=100_000.0)
    ledger.apply_fill(make_fill(side=OrderSide.BUY, quantity=1000, price=10.0))

    with pytest.raises(RiskRuleError, match="T\\+1"):
        ledger.apply_fill(make_fill(side=OrderSide.SELL, quantity=100, price=10.5))


def test_ledger_snapshot_requires_prices() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=100_000.0)
    ledger.apply_fill(make_fill(side=OrderSide.BUY, quantity=1000, price=10.0))

    with pytest.raises(DataUnavailableError, match="missing positive price"):
        ledger.account_snapshot(prices={}, snapshot_time=datetime(2024, 1, 2, 15, 0))

    snapshot = ledger.account_snapshot(
        prices={"000001.SZ": 10.2},
        snapshot_time=datetime(2024, 1, 2, 15, 0),
    )
    assert snapshot.nav == pytest.approx(ledger.cash + 10_200)


def test_restored_order_fee_does_not_charge_the_minimum_again() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=20_000)
    ledger.restore_order_fee_state(
        order_id="ord_1", symbol="000001.SZ", side=OrderSide.BUY,
        cumulative_notional=Decimal("1000"), charged_fee=Decimal("5"),
    )
    assert ledger.cash == 20_000
    assert not ledger.positions and not ledger.fills and not ledger.cash_entries
    ledger.apply_fill(make_fill(side=OrderSide.BUY, quantity=100, price=10))
    assert ledger.cash_entries[0].fee == Decimal("0.00")
    assert ledger.cash == 19_000


def test_restored_fee_credits_actual_historical_charges_without_refunding() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=20_000)
    ledger.restore_order_fee_state(
        order_id="ord_1", symbol="000001.SZ", side=OrderSide.BUY,
        cumulative_notional=Decimal("60000"), charged_fee=Decimal("10"),
    )
    ledger.apply_fill(make_fill(side=OrderSide.BUY, quantity=100, price=10))
    assert ledger.cash_entries[0].fee == Decimal("0.00")


@pytest.mark.parametrize("notional,fee", [("-1", "5"), ("1000", "-1"), ("NaN", "5")])
def test_restored_order_fee_rejects_invalid_money(notional: str, fee: str) -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=20_000)
    with pytest.raises(ValueError):
        ledger.restore_order_fee_state(
            order_id="ord_1", symbol="000001.SZ", side=OrderSide.BUY,
            cumulative_notional=Decimal(notional), charged_fee=Decimal(fee),
        )


def test_order_fee_restore_is_idempotent_and_rejects_conflicting_prefix() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=20_000)
    history = dict(order_id="ord_1", symbol="000001.SZ", side=OrderSide.BUY,
                   cumulative_notional=Decimal("1000"), charged_fee=Decimal("5"))
    ledger.restore_order_fee_state(**history)
    ledger.restore_order_fee_state(**history)
    with pytest.raises(ValueError, match="LOCAL_SIM_ORDER_FEE_HISTORY_CONFLICT"):
        ledger.restore_order_fee_state(**{**history, "charged_fee": Decimal("10")})
    ledger.apply_fill(make_fill(side=OrderSide.BUY, quantity=100, price=10))
    assert ledger.cash_entries[0].fee == 0


def test_restored_sell_fee_does_not_charge_the_minimum_again() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_1", initial_cash=20_000)
    ledger.positions["000001.SZ"] = PositionLot(
        portfolio_id="paper_1", symbol="000001.SZ", quantity=100, available_quantity=100,
        avg_cost=10, trade_date=datetime(2024, 1, 1).date(),
    )
    ledger.restore_order_fee_state(
        order_id="ord_1", symbol="000001.SZ", side=OrderSide.SELL,
        cumulative_notional=Decimal("1000"), charged_fee=Decimal("5"),
    )
    ledger.apply_fill(make_fill(side=OrderSide.SELL, quantity=100, price=10))
    assert ledger.cash_entries[0].fee == 0
    assert ledger.cash == 21_000

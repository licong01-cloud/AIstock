from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    OrderIntentRecord,
    PositionLotRecord,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


class CountingBroker:
    def __init__(self, positions: list[dict] | None = None) -> None:
        self.positions = positions or []
        self.place_order_calls = 0
        self.cancel_order_calls = 0

    def get_positions(self) -> list[dict]:
        return self.positions

    def place_order(self, **kwargs):
        self.place_order_calls += 1
        return 108, "accepted"

    def cancel_order(self, order_id: str):
        self.cancel_order_calls += 1
        return True, "cancelled"


def _repo(cash: Decimal = Decimal("10000000"), available_lot: int = 0) -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=cash,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    if available_lot:
        repo.create_position_lot(
            PositionLotRecord(
                lot_id="lot_a",
                strategy_id="strat_a",
                symbol="300604.SZ",
                open_trade_id="trade_a",
                open_date=TRADE_DATE,
                quantity=available_lot,
                available_quantity=available_lot,
                remaining_quantity=available_lot,
                avg_cost=Decimal("10"),
                cost_amount=Decimal(available_lot * 10),
                account_id=ACCOUNT_ID,
            )
        )
    return repo


def _buy_request(**overrides) -> ManagedOrderRequest:
    data = {
        "account_id": ACCOUNT_ID,
        "strategy_name": "poc_strategy_a",
        "symbol": "300604.SZ",
        "side": "BUY",
        "order_type": BUY_ORDER_TYPE,
        "quantity": 1000,
        "price_type": 5,
        "price": Decimal("10"),
        "order_remark": "remark_buy",
        "trade_date": TRADE_DATE,
        "mode": "SIM",
    }
    data.update(overrides)
    return ManagedOrderRequest(**data)


def _sell_request(**overrides) -> ManagedOrderRequest:
    data = {
        "account_id": ACCOUNT_ID,
        "strategy_name": "poc_strategy_a",
        "symbol": "300604.SZ",
        "side": "SELL",
        "order_type": SELL_ORDER_TYPE,
        "quantity": 1000,
        "price_type": 5,
        "price": Decimal("10"),
        "order_remark": "remark_sell",
        "trade_date": TRADE_DATE,
        "mode": "SIM",
    }
    data.update(overrides)
    return ManagedOrderRequest(**data)


def test_preview_rejects_blank_strategy_without_broker_call() -> None:
    broker = CountingBroker()
    result = QmtManagedOrderService(repository=_repo(), broker=broker).preview_order(_buy_request(strategy_name=" "))

    assert result.allowed is False
    assert [error.code for error in result.errors] == ["BLANK_STRATEGY_NAME"]
    assert broker.place_order_calls == 0


def test_preview_rejects_duplicate_remark_without_broker_call() -> None:
    repo = _repo()
    repo.create_order_intent(
        OrderIntentRecord(
            intent_id="intent_existing",
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            symbol="300604.SZ",
            side="BUY",
            order_type=BUY_ORDER_TYPE,
            quantity=1000,
            price_type=5,
            order_remark="remark_buy",
            account_id=ACCOUNT_ID,
            trade_date=TRADE_DATE,
        )
    )
    broker = CountingBroker()

    result = QmtManagedOrderService(repository=repo, broker=broker).preview_order(_buy_request())

    assert result.allowed is False
    assert "DUPLICATE_ORDER_REMARK" in {error.code for error in result.errors}
    assert broker.place_order_calls == 0


def test_preview_rejects_insufficient_cash_and_buy_board_lot() -> None:
    result = QmtManagedOrderService(repository=_repo(cash=Decimal("100")), broker=CountingBroker()).preview_order(
        _buy_request(quantity=101)
    )

    assert result.allowed is False
    assert {"BUY_BOARD_LOT", "INSUFFICIENT_CASH"} <= {error.code for error in result.errors}


def test_preview_accepts_star_market_buy_quantity_after_minimum() -> None:
    service = QmtManagedOrderService(repository=_repo(), broker=CountingBroker())

    assert service.preview_order(_buy_request(symbol="688379.SH", quantity=201)).allowed is True
    assert service.preview_order(_buy_request(symbol="689009.SH", quantity=2706)).allowed is True


def test_preview_rejects_star_market_buy_quantity_below_minimum() -> None:
    result = QmtManagedOrderService(repository=_repo(), broker=CountingBroker()).preview_order(
        _buy_request(symbol="688379.SH", quantity=199)
    )

    assert result.allowed is False
    [error] = result.errors
    assert error.code == "BUY_BOARD_LOT"
    assert error.context["symbol"] == "688379.SH"
    assert error.context["min_quantity"] == 200
    assert error.context["increment"] == 1
    assert error.context["canonical_quantity"] == 0


def test_preview_rejects_main_board_and_chinext_non_100_share_buys() -> None:
    service = QmtManagedOrderService(repository=_repo(), broker=CountingBroker())

    for symbol in ("600000.SH", "000001.SZ", "300604.SZ"):
        result = service.preview_order(_buy_request(symbol=symbol, quantity=101))

        assert result.allowed is False
        [error] = result.errors
        assert error.code == "BUY_BOARD_LOT"
        assert error.context["symbol"] == symbol
        assert error.context["min_quantity"] == 100
        assert error.context["increment"] == 100
        assert error.context["canonical_quantity"] == 100


def test_preview_rejects_t1_strategy_lot_shortage() -> None:
    result = QmtManagedOrderService(repository=_repo(available_lot=0), broker=CountingBroker()).preview_order(
        _sell_request(quantity=1000)
    )

    assert result.allowed is False
    assert [error.code for error in result.errors] == ["INSUFFICIENT_STRATEGY_AVAILABLE_LOT"]


def test_submit_rejects_broker_can_sell_shortage_before_order_call() -> None:
    broker = CountingBroker(positions=[{"stock_code": "300604.SZ", "quantity": 1000, "can_sell": 0}])

    result = QmtManagedOrderService(repository=_repo(available_lot=1000), broker=broker).submit_order(_sell_request())

    assert result.success is False
    assert result.broker_called is False
    assert result.preflight.broker_can_sell == 0
    assert [error.code for error in result.preflight.errors] == ["INSUFFICIENT_BROKER_CAN_SELL"]
    assert broker.place_order_calls == 0
